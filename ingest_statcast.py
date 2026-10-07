import argparse
from datetime import datetime
from pybaseball import cache
from pybaseball import statcast
from pathlib import Path
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import sys



BQ_DATASET = "STATCAST_RAW_DATA"           
BQ_TABLE = "STATCAST_PITCH_DATA"
PARTITION_FIELD = "game_date"

cache.enable()

LOCAL_ROOT = Path("output")
GCS_BUCKET = "mlb-catcher-framing"
GCS_PREFIX = "raw/statcast"

class VerificationError(Exception):
    """Raised when a written file doesn't match the DataFrame in memory."""


def verify_parquet(df, path):
    """Read the file back and confirm it matches df. Raises on any divergence."""
    df2 = pd.read_parquet(path)

    problems = []

    if df.shape != df2.shape:
        problems.append(f"shape: wrote {df.shape}, read back {df2.shape}")

    if list(df.columns) != list(df2.columns):
        missing = set(df.columns) - set(df2.columns)
        added = set(df2.columns) - set(df.columns)
        if missing:
            problems.append(f"columns lost: {sorted(missing)}")
        if added:
            problems.append(f"columns appeared: {sorted(added)}")

    shared = [c for c in df.columns if c in df2.columns]
    changed = {
        c: (str(df[c].dtype), str(df2[c].dtype))
        for c in shared
        if df[c].dtype != df2[c].dtype
    }
    if changed:
        detail = ", ".join(f"{c}: {before} -> {after}" for c, (before, after) in changed.items())
        problems.append(f"dtypes changed on {len(changed)} column(s): {detail}")

    if problems:
        raise VerificationError(
            f"round-trip verification failed for {path}\n  - "
            + "\n  - ".join(problems)
        )

def write_parquet(df, date):
    """Write the day's pitches to a date-partitioned local Parquet file."""
    out_path = local_path(date)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path)
    return out_path

def partition_tail(date):
    """The path fragment shared by every layer: dt=<date>/statcast.parquet."""
    return f"dt={date}/statcast.parquet"


def local_path(date):
    return LOCAL_ROOT / partition_tail(date)


def blob_path(date):
    return f"{GCS_PREFIX}/{partition_tail(date)}"

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid date: {s!r} (expected YYYY-MM-DD)"
        )


def get_statcast_day(date):
    df = statcast(start_dt=date, end_dt=date)
    if df.empty:
        return df                      # no games; caller decides
    df[PARTITION_FIELD] = pd.to_datetime(df[PARTITION_FIELD]).astype("datetime64[us]")
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    dates = df[PARTITION_FIELD].unique()
    if len(dates) != 1:
        raise VerificationError(
            f"expected one game_date for {date}, found {len(dates)}: {sorted(dates)}"
        )
    if not (df[PARTITION_FIELD].dt.normalize() == df[PARTITION_FIELD]).all():
        raise VerificationError(
            f"{PARTITION_FIELD} has a time component; UTC-day partitioning would drift"
        )
    return df


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pull one day of Statcast pitch data."
    )
    parser.add_argument(
        "date",
        type=valid_date,
        help="date to pull, formatted YYYY-MM-DD",
    )
    return parser.parse_args()


def upload_to_gcs(local_file, date):
    """Upload a local Parquet file to its partition path in GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path(date))
    blob.upload_from_filename(local_file)
    return f"gs://{GCS_BUCKET}/{blob_path(date)}"

def load_to_bigquery(date):
    """Load one day into its own partition, replacing any existing rows for that day."""
    client = bigquery.Client()
    table_id = f"{client.project}.{BQ_DATASET}.{BQ_TABLE}"
    uri = f"gs://{GCS_BUCKET}/{blob_path(date)}"

    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    if table_exists:
        # Steady state: target the single partition.
        destination = f"{table_id}${date.replace('-', '')}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
        # First load: create the table partitioned on game_date.
        destination = table_id
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=PARTITION_FIELD,
            ),
        )

    job = client.load_table_from_uri(uri, destination, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"{table_id}: {table.num_rows} rows total")
    return table_id

if __name__ == "__main__":
    args = parse_args()
    df = get_statcast_day(args.date)
    if df.empty:
        print(f"{args.date}: no games, nothing to load")
        sys.exit(0)
    out_path = write_parquet(df, args.date)
    verify_parquet(df, out_path)
    uri = upload_to_gcs(out_path, args.date)
    table_id = load_to_bigquery(args.date)
    print(f"{args.date}: {df.shape[0]} pitches -> {uri} -> {table_id}")

   