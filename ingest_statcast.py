"""Pull one day of Statcast data, verify it, land it in GCS, load it into BigQuery.

Exit codes:
    0  success — a day's events were written, uploaded and loaded
    3  no games on that date (off-day, All-Star break, full rainout)
    1  anything else (validation failure, network, BigQuery error)
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from pybaseball import cache, statcast

cache.enable()

# --- configuration ----------------------------------------------------------

LOCAL_ROOT = Path("output")
GCS_BUCKET = "mlb-catcher-framing"
GCS_PREFIX = "raw/statcast"

BQ_DATASET = "STATCAST_RAW_DATA"
BQ_TABLE = "STATCAST_PITCH_DATA"
PARTITION_FIELD = "game_date"

# BigQuery field type -> the pandas dtype that writes to it cleanly.
BQ_TO_PANDAS = {
    "FLOAT": "float64",
    "INTEGER": "Int64",
    "STRING": "string",
    "BOOLEAN": "boolean",
}


class VerificationError(Exception):
    """Raised when data doesn't match what the pipeline assumes about it."""


# --- paths: one source of truth ---------------------------------------------

def partition_tail(date):
    """The path fragment shared by every layer: dt=<date>/statcast.parquet."""
    return f"dt={date}/statcast.parquet"


def local_path(date):
    return LOCAL_ROOT / partition_tail(date)


def blob_path(date):
    # A GCS object name, not a filesystem path — always forward slashes.
    return f"{GCS_PREFIX}/{partition_tail(date)}"


def table_ref(client):
    return f"{client.project}.{BQ_DATASET}.{BQ_TABLE}"


# --- CLI --------------------------------------------------------------------

def valid_date(s):
    """Validate and normalize a YYYY-MM-DD string for argparse."""
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid date: {s!r} (expected YYYY-MM-DD)"
        )


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


# --- extract ----------------------------------------------------------------

def get_statcast_day(date):
    """Return one day of Statcast events, or an empty frame if no games.

    Timestamps are cast to microsecond precision because BigQuery's Parquet
    reader does not understand nanosecond timestamps and falls back to reading
    the underlying INT64 — which cannot be used as a partition field.
    """
    df = statcast(start_dt=date, end_dt=date)

    if df.empty:
        return df

    # Unconditional: cached pybaseball responses can return this as a string.
    df[PARTITION_FIELD] = (
        pd.to_datetime(df[PARTITION_FIELD]).astype("datetime64[us]")
    )
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    dates = df[PARTITION_FIELD].unique()
    if len(dates) != 1:
        raise VerificationError(
            f"expected one {PARTITION_FIELD} for {date}, "
            f"found {len(dates)}: {sorted(dates)}"
        )
    if not (df[PARTITION_FIELD].dt.normalize() == df[PARTITION_FIELD]).all():
        raise VerificationError(
            f"{PARTITION_FIELD} has a time component; "
            "DAY partitioning is by UTC day and would drift"
        )

    return df


def conform_to_table_schema(df):
    """Make this day's column types match the BigQuery table's.

    autodetect types each day's Parquet file independently, and pandas types a
    column with no values at all as Int64 — so any column that happens to be
    entirely null on a given day arrives as INTEGER and the partition load is
    rejected. Small slates (a playoff day, a rained-out schedule) make an
    all-null column far likelier than a full regular-season day does.

    Two conversions, both lossless:
      - an all-null column is cast to whatever the table already holds
      - an integer column is widened where the table holds FLOAT

    Nothing else is coerced. Narrowing a populated column would lose data and
    should fail loudly rather than be silently repaired.
    """
    client = bigquery.Client()

    try:
        table = client.get_table(table_ref(client))
    except NotFound:
        return df  # first load — autodetect establishes the schema

    for field in table.schema:
        if field.name not in df.columns:
            continue
        target = BQ_TO_PANDAS.get(field.field_type)
        if target is None:
            continue

        col = df[field.name]
        if str(col.dtype) == target:
            continue

        if col.isna().all():
            df[field.name] = col.astype(target)
        elif target == "float64" and pd.api.types.is_integer_dtype(col):
            df[field.name] = col.astype("float64")

    return df


# --- load: local ------------------------------------------------------------

def write_parquet(df, date):
    """Write the day's events to a date-partitioned local Parquet file."""
    out_path = local_path(date)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path)
    return out_path


def verify_parquet(df, path):
    """Read the file back and confirm it matches df. Raises on any divergence.

    All problems are collected and raised together, so one run reports the
    whole picture rather than the first thing that went wrong.
    """
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
        detail = ", ".join(
            f"{c}: {before} -> {after}" for c, (before, after) in changed.items()
        )
        problems.append(f"dtypes changed on {len(changed)} column(s): {detail}")

    if problems:
        raise VerificationError(
            f"round-trip verification failed for {path}\n  - "
            + "\n  - ".join(problems)
        )


# --- load: cloud ------------------------------------------------------------

def upload_to_gcs(local_file, date):
    """Upload a local Parquet file to its partition path in GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path(date))
    blob.upload_from_filename(local_file)
    return f"gs://{GCS_BUCKET}/{blob_path(date)}"


def load_to_bigquery(date):
    """Load one day into its own partition, replacing any rows already there.

    Steady state uses a partition decorator plus WRITE_TRUNCATE, so the
    truncate applies to that one partition and the replacement is atomic.
    The first load has no table to decorate, so it creates one partitioned
    on game_date.
    """
    client = bigquery.Client()
    table_id = table_ref(client)
    uri = f"gs://{GCS_BUCKET}/{blob_path(date)}"

    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    if table_exists:
        destination = f"{table_id}${date.replace('-', '')}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
    else:
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
    job.result()  # blocks; raises if the load failed

    table = client.get_table(table_id)
    print(f"{table_id}: {table.num_rows} rows total")
    return table_id


# --- entry point ------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()

    df = get_statcast_day(args.date)
    if df.empty:
        print(f"{args.date}: no games, nothing to load")
        sys.exit(3)

    df = conform_to_table_schema(df)

    out_path = write_parquet(df, args.date)
    verify_parquet(df, out_path)
    uri = upload_to_gcs(out_path, args.date)
    table_id = load_to_bigquery(args.date)

    print(f"{args.date}: {df.shape[0]} events -> {uri} -> {table_id}")