# baseball-warehouse

Pitch-level daily Statcast ingestion into BigQuery.

`ingest_statcast.py` pulls a single day of Statcast data from Baseball Savant,
writes it to a partitioned local Parquet file, verifies,
uploads it to GCS, and loads it into BigQuery — replacing that day's partition
atomically so re-runs are safe.

---

## Architecture

```
Baseball Savant  ──(pybaseball)──▶  pandas DataFrame
                                          │
                                          ▼
                          output/dt=<date>/statcast.parquet   (local)
                                          │
                                    round-trip verify
                                          │
                                          ▼
      gs://mlb-catcher-framing/raw/statcast/dt=<date>/statcast.parquet   (GCS, us-west1)
                                          │
                                    load job (WRITE_TRUNCATE)
                                          ▼
      mlb-catcher-framing.STATCAST_RAW_DATA.STATCAST_PITCH_DATA$<YYYYMMDD>
```

Every layer derives its path from a single function, `partition_tail(date)`,
which returns `dt=<date>/statcast.parquet`. The local writer, the GCS uploader,
and the BigQuery loader all build on that one string. Path drift between stages
is a classic silent pipeline bug — one source of truth prevents it.

**Region note:** the GCS bucket and the BigQuery dataset are both in `us-west1`.
BigQuery cannot load from a bucket in a different location, and datasets cannot
be moved after creation, so this pairing is deliberate rather than incidental.

---

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Google Cloud authentication:

```bash
gcloud auth application-default login
gcloud config set project mlb-catcher-framing
```

Create the BigQuery dataset (the load job creates the *table*, but not the
dataset):

```bash
bq --location=us-west1 mk --dataset STATCAST_RAW_DATA
```

The GCS bucket `mlb-catcher-framing` must already exist in `us-west1` with
versioning off.

---

## Usage

One day:

```bash
python3 ingest_statcast.py 2026-08-01
```

A backfill range:

```bash
for d in 2026-08-0{1..7}; do
  echo "=== $d ==="
  python3 ingest_statcast.py "$d" || { echo "FAILED on $d"; break; }
done
```

The `|| break` is deliberate: without it a mid-week failure scrolls past and the
backfill finishes looking successful with a day missing.

The date argument is positional and validated against `YYYY-MM-DD` before any
network call, so a malformed date fails immediately rather than thirty seconds
later as an empty result.

---

## Verification

### Rows per day

```sql
SELECT game_date, COUNT(*) AS pitches
FROM `mlb-catcher-framing.STATCAST_RAW_DATA.STATCAST_PITCH_DATA`
GROUP BY game_date
ORDER BY game_date;
```

Results after loading Aug 1–7, 2026:

| game_date | pitches |
|---|---|
| 2025-06-01 | 4215 |
| 2026-08-01 | 4438 |
| 2026-08-02 | 4321 |
| 2026-08-03 | 2356 |
| 2026-08-04 | 4167 |
| 2026-08-05 | 4376 |
| 2026-08-06 | 3530 |
| 2026-08-07 | 4355 |

### Idempotency evidence

Re-running a single already-loaded date and re-running the query above returns
**identical counts**. This is the partition-overwrite design working: the load
replaces one day's partition rather than appending to the table.


### Object-level check

```bash
gcloud storage ls -l "gs://mlb-catcher-framing/raw/statcast/**"
```

Eight objects, one per day, sizes matching the local files. A write that
returned without error is not a verified write.

---

## Decisions

### Raw-zone philosophy: faithful to source

The raw layer's job is to be re-derivable and un-opinionated. Column types are
not cast at ingest and unused columns are not dropped: if a type opinion turns
out to be wrong, fixing it in staging costs a dbt run, while fixing it in the
raw layer costs a full re-pull from Savant. Type opinions belong downstream,
where they can change without a backfill.

One caveat worth stating plainly: the Parquet engine already imposes a schema
whether or not we ask it to, so "faithful" here means *we* add no opinions, not
that no opinions exist.

### Schema autodetect at load

Parquet files carry their own schema, so BigQuery derives column names and types
directly from the file. Hand-maintaining a 100+ column schema would add drift
risk and maintenance work for no fidelity gain — and it would be the same kind
of type opinion the raw layer explicitly defers to staging. Autodetect keeps
that deferral consistent across both the file and the table.

### One deliberate exception: timestamp precision

pandas produces `datetime64[ns]`, and BigQuery's Parquet reader understands only
millisecond and microsecond timestamp logical types. A nanosecond column is read
as its underlying `INT64`, which cannot be used as a partition field. The
partition column is therefore cast to `datetime64[us]` before writing.

This is a lossless precision cast forced by a downstream engine limitation, not
a type opinion — Statcast's `game_date` values are date-only, so the discarded
digits are all zero. That is a different category from casting mixed object
columns, which remains deferred to staging. Naming the exception explicitly
matters, because "we don't editorialize in raw, except here" is exactly the kind
of carve-out that needs writing down.

`game_date` is also coerced with `pd.to_datetime` unconditionally rather than
behind a dtype check, because cached pybaseball responses can return it as a
string. A cast guarded by a dtype check only fires when the type is already
correct — which is precisely when it isn't needed.

### Idempotency: partitioned table + partition overwrite

The table is partitioned by day on `game_date`. Each load targets a single
partition via a decorator (`...$20260801`) combined with `WRITE_TRUNCATE`, so
the truncate applies to that partition alone. Re-running a date replaces one
day's rows and leaves every other day untouched.

**Why this over delete-then-load.** The strongest argument isn't elegance, it's
the failure window. Delete-then-load is two operations: DELETE the day, then
append it. If the process dies between them — network drop, killed terminal,
OOM — the day is gone and the table is silently short. Nothing errors, because
both operations individually either succeeded or never ran. Partition overwrite
has no such window: the load either fully replaces the partition or leaves it
exactly as it was. There is no intermediate state a crash can strand you in.

**Why this over append-and-never-rerun.** The problem isn't that duplicates
appear — it's that they are undetectable afterward. Statcast ships no clean
pitch-level primary key, so de-duplicating would mean inventing a composite one,
which is more work than partitioning required in the first place. For a framing
model, silently double-counted pitches corrupt the output without ever looking
wrong.

**A property this buys for free.** A decorator load rejects any row whose
partition value falls outside the targeted partition. The path claims
`dt=2026-08-01`; if that file contained August 2nd pitches, the job fails rather
than misfiling them. The filename's claim about its contents is enforced by the
engine rather than trusted.

**What it cost.** This design is why the timestamp-precision and string-date
bugs above surfaced at all. Delete-then-load would have accepted both — the
nanosecond column would have loaded as `INT64` and the string dates as `STRING`
— and the problem would have appeared much later, in a dbt model or in a framing
number that looked only slightly off. The strictness is the feature.

**Considered and deferred:** `require_partition_filter` would force every query
against this table to filter on `game_date`, preventing accidental full-table
scans and the bill that follows. Left off for now; worth revisiting once dbt
models are querying this table regularly.

**Partition Pruning:** A query filtering WHERE game_date = TIMESTAMP('2026-08-01') scans only that day's partition — roughly 1/7th of the current table and a fixed cost as the table grows — instead of the full table, which is what makes require_partition_filter worth revisiting: it converts a forgotten filter from a silent full scan into an error.

### GCS overwrite semantics

Re-running a date overwrites the existing object at the same path, and with
versioning off the previous copy is gone. For this pipeline that is a feature:
a partial or corrupt day is fixed by running the day again, with no cleanup
step. Storage-layer idempotency and warehouse-layer idempotency are separate
mechanisms — deterministic paths handle the former, partition overwrite the
latter.

### Why Parquet

A single day of Statcast data is 2.5 MB as CSV and 798 KB as Parquet, a 3.2x
reduction. The columnar layout compresses well because most of the 119 columns
are repetitive floats and low-cardinality strings.

3.2x is modest for Parquet, and that's a fact about this data rather than a
disappointment: the high-cardinality float columns — pitch coordinates, spin
rate, release point, all carried to several decimals — don't dictionary- or
run-length-encode well.

The size ratio is the easy half of the argument. The half that matters more once
the data sits in GCS is that a query touching four columns reads four columns,
not all 119 — a bandwidth and cost argument rather than a storage one.

### CLI: positional date argument

The date is the script's sole required input, so there is nothing to
disambiguate it from, and a required flag is a contradiction in terms — argparse
calls them "optional arguments" for a reason.

If a second date is ever added for range support, this should switch to
`--start` / `--end`: with two positionals, `2026-08-01 2026-08-07` requires the
reader to remember which position is which, and a reversed range returns an
empty DataFrame rather than an error — a silent failure that format validation
cannot catch.

### Empty-day behavior

Off-days, the All-Star break, and full rainouts raised a KeyError() on game_date
so no parquet is written.
---

## Repository hygiene

`.gitignore` covers `output/`, `*.parquet`, `*.csv`, `venv/`, and
`__pycache__/`. Data belongs in GCS, never in the repo — `output/` covers the
location and `*.parquet` covers the mistake of writing somewhere else.

## Schema, Datasets, Tables, SQL - Uppercase

Personal preference. Everything will be uppercase.

## Exploratory Findings

Not all takes are good data points for the model. Some are clearly strikes (right down the middle), and some are clearly balls (in the other batter's box). We will be filtering to pitches around the strike zone edges for the model. 

I don't understand mathematically how batter height creates the strike zone.