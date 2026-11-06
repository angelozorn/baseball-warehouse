# baseball-warehouse

Pitch-level Statcast ingestion into BigQuery, transformed with dbt into
modeling-ready fact tables of taken pitches, on a daily schedule, with a
Tableau Public dashboard and a catcher-framing model on top.

`ingest_statcast.py` pulls a single day of Statcast data from Baseball Savant,
writes it to a partitioned local Parquet file, verifies the write round-trips,
uploads it to GCS, and loads it into BigQuery — replacing that day's partition
atomically so re-runs are safe. dbt then cleans that raw table into a staging
view, filters it into `FCT_TAKES`, and attaches a location-based
expected-strike baseline in `FCT_TAKES_EXPECTED`. A launchd agent runs the
whole chain every morning. `analysis/evaluate_models.py` is the scoreboard every
model is graded by, `analysis/model_framing.py` holds the framing baseline and
the chance test, and `analysis/framing_runs.py` converts the result to runs and
validates it against Baseball Savant's published leaderboard.

The warehouse holds the full 2026 regular season: 184 game days, 716,792
events, 356,498 takes — plus postseason games as they are played, which staging
keeps and the marts exclude.

**The question, and the answer.** Can catcher framing be measured in public data,
and separated from the quality of the pitching staff? Yes to the first: across
54 qualified catchers the spread in strikes above expected is 33.58, against
13.70 from chance, p < 0.001. Partly to the second: staff command is a real
confound (−1,428 strikes per unit expected rate, t = −2.58) but explains only
12%, and the spread survives removing it. In runs, one standard deviation of
true catcher skill is **4.3 to 4.8 runs a season**, and the leaderboard
correlates with Baseball Savant's independently built metric at **Spearman
+0.94** across all 54 catchers.

**Project status: complete as scoped.** Layers 1 through 3c are built, tested
and documented. *Conclusions* states what is established and what is not;
*Where this stops* names the one design that would resolve the remainder.

---

## Architecture

```
Baseball Savant  ──(pybaseball)──▶  pandas DataFrame
                                          │
                                   conform to table schema
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
                                          │
                                       dbt source
                                          ▼
              DBT_DEV.STG_STATCAST_PITCHES          (view — all events)
                                          │
                                        ref()
                                          ▼
              DBT_DEV.FCT_TAKES                     (table — regular-season takes)
                        │          │             │
                        │        ref()         ref()
                        │          ▼             ▼
                        │   INT_EXPECTED_STRIKE_BY_BIN  ────▶  FCT_TAKES_EXPECTED
                        │   (league rate per location)         (takes + expected probability)
                        │                                               │
                        │                                         CSV extract
                        │                                               ▼
                        │                                      Tableau Public dashboard
                        ▼
              analysis/evaluate_models.py            (scoreboard — log loss, AUC, calibration)
                        │
                        │  imports the jury and the data pull
                        ▼
              analysis/model_framing.py              (smooth baseline, shrinkage, chance test)
                        │
                        │  imports the baseline and the attribution
                        ▼
              analysis/framing_runs.py               (runs conversion, Savant validation)
```

Every ingest layer derives its path from a single function, `partition_tail(date)`,
which returns `dt=<date>/statcast.parquet`. The local writer, the GCS uploader,
and the BigQuery loader all build on that one string. Path drift between stages
is a classic silent pipeline bug — one source of truth prevents it.

Inside dbt, `{{ ref() }}` plays the same role: each model names its upstream
rather than a hardcoded dataset, so dbt derives the build order from the SQL and
the whole DAG retargets by changing one line in `profiles.yml`.

The analysis layer follows the same rule in Python: each script imports the
previous layer's jury, baseline and attribution rather than reimplementing them,
so no two layers are ever compared across different evaluation code. All three
read `FCT_TAKES` rather than `FCT_TAKES_EXPECTED` — see *The harness reads the
un-baselined mart* under Layer 3.

**Region note:** the GCS bucket, both BigQuery datasets, and the dbt profile are
all `us-west1`. BigQuery cannot load from a bucket in a different location, and
datasets cannot be moved after creation, so this pairing is deliberate rather
than incidental.

---

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt          # curated top-level dependencies
# or, to reproduce an exact environment:
# pip install -r requirements-lock.txt
```

`requirements.txt` lists only what this project imports directly;
`requirements-lock.txt` is a full `pip freeze` snapshot for exact reproduction.
The curated file is the one to edit when a dependency is added — the lock is
regenerated from a working environment, never hand-written.

`db-dtypes` is in the curated file despite never being imported by name. The
BigQuery client requires it to materialize DATE columns into pandas, and without
it `to_dataframe()` raises at runtime — a dependency that is real but invisible
to a grep for imports, which is exactly the kind the curated file exists to hold.

Google Cloud authentication:

```bash
gcloud auth application-default login
gcloud config set project mlb-catcher-framing
```

Create the BigQuery raw dataset (the load job creates the *table*, but not the
dataset):

```bash
bq --location=us-west1 mk --dataset STATCAST_RAW_DATA
```

The GCS bucket `mlb-catcher-framing` must already exist in `us-west1` with
versioning off.

dbt connection config lives in `~/.dbt/profiles.yml`, not in this repo — it is
per-developer connection state, and the service-account variant of it holds a
keyfile path outright. Keeping it outside the repo means there is never a shape
where committing a secret is one careless edit away. The profile's `location`
must be `us-west1`; when it is wrong, the error reports a missing *dataset*
rather than a location mismatch, which sends you looking in the wrong place.

```bash
cd transform
dbt deps
dbt debug
```

**Location matters for the repo itself.** It must not live under `~/Desktop`,
`~/Documents` or `~/Downloads` — see *Nothing ambient* under Scheduling.

---

## Usage

### Ingest one day

```bash
python3 ingest_statcast.py 2026-08-01
```

The date argument is positional and validated against `YYYY-MM-DD` before any
network call, so a malformed date fails immediately rather than thirty seconds
later as an empty result.

### Backfill a range

`backfill.sh` walks a date range. Two properties matter at 187 days that did not
matter at seven:

- **Failures are logged, not fatal.** One transient network blip should not cost
  the remaining five months. Each failed date is appended to
  `backfill_failed.txt` and the loop continues.
- **It is resumable.** Completed dates are appended to `backfill_done.txt` and
  skipped on a later run, so an interrupt — Ctrl-C, a closed laptop, a crash —
  costs at most the day in flight. Partition overwrite makes redoing that day
  harmless.

```bash
caffeinate -i ./backfill.sh 2>&1 | tee backfill_run.log
```

`caffeinate -i` prevents the machine sleeping through the run (~90 minutes for a
full season). To retry a failed date, remove it from `backfill_failed.txt` and
re-run.

### Transform

```bash
cd transform
dbt build
```

### Extract for Tableau

Tableau Public cannot connect to BigQuery (see *Tableau Public constraints*),
so the dashboard is fed by a CSV extract:

```bash
bq extract --destination_format=CSV \
  'mlb-catcher-framing:DBT_DEV.FCT_TAKES_EXPECTED' \
  'gs://mlb-catcher-framing/exports/fct_takes_expected.csv'

gcloud storage cp 'gs://mlb-catcher-framing/exports/fct_takes_expected.csv' tableau/
```

Catcher names are not in the warehouse; the tracked mapping is
`analysis/catcher_names.csv` — see *Score models*.

### Score models

```bash
python3 analysis/evaluate_models.py                      # raw coordinates
python3 analysis/evaluate_models.py --features distance  # distance from zone centre

OMP_NUM_THREADS=1 python3 analysis/model_framing.py      # L3b
OMP_NUM_THREADS=1 python3 analysis/framing_runs.py       # L3c
```

Output is committed to `analysis/results_raw.txt`,
`analysis/results_distance.txt`, `analysis/results_framing.txt` and
`analysis/results_runs.txt`. Every figure in the Layer 3 sections is
re-derivable from those files.

**`OMP_NUM_THREADS=1` is not decoration.** `HistGradientBoostingClassifier`
builds its histograms in parallel, and floating-point summation order varies with
thread scheduling, so `random_state` does not make it deterministic. Two
unpinned runs on identical data produced test log losses of 0.11848 and 0.11857
and moved individual catchers by about a strike — enough to swap adjacent ranks,
and 7% of the whole leakage gap Layer 3 spent a section measuring. Pinned to one
thread the run is slower and byte-identical, verified by:

```bash
OMP_NUM_THREADS=1 python3 analysis/model_framing.py | diff - analysis/results_framing.txt
```

A committed results file that does not reproduce is not evidence, which is why
every L3b and L3c figure below is quoted to five decimals and the pin is part of
the command rather than a footnote.

`model_framing.py` writes `analysis/catcher_names.csv` on its first run, from one
`pybaseball` lookup, and reads that cache on every run afterward. The file is
committed, so a clone reproduces byte-identical output with no network call —
results that depend on whether a lookup service answered are not reproducible.
Hand edits survive, which is how `str.title()`'s "Mccann" became "McCann". That
file is also the dbt seed this README has been promising since Layer 2; moving it
to `transform/seeds/CATCHER_NAMES.csv` is the permanent fix.

`framing_runs.py` additionally needs `analysis/savant_framing_2026.csv`, the
published leaderboard downloaded from `baseballsavant.mlb.com/catcher_framing`.
Without it the script runs and skips section 4.

---

## Scheduling

A launchd agent (`com.angelozorn.statcast-daily`) runs `run_daily.sh` at 06:37
daily: ingest the previous day's games, then `dbt build`.

### Installing the agent

The agent definition is committed at
`scheduler/com.angelozorn.statcast-daily.plist` so the scheduler can be rebuilt
from a clone. Its paths are absolute — launchd does not expand `~` — so the
install rewrites them for wherever the repo actually lives:

```bash
mkdir -p logs
sed "s|/Users/angelozorn/baseball-warehouse|$PWD|g" scheduler/com.angelozorn.statcast-daily.plist \
  > ~/Library/LaunchAgents/com.angelozorn.statcast-daily.plist
launchctl bootout gui/$(id -u)/com.angelozorn.statcast-daily 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.angelozorn.statcast-daily.plist
launchctl kickstart -k gui/$(id -u)/com.angelozorn.statcast-daily   # fire now
launchctl list | grep statcast                                      # PID, last exit status
```

Three details that are each a failed install otherwise:

- **`mkdir -p logs` comes first.** launchd opens `StandardErrorPath` before the
  script runs, and `logs/` is gitignored — `run_daily.sh` creating the directory
  happens too late.
- **The `bootout` with `|| true`** makes the block idempotent. launchd refuses to
  bootstrap a label that is already loaded (`Bootstrap failed: 5: Input/output
  error`), and it ignores edits to a loaded job, so every reinstall needs an
  unload first. `|| true` keeps that harmless on a first install.
- **`plutil -lint` the result** if bootstrap fails for any other reason; a
  malformed plist produces the same unhelpful error as an already-loaded one.

### Why launchd rather than cron

The usual framing — "cron is old, launchd is the macOS way" — is weak. The real
argument is that **cron does not fire missed jobs**. A 6am cron job on a closed
laptop simply does not run, and the gap is discovered days later. launchd's
`StartCalendarInterval` fires the job when the machine next wakes. On a machine
that sleeps, that difference is the whole point.

### Two heartbeats, not one

- `last_run.txt` — the scheduler fired and the run completed
- `last_data_loaded.txt` — data actually arrived

They are separate because a single stamp would be refreshed daily by offseason
no-ops, so a pipeline that silently broke in February would still look healthy.
In the offseason the first advances daily while the second goes stale, which is
correct. In season, a widening gap between them is the alarm.

A failure writes neither. `set -Eeuo pipefail` means the stamps are unreachable
unless everything above them succeeded, so a heartbeat cannot lie by being
written on a partial run.

Both files are gitignored. They are runtime state written by the scheduler every
morning; tracking them would make the repository dirty daily, forever, with
churn that carries no information.

### Failure visibility

A scheduled job that fails silently is worse than no job, because it
manufactures confidence: you stop thinking about ingestion and the data quietly
stops arriving. Failures write a `FAILED` line to the day's log and raise a
macOS notification. Both paths were deliberately exercised before the agent was
loaded — a failure path that has never been watched fire is not a failure path.

### Nothing ambient

Two bugs surfaced only under the scheduler, and both had the same cause: the
script depended on something its environment supplied rather than something it
stated.

**The repository had to move off `~/Desktop`.** macOS TCC protects `~/Desktop`,
`~/Documents` and `~/Downloads`, and a launchd agent runs with less privilege
than a terminal, so the job failed with exit 126 (`Operation not permitted`)
while the identical command worked by hand. Granting Full Disk Access to
`/bin/bash` would have fixed it, but that is a broad grant — every script
anything runs through bash would gain it — to buy one scheduled job. Moving the
repo to `~/baseball-warehouse` avoids TCC entirely. The cost was recreating the
virtualenv, since a venv bakes its absolute path into `bin/activate`.

**The GCP project had to be named explicitly.** `bigquery.Client()` resolves the
project from ambient gcloud config, which launchd's minimal environment does not
reach: `Project was not passed and could not be determined from the
environment`. A `GCP_PROJECT` constant is now passed to every client. The
general rule this produced: a scheduled job should depend on nothing it does not
state.

Neither failure can reproduce by hand, which is why the agent was kickstarted
immediately rather than left to fire at 06:37.

---

## Verification

### Test suite

`dbt build` runs 27 data tests across 4 models, interleaved with the models so a
model whose tests fail is not used downstream. All pass except
`ASSERT_UNEXPLAINED_NULL_LOCATION_RATE`, which warns with 369 rows by design:
the tracking-failure rate is reported rather than suppressed.

### Season coverage

```sql
SELECT COUNT(DISTINCT game_date) AS days,
       MIN(game_date) AS first_day,
       MAX(game_date) AS last_day
FROM `mlb-catcher-framing.STATCAST_RAW_DATA.STATCAST_PITCH_DATA`;
```

| days | first_day | last_day |
|---|---|---|
| 184 | 2026-03-25 | 2026-09-27 |

184 game days across 187 calendar days — the three missing dates are the
All-Star break, which the ingest skips cleanly rather than failing on.

This check is worth running every time and not only once. It is what caught that
a `2025-06-01` test partition from early development was still sitting in the
table, contaminating every figure computed from it. The partition was dropped by
decorator (`bq rm -t '...$20250601'`), a metadata operation rather than a DELETE,
and every number below was re-measured afterward.

### Game type

```sql
SELECT game_type, COUNT(*) AS n, MIN(game_date), MAX(game_date)
FROM `mlb-catcher-framing.STATCAST_RAW_DATA.STATCAST_PITCH_DATA`
GROUP BY game_type ORDER BY n DESC;
```

pybaseball's query string hardcodes `hfGT=R|PO|S|` — regular season, postseason
**and spring training** — so the backfill window being regular-season-only was
luck rather than design. Verified `R`-only at 716,792 events before the mart
filter was added, and the filter now guarantees it going forward.

### Rows per day

```sql
SELECT game_date, COUNT(*) AS pitches
FROM `mlb-catcher-framing.STATCAST_RAW_DATA.STATCAST_PITCH_DATA`
GROUP BY game_date
ORDER BY game_date;
```

A spot-check across one week, showing the day-to-day range a full slate
produces:

| game_date | pitches |
|---|---|
| 2026-08-01 | 4438 |
| 2026-08-02 | 4321 |
| 2026-08-03 | 2356 |
| 2026-08-04 | 4167 |
| 2026-08-05 | 4376 |
| 2026-08-06 | 3530 |
| 2026-08-07 | 4355 |

Season average is ~3,900 events per game day. Low days are light schedules, not
missing data.

### Idempotency evidence

Re-running a single already-loaded date and re-running the query above returns
**identical counts**. This is the partition-overwrite design working: the load
replaces one day's partition rather than appending to the table.

### Object-level check

```bash
gcloud storage ls -l "gs://mlb-catcher-framing/raw/statcast/**"
```

One object per game day, sizes matching the local files. A write that returned
without error is not a verified write.

### Mart reconciliation

```sql
SELECT
  (SELECT COUNT(*) FROM `mlb-catcher-framing.DBT_DEV.FCT_TAKES`) AS TAKES,
  (SELECT COUNTIF(DESCRIPTION NOT IN ('automatic_ball','automatic_strike'))
   FROM `mlb-catcher-framing.DBT_DEV.STG_STATCAST_PITCHES`) AS THROWN_PITCHES;
```

Full 2026 regular season:

| | N |
|---|---|
| game days | 184 |
| events in raw | 716,792 |
| automatic rulings (no pitch thrown) | 2,309 |
| **thrown pitches** | **714,483** |
| takes by description (`ball` + `called_strike`) | 356,690 |
| dropped for null location | 192 |
| **rows in `FCT_TAKES`** | **356,498** |

Takes fraction: **49.92%** of thrown pitches by description, 49.90% after the
tracking filter. The seven-day August sample measured 50.03% — representative on
this measure, though not on two others (see *What a full season changed*).

The denominator is **thrown pitches**, not all events. Automatic ball and strike
rulings are excluded from both sides of the ratio, because with no pitch thrown
neither a take nor a swing was possible.

Note that this query is run against a live table: once postseason games land in
raw, `THROWN_PITCHES` grows while `TAKES` does not, because the marts are
restricted to `game_type = 'R'`.

### Expected-strike reconciliation

```sql
SELECT ROUND(AVG(EXPECTED_CS), 4) AS expected_rate,
       ROUND(AVG(CAST(IS_CALLED_STRIKE AS INT64)), 4) AS actual_rate,
       COUNT(*) AS rows
FROM `mlb-catcher-framing.DBT_DEV.FCT_TAKES_EXPECTED`;
```

Both rates must equal **0.3244** and rows must equal **356,498**. Every pitch's
expectation is its own location bin's mean, so the row-weighted average of
`EXPECTED_CS` equals the actual called-strike rate by construction. This is
asserted on every build by `ASSERT_EXPECTED_RECONCILES` — see *Where the
expected-strike baseline lives* for why that test exists.

---

## Decisions

### Raw-zone philosophy: faithful to source

The raw layer's job is to be re-derivable and un-opinionated. Column types are
not cast at ingest and unused columns are not dropped: if a type opinion turns
out to be wrong, fixing it in staging costs a dbt run, while fixing it in the
raw layer costs a full re-pull from Savant. Type opinions belong downstream,
where they can change without a backfill.

That the raw layer keeps everything paid off in L3c: `delta_run_exp` is not
carried through staging and was never expected to be needed, but it is sitting
in raw and is what the count-weighted run values are measured from.

One caveat worth stating plainly: the Parquet engine already imposes a schema
whether or not we ask it to, so "faithful" here means *we* add no opinions, not
that no opinions exist.

### Schema autodetect at load

Parquet files carry their own schema, so BigQuery derives column names and types
directly from the file. Hand-maintaining a 119-column schema would add drift
risk and maintenance work for no fidelity gain — and it would be the same kind
of type opinion the raw layer explicitly defers to staging. Autodetect keeps
that deferral consistent across both the file and the table.

**What autodetect costs.** Per-file type inference is not stable across days of
different sizes. pandas types a column with no values at all as `Int64`, so a
column that happens to be entirely null on one day arrives as INTEGER where the
table holds FLOAT, and the partition load is rejected. This surfaced on the
first playoff day — 598 events, with `arm_angle` empty throughout — after 184
full regular-season days never triggered it.

The fix is `conform_to_table_schema()`, which casts all-null columns to whatever
the table already holds and widens integers into FLOAT columns. Both conversions
are lossless; nothing else is coerced, because narrowing a populated column
would lose data and should fail loudly instead.

So autodetect buys freedom from maintaining 119 column types and charges a
conform step that partially reconstructs the same discipline. Still the better
trade — a hand-maintained schema would be worse to own — but it is a trade, not
a free win.

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

That property paid off directly during the season backfill, where a resumable
187-day loop re-runs any interrupted day without a cleanup step — and again on
every failed daily run, each of which could simply be retried.

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

**What it cost.** This design is why the timestamp-precision, string-date and
all-null-column bugs surfaced at all. Delete-then-load would have accepted them
— the nanosecond column as `INT64`, the string dates as `STRING`, the empty
column as INTEGER — and the problem would have appeared much later, in a dbt
model or in a framing number that looked only slightly off. The strictness is
the feature.

**Considered and deferred:** `require_partition_filter` would force every query
against this table to filter on `game_date`, preventing accidental full-table
scans and the bill that follows. Left off for now; worth revisiting now that dbt
models query this table daily.

### Partition pruning

A query filtering `WHERE game_date = TIMESTAMP('2026-08-01')` scans one
partition out of 184 instead of the full table, and that stays a fixed cost as
the table grows. Wrapping the partition column in a function defeats this, and
the broken form looks more correct than the working one:

```sql
-- prunes: reads one partition
WHERE game_date = TIMESTAMP('2026-08-01')

-- does not prune: reads the whole table
WHERE DATE(game_date) = '2026-08-01'
```

This is what makes `require_partition_filter` worth revisiting: it converts a
forgotten filter from a silent full scan into an error. `FCT_TAKES` and
`FCT_TAKES_EXPECTED` are both partitioned on `GAME_DATE`, so the same pruning
holds at the mart layer, where the staging view's `CAST` would otherwise leave
it to the optimizer.

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
cannot catch. The backfill sidesteps this by driving the existing single-date
interface from a shell loop rather than widening it.

### Exit codes as a contract

`ingest_statcast.py` exits `0` on success, `3` when no games were scheduled, and
`1` on anything else. The distinction exists so callers can tell "nothing to do"
from "something broke" — the daily run skips `dbt build` and stamps only
`last_run.txt`, while the backfill marks the date done rather than failed.

Worth noting as a coupling: changing an exit-code contract changes every caller.
Introducing `3` required editing `backfill.sh`, which had been treating any
non-zero code as failure and would have logged every off-day as an error.

### Empty-day behavior

Off-days, the All-Star break, and full rainouts return a `(0, 0)` DataFrame.
The script detects this before the single-date invariant check, prints
`no games, nothing to load`, and exits 3 — so a backfill loop continues past it
and the daily run no-ops.

This was not free. The original invariant guard asserted exactly one distinct
`game_date` in the pull, which raises on zero rows; combined with a `|| break`
loop, the season backfill would have halted at the All-Star break and finished
looking successful with five months missing. The guard was right and the empty
case simply hadn't been considered, which is the ordinary shape of this kind of
bug.

**The honest limitation:** a genuine off-day and a silently failed pull both
produce zero rows, and nothing in the data distinguishes them. The backfill loop
therefore logs every skip, and the expected skip count is known in advance — 3
for the 2026 regular season. A larger skipped set means something went wrong.

### Grain of the source: not one row per pitch

The raw table carries **one row per plate-appearance event**, not one row per
pitch. 2,309 of 716,792 events — 2,217 `automatic_ball` and 92
`automatic_strike` — are pitch-clock violations and intentional walks, where a
ball or strike is recorded but **no pitch was thrown**. Those rows have null
location, correctly: there is nothing to locate.

This was found by a failing `not_null` test on `PLATE_X`, `PLATE_Z`, `SZ_TOP`
and `SZ_BOT` in staging. The fix was not to loosen the test but to recognize it
was asserted in the wrong layer: staging's job is to represent the source
faithfully, and in the source those nulls are *correct*. Asserting otherwise
makes staging fail for telling the truth. The four location `not_null` tests
moved to `FCT_TAKES`, where the takes filter guarantees them and a violation
would mean something genuinely went wrong.

### Location nulls: a conclusion the season overturned

On seven days of August data, every null-location row was an automatic ruling,
and a singular test asserting *"location is null only for automatic rulings"*
passed. That conclusion was true of the sample and false of the season.

Across all 184 game days, **369 events have null location and are not automatic
rulings** — 0.052% of thrown pitches. These are genuine tracking failures, and
the August week happened to contain none.

They cluster rather than scatter: roughly 369 rows across ~20 games, about 18
per game. A full game is ~290 pitches, so these are partial in-game outages — a
tracking system dropping out for an inning or so — not dead games and not
diffuse per-pitch noise.

**Failure is uncorrelated with pitch outcome.** The description mix of the null
rows closely matches the season's overall mix:

| | share of all events | share of the 369 nulls |
|---|---|---|
| ball | 33.6% | 36.0% |
| foul | 18.0% | 17.9% |
| hit_into_play | 17.4% | 16.5% |
| called_strike | 16.1% | 16.0% |
| swinging_strike | 10.3% | 10.8% |

So dropping these rows does not preferentially remove called strikes or balls,
and will not bias the framing target.

**The limit of that evidence, stated plainly:** this shows independence from
*outcome*, not from *location* — and location is the missing field, so it is
unverifiable by construction. If outages clustered on pitches near the zone edge,
this data could not reveal it. At 0.05% of rows the exposure is small enough that
it is unlikely to matter, but the check is weaker than it looks.

**What the test asserts now.** The contract is not "never" but "rarely, and tell
me when that changes." `tests/ASSERT_UNEXPLAINED_NULL_LOCATION_RATE.sql` warns on
any occurrence and errors above 750 — roughly 2x the season baseline, a judgment
call rather than a derived threshold. It also covers all four location columns
rather than `PLATE_X` alone. The test was renamed because the old filename named
the claim the season disproved, and dbt uses filenames as test names in every run
log.

**That severity choice is now load-bearing.** Under `dbt build`, tests run
between the models that depend on them, so an error-severity failure on staging
would skip every downstream model — the marts would never be produced, every
day, forever. Choosing `warn` began as a judgment about test philosophy and has
become a pipeline dependency. Anyone raising it back to `error` months from now
would silently stop the warehouse.

### Takes filter: `ball` and `called_strike`, regular season, complete tracking

A take is a pitch the batter did not swing at **and** on which the umpire made a
judgment about location. That second clause does the work, because framing only
exists where an umpire had a call to make. Across the 2026 regular season:

| excluded | N | why |
|---|---|---|
| `blocked_ball` | 14,894 | a take, but the pitch bounced — the call was never in doubt and the catcher was blocking, not framing |
| `hit_by_pitch` | 2,119 | outcome determined by contact with the batter, not by an umpire judging location |
| `pitchout` | 39 | thrown deliberately wide; no framing attempt and no judgment call to influence |
| `swinging_pitchout` | 1 | a swing |
| `automatic_ball` / `automatic_strike` | 2,309 | **no pitch was thrown at all** — no location, no catcher receiving, nothing to frame |

The `blocked_ball` exclusion is the one with a real counterargument: it *is* a
take, the umpire *did* call it, and some framing implementations keep it. It is
excluded here as a decision about population, not an obvious truth — including it
would add ~15k rows of near-zero framing signal and skew the location
distribution toward the dirt.

**That decision is now measured rather than argued.** L3c's Savant comparison
shows their called-pitch count running a median 1.043x this project's take count,
and `blocked_ball` is 4.0% of takes-plus-blocked. The exclusion accounts for
essentially the whole population difference between the two leaderboards.

The automatic rulings deserve separate mention because the reason differs in
kind: an automatic strike on a pitch-clock violation is a strike the catcher had
no part in, and including it would credit or debit a catcher for an umpire's
clock.

**Two further filters**, both declared in the model's own description rather than
implied:

- **`game_type = 'R'`.** Postseason umpire crews are selected rather than
  rotated, leverage differs, and the sample is small; published framing work is
  conventionally regular-season-only. Spring training is worse still —
  minor-leaguers, experimental rules, and parks without full tracking. The
  filter was added while it was still a no-op, which is the only moment it could
  be verified as one; it did real work the next day, excluding all 598 events
  from the first playoff game.
- **Non-null location**, removing 192 takes (see *Location nulls*). A pitch with
  no recorded location cannot be used by a framing model regardless, so the only
  real question was whether the mart declares the exclusion or hides it.

All of these live in the marts, not staging, for the same reason the location
assertions do: staging keeps every row the source has, including broken ones,
because its job is faithful representation. The mart is the layer entitled to say
"these rows are unusable for my purpose."

### Why staging is a view and the marts are tables

**Staging is a view.** It is a thin projection over raw — a cast, some renames,
a column selection, no aggregation and no filter. Materializing it would
duplicate the full raw dataset in storage and add a refresh step, in exchange
for savings on a query that is nearly free to recompute. A view is also always
consistent with whatever raw currently holds, which is the right default for a
layer whose job is faithful representation.

**The marts are tables.** Three reasons, in order of weight:

1. **The work runs once.** The filters, the normalized-location derivation and
   the expected-strike join execute at build time instead of on every downstream
   read. A framing model will scan these tables repeatedly.
2. **Consistency across consumers.** A materialized table means a training run,
   a dashboard extract and an ad-hoc query see byte-identical derived values. A
   view recomputes, so if raw or staging changed between two reads, two
   consumers could silently disagree about the same "pitch."
3. **Physical layout.** Both marts are partitioned on `GAME_DATE` and clustered
   on `CATCHER_ID` — neither is possible for a view, and both matter for a
   workload whose every query groups by catcher and filters by date.

The trade-off is that the marts are stale until the next `dbt build`. With a
daily scheduled run, staleness is bounded by the ingest cadence anyway, so a
view would buy no real freshness.

### Mart derivations

**`PLATE_Z_NORM`** = `(PLATE_Z - SZ_BOT) / (SZ_TOP - SZ_BOT)`, computed with
`SAFE_DIVIDE`. The strike zone's vertical bounds are set per batter from their
stance, so raw `PLATE_Z` is not comparable across hitters — a pitch at 2.5 feet
is a strike to one batter and a ball to another. Normalizing puts every pitch on
a scale where 0.0 is the bottom of that batter's zone and 1.0 is the top, and
values outside `[0, 1]` are outside the zone. Raw `PLATE_Z`, `SZ_TOP` and
`SZ_BOT` are kept alongside so the normalization can be audited or redone.

`SAFE_DIVIDE` rather than `/` because BigQuery *errors* on division by zero
rather than returning null, so a degenerate zone (`SZ_TOP = SZ_BOT`) would kill
the build with an arithmetic error instead of naming the problem. `SAFE_DIVIDE`
returns null, and the `not_null` test on `PLATE_Z_NORM` then identifies the
column. That test passes across all 356,498 rows — there are no degenerate
strike zones in a full season of data.

**`IS_CALLED_STRIKE`** is `BOOL`, not `0`/`1`. That is the honest type for "the
umpire called it a strike," and casting to an integer at training time is
trivial. The warehouse should not pre-flatten a boolean into a model's preferred
encoding.

**Horizontal location is not normalized.** The plate is a fixed width, so
`PLATE_X` is already comparable across batters and needs no per-batter scaling.
Handedness is carried (`BATTER_STANDS`, `PITCHER_THROWS`) so a model can learn
the inside/outside asymmetry itself rather than having it baked in here. Layer 3b
does exactly that, and both handedness flags earn their place in the model.

### The expected-strike baseline

`INT_EXPECTED_STRIKE_BY_BIN` holds the league called-strike rate for each
location bin; `FCT_TAKES_EXPECTED` attaches that rate to every take as
`EXPECTED_CS`, plus `CS_ABOVE_EXPECTED` for the per-pitch difference.

The league rate within a bin **is** an empirical expected-strike probability. A
pitch down the middle carries an expectation near 0.98 and one six inches off
the plate near 0.05, so meeting expectation earns a catcher no credit and
pitcher command largely cancels out. That is the entire mechanism.

Three implementation choices worth stating:

**Bin size and the zone constants are dbt vars** (`expected_bin_size`,
`zone_bot`, `zone_top` in `dbt_project.yml`). They appear in two models, and
during development the bin size changed four times. One edit beats a hunt. The
evaluation harness duplicates all three as Python constants, because it has to
rebuild the bins from `FCT_TAKES` on the training split alone — so a change to
the vars now means a change in two places, and the harness says so in a comment
at the top of the file.

**Bins are stored as integer indices, not float coordinates.** The join between
the two models is on `X_BIN_IDX` / `Z_BIN_IDX` rather than on
`ROUND(x/0.25)*0.25`. Joining floats is only safe when the bin size is exactly
representable in binary — 0.25 is, 0.1 is not — so an index join is correct at
any bin size, and changing the var cannot silently break the join.

**The zone constants come from the data**: `AVG(SZ_BOT) = 1.62` and
`AVG(SZ_TOP) = 3.21`, giving a tracked zone 1.59 ft tall against the plate's
1.42 ft wide. That ratio of 1.12 means a correctly-proportioned strike zone is
only slightly taller than wide — noticeably less elongated than textbook
diagrams suggest, and a real property of Statcast's per-pitch stance estimates.

### Where the expected-strike baseline lives — and why

The baseline was first built as a Tableau `FIXED` LOD. It produced wrong numbers,
and the way it was wrong is worth recording.

Tableau aggregated the LOD at **bin grain rather than row grain**, so each of the
~1,028 location bins counted once regardless of how many pitches fell in it. Most
bins sit far outside the zone with near-zero called-strike rates, and almost no
pitches land there — so the unweighted mean came out at **0.049** against a true
league rate of **0.324**. Downstream, `SUM(expected)` contributed 50 instead of
115,700, and "strikes above expected" silently became the raw strike count.

Nothing errored. The chart rendered. The numbers were simply wrong by a factor
of six, and the only reason it was caught was that a scatter plot's axis ran
0.10–0.14 where 0.32 was expected.

The baseline now lives in dbt as a row-level column, computed once, and
`ASSERT_EXPECTED_RECONCILES` encodes the invariant that caught it:

```sql
HAVING ABS(AVG(CAST(IS_CALLED_STRIKE AS INT64)) - AVG(EXPECTED_CS)) > 0.0001
```

Every pitch's expectation is its own bin's mean, so those two averages must be
equal. They differ only if the weighting is wrong — which is exactly the failure
mode above.

**The general rule this produced:** transformation logic belongs in the
warehouse, where it is versioned, tested, and identical for every consumer. The
BI layer should do arithmetic on columns, not define them. A BI tool's
aggregation semantics are subtle enough that a wrong answer looks like a right
one, and nothing in the tool will tell you.

### Why location-controlled still isn't framing

Raw called-strike rate ranks William Contreras first among 54 qualified catchers
and Jonah Heim last. Both results were suspicious for the same reason: Contreras
catches Milwaukee's staff and Heim catches Oakland's.

Controlling for location confirms it, in both directions:

| | raw rate | raw rank | expected rate | strikes above expected |
|---|---|---|---|---|
| William Contreras | .3428 | **1st of 54** | **.3456** (highest) | −24.5 |
| Patrick Bailey | .3306 | 14th | .3270 (average) | +24.4 |
| Jonah Heim | .3042 | **last** | **.3044** (lowest) | −0.9 |

The `expected_rate` column is effectively a pitching-staff command index, and it
ranks Milwaukee first and Oakland last out of all 54. Contreras's pitchers put
the ball in the best locations in baseball; Heim's in the worst. Once that is
accounted for, both land at average.

**Significance, and a formula that was wrong.** The bound originally quoted here
used `sqrt(n × 0.32 × 0.68)` — the standard deviation of a catcher's strike total
if every take were an independent coin flip at the league rate. Takes are not
coin flips. A pitch down the middle is called a strike about 99.98% of the time
and a pitch two feet outside essentially never, and neither contributes anything
to the variance of a catcher's total.

The correct variance is `Σ p_i(1−p_i)` over the catcher's own pitches, where each
`p_i` is that pitch's expected strike probability. On this data the mean of
`p(1−p)` is about 0.038 against the pooled 0.219, so **the original bound
overstated the noise by roughly a factor of 2.4** and every standard-error and
standard-deviation figure derived from it was too large by that factor.

Layer 3b computes the bound exactly, per catcher, and tests it against a
permutation null that assumes nothing. See *The chance test* below.

**Measured against chance — and this conclusion was reversed in Layer 3b.** The
claim made here was that the observed spread of strikes above expected (33.5
across 54 qualified catchers) matched the spread expected from chance alone
(33.9), so no true between-catcher difference was detectable.

**The observed figure was right and the chance figure was wrong.** It came from
`SQRT(AVG(takes) × 0.2176)`, the inflated formula described above. A permutation
test — shuffling catcher labels within strata of pitch difficulty, which assumes
no variance formula at all — puts the chance spread at **13.70**, not 33.9.
Against an observed 33.58 that is a rejection at p < 0.001, and the permuted null
never exceeded 16.51 in a thousand draws.

So catchers do differ by more than luck, and Layer 3b goes on to show that staff
command explains only 12% of it. *The chance test* and *Partialling out the
pitching staff* carry the full result.

**Why the error survived as long as it did.** Two numbers agreed to within a
percent and the agreement looked like confirmation. Nothing about 33.5 ≈ 33.9
invited a second look at where 33.9 came from — a near-match is the most
comfortable kind of wrong answer, and the only thing that caught it was replacing
the formula with a procedure that could not inherit its assumption.

**Location control is still not sufficient.** If umpires give pitchers with
established command more benefit of the doubt on borderline pitches — a
reputation effect belonging to the pitcher — the extra strikes still accrue to
whoever is catching. The expected-strike baseline cannot see this, because it
conditions only on where the pitch crossed, not on who threw it.

There is also published work on sequential effects in umpire calls (Chen,
Moskowitz & Shue, on the gambler's fallacy) reporting that umpires are *less*
likely to call a strike immediately after calling one — negative
autocorrelation, the opposite of a "rhythm" effect. Whether that holds in 2026
is testable from this data by adding the previous pitch's call as a feature, and
is on the list.

**Consequence for Layer 3:** the model needs **pitcher effects alongside catcher
effects**, so the two can be separated rather than assumed apart. A model
conditioning on location alone would reproduce the same confound in smoother
form. Layer 3b revises this: conditioning on the pitcher is the weaker answer,
because catcher and staff are nearly the same variable. See *Where this stops*.

### Bins are not a model

The expected-strike baseline is an empirical average within 0.25-ft bins. Three
limitations follow, and all three are arguments for the next phase rather than
fixable by finer binning:

**Per-cell noise.** At roughly 50 pitches per cell for a single catcher, the
standard error on a cell is about 7 percentage points. Individual cells in the
per-catcher heatmaps are not interpretable; only the broad pattern is.

**Attenuation.** A 0.25-ft bin straddling the zone edge contains pitches whose
true strike probability ranges from roughly 0.2 to 0.8. Averaging across that
gradient discards exactly the margin framing operates in. Layer 3b measures the
cost and localizes it: a smooth model recovers 0.02913 of log loss over these
bins, and **93% of that comes from the 28% of pitches whose call was genuinely in
doubt.** The attenuation is not a diffuse inefficiency — it is concentrated
precisely at and just outside the called-zone edge.

The **"+0.63 standard deviations" figure originally quoted here for Patrick
Bailey is withdrawn.** It divided his strikes above expected by the inflated
standard error described under *Significance, and a formula that was wrong*, so
it understated him by roughly a factor of 2.4. His corrected position is in
*The external check* under Layer 3b.

**No other covariates.** Count, handedness and pitch type all shift the called
zone and are not conditioned on. L3c quantifies how much the count matters: the
run value of a flip ranges 7.8x across the twelve count states, so a baseline
blind to the count is blind to most of the leverage.

Bailey is the most valuable row in the table precisely because there is an
outside opinion to check against. The method points the right direction and
cannot resolve the magnitude — a specific, diagnosable shortfall rather than an
unexplained disagreement.

Layer 3 replaces bins with a smooth model — logistic regression or a GAM over
location, handedness, count and pitcher — which pools information across
neighbouring locations instead of treating each cell as an independent sample.
That is exactly what thin per-catcher data needs. The attenuation argument above
was a prediction when it was written; Layer 3's distance model turns it into a
measurement, and Layer 3b's gradient-boosted baseline cashes it in.

### Naming convention: uppercase identifiers

Datasets, tables, dbt models, and columns in the dbt layer are uppercase, with
SQL keywords uppercase as well. This is a personal preference carried over from
Snowflake habits, applied consistently rather than drifted into.

Two BigQuery-specific facts make this a real commitment rather than cosmetics:
dataset and table names are **case-sensitive** — `RAW.STATCAST` and
`raw.statcast` are different objects, and a lowercase reference errors as
not-found — while **column names are case-insensitive**, which is why the
staging model can select `GAME_DATE` from a raw table whose column is
`game_date`. The dbt project name itself stays lowercase because dbt requires
it; it is not a BigQuery object.

Raw-layer column names stay as Savant delivers them, lowercase, because renaming
them in the raw zone would be exactly the kind of editorializing that layer is
supposed to avoid. Uppercase begins at staging, which is the layer entitled to
hold naming opinions.

Python in `analysis/` follows Python conventions, not this one. A convention
about warehouse identifiers is not a convention about every file in the repo, and
pretending otherwise would mean `SNAKE_CASE` function names.

### What a full season changed about a one-week sample

Both failures the season backfill produced were one-week assumptions caught by
tests written against that week:

1. **`accepted_values` on `DESCRIPTION`** failed with one row:
   `swinging_pitchout`, which occurs once in 184 days. Three values now sit at
   ≤39 occurrences across the season, so the test moved to `severity: warn` — a
   novel description should surface in the run log without breaking the build.
2. **The location contract** was disproved outright, as described above.

Neither test was loosened to make it pass. The first changed severity because
asserting a closed vocabulary on a long-tailed categorical is the wrong shape of
test, not because the assertion was inconvenient. The second was rewritten to
express the contract that is actually true.

The general lesson, worth stating because it kept recurring: a test written
against a sample encodes that sample's accidents as if they were rules. The
tests were still worth writing — they are what *caught* both errors — but a
passing test on seven days is evidence about seven days. The all-null-column
schema drift was the same lesson again, from a different direction: 184 days of
full slates had never produced an empty column, and the first small day did.

---

## Layer 2 — Tableau Public dashboard

### Tableau Public constraints

Tableau Desktop Public Edition cannot connect to enterprise databases, BigQuery
included. It reads files and a handful of cloud sources. The dashboard is
therefore fed by a **CSV extract of `FCT_TAKES_EXPECTED`**, not a live
connection — so the published dashboard is a snapshot and the daily pipeline
does not refresh it.

The one path to a live Tableau Public dashboard is a Google Sheets source, which
it can refresh daily. That would require small pre-aggregated marts rather than
the 356k-row extract, since the full table approaches Sheets' cell ceiling. Not
built; noted as the option if it becomes worth it.

Everything published to Tableau Public is public. That is fine for public MLB
data and would not be for anything else.

**The workbook is built on the binned baseline, which matters for its text.** The
Layer 3b corrections to `docs/dashboard_copy.md` fix the chance-spread arithmetic
without quoting Layer 3b's model-specific figures on bin-based charts, because
that would be a new version of the error it is correcting.

### The views

**1 — Called-strike rate by location.** The empirical zone, binned at 0.1 ft,
with the rulebook rectangle drawn on it. The called zone extends to roughly
±0.85 ft horizontally where the plate edge is at ±0.708 — a difference of about
a baseball's radius (0.12 ft), because a pitch is a strike if *any part of the
ball* crosses the zone. Umpires are applying the rule correctly; the naive
rectangle was the wrong rectangle.

**2 — Called-strike rate by catcher.** Dot plot rather than bars: rates cluster
around 0.32 across a 3.8-point range, and bars from zero would render every
catcher identical. Dots sized by sample, minimum 3,000 takes, with a league
average line. Labelled "called strike rate on takes," not "framing."

**3 — Zone by count leverage.** The called zone under hitter's counts, even
counts and pitcher's counts. Specific counts were abandoned for grouped ones
because 3-0 holds only 6,999 takes — ~17 per bin, which the per-bin threshold
erases. The conventional groupings (3-2 counts as even, not a hitter's count)
hold 82,536 / 161,855 / 112,107 and survive the same bin settings.

**4 — Framing by catcher, location-controlled.** Called strikes above expected
by location, for Contreras, Bailey and Heim. Captioned with the per-cell noise
limitation above.

**5 — Staff vs skill.** Scatter of expected rate (the hand dealt) against
strikes above expected (what they did with it), one dot per qualified catcher.
A trend line on this chart is a test rather than decoration: the reputation
effect described above predicts a positive slope, so a flat line is evidence
against it. The measured slope is **−1,328 with a standard error of 506**
(t = −2.62, p = 0.011, R² = 0.11) — significantly *negative*, the opposite of
the reputation hypothesis. Catchers with better-locating staffs post lower
strikes above expected, which means the location control is incomplete and the
metric is still partly measuring pitcher behaviour.

Layer 3b reproduces this independently: weighted least squares on the
gradient-boosted baseline gives **−1,428** strikes per unit expected rate
(t = −2.58) and the same 12% of variance explained. Two methods, two baselines,
the same answer.

**6 — Methods & Findings.** A text worksheet, not a dashboard. Published
verbatim text lives in `docs/dashboard_copy.md`, versioned alongside the
pipeline that produced the figures in it, because none of those figures are
Tableau fields — they are typed by hand and would go stale silently on a
refresh. One of them did worse than go stale: see *The chance test*.

### Design notes that cost time

**Normalizing vertical location destroys the aspect ratio.** `PLATE_X` is in
feet and `PLATE_Z_NORM` is unitless, so a zone 1.42 ft wide and 1.0 units tall
renders wider than tall. Rescaling the normalized height back onto a
league-average zone (1.62–3.21 ft) puts both axes in feet; equal 4-ft spans and
a square plot area then give true proportions. Tableau has no aspect-ratio lock,
so the plot must be squared by sizing a fixed container on the dashboard.

**Continuous versus discrete axes is a real trade.** Continuous bin pills give
numeric axes that support reference lines, at the cost of marks that only tile
when pixels-per-foot match in both directions. Discrete pills guarantee tiling
at any size but lose reference lines. View 1 uses continuous because the
rulebook rectangle carries its entire argument; the per-catcher maps use
discrete because a clean tiled surface matters more there.

**Reference lines are unavailable on Tableau bin objects.** The rulebook
rectangle therefore comes from `ROUND(x / size) * size` calculated fields set to
continuous dimensions, rather than from Tableau's own binning. Same bin edges,
but a pill the reference-line dialog will accept.

**Aggregation on a scatter axis changes what the chart is about.** The staff-vs-
skill plot first used `SUM(EXPECTED_CS)` on the X axis, which ranges 0–3,000 and
measures *playing time*. `AVG(EXPECTED_CS)` is what measures staff quality. The
chart rendered identically well either way, and only one of them answered the
question.

**Publish a fixed-size dashboard, not a worksheet.** A worksheet reflows to the
viewer's browser window, which is wide, and the zone squashes again. A
fixed-size dashboard scales proportionally and holds its aspect on any screen.

**Hidden worksheets do not publish.** Sheets hidden while building a dashboard
stay hidden on Tableau Public, which is how a workbook ends up published with
only its text tab visible. Unhide every sheet and check *Show sheets as tabs*
before publishing, then verify in a private browser window — the author's own
session shows sheets a visitor cannot see.

**Text objects clip rather than scroll**, and web authoring on Tableau Public is
unreliable for editing them — a greyed-out OK button means the dialog never
registered the change. Edit in Desktop and republish under the same workbook
name, which overwrites in place and keeps the URL.

### ABS challenge data: looked at and rejected

The automated ball-strike challenge system is live in 2026 — 10,557 challenges
at a 54% overturn rate — and an umpire-independent ruling on borderline pitches
would be close to ground truth for framing. It is not usable from this export.

Statcast carries no challenge column. The only trace is in the plate-appearance
description text, which exists solely for the **final pitch of an at-bat**. That
truncation is not merely a sample-size problem: on a terminal pitch, a confirmed
strike-three call and an overturned ball-four call enter the data under opposite
conditions, so availability is correlated with the outcome being measured. A
sample selected on its own dependent variable is worse than no sample.

Recorded here rather than dropped, because "we considered the obvious better
instrument and here is the specific reason it fails" is worth more than silence
about it.

---

## Layer 3 — modeling

### The harness comes before any model

`analysis/evaluate_models.py` is the scoreboard. Nothing in this layer is
compared across different evaluation code: every model is scored by the same
function, on the same split, with the same three metrics.

**Three metrics, because one is not enough.** Log loss is the primary score — it
penalizes confident wrongness, which is what a probability model should fear
most. AUC measures ranking only, and ignores whether the numbers themselves mean
anything. The calibration curve catches the opposite failure: a model can rank
well and still emit probabilities nowhere near the observed rates. The raw
logistic regression below fails calibration while passing AUC, which is exactly
why all three are reported rather than one.

**The split holds out whole games, not random pitches.** `GroupShuffleSplit` on
`GAME_PK`, 80/20, `random_state=42`. Pitches in the same game share umpire,
catcher, park and tracking calibration. A random pitch-level split would put
those shared conditions on both sides and the model would be scored on context
it had already seen.

For a location-only model this costs almost nothing — location bins carry no
game-specific information — and that near-zero cost is itself worth recording.
The grouped split is insurance for the model *Why location-controlled still
isn't framing* says Layer 3 has to become: once features carry umpire or catcher
identity, group leakage becomes real, and the evaluation setup has to already be
the right one.

```
all      356,498 pitches, 2,429 games
train    285,076 pitches, 1,943 games
test      71,422 pitches,   486 games
base rate: train 0.3248, test 0.3226
```

### Scoreboard

| Model | Log loss | AUC | Max calibration error |
|---|---|---|---|
| Constant (train base rate 0.3248) | 0.62885 | 0.50000 | 0.0022 |
| Logistic regression, raw coordinates | 0.62793 | 0.54368 | 0.3575 |
| Logistic regression, distance from centre | 0.21745 | 0.96628 | 0.0567 |
| Binned lookup, fit on train, scored on held-out games | 0.14770 | 0.98450 | 0.0078 |

All scored on the same 71,422 held-out pitches. The constant baseline uses the
**train** base rate, not the season-wide 0.324 — a floor computed partly from the
test set would itself be leaking, and the floor is the one number that has to be
clean.

The headline: **location alone reaches AUC 0.985.** Where the ball crossed very
nearly determines the call, and whatever framing effect exists has to live inside
the 0.148 of log loss that remains.

### The leakage measurement, and why the obvious comparison fails

`FCT_TAKES_EXPECTED` fits its bin means on the whole season and then scores the
whole season, so every pitch contributes to its own prediction. The task was to
convert that into a number by comparing it against a model fit on train and
scored on held-out games.

That comparison gives the wrong sign:

```
honest (fit train, score test)   0.14770
leaky  (fit all,   score all)    0.14792
                        gap     -0.00022
```

Cheating appears to have made the model *worse*, which is impossible. The two
figures are computed on different row sets, and the difference between those row
sets is larger than the leakage. Scoring the identical rows both ways separates
them:

| Comparison | Difference | What it isolates |
|---|---|---|
| honest − leaky, both on test rows | **+0.00131** | leakage |
| leaky on all rows − leaky on test rows | +0.00154 | evaluation-set difficulty |
| honest − leaky as originally framed | −0.00022 | the two above, cancelling |

**The confound is larger than the signal.** The held-out games are 0.00154
easier than the season average, which swamps 0.00131 of real leakage and flips
the sign. Only the first row is a measurement of leakage; any comparison that
changes both the fitting set and the scoring set at once is uninterpretable, and
this one happens to be uninterpretable in the flattering direction.

**Prediction against measurement.** The standard result for in-sample optimism is
`k/N` nats per observation, which for 1,028 bins over 356,498 pitches predicts
0.0029 — written down before running, along with the expectation that it would
land in the third decimal and leave AUC essentially unmoved. Measured: 0.00131,
under 1% of the binned model's log loss, with AUC moving 0.0001.

Right order of magnitude, roughly half the predicted size, and the shortfall is
left unexplained deliberately. `k/N` assumes each bin mean is estimated where it
has room to vary, and a census shows most are nowhere near it:

| Minority outcomes in bin | Bins |
|---|---|
| 0 | 891 |
| 1–4 | 52 |
| 5 or more | 85 |
| **Total** | **1,028** |

A bin whose 300 pitches are all balls has nothing to overfit — the fit is 0 on
train, 0 on the season, and every held-out pitch in it is a ball, so both models
pay the same nothing. **891 of the 1,028 bins contribute exactly zero to the
gap.** Attributing the remaining 93.6 nats precisely would need per-row
decomposition. Not done: the figure is a diagnostic, it changes no framing
result and no modelling decision, and two separate attempts to explain the
factor of two were refuted by the bin census itself. Writing down the
measurement, the prediction and the size of the disagreement is the honest
stopping point.

### Feature shape, decided before fitting

Both feature sets and both predictions were written down before either
regression was run, because a prediction made after seeing the number is not a
prediction.

- **Raw** `PLATE_X` / `PLATE_Z_FT`. Expected to land near the intercept-only
  floor with AUC clearly above 0.5.
- **Distance**: `|PLATE_X|` and `|PLATE_Z_FT − 2.415|`, the midpoint of the
  measured 1.62–3.21 ft zone. Expected to close most of the gap to the binned
  model without beating it.

Both held.

**What a straight line gets wrong about a rectangle.** The raw model is
`logit(p) = b₀ + b₁x + b₂z`. Every probability contour is a parallel straight
line, so the model can only say "strikes get likelier as you move *this way*
across the plane." The zone is a bounded box with balls on all four sides.
Sorting pitches by a single linear score therefore walks from outside the zone,
through it, and out the far side:

```
     predicted      actual
        0.2941      0.0000
        0.3054      0.0168
        0.3112      0.3136
        0.3162      0.5589
        0.3211      0.5771   <- peak
        0.3261      0.5522
        0.3315      0.5400
        0.3376      0.4993
        0.3450      0.1677
        0.3581      0.0006
```

Predictions span six percentage points; observed rates span zero to 58%, and are
**non-monotone** — they rise and fall again while the model's score rises
throughout. The model's most confident strike decile is called a strike 0.06% of
the time. Maximum calibration error 0.3575. This is not an uninformative model,
it is a confidently inverted one, and it is the single clearest artefact this
layer produced.

It is also the argument for reporting three metrics. AUC 0.544 reads as "weak but
positive." The calibration table reads as "this model is wrong in a specific,
structural way." Only one of those descriptions is useful.

**Where the distance model still loses, and why it matters.** Two parameters
close 0.411 of the 0.481 log-loss gap between raw and binned — 85%, against
1,028 bin means. But two linear terms on absolute distances produce diamond
contours and a cone: highest at the exact centre, falling at a constant rate
outward. The real surface is a plateau with cliffs. The two worst-calibrated
deciles are precisely the transition band:

```
        0.1434      0.1841   +0.0407   just outside, under-predicted
        0.7208      0.6641   -0.0567   just inside,  over-predicted
```

Interior and far-exterior deciles are calibrated within 0.0014, and AUC 0.966
against the binned 0.985 says the *ranking* is nearly intact. The loss is almost
entirely bad probabilities at the edge.

**That is the binding constraint on everything after this.** Framing only exists
at the zone boundary — nobody frames a pitch down the middle or two feet
outside. A model that is 85% as good overall can be worthless for this question
if its error concentrates in the only band that matters. *Bins are not a model*
predicted attenuation at the edge from the bin width; this measures the same
failure in a smooth model that has no bins at all.

### Decisions

**The harness reads the un-baselined mart.** `FCT_TAKES`, not
`FCT_TAKES_EXPECTED` — the `EXPECTED_CS` column in the latter is fit on the whole
season, which is the leakage being measured. Bin indices are recomputed in Python
so they can be fit on the training split alone. The cost is three constants
duplicated from the dbt vars, noted under *The expected-strike baseline*.

**Unseen bins fall back to the training base rate.** 38 held-out pitches (0.05%)
landed in bins with no training pitches. The base rate is the honest choice,
because the model genuinely knows nothing about those locations. Their cost is
roughly 0.0002 of the mean log loss — about 15% of the measured leakage gap.
Small, but it is not leakage and should not be read as part of it.

**Known fragility: the clip.** Predictions are clipped to `1e-15` so log loss
stays finite. A training bin at exactly 0 that catches one held-out strike
therefore costs 34.5 nats — 0.0005 of the mean, over a third of the measured
gap, from one pitch. The honest run's lowest calibration decile shows zero
observed strikes across ~7,100 pitches, so this did not fire here, but with 891
single-outcome bins the exposure is real and the number is one unlucky pitch away
from meaning something different.

Laplace smoothing was considered and **rejected**. Roughly 40% of pitches sit in
bins with rates under 0.012, and smoothing an all-ball bin of 300 from 0.000 to
0.0033 charges that cost to every pitch in it — about 0.0012 overall, the same
size as the quantity being measured. A correction as large as the signal is not a
fix. Layer 3b's partial pooling is the technique that fixes this properly, by
shrinking toward a smooth function of location rather than toward a constant, and
its isotonic floor of `1e-6` replaces the inherited float64 limit with a bound
chosen for a reason.

**Script, not notebook.** The harness is run from the command line, its output is
committed as text, and its split is seeded. A notebook's cells can be run out of
order and its state cannot be reconstructed from what is saved, which is a poor
property for the file every later number is compared against.

### What Layer 3b has to do

Three constraints, each measured rather than assumed:

1. **Flexibility at the zone edge**, because that is where both the binned model
   and the distance model lose, and it is the only band framing lives in.
2. **Pitcher effects alongside catcher effects**, from *Why location-controlled
   still isn't framing* — the staff-quality slope of −1,328 (p = 0.011) says
   location alone does not separate them.
3. **A target worth measuring against.** Observed between-catcher spread 33.5
   against 33.9 from chance means the current method has no signal to improve on.

Point 3 is the one that did not survive contact with the data: the 33.9 was
wrong, and there was signal all along.

---

## Layer 3b — the smooth baseline, the shrunk catcher, and the chance test

`analysis/model_framing.py`, scored by the same jury as L3a on the same held-out
games. Results in `analysis/results_framing.txt`, reproducible byte-for-byte
under `OMP_NUM_THREADS=1` — see *Score models*.

### The smooth baseline, and where its gain actually comes from

| Model | Log loss | AUC | Max calibration error |
|---|---|---|---|
| Constant (train base rate 0.3248) | 0.62885 | 0.50000 | 0.0022 |
| Binned lookup, fit on train | 0.14770 | 0.98450 | 0.0078 |
| **Gradient boosting, 100 iterations** | **0.11857** | **0.99024** | 0.0091 |

A 20% reduction in log loss. `HistGradientBoostingClassifier` on `PLATE_X`,
`PLATE_Z_FT`, `BALLS`, `STRIKES` and both handedness flags, with `max_iter`
chosen by log loss on a grouped inner split of TRAIN (50: 0.12218, **100:
0.11988**, 200: 0.12010, 400: 0.12056).

**The gain is concentrated where framing lives, which was not obvious and was
initially argued the wrong way round.** Restricting the comparison to pitches
either location model calls uncertain:

| | pitches | binned | GBM | gap | share of total gain |
|---|---|---|---|---|---|
| borderline, predicted 0.05–0.95 | 20,080 (28.1%) | 0.48556 | 0.38880 | **0.09676** | **93%** |
| outside the band | 51,342 (71.9%) | 0.0155 | 0.0129 | 0.0027 | 7% |
| all test pitches | 71,422 | 0.14770 | 0.11857 | 0.02913 | 100% |

(The band rows are measured; the outside-band row is derived from them and the
aggregate.) The band's called-strike rate is **0.4574** against 0.3226 overall —
these really are the pitches in doubt. **28% of the pitches carry 93% of the
improvement.**

The first version of this section claimed the opposite — that almost all the gain
was on easy pitches and the aggregate table was "a contest about obvious balls."
The evidence offered was the binned model hedging at 0.0785 where the GBM says
0.0050. **But 0.0785 is above 0.05, so those pitches are inside the borderline
band.** A lookup table predicting 8% on pitches that go 0.7% is not a model doing
well on easy calls; it is the edge-attenuation failure itself. The band's own
contents were used as evidence that the band did not matter.

One qualification on the 93%: the band is the union of what either model calls
uncertain, and binned puts 25.9% of pitches in it against the GBM's 21.4%. So
roughly 4,800 of the 20,080 are pitches only the lookup table finds hard, where
the GBM wins by correctly calling them near-certain. That is still the
edge-attenuation failure — those pitches sit spatially just outside the called
zone — but it is not the framing margin proper.

**Why early stopping is switched off.** sklearn's `early_stopping='auto'` carves a
*random* validation slice out of the training rows, which puts pitches from the
same game on both sides of it — the exact leakage the grouped split exists to
prevent, applied inside the estimator where no reported split would show it.
`max_iter` is selected on a grouped split instead. `min_samples_leaf` is raised
from 20 to 200 because a 20-pitch leaf at the zone edge estimates a rate to about
±11 points, which is noise the model is supposed to pool away.

**It needs no recalibration, and the check says so rather than the eye.** Max
calibration gap 0.0077 on held-out games, with errors that are not monotone — low
deciles over-predict, 0.3503 under-predicts by 0.0077, 0.8748 is nearly exact. A
two-fold grouped isotonic fit inside the calibration split made held-out log loss
*worse*, 0.11989 to 0.12194. Isotonic is a step function, and in the region where
the GBM distinguishes 0.0001 from 0.0009 the resolution it destroys costs more
than the calibration it buys.

The decision rule was fixed in advance: adopt recalibration if and only if an
isotonic fit improves log loss on games its own fit never saw. The test set plays
no part in it.

**The number that mattered most** is the out-of-fold mean: expected 0.32442
against actual 0.32437, off by **0.00004** — three tenths of a strike on a
7,000-take catcher. Strikes above expected is a subtraction, so a baseline whose
mean misses the true rate shifts *every* catcher the same direction, and nothing
in a leaderboard reveals it.

### The chance test

Every catcher's expectation comes from a model fit on games that catcher's pitch
was not in — `GroupKFold(5)` on `GAME_PK` across all 356,498 takes, with the
fit/calibrate recipe repeated inside each fold. Without this, four fifths of the
expectations would be in-sample, each pulled toward its own outcome, which
shrinks strikes above expected toward zero and biases a chance test toward
finding nothing. That is the one direction a chance test must not be biased in.

**That protection is partial, and the bound belongs here.** Grouping by game
removes the pitch's own game, but a catcher appears in roughly 130 games and four
fifths of them sit in the training set of any given fold. His contribution to the
fitted surface survives at about 80%. The absorption is small — a catcher with
5,400 takes is 1.5% of the data, and with `min_samples_leaf=200` his share of a
typical leaf stays near that, so his effect is understated by something like 1.5%,
or half a strike on an SAE of 33. The complete fix is `GroupKFold` on
`CATCHER_ID`, fitting the surface on other catchers entirely. Not done; the
residual bias is conservative in direction and bounded by that estimate.

Catcher labels are then permuted **within 20 strata of the baseline's own
predicted probability**, 1,000 times.

**Why that stratum.** Permuting globally would scramble the fact that catchers
receive different mixes of easy and hard pitches, and that mix moves strikes above
expected with no skill involved — so a global null would be too narrow and the
test anti-conservative. Stratifying on raw location is not enough either, because
the baseline also conditions on count and handedness and the called zone shifts
with both. The predicted probability is the sufficient summary of everything the
baseline knows, so holding it fixed holds fixed exactly the difficulty each
catcher was dealt.

Permuting a label vector *within* a stratum preserves the multiset of labels in
it, so every permuted catcher keeps **exactly** their real number of takes and
**exactly** their real distribution across difficulty strata. Only the pairing to
outcomes is randomized.

```
spread of strikes above expected
  observed            33.58
  null mean           13.70
  null 2.5 / 97.5     11.03 / 16.51
  observed is 14.2 null SDs above the null mean
  p                   0.0010      (the floor at 1,000 permutations)

sum of z-squared
  observed           331.5
  null mean           53.5        (must be ~54 if the variance model is right)
  null 97.5           76.8
  p                   0.0010
```

The observed spread is 2.4x the null and more than twice the null's own 97.5th
percentile. Seven catchers clear |z| = 4, where the null expects 0.003 of them.

**This is where the 33.9 in *Why location-controlled still isn't framing* died.**
That figure assumed every take was a coin flip at the league rate; the permutation
null, which assumes no formula at all, puts chance at 13.70.

**Two free correctness checks came with it.** The `Σz²` null mean must equal the
number of qualified catchers if `Σp(1−p)` is the right per-pitch variance — it
came back 53.5 against 54, confirming the variance model that replaced the wrong
formula. And the calibration split scored 0.11989 against the test set's 0.11857,
making the test games 0.00132 easier than average; L3a measured the same effect at
0.00154 by an unrelated route.

### Partialling out the pitching staff

`EXP_RATE` — a catcher's mean expected called-strike rate — is a pitching-staff
command index. Regressing excess rate on it by weighted least squares, with two
passes of feasible GLS so the slope's standard error admits real between-catcher
variance rather than crediting all residual scatter to sampling noise:

```
slope  -0.2702 per unit expected rate   (SE 0.1047, t = -2.58)
on the strikes scale, at the mean take count:  -1,428
Layer 2 measured -1,328 (SE 506, p = 0.011) by OLS in Tableau

tau^2 unadjusted   4.227e-05   ->  sqrt = 0.00650   (0.65 points)
tau^2 adjusted     3.736e-05   ->  sqrt = 0.00611   (0.61 points)
staff quality explains 12% of the between-catcher variance

spread after partialling out staff quality
  observed  31.65     null mean 13.54     null 97.5 16.39     p 0.0010
```

**The confound is real and small.** The slope reproduces the Layer 2 chart by a
different method on a different baseline (−1,428 against −1,328), and the 12%
variance share matches that chart's R² = 0.11. So the staff effect is confirmed
twice and bounded: **88% of the between-catcher variance survives it, and the
residual spread still rejects chance at p = 0.001.**

A reading of the leaderboard's `EXP_RATE` columns initially suggested the confound
was far larger — the top ten average 0.3208 against the bottom ten's 0.3289. The
arithmetic resolves it: a 0.0081 gap times the slope times a median 5,400 takes is
about 12 strikes of a 96.5-strike top-to-bottom difference. Twelve percent, the
same answer from both directions. The 0.0081 looked impressive as a fraction of
`EXP_RATE`'s range and was never multiplied by the slope.

**Over-control, which is why 12% is an upper bound on the confound.** Staff
quality is not a clean instrument for staff quality. If good framers are
systematically assigned to worse staffs, or if framing itself depresses a staff's
apparent expected rate, this regression removes real catcher signal along with the
confound. So the adjusted spread of 0.61 points is a **lower** bound on catcher
effects and the unadjusted 0.65 an upper bound.

**One uncertainty this does not propagate.** `SHRINK_ADJ` uses only the sampling
variance of the residual, not the variance contributed by *estimating the slope*.
For a catcher near the mean `EXP_RATE` that is negligible. At the extremes it is
not: Contreras sits at 0.345, about +0.020 from the mean, so the slope's SE of
0.1047 contributes roughly ±18 strikes to his adjustment on top of his own ±17.9.
His adjusted +27.7 is therefore barely one standard error from zero, and his move
from 40th to 14th should be read as "the adjustment is large and uncertain for
him," not as a ranking. A within-team contrast needs no slope and so has no such
term.

### Partial pooling, with the strength measured rather than chosen

The model is `d_c = θ_c + noise`, where `d_c` is the catcher's excess
called-strike rate, the noise variance `v_c` is known exactly from the baseline's
own probabilities, and `θ_c ~ N(0, τ²)`. The prior mean is zero because the
baseline *is* the expectation — a catcher with no effect has `d_c = 0` by
construction. Method of moments gives `τ² = max(0, Var(d) − mean(v))`, and the
posterior mean is `τ²/(τ² + v_c) · d_c`.

**The shrinkage strength is not a knob.** It is the ratio of real between-catcher
variance to within-catcher noise, both estimated. The equivalent Beta-Binomial
strength is **κ = 5,185 takes** — the point at which a catcher's own record
carries half the weight — against a median qualified catcher's 5,263. So
shrinkage here is mild: factors run 0.772 to 0.913, nobody loses more than 23% of
their estimate, and the raw ordering largely survives.

The floor at zero is load-bearing rather than defensive. Had the observed spread
failed to exceed the noise, `τ²` would be zero, every posterior mean exactly zero,
and the honest leaderboard a flat column. Given this README's previous conclusion,
that was the expected outcome when the code was written.

**Quote `sqrt(τ²)` for the spread, not a column's standard deviation.** Raw SAE
has sd 33.58, shrunk 28.68, adjusted 26.86 — and none of the three estimates the
true spread, because posterior means under-disperse by construction. Mechanically,
`Var(θ̂) = w̄ · τ² < τ²`: the construction multiplies every estimate by a factor
below one, so a narrower collection is guaranteed by arithmetic rather than
produced by this sample. The raw column overstates by the noise and the shrunk
column understates by the shrinkage; τ² is the quantity built to be neither.

### The external check

```
 RANK_RAW  RANK_ADJ              NAME    N  EXP_RATE     SAE      Z  SAE_RESID  SAE_ADJ
       11         7    Patrick Bailey 6873     0.326  33.427  2.142     36.313   31.908
       40        14 William Contreras 8798     0.345 -22.736 -1.273     27.745   24.989
       29        43        Jonah Heim 5404     0.304   1.720  0.124    -27.844  -23.697
```

*Bins are not a model* offered Patrick Bailey at +0.63 standard deviations,
against his reputation as the consensus best framer in baseball, as evidence the
binned method was too coarse. That figure used the inflated standard error, so it
understated him by roughly a factor of 2.4.

**Bailey is 11th of 54 raw and 7th adjusted — clearly positive on both, at
z = 2.14.** His `EXP_RATE` of 0.326 sits near the mean, so the staff adjustment
barely moves him (33.4 to 36.3) and his improvement in rank comes mostly from
others moving. That makes his the robust row on this table, and it is the check
passing: the method the broken standard error could not distinguish from average
now places the one catcher with an outside reputation in the top eighth.

It does not place him first, which is the honest limit of the result. Either the
metric is still carrying circumstance the staff adjustment does not reach, or 2026
was not his best season, or a reputation built over several years is not a
prediction about one of them. The data here cannot choose between those.

**The name mapping is verified rather than assumed.** All three rows reproduce
figures computed independently under the binned baseline: Contreras .343/.345 and
8,798 takes against .3428/.3456, Bailey .331/.326 against .3306/.3270, Heim
.304/.304 and 5,404 takes against .3042/.3044.

---

## Layer 3c — runs, and validation against an outside metric

`analysis/framing_runs.py`. Results in `analysis/results_runs.txt`.

### The run value of a called strike is not one number

Converting strikes above expected into runs needs a run value per called strike.
The canonical figure is about 0.125. But the value of flipping a ball into a
strike depends on the count, and this project has `BALLS` and `STRIKES` on every
take, so the dependence can be measured rather than assumed.

Statcast carries `delta_run_exp`, the change in run expectancy attributable to
each pitch from the batting team's point of view. Within a count, the difference
between the average for a ball and the average for a called strike **is** what
flipping that call is worth:

```
run value of a flip in count (b,s)
    = mean(delta_run_exp | ball, b, s) - mean(delta_run_exp | called_strike, b, s)
```

Measured over all 356,688 takes:

```
0-0  0.0792      1-0  0.1164      2-0  0.1779      3-0  0.1977
0-1  0.0863      1-1  0.1191      2-1  0.1848      3-1  0.2901
0-2  0.1896      1-2  0.2331      2-2  0.3276      3-2  0.6157
```

**The structure is terminality.** A flip matters most when one of the two
outcomes ends the plate appearance. Every two-strike count roughly doubles off
its one-strike neighbour (0-1 → 0-2: 0.086 → 0.190) because the strike ends it.
Every three-ball count is elevated because the *ball* ends it. And 3-2 is the
only count where **both** outcomes are terminal — a flip converts a walk into a
strikeout — which is why it reads 0.6157, nearly **eight times** a 0-0 flip.

Measuring these from the same 184 days as the framing estimates means the run
environment matches by construction rather than by assumption, and it makes the
count dependence visible instead of asserted.

**The canonical 0.125 is biased low, not merely imprecise.** The
occurrence-weighted value across this take population is **0.1493**, 19% higher.
Takes skew heavily toward 0-0 — a third of them, at the lowest value — but the
high-leverage tail outweighs that. So a flat value understates framing in
aggregate on top of mis-ranking individuals.

Two count states fall below 200 of either outcome and would use the pooled value;
in this season none did.

### What count weighting costs

Runs are a weighted sum of the same per-pitch residuals the strike counts use, so
the sampling variance is `Σ v(count)² · p(1−p)` — not the strike variance
rescaled. A catcher who earns his extra strikes on two-strike counts gets more
runs per strike **and** more variance per strike, so `τ²` has to be re-estimated
on the runs scale rather than inherited.

```
                 tau^2        noise      S/N   reliability   1 SD / season
flat 0.125     6.600e-07    1.149e-07   5.75      0.853        4.3 runs
weighted       8.158e-07    2.011e-07   4.06      0.805        4.8 runs
```

**Count weighting finds more true signal and measures it worse.** τ² rises 24%,
because the high-leverage counts are where catchers differ most — but sampling
noise rises 75%, because those counts are rare. Signal-to-noise falls from 5.75
to 4.06, and shrinkage tightens from a 0.772 floor to 0.691.

So the weighted metric is **the more correct target and the less precise
estimate**. A 3-2 flip really is worth eight 0-0 flips; inferring a catcher's
high-leverage performance from a few thousand pitches is genuinely harder than
inferring his overall performance from all of them. Not a free upgrade.

**An internal check:** 34 strikes × 0.125 = 4.25 runs against the flat model's
measured 4.3. The strike-scale and runs-scale estimates agree to two digits by
independent routes.

### How much the weighting moves the leaderboard

```
correlation of values       0.9609
rank correlation            0.9530
mean absolute rank change   3.48 places
largest rank change         12 places
catchers moving 5+ places   16 of 54
```

The aggregate ordering is stable; individual placement is not. **Liam Hicks rises
ten places** (15th → 5th) because he earns his extra strikes where they are worth
most; **Adley Rutschman falls eight** (5th → 13th) because his come cheap. The
flat value is adequate for "is this catcher good" and misleading for "how good."

Dillon Dingler leads either way at **+9.09 runs**; Tyler Stephenson is last at
**−7.37**. About 16 runs separates best from worst, or roughly a win and a half
at the conventional ten runs per win. Bailey sits 9th at +3.78.

**The runs leaderboard is not staff-adjusted.** It is built from raw strikes above
expected. `EXP_RATE` travels with it in
`analysis/framing_runs_leaderboard.csv` so the adjustment can be applied, but the
published leaderboard is the unadjusted one and L3b's staff finding is measured on
a different basis. Said here because a reader would otherwise have to infer it.

The sum of runs over the 54 qualified catchers — +12.2 flat, +11.4 weighted — is
near zero by construction and is **not** a league total.

### Validation against Baseball Savant

Savant publishes a 2026 catcher framing leaderboard built independently, on a
different population and a different baseline. All 54 qualified catchers matched
on MLBAM id.

```
flat, shrunk              Spearman +0.937 (p=2.1e-25)   Pearson +0.947
count-weighted, shrunk    Spearman +0.890 (p=2.2e-19)   Pearson +0.915
staff-adjusted strikes    Spearman +0.816 (p=5.4e-14)   Pearson +0.839
```

**ρ = 0.94 against an independently constructed published metric** is the
strongest external validation available to this project. The staff-adjusted
version correlating lowest is a pass rather than a failure: Savant does not
adjust for staff, so a metric that does should move away from theirs, and
confirming that it does is a check on the adjustment.

**The population difference, predicted and confirmed.** Savant's called-pitch
counts run a median **1.043x** this project's take counts (range 1.030–1.060).
`blocked_ball` is 14,894 league-wide, 4.0% of takes-plus-blocked, and this
project excludes it. One documented filter decision accounts for essentially the
whole population gap between the two leaderboards — see *Takes filter*.

### What explains the disagreement

Two hypotheses were tested against all 54 catchers rather than against the
extremes of a sorted table, plus a deliberate null check:

```
sensitivity to run-value weighting           Spearman +0.355 (p=0.009)
distance of staff quality from league mean   Spearman -0.086 (p=0.538)
sample size (null check)                     Spearman -0.062 (p=0.655)
```

**The null check passing matters.** Sample size showing nothing means the method
is not finding correlations everywhere, so the one positive result is not an
artefact of the frame.

**Run-value weighting is the explanation, and the signed test is decisive.** The
absolute-value correlation above is suggestive; the signed version settles it:

```
corr(DISAGREE, RANK_MOVE) = -0.568   p = 7.4e-6
```

The catchers this project's count weighting *promotes* are precisely the ones
Savant *demotes*. Jensen moves +12 places under weighting and Savant rates him 17
places higher; Rutschman +8 and 8 higher; Hicks **−10** and Savant rates him 20
places lower. **Savant's `rv_tot` behaves like a flat-weighted metric rather than
a count-weighted one** — an inference about another method's internals drawn from
nothing but its published outputs.

**Why the correlation gap alone could not have told us that.** The flat version
correlates 1.035x better than the weighted one in Pearson terms. But the weighted
metric is noisier, and correlation against any external measure attenuates by
roughly the square root of reliability — 0.853 against 0.805 predicts a 1.029x
gap from noise alone. The observed gap is inside the attenuation floor and says
nothing. The signed test is immune to this, because attenuation is about overall
noise level and would show up as disagreement tracking *sample size*, which it
does not.

The script prints that attenuation floor beside the correlations so the invalid
inference cannot be made again by whoever reads it next.

**Shadow-zone exposure was proposed and refuted.** Savant evaluates only pitches
near the zone edge, so a catcher whose staff locates unusually well or badly
should have an unusual share of his takes inside their window — which predicts
disagreement tracking the *distance* of staff quality from the league mean. It
does not: ρ = −0.086, wrong sign, p = 0.538, and the split comparison agrees
(0.0076 against 0.0070). The hypothesis came from noticing that Heim (lowest
staff quality) and Hicks (highest) were the two largest disagreements — in a
table **sorted by disagreement**, which guarantees extremes at its ends. The
mechanism may well be real; this data cannot see it.

---

## Conclusions

**The question.** Can catcher framing be measured from public pitch-level data,
and separated from the quality of the pitching staff?

**Can it be measured: yes, and the measurement validates externally.** Across 54
catchers with 3,000+ takes, the spread in strikes above expected is 33.58 against
13.70 from a permutation null — fourteen null standard deviations out, p below the
resolution of a thousand shuffles. One standard deviation of true catcher skill is
0.61 to 0.65 points of called-strike rate, about 34 strikes or 4.3 to 4.8 runs
across a season. The resulting leaderboard correlates with Baseball Savant's
independently built metric at Spearman +0.94 on all 54 catchers, and the
population difference between the two lists is fully accounted for by one
documented filter decision.

**Can it be separated from the staff: partly, and the residual is the limit.**
Pitching-staff command is a real confound, measured twice at −1,328 and −1,428
strikes per unit expected rate by different methods on different baselines, and
it accounts for 12% of the between-catcher variance. Remove it and the spread
still rejects chance at p = 0.001. But catcher identity remains nearly collinear
with home park and umpire-crew exposure, which the adjustment does not touch, and
54 catchers across 30 teams leaves little room to separate a man from his
circumstances by regression. **The chance test proves catcher identity carries
information the baseline lacks; it does not prove that information is framing
skill.**

**What would change the answer.** A within-team contrast — a starter against his
own backup, who share the staff, the park and the crews. That removes the
confound by design rather than by modelling, needs no slope to estimate, and has
no over-control caveat. It is thin on one season of data and wants several.

**What the project demonstrates about method, which is the part worth reading.**
Seven claims in this repository were made and withdrawn, and the *Corrections*
table below lists each with what caught it. The pattern is consistent: five of
the seven were caught by a check that did not share the broken assumption, and
the two that were not were one error propagating into a second claim. The
longest-lived error survived because it **agreed** with the number beside it —
33.5 against 33.9 reads as confirmation, and a figure that matches its neighbour
gets re-examined least. Twice an inference was built from the extremes of a table
sorted by the very quantity being explained, and twice a correlation across all
the data refused it.

The deliverables are a pipeline that runs unattended, a set of estimates that
survive an external check, and a written record of every conclusion that did not
survive. The third is the one that took the longest and is the hardest to fake.

---

## Where this stops

The project is complete as scoped. One design would resolve what is left, and it
is a separate project rather than a missing piece of this one: **the within-team
contrast** described under *Conclusions*.

Three smaller items the measurements point at, all cheap and all deliberately
deferred:

- **Flexibility in the borderline band specifically.** The GBM's own worst
  calibration sits at 0.3503 and 0.8748, and that band is 28% of pitches and 93%
  of the modelling gain. The aggregate score should not be the objective for a
  question that lives in one band.
- **`GroupKFold` on `CATCHER_ID`** instead of `GAME_PK`, which would remove the
  ~1.5% catcher absorption bounded under *The chance test* rather than leaving it
  as a stated limitation.
- **The previous pitch's call as a feature**, testing the sequential-effects
  result cited under *Why location-controlled still isn't framing* against 2026
  data.

A Streamlit app over `framing_runs_leaderboard.csv` would also be a natural
front end. It is not built: Streamlit Community Cloud cannot reach this BigQuery
project, so the app would need a committed aggregate and a deploy pipeline of its
own.

---

## Exploratory findings

Not all takes are good data points for the model. Some are clearly strikes
(right down the middle), and some are clearly balls (in the other batter's box).
Layer 3 put a number on why: 891 of 1,028 location bins contain a single outcome
across an entire season. Layer 3b put the sharper number on it — the 28% of
pitches whose call is genuinely in doubt carry 93% of what a better model buys.

I don't understand mathematically how batter height creates the strike zone.
(Partial answer found since: `sz_top` and `sz_bot` are not computed from height.
They are measured per pitch from the batter's stance as the pitch is released —
roughly the midpoint between shoulders and belt down to the hollow beneath the
kneecap. They therefore vary pitch to pitch for the same batter, which means
`PLATE_Z_NORM`'s denominator is a noisy measurement rather than a constant. Open
question worth answering from this data: how much of the within-batter variation
in `sz_top`/`sz_bot` is real stance variation versus measurement noise?)

### Row ordering from `statcast()`

`pybaseball.statcast()` returns pitches in **reverse chronological order**,
newest first. `head()` therefore shows the last pitches thrown that day, which
is why a closer appears at the top of a fresh pull. The practical consequence:
`head()` on this data is a sample of ninth innings, not a random sample, so
eyeballing it is biased toward late-game leverage situations and the pitchers
who work them.

---

## Repository hygiene

Everything that a run produces is ignored: `output/`, `*.parquet`, `*.csv`,
`venv/`, `__pycache__/`, `logs/`, `target/`, `tableau/`, the backfill ledgers
(`dates.txt`, `backfill_*.txt`, `backfill_run.log`) and the heartbeat stamps
(`last_run.txt`, `last_data_loaded.txt`). Data belongs in GCS, never in the
repo — `output/` covers the location and `*.parquet` covers the mistake of
writing somewhere else. The heartbeats are the same ruling applied to the
scheduler: runtime state the agent rewrites every morning would otherwise make
the repository dirty daily, forever, with churn that carries no information.

One pattern per line, no delimiters: git treats each line literally, so a
trailing comma makes a rule match only filenames ending in a comma. A missing
final newline is just as bad — appending with `>>` to a file that lacks one
glues the first new pattern onto the last old one, producing a single dead rule.
`git check-ignore -v <path>` reports which pattern matched a path, and is the
right way to verify: it asks git what git thinks instead of reading the file and
inferring.

**Exceptions in `analysis/`, all deliberate.**

`analysis/results_*.txt` are run output, which the rule above would ordinarily
ignore — but they are the evidence behind every figure in the Layer 3 sections,
and a claim whose evidence lives only on one laptop is not a claim. They change
only when a model changes, so they carry information rather than churn.

`analysis/catcher_names.csv`, `analysis/framing_runs_leaderboard.csv` and
`analysis/savant_framing_2026.csv` each need an explicit negation, because
`*.csv` catches them. The first is tracked so a clone reproduces the leaderboard
with names and no network call. The second is the published leaderboard itself.
The third is third-party input that the Savant comparison cannot be reproduced
without, and which Savant may revise. The same reasoning that excludes the
heartbeats includes all of these.

`packages.yml` and `package-lock.yml` are tracked; `dbt_packages/` is not — the
same relationship as `requirements.txt` to `venv/`. The lock file pins the
resolved dbt_utils version so a fresh `dbt deps` cannot drift within the version
range.

**What must be committed to rebuild from a clone:** the ingest script, the two
shell scripts, the dbt project, `analysis/`, and `scheduler/`. The launchd plist
is the one piece of the pipeline that otherwise exists only on one machine — the
test is whether someone with a fresh clone can rebuild the scheduler, and without
the committed copy the answer is no.

**A dbt gotcha worth filing away:** deleting a model file does not drop the
object it built. The `dbt init` example models were removed from
`models/example/` but their views persisted in BigQuery until dropped by hand.
"Why is this stale object still in the warehouse" is a real failure mode.

---

## Corrections

Claims this repository made and later had to withdraw. Kept as a list rather
than quietly edited away, because the pattern in them is more useful than any one
entry.

| Claim | What was wrong | Found by |
|---|---|---|
| Location is null only for automatic rulings | True of a seven-day sample, false of the season — 369 genuine tracking failures | A singular test, once it ran on 184 days |
| Expected strike rate is 0.049 | A Tableau LOD aggregated at bin grain, not row grain | A scatter axis reading 0.10–0.14 where 0.32 was expected |
| Leakage gap is the honest-minus-leaky difference | Those two numbers are scored on different row sets; the confound exceeded the signal and flipped the sign | Scoring identical rows both ways |
| Chance spread is 33.9, so no catcher differs from average | The variance formula assumed every take was a coin flip at the league rate; the real figure is 13.70 and catchers do differ | A permutation test, which could not inherit the assumption |
| Patrick Bailey is +0.63 SD | Divided by the same inflated standard error | The same correction |
| The smooth model's gain is mostly on easy pitches | Backwards: 93% of it is on the 28% of pitches whose call was in doubt. The hedged 0.0785 decile offered as evidence is *inside* the borderline band | The band comparison, once a bug stopped it selecting every row |
| Staff quality dominates the leaderboard | An `EXP_RATE` gap read as a fraction of its own range rather than multiplied by the slope; it is 12% | Weighted least squares, agreeing with the eyeball once the eyeball did the arithmetic |
| The flat-vs-Savant correlation gap shows how Savant weights counts | The gap (1.035x) is inside the attenuation floor (1.029x) that the weighted metric's extra noise already predicts. The conclusion turned out to be right; this evidence for it was not | Computing the attenuation floor, then testing the signed correlation instead |
| Savant disagreement tracks staff-quality extremity | ρ = −0.086, wrong sign, p = 0.538. The hypothesis came from the two most extreme rows of a table *sorted by disagreement*, which guarantees extremes at its ends | A correlation across all 54 catchers, plus a sample-size null check that correctly found nothing |

Five of the nine were caught by a check that did not share the broken assumption.
Two were one error propagating into a second claim. Two were inferences drawn
from the extremes of a table sorted by the quantity being explained — the same
mistake twice, caught both times by widening to all the data.

The one that survived longest is the chance spread, and it survived because
**33.5 and 33.9 agreed.** A near-match reads as confirmation, so it gets
re-examined least. A row that matches its neighbour is the most dangerous row in
any table here, not the safest.

---

## Status

| Stage | |
|---|---|
| A1 / 1b — pull + EDA | ✅ |
| A2 — ingest → GCS → BigQuery, partitioned, idempotent | ✅ |
| A3a — dbt + staging model | ✅ |
| A3b — staging tests | ✅ |
| A3c — `FCT_TAKES` mart | ✅ |
| Full 2026 season backfill | ✅ |
| A4 — daily scheduling | ✅ |
| **Layer 1 complete** | |
| Expected-strike baseline (`FCT_TAKES_EXPECTED`) | ✅ |
| Layer 2 — Tableau Public dashboard | ✅ |
| L3a — evaluation harness + baselines | ✅ |
| L3b — smooth baseline, partial pooling, chance test | ✅ |
| L3c — runs conversion, Savant validation | ✅ |
| **Project complete as scoped** — see *Conclusions* and *Where this stops* | |