# baseball-warehouse

Pitch-level Statcast ingestion into BigQuery, transformed with dbt into
modeling-ready fact tables of taken pitches, on a daily schedule, with a
Tableau Public dashboard on top.

`ingest_statcast.py` pulls a single day of Statcast data from Baseball Savant,
writes it to a partitioned local Parquet file, verifies the write round-trips,
uploads it to GCS, and loads it into BigQuery — replacing that day's partition
atomically so re-runs are safe. dbt then cleans that raw table into a staging
view, filters it into `FCT_TAKES`, and attaches a location-based
expected-strike baseline in `FCT_TAKES_EXPECTED`. A launchd agent runs the
whole chain every morning. `analysis/evaluate_models.py` is the scoreboard every
model from here on is graded by.

The warehouse holds the full 2026 regular season: 184 game days, 716,792
events, 356,498 takes — plus postseason games as they are played, which staging
keeps and the marts exclude.

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
```

Every ingest layer derives its path from a single function, `partition_tail(date)`,
which returns `dt=<date>/statcast.parquet`. The local writer, the GCS uploader,
and the BigQuery loader all build on that one string. Path drift between stages
is a classic silent pipeline bug — one source of truth prevents it.

Inside dbt, `{{ ref() }}` plays the same role: each model names its upstream
rather than a hardcoded dataset, so dbt derives the build order from the SQL and
the whole DAG retargets by changing one line in `profiles.yml`.

The harness reads `FCT_TAKES` rather than `FCT_TAKES_EXPECTED` — see *The
harness reads the un-baselined mart* under Layer 3.

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

Catcher names are not in the warehouse; `tableau/catchers.csv` maps MLBAM IDs to
names via `pybaseball.playerid_reverse_lookup` and is joined in Tableau. That
mapping belongs in dbt as a seed eventually.

### Score models

```bash
python3 analysis/evaluate_models.py                      # raw coordinates
python3 analysis/evaluate_models.py --features distance  # distance from zone centre
```

Output is committed to `analysis/results_raw.txt` and
`analysis/results_distance.txt`. Every figure in the Layer 3 section is
re-derivable from those two files.

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
the inside/outside asymmetry itself rather than having it baked in here.

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

**Significance, because the headline deserves a bound.** The standard deviation
on strikes-above-expected is `sqrt(n × 0.32 × 0.68)` — about ±44 strikes for
Contreras's 8,798 takes and ±34 for Heim's 5,404. So the correct statement is
"indistinguishable from average," not "below average." Across all 54 catchers,
the range runs +69 to −64 and almost nothing clears two standard deviations.

**Measured against chance, the spread is all noise.** Across the 54 qualified
catchers the observed standard deviation of strikes above expected is **33.5**.
The standard deviation expected from binomial noise alone,
`SQRT(AVG(takes) × 0.2176)`, is **33.9**. They are the same number, so **no true
between-catcher difference is detectable at one season with this method** —
controlling for location does not shrink the apparent spread, it eliminates it.

That is a statement about the method rather than about framing. Published metrics
do find repeatable catcher differences, so the conclusion is not that framing
does not exist; it is that 0.25-ft bins with no adjustment for count, handedness
or pitcher are too coarse to see an effect measured in inches.

**Location control is still not sufficient**, and the reason determines what
Layer 3 must contain.

If umpires give pitchers with established command more benefit of the doubt on
borderline pitches — a reputation effect belonging to the pitcher — the extra
strikes still accrue to whoever is catching. The expected-strike baseline cannot
see this, because it conditions only on where the pitch crossed, not on who
threw it.

There is also published work on sequential effects in umpire calls (Chen,
Moskowitz & Shue, on the gambler's fallacy) reporting that umpires are *less*
likely to call a strike immediately after calling one — negative
autocorrelation, the opposite of a "rhythm" effect. Whether that holds in 2026
is testable from this data by adding the previous pitch's call as a feature, and
is on the list.

**Consequence for Layer 3:** the model needs **pitcher effects alongside catcher
effects**, so the two can be separated rather than assumed apart. A model
conditioning on location alone would reproduce the same confound in smoother
form.

### Bins are not a model

The expected-strike baseline is an empirical average within 0.25-ft bins. Three
limitations follow, and all three are arguments for the next phase rather than
fixable by finer binning:

**Per-cell noise.** At roughly 50 pitches per cell for a single catcher, the
standard error on a cell is about 7 percentage points. Individual cells in the
per-catcher heatmaps are not interpretable; only the broad pattern is.

**Attenuation.** A 0.25-ft bin straddling the zone edge contains pitches whose
true strike probability ranges from roughly 0.2 to 0.8. Averaging across that
gradient discards exactly the margin framing operates in. This likely explains
why the observed spread (+69 to −64 strikes) is narrower than published framing
estimates, and why Patrick Bailey — the consensus best framer in baseball —
lands at +0.63 standard deviations rather than clearly positive.

**No other covariates.** Count, handedness and pitch type all shift the called
zone and are not conditioned on.

Bailey is the most valuable row in the table precisely because there is an
outside opinion to check against. The method points the right direction and
cannot resolve the magnitude — a specific, diagnosable shortfall rather than an
unexplained disagreement.

Layer 3 replaces bins with a smooth model — logistic regression or a GAM over
location, handedness, count and pitcher — which pools information across
neighbouring locations instead of treating each cell as an independent sample.
That is exactly what thin per-catcher data needs. The attenuation argument above
was a prediction when it was written; Layer 3's distance model turns it into a
measurement, and the measurement is worse than the prediction.

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

**6 — Methods & Findings.** A text worksheet, not a dashboard. Published
verbatim text lives in `docs/dashboard_copy.md`, versioned alongside the
pipeline that produced the figures in it, because none of those figures are
Tableau fields — they are typed by hand and would go stale silently on a
refresh.

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
measurement, the prediction and the size of the disagreement is the honest stopping
point.

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
failure in a smooth model that has no bins at all, which means the next model
needs flexibility at the edge specifically, not more features generally.

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
fix. If this is extended, the right move is to count contradicting rows
explicitly rather than to shift every prediction.

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
   Any new model has to beat chance before its catcher coefficients mean
   anything, and the harness is what will say whether it does.

---

## Exploratory findings

Not all takes are good data points for the model. Some are clearly strikes
(right down the middle), and some are clearly balls (in the other batter's box).
We will be filtering to pitches around the strike zone edges for the model. Layer
3 put a number on why: 891 of 1,028 location bins contain a single outcome
across an entire season, so the great majority of bins carry no information about
a borderline call at all.

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

**`analysis/results_*.txt` are tracked, deliberately.** They are run output,
which the rule above would ordinarily ignore — but they are the evidence behind
every figure in the Layer 3 section, and a claim whose evidence lives only on one
laptop is not a claim. They change only when a model changes, so they carry
information rather than churn. The same reasoning that excludes the heartbeats
includes these.

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
| Layer 2 — Tableau Public dashboard | in progress |
| L3a — evaluation harness + baselines | ✅ |
| L3b — framing model with pitcher effects | not started |