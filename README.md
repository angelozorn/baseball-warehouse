# baseball-warehouse

Pitch-level Statcast ingestion into BigQuery, transformed with dbt into a
modeling-ready fact table of taken pitches, on a daily schedule.

`ingest_statcast.py` pulls a single day of Statcast data from Baseball Savant,
writes it to a partitioned local Parquet file, verifies the write round-trips,
uploads it to GCS, and loads it into BigQuery — replacing that day's partition
atomically so re-runs are safe. dbt then cleans that raw table into a staging
view and filters it into `FCT_TAKES`, the fact table a catcher-framing model
reads. A launchd agent runs the whole chain every morning.

The warehouse holds the full 2026 regular season: 184 game days, 716,792
events, 356,498 takes — plus postseason games as they are played, which staging
keeps and the mart excludes.

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
```

Every ingest layer derives its path from a single function, `partition_tail(date)`,
which returns `dt=<date>/statcast.parquet`. The local writer, the GCS uploader,
and the BigQuery loader all build on that one string. Path drift between stages
is a classic silent pipeline bug — one source of truth prevents it.

Inside dbt, `{{ ref() }}` plays the same role: `FCT_TAKES` names
`STG_STATCAST_PITCHES` rather than a hardcoded dataset, so dbt derives the build
order from the SQL and the whole DAG retargets by changing one line in
`profiles.yml`.

**Region note:** the GCS bucket, both BigQuery datasets, and the dbt profile are
all `us-west1`. BigQuery cannot load from a bucket in a different location, and
datasets cannot be moved after creation, so this pairing is deliberate rather
than incidental.

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

---

## Scheduling

A launchd agent (`com.angelozorn.statcast-daily`) runs `run_daily.sh` at 06:37
daily: ingest the previous day's games, then `dbt build`.

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.angelozorn.statcast-daily.plist
launchctl kickstart -k gui/$(id -u)/com.angelozorn.statcast-daily   # fire now
launchctl list | grep statcast                                      # PID, last exit status
```

launchd ignores edits to a loaded job, so any plist change needs `bootout` then
`bootstrap` again.

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

`dbt build` runs 23 data tests — 10 on staging, 13 on the mart — interleaved
with the models so a model whose tests fail is not used downstream. All pass
except `ASSERT_UNEXPLAINED_NULL_LOCATION_RATE`, which warns with 369 rows by
design: the tracking-failure rate is reported rather than suppressed.

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
raw, `THROWN_PITCHES` grows while `TAKES` does not, because the mart is
restricted to `game_type = 'R'`.

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
forgotten filter from a silent full scan into an error. `FCT_TAKES` is itself
partitioned on `GAME_DATE`, so the same pruning holds at the mart layer, where
the staging view's `CAST` would otherwise leave it to the optimizer.

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
would skip `FCT_TAKES` entirely — the mart would never be produced, every day,
forever. Choosing `warn` began as a judgment about test philosophy and has become
a pipeline dependency. Anyone raising it back to `error` months from now would
silently stop the warehouse.

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

All of these live in the mart, not staging, for the same reason the location
assertions do: staging keeps every row the source has, including broken ones,
because its job is faithful representation. The mart is the layer entitled to say
"these rows are unusable for my purpose."

### Why staging is a view and the mart is a table

**Staging is a view.** It is a thin projection over raw — a cast, some renames,
a column selection, no aggregation and no filter. Materializing it would
duplicate the full raw dataset in storage and add a refresh step, in exchange
for savings on a query that is nearly free to recompute. A view is also always
consistent with whatever raw currently holds, which is the right default for a
layer whose job is faithful representation.

**The mart is a table.** Three reasons, in order of weight:

1. **The work runs once.** The filters and the normalized-location derivation
   execute at build time instead of on every downstream read. A framing model
   will scan this table repeatedly during training and evaluation.
2. **Consistency across consumers.** A materialized table means a training run
   and a dashboard reading at different moments see byte-identical derived
   values. A view recomputes, so if raw or staging changed between two reads,
   two consumers could silently disagree about the same "pitch."
3. **Physical layout.** `FCT_TAKES` is partitioned on `GAME_DATE` and clustered
   on `CATCHER_ID` — neither is possible for a view, and both matter for a
   workload whose every query groups by catcher and filters by date.

The trade-off is that the mart is stale until the next `dbt build`. With a daily
scheduled run, staleness is bounded by the ingest cadence anyway, so a view would
buy no real freshness.

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

## Exploratory findings

Not all takes are good data points for the model. Some are clearly strikes
(right down the middle), and some are clearly balls (in the other batter's box).
We will be filtering to pitches around the strike zone edges for the model.

I don't understand mathematically how batter height creates the strike zone.

### Row ordering from `statcast()`

`pybaseball.statcast()` returns pitches in **reverse chronological order**,
newest first. `head()` therefore shows the last pitches thrown that day, which
is why a closer appears at the top of a fresh pull. The practical consequence:
`head()` on this data is a sample of ninth innings, not a random sample, so
eyeballing it is biased toward late-game leverage situations and the pitchers
who work them.

---

## Repository hygiene

`.gitignore` covers `output/`, `*.parquet`, `*.csv`, `venv/`, `__pycache__/`,
`logs/`, `target/`, the backfill bookkeeping files (`dates.txt`,
`backfill_*.txt`, `backfill_run.log`) and the heartbeat stamps
(`last_run.txt`, `last_data_loaded.txt`). Data belongs in GCS, never in the
repo — `output/` covers the location and `*.parquet` covers the mistake of
writing somewhere else.

One pattern per line, no delimiters: git treats each line literally, so a
trailing comma makes a rule match only filenames ending in a comma. A missing
final newline is just as bad — appending with `>>` to a file that lacks one
glues the first new pattern onto the last old one, producing a single dead rule.
`git check-ignore -v <path>` reports which pattern matched a path, and is the
right way to verify: it asks git what git thinks instead of reading the file and
inferring.

`packages.yml` and `package-lock.yml` are tracked; `dbt_packages/` is not — the
same relationship as `requirements.txt` to `venv/`. The lock file pins the
resolved dbt_utils version so a fresh `dbt deps` cannot drift within the version
range.

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
| Layer 2 — Tableau | not started |
| Layer 3 — framing model | not started |