# Published dashboard text

The text that appears in the Tableau Public workbook, kept here so it is
versioned alongside the pipeline that produced the numbers in it.

**Why this file exists.** Every statistic below is typed by hand — Tableau does
not expose R², p-values or computed standard deviations as fields. If the
extract is ever refreshed, these figures go stale silently and nothing in the
workbook will say so. Re-check anything that is a number rather than a
statement whenever `FCT_TAKES_EXPECTED` is rebuilt.

**One figure here was wrong and published.** The chance standard deviation was
computed from a variance formula that does not apply to heterogeneous per-pitch
probabilities, and it reversed the conclusion drawn from it. See *Figures to
re-verify*.

Workbook: https://public.tableau.com/app/profile/<profile>/viz/<name>

---

## Workbook structure

Published as sheet tabs rather than a dashboard:

| Tab | Shows |
|---|---|
| Strike Zone | Called-strike rate by location, rulebook zone drawn on |
| Catcher Rates | Raw called-strike rate, catchers with 3,000+ takes |
| Zone by Count | How the called zone shifts with the count |
| Framing by Catcher | Strikes above expected by location — Contreras, Bailey, Heim |
| Staff vs Skill | Staff location quality against performance above expectation |
| Methods & Findings | The text below |

---

## Methods & Findings tab — published verbatim

```
CATCHER FRAMING — 2026 MLB REGULAR SEASON

Can you measure catcher framing in public data, and separate it from the
quality of the pitching staff?

DATA
Every taken pitch of the 2026 regular season: 356,498 balls and called strikes
across 184 game days, from Statcast. Swings, bunts, hit-by-pitch, pitchouts,
blocked balls and automatic rulings are excluded — framing only exists where an
umpire judged location.

THE VIEWS
Strike Zone — called-strike rate by location, with the rulebook zone drawn on.
Catcher Rates — raw called-strike rate, catchers with 3,000+ takes.
Zone by Count — how the called zone shifts with the count.
Framing by Catcher — strikes above expected, by location, for three catchers.
Staff vs Skill — pitching-staff quality against performance above expectation.

FINDINGS

1. The called zone is about a baseball's radius wider than home plate. That is
the rule working correctly, not umpire error: a pitch is a strike if any part of
the ball crosses the zone.

2. Raw called-strike rate ranks catchers mostly by their pitchers. William
Contreras led at 34.3% and catches the staff with the best locations in
baseball. Jonah Heim ranked last at 30.4% with the worst. Controlling for
location, both are average.

3. Catchers do differ by more than luck — but this workbook cannot say the
difference is theirs. The observed spread in strikes above expected is 33.5. An
earlier version of this text put the spread expected from chance at 33.9 and
concluded there was nothing to see. That chance figure was wrong: it assumed
every taken pitch was a coin flip at the league rate, when most are near-certain
calls carrying almost no randomness. Computed correctly — by shuffling catcher
labels across pitches of matched difficulty, thousands of times — chance
produces well under half that spread, and the real spread is rejected as luck at
p < 0.001.

That does not establish framing skill. There are 54 qualified catchers across 30
teams, so "which catcher" is nearly the same variable as which pitching staff,
which home park and which umpire crews. The limitation below shows that confound
is measurable. Separating a catcher from his circumstances needs a comparison
this workbook cannot make — starters against their own backups, who share both.

METHOD
Each taken pitch is assigned to a 0.25-ft location bin; the league called-strike
rate in that bin is its expected strike probability. Strikes above expected =
actual strikes minus summed expectations. Vertical location is normalized to
each batter's own zone, then rescaled to the league-average zone (1.62–3.21 ft)
so axes read in feet. The baseline is a tested dbt model in BigQuery.

LIMITATIONS
Binning averages across the zone edge, discarding the exact margin framing works
in. A smooth model in the repository recovers 20% of the binned model's error,
and 93% of that recovery is on the 28% of pitches whose call was in doubt.

Location control is incomplete: catchers with better-locating staffs still post
lower strikes above expected (p = 0.011, R² = 0.11), so the metric is partly
measuring pitcher behavior. Measured properly, staff quality accounts for about
12% of the between-catcher spread — real, but not most of it.

ABS challenge data would be ground truth, but Statcast exposes it only in
plate-appearance text, visible solely for challenges on the final pitch of an
at-bat — where confirmations and overturns enter under different conditions.

NEXT
A within-team contrast: a starter against his own backup, who share the staff,
the park and the umpire crews. That is the comparison regression cannot fake.

Pipeline: Python → GCS → BigQuery → dbt, scheduled daily. Full arithmetic,
including the corrected chance figure, in the repo.
github.com/angelozorn/baseball-warehouse
```

---

## Tableau Public viz description

Edited on the web at Profile → the viz → Edit Details. Plain text only.

```
Catcher framing, 2026 MLB regular season.

Every taken pitch of the season — 356,498 across 184 game days — pulled from
Statcast and run through a pipeline built end to end: a Python ingest with
round-trip verification, Parquet landed in Google Cloud Storage, partitioned
loads into BigQuery with atomic re-run semantics, dbt models with 27 tests, and
a scheduled daily refresh.

The question: can you see catcher framing in public data, and separate it from
the quality of the pitching staff?

The strike zone itself is visibly not the rulebook rectangle — it is a
baseball's radius wider, which turns out to be the rule working correctly rather
than umpires erring. Raw called-strike rate ranks catchers in an order that
mostly reflects their pitchers: the league leader catches the staff with the
best locations in baseball, the last-place catcher the worst, and both move to
average once location is controlled.

The answer to the second question is a qualified yes, arrived at by first
getting it wrong. The spread between catchers after controlling for location is
33.5 strikes. An earlier version of this write-up compared that to a chance
spread of 33.9 and concluded the two were indistinguishable — but that chance
figure treated every taken pitch as a coin flip, when most are near-certain
calls carrying no randomness at all. A permutation test, which assumes no
formula, puts chance well under half the observed spread and rejects it at
p < 0.001.

So the differences are real. Whether they belong to the catcher is still open:
54 catchers across 30 teams means catcher identity is nearly the same variable
as pitching staff and home park, and the workbook shows that confound is
measurable. The arithmetic, the correction and the next test are in the repo.

Code, pipeline and full methodology:
github.com/angelozorn/baseball-warehouse
```

---

## Figures to re-verify after any rebuild

| Figure | Source |
|---|---|
| 356,498 takes / 184 game days | `FCT_TAKES` row count, `COUNT(DISTINCT GAME_DATE)` |
| 34.3% / 30.4% raw rates | per-catcher `AVG(IS_CALLED_STRIKE)` |
| 33.5 observed SD | `STDDEV` of strikes above expected across qualified catchers |
| ~13.8 chance SD | permutation null in `analysis/model_framing.py` — **not** `SQRT(AVG(takes) * 0.2176)`, which overstates it ~2.4x by assuming every take is a coin flip at the league rate |
| p = 0.011, R² = 0.11 | Tableau → Analysis → Trend Lines → Describe Trend Model |
| 12% staff share | `staff_adjusted()` in `analysis/model_framing.py` |
| 1.62 / 3.21 zone bounds | `AVG(SZ_BOT)`, `AVG(SZ_TOP)` — also dbt vars |

**The lesson for this table.** The chance standard deviation is the figure that
was wrong, and it survived longest because it *agreed* with the number beside it
— 33.5 against 33.9 reads as confirmation, so it got re-examined least. A row
that matches its neighbour is the most dangerous row here, not the safest.