# Published dashboard text

The text that appears in the Tableau Public workbook, kept here so it is
versioned alongside the pipeline that produced the numbers in it.

**Why this file exists.** Every statistic below is typed by hand — Tableau does
not expose R², p-values or computed standard deviations as fields. If the
extract is ever refreshed, these figures go stale silently and nothing in the
workbook will say so. Re-check anything that is a number rather than a
statement whenever `FCT_TAKES_EXPECTED` is rebuilt.

The panel states current findings. Where an earlier version of this text said
something later corrected, the history lives in the repo's `## Corrections`
table rather than on the dashboard.

**Written to fit one text object.** Tableau text objects clip rather than
scroll, so this is cut to length deliberately: the tab list that used to appear
under "THE VIEWS" is gone, since the tabs are visible on screen and describing
them in prose was pure duplication. Anything added here has to displace
something.

Workbook: Workbook: https://public.tableau.com/app/profile/angelo.zorn/viz/MLBCatcherFraming-AngeloZorn/StrikeZone

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

**Note on baselines, which the panel states plainly.** The charts are drawn from
the 0.25-ft binned baseline in the CSV extract. Findings 3 through 6 come from
the gradient-boosted model in the repo, which the extract does not carry.

---

## Methods & Findings tab — published verbatim

```
CATCHER FRAMING — 2026 MLB REGULAR SEASON

Can you measure catcher framing in public data, and separate it from the
quality of the pitching staff?

DATA
Every taken pitch of the 2026 regular season — 356,498 balls and called strikes
across 184 game days, from Statcast. Swings, hit-by-pitch, pitchouts, blocked
balls and automatic rulings are excluded: framing only exists where an umpire
judged location.

FINDINGS

1. The called zone is a baseball's radius wider than the plate. The plate edge
sits at 0.708 ft; called strikes reach about 0.85 ft. That 0.12 ft gap is a
ball's radius — the rule working correctly, since a strike needs only part of
the ball to cross the zone.

2. Raw called-strike rate ranks catchers by their pitchers. Contreras led at
34.3% with the best-locating staff in baseball; Heim was last at 30.4% with the
worst. Control for where the pitch crossed and both land at average.

3. Catchers still differ by more than luck. The spread in strikes above
expected is 33.58. Shuffling catcher labels across pitches of matched
difficulty a thousand times — each catcher keeping his real workload and pitch
mix — gives a chance spread of 13.70 that never exceeded 16.51. The real spread
is fourteen standard deviations outside that, p < 0.001. The variance model was
audited, not assumed: a statistic that must equal 54 under chance returned 53.5.

4. The effect is small. One standard deviation of true catcher skill is 0.65
points of called-strike rate, about 34 strikes across a season. Individual
figures are pulled toward average in proportion to their own noise.

5. Patrick Bailey, widely regarded as the best framer in baseball, ranks 11th
of 54 raw and 7th after adjusting for his staff — clearly positive either way,
and the only outside check this analysis has on itself.

6. Unproven: that the difference is the catcher's. 54 catchers across 30 teams
makes "which catcher" nearly "which staff, park and umpire crew." Staff command
accounts for 12% and the rest survives removing it, but park and crew do not.

METHOD
Each pitch gets a 0.25-ft location bin whose league called-strike rate is its
expected strike probability; strikes above expected is actual minus summed
expectation. Vertical location is normalized per batter, then rescaled onto the
league zone (1.62–3.21 ft) so both axes read in feet. The charts use these bins.
Findings 3–6 use a gradient-boosted model over location, count and handedness,
every pitch scored by a fit that never saw its game.

LIMITATIONS
Bins average across the zone edge, discarding the margin framing works in: the
smooth model recovers 20% of their error, and 93% of that lands on the 28% of
pitches genuinely in doubt. Location control is incomplete — better-locating
staffs still post lower strikes above expected (p = 0.011, R² = 0.11).
Adjusting for staff cuts both ways, so 0.61 points is a floor on the true
spread and 0.65 a ceiling. ABS challenge data would be ground truth but
Statcast exposes it only on at-bat-ending pitches, where confirmations and
overturns enter under opposite conditions.

NEXT
A starter against his own backup — same staff, same park, same crews. The one
comparison regression cannot fake.

Python → GCS → BigQuery → dbt, daily. Full methodology in the repo.
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
baseball's radius wider, which turns out to be the rule working correctly
rather than umpires erring. Raw called-strike rate ranks catchers in an order
that mostly reflects their pitchers: the league leader catches the staff with
the best locations in baseball, the last-place catcher the worst, and both move
to average once location is controlled.

Catchers do differ by more than luck, and the test is a shuffle rather than a
formula. Reassign catcher labels across pitches of matched difficulty a
thousand times, holding each catcher's workload and pitch mix fixed, and chance
produces a spread of 13.70 strikes. The real spread is 33.58 — fourteen
standard deviations outside anything the shuffle produced. One standard
deviation of true catcher skill works out to about 34 strikes a season.

Whether that difference belongs to the catcher is still open. With 54 qualified
catchers across 30 teams, catcher identity is nearly the same variable as
pitching staff and home park, and staff command accounts for about 12% of the
spread. The workbook shows that confound rather than hiding it, and the repo
carries the arithmetic, the audit behind the variance model, and the design
that would resolve what is left.

Code, pipeline and full methodology:
github.com/angelozorn/baseball-warehouse
```

---

## Figures to re-verify after any rebuild

| Figure | Source |
|---|---|
| 356,498 takes / 184 game days | `FCT_TAKES` row count, `COUNT(DISTINCT GAME_DATE)` |
| 34.3% / 30.4% raw rates | per-catcher `AVG(IS_CALLED_STRIKE)` |
| 0.708 / 0.85 ft zone edge | plate half-width; Strike Zone view |
| 33.58 observed spread | `permutation_test` observed, smooth baseline |
| 13.70 / 16.51 chance spread | permutation null mean and 97.5th percentile in `analysis/model_framing.py` — **not** `SQRT(AVG(takes) * 0.2176)`, which overstates it ~2.4x by assuming every take is a coin flip at the league rate |
| 53.5 against 54 | `Σz²` null mean, the audit of the variance model |
| 0.61 / 0.65 points, ~34 strikes | `sqrt(tau^2)` adjusted and unadjusted |
| Bailey 11th / 7th of 54 | section 8 of `results_framing.txt` |
| p = 0.011, R² = 0.11 | Tableau → Analysis → Trend Lines → Describe Trend Model |
| 12% staff share | `staff_adjusted()` in `analysis/model_framing.py` |
| 20% / 93% / 28% | borderline band comparison in `results_framing.txt` |
| 1.62 / 3.21 zone bounds | `AVG(SZ_BOT)`, `AVG(SZ_TOP)` — also dbt vars |

**The lesson for this table.** The chance standard deviation is the figure that
was once wrong here, and it survived longest because it *agreed* with the number
beside it — two values a percent apart read as confirmation, so that row got
re-examined least. A row that matches its neighbour is the most dangerous row
here, not the safest.

