# Baseball Warehouse — Angelo's skill-building project

## THE PRIME DIRECTIVE (read this before anything else)

**This project ran under two different authorship rules. Know which one applies
before you touch anything.**

### Regime 1 — Layers 1 and 2: hand-written, and that claim is verified

Through 2026-10-21, Angelo wrote every line of implementation himself. That
covers the Python ingest (`ingest_statcast.py`), both shell scripts, the launchd
plist, the entire dbt project (`transform/`), and the Tableau workbook. Claude
explained, reviewed, and unblocked; it did not type the solution.

**Do not rewrite that code.** Review it, explain it, help debug it — but a
wholesale rewrite destroys a provenance claim Angelo can currently defend.

**Scope of the claim, stated precisely:** code and SQL. The README prose was
drafted in pair sessions from early on, for Layer 1 and 2 sections too. "Angelo
wrote the pipeline" is true. "Angelo wrote the documentation" is not, and he
does not say it.

### Regime 2 — Layer 3 onward: AI pair sessions are allowed

On 2026-10-24 Angelo decided to pair on Layer 3. **This was a deliberate
decision, not drift.** `analysis/evaluate_models.py` and
`analysis/model_framing.py` were built in pair sessions, and any later work is
under the same rule. A fresh session should pair freely here and should not
invoke Regime 1 to refuse.

### The price of pairing — non-negotiable, both halves

1. **Every pair-built commit carries the co-author trailer.** No exceptions, no
   retroactive cleanup. The git history is the provenance record, and a history
   that hides the pairing is worse than no record.
2. **Understanding is verified, not assumed.** Review quizzes on the paired code
   and mock interviews on the findings. The bar is **"defend every line cold,"
   not "typed every line."** If Angelo cannot explain a design choice in a
   paired file without reading it, that file is not finished — regardless of
   whether it runs.

Claude should offer the quiz rather than wait to be asked. A paired file that
has never been quizzed is an open liability, not a deliverable.

### Representation

Resume and interviews say **"built with AI assistance"** for the layers where
that is true, and say it unprompted. Angelo's standard is that he never claims a
skill he cannot defend cold — pairing does not violate that standard, but
concealing the pairing would.

Three honest modes now exist across his work, and they get labeled differently:
Edova was built by **directing** AI agents; Layers 1–2 here were **hand-typed**;
Layer 3 was **paired**. Collapsing them into one story is the failure mode this
section exists to prevent.

### Exception, unchanged

Non-learning chores are always fine: researching an API's docs, explaining an
error, reviewing a diff, suggesting data sources, drafting prose.

## Who Angelo is (context the fresh session needs)

- Senior Business Analyst at AAA Mountain West (insurance). Advanced SQL,
  solid Python (pandas, scikit-learn), Tableau, dimensional modeling — those
  are real, hands-on skills.
- Learned on this project: dbt, BigQuery as a hands-on builder, launchd
  scheduling, pipeline testing, held-out evaluation, permutation testing,
  partial pooling.
- Founder of Edova (edova.ai): Sentinel (cloud security posture), Meld (entity
  resolution/MDM), Vigil (data observability) — built by directing AI agents.
- Played DIII baseball (Occidental). Prior sports projects: MLB "scissor move"
  Streamlit app, NFL rest-vs-rust dashboard (github.com/angelozorn).
- Goal: become a competitive DA/DS/(DE) candidate through this project, while
  Edova pursues customers separately.

## The project: 3 layers, all on GCP

Source: MLB Statcast via pybaseball, full 2026 regular season.

1. **Layer 1 — Engineering:** Python ingest → GCS raw → BigQuery
   (raw → staging → marts) modeled with dbt, 27 tests, launchd daily schedule.
2. **Layer 2 — Analysis:** Tableau Public workbook on the marts.
3. **Layer 3 — Science:** catcher framing. Held-out evaluation harness,
   gradient-boosted baseline, partial pooling, permutation test.

## Current status — complete as scoped

| | |
|---|---|
| Layer 1 — ingest, warehouse, dbt, scheduling | done |
| Full 2026 season backfill (356,498 takes, 184 game days) | done |
| Layer 2 — Tableau Public workbook | done |
| L3a — evaluation harness and baselines | done |
| L3b — smooth baseline, partial pooling, chance test | done |
| L3c — within-team contrast | not started, deliberately |

**The headline finding.** Catchers differ in called strikes above expected by
more than chance: observed spread 33.58 against a permutation null of 13.70,
p < 0.001, 14.2 null SDs out. Pitching-staff command is a real confound (−1,428
strikes per unit expected rate, t = −2.58) but explains only 12%, and the spread
survives removing it. True spread 0.61–0.65 points of called-strike rate.

**This reversed a published conclusion.** The README previously claimed no
between-catcher difference was detectable, comparing 33.5 observed to 33.9 "from
chance" — and that 33.9 was wrong, computed from a formula treating every take
as a coin flip at the league rate. See the README's `## Corrections` table, which
lists all seven claims this repo has withdrawn. **That table is load-bearing:
keep adding to it.**

## Conventions a fresh session must not break

- **Uppercase identifiers** in BigQuery and dbt. Personal preference, applied
  consistently. Python in `analysis/` follows Python conventions instead.
- **`OMP_NUM_THREADS=1`** on `analysis/model_framing.py`. HistGBM's parallel
  histogram building is not deterministic; unpinned runs differ by ~9e-5 of log
  loss, enough to swap adjacent catcher ranks.
- **`analysis/results_*.txt` and `analysis/catcher_names.csv` are committed**
  despite the gitignore, because they are the evidence behind every figure in
  the README. Never quote a number the results files don't contain.
- **Fail loudly, not quietly.** Checks raise; they do not print a warning and
  continue.
- **Claims get a source.** Any figure in the README or dashboard copy must be
  re-derivable from a committed file or a named query.

## Edova dogfood connections (planned, not built)

- Mirror summary tables to a small Neon Postgres DB → **Vigil** monitors the
  pipeline. Vigil has no BigQuery connector; the Postgres mirror is the bridge.
- **Meld** resolves the player-ID crosswalk across sources (Statcast /
  Retrosheet / FanGraphs). `analysis/catcher_names.csv` is the hand-rolled
  version of this problem.
- **Sentinel** watches this GCP project's IAM.

## Working agreements

- Every session ends with something that runs.
- Public GitHub repo, commits in Angelo's own rhythm, imperfections included.
- Pair-built commits carry the co-author trailer.
- Code reviews stay specific: what's good, what breaks, what to look up next.
- When a claim turns out to be wrong, it goes in the Corrections table rather
  than getting edited away.