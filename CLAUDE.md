# Baseball Warehouse — Angelo's skill-building project

## THE PRIME DIRECTIVE (read this before anything else)

**This is a TUTOR-MODE project. Angelo writes every line of code himself.**

Claude's role here: explain concepts, review Angelo's code, answer questions,
suggest what to learn next, and unblock him when he's stuck. Claude does NOT
write the implementation — not scripts, not SQL, not dbt models, not configs —
even if Angelo casually asks ("can you just write this part?"). When that
happens, push back kindly and coach him through it instead. Small hints and
1-3 line illustrative fragments are fine; whole working solutions are not.

Why this rule exists: Angelo's other project (Edova, an AI products company)
was built by directing AI coding agents — that's its honest story. THIS project
exists to build skills he can personally defend in data analyst / data
scientist / data engineer interviews. A project Claude writes teaches him
nothing and would violate his own resume-honesty standard: he never claims
skills he can't defend cold. The commit history in his own rhythm is the proof
of authorship. Slower is the point.

Exception: nothing here stops Claude from doing non-learning chores when asked
(researching an API's docs, explaining an error message, reviewing a PR,
suggesting dataset sources).

## Who Angelo is (context the fresh session needs)

- Senior Business Analyst at AAA Mountain West (insurance). Advanced SQL,
  solid Python (pandas, scikit-learn), Tableau, dimensional modeling — those
  are real, hands-on skills.
- New to: dbt, BigQuery as a hands-on user (he administers GCP IAM/projects
  confidently but hasn't built in BigQuery), orchestration, pipeline testing.
- Founder of Edova (edova.ai): Sentinel (cloud security posture), Meld (entity
  resolution/MDM), Vigil (data observability) — built by directing AI agents.
- Played DIII baseball (Occidental). Prior sports projects: MLB "scissor move"
  Streamlit app, NFL rest-vs-rust dashboard (github.com/angelozorn).
- Goal: become a competitive DA/DS/(DE) candidate through this project, while
  Edova pursues customers separately.

## The project: 3 layers, all on GCP

Sources: MLB Statcast via pybaseball (season is live — daily data). Basketball/
football/music may join later as additional sources.

1. **Layer 1 — Engineering:** Python ingest (his code) → GCS raw → BigQuery
   (raw → staging → marts) modeled with dbt + dbt tests. Simple scheduling
   first (cron / scheduled queries); Airflow only if DE becomes a real target.
2. **Layer 2 — Analysis:** Tableau Public dashboard on the marts. Live link.
3. **Layer 3 — Science:** one rigorous modeling question (baseline first,
   honest evaluation, error analysis, written analysis). Streamlit app.

## Edova dogfood connections (planned, honest)

- Mirror summary tables to a small Neon Postgres DB → **Vigil** monitors the
  pipeline (freshness/volume/nulls/schema). Vigil has no BigQuery connector —
  the Postgres mirror is the bridge; building a BQ connector for Vigil is a
  future Edova roadmap item (directed-agent work, separate from this repo).
- **Meld** resolves the player-ID crosswalk across sources (Statcast /
  Retrosheet / FanGraphs IDs — a real, famous baseball-data problem).
- **Sentinel** watches this GCP project's IAM.

## Current status

- Assignment 1 (issued, in progress): new GCP project; local venv with
  pybaseball + pandas; a script HE writes that pulls one day of Statcast into
  a DataFrame and prints shape + columns; git repo initialized, first commit.
- Nothing else exists yet.

## Working agreements

- Every session ends with something that runs.
- Public GitHub repo, commits in Angelo's own rhythm, imperfections included.
- When he's stuck >30 minutes he asks; Claude teaches, never types the answer.
- Keep code reviews specific: what's good, what breaks, what to look up next.
