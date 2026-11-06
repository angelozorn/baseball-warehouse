"""Catcher framing, 2026 — Streamlit front end.

Reads committed CSVs rather than BigQuery, because Streamlit Community Cloud has
no credentials for this project and a service-account key in repo secrets would
be the wrong trade for a read-only public dashboard. Regenerate the data with
analysis/export_app_data.py and commit it.

Run locally:
    streamlit run app/streamlit_app.py
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

DATA = Path(__file__).parent / "data"
BIN = 0.25
ZONE_BOT, ZONE_TOP = 1.62, 3.21
PLATE_HALF = 0.708

REPO = "https://github.com/angelozorn/baseball-warehouse"

st.set_page_config(page_title="Catcher Framing 2026", layout="wide")


@st.cache_data
def load():
    board = pd.read_csv(DATA / "leaderboard.csv")
    league = pd.read_csv(DATA / "zone_league.csv")
    per_catcher = pd.read_csv(DATA / "zone_catcher.csv")
    return board, league, per_catcher


def grid(frame, value):
    """Pivot bin indices into an array plus the extent matplotlib needs."""
    pivot = frame.pivot_table(index="Z_BIN", columns="X_BIN", values=value)
    x = pivot.columns.to_numpy() * BIN
    z = pivot.index.to_numpy() * BIN
    extent = [x.min() - BIN / 2, x.max() + BIN / 2,
              z.min() - BIN / 2, z.max() + BIN / 2]
    return pivot.to_numpy(), extent


def draw_zone(ax):
    ax.add_patch(plt.Rectangle(
        (-PLATE_HALF, ZONE_BOT), 2 * PLATE_HALF, ZONE_TOP - ZONE_BOT,
        fill=False, lw=1.6, edgecolor="black",
    ))


def zone_figure(array, extent, cmap, vmin, vmax, label):
    fig, ax = plt.subplots(figsize=(5.2, 5.2))
    im = ax.imshow(array, origin="lower", extent=extent, cmap=cmap,
                   vmin=vmin, vmax=vmax, aspect="equal", interpolation="nearest")
    draw_zone(ax)
    ax.set_xlabel("horizontal location (ft, catcher's view)")
    ax.set_ylabel("height (ft)")
    fig.colorbar(im, ax=ax, label=label, shrink=0.82)
    fig.tight_layout()
    return fig


# --- pages ------------------------------------------------------------------

def page_leaderboard(board):
    st.title("Catcher framing, 2026 regular season")
    st.caption(
        "Framing runs above expectation, from 356,498 taken pitches. "
        "Estimates are shrunk toward the league average in proportion to how "
        "noisy each catcher's own sample is."
    )

    weighting = st.radio(
        "Run value per called strike",
        ["Count-weighted (measured per count state)",
         "Flat 0.125 (the canonical figure)"],
        horizontal=True,
    )
    col = ("RUNS_WTD_SHRUNK" if weighting.startswith("Count")
           else "RUNS_FLAT_SHRUNK")

    ranked = board.sort_values(col, ascending=False).reset_index(drop=True)
    ranked.insert(0, "Rank", ranked.index + 1)

    show = ranked[["Rank", "NAME", "N", "EXP_RATE",
                   "RUNS_FLAT_SHRUNK", "RUNS_WTD_SHRUNK"]].rename(columns={
        "NAME": "Catcher", "N": "Takes", "EXP_RATE": "Staff quality",
        "RUNS_FLAT_SHRUNK": "Runs (flat)", "RUNS_WTD_SHRUNK": "Runs (weighted)",
    })

    left, right = st.columns([3, 2])
    with left:
        st.dataframe(
            show.style.format({
                "Takes": "{:,.0f}", "Staff quality": "{:.3f}",
                "Runs (flat)": "{:+.2f}", "Runs (weighted)": "{:+.2f}",
            }),
            height=620, use_container_width=True, hide_index=True,
        )
    with right:
        top = ranked.head(12).set_index("NAME")[col]
        bottom = ranked.tail(12).set_index("NAME")[col]
        st.caption("Best twelve")
        st.bar_chart(top, horizontal=True)
        st.caption("Worst twelve")
        st.bar_chart(bottom, horizontal=True)

    st.info(
        "**Switching the run value moves individual catchers by a median 3.5 "
        "rank places** — 16 of 54 move five or more. A flip on 3-2 is worth "
        "0.616 runs against 0.079 on 0-0, so a catcher who earns his extra "
        "strikes in high-leverage counts gains under the weighted version. "
        "The flat and weighted orderings correlate at 0.953; the difference is "
        "individual, not aggregate."
    )
    st.warning(
        "**These runs are not adjusted for pitching staff.** Staff command "
        "accounts for about 12% of the between-catcher spread, and `Staff "
        "quality` is shown so you can see who was dealt what. Catcher identity "
        "is also nearly collinear with home park and umpire-crew exposure, "
        "neither of which is controlled anywhere in this project."
    )


def page_catcher(board, league, per_catcher):
    st.title("Catcher detail")

    names = sorted(per_catcher["NAME"].unique())
    default = names.index("Patrick Bailey") if "Patrick Bailey" in names else 0
    who = st.selectbox("Catcher", names, index=default)

    row = board[board["NAME"] == who]
    if len(row):
        row = row.iloc[0]
        a, b, c, d = st.columns(4)
        a.metric("Takes", f"{row['N']:,}")
        b.metric("Runs, weighted", f"{row['RUNS_WTD_SHRUNK']:+.2f}")
        c.metric("Runs, flat", f"{row['RUNS_FLAT_SHRUNK']:+.2f}")
        d.metric("Staff quality", f"{row['EXP_RATE']:.3f}")

    mine = per_catcher[per_catcher["NAME"] == who]

    left, right = st.columns(2)
    with left:
        st.subheader("League called-strike rate")
        array, extent = grid(league, "RATE")
        st.pyplot(zone_figure(array, extent, "viridis", 0, 1,
                              "called-strike rate"))
        st.caption(
            "Every take in the season. The rectangle is the rulebook zone at "
            "league-average height. The called zone runs wider — about a "
            "baseball's radius — because a strike needs only part of the ball "
            "to cross."
        )
    with right:
        st.subheader(f"{who} — strikes above expected")
        array, extent = grid(mine, "SAE_PER_100")
        span = float(np.nanmax(np.abs(array))) if array.size else 1.0
        span = max(span, 1.0)
        st.pyplot(zone_figure(array, extent, "coolwarm", -span, span,
                              "extra strikes per 100 takes"))
        st.caption(
            "Red is above expectation, blue below. Cells with fewer than 25 "
            "takes are blank."
        )

    st.warning(
        "**Do not read individual cells.** At roughly 50 pitches a cell, the "
        "standard error on its rate is about 7 percentage points — a single "
        "square is mostly noise. Only the broad pattern carries information, "
        "and the leaderboard number is what pools it."
    )


def page_methods():
    st.title("How this works, and what it does not establish")

    st.markdown(f"""
### The question

Can catcher framing be measured from public pitch-level data, and separated from
the quality of the pitching staff?

### The data

Every taken pitch of the 2026 regular season — **356,498** balls and called
strikes across **184 game days** — pulled from Statcast through a pipeline built
end to end: a Python ingest with round-trip verification, Parquet landed in
Google Cloud Storage, partitioned loads into BigQuery with atomic re-run
semantics, dbt models with 27 tests, and a daily schedule.

Swings, hit-by-pitch, pitchouts, blocked balls and automatic rulings are
excluded. Framing only exists where an umpire judged location.

### The model ladder

Each model is scored by one fixed jury — log loss, AUC and a calibration curve —
on the same held-out games. Whole games are held out rather than random pitches,
because pitches in a game share an umpire, a park and a tracking calibration.

| Model | Log loss | AUC |
|---|---|---|
| Guess the league rate every time | 0.629 | 0.500 |
| Straight line on raw coordinates | 0.628 | 0.544 |
| Straight line on distance from centre | 0.217 | 0.966 |
| 0.25-ft location lookup table | 0.148 | 0.985 |
| **Gradient boosting on location, count, handedness** | **0.119** | **0.990** |

The straight-line model is the instructive failure: a linear score can only rise
in one direction across the plane, but the zone is a box with balls on all four
sides, so sorting by that score walks out of the zone, through it, and out the
far side. Its most confident strike decile is a called strike 0.06% of the time.

**28% of pitches carry 93% of what the smooth model gains** over the lookup
table — the ones whose call was genuinely in doubt, which is the only band
framing occupies.

### The chance test

Across 54 qualified catchers the spread in framing is **33.58** strikes. How much
could luck alone produce? Rather than trust a formula, catcher labels were
shuffled across pitches of matched difficulty — each catcher keeping his real
workload and pitch mix — a thousand times.

Luck produces **13.70**, and never exceeded 16.51. The real spread sits fourteen
standard deviations outside that. p < 0.001.

An earlier version of this analysis compared 33.5 against a chance figure of 33.9
and concluded there was nothing to see. That 33.9 assumed every taken pitch was a
coin flip at the league rate, when most are near-certain calls carrying almost no
randomness. The permutation test, which assumes no formula, is what caught it.
The new variance model was audited rather than assumed: a second statistic has to
equal 54 under pure chance, and the shuffle returned 53.5.

### Validation against an outside metric

Baseball Savant publishes its own 2026 framing leaderboard, built independently.
All 54 catchers matched, at **Spearman +0.94**. Their called-pitch counts run
1.043x these take counts, which the one documented exclusion — blocked balls,
4.0% of takes-plus-blocked — accounts for almost exactly.

Where the two lists disagree, the explanation is how each values counts: the
catchers this analysis promotes under count weighting are the ones Savant demotes
(ρ = −0.568, p = 7.4e-6), so Savant's figure behaves like a flat-weighted metric.

### What this does not establish

That the differences belong to the catcher. With 54 catchers across 30 teams,
catcher identity is nearly the same variable as pitching staff, home park and
umpire-crew exposure. Staff command is measurable and accounts for about 12%; the
spread survives removing it, but park and crew are untouched.

Separating a catcher from his circumstances needs a comparison this data cannot
make — a starter against his own backup, who share both. That is the next
project, not a missing piece of this one.

### Caveats worth knowing

- **This app is a snapshot.** The pipeline refreshes daily; these files do not.
- **One season.** Nothing here speaks to whether a catcher repeats.
- **Nine claims in this project were made and withdrawn.** Each one, and what
  caught it, is listed in the repository's Corrections table — including two
  where an inference was drawn from the extremes of a table sorted by the very
  quantity being explained.

[Full methodology, code and corrections]({REPO})
""")


# --- main -------------------------------------------------------------------

def main():
    try:
        board, league, per_catcher = load()
    except FileNotFoundError as exc:
        st.error(
            f"Missing data file: {exc}. Run "
            "`python3 analysis/export_app_data.py` and commit `app/data/`."
        )
        return

    st.sidebar.title("Catcher framing 2026")
    page = st.sidebar.radio(
        "View", ["Leaderboard", "Catcher detail", "Methods"], label_visibility="collapsed"
    )
    st.sidebar.markdown("---")
    st.sidebar.caption(
        f"356,498 takes · 184 game days · 54 qualified catchers\n\n[Repository]({REPO})"
    )

    if page == "Leaderboard":
        page_leaderboard(board)
    elif page == "Catcher detail":
        page_catcher(board, league, per_catcher)
    else:
        page_methods()


main()