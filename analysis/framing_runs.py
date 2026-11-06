"""L3c — framing in runs, and the Savant comparison.

Two things:

  1. Strikes above expected converted to runs. First with the canonical flat
     ~0.125 runs per called strike, then with a count-weighted value measured
     from this season's own data, and the sensitivity between them.
  2. Comparison against Baseball Savant's published 2026 catcher framing
     leaderboard: rank correlation, a scatter, and an attribution of who
     disagrees and why.

Run:
    OMP_NUM_THREADS=1 python3 analysis/framing_runs.py

Step 2 is skipped unless analysis/savant_framing_2026.csv exists. Download it
from baseballsavant.mlb.com/catcher_framing (season 2026, set the minimum to
match MIN_TAKES) and save it there.

OMP_NUM_THREADS=1 for the same reason model_framing.py needs it: HistGBM is not
deterministic across thread counts, and this script refits it.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery

from model_framing import (
    MIN_TAKES,
    RANDOM_STATE,
    catcher_names,
    catcher_table,
    load_takes,
    out_of_fold_predictions,
    shrink,
    staff_adjusted,
)

GCP_PROJECT = "mlb-catcher-framing"

# The configuration model_framing.py selected on held-out games. Hardcoded
# rather than re-derived so this script cannot silently diverge from the
# baseline the README quotes; results_framing.txt is the record.
MAX_ITER = 100
USE_ISOTONIC = False

# The conventional figure: one extra called strike is worth about an eighth of
# a run. Used as the starting point and as the thing the measured version is
# compared against.
FLAT_RUN_VALUE = 0.125

# A count state needs this many of BOTH outcomes before its own measured value
# is trusted; below it, the pooled value is used instead. 3-0 holds only ~7,000
# takes across the season and splits thin.
MIN_PER_OUTCOME = 200

# A catcher is counted as "disagreeing" with Savant at this many rank places.
DISAGREE_THRESHOLD = 10

SAVANT_CSV = Path(__file__).with_name("savant_framing_2026.csv")
OUT_CSV = Path(__file__).with_name("framing_runs_leaderboard.csv")
OUT_PNG = Path(__file__).with_name("savant_comparison.png")


# --- run value per count ----------------------------------------------------

def count_run_values():
    """Measure the run value of a ball -> called-strike flip, by count.

    Statcast carries `delta_run_exp`: the change in run expectancy attributable
    to each pitch, from the batting team's point of view. A called strike is bad
    for the offense and carries a negative value; a ball is good and carries a
    positive one. The difference between those two averages, within a count, is
    what flipping that call is worth.

    So the run value of an extra strike in count (b, s) is

        mean(delta_run_exp | ball, b, s) - mean(delta_run_exp | strike, b, s)

    which comes out positive and is read as runs prevented.

    Why this beats a table from elsewhere: it is measured from the same 184 days
    of games the framing estimates come from, so the run environment matches by
    construction rather than by assumption. It also makes the count dependence
    visible instead of asserted — a flip on 3-2 turns a walk into a strikeout
    and is worth multiples of a flip on 0-0.

    This reads the raw table, which is the one place in the project that does.
    `delta_run_exp` is not carried through staging, and this is a one-off
    aggregate of 24 constants rather than a modeled dependency. The proper fix
    is to add the column to STG_STATCAST_PITCHES; until then the exception is
    stated rather than hidden.
    """
    client = bigquery.Client(project=GCP_PROJECT)
    sql = f"""
        SELECT
            balls   AS BALLS,
            strikes AS STRIKES,
            description AS DESCRIPTION,
            COUNT(*) AS N,
            AVG(delta_run_exp) AS MEAN_DRE
        FROM `{GCP_PROJECT}.STATCAST_RAW_DATA.STATCAST_PITCH_DATA`
        WHERE game_type = 'R'
          AND description IN ('ball', 'called_strike')
          AND delta_run_exp IS NOT NULL
          AND balls BETWEEN 0 AND 3
          AND strikes BETWEEN 0 AND 2
        GROUP BY BALLS, STRIKES, DESCRIPTION
        ORDER BY BALLS, STRIKES, DESCRIPTION
    """
    raw = client.query(sql).to_dataframe()

    if raw.empty:
        raise RuntimeError(
            "no rows with delta_run_exp — check the column exists in "
            "STATCAST_PITCH_DATA (SELECT * LIMIT 1 and look for it)"
        )

    wide = raw.pivot_table(
        index=["BALLS", "STRIKES"], columns="DESCRIPTION",
        values=["N", "MEAN_DRE"],
    )
    tab = pd.DataFrame({
        "N_BALL": wide[("N", "ball")],
        "N_STRIKE": wide[("N", "called_strike")],
        "DRE_BALL": wide[("MEAN_DRE", "ball")],
        "DRE_STRIKE": wide[("MEAN_DRE", "called_strike")],
    }).reset_index()

    tab["RUN_VALUE"] = tab["DRE_BALL"] - tab["DRE_STRIKE"]
    tab["THIN"] = (tab["N_BALL"] < MIN_PER_OUTCOME) | (tab["N_STRIKE"] < MIN_PER_OUTCOME)

    # Pooled value, weighted by how often each count actually occurs, for the
    # thin states and as the measured counterpart to the canonical 0.125.
    weight = tab["N_BALL"] + tab["N_STRIKE"]
    pooled = float((tab["RUN_VALUE"] * weight).sum() / weight.sum())
    tab["RUN_VALUE_USED"] = np.where(tab["THIN"], pooled, tab["RUN_VALUE"])

    print(f"\n  run value of a ball -> called-strike flip, by count")
    print(f"  measured from delta_run_exp over "
          f"{int(weight.sum()):,} takes\n")
    print(f"    {'count':>7}  {'balls':>8}  {'strikes':>8}  "
          f"{'runs':>7}  {'used':>7}")
    for row in tab.itertuples():
        flag = "  pooled" if row.THIN else ""
        print(f"    {row.BALLS}-{row.STRIKES:<5}  {row.N_BALL:>8,}  "
              f"{row.N_STRIKE:>8,}  {row.RUN_VALUE:>7.4f}  "
              f"{row.RUN_VALUE_USED:>7.4f}{flag}")
    print(f"\n  pooled (occurrence-weighted): {pooled:.4f}")
    print(f"  canonical flat value:         {FLAT_RUN_VALUE:.4f}")
    print(f"  spread across counts:         "
          f"{tab['RUN_VALUE'].min():.4f} to {tab['RUN_VALUE'].max():.4f} "
          f"({tab['RUN_VALUE'].max() / tab['RUN_VALUE'].min():.1f}x)")

    return tab, pooled


# --- runs per catcher -------------------------------------------------------

def framing_runs(df, values):
    """Per-catcher framing runs, flat and count-weighted, with exact variance.

    Runs are a weighted sum of the same per-pitch residuals the strike counts
    use:

        R_c = sum over the catcher's pitches of v(count) * (y - p)

    so its sampling variance is sum of v(count)^2 * p(1-p). That is not the
    strike-count variance rescaled, because the weights differ pitch to pitch —
    a catcher who gets his extra strikes on two-strike counts earns more runs
    per strike AND carries more variance per strike. Shrinkage therefore has to
    be re-estimated on the runs scale rather than inherited from the counts.
    """
    v = values.set_index(["BALLS", "STRIKES"])["RUN_VALUE_USED"]
    key = pd.MultiIndex.from_arrays([df["BALLS"], df["STRIKES"]])
    df = df.assign(
        RESID=df["Y"] - df["P_OOF"],
        PQ=df["P_OOF"] * (1 - df["P_OOF"]),
        RV=v.reindex(key).to_numpy(),
    )

    missing = int(df["RV"].isna().sum())
    if missing:
        raise RuntimeError(f"{missing:,} takes have a count with no run value")

    df["RUNS_WTD"] = df["RV"] * df["RESID"]
    df["RUNS_FLAT"] = FLAT_RUN_VALUE * df["RESID"]
    df["VAR_WTD"] = df["RV"] ** 2 * df["PQ"]
    df["VAR_FLAT"] = FLAT_RUN_VALUE ** 2 * df["PQ"]

    g = df.groupby("CATCHER_ID")
    tab = pd.DataFrame({
        "N": g.size(),
        "RUNS_FLAT": g["RUNS_FLAT"].sum(),
        "RUNS_WTD": g["RUNS_WTD"].sum(),
        "VAR_FLAT": g["VAR_FLAT"].sum(),
        "VAR_WTD": g["VAR_WTD"].sum(),
    })
    return tab[tab["N"] >= MIN_TAKES].copy()


def shrink_runs(tab, label):
    """Partial pooling on the runs scale, strength estimated as in L3b.

    The mean shrinkage factor is also the reliability of the estimate, which
    matters for the Savant comparison: a correlation against any external
    measure attenuates by roughly the square root of reliability, so a noisier
    metric correlates worse for reasons that have nothing to do with being
    wrong.
    """
    n = tab["N"].to_numpy(float)
    d = tab[f"RUNS_{label}"].to_numpy() / n
    v = tab[f"VAR_{label}"].to_numpy() / n ** 2

    observed = float(np.var(d, ddof=1))
    noise = float(v.mean())
    tau2 = max(0.0, observed - noise)
    w = tau2 / (tau2 + v) if tau2 > 0 else np.zeros_like(v)

    tab[f"SHRINK_{label}"] = w
    tab[f"RUNS_{label}_SHRUNK"] = w * d * n
    print(f"\n  {label.lower()} run value")
    print(f"    observed variance   {observed:.3e}")
    print(f"    sampling variance   {noise:.3e}")
    print(f"    tau^2               {tau2:.3e}"
          f"   sqrt = {np.sqrt(tau2):.5f} runs per take")
    if tau2 > 0:
        print(f"    signal-to-noise     {tau2 / noise:.2f}")
        print(f"    mean reliability    {w.mean():.3f}"
              f"   (attenuates an external correlation by "
              f"{np.sqrt(w.mean()):.3f})")
        print(f"    one SD over a median season "
              f"({int(np.median(n)):,} takes): "
              f"{np.sqrt(tau2) * np.median(n):.1f} runs")
        print(f"    shrinkage {w.min():.3f} to {w.max():.3f}")
    return tab


def sensitivity(tab):
    """How much does the count weighting actually move the leaderboard?"""
    flat = tab["RUNS_FLAT_SHRUNK"]
    wtd = tab["RUNS_WTD_SHRUNK"]

    r_flat = flat.rank(ascending=False)
    r_wtd = wtd.rank(ascending=False)
    moves = (r_wtd - r_flat).abs()

    print(f"\n  flat {FLAT_RUN_VALUE} against count-weighted")
    print(f"    correlation of values      {np.corrcoef(flat, wtd)[0, 1]:.4f}")
    print(f"    rank correlation           "
          f"{flat.rank().corr(wtd.rank(), method='spearman'):.4f}")
    print(f"    mean absolute rank change  {moves.mean():.2f} places")
    print(f"    largest rank change        {moves.max():.0f} places")
    print(f"    catchers moving 5+ places  {int((moves >= 5).sum())} of {len(tab)}")
    print(f"    sum over qualified, flat       {flat.sum():+.1f}")
    print(f"    sum over qualified, weighted   {wtd.sum():+.1f}")
    print("  that sum is near zero by construction and is not a league total")

    tab["RANK_FLAT"] = r_flat.astype(int)
    tab["RANK_WTD"] = r_wtd.astype(int)
    tab["RANK_MOVE"] = (r_wtd - r_flat).astype(int)
    return tab


# --- Savant comparison ------------------------------------------------------

SAVANT_ID_CANDIDATES = ("id", "player_id", "entity_id", "catcher", "mlbam_id")
SAVANT_RUNS_CANDIDATES = (
    "rv_tot", "runs_extra_strikes", "catcher_framing_runs", "framing_runs",
    "runs", "strike_rate_runs", "extra_strikes_runs",
)
SAVANT_PITCHES_CANDIDATES = ("pitches", "n_called_pitches", "n_pitches")
SAVANT_STRIKES_CANDIDATES = ("extra_strikes", "strikes_above_average", "n_extra_strikes")


def _pick(columns, candidates, what):
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand in lower:
            return lower[cand]
    return None


def savant_comparison(mine):
    """Compare against Savant's published leaderboard.

    Correlation validates. Explained disagreement is the part worth writing up,
    because the two methods differ in ways that predict who should move:

      - Savant evaluates the shadow zone only — pitches near the edge — while
        this analysis uses every take and lets the model handle the easy ones.
      - Different population: our takes filter drops blocked_ball and
        null-location rows, which Savant's called-pitch count appears to keep.
        The pitch-count ratio below measures that rather than assuming it.
      - Different baseline: theirs conditions on its own zone model, ours on a
        gradient-boosted fit over location, count and handedness.
      - Ours offers a staff adjustment; theirs does not.

    The attribution tests two of those against the data. Shadow-zone exposure
    is not directly observable here, but it has a signature: a catcher whose
    staff locates unusually well OR unusually badly has an unusual share of his
    takes inside the window Savant evaluates. That predicts disagreement
    tracking the DISTANCE of staff quality from the league mean, not its level —
    an earlier version of this function compared means and found nothing,
    because a set holding both extremes averages back to the middle.
    """
    if not SAVANT_CSV.exists():
        print(f"\n  {SAVANT_CSV.name} not found — skipping the comparison.")
        print("  Download from baseballsavant.mlb.com/catcher_framing")
        print(f"  (season 2026, minimum {MIN_TAKES} pitches) and save it there.")
        return None

    sav = pd.read_csv(SAVANT_CSV)
    print(f"\n  {SAVANT_CSV.name}: {len(sav)} rows")
    print(f"  columns: {list(sav.columns)}")

    id_col = _pick(sav.columns, SAVANT_ID_CANDIDATES, "player id")
    runs_col = _pick(sav.columns, SAVANT_RUNS_CANDIDATES, "runs")
    if runs_col is None:
        runs_col = _pick(sav.columns, SAVANT_STRIKES_CANDIDATES, "strikes")

    if id_col is None or runs_col is None:
        print("\n  could not identify the id and runs columns automatically.")
        print("  Add the real names to SAVANT_ID_CANDIDATES / "
              "SAVANT_RUNS_CANDIDATES and re-run.")
        return None

    print(f"  using id column '{id_col}' and value column '{runs_col}'")

    pitch_col = _pick(sav.columns, SAVANT_PITCHES_CANDIDATES, "pitches")
    keep = [id_col, runs_col] + ([pitch_col] if pitch_col else [])
    rename = {id_col: "CATCHER_ID", runs_col: "SAVANT"}
    if pitch_col:
        rename[pitch_col] = "SAVANT_PITCHES"
    sav = sav[keep].rename(columns=rename)

    sav["CATCHER_ID"] = pd.to_numeric(sav["CATCHER_ID"], errors="coerce")
    sav = sav.dropna(subset=["CATCHER_ID"])
    sav["CATCHER_ID"] = sav["CATCHER_ID"].astype(int)

    joined = mine.reset_index().merge(sav, on="CATCHER_ID", how="inner")
    print(f"\n  matched {len(joined)} of {len(mine)} qualified catchers")
    if len(joined) < 10:
        print("  too few matches to correlate — check the id column")
        return None

    from scipy.stats import spearmanr

    print()
    for col, label in [("RUNS_WTD_SHRUNK", "count-weighted, shrunk"),
                       ("RUNS_FLAT_SHRUNK", "flat, shrunk"),
                       ("SAE_ADJ", "staff-adjusted strikes")]:
        if col not in joined:
            continue
        rho, p = spearmanr(joined[col], joined["SAVANT"])
        r = float(np.corrcoef(joined[col], joined["SAVANT"])[0, 1])
        print(f"    {label:<28}  Spearman {rho:+.3f} (p={p:.2g})   "
              f"Pearson {r:+.3f}")

    # Any gap between the flat and weighted correlations has to clear the
    # attenuation the weighted metric's extra noise already predicts, before it
    # can be read as evidence about how Savant weights counts.
    if {"SHRINK_FLAT", "SHRINK_WTD"} <= set(joined.columns):
        predicted = float(
            np.sqrt(joined["SHRINK_FLAT"].mean() / joined["SHRINK_WTD"].mean())
        )
        print(f"\n  attenuation floor: the weighted metric is noisier, which")
        print(f"  alone predicts the flat correlation running {predicted:.3f}x")
        print(f"  higher. Only a gap clearly beyond that says anything about")
        print(f"  Savant's own run-value weighting.")

    if "SAVANT_PITCHES" in joined:
        ratio = joined["SAVANT_PITCHES"] / joined["N"]
        print(f"\n  population difference")
        print(f"    their called pitches / our takes:  "
              f"median {ratio.median():.3f}, "
              f"range {ratio.min():.3f} to {ratio.max():.3f}")
        print("  our takes filter drops blocked_ball — 14,894 league-wide, or")
        print("  4.0% of takes-plus-blocked — and null-location rows. A ratio")
        print("  near 1.04 means that exclusion IS the population difference,")
        print("  measured rather than asserted.")

    joined["RANK_MINE"] = joined["RUNS_WTD_SHRUNK"].rank(ascending=False)
    joined["RANK_SAVANT"] = joined["SAVANT"].rank(ascending=False)
    joined["DISAGREE"] = joined["RANK_SAVANT"] - joined["RANK_MINE"]
    joined["ABS_DISAGREE"] = joined["DISAGREE"].abs()

    center = float(joined["EXP_RATE"].mean()) if "EXP_RATE" in joined else None
    if center is not None:
        joined["STAFF_DEV"] = (joined["EXP_RATE"] - center).abs()

    cols = ["NAME", "N", "EXP_RATE", "STAFF_DEV", "RUNS_WTD_SHRUNK", "SAVANT",
            "RANK_MINE", "RANK_SAVANT", "DISAGREE", "RANK_MOVE"]
    cols = [c for c in cols if c in joined]
    ordered = joined.sort_values("DISAGREE")

    print(f"\n  they rate these catchers much higher than we do")
    print(ordered[cols].head(8).to_string(
        index=False, float_format=lambda x: f"{x:.3f}"))
    print(f"\n  we rate these much higher than they do")
    print(ordered[cols].tail(8).to_string(
        index=False, float_format=lambda x: f"{x:.3f}"))

    _attribution(joined, center, spearmanr)
    _scatter(joined)
    return joined


def _attribution(joined, center, spearmanr):
    """Which explanation tracks the disagreement?

    Correlations across all catchers rather than a split at a threshold,
    because a split throws away most of the signal and invites an arbitrary
    cutoff. The split is printed afterwards for readability only.
    """
    print(f"\n  what tracks the disagreement, across all {len(joined)} catchers")
    print(f"  (|rank gap| against each candidate explanation)")

    tests = []
    if "STAFF_DEV" in joined:
        tests.append((joined["STAFF_DEV"],
                      "distance of staff quality from league mean"))
    if "RANK_MOVE" in joined:
        tests.append((joined["RANK_MOVE"].abs(),
                      "sensitivity to run-value weighting"))
    tests.append((joined["N"], "sample size (a null check — should be weak)"))

    for series, label in tests:
        rho, p = spearmanr(joined["ABS_DISAGREE"], series)
        print(f"    {label:<48}  Spearman {rho:+.3f} (p={p:.3f})")

    big = joined[joined["ABS_DISAGREE"] >= DISAGREE_THRESHOLD]
    rest = joined.loc[~joined.index.isin(big.index)]
    if len(big) and center is not None:
        print(f"\n  the {len(big)} catchers moving {DISAGREE_THRESHOLD}+ places "
              f"against the other {len(rest)}")
        print(f"    mean staff quality             "
              f"{big['EXP_RATE'].mean():.4f}  vs  {rest['EXP_RATE'].mean():.4f}")
        print(f"    mean |staff quality - league|  "
              f"{big['STAFF_DEV'].mean():.4f}  vs  {rest['STAFF_DEV'].mean():.4f}")
        if "RANK_MOVE" in joined:
            print(f"    mean |weighting move|          "
                  f"{big['RANK_MOVE'].abs().mean():.1f}  vs  "
                  f"{rest['RANK_MOVE'].abs().mean():.1f} places")

    print("\n  reading these: the MEAN of staff quality is the wrong test, and")
    print("  an earlier version of this function used it and found nothing —")
    print("  a set holding both the highest and lowest staff averages back to")
    print("  the middle. Extremity is the hypothesis, because Savant scores")
    print("  only pitches near the zone edge, so a catcher whose staff locates")
    print("  unusually well or badly has an unusual share of his takes inside")
    print("  the window they evaluate. Sensitivity to run-value weighting")
    print("  points instead at the two methods valuing counts differently.")


def _scatter(joined):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:                                  # noqa: BLE001
        print(f"\n  no scatter ({type(exc).__name__}) — install matplotlib")
        return

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.axhline(0, lw=0.6, color="0.7")
    ax.axvline(0, lw=0.6, color="0.7")
    ax.scatter(joined["RUNS_WTD_SHRUNK"], joined["SAVANT"], s=28, alpha=0.75)

    for row in joined.itertuples():
        label = getattr(row, "NAME", str(row.CATCHER_ID))
        ax.annotate(str(label).split()[-1],
                    (row.RUNS_WTD_SHRUNK, row.SAVANT),
                    fontsize=7, alpha=0.8,
                    xytext=(3, 3), textcoords="offset points")

    lo = min(joined["RUNS_WTD_SHRUNK"].min(), joined["SAVANT"].min())
    hi = max(joined["RUNS_WTD_SHRUNK"].max(), joined["SAVANT"].max())
    pad = 0.08 * (hi - lo)
    ax.plot([lo - pad, hi + pad], [lo - pad, hi + pad],
            lw=0.8, ls="--", color="0.5")
    ax.set_xlim(lo - pad, hi + pad)
    ax.set_ylim(lo - pad, hi + pad)
    ax.set_aspect("equal")
    ax.set_xlabel("this analysis — framing runs, count-weighted and shrunk")
    ax.set_ylabel("Baseball Savant — published framing runs")
    ax.set_title("2026 catcher framing: two methods")
    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=150)
    print(f"\n  wrote {OUT_PNG.name}")


# --- main -------------------------------------------------------------------

def main():
    print("loading FCT_TAKES ...")
    df = load_takes()
    names = catcher_names(df["CATCHER_ID"].unique())

    print(f"\n{'=' * 70}")
    print("1. RUN VALUE OF A CALLED STRIKE, BY COUNT")
    print(f"{'=' * 70}")
    values, pooled = count_run_values()

    print(f"\n{'=' * 70}")
    print("2. OUT-OF-FOLD EXPECTATIONS")
    print(f"{'=' * 70}")
    df = df.assign(P_OOF=out_of_fold_predictions(df, MAX_ITER, USE_ISOTONIC))

    print(f"\n{'=' * 70}")
    print("3. FRAMING RUNS PER CATCHER")
    print(f"{'=' * 70}")
    runs = framing_runs(df, values)
    runs = shrink_runs(runs, "FLAT")
    runs = shrink_runs(runs, "WTD")
    runs = sensitivity(runs)

    # Carry the L3b strike-scale columns alongside, so the comparison and the
    # attribution have staff quality and the adjusted strikes to work with.
    # Note that the runs columns are NOT staff-adjusted; EXP_RATE travels with
    # them so the adjustment can be applied, and the README has to say which
    # leaderboard it is publishing.
    strikes = catcher_table(df, names)
    shrunk, *_ = shrink(strikes)
    adj, _ = staff_adjusted(shrunk)
    runs = runs.join(
        adj[["NAME", "EXP_RATE", "SAE", "SAE_SHRUNK", "SAE_ADJ"]], how="left"
    ).sort_values("RUNS_WTD_SHRUNK", ascending=False)

    cols = ["NAME", "N", "EXP_RATE", "RUNS_FLAT_SHRUNK", "RUNS_WTD_SHRUNK",
            "RANK_FLAT", "RANK_WTD", "RANK_MOVE"]
    print(f"\n  top 15, count-weighted framing runs")
    print(runs[cols].head(15).to_string(
        index=False, float_format=lambda x: f"{x:.2f}"))
    print(f"\n  bottom 10")
    print(runs[cols].tail(10).to_string(
        index=False, float_format=lambda x: f"{x:.2f}"))

    runs.to_csv(OUT_CSV)
    print(f"\n  wrote {OUT_CSV.name} ({len(runs)} catchers)")

    print(f"\n{'=' * 70}")
    print("4. SAVANT COMPARISON")
    print(f"{'=' * 70}")
    savant_comparison(runs)


if __name__ == "__main__":
    argparse.ArgumentParser(description=__doc__).parse_args()
    main()