"""Export the Streamlit app's data as committed CSVs.

Streamlit Community Cloud cannot reach this BigQuery project — there are no
credentials there and putting a service-account key in repo secrets to serve a
read-only public dashboard would be the wrong trade. So the app reads flat files
committed alongside it, and this script produces them.

The cost of that choice, stated plainly: the app is a snapshot, not a live view,
and goes stale the moment FCT_TAKES is rebuilt. The pipeline refreshes daily and
the app does not. Re-run this and commit the output to update it.

Run:
    OMP_NUM_THREADS=1 python3 analysis/export_app_data.py

Writes app/data/leaderboard.csv, app/data/zone_league.csv and
app/data/zone_catcher.csv.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from model_framing import MIN_TAKES, catcher_names, load_takes, out_of_fold_predictions

MAX_ITER = 100
USE_ISOTONIC = False

# Display window, in 0.25-ft bin indices. The plate is 1.42 ft wide and the
# league zone runs 1.62-3.21 ft, so +/-2 ft horizontally and 0.5-4.5 ft
# vertically covers every pitch anyone frames and clips the long tail of
# wild ones that would otherwise flatten the colour scale.
X_BIN_RANGE = (-8, 8)
Z_BIN_RANGE = (2, 18)

# A cell needs this many pitches before it is shown. The README's per-cell
# noise limitation is the reason: at ~50 pitches a cell's rate carries about
# 7 points of standard error, so a thin cell is a colour with no content.
MIN_CELL = 25

OUT_DIR = Path(__file__).resolve().parents[1] / "app" / "data"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("loading FCT_TAKES ...")
    df = load_takes()
    names = catcher_names(df["CATCHER_ID"].unique())

    print("out-of-fold expectations ...")
    df = df.assign(P_OOF=out_of_fold_predictions(df, MAX_ITER, USE_ISOTONIC))

    df = df[
        df["X_BIN"].between(*X_BIN_RANGE) & df["Z_BIN"].between(*Z_BIN_RANGE)
    ].copy()
    df["NAME"] = [names.get(int(i), str(i)) for i in df["CATCHER_ID"]]

    # --- league surface ----------------------------------------------------
    league = (
        df.groupby(["X_BIN", "Z_BIN"])
        .agg(N=("Y", "size"), STRIKES=("Y", "sum"), EXPECTED=("P_OOF", "sum"))
        .reset_index()
    )
    league = league[league["N"] >= MIN_CELL]
    league["RATE"] = league["STRIKES"] / league["N"]
    league.to_csv(OUT_DIR / "zone_league.csv", index=False)
    print(f"  zone_league.csv      {len(league):,} cells")

    # --- per-catcher surface -----------------------------------------------
    qualified = df.groupby("CATCHER_ID").size()
    qualified = qualified[qualified >= MIN_TAKES].index
    sub = df[df["CATCHER_ID"].isin(qualified)]

    per_catcher = (
        sub.groupby(["CATCHER_ID", "NAME", "X_BIN", "Z_BIN"])
        .agg(N=("Y", "size"), STRIKES=("Y", "sum"), EXPECTED=("P_OOF", "sum"))
        .reset_index()
    )
    per_catcher = per_catcher[per_catcher["N"] >= MIN_CELL]
    per_catcher["SAE"] = per_catcher["STRIKES"] - per_catcher["EXPECTED"]
    per_catcher["SAE_PER_100"] = 100 * per_catcher["SAE"] / per_catcher["N"]
    per_catcher.to_csv(OUT_DIR / "zone_catcher.csv", index=False)
    print(f"  zone_catcher.csv     {len(per_catcher):,} cells, "
          f"{per_catcher['CATCHER_ID'].nunique()} catchers")

    # --- leaderboard -------------------------------------------------------
    # Produced by framing_runs.py; copied here so the app has one data
    # directory rather than reaching back into analysis/.
    src = Path(__file__).with_name("framing_runs_leaderboard.csv")
    if not src.exists():
        raise SystemExit(
            f"{src.name} not found — run framing_runs.py first"
        )
    board = pd.read_csv(src)
    keep = [c for c in [
        "CATCHER_ID", "NAME", "N", "EXP_RATE",
        "RUNS_FLAT_SHRUNK", "RUNS_WTD_SHRUNK", "RANK_FLAT", "RANK_WTD",
        "SAE", "SAE_SHRUNK", "SAE_ADJ",
    ] if c in board.columns]
    board[keep].to_csv(OUT_DIR / "leaderboard.csv", index=False)
    print(f"  leaderboard.csv      {len(board)} catchers")

    print(f"\n  wrote to {OUT_DIR}")
    print("  commit these three files — Community Cloud reads them from git")


if __name__ == "__main__":
    main()