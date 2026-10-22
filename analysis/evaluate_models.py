import argparse

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.calibration import calibration_curve
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

GCP_PROJECT = "mlb-catcher-framing"

BIN_SIZE = 0.25
ZONE_BOT = 1.62
ZONE_TOP = 3.21

TEST_FRACTION = 0.20
RANDOM_STATE = 42
CAL_BINS = 10


# --- data -------------------------------------------------------------------

def load_takes():
    
    client = bigquery.Client(project=GCP_PROJECT)
    sql = f"""
        SELECT
            GAME_PK,
            GAME_DATE,
            CATCHER_ID,
            PLATE_X,
            PLATE_Z_NORM,
            BATTER_STANDS,
            PITCHER_THROWS,
            BALLS,
            STRIKES,
            CAST(IS_CALLED_STRIKE AS INT64) AS Y
        FROM `{GCP_PROJECT}.DBT_DEV.FCT_TAKES`
    """
    df = client.query(sql).to_dataframe()

    # Vertical location back onto a feet scale, matching the dbt model.
    df["PLATE_Z_FT"] = df["PLATE_Z_NORM"] * (ZONE_TOP - ZONE_BOT) + ZONE_BOT

    # Integer bin indices — never join or group on floats.
    df["X_BIN"] = np.round(df["PLATE_X"] / BIN_SIZE).astype(int)
    df["Z_BIN"] = np.round(df["PLATE_Z_FT"] / BIN_SIZE).astype(int)

    return df


def split_by_game(df):
    """80/20 split with whole games on one side or the other.

    Pitches within a game share umpire, catcher, park and tracking calibration.
    A random pitch-level split would put those shared conditions on both sides,
    and the model would be scored on context it had already seen.
    """
    splitter = GroupShuffleSplit(
        n_splits=1, test_size=TEST_FRACTION, random_state=RANDOM_STATE
    )
    train_idx, test_idx = next(splitter.split(df, groups=df["GAME_PK"]))
    return df.iloc[train_idx].copy(), df.iloc[test_idx].copy()


# --- the jury ---------------------------------------------------------------

def evaluate(name, y_true, y_pred, note=""):
    y_pred = np.clip(y_pred, 1e-15, 1 - 1e-15)  # log loss is undefined at 0/1

    ll = log_loss(y_true, y_pred)
    auc = roc_auc_score(y_true, y_pred)

    # Quantile bins: predictions cluster heavily near 0 and 1, and uniform
    # bins would leave the middle almost empty.
    prob_true, prob_pred = calibration_curve(
        y_true, y_pred, n_bins=CAL_BINS, strategy="quantile"
    )
    max_cal_error = np.max(np.abs(prob_true - prob_pred))

    print(f"\n{'=' * 64}")
    print(f"{name}")
    if note:
        print(f"  {note}")
    print(f"{'-' * 64}")
    print(f"  n             {len(y_true):,}")
    print(f"  log loss      {ll:.5f}   (lower is better)")
    print(f"  AUC           {auc:.5f}   (higher is better)")
    print(f"  max cal error {max_cal_error:.4f}")
    print(f"\n  calibration ({CAL_BINS} quantile bins)")
    print(f"    {'predicted':>10}  {'actual':>10}  {'gap':>8}")
    for p, a in zip(prob_pred, prob_true):
        print(f"    {p:>10.4f}  {a:>10.4f}  {a - p:>+8.4f}")

    return {
        "model": name,
        "n": len(y_true),
        "log_loss": ll,
        "auc": auc,
        "max_cal_error": max_cal_error,
    }


# --- models -----------------------------------------------------------------

def fit_bins(df):
    """League called-strike rate per location bin."""
    return df.groupby(["X_BIN", "Z_BIN"])["Y"].mean()


def predict_bins(bin_means, df, fallback):
    key = pd.MultiIndex.from_arrays([df["X_BIN"], df["Z_BIN"]])
    preds = bin_means.reindex(key)
    n_missing = int(preds.isna().sum())
    return preds.fillna(fallback).to_numpy(), n_missing


def features_raw(df):
    return df[["PLATE_X", "PLATE_Z_FT"]].to_numpy()


def features_distance(df):
    z_centre = (ZONE_TOP + ZONE_BOT) / 2
    return np.column_stack([
        np.abs(df["PLATE_X"].to_numpy()),
        np.abs(df["PLATE_Z_FT"].to_numpy() - z_centre),
    ])


FEATURE_SETS = {"raw": features_raw, "distance": features_distance}


# --- main -------------------------------------------------------------------

def main(feature_set):
    print("loading FCT_TAKES ...")
    df = load_takes()
    train, test = split_by_game(df)

    print(f"\n  all      {len(df):,} pitches, {df['GAME_PK'].nunique():,} games")
    print(f"  train    {len(train):,} pitches, {train['GAME_PK'].nunique():,} games")
    print(f"  test     {len(test):,} pitches, {test['GAME_PK'].nunique():,} games")
    print(f"  base rate: train {train['Y'].mean():.4f}, test {test['Y'].mean():.4f}")

    results = []
    base_rate = train["Y"].mean()

    results.append(evaluate(
        "Constant (train base rate)",
        test["Y"],
        np.full(len(test), base_rate),
        note=f"always predicts {base_rate:.4f}",
    ))

    # --- the sinful number --------------------------------------------------
    bins_all = fit_bins(df)
    preds_all, _ = predict_bins(bins_all, df, df["Y"].mean())
    results.append(evaluate(
        "Binned — LEAKY (fit on all, scored on all)",
        df["Y"],
        preds_all,
        note="every pitch contributed to its own prediction",
    ))

    # --- the honest number --------------------------------------------------
    bins_train = fit_bins(train)
    preds_test, n_missing = predict_bins(bins_train, test, base_rate)
    results.append(evaluate(
        "Binned — honest (fit on train, scored on held-out games)",
        test["Y"],
        preds_test,
        note=f"{n_missing:,} test pitches ({n_missing / len(test):.2%}) "
             f"fell in bins unseen in training",
    ))

    hard = bins_train[(bins_train == 0.0) | (bins_train == 1.0)]
    key = pd.MultiIndex.from_arrays([test["X_BIN"], test["Z_BIN"]])
    in_hard = key.isin(hard.index)
    blowups = int((
        (test["Y"].to_numpy() == 1) & in_hard &
        (preds_test == 0.0)
    ).sum() + (
        (test["Y"].to_numpy() == 0) & in_hard &
        (preds_test == 1.0)
    ).sum())
    print(f"\n  saturated train bins: {len(hard):,}")
    print(f"  test pitches contradicting a saturated bin: {blowups}")
    print(f"  their cost at the 1e-15 clip: "
          f"{blowups * 34.54 / len(test):.5f} of the mean log loss")

    # --- apples to apples ---------------------------------------------------
    # The two numbers above differ in both what was fit AND what was scored.
    # This one changes only the fitting, on identical rows, which isolates the
    # leakage rather than mixing it with a change of evaluation set.
    preds_leaky_on_test, _ = predict_bins(bins_all, test, df["Y"].mean())
    results.append(evaluate(
        "Binned — leaky bins scored on the test rows only",
        test["Y"],
        preds_leaky_on_test,
        note="isolates the leakage: same rows, only the fitting differs",
    ))

        # --- attribute the gap, don't predict it --------------------------------
    counts = df.groupby(["X_BIN", "Z_BIN"])["Y"].agg(n="size", k="sum")
    counts["minority"] = np.minimum(counts["k"], counts["n"] - counts["k"])

    key = pd.MultiIndex.from_arrays([test["X_BIN"], test["Z_BIN"]])
    y = test["Y"].to_numpy()

    def rowwise(p):
        p = np.clip(np.asarray(p, dtype=float), 1e-15, 1 - 1e-15)
        return -(y * np.log(p) + (1 - y) * np.log(1 - p))

    delta = rowwise(preds_test) - rowwise(preds_leaky_on_test)
    m = counts["minority"].reindex(key).fillna(-1).to_numpy()

    print(f"\n{'=' * 64}")
    print("WHERE THE GAP COMES FROM   (honest - leaky, identical rows)")
    print(f"{'-' * 64}")
    print(f"  {'band':>16}  {'rows':>8}  {'nats':>10}  {'share':>7}")
    for label, sel in [
        ("unseen in train", m < 0),
        ("minority = 0", m == 0),
        ("minority 1-4", (m >= 1) & (m <= 4)),
        ("minority >= 5", m >= 5),
    ]:
        nats = delta[sel].sum()
        print(f"  {label:>16}  {sel.sum():>8,}  {nats:>10.1f}  "
              f"{nats / delta.sum():>6.1%}")

    print(f"\n  total  {delta.sum():.1f} nats  "
          f"({delta.sum() / len(test):.5f} mean log loss)")
    worst = np.sort(delta)[-5:][::-1]
    print(f"  five largest single rows: "
          f"{', '.join(f'{w:.1f}' for w in worst)}")

    # --- first real model ---------------------------------------------------
    build = FEATURE_SETS[feature_set]
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
    model.fit(build(train), train["Y"])
    results.append(evaluate(
        f"Logistic regression ({feature_set} features)",
        test["Y"],
        model.predict_proba(build(test))[:, 1],
        note=f"features: {feature_set}",
    ))

    # --- summary ------------------------------------------------------------
    print(f"\n\n{'=' * 64}")
    print("SUMMARY")
    print(f"{'=' * 64}")
    summary = pd.DataFrame(results)[["model", "n", "log_loss", "auc"]]
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.5f}"))

    leaky = next(r for r in results if "LEAKY" in r["model"])
    honest = next(r for r in results if "honest" in r["model"])
    isolated = next(r for r in results if "test rows only" in r["model"])
    print(f"\n  leakage gap (as quoted):  "
          f"{honest['log_loss'] - leaky['log_loss']:+.5f} log loss")
    print(f"  leakage gap (isolated):   "
          f"{honest['log_loss'] - isolated['log_loss']:+.5f} log loss")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--features",
        choices=sorted(FEATURE_SETS),
        default="raw",
        help="feature shape for the logistic regression",
    )
    main(parser.parse_args().features)