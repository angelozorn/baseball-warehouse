"""The smooth baseline, the shrunk catcher, and the chance test.

Four things, in the order they have to happen:

  1. A gradient-boosted baseline through the same jury as L3a, with its
     calibration checked rather than assumed, because every number downstream
     is a sum of its probabilities.
  2. A permutation test that asks whether the catcher spread is bigger than
     luck — run before the leaderboard is read, not after.
  3. Per-catcher strikes above expected, shrunk by a factor estimated from the
     data rather than chosen by taste.
  4. The same thing again with pitching-staff quality partialled out, because
     catcher identity and staff identity are nearly the same variable.

Run:
    OMP_NUM_THREADS=1 python3 analysis/model_framing.py
    OMP_NUM_THREADS=1 python3 analysis/model_framing.py --permutations 5000

OMP_NUM_THREADS=1 is not decoration. HistGradientBoostingClassifier builds its
histograms in parallel, and floating-point summation order varies with thread
scheduling, so two runs on the same data with the same seed produced log losses
of 0.11848 and 0.11857 and moved individual catchers by about a strike. Pinning
to one thread is slower and makes the committed results file reproducible
evidence rather than one sample from a distribution.

The jury and the data pull are imported from evaluate_models rather than
reimplemented. Nothing in this project is compared across two pieces of
evaluation code.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss
from sklearn.model_selection import GroupKFold, GroupShuffleSplit

from evaluate_models import evaluate, fit_bins, load_takes, predict_bins, split_by_game

RANDOM_STATE = 42

# TRAIN is split again, by game, into the rows the model is fit on and the rows
# used to choose max_iter and to fit the recalibrator. TEST is never touched
# until the jury at the end.
CAL_FRACTION = 0.20

MAX_ITER_GRID = (50, 100, 200, 400)

N_ATTRIBUTION_FOLDS = 5
N_PERMUTATIONS = 1000
N_STRATA = 20

# Same qualification threshold as the Tableau views and the README table, so
# the leaderboard here is comparable to the one already published.
MIN_TAKES = 3000

# The band where a call is genuinely in doubt, and therefore the only band
# framing can happen in. Everything outside it is a near-certain call.
BAND_LO, BAND_HI = 0.05, 0.95

# Catchers with an outside reputation to check the method against. Bailey is the
# load-bearing one: the README cites him as the consensus best framer in
# baseball, so where he lands is the closest thing this project has to an
# external validation of its own baseline.
NAMED_CHECKS = ("Bailey", "Contreras", "Heim")

CLIP = 1e-15

NAME_CACHE = Path(__file__).with_name("catcher_names.csv")


# --- names ------------------------------------------------------------------

def catcher_names(ids):
    """Map MLBAM ids to names, from a committed cache or one network lookup.

    A leaderboard of integers cannot be checked against anything, and the whole
    value of Bailey's row is that someone outside this project has an opinion
    about it.

    The cache is written on first run and committed, so later runs — and anyone
    with a clone — produce byte-identical output with no network call. That
    matters because the results file is the evidence behind the README, and
    evidence that changes depending on whether a lookup service answered is not
    evidence. The file is also the dbt seed the README has been promising;
    moving it to transform/seeds/CATCHER_NAMES.csv is the permanent fix.

    Hand edits to the cache survive, which is how "Mccann" became "McCann" —
    str.title() does not know about intercapped surnames.
    """
    wanted = sorted(int(i) for i in ids)

    if NAME_CACHE.exists():
        cached = pd.read_csv(NAME_CACHE)
        mapping = dict(zip(cached["CATCHER_ID"].astype(int), cached["NAME"]))
        if set(wanted) <= set(mapping):
            print(f"  names from {NAME_CACHE.name} ({len(mapping)} catchers)")
            return mapping
        print(f"  {NAME_CACHE.name} is missing "
              f"{len(set(wanted) - set(mapping))} ids; looking them up again")

    try:
        from pybaseball import playerid_reverse_lookup
        found = playerid_reverse_lookup(wanted, key_type="mlbam")
    except Exception as exc:                                  # noqa: BLE001
        print(f"  name lookup unavailable ({type(exc).__name__}: {exc})")
        print("  leaderboard keeps raw ids")
        return {}

    found = found.dropna(subset=["key_mlbam"])
    mapping = {
        int(row.key_mlbam):
            f"{str(row.name_first).title()} {str(row.name_last).title()}"
        for row in found.itertuples()
    }
    missing = [i for i in wanted if i not in mapping]
    if missing:
        print(f"  no name found for {len(missing)} ids: {missing}")

    pd.DataFrame(
        {"CATCHER_ID": list(mapping), "NAME": list(mapping.values())}
    ).sort_values("NAME").to_csv(NAME_CACHE, index=False)
    print(f"  wrote {NAME_CACHE.name} ({len(mapping)} catchers) — commit it")
    return mapping


# --- features ---------------------------------------------------------------

FEATURES = ["PLATE_X", "PLATE_Z_FT", "BALLS", "STRIKES", "STANDS_R", "THROWS_R"]


def build_features(df):
    """Location, count and handedness.

    PLATE_Z_FT rather than PLATE_Z_NORM is cosmetic here: the two differ by an
    affine rescale, and a tree's splits are invariant to that. It is used so the
    thresholds a reader inspects are in feet, matching every other figure in the
    project.

    Handedness is encoded as two binary columns rather than declared as
    categorical. Both variables have two levels, so a split on 0/1 is exactly a
    split on the category, and the simpler encoding has no configuration to get
    wrong.
    """
    X = pd.DataFrame(index=df.index)
    X["PLATE_X"] = df["PLATE_X"].astype(float)
    X["PLATE_Z_FT"] = df["PLATE_Z_FT"].astype(float)
    X["BALLS"] = df["BALLS"].astype(float)
    X["STRIKES"] = df["STRIKES"].astype(float)
    X["STANDS_R"] = (df["BATTER_STANDS"] == "R").astype(float)
    X["THROWS_R"] = (df["PITCHER_THROWS"] == "R").astype(float)
    return X[FEATURES].to_numpy()


def check_features(df):
    """Nulls in a feature column become a silent 'L' under `== "R"`. Say so."""
    problems = []
    for col in ["PLATE_X", "PLATE_Z_FT", "BALLS", "STRIKES",
                "BATTER_STANDS", "PITCHER_THROWS"]:
        n = int(df[col].isna().sum())
        if n:
            problems.append(f"{col}: {n:,} null")
    if problems:
        print("  feature nulls — handedness nulls silently read as 'L':")
        for p in problems:
            print(f"    {p}")
    else:
        print("  no nulls in any feature column")


# --- the model --------------------------------------------------------------

def make_gbm(max_iter):
    """A deliberately plain HistGradientBoostingClassifier.

    Three departures from the defaults, each for a reason:

    early_stopping=False — sklearn's automatic early stopping carves a RANDOM
    validation slice out of the training rows, which puts pitches from the same
    game on both sides of it. That is exactly the leakage the grouped split
    exists to prevent, applied inside the estimator where it would not show up
    in any reported split. max_iter is chosen on a grouped split instead.

    min_samples_leaf=200 rather than 20 — the zone edge is where leaves get
    thin, and the edge is the only band framing lives in. A leaf of 20 pitches
    there estimates a called-strike rate to about +/-11 points, which is noise
    this model is supposed to be pooling away, not reproducing.

    l2_regularization=1.0 rather than 0 — mild, same motive.

    random_state does NOT make this deterministic across runs. See the module
    docstring on OMP_NUM_THREADS.
    """
    return HistGradientBoostingClassifier(
        max_iter=max_iter,
        learning_rate=0.1,
        max_leaf_nodes=31,
        min_samples_leaf=200,
        l2_regularization=1.0,
        early_stopping=False,
        random_state=RANDOM_STATE,
    )


def split_by_game_fraction(df, fraction, seed):
    """Hold out whole games, as split_by_game does, at an arbitrary fraction."""
    splitter = GroupShuffleSplit(n_splits=1, test_size=fraction, random_state=seed)
    a, b = next(splitter.split(df, groups=df["GAME_PK"]))
    return df.iloc[a].copy(), df.iloc[b].copy()


def choose_max_iter(fit_df, cal_df):
    """Pick max_iter by log loss on held-out games, not by the default."""
    print(f"\n  {'max_iter':>9}  {'CAL log loss':>13}")
    scores = {}
    for n in MAX_ITER_GRID:
        model = make_gbm(n).fit(build_features(fit_df), fit_df["Y"])
        p = model.predict_proba(build_features(cal_df))[:, 1]
        scores[n] = log_loss(cal_df["Y"], np.clip(p, CLIP, 1 - CLIP), labels=[0, 1])
        print(f"  {n:>9}  {scores[n]:>13.5f}")
    best = min(scores, key=scores.get)
    print(f"  chosen: {best}")
    return best


# --- calibration ------------------------------------------------------------

def fit_isotonic(p, y):
    """Isotonic regression, floored away from 0 and 1.

    y_min/y_max keep the calibrated output off the endpoints, so no prediction
    ever asserts certainty and the 1e-15 clip from L3a cannot bind. The floor is
    1e-6, which charges 13.8 nats for a contradiction instead of 34.5 — a bound
    chosen rather than inherited from float64's limits.
    """
    return IsotonicRegression(
        out_of_bounds="clip", y_min=1e-6, y_max=1 - 1e-6
    ).fit(p, y)


def isotonic_helps(p, y, games):
    """Does recalibration help on rows the recalibrator never saw?

    Two grouped folds: fit isotonic on one half of CAL, score the other, and
    swap. Isotonic can only improve held-out log loss if the miscalibration it
    corrects is real rather than noise, so any improvement here is evidence and
    no arbitrary threshold is needed.

    The decision is made entirely inside CAL. TEST plays no part in it.
    """
    raw_scores, cal_scores = [], []
    for a, b in GroupKFold(n_splits=2).split(p, y, groups=games):
        iso = fit_isotonic(p[a], y[a])
        raw_scores.append(
            log_loss(y[b], np.clip(p[b], CLIP, 1 - CLIP), labels=[0, 1])
        )
        cal_scores.append(
            log_loss(y[b], np.clip(iso.predict(p[b]), CLIP, 1 - CLIP), labels=[0, 1])
        )
    return float(np.mean(raw_scores)), float(np.mean(cal_scores))


def report_calibration(label, y, p, n_bins=10):
    """Print the curve and return the worst gap, without scoring anything."""
    prob_true, prob_pred = calibration_curve(y, p, n_bins=n_bins, strategy="quantile")
    worst = float(np.max(np.abs(prob_true - prob_pred)))
    print(f"\n  calibration — {label}")
    print(f"    {'predicted':>10}  {'actual':>10}  {'gap':>8}")
    for a, b in zip(prob_pred, prob_true):
        print(f"    {a:>10.4f}  {b:>10.4f}  {b - a:>+8.4f}")
    print(f"    max gap {worst:.4f}")
    return worst


def borderline_comparison(y, predictions, reference=None):
    """Score every model again on the pitches whose call was actually in doubt.

    A ten-bin quantile calibration curve over all takes spends seven bins on
    near-certain pitches, because that is where the mass is. It is therefore
    nearly blind in the band framing lives in, and an aggregate log loss is
    dominated by the same pitches for the same reason — the log is steep near 0
    and 1, so sharpening an obvious ball from 0.08 to 0.005 scores better than
    fixing a real error at 0.4.

    The band is the union of the rows any model with varying predictions calls
    uncertain, so the comparison is on one fixed row set rather than each
    model's own idea of where the difficulty is.

    A model whose predictions have no spread is excluded from defining the
    band, because it has no notion of which pitches were difficult. The first
    version of this function did not exclude it, and the constant baseline —
    0.3248 for every pitch, which sits inside the band — selected all 71,422
    rows and silently turned this comparison back into the aggregate table it
    exists to replace. Nothing errored. The only tell was the band reading
    100.0%, which is why the guard below raises rather than prints.
    """
    y = np.asarray(y)
    shares = {
        name: float(((np.asarray(p) > BAND_LO) & (np.asarray(p) < BAND_HI)).mean())
        for name, p in predictions.items()
    }
    definers = [
        name for name, p in predictions.items()
        if np.ptp(np.asarray(p)) > 1e-12
    ]

    if not definers:
        raise ValueError(
            "no model varies its predictions, so there is no difficulty band "
            "to define"
        )

    mask = np.zeros(len(y), dtype=bool)
    for name in definers:
        p = np.asarray(predictions[name])
        mask |= (p > BAND_LO) & (p < BAND_HI)

    if mask.mean() > 0.6:
        raise ValueError(
            f"borderline band covers {mask.mean():.1%} of pitches, so it is not "
            f"selecting the uncertain ones. Band defined by: {definers}. "
            f"Per-model shares: "
            + ", ".join(f"{n} {s:.1%}" for n, s in shares.items())
        )

    y_band = y[mask]
    print(f"\n  borderline band — predictions in ({BAND_LO}, {BAND_HI})")
    print(f"  defined by {len(definers)} of {len(predictions)} models; "
          f"a model with no spread in its predictions cannot say")
    print(f"  which pitches were hard, so it is scored but does not define")
    print(f"\n  {mask.sum():,} of {len(y):,} pitches ({mask.mean():.1%}), "
          f"called-strike rate {y_band.mean():.4f} "
          f"against {y.mean():.4f} overall")

    header = f"\n    {'model':<34}  {'in band':>8}  {'log loss':>9}"
    if reference is not None:
        header += f"  {'vs ref':>8}"
    print(header)

    scores = {}
    for name, p in predictions.items():
        scores[name] = log_loss(
            y_band, np.clip(np.asarray(p)[mask], CLIP, 1 - CLIP), labels=[0, 1]
        )

    ref_score = scores.get(reference)
    for name, score in scores.items():
        line = f"    {name:<34}  {shares[name]:>7.1%}  {score:>9.5f}"
        if ref_score is not None:
            line += ("     ref" if name == reference
                     else f"  {score - ref_score:>+8.5f}")
        print(line)

    print("\n  this is the comparison that bears on framing; the aggregate")
    print("  table above is mostly a contest about obvious balls")
    return scores, mask


# --- out-of-fold expectations ----------------------------------------------

def out_of_fold_predictions(df, max_iter, use_isotonic):
    """An expectation for every pitch, from a model that never saw its game.

    Five grouped folds. Inside each fold's training portion the same recipe runs
    again — fit on most of it, calibrate on a held-out slice of games — so the
    pipeline that produced the TEST numbers is the pipeline that produces these.

    Without this, four fifths of the expectations would be in-sample. Every
    in-sample expectation is pulled toward its own outcome, which shrinks
    strikes above expected toward zero and would bias the chance test toward
    finding nothing — the one direction a chance test must not be biased in.
    """
    preds = np.full(len(df), np.nan)
    positions = np.arange(len(df))

    for fold, (tr, te) in enumerate(
        GroupKFold(n_splits=N_ATTRIBUTION_FOLDS).split(
            df, df["Y"], groups=df["GAME_PK"]
        ), start=1
    ):
        train_df = df.iloc[tr]
        inner_fit, inner_cal = split_by_game_fraction(
            train_df, CAL_FRACTION, RANDOM_STATE + fold
        )

        model = make_gbm(max_iter).fit(build_features(inner_fit), inner_fit["Y"])
        p = model.predict_proba(build_features(df.iloc[te]))[:, 1]

        if use_isotonic:
            p_cal = model.predict_proba(build_features(inner_cal))[:, 1]
            p = fit_isotonic(p_cal, inner_cal["Y"].to_numpy()).predict(p)

        preds[positions[te]] = p
        print(f"    fold {fold}: {len(te):,} pitches scored")

    if np.isnan(preds).any():
        raise RuntimeError(f"{int(np.isnan(preds).sum())} pitches never scored")
    return np.clip(preds, 1e-6, 1 - 1e-6)


# --- catcher attribution ----------------------------------------------------

def catcher_table(df, names):
    """Per-catcher counts, expectations and exact sampling variance.

    VAR is sum p(1-p) over the catcher's own pitches, not n * pbar * (1-pbar).
    Each take has its own expected probability, so the variance of the catcher's
    strike total is the sum of the per-pitch variances. Using the pooled form
    overstates the noise by a factor of about 2.4 on this data, because most
    takes are near-certain calls that contribute almost nothing to it — which is
    the error that produced the README's original 33.9.
    """
    df = df.assign(PQ=df["P_OOF"] * (1 - df["P_OOF"]))
    g = df.groupby("CATCHER_ID")
    tab = pd.DataFrame({
        "N": g.size(),
        "K": g["Y"].sum(),
        "E": g["P_OOF"].sum(),
        "VAR": g["PQ"].sum(),
    })
    tab = tab[tab["N"] >= MIN_TAKES].copy()
    tab["RAW_RATE"] = tab["K"] / tab["N"]
    tab["EXP_RATE"] = tab["E"] / tab["N"]
    tab["SAE"] = tab["K"] - tab["E"]
    tab["SE"] = np.sqrt(tab["VAR"])
    tab["Z"] = tab["SAE"] / tab["SE"]
    # On the rate scale, where shrinkage is defined.
    tab["D"] = tab["SAE"] / tab["N"]
    tab["V"] = tab["VAR"] / tab["N"] ** 2

    tab.insert(0, "NAME", [
        names.get(int(i), str(i)) for i in tab.index
    ])
    return tab.sort_values("SAE", ascending=False)


def shrink(tab):
    """Partial pooling with the strength estimated, not chosen.

    The model is d_c = theta_c + noise, where d_c is the catcher's excess
    called-strike rate, the noise variance v_c is known exactly from the
    baseline's own probabilities, and theta_c ~ N(0, tau^2). The prior mean is
    zero because the baseline is already the expectation: a catcher with no
    effect has d_c = 0 by construction.

    Method of moments. The observed variance of d across catchers is the true
    variance plus the average sampling variance, so

        tau^2 = max(0, Var(d) - mean(v))

    and the posterior mean is tau^2 / (tau^2 + v_c) times d_c. The shrinkage
    strength is therefore a measurement — the ratio of real between-catcher
    variance to within-catcher noise — rather than a knob. A catcher with thin
    data has large v_c and gets pulled hard toward zero; a catcher with a season
    of takes keeps more of their estimate.

    The floor at zero is load-bearing. If the observed spread does not exceed
    the noise, tau^2 is zero, every posterior mean is exactly zero, and the
    honest leaderboard is flat. That is a real possible answer, not a failure.
    """
    observed_var = float(tab["D"].var(ddof=1))
    mean_noise = float(tab["V"].mean())
    tau2 = max(0.0, observed_var - mean_noise)

    out = tab.copy()
    out["SHRINK"] = tau2 / (tau2 + out["V"]) if tau2 > 0 else 0.0
    out["D_SHRUNK"] = out["SHRINK"] * out["D"]
    out["SAE_SHRUNK"] = out["D_SHRUNK"] * out["N"]
    return out, observed_var, mean_noise, tau2


# --- staff adjustment -------------------------------------------------------

def _wls(x, d, w):
    """Weighted least squares of d on x, with an intercept.

    Returns the coefficients, their covariance, and the residuals. The weights
    are the inverse variance of each catcher's excess rate, so a catcher with
    3,000 takes does not pull the line as hard as one with 8,000.
    """
    X = np.column_stack([np.ones_like(x), x])
    XtWX = X.T @ (X * w[:, None])
    beta = np.linalg.solve(XtWX, X.T @ (d * w))
    return beta, np.linalg.inv(XtWX), d - X @ beta


def staff_adjusted(tab):
    """Partial staff quality out of the catcher estimates, then re-shrink.

    EXP_RATE is the mean expected called-strike rate over a catcher's own
    pitches, which is a pitching-staff command index: it is high when his
    pitchers locate well. The Layer 2 chart found excess rate falling as staff
    quality rises (slope -1,328, p = 0.011), meaning the location baseline does
    not fully separate the catcher from the staff. This regression removes the
    part of each catcher's excess rate that his staff explains, and re-estimates
    tau^2 on what is left.

    Two passes of feasible GLS: the first uses 1/v as weights to get tau^2, the
    second uses 1/(v + tau^2), which is the correct error variance once real
    between-catcher spread is admitted. Without the second pass the slope's
    standard error is too small, because it credits all the residual scatter to
    sampling noise.

    THE OVER-CONTROL CAVEAT, which belongs next to any number from this
    function: staff quality is not a clean instrument for staff quality. If good
    framers are systematically assigned to bad staffs — or if a framer's own
    skill makes his staff's expected rate look lower — this regression removes
    real catcher signal along with the confound. The adjusted spread is
    therefore a LOWER bound on catcher effects, not an unbiased estimate of
    them. The unadjusted figure is an upper bound. The truth is between, and
    only a within-team contrast can narrow it.
    """
    x = tab["EXP_RATE"].to_numpy()
    d = tab["D"].to_numpy()
    n = tab["N"].to_numpy(float)
    v = tab["V"].to_numpy()

    w = 1.0 / v
    for _ in range(2):
        beta, cov, resid = _wls(x, d, w)
        tau2 = max(0.0, float(np.var(resid, ddof=2)) - float(v.mean()))
        w = 1.0 / (v + tau2)

    out = tab.copy()
    out["D_RESID"] = resid
    out["SAE_RESID"] = resid * n
    out["SHRINK_ADJ"] = tau2 / (tau2 + v) if tau2 > 0 else 0.0
    out["SAE_ADJ"] = out["SHRINK_ADJ"] * out["D_RESID"] * n

    stats = {
        "slope_rate": float(beta[1]),
        "slope_se": float(np.sqrt(cov[1, 1])),
        "slope_sae": float(beta[1] * n.mean()),
        "observed_var": float(np.var(resid, ddof=2)),
        "mean_noise": float(v.mean()),
        "tau2": tau2,
    }
    return out.sort_values("SAE_ADJ", ascending=False), stats


# --- the chance test --------------------------------------------------------

def permutation_test(df, n_permutations, rng):
    """Is the catcher spread bigger than luck?

    Catcher labels are permuted WITHIN strata of the baseline's own predicted
    probability. Two reasons that stratum and not raw location:

      - The baseline conditions on location, count and handedness, and the
        called zone shifts with all three. Stratifying on location alone would
        leave a catcher who sees more 3-0 counts free to produce a wide null for
        reasons that have nothing to do with chance pairing.
      - The predicted probability is the sufficient summary of everything the
        baseline knows, so holding it fixed holds fixed exactly the pitch
        difficulty each catcher was dealt.

    Permuting a label vector inside a stratum preserves the multiset of labels
    in that stratum. So every permuted catcher has exactly their real number of
    takes AND exactly their real distribution across difficulty strata. The only
    thing randomized is which outcomes they are paired with.

    Three statistics. The spread of strikes above expected is the one the README
    quotes and the one a reader can interpret. Sum of z-squared weights each
    catcher by how precisely they are measured, so it has more power against a
    few genuinely extreme catchers among many noisy ones — and its null mean is
    a free correctness check, since under a correct variance model it must come
    out at the number of qualified catchers. The residual spread repeats the
    first statistic with staff quality regressed out, which asks whether
    anything about the catcher survives his circumstances.
    """
    codes, catchers = pd.factorize(df["CATCHER_ID"])
    n_cat = len(catchers)

    y = df["Y"].to_numpy(float)
    p = df["P_OOF"].to_numpy(float)
    pq = p * (1 - p)

    strata = pd.qcut(
        df["P_OOF"], N_STRATA, labels=False, duplicates="drop"
    ).to_numpy()
    stratum_rows = [np.flatnonzero(strata == s) for s in range(int(strata.max()) + 1)]

    counts = np.bincount(codes, minlength=n_cat)
    qualified = counts >= MIN_TAKES
    print(f"  {int(qualified.sum())} qualified catchers, "
          f"{len(stratum_rows)} probability strata")

    n_takes = counts[qualified].astype(float)

    def statistics(labels):
        k = np.bincount(labels, weights=y, minlength=n_cat)[qualified]
        e = np.bincount(labels, weights=p, minlength=n_cat)[qualified]
        v = np.bincount(labels, weights=pq, minlength=n_cat)[qualified]
        sae = k - e

        # The same staff adjustment, applied inside the null. A permuted
        # catcher keeps his real stratum mix, so his expected rate is
        # essentially unchanged and the regressor is the same one the observed
        # statistic uses. Weights are 1/v because the null has no true
        # between-catcher variance to add to it.
        _, _, resid = _wls(e / n_takes, sae / n_takes, n_takes ** 2 / v)

        return (float(sae.std(ddof=1)),
                float(np.sum(sae ** 2 / v)),
                float((resid * n_takes).std(ddof=1)))

    observed_sd, observed_chi2, observed_resid = statistics(codes)

    null_sd = np.empty(n_permutations)
    null_chi2 = np.empty(n_permutations)
    null_resid = np.empty(n_permutations)
    labels = codes.copy()

    for b in range(n_permutations):
        for rows in stratum_rows:
            labels[rows] = rng.permutation(codes[rows])
        null_sd[b], null_chi2[b], null_resid[b] = statistics(labels)
        if (b + 1) % 200 == 0:
            print(f"    {b + 1:,} / {n_permutations:,} permutations")

    return {
        "n_qualified": int(qualified.sum()),
        "observed_sd": observed_sd,
        "null_sd": null_sd,
        "p_sd": float((1 + np.sum(null_sd >= observed_sd)) / (1 + n_permutations)),
        "observed_chi2": observed_chi2,
        "null_chi2": null_chi2,
        "p_chi2": float((1 + np.sum(null_chi2 >= observed_chi2)) / (1 + n_permutations)),
        "observed_resid": observed_resid,
        "null_resid": null_resid,
        "p_resid": float(
            (1 + np.sum(null_resid >= observed_resid)) / (1 + n_permutations)
        ),
    }


# --- main -------------------------------------------------------------------

def main(n_permutations):
    rng = np.random.default_rng(RANDOM_STATE)

    print("loading FCT_TAKES ...")
    df = load_takes()
    check_features(df)
    names = catcher_names(df["CATCHER_ID"].unique())

    train, test = split_by_game(df)
    fit_df, cal_df = split_by_game_fraction(train, CAL_FRACTION, RANDOM_STATE)

    print(f"\n  all    {len(df):,} pitches, {df['GAME_PK'].nunique():,} games")
    print(f"  train  {len(train):,}  ->  fit {len(fit_df):,} / cal {len(cal_df):,}")
    print(f"  test   {len(test):,} pitches, {test['GAME_PK'].nunique():,} games")

    # --- 1. the smooth baseline --------------------------------------------
    print(f"\n{'=' * 70}")
    print("1. CHOOSING max_iter ON HELD-OUT GAMES")
    print(f"{'=' * 70}")
    max_iter = choose_max_iter(fit_df, cal_df)

    model = make_gbm(max_iter).fit(build_features(fit_df), fit_df["Y"])
    p_cal = model.predict_proba(build_features(cal_df))[:, 1]

    print(f"\n{'=' * 70}")
    print("2. DOES IT NEED RECALIBRATION?")
    print(f"{'=' * 70}")
    report_calibration("GBM on CAL (never fit on these games)", cal_df["Y"], p_cal)

    raw_ll, cal_ll = isotonic_helps(
        p_cal, cal_df["Y"].to_numpy(), cal_df["GAME_PK"].to_numpy()
    )
    use_isotonic = cal_ll < raw_ll
    print(f"\n  two-fold grouped check inside CAL")
    print(f"    raw        {raw_ll:.5f}")
    print(f"    isotonic   {cal_ll:.5f}   ({cal_ll - raw_ll:+.5f})")
    print(f"  decision: {'ADOPT' if use_isotonic else 'SKIP'} recalibration")

    isotonic = fit_isotonic(p_cal, cal_df["Y"].to_numpy()) if use_isotonic else None

    # --- the jury ----------------------------------------------------------
    print(f"\n{'=' * 70}")
    print("3. THE JURY, ON THE SAME TEST GAMES AS L3a")
    print(f"{'=' * 70}")

    results = []
    base_rate = train["Y"].mean()
    band = {}

    preds_constant = np.full(len(test), base_rate)
    results.append(evaluate(
        "Constant (train base rate)",
        test["Y"], preds_constant,
        note=f"always predicts {base_rate:.4f}",
    ))

    bins_train = fit_bins(train)
    preds_bins, n_missing = predict_bins(bins_train, test, base_rate)
    results.append(evaluate(
        "Binned lookup — honest",
        test["Y"], preds_bins,
        note=f"{n_missing:,} test pitches in bins unseen in training",
    ))
    band["Binned lookup"] = preds_bins

    p_test = model.predict_proba(build_features(test))[:, 1]
    results.append(evaluate(
        f"GBM, raw ({max_iter} iterations)",
        test["Y"], p_test,
        note="location + count + handedness, no recalibration",
    ))
    band[f"GBM ({max_iter} iterations)"] = p_test

    if use_isotonic:
        p_iso = isotonic.predict(p_test)
        results.append(evaluate(
            "GBM + isotonic",
            test["Y"], p_iso,
            note="recalibrator fit on CAL games only",
        ))
        band["GBM + isotonic"] = p_iso

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(pd.DataFrame(results)[["model", "n", "log_loss", "auc", "max_cal_error"]]
          .to_string(index=False, float_format=lambda x: f"{x:.5f}"))

    band["Constant"] = preds_constant
    borderline_comparison(test["Y"].to_numpy(), band, reference="Binned lookup")

    # --- out-of-fold expectations ------------------------------------------
    print(f"\n{'=' * 70}")
    print("4. OUT-OF-FOLD EXPECTATIONS FOR EVERY PITCH")
    print(f"{'=' * 70}")
    df = df.assign(P_OOF=out_of_fold_predictions(df, max_iter, use_isotonic))

    bias = df["P_OOF"].mean() - df["Y"].mean()
    print(f"\n  mean expected {df['P_OOF'].mean():.5f} "
          f"vs actual {df['Y'].mean():.5f}  (difference {bias:+.5f})")
    print(f"  on a 7,000-take catcher that bias is {abs(bias) * 7000:.1f} strikes;")
    print("  a baseline off its own mean shifts every catcher the same way, so")
    print("  this is the first number to read and nothing downstream reveals it")
    report_calibration("out-of-fold, all 356k pitches", df["Y"], df["P_OOF"])

    # --- 5. the chance test, BEFORE the leaderboard -------------------------
    print(f"\n{'=' * 70}")
    print("5. THE CHANCE TEST  (run before the leaderboard is read)")
    print(f"{'=' * 70}")
    perm = permutation_test(df, n_permutations, rng)

    print(f"\n  spread of strikes above expected")
    print(f"    observed            {perm['observed_sd']:.2f}")
    print(f"    null mean           {perm['null_sd'].mean():.2f}")
    print(f"    null 2.5 / 97.5     {np.percentile(perm['null_sd'], 2.5):.2f}"
          f" / {np.percentile(perm['null_sd'], 97.5):.2f}")
    print(f"    observed is "
          f"{(perm['observed_sd'] - perm['null_sd'].mean()) / perm['null_sd'].std(ddof=1):.1f}"
          f" null SDs above the null mean")
    print(f"    p                   {perm['p_sd']:.4f}")

    print(f"\n  sum of z-squared")
    print(f"    observed            {perm['observed_chi2']:.1f}")
    print(f"    null mean           {perm['null_chi2'].mean():.1f}"
          f"   (must be ~{perm['n_qualified']} if the variance model is right)")
    print(f"    null 97.5           {np.percentile(perm['null_chi2'], 97.5):.1f}")
    print(f"    p                   {perm['p_chi2']:.4f}")

    print(f"\n  spread after partialling out staff quality")
    print(f"    observed            {perm['observed_resid']:.2f}")
    print(f"    null mean           {perm['null_resid'].mean():.2f}")
    print(f"    null 97.5           {np.percentile(perm['null_resid'], 97.5):.2f}")
    print(f"    p                   {perm['p_resid']:.4f}")
    print("  if this survives, something about the catcher remains after his")
    print("  staff is accounted for; if it collapses, the earlier rejection was")
    print("  largely the staff confound wearing the catcher's name")

    # --- 6. shrinkage and the leaderboard ----------------------------------
    print(f"\n{'=' * 70}")
    print("6. PARTIAL POOLING")
    print(f"{'=' * 70}")

    tab = catcher_table(df, names)
    shrunk, observed_var, mean_noise, tau2 = shrink(tab)

    print(f"\n  {len(tab)} catchers with {MIN_TAKES:,}+ takes")
    print(f"  observed variance of excess rate   {observed_var:.3e}")
    print(f"  mean sampling variance             {mean_noise:.3e}")
    print(f"  tau^2 = max(0, difference)         {tau2:.3e}")

    if tau2 == 0:
        print("\n  tau^2 is zero: the observed spread does not exceed the noise,")
        print("  so every posterior mean is exactly zero. The honest leaderboard")
        print("  is flat, and no catcher is distinguishable from average.")
    else:
        ebar = float(tab["EXP_RATE"].mean())
        kappa = ebar * (1 - ebar) / tau2
        print(f"\n  true spread  sqrt(tau^2) = {np.sqrt(tau2):.5f}"
              f"  ({np.sqrt(tau2) * 100:.2f} points of called-strike rate)")
        print(f"  equivalent Beta-Binomial strength  kappa = {kappa:,.0f} takes")
        print(f"  the median qualified catcher has {int(tab['N'].median()):,}")
        print(f"  shrinkage factor range  "
              f"{shrunk['SHRINK'].min():.3f} to {shrunk['SHRINK'].max():.3f}")

    cols = ["NAME", "N", "RAW_RATE", "EXP_RATE", "SAE", "SE", "Z",
            "SHRINK", "SAE_SHRUNK"]
    print(f"\n  top 10 by raw strikes above expected")
    print(shrunk[cols].head(10).to_string(index=False,
                                          float_format=lambda x: f"{x:.3f}"))
    print(f"\n  bottom 10")
    print(shrunk[cols].tail(10).to_string(index=False,
                                          float_format=lambda x: f"{x:.3f}"))

    # --- 7. staff adjustment ------------------------------------------------
    print(f"\n{'=' * 70}")
    print("7. PARTIALLING OUT THE PITCHING STAFF")
    print(f"{'=' * 70}")

    adj, st = staff_adjusted(shrunk)
    t = st["slope_rate"] / st["slope_se"] if st["slope_se"] else float("nan")

    print(f"\n  excess rate on staff expected rate")
    print(f"    slope       {st['slope_rate']:+.4f} per unit expected rate"
          f"   (SE {st['slope_se']:.4f}, t = {t:+.2f})")
    print(f"    on the strikes scale, at the mean take count: "
          f"{st['slope_sae']:+,.0f}")
    print(f"    Layer 2 measured -1,328 (SE 506, p = 0.011) by OLS in Tableau")
    print(f"\n  residual variance                  {st['observed_var']:.3e}")
    print(f"  mean sampling variance             {st['mean_noise']:.3e}")
    print(f"  tau^2 after adjustment             {st['tau2']:.3e}")

    if st["tau2"] == 0:
        print("\n  nothing survives: once staff quality is removed the remaining")
        print("  spread does not exceed sampling noise, and every adjusted")
        print("  estimate is zero")
    else:
        print(f"  adjusted true spread  sqrt(tau^2) = {np.sqrt(st['tau2']):.5f}"
              f"  ({np.sqrt(st['tau2']) * 100:.2f} points)")
        print(f"  unadjusted was        sqrt(tau^2) = {np.sqrt(tau2):.5f}"
              f"  ({np.sqrt(tau2) * 100:.2f} points)")
        print(f"  staff quality explains "
              f"{1 - st['tau2'] / tau2:.0%} of the between-catcher variance")

    print("\n  OVER-CONTROL: this is a lower bound on catcher effects. If good")
    print("  framers are assigned to worse staffs, or if framing itself lowers")
    print("  a staff's apparent expected rate, real signal is removed here.")
    print("  The unadjusted figure is the upper bound. Only a within-team")
    print("  contrast narrows the range.")

    adj_cols = ["NAME", "N", "EXP_RATE", "SAE", "Z", "SAE_SHRUNK",
                "SAE_RESID", "SAE_ADJ"]
    print(f"\n  top 10, staff-adjusted")
    print(adj[adj_cols].head(10).to_string(
        index=False, float_format=lambda x: f"{x:.3f}"))
    print(f"\n  bottom 10, staff-adjusted")
    print(adj[adj_cols].tail(10).to_string(
        index=False, float_format=lambda x: f"{x:.3f}"))

    # --- 8. the external check ---------------------------------------------
    print(f"\n{'=' * 70}")
    print("8. CATCHERS WITH AN OUTSIDE REPUTATION")
    print(f"{'=' * 70}")
    print("  The README cites Bailey as the consensus best framer in baseball,")
    print("  landing at only +0.63 SD under the binned baseline — offered as")
    print("  evidence the method was too coarse. His rank here, especially the")
    print("  staff-adjusted one, is the closest thing this project has to an")
    print("  independent check on its own model.")

    raw_rank = {name: i for i, name in enumerate(shrunk["NAME"], start=1)}
    adj_rank = {name: i for i, name in enumerate(adj["NAME"], start=1)}

    ranked = adj.copy()
    ranked.insert(0, "RANK_ADJ", [adj_rank[n] for n in ranked["NAME"]])
    ranked.insert(0, "RANK_RAW", [raw_rank[n] for n in ranked["NAME"]])

    hits = ranked[ranked["NAME"].str.contains(
        "|".join(NAMED_CHECKS), case=False, na=False
    )]
    if hits.empty:
        print("\n  none of the named catchers were matched — check the name cache")
    else:
        print()
        print(hits[["RANK_RAW", "RANK_ADJ"] + adj_cols].to_string(
            index=False, float_format=lambda x: f"{x:.3f}"
        ))
        print(f"\n  of {len(ranked)} qualified catchers")
        print("  a rank that improves under adjustment means the catcher was")
        print("  being held back by a good staff, not helped by a bad one")

    print(f"\n  raw SAE        sd {tab['SAE'].std(ddof=1):.2f}   "
          f"range {tab['SAE'].min():+.1f} to {tab['SAE'].max():+.1f}")
    print(f"  shrunk SAE     sd {shrunk['SAE_SHRUNK'].std(ddof=1):.2f}   "
          f"range {shrunk['SAE_SHRUNK'].min():+.1f} to "
          f"{shrunk['SAE_SHRUNK'].max():+.1f}")
    print(f"  adjusted SAE   sd {adj['SAE_ADJ'].std(ddof=1):.2f}   "
          f"range {adj['SAE_ADJ'].min():+.1f} to {adj['SAE_ADJ'].max():+.1f}")
    print("  none of these three is the estimated true spread — posterior means")
    print("  under-disperse by construction. Quote sqrt(tau^2) for that.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--permutations", type=int, default=N_PERMUTATIONS,
        help="permutations for the chance test (default 1000)",
    )
    main(parser.parse_args().permutations)