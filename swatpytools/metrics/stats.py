"""Performance metrics for SWAT model evaluation.

All functions accept array-like inputs and handle NaN pairs by excluding them.
Simulated and observed arrays must be the same length.
"""

from __future__ import annotations

import numpy as np


def _clean(simulated: np.ndarray, observed: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Remove time steps where either array is NaN."""
    sim = np.asarray(simulated, dtype=float)
    obs = np.asarray(observed, dtype=float)
    valid = ~(np.isnan(sim) | np.isnan(obs))
    return sim[valid], obs[valid]


def nse(simulated, observed) -> float:
    """Nash-Sutcliffe Efficiency (NSE).

    Range: (-inf, 1]. Perfect = 1. Acceptable typically > 0.5.

    NSE = 1 - sum((obs - sim)^2) / sum((obs - mean(obs))^2)
    """
    sim, obs = _clean(simulated, observed)
    numerator = np.sum((obs - sim) ** 2)
    denominator = np.sum((obs - np.mean(obs)) ** 2)
    if denominator == 0:
        return float("nan")
    return float(1.0 - numerator / denominator)


def kge(simulated, observed) -> float:
    """Kling-Gupta Efficiency (KGE).

    Range: (-inf, 1]. Perfect = 1. Acceptable typically > 0.5.
    Balances correlation, bias ratio, and variability ratio.

    KGE = 1 - sqrt((r-1)^2 + (alpha-1)^2 + (beta-1)^2)
    where r = Pearson r, alpha = std_sim/std_obs, beta = mean_sim/mean_obs
    """
    sim, obs = _clean(simulated, observed)
    if np.std(obs) == 0 or np.mean(obs) == 0:
        return float("nan")
    r = float(np.corrcoef(sim, obs)[0, 1])
    alpha = float(np.std(sim) / np.std(obs))
    beta = float(np.mean(sim) / np.mean(obs))
    return float(1.0 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2))


def pbias(simulated, observed) -> float:
    """Percent Bias (PBIAS).

    Range: (-inf, +inf). Perfect = 0.
    Positive = model underestimation; negative = overestimation.
    Acceptable: |PBIAS| < 25% for streamflow.

    PBIAS = 100 * sum(obs - sim) / sum(obs)
    """
    sim, obs = _clean(simulated, observed)
    total_obs = np.sum(obs)
    if total_obs == 0:
        return float("nan")
    return float(100.0 * np.sum(obs - sim) / total_obs)


def rmse(simulated, observed) -> float:
    """Root Mean Square Error (RMSE).

    Range: [0, +inf). Perfect = 0. Same units as input.
    """
    sim, obs = _clean(simulated, observed)
    return float(np.sqrt(np.mean((sim - obs) ** 2)))


def mae(simulated, observed) -> float:
    """Mean Absolute Error (MAE).

    Range: [0, +inf). Perfect = 0. Same units as input.
    """
    sim, obs = _clean(simulated, observed)
    return float(np.mean(np.abs(sim - obs)))


def r_squared(simulated, observed) -> float:
    """Coefficient of Determination (R²).

    Range: [0, 1]. Perfect = 1.
    """
    sim, obs = _clean(simulated, observed)
    ss_res = np.sum((obs - sim) ** 2)
    ss_tot = np.sum((obs - np.mean(obs)) ** 2)
    if ss_tot == 0:
        return float("nan")
    return float(1.0 - ss_res / ss_tot)


def index_of_agreement(simulated, observed) -> float:
    """Willmott's Index of Agreement (d).

    Range: [0, 1]. Perfect = 1.

    d = 1 - sum((obs - sim)^2) / sum((|sim - mean(obs)| + |obs - mean(obs)|)^2)
    """
    sim, obs = _clean(simulated, observed)
    obs_mean = np.mean(obs)
    numerator = np.sum((obs - sim) ** 2)
    denominator = np.sum((np.abs(sim - obs_mean) + np.abs(obs - obs_mean)) ** 2)
    if denominator == 0:
        return float("nan")
    return float(1.0 - numerator / denominator)


# Convenience alias matching old Tools usage
d1 = index_of_agreement


def calc_scores(simulated, observed) -> dict[str, float]:
    """Compute all metrics and return as a dict.

    Args:
        simulated: Array-like of simulated values.
        observed: Array-like of observed values.

    Returns:
        Dict mapping metric name to rounded float value.
    """
    return {
        "nse": round(nse(simulated, observed), 4),
        "kge": round(kge(simulated, observed), 4),
        "pbias": round(pbias(simulated, observed), 4),
        "rmse": round(rmse(simulated, observed), 4),
        "mae": round(mae(simulated, observed), 4),
        "r_squared": round(r_squared(simulated, observed), 4),
        "index_of_agreement": round(index_of_agreement(simulated, observed), 4),
    }
