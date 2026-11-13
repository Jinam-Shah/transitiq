"""Scoring quantile predictions: pinball loss, calibration, the schedule-only baseline (D-027).

Everything here is plain numpy so it is unit-tested without LightGBM.

    calibration   share of real journeys at or under the predicted q-quantile. A calibrated
                  90% estimate is beaten by about 90% of journeys.
    pinball loss  the standard loss for a quantile; lower is better. Under-predicting costs
                  q per second, over-predicting (1 - q) per second.
    baseline      what the official schedule alone says: scheduled ride time plus a wait that
                  is uniform over the scheduled headway (trains exactly on time, rider arriving
                  at a random moment), so its q-quantile is ride + q * headway.
"""

from __future__ import annotations

import math

import numpy as np

QUANTILES: tuple[float, ...] = (0.5, 0.8, 0.9, 0.95)


def pinball(y: np.ndarray, pred: np.ndarray, q: float) -> float:
    diff = np.asarray(y, dtype=float) - np.asarray(pred, dtype=float)
    return float(np.mean(np.maximum(q * diff, (q - 1) * diff)))


def coverage(y: np.ndarray, pred: np.ndarray) -> float:
    return float(np.mean(np.asarray(y) <= np.asarray(pred)))


def schedule_baseline(
    sched_ride_s: np.ndarray, sched_headway_s: np.ndarray, quantiles=QUANTILES
) -> np.ndarray:
    """n x len(quantiles) seconds; NaN where the schedule does not give both numbers."""
    ride = np.asarray(sched_ride_s, dtype=float)[:, None]
    headway = np.asarray(sched_headway_s, dtype=float)[:, None]
    return ride + np.asarray(quantiles)[None, :] * headway


def non_crossing(pred: np.ndarray) -> np.ndarray:
    """Quantiles must not cross (p80 >= p50 ...); separately trained models can, slightly."""
    return np.sort(pred, axis=1)


def conformal_offsets(y: np.ndarray, pred: np.ndarray, quantiles=QUANTILES) -> np.ndarray:
    """Per-quantile shift in log space that makes the calibration set hit each target exactly.

    One-sided split conformal: for target q, the shift is the ceil((n + 1) q)-th smallest
    residual log(y) - log(pred). Applied as pred * exp(shift). On exchangeable data this gives
    coverage >= q; on later months it is an honest correction for drift since training.
    """
    y = np.asarray(y, dtype=float)
    out = []
    for j, q in enumerate(quantiles):
        r = np.sort(np.log(y) - np.log(pred[:, j]))
        n = len(r)
        k = min(n, max(1, math.ceil((n + 1) * q)))
        out.append(r[k - 1])
    return np.asarray(out)


def apply_offsets(pred: np.ndarray, offsets: np.ndarray) -> np.ndarray:
    return non_crossing(pred * np.exp(np.asarray(offsets)[None, :]))


def scorecard(y: np.ndarray, pred: np.ndarray, quantiles=QUANTILES) -> dict[str, dict]:
    """{"q90": {"coverage": .., "pinball": ..}, ...} for one set of predictions."""
    return {
        f"q{round(q * 100)}": {
            "coverage": coverage(y, pred[:, j]),
            "pinball": pinball(y, pred[:, j], q),
        }
        for j, q in enumerate(quantiles)
    }


def improvement(model_loss: float, baseline_loss: float) -> float:
    """Share of the baseline's pinball loss removed by the model (0.3 = 30% lower)."""
    return 1.0 - model_loss / baseline_loss if baseline_loss else float("nan")
