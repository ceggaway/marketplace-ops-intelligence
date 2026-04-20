"""
Demand pressure proxy utilities.

This module does not estimate true rider demand. It constructs a transparent,
bounded pressure index from public exogenous signals that are plausibly related
to near-term taxi demand intensity.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


def demand_pressure_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the feature columns needed for demand-pressure scoring exist.

    Expected features are proxies, not direct demand observations:
    - time of day / day of week / weekend
    - rainfall intensity
    - train disruption flag
    - congestion ratio
    """
    out = df.copy()
    defaults = {
        "hour_of_day": 0,
        "day_of_week": 0,
        "is_weekend": False,
        "rainfall_mm": 0.0,
        "rain_intensity": 0,
        "congestion_ratio": 1.0,
        "train_disruption_flag": 0,
    }
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default
    return out


def _peak_hour_pressure(hour_of_day: Iterable[int]) -> np.ndarray:
    hours = np.asarray(list(hour_of_day), dtype=float)
    # Morning and evening peaks; bounded between 0 and 1.
    morning = np.exp(-0.5 * ((hours - 8.0) / 1.8) ** 2)
    evening = np.exp(-0.5 * ((hours - 18.0) / 2.0) ** 2)
    return np.clip(np.maximum(morning, evening), 0.0, 1.0)


def score_demand_pressure(df: pd.DataFrame) -> pd.Series:
    """
    Build a simple, interpretable demand-pressure proxy in [0, 1].

    This is a v1 heuristic index combining exogenous conditions that often
    correlate with demand pressure. It intentionally avoids any claim of
    directly observing or estimating true rider demand.
    """
    feat = demand_pressure_features(df)
    hour_component = _peak_hour_pressure(feat["hour_of_day"].tolist())
    weekday_component = np.where(feat["is_weekend"].astype(bool), 0.35, 0.15)
    rain_component = np.clip(feat["rain_intensity"].astype(float) / 3.0, 0.0, 1.0)
    disruption_component = feat["train_disruption_flag"].astype(float).clip(0.0, 1.0)
    congestion_component = np.clip((feat["congestion_ratio"].astype(float) - 1.0) / 0.8, 0.0, 1.0)

    score = (
        0.35 * hour_component +
        0.10 * weekday_component +
        0.20 * rain_component +
        0.20 * disruption_component +
        0.15 * congestion_component
    )
    return pd.Series(np.clip(score, 0.0, 1.0), index=df.index, dtype="float32")


def classify_pressure_level(score: float) -> str:
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"

