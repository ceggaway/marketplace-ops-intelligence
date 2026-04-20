"""
Evaluation scaffolding for future intervention-vs-holdout analysis.

These helpers are intentionally lightweight. They provide stable shapes for
offline comparison without claiming causal validity yet.
"""

from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd


def assign_holdout_bucket(zone_id: int, timestamp: str, holdout_rate: float = 0.1) -> str:
    raw = f"{zone_id}|{timestamp}"
    bucket = int(hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF
    return "holdout" if bucket < holdout_rate else "treatment"


def compare_policy_outcomes(records: Iterable[dict]) -> dict:
    df = pd.DataFrame(list(records))
    if df.empty:
        return {
            "treatment_count": 0,
            "holdout_count": 0,
            "treatment_recovery_rate": 0.0,
            "holdout_recovery_rate": 0.0,
            "recovery_rate_gap": 0.0,
        }

    if "evaluation_bucket" not in df.columns:
        df["evaluation_bucket"] = "treatment"
    if "outcome" not in df.columns:
        df["outcome"] = "unknown"

    def _recovery_rate(series: pd.Series) -> float:
        if series.empty:
            return 0.0
        return float((series == "recovered").mean())

    treatment = df[df["evaluation_bucket"] == "treatment"]
    holdout = df[df["evaluation_bucket"] == "holdout"]
    treatment_rate = _recovery_rate(treatment["outcome"])
    holdout_rate = _recovery_rate(holdout["outcome"])
    return {
        "treatment_count": int(len(treatment)),
        "holdout_count": int(len(holdout)),
        "treatment_recovery_rate": round(treatment_rate, 4),
        "holdout_recovery_rate": round(holdout_rate, 4),
        "recovery_rate_gap": round(treatment_rate - holdout_rate, 4),
    }
