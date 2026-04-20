"""
Imbalance scoring utilities.

Imbalance is modeled as the interaction between supply-side depletion risk and
demand-pressure proxies. These scores are descriptive signals for decision
support, not direct estimates of true rider demand.
"""

from __future__ import annotations

import math


def ratio_imbalance_score(demand_pressure_score: float, current_supply: float, supply_baseline: float | None = None) -> float:
    """
    Ratio-style imbalance score.

    Demand pressure is compared against supply availability after normalizing
    supply into a 0..1 support term. Safe numerics prevent divide-by-zero.
    """
    effective_baseline = max(float(supply_baseline or max(current_supply, 1.0)), 1.0)
    supply_support = max(float(current_supply), 0.0) / effective_baseline
    raw = float(demand_pressure_score) / max(supply_support, 0.05)
    # Compress into a bounded range for UI and policy use.
    return float(max(0.0, min(raw / 2.5, 1.0)))


def normalized_difference_imbalance_score(demand_pressure_score: float, current_supply: float, supply_baseline: float | None = None) -> float:
    """
    Normalized-difference imbalance score in [0, 1].

    Values > 0.5 indicate pressure exceeding normalized supply support.
    """
    effective_baseline = max(float(supply_baseline or max(current_supply, 1.0)), 1.0)
    normalized_supply = max(0.0, min(float(current_supply) / effective_baseline, 1.0))
    diff = float(demand_pressure_score) - normalized_supply
    return float(max(0.0, min((diff + 1.0) / 2.0, 1.0)))


def composite_imbalance_score(demand_pressure_score: float, current_supply: float, supply_baseline: float | None = None) -> float:
    ratio = ratio_imbalance_score(demand_pressure_score, current_supply, supply_baseline)
    delta = normalized_difference_imbalance_score(demand_pressure_score, current_supply, supply_baseline)
    return float(round((ratio * 0.6) + (delta * 0.4), 4))


def classify_imbalance_level(score: float) -> str:
    if not math.isfinite(score):
        return "unknown"
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"

