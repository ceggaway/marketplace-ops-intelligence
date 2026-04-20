"""Predicted shortage / market-stress scoring utilities."""

from __future__ import annotations

import math


def compute_predicted_shortage(
    demand_pressure_score: float,
    current_taxi_count: float,
    depletion_risk_score: float,
    baseline_supply: float | None,
) -> float:
    """
    Compute a bounded shortage/stress index in [0, 1].

    The score represents marketplace stress, not literal unmet rider demand.
    It combines exogenous demand pressure, depletion risk, and relative supply
    gap against a baseline, with safe numerics and bounded output.
    """
    demand_pressure = min(max(float(demand_pressure_score or 0.0), 0.0), 1.0)
    depletion_risk = min(max(float(depletion_risk_score or 0.0), 0.0), 1.0)
    current_supply = max(float(current_taxi_count or 0.0), 0.0)
    effective_baseline = max(float(baseline_supply or current_supply or 1.0), 1.0)
    normalized_supply = min(current_supply / effective_baseline, 1.0)
    supply_gap = 1.0 - normalized_supply
    raw = (0.45 * demand_pressure) + (0.35 * depletion_risk) + (0.20 * supply_gap)
    return round(max(0.0, min(raw, 1.0)), 4)


def classify_severity_bucket(predicted_shortage: float, thresholds: dict) -> str:
    """Map predicted shortage into low/moderate/high/severe buckets."""
    score = float(predicted_shortage or 0.0)
    monitor_max = float(thresholds.get("monitor_max", 0.25))
    watch_max = float(thresholds.get("watch_max", 0.55))
    severe_min = float(thresholds.get("severe_min", 0.85))
    if not math.isfinite(score):
        return "low"
    if score <= monitor_max:
        return "low"
    if score <= watch_max:
        return "moderate"
    if score >= severe_min:
        return "severe"
    return "high"
