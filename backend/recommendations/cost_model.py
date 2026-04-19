"""
Cost Model
==========
Simple numeric economics for candidate interventions.
"""

from __future__ import annotations


def estimate_action_cost_sgd(action: dict, context: dict) -> float:
    base = float(action.get("cost_base_sgd", 0.0))
    risk_level = context.get("risk_level", "low")
    is_raining = bool(context.get("is_raining"))
    multiplier = 1.0
    if risk_level == "high":
        multiplier += 0.2
    if risk_level == "critical":
        multiplier += 0.35
    if is_raining and action.get("pricing_level") != "none":
        multiplier += 0.1
    return round(base * multiplier, 2)


def estimate_fairness_penalty(action: dict, context: dict) -> float:
    penalty = float(action.get("fairness_penalty", 0.0))
    if context.get("root_cause") == "weather" and action.get("pricing_level") == "strong":
        penalty += 0.2
    if context.get("risk_level") == "medium" and action.get("pricing_level") != "none":
        penalty += 0.15
    return round(penalty, 3)
