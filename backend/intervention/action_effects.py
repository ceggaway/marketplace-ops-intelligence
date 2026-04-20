"""Deterministic first-pass intervention effect estimates."""

from __future__ import annotations


INCENTIVE_ACTIONS = {"incentive", "rebalance_plus_incentive"}


def estimated_action_cost(action: str, config: dict) -> float:
    """Config-driven direct cost estimate for an action."""
    return round(float(config.get("action_costs", {}).get(action, 0.0)), 4)


def estimated_shortage_reduction(action: str, predicted_shortage: float, neighbor_surplus: float, config: dict) -> float:
    """
    Estimate shortage reduction as a bounded stress-index delta.

    Rebalance actions are capped by available neighbor surplus. Incentive-only
    actions are capped only by current predicted shortage.
    """
    uplift = float(config.get("action_uplifts", {}).get(action, 0.0))
    base_reduction = max(float(predicted_shortage), 0.0) * uplift
    if action in {"rebalance", "rebalance_plus_incentive"}:
        base_reduction *= min(max(float(neighbor_surplus), 0.0), 1.0)
    if action == "rebalance_plus_incentive":
        base_reduction *= 1.2
    return round(max(0.0, min(base_reduction, max(float(predicted_shortage), 0.0))), 4)


def net_value(action: str, shortage_reduction: float, action_cost: float, config: dict) -> float:
    """Score actions by reduction benefit minus weighted cost and alert penalty."""
    weights = config.get("net_value_weights", {})
    reduction_weight = float(weights.get("shortage_reduction", 1.0))
    cost_weight = float(weights.get("cost", 0.35))
    alert_penalty = float(weights.get("ops_alert_penalty", 0.6))
    penalty = alert_penalty if action == "ops_alert" else 0.0
    return round((shortage_reduction * reduction_weight) - (action_cost * cost_weight) - penalty, 4)
