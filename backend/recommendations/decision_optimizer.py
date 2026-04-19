"""
Decision Optimizer
==================
Scores viable intervention candidates using learned effectiveness, cost, and
simple driver-response estimates.
"""

from __future__ import annotations

from backend.recommendations.cost_model import estimate_action_cost_sgd, estimate_fairness_penalty
from backend.recommendations.policy_effectiveness import effectiveness_for_context


def _action_prior(action: dict, context: dict) -> float:
    risk_level = context.get("risk_level", "low")
    root_cause = context.get("root_cause", "unknown")
    is_raining = bool(context.get("is_raining"))
    is_fast_depleting = bool(context.get("is_fast_depleting"))
    is_critically_low = bool(context.get("is_critically_low"))
    action_id = action.get("action_id")

    prior = 0.0
    if risk_level == "low":
        if action_id == "none":
            prior += 80
        elif action_id == "monitor":
            prior += 40
        else:
            prior -= 60
    if risk_level == "medium":
        if action_id == "monitor":
            prior += 25
        if action_id in {"mild_surge", "mild_driver_incentive", "push_only"}:
            prior += 12
    if risk_level == "high":
        if action_id in {"monitor", "none"}:
            prior -= 80
        if action_id in {"mild_surge", "strong_surge", "mild_driver_incentive", "strong_driver_incentive", "surge_plus_incentive", "surge_plus_push", "escalation"}:
            prior += 20
    if is_raining:
        if action.get("pricing_level") != "none":
            prior += 35
        if action_id in {"monitor", "push_only", "none"}:
            prior -= 25
    if root_cause == "structural_gap" and action.get("incentive_level") != "none":
        prior += 18
    if is_fast_depleting and action_id in {"mild_surge", "strong_surge", "surge_plus_incentive", "surge_plus_push", "strong_driver_incentive"}:
        prior += 18
    if is_critically_low and action_id in {"strong_driver_incentive", "surge_plus_incentive", "escalation"}:
        prior += 24
    if context.get("intervention_window") == "too_late":
        if action_id == "escalation":
            prior += 200
        else:
            prior -= 200
    return prior


def score_candidate_action(
    action: dict,
    context: dict,
    records: list[dict] | None = None,
) -> dict:
    stats = effectiveness_for_context(
        action_type=str(action.get("action_type", "unknown")),
        risk_level=str(context.get("risk_level", "low")),
        root_cause=str(context.get("root_cause", "unknown")),
        intervention_window=str(context.get("intervention_window", "unknown")),
        adjacent_risk_flag=bool(context.get("adjacent_risk_flag", False)),
        action_id=str(action.get("action_id", "")),
        pricing_level=str(action.get("pricing_level", "none")),
        incentive_level=str(action.get("incentive_level", "none")),
        push_level=str(action.get("push_level", "none")),
        records=records,
    )

    estimated_cost_sgd = estimate_action_cost_sgd(action, context)
    fairness_penalty = estimate_fairness_penalty(action, context)
    expected_supply_response_30m = stats.get("expected_supply_response_30m")
    if expected_supply_response_30m is None:
        expected_supply_response_30m = float(action.get("supply_response_base", 0.0))
    confidence = stats.get("confidence_band", "low")
    confidence_mult = {"high": 1.0, "medium": 0.88, "low": 0.74}.get(confidence, 0.74)

    expected_recovery_probability = float(stats.get("recovery_rate", 0.0))
    expected_improvement_probability = float(stats.get("improvement_rate", 0.0))
    expected_score_delta = float(stats.get("estimated_score_delta", 0.0))
    supply_response = float(expected_supply_response_30m or 0.0)

    reliability_value = (
        expected_recovery_probability * 120
        + expected_improvement_probability * 90
        + max(-expected_score_delta, 0.0) * 80
        + max(supply_response, 0.0) * 6
    )
    net_value = confidence_mult * reliability_value - estimated_cost_sgd * 14 - fairness_penalty * 18 + _action_prior(action, context)
    expected_roi = round(net_value / estimated_cost_sgd, 4) if estimated_cost_sgd > 0 else round(net_value, 4)

    return {
        **action,
        "estimated_cost_sgd": estimated_cost_sgd,
        "expected_supply_response_30m": round(supply_response, 2),
        "expected_recovery_probability": round(expected_recovery_probability, 4),
        "expected_improvement_probability": round(expected_improvement_probability, 4),
        "expected_score_delta": round(expected_score_delta, 4),
        "expected_roi": expected_roi,
        "expected_net_value": round(net_value, 4),
        "confidence_band": stats.get("confidence_band", "low"),
        "evidence_count": int(stats.get("evidence_count", 0)),
        "follow_rate": stats.get("follow_rate", 0.0),
        "policy_rank_reason": stats.get("policy_rank_reason", ""),
        "driver_response_reason": stats.get("driver_response_reason", ""),
    }
