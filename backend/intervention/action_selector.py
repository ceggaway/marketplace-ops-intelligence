"""Candidate filtering and best-action selection for interventions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from backend.intervention.action_effects import estimated_action_cost, estimated_shortage_reduction, net_value
from backend.intervention.constraints import evaluate_constraints
from backend.intervention.policy import bucket_severity, candidate_actions_for_severity


@dataclass
class ActionDecision:
    recommended_action: str
    action_reason: str
    estimated_action_cost: float
    estimated_shortage_reduction: float
    budget_remaining: float
    neighbor_surplus: float
    persistence_count: int
    severity_bucket: str
    net_value: float
    constraints_triggered: list[str]


def select_action(
    predicted_shortage: float,
    persistence_count: int,
    neighbor_surplus: float,
    remaining_budget: float,
    last_actions_for_zone: dict[str, str],
    config: dict,
    now: datetime | None = None,
) -> ActionDecision:
    """Choose the best feasible action from severity-driven candidates."""
    current_time = now or datetime.now(timezone.utc)
    severity_bucket = bucket_severity(predicted_shortage, config.get("shortage_thresholds", {}))
    candidates = candidate_actions_for_severity(severity_bucket)

    best: dict | None = None
    infeasible_reasons: list[str] = []

    for action in candidates:
        action_cost = estimated_action_cost(action, config)
        shortage_reduction = estimated_shortage_reduction(action, predicted_shortage, neighbor_surplus, config)
        feasible, reasons = evaluate_constraints(
            action=action,
            persistence_count=persistence_count,
            last_action_at=last_actions_for_zone.get(action),
            remaining_budget=remaining_budget,
            estimated_cost=action_cost,
            neighbor_surplus=neighbor_surplus,
            now=current_time,
            config=config,
        )
        if not feasible:
            infeasible_reasons.extend(reasons)
            continue
        candidate = {
            "action": action,
            "action_cost": action_cost,
            "shortage_reduction": shortage_reduction,
            "net_value": net_value(action, shortage_reduction, action_cost, config),
            "constraints_triggered": [],
        }
        if best is None or candidate["net_value"] > best["net_value"]:
            best = candidate

    if best is None:
        fallback = "ops_alert" if severity_bucket == "severe" else "monitor"
        fallback_cost = estimated_action_cost(fallback, config)
        fallback_reduction = estimated_shortage_reduction(fallback, predicted_shortage, neighbor_surplus, config)
        remaining_after = remaining_budget - fallback_cost if fallback in {"incentive", "rebalance_plus_incentive"} else remaining_budget
        return ActionDecision(
            recommended_action=fallback,
            action_reason="No automated action was feasible under current constraints."
            if fallback == "ops_alert"
            else "Constraints blocked active intervention; continue monitoring.",
            estimated_action_cost=fallback_cost,
            estimated_shortage_reduction=fallback_reduction,
            budget_remaining=round(max(remaining_after, 0.0), 4),
            neighbor_surplus=neighbor_surplus,
            persistence_count=persistence_count,
            severity_bucket=severity_bucket,
            net_value=net_value(fallback, fallback_reduction, fallback_cost, config),
            constraints_triggered=sorted(set(infeasible_reasons)),
        )

    remaining_after = remaining_budget - best["action_cost"] if best["action"] in {"incentive", "rebalance_plus_incentive"} else remaining_budget
    return ActionDecision(
        recommended_action=best["action"],
        action_reason=f"Selected {best['action']} because it maximized deterministic net value under current constraints.",
        estimated_action_cost=best["action_cost"],
        estimated_shortage_reduction=best["shortage_reduction"],
        budget_remaining=round(max(remaining_after, 0.0), 4),
        neighbor_surplus=neighbor_surplus,
        persistence_count=persistence_count,
        severity_bucket=severity_bucket,
        net_value=best["net_value"],
        constraints_triggered=best["constraints_triggered"],
    )
