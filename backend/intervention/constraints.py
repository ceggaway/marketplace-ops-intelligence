"""Feasibility checks and neighbor-surplus utilities for interventions."""

from __future__ import annotations

from datetime import datetime, timezone

from backend.intervention.action_effects import INCENTIVE_ACTIONS


def is_persistence_satisfied(action: str, persistence_count: int, config: dict) -> bool:
    """Monitor is always allowed; active interventions require persistence."""
    if action == "monitor":
        return True
    return int(persistence_count) >= int(config.get("persistence_threshold", 1))


def is_cooldown_satisfied(action: str, last_action_at: str | None, now: datetime, config: dict) -> bool:
    """Return False if the action is still inside its cooldown window."""
    cooldown_min = int(config.get("cooldown_windows_min", {}).get(action, 0))
    if cooldown_min <= 0 or not last_action_at:
        return True
    try:
        last_dt = datetime.fromisoformat(str(last_action_at))
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    return (now - last_dt).total_seconds() >= cooldown_min * 60


def is_budget_satisfied(action: str, remaining_budget: float, estimated_cost: float) -> bool:
    """Only incentive-bearing actions consume constrained intervention budget."""
    if action not in INCENTIVE_ACTIONS:
        return True
    return float(remaining_budget) >= float(estimated_cost)


def is_neighbor_surplus_satisfied(action: str, neighbor_surplus: float) -> bool:
    """Rebalance actions require exportable surplus in adjacent zones."""
    if action not in {"rebalance", "rebalance_plus_incentive"}:
        return True
    return float(neighbor_surplus) > 0.0


def compute_neighbor_surplus(
    zone_name: str,
    snapshot_by_zone: dict[str, dict],
    adjacency_map: dict[str, list[str]],
    surplus_buffer: float,
) -> float:
    """
    Compute exportable normalized neighbor surplus after a safety buffer.

    Each neighbor keeps at least baseline_supply * (1 - surplus_buffer).
    Surplus is normalized by neighbor baseline to keep the score interpretable
    across zones with different base supply levels.
    """
    total_surplus = 0.0
    for neighbor in adjacency_map.get(zone_name, []):
        row = snapshot_by_zone.get(neighbor)
        if not row:
            continue
        current_supply = max(float(row.get("taxi_count", 0.0) or 0.0), 0.0)
        baseline_supply = max(float(row.get("baseline_supply", current_supply or 1.0) or 1.0), 1.0)
        buffered_min_supply = max(baseline_supply * (1.0 - float(surplus_buffer)), 1.0)
        exportable = max(current_supply - buffered_min_supply, 0.0)
        total_surplus += exportable / baseline_supply
    return round(max(total_surplus, 0.0), 4)


def evaluate_constraints(
    action: str,
    persistence_count: int,
    last_action_at: str | None,
    remaining_budget: float,
    estimated_cost: float,
    neighbor_surplus: float,
    now: datetime,
    config: dict,
) -> tuple[bool, list[str]]:
    """Evaluate all feasibility checks and return constraint reasons."""
    reasons: list[str] = []
    if not is_persistence_satisfied(action, persistence_count, config):
        reasons.append("persistence_requirement")
    if not is_cooldown_satisfied(action, last_action_at, now, config):
        reasons.append("cooldown_active")
    if not is_budget_satisfied(action, remaining_budget, estimated_cost):
        reasons.append("daily_budget_exhausted")
    if not is_neighbor_surplus_satisfied(action, neighbor_surplus):
        reasons.append("neighbor_surplus_unavailable")
    return len(reasons) == 0, reasons
