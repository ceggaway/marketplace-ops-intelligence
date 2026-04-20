"""File-backed intervention state for persistence, cooldowns, and budget."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

STATE_PATH = Path("data/outputs/intervention_state.json")


def _today_iso(now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.date().isoformat()


def initial_state(config: dict, now: datetime | None = None) -> dict:
    """Return a fresh empty state payload."""
    return {
        "persistence_counts": {},
        "last_actions": {},
        "budget": {
            "date": _today_iso(now),
            "spent": 0.0,
            "remaining": float(config.get("daily_budget", 0.0)),
        },
    }


def load_state(config: dict, path: Path = STATE_PATH, now: datetime | None = None) -> dict:
    """Load state file, auto-initializing and resetting budget on day rollover."""
    if not path.exists():
        state = initial_state(config, now=now)
        save_state(state, path)
        return state
    try:
        state = json.loads(path.read_text())
    except Exception:
        state = initial_state(config, now=now)
    budget = state.setdefault("budget", {})
    today = _today_iso(now)
    if budget.get("date") != today:
        budget["date"] = today
        budget["spent"] = 0.0
        budget["remaining"] = float(config.get("daily_budget", 0.0))
    state.setdefault("persistence_counts", {})
    state.setdefault("last_actions", {})
    return state


def save_state(state: dict, path: Path = STATE_PATH) -> None:
    """Persist the intervention state to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2) + "\n")


def get_persistence_count(state: dict, zone_key: str) -> int:
    return int(state.get("persistence_counts", {}).get(zone_key, 0))


def get_last_actions(state: dict, zone_key: str) -> dict[str, str]:
    return dict(state.get("last_actions", {}).get(zone_key, {}))


def get_remaining_budget(state: dict) -> float:
    return float(state.get("budget", {}).get("remaining", 0.0))


def update_persistence(state: dict, zone_key: str, should_persist: bool) -> int:
    """Increment or reset persistence count for a zone."""
    counts = state.setdefault("persistence_counts", {})
    if should_persist:
        counts[zone_key] = int(counts.get(zone_key, 0)) + 1
    else:
        counts[zone_key] = 0
    return int(counts[zone_key])


def apply_action(
    state: dict,
    zone_key: str,
    action: str,
    action_cost: float,
    config: dict,
    path: Path = STATE_PATH,
    now: datetime | None = None,
) -> dict:
    """Record last action and decrement budget for incentive-bearing actions."""
    current_time = now or datetime.now(timezone.utc)
    state = load_state(config, path=path, now=current_time) if "budget" not in state else state
    zone_actions = state.setdefault("last_actions", {}).setdefault(zone_key, {})
    zone_actions[action] = current_time.isoformat()
    if action in {"incentive", "rebalance_plus_incentive"}:
        budget = state.setdefault("budget", {})
        spent = float(budget.get("spent", 0.0)) + float(action_cost)
        daily_budget = float(config.get("daily_budget", 0.0))
        budget["spent"] = round(spent, 4)
        budget["remaining"] = round(max(daily_budget - spent, 0.0), 4)
    return state
