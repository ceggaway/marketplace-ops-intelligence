"""
Outcome Tracker
===============
Logs every recommendation at the time it is generated, then checks 30 minutes
later whether the zone actually recovered. This creates the feedback loop
needed to eventually make the decisioning layer learnable.

Log file: data/outputs/recommendation_outcomes.jsonl
Each line: one JSON record with these fields:
    zone_id, zone_name, action_type, priority
    score_at_time, risk_level
    logged_at (ISO), check_after (ISO, logged_at + 30 min)
    outcome: null until checked, then "recovered" | "improved" | "unchanged" | "worsened"
    score_after: null until checked

Action types (for aggregation / learning):
    "surge_pricing"     – fare adjustment recommended
    "driver_incentive"  – driver bonus/incentive recommended
    "push_notification" – driver push only (medium risk pre-positioning)
    "escalation"        – human ops manager alert
    "monitor"           – watchlist, no intervention
    "none"              – low risk, no action
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

OUTCOME_LOG = Path("data/outputs/recommendation_outcomes.jsonl")
RECOVERY_WINDOW_MIN = 30
SCORE_RECOVERED_THRESH = 0.70   # score dropped to below this fraction of original
SCORE_IMPROVED_THRESH  = 0.90   # score dropped to below this fraction of original


def _classify_action_type(priority: str, recommendation: str) -> str:
    """Derive a canonical action type from the recommendation text."""
    rec_lower = recommendation.lower()
    if "escalat" in rec_lower:
        return "escalation"
    if "surge" in rec_lower or "fare" in rec_lower:
        return "surge_pricing"
    if "incentive" in rec_lower:
        return "driver_incentive"
    if "push" in rec_lower or "notification" in rec_lower or "notify" in rec_lower:
        return "push_notification"
    if priority in ("medium",) and "monitor" in rec_lower:
        return "monitor"
    if priority == "low":
        return "none"
    return "push_notification"


def log_recommendations(recs_df: pd.DataFrame) -> None:
    """Append one outcome record per recommendation. Call after generate_recommendations."""
    if recs_df.empty:
        return

    now     = datetime.now(timezone.utc)
    check_t = (now + timedelta(minutes=RECOVERY_WINDOW_MIN)).isoformat()
    now_iso = now.isoformat()

    OUTCOME_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTCOME_LOG, "a") as f:
        for _, row in recs_df.iterrows():
            action_type = _classify_action_type(
                str(row.get("priority", "low")),
                str(row.get("recommendation", "")),
            )
            record = {
                "zone_id":       int(row.get("zone_id", 0)),
                "zone_name":     str(row.get("zone_name", "")),
                "action_type":   action_type,
                "priority":      str(row.get("priority", "low")),
                "score_at_time": round(float(row.get("delay_risk_score", 0)), 4),
                "risk_level":    str(row.get("risk_level", "low")),
                "logged_at":     now_iso,
                "check_after":   check_t,
                "outcome":       None,
                "score_after":   None,
            }
            f.write(json.dumps(record) + "\n")


def check_and_update_outcomes(current_preds_df: pd.DataFrame) -> list[dict]:
    """
    For any recommendation logged ≥30 min ago without an outcome, look up the
    current risk score and classify the outcome. Rewrites the log in-place.
    Returns a list of newly resolved outcomes (for logging / alerting).
    """
    if not OUTCOME_LOG.exists() or current_preds_df.empty:
        return []

    now = datetime.now(timezone.utc)

    # Build zone_id → current score map
    score_map: dict[int, float] = {}
    if "zone_id" in current_preds_df.columns and "delay_risk_score" in current_preds_df.columns:
        for _, r in current_preds_df.iterrows():
            score_map[int(r["zone_id"])] = float(r["delay_risk_score"])

    raw = OUTCOME_LOG.read_text().strip().splitlines()
    updated: list[dict] = []
    resolved: list[dict] = []

    for line in raw:
        try:
            rec = json.loads(line)
        except Exception:
            continue

        if rec.get("outcome") is not None:
            # already resolved — keep as-is
            updated.append(rec)
            continue

        check_after = datetime.fromisoformat(rec["check_after"])
        if check_after.tzinfo is None:
            check_after = check_after.replace(tzinfo=timezone.utc)

        if now < check_after:
            # not yet time to check
            updated.append(rec)
            continue

        zone_id      = rec["zone_id"]
        score_before = rec["score_at_time"]
        score_now    = score_map.get(zone_id)

        if score_now is None:
            updated.append(rec)
            continue

        ratio = score_now / score_before if score_before > 0 else 1.0
        if ratio <= SCORE_RECOVERED_THRESH:
            outcome = "recovered"
        elif ratio <= SCORE_IMPROVED_THRESH:
            outcome = "improved"
        elif ratio >= 1.10:
            outcome = "worsened"
        else:
            outcome = "unchanged"

        rec["outcome"]     = outcome
        rec["score_after"] = round(score_now, 4)
        updated.append(rec)
        resolved.append(rec)

    OUTCOME_LOG.write_text("\n".join(json.dumps(r) for r in updated) + "\n")
    return resolved


def get_recent_outcomes(n: int = 100) -> list[dict]:
    """Return the last n resolved outcome records for the API / Ops AI context."""
    if not OUTCOME_LOG.exists():
        return []
    lines = OUTCOME_LOG.read_text().strip().splitlines()[-n:]
    records = []
    for line in lines:
        try:
            r = json.loads(line)
            if r.get("outcome") is not None:
                records.append(r)
        except Exception:
            pass
    return records


def outcome_summary() -> dict:
    """Aggregate outcome stats for monitoring / model evaluation."""
    records = get_recent_outcomes(n=500)
    if not records:
        return {}

    by_action: dict[str, dict[str, int]] = {}
    for r in records:
        at = r.get("action_type", "unknown")
        oc = r.get("outcome", "unknown")
        by_action.setdefault(at, {})
        by_action[at][oc] = by_action[at].get(oc, 0) + 1

    return {
        "total_resolved": len(records),
        "by_action": by_action,
    }
