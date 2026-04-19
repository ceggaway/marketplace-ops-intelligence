"""
Policy Effectiveness Helpers
============================
Builds lightweight decision-support statistics from historical recommendation
outcomes. This is intentionally heuristic and data-sparse friendly: it uses
simple smoothed rates with fallback buckets instead of a learned policy model.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


OUTCOME_LOG = Path("data/outputs/recommendation_outcomes.jsonl")
MIN_EVIDENCE = 3


def action_type_from_recommendation(priority: str, recommendation: str) -> str:
    """Derive a canonical action type from free-text recommendation content."""
    rec_lower = recommendation.lower()
    if "escalat" in rec_lower:
        return "escalation"
    if "surge" in rec_lower or "fare" in rec_lower or "pricing" in rec_lower:
        return "surge_pricing"
    if "incentive" in rec_lower or "bonus" in rec_lower:
        return "driver_incentive"
    if "push" in rec_lower or "notification" in rec_lower or "notify" in rec_lower:
        return "push_notification"
    if priority in ("medium", "low") and "monitor" in rec_lower:
        return "monitor"
    if priority == "low" or "no action" in rec_lower:
        return "none"
    return "push_notification"


def eta_bucket(eta_minutes: int | None) -> str:
    if eta_minutes is None:
        return "unknown"
    if eta_minutes < 10:
        return "under_10m"
    if eta_minutes < 20:
        return "10_to_20m"
    if eta_minutes < 45:
        return "20_to_45m"
    return "45m_plus"


def confidence_band(sample_size: int) -> str:
    if sample_size >= 12:
        return "high"
    if sample_size >= 6:
        return "medium"
    return "low"


def load_outcome_records(path: Path = OUTCOME_LOG, n: int = 1000) -> list[dict]:
    if not path.exists():
        return []
    lines = path.read_text().strip().splitlines()[-n:]
    records: list[dict] = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except Exception:
            continue
    return records


def summarise_action_outcomes(records: Iterable[dict]) -> dict:
    records = list(records)
    total = len(records)
    if total == 0:
        return {
            "total": 0,
            "resolved": 0,
            "follow_rate": 0.0,
            "followed_resolved": 0,
            "unfollowed_resolved": 0,
            "recovery_rate": 0.0,
            "improvement_rate": 0.0,
            "estimated_score_delta": 0.0,
            "confidence_band": "low",
        }

    resolved = [r for r in records if r.get("outcome") is not None]
    followed_known = [r for r in records if r.get("followed_status") in ("followed", "not_followed")]
    follow_rate = (
        sum(1 for r in followed_known if r.get("followed_status") == "followed") / len(followed_known)
        if followed_known else 0.0
    )

    followed_resolved = [r for r in resolved if r.get("followed_status") == "followed"]
    unfollowed_resolved = [r for r in resolved if r.get("followed_status") == "not_followed"]
    effective_sample = followed_resolved or resolved
    n = len(effective_sample)

    recovered = sum(1 for r in effective_sample if r.get("outcome") == "recovered")
    improved = sum(1 for r in effective_sample if r.get("outcome") in ("recovered", "improved"))
    deltas = [
        float(r.get("score_after", 0)) - float(r.get("score_at_time", 0))
        for r in effective_sample
        if r.get("score_after") is not None and r.get("score_at_time") is not None
    ]

    # Beta-style smoothing keeps tiny samples from looking falsely certain.
    recovery_rate = (recovered + 1) / (n + 2) if n else 0.0
    improvement_rate = (improved + 1) / (n + 2) if n else 0.0
    estimated_score_delta = sum(deltas) / len(deltas) if deltas else 0.0

    return {
        "total": total,
        "resolved": len(resolved),
        "follow_rate": round(follow_rate, 4),
        "followed_resolved": len(followed_resolved),
        "unfollowed_resolved": len(unfollowed_resolved),
        "recovery_rate": round(recovery_rate, 4),
        "improvement_rate": round(improvement_rate, 4),
        "estimated_score_delta": round(estimated_score_delta, 4),
        "confidence_band": confidence_band(n),
    }


def _filter_records(records: list[dict], **criteria) -> list[dict]:
    result = records
    for key, value in criteria.items():
        result = [r for r in result if r.get(key) == value]
    return result


def effectiveness_for_context(
    action_type: str,
    risk_level: str,
    root_cause: str,
    intervention_window: str,
    adjacent_risk_flag: bool,
    records: list[dict] | None = None,
) -> dict:
    """
    Return smoothed effectiveness stats with fallback from narrow to broad
    context buckets.
    """
    records = records if records is not None else load_outcome_records()

    filters = [
        (
            {
                "action_type": action_type,
                "risk_level": risk_level,
                "root_cause": root_cause,
                "intervention_window": intervention_window,
                "adjacent_risk_flag": adjacent_risk_flag,
            },
            "exact context",
        ),
        (
            {
                "action_type": action_type,
                "risk_level": risk_level,
                "root_cause": root_cause,
                "intervention_window": intervention_window,
            },
            "same action, risk, cause, and window",
        ),
        (
            {
                "action_type": action_type,
                "risk_level": risk_level,
                "root_cause": root_cause,
            },
            "same action, risk, and cause",
        ),
        (
            {
                "action_type": action_type,
                "risk_level": risk_level,
            },
            "same action and risk level",
        ),
        (
            {
                "action_type": action_type,
            },
            "all historical uses of this action",
        ),
    ]

    chosen_records: list[dict] = []
    chosen_reason = "no historical evidence yet"
    for criteria, reason in filters:
        bucket = _filter_records(records, **criteria)
        stats = summarise_action_outcomes(bucket)
        if stats["resolved"] >= MIN_EVIDENCE:
            chosen_records = bucket
            chosen_reason = reason
            break
        if not chosen_records and bucket:
            chosen_records = bucket
            chosen_reason = reason

    stats = summarise_action_outcomes(chosen_records)
    evidence_count = stats["followed_resolved"] or stats["resolved"]
    stats.update({
        "action_type": action_type,
        "evidence_count": evidence_count,
        "policy_rank_reason": (
            f"Based on {chosen_reason}; {evidence_count} resolved examples, "
            f"{stats['confidence_band']} confidence."
            if chosen_records else
            "No resolved outcomes for this action yet; using rule-based default."
        ),
    })
    return stats


def effectiveness_by_action(records: list[dict] | None = None) -> dict[str, dict]:
    records = records if records is not None else load_outcome_records()
    by_action: dict[str, list[dict]] = {}
    for record in records:
        by_action.setdefault(record.get("action_type", "unknown"), []).append(record)
    return {
        action: summarise_action_outcomes(action_records)
        for action, action_records in by_action.items()
    }
