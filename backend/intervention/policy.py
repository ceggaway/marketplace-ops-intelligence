"""Severity bucketing and candidate generation for intervention actions."""

from __future__ import annotations

from backend.modeling.shortage import classify_severity_bucket

ALL_ACTIONS = {
    "monitor",
    "rebalance",
    "incentive",
    "rebalance_plus_incentive",
    "ops_alert",
}


def bucket_severity(predicted_shortage: float, thresholds: dict) -> str:
    """Classify predicted shortage into low/moderate/high/severe."""
    return classify_severity_bucket(predicted_shortage, thresholds)


def candidate_actions_for_severity(severity_bucket: str) -> list[str]:
    """Return the allowed action set for a severity bucket."""
    if severity_bucket == "low":
        return ["monitor"]
    if severity_bucket == "moderate":
        return ["monitor", "rebalance", "incentive"]
    return ["rebalance", "incentive", "rebalance_plus_incentive", "ops_alert"]
