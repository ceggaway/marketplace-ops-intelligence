"""
Baseline intervention policy.

This module is intentionally heuristic. It is a transparent fallback mapping
from depletion risk and imbalance signals to coarse intervention tiers.
"""

from __future__ import annotations


def classify_policy_action(depletion_risk_score: float, imbalance_score: float, current_supply: int) -> str:
    if depletion_risk_score >= 0.75 or imbalance_score >= 0.75:
        return "intervention"
    if depletion_risk_score >= 0.45 or imbalance_score >= 0.45 or current_supply < 15:
        return "watchlist"
    return "no_action"


def baseline_policy_reason(depletion_risk_score: float, imbalance_score: float, current_supply: int) -> str:
    if depletion_risk_score >= 0.75 or imbalance_score >= 0.75:
        return "Heuristic baseline policy recommends intervention because depletion risk or imbalance is already high."
    if depletion_risk_score >= 0.45 or imbalance_score >= 0.45 or current_supply < 15:
        return "Heuristic baseline policy recommends watchlist monitoring because signals are elevated but not yet acute."
    return "Heuristic baseline policy recommends no action because depletion risk and imbalance remain low."

