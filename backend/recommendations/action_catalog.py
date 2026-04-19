"""
Action Catalog
==============
Parameterized intervention definitions used by the decision engine.
"""

from __future__ import annotations

from copy import deepcopy


ACTION_CATALOG: dict[str, dict] = {
    "none": {
        "action_id": "none",
        "action_type": "none",
        "title": "No action",
        "pricing_level": "none",
        "incentive_level": "none",
        "push_level": "none",
        "lag_min": 0,
        "supply_response_base": 0.0,
        "cost_base_sgd": 0.0,
        "fairness_penalty": 0.0,
        "rider_message": False,
    },
    "monitor": {
        "action_id": "monitor",
        "action_type": "monitor",
        "title": "Monitor only",
        "pricing_level": "none",
        "incentive_level": "none",
        "push_level": "none",
        "lag_min": 0,
        "supply_response_base": 0.0,
        "cost_base_sgd": 0.1,
        "fairness_penalty": 0.0,
        "rider_message": False,
    },
    "mild_surge": {
        "action_id": "mild_surge",
        "action_type": "surge_pricing",
        "title": "Mild surge pricing",
        "pricing_level": "mild",
        "incentive_level": "none",
        "push_level": "none",
        "lag_min": 5,
        "supply_response_base": 2.0,
        "cost_base_sgd": 0.8,
        "fairness_penalty": 0.5,
        "rider_message": True,
    },
    "strong_surge": {
        "action_id": "strong_surge",
        "action_type": "surge_pricing",
        "title": "Strong surge pricing",
        "pricing_level": "strong",
        "incentive_level": "none",
        "push_level": "none",
        "lag_min": 5,
        "supply_response_base": 4.0,
        "cost_base_sgd": 1.6,
        "fairness_penalty": 1.0,
        "rider_message": True,
    },
    "mild_driver_incentive": {
        "action_id": "mild_driver_incentive",
        "action_type": "driver_incentive",
        "title": "Mild driver incentive",
        "pricing_level": "none",
        "incentive_level": "mild",
        "push_level": "none",
        "lag_min": 20,
        "supply_response_base": 3.0,
        "cost_base_sgd": 1.2,
        "fairness_penalty": 0.1,
        "rider_message": False,
    },
    "strong_driver_incentive": {
        "action_id": "strong_driver_incentive",
        "action_type": "driver_incentive",
        "title": "Strong driver incentive",
        "pricing_level": "none",
        "incentive_level": "strong",
        "push_level": "none",
        "lag_min": 20,
        "supply_response_base": 5.5,
        "cost_base_sgd": 2.4,
        "fairness_penalty": 0.1,
        "rider_message": False,
    },
    "push_only": {
        "action_id": "push_only",
        "action_type": "push_notification",
        "title": "Driver push only",
        "pricing_level": "none",
        "incentive_level": "none",
        "push_level": "standard",
        "lag_min": 20,
        "supply_response_base": 2.0,
        "cost_base_sgd": 0.3,
        "fairness_penalty": 0.0,
        "rider_message": False,
    },
    "surge_plus_incentive": {
        "action_id": "surge_plus_incentive",
        "action_type": "surge_plus_incentive",
        "title": "Surge + incentive",
        "pricing_level": "mild",
        "incentive_level": "mild",
        "push_level": "none",
        "lag_min": 15,
        "supply_response_base": 6.0,
        "cost_base_sgd": 2.0,
        "fairness_penalty": 0.6,
        "rider_message": True,
    },
    "surge_plus_push": {
        "action_id": "surge_plus_push",
        "action_type": "surge_plus_push",
        "title": "Surge + push",
        "pricing_level": "mild",
        "incentive_level": "none",
        "push_level": "standard",
        "lag_min": 15,
        "supply_response_base": 4.0,
        "cost_base_sgd": 1.2,
        "fairness_penalty": 0.5,
        "rider_message": True,
    },
    "escalation": {
        "action_id": "escalation",
        "action_type": "escalation",
        "title": "Escalate to ops",
        "pricing_level": "none",
        "incentive_level": "none",
        "push_level": "none",
        "lag_min": 2,
        "supply_response_base": 1.0,
        "cost_base_sgd": 3.0,
        "fairness_penalty": 0.0,
        "rider_message": False,
    },
}


def candidate_actions_for_risk(risk_level: str) -> list[dict]:
    if risk_level == "high":
        action_ids = [
            "mild_surge",
            "strong_surge",
            "mild_driver_incentive",
            "strong_driver_incentive",
            "push_only",
            "surge_plus_incentive",
            "surge_plus_push",
            "escalation",
            "monitor",
        ]
    elif risk_level == "medium":
        action_ids = [
            "monitor",
            "mild_surge",
            "mild_driver_incentive",
            "push_only",
            "surge_plus_push",
        ]
    else:
        action_ids = ["none", "monitor"]
    return [deepcopy(ACTION_CATALOG[action_id]) for action_id in action_ids]
