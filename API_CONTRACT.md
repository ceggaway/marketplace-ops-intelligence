# API Contract

Base URL:

```text
http://localhost:8000/api/v1
```

This contract reflects the backend responses as implemented today.

## Compatibility Notes

The repo has been reframed from "shortage prediction" to **supply depletion and imbalance intelligence**, but several wire fields are intentionally preserved for compatibility.

Compatibility fields still in use:
- `delay_risk_score` — legacy field name for the model's **depletion risk** output
- `demand_trend` — currently used as a supply trend series in the overview
- `delay_trend` — currently used as a rapid-depletion trend series in the overview
- `fulfilment_trend` — currently used as a high-risk zone count trend

Additive fields introduced by the refactor:
- `depletion_risk_score`
- `demand_pressure_score`
- `demand_pressure_level`
- `imbalance_score`
- `imbalance_level`
- `policy_action`
- `policy_reason`

The API does **not** claim to directly observe true rider demand.

## Semantic Notes

### Depletion Risk
The core ML model estimates near-term **supply depletion risk** from supply-state features.

### Demand Pressure
Demand pressure is a proxy score derived from public exogenous signals such as:
- hour of day
- day of week / weekend
- rainfall intensity
- congestion
- train disruption placeholder flag

### Imbalance
Imbalance is a descriptive combination of:
- depletion risk
- demand-pressure proxy score
- current supply availability

It is not a direct measurement of market equilibrium or true rider demand.

## GET `/overview`

```json
{
  "kpis": {
    "high_risk_zone_count": 5,
    "medium_risk_zone_count": 12,
    "total_taxi_supply": 482,
    "rapid_depletion_zones": 3,
    "critical_actions_count": 7,
    "model_psi": 0.05,
    "minutes_since_last_run": 8,
    "as_of": "2026-04-17T04:00:00Z"
  },
  "demand_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 482.0 }
  ],
  "delay_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 3.0 }
  ],
  "fulfilment_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 5.0 }
  ],
  "alerts": []
}
```

Trend semantics:

| field | actual metric |
|---|---|
| `demand_trend` | active taxi supply by run |
| `delay_trend` | rapid-depletion zone count by run |
| `fulfilment_trend` | high depletion-risk zone count by run |

These names are retained for compatibility.

## GET `/zones`

```json
[
  {
    "zone_id": 27,
    "zone_name": "Orchard",
    "region": "Central",
    "taxi_count": 42,
    "current_supply": 42,
    "depletion_rate_1h": 0.45,
    "supply_vs_yesterday": 0.65,
    "delay_risk_score": 0.85,
    "depletion_risk_score": 0.85,
    "demand_pressure_score": 0.72,
    "imbalance_score": 0.78,
    "imbalance_level": "high",
    "policy_action": "intervention",
    "risk_level": "high",
    "recommendation": "Increase driver incentive",
    "explanation_tag": "rapid depletion + peak hour"
  }
]
```

## GET `/zones/{zone_id}`

Same core fields as `/zones`, plus:
- `demand_trend`
- `delay_trend`
- `risk_score_history`

`demand_trend` is currently per-zone taxi-count history, not direct demand history.

## GET `/recommendations`

```json
[
  {
    "zone_id": 27,
    "zone_name": "Orchard",
    "region": "Central",
    "risk_level": "high",
    "delay_risk_score": 0.85,
    "depletion_risk_score": 0.85,
    "demand_pressure_score": 0.72,
    "imbalance_score": 0.78,
    "imbalance_level": "high",
    "policy_action": "intervention",
    "policy_reason": "Heuristic baseline policy recommends intervention because depletion risk or imbalance is already high.",
    "issue_detected": "Elevated depletion risk ...",
    "recommendation": "Increase driver incentive ...",
    "confidence": 0.85,
    "priority": "high"
  }
]
```

Recommendation responses now include both:
- the existing recommendation-engine outputs
- additive baseline-policy / imbalance context

## GET `/pipeline/latest-run`

Includes the existing run metadata plus optional additive fields:
- `avg_demand_pressure_score`
- `avg_imbalance_score`

These are descriptive summary metrics from the latest scoring run.

## GET `/model/status`

Unchanged shape. This endpoint describes training/registry state, not imbalance or decision performance.

## GET `/model/versions`

Unchanged route. Registry lineage is filtered to show meaningful recent versions by default.

## GET `/monitoring/drift`

Unchanged route. Drift currently monitors the model-score distribution and selected feature distributions.

## Important Non-Claims

The current API does **not** provide:
- true rider demand
- causal intervention effectiveness
- optimized policy selection

It provides:
- depletion-risk estimates
- demand-pressure proxy signals
- imbalance scores
- heuristic intervention support
