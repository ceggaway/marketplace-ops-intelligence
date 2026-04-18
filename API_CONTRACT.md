# API Contract

Base URL:

```text
http://localhost:8000/api/v1
```

This contract reflects the responses the backend returns today and is
enforced through FastAPI `response_model`s.

## Notes On Compatibility

Some field names still use the earlier "delay risk" framing because the
frontend depends on them.

Compatibility fields still in use:
- `delay_risk_score`
- `predicted_demand_next_hour`
- `fulfilment_rate`
- `avg_delivery_time_min`

Those names should be read as UI compatibility fields, not as proof that the
current backend is modeling delivery delay directly.

## GET `/overview`

```json
{
  "kpis": {
    "fulfilment_rate": 0.63,
    "avg_delivery_time_min": 0.0,
    "delayed_zones_count": 12,
    "predicted_demand_next_hour": 1890,
    "high_risk_zone_count": 5,
    "estimated_intervention_cost": 7.5,
    "as_of": "2026-04-17T04:00:00Z"
  },
  "demand_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 1320.0 }
  ],
  "delay_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 0.0 }
  ],
  "fulfilment_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 0.63 }
  ],
  "alerts": [
    {
      "alert_id": "HIGH_RISK_ZONES",
      "zone_id": null,
      "severity": "high",
      "message": "5 zones currently at high delay risk",
      "created_at": "2026-04-17T04:00:00Z"
    }
  ]
}
```

## GET `/zones`

Query params:
- `risk_level` optional
- `region` optional

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
    "risk_level": "high",
    "recommendation": "Increase driver incentive",
    "explanation_tag": "rapid depletion + peak hour"
  }
]
```

## GET `/zones/{zone_id}`

```json
{
  "zone_id": 27,
  "zone_name": "Orchard",
  "region": "Central",
  "taxi_count": 42,
  "current_supply": 42,
  "depletion_rate_1h": 0.45,
  "supply_vs_yesterday": 0.65,
  "delay_risk_score": 0.85,
  "risk_level": "high",
  "recommendation": "Increase driver incentive",
  "explanation_tag": "rapid depletion + peak hour",
  "demand_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 24.0 }
  ],
  "delay_trend": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 0.0 }
  ],
  "risk_score_history": [
    { "timestamp": "2026-04-17T01:00:00Z", "value": 0.85 }
  ]
}
```

## GET `/recommendations`

Query params:
- `priority` optional

```json
[
  {
    "zone_id": 27,
    "zone_name": "Orchard",
    "region": "Central",
    "risk_level": "high",
    "delay_risk_score": 0.85,
    "issue_detected": "Severe supply shortage forming in this zone",
    "recommendation": "Increase driver incentive: +$1.50 bonus for next hour in this zone",
    "expected_impact": "Expected to improve available supply within 30 min",
    "confidence": 0.85,
    "priority": "high",
    "explanation_tag": "rapid depletion + peak hour",
    "last_updated": "2026-04-17T04:00:00Z"
  }
]
```

## GET `/model/status`

```json
{
  "active_version": "v1",
  "promoted_at": "2026-04-16T20:00:00Z",
  "last_retrained_at": "2026-04-16T19:30:00Z",
  "training_metrics": {
    "precision": 0.9,
    "recall": 0.94,
    "f1": 0.92,
    "roc_auc": 0.95,
    "train_rows": 25000,
    "val_rows": 6000
  },
  "candidate_version": null,
  "candidate_metrics": null
}
```

## GET `/model/versions`

```json
[
  {
    "version_id": "v1",
    "status": "active",
    "trained_at": "2026-04-16T19:30:00Z",
    "promoted_at": "2026-04-16T20:00:00Z",
    "metrics": {
      "f1": 0.92,
      "roc_auc": 0.95
    }
  }
]
```

## GET `/pipeline/latest-run`

```json
{
  "run_id": "abc123",
  "timestamp": "2026-04-17T01:00:00Z",
  "rows_scored": 1320,
  "failed_rows": 0,
  "flagged_zones": 200,
  "drift_flag": false,
  "rollback_status": false,
  "run_status": "success",
  "latency_ms": 500,
  "model_version": "v1",
  "psi": 0.05,
  "logged_at": "2026-04-17T01:00:01Z",
  "avg_delay_min": 8.5,
  "fulfilment_rate": 0.72,
  "total_taxi_count": 4200
}
```

## GET `/monitoring/drift`

```json
{
  "run_id": "abc123",
  "timestamp": "2026-04-17T01:00:00Z",
  "psi": 0.05,
  "drift_flag": false,
  "drift_level": "stable",
  "reference_mean": 0.30,
  "current_mean": 0.32,
  "reference_std": 0.15,
  "current_std": 0.16,
  "reference_n": 1000,
  "current_n": 1000,
  "feature_drift": {
    "taxi_count": {
      "psi": 0.03,
      "drift_level": "stable",
      "reference_mean": 48.2,
      "current_mean": 47.1,
      "reference_std": 18.4,
      "current_std": 19.0
    },
    "depletion_rate_1h": {
      "psi": 0.01,
      "drift_level": "stable",
      "reference_mean": 0.12,
      "current_mean": 0.13,
      "reference_std": 0.09,
      "current_std": 0.10
    }
  }
}
```

`feature_drift` is `null` on the first scoring run (no reference snapshot yet).

## GET `/monitoring/history`

Query params:
- `n` optional (default 20) — number of recent runs to return

Returns a list of `PipelineRun` (same shape as `/pipeline/latest-run`).

## GET `/alerts`

Returns a list of active alerts, merged from `alerts.log` and on-the-fly
pipeline checks. Deduplicated by `alert_id`.

```json
[
  {
    "alert_id": "DRIFT_ALERT",
    "zone_id": null,
    "severity": "high",
    "message": "Prediction drift detected — PSI=0.31 (features: taxi_count)",
    "created_at": "2026-04-17T04:00:00Z"
  }
]
```

`severity` values: `"high"` | `"medium"` | `"info"`

Known `alert_id` values emitted by the system:

| alert_id | severity | trigger |
|---|---|---|
| `DRIFT_ALERT` | high | PSI > 0.25 |
| `DRIFT_WARNING` | medium | PSI 0.10–0.25 |
| `ROLLBACK_OCCURRED` | high | model rolled back |
| `ROLLBACK_FAILED` | high | rollback attempt failed |
| `HIGH_FAILED_ROWS` | medium | >10% rows failed validation |

## GET `/health/services`

Returns the health of each logical service, based on output file freshness.
Files older than 2 hours are marked `degraded`; missing files are `down`.

```json
{
  "services": [
    {
      "name": "Prediction API",
      "status": "ok",
      "detail": null,
      "last_updated": "2026-04-17T03:55:00Z"
    },
    {
      "name": "Data Pipeline",
      "status": "degraded",
      "detail": "Last updated 145 min ago",
      "last_updated": "2026-04-17T01:35:00Z"
    },
    {
      "name": "Feature Store",
      "status": "ok",
      "detail": null,
      "last_updated": "2026-04-17T03:55:00Z"
    },
    {
      "name": "Model Serving",
      "status": "ok",
      "detail": "Active: v1",
      "last_updated": null
    },
    {
      "name": "Drift Monitor",
      "status": "down",
      "detail": "Output file not found",
      "last_updated": null
    }
  ],
  "checked_at": "2026-04-17T04:00:00Z"
}
```

`status` values: `"ok"` | `"degraded"` | `"down"`
