# Marketplace Ops Intelligence

Singapore taxi supply depletion and imbalance intelligence system for ops teams.

Marketplace Ops Intelligence monitors zone-level taxi supply, estimates near-term depletion risk, recommends governed interventions, and tracks whether recommendations and models are working. It is built around a deliberately honest constraint: this repo does not directly observe rider demand. It uses public supply and stressor signals to reason about imbalance risk, not true demand.

Full product spec: [docs/PRD.md](docs/PRD.md)

## What This System Does

The operating loop is:

1. See current marketplace health.
2. Diagnose risky zones and supply drivers.
3. Recommend human-approved actions.
4. Learn from model health and recommendation outcomes.

The system does not dispatch drivers, set prices, auto-fire interventions, or estimate true rider demand.

## Dashboards

The React frontend has five primary dashboards:

- **Project Brief** (`/project`): product summary, architecture, pipeline flow, dashboard map, and H3 spatial-mode notes.
- **Overview** (`/`): current supply health, high-risk zones, rapid depletion, pending actions, model status, system status, recent model run, and recommended next action.
- **Zone Risk Monitor** (`/zones`): searchable zone table, risk filters, selected-zone drilldown, key drivers, supply signals, recommendation context, risk distribution, and pipeline status.
- **Action Center** (`/actions`): ranked recommendations, priority tabs, confidence, expected impact, estimated cost, governance tier, approver requirement, opportunity ratio, and followed/not-followed logging.
- **Model Health** (`/health`): ROC AUC, F1, precision, recall, RMSE/probability-error metrics, PSI drift, model versions, MLflow-style model comparison, retraining trigger, data/prediction drift, operational run history, and alert banner.

Supporting reporting surface:

- **Reports** (`/reports`): zone performance, intervention outcomes, follow-through split, strongest context buckets, recent resolved outcomes, and model impact summary.

## Business Logic Added From The Product Spec

Recent business-logic additions from the updated PRD/operational brief:

- Action governance tiers:
  - Tier 0: monitor only
  - Tier 1: ops alert
  - Tier 2: driver-comms recommendation
  - Tier 3: incentive proposal
  - Tier 4: rebalancing recommendation
- Explicit approver metadata per action.
- Recommendation trigger condition.
- Expected supply uplift.
- Estimated recoverable opportunity, framed as illustrative opportunity rather than expected revenue.
- Opportunity-to-cost ratio.
- Outcome logging for governance and ROI fields.
- Synthetic-control validity-gate helper for future causal evaluation.
- YAML-configurable governance, ROI bridge, and causal gate thresholds.

## System Architecture

```text
LTA / public signals
  -> ingestion
  -> preprocessing
  -> depletion-risk model
  -> demand-pressure proxy scoring
  -> imbalance scoring
  -> governed recommendation engine
  -> FastAPI
  -> React dashboard
  -> outcome and model-health reporting
```

### Layer 1: Supply Availability And Depletion Risk

- Taxi availability snapshots are ingested at zone level.
- Feature engineering builds lag, rolling, baseline, and depletion signals.
- A LightGBM classifier estimates near-term depletion risk.
- The compatibility field `delay_risk_score` remains in outputs; preferred meaning is `depletion_risk_score`.

### Layer 2: Demand-Pressure Proxies

Signals include:

- Time of day and day of week
- Weekend effects
- Rainfall intensity
- Traffic congestion
- Public holiday / calendar effects
- Live LTA TrainServiceAlerts disruption signal with zero-flag fallback

These are pressure proxies, not direct demand labels.

### Layer 3: Imbalance Scoring

Demand-pressure proxies are combined with live supply availability to produce bounded imbalance scores:

- `demand_pressure_score`
- `demand_pressure_level`
- `imbalance_score`
- `imbalance_level`
- `predicted_shortage`

### Layer 4: Intervention Recommendation

The recommendation engine maps depletion risk, predicted shortage, persistence, budget, cooldowns, and adjacent-zone surplus into action recommendations.

Recommendations include:

- action tier and tier label
- required approver
- trigger condition
- expected supply uplift
- estimated action cost
- estimated recoverable opportunity
- opportunity ratio
- constraints triggered
- confidence and priority

The system recommends; humans approve and execute.

### Layer 5: Monitoring And Evaluation

The monitoring layer includes:

- model registry and promotion/rollback support
- drift monitoring via PSI
- pipeline run metadata
- MLflow-style model-version comparison across classification and probability-error metrics
- recommendation outcome logging
- follow-through feedback
- causal-validity scaffolding for future synthetic-control evaluation

## Data Sources

Current and planned public-signal inputs:

- **Taxi availability**: LTA taxi snapshots
- **Weather / rainfall**: public weather and rainfall features
- **Traffic congestion**: LTA travel-time derived congestion ratio
- **Calendar effects**: hour of day, weekday, weekend, holidays
- **Train disruption flag**: LTA TrainServiceAlerts-derived disruption signal with safe zero fallback

## Outputs

Core batch outputs in `data/outputs/`:

- `predictions.csv`
- `h3_predictions.csv` when optional H3 scoring is run
- `flagged_zones.csv`
- `recommended_actions.csv`
- `score_distribution.json`
- `zone_scores_history.jsonl`
- `pipeline.log`
- `recommendation_outcomes.jsonl`

Prediction and recommendation outputs include fields such as:

- `depletion_risk_score`
- `demand_pressure_score`
- `imbalance_score`
- `imbalance_level`
- `policy_action`
- `recommended_action`
- `action_tier`
- `requires_approver`
- `expected_supply_uplift`
- `estimated_recoverable_opportunity`
- `opportunity_ratio`

## API

Base URL:

```text
http://localhost:8000/api/v1
```

Primary endpoints:

- `GET /overview`
- `GET /zones`
- `GET /zones/{zone_id}`
- `GET /recommendations`
- `POST /recommendations/{recommendation_id}/feedback`
- `GET /model/status`
- `GET /model/versions`
- `GET /pipeline/latest-run`
- `GET /monitoring/drift`
- `GET /monitoring/history`
- `GET /alerts`
- `GET /health/services`
- `POST /pipeline/retrain`
- `GET /reports/zone-performance`
- `GET /reports/outcomes`
- `GET /reports/model-impact`
- `GET /h3/cells`
- `GET /h3/cells/{h3_cell}`
- `GET /h3/heatmap`

Full schema notes: [API_CONTRACT.md](API_CONTRACT.md)

## Running Locally

### Install

```bash
make install
make install-frontend
```

### Train

```bash
make train
```

### Score

```bash
make score
```

Optional H3 hex-cell scoring expects prepared H3 features in
`data/processed/h3_supply_features.csv`:

```bash
python scripts/run_scoring.py --h3
python scripts/run_scoring.py --h3 --zone
```

### Start API

```bash
make api
```

API URL:

```text
http://localhost:8000
```

### Start Frontend

```bash
make frontend
```

Frontend URL:

```text
http://localhost:5173
```

## Live / Continuous Operation

To keep outputs fresh in local demo or operational runs:

```bash
make poller
make monitor-loop
```

`Retrain Model` creates a new model version. It does not refresh stale prediction outputs by itself; run scoring or the poller/monitor loop to refresh operational data.

## Deployment

Non-Docker deployment notes are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

Vercel or Netlify should be used for the React frontend only. The FastAPI API,
poller, monitor loop, model registry, and runtime outputs need a backend host
with persistent storage, such as Render, Railway, Fly.io, or a VM.

Key deployment requirements:

- Set `MARKETPLACE_DATA_DIR` to persistent storage.
- Restore or train an active model artifact before serving the API.
- Build the frontend with `npm --prefix frontend-react run build`.
- Set frontend build env `VITE_API_BASE_URL=https://your-api-domain/api/v1`.
- Set backend env `CORS_ORIGINS=https://your-frontend-domain`.
- Run `python scripts/deploy_check.py --require-frontend-build` before routing traffic.
- Use `python scripts/run_scoring.py --require-live-data` for production-like scoring.
- Run optional H3 scoring only after `data/processed/h3_supply_features.csv` exists.

### Frontend On Vercel Or Netlify

Use either platform with the same Vite settings:

- Root/base directory: `frontend-react`
- Build command: `npm run build`
- Publish/output directory: `dist`
- Environment variable: `VITE_API_BASE_URL=https://your-api-domain/api/v1`

After the frontend URL is live, add it to the backend `CORS_ORIGINS`.

Recommended simple split:

- Netlify or Vercel: frontend static site.
- Render or Railway: FastAPI backend plus persistent disk for `MARKETPLACE_DATA_DIR`.

## Tests

```bash
make test
```

Equivalent direct command:

```bash
.venv/bin/python -m pytest tests/
```

## Limitations

- Rider demand is not directly observed.
- Demand pressure is approximated from public exogenous signals.
- Recommendation logic is still deterministic and heuristic.
- Outcome tracking exists, but full causal evaluation is not yet wired end to end.
- Synthetic-control validity gates exist as scaffolding, but there is not yet a full experiment result endpoint.
- Some API and file fields retain legacy naming for frontend compatibility.

## Future Work

- Supply recovery simulator endpoint with uncertainty bands.
- Switchback experiment designer.
- Synthetic-control result endpoint backed by real intervention logs.
- SHAP-style explanation endpoint.
- Training-serving skew dashboard.
- Richer event and crowding signals.
- Optimized decision policy once enough followed recommendation outcomes exist.
