# Marketplace Ops Intelligence

Real-time taxi supply shortage monitoring for Singapore's 55 planning zones. A LightGBM model scores each zone every 5 minutes, predicts which areas are likely to run out of taxis in the next hour, and surfaces recommended interventions to an ops team through a React dashboard.

## What It Does

The model predicts `supply_shortage` — the probability that available taxi count in a zone will drop by more than 40% in the next hour. Scores are mapped to risk levels (high ≥ 0.70, medium ≥ 0.40, low < 0.40) and written to flat files that the FastAPI backend serves to the frontend.

```
LTA DataMall API (5-min taxi snapshots)
  → feature engineering (depletion rates, lag features, weather, peak hours)
  → LightGBM batch scoring (55 zones)
  → predictions.csv + recommended_actions.csv
  → FastAPI (/api/v1/*)
  → React/Vite dashboard
```

No database. No message queue. Everything is flat files in `data/outputs/`.

## Stack

| Layer | Technology |
|---|---|
| ML model | LightGBM (binary classifier) |
| Backend API | FastAPI + Uvicorn |
| Frontend | Vite + React + TypeScript |
| Live data | LTA DataMall API (taxi availability) |
| Weather | Open-Meteo API |
| CI/CD | GitHub Actions |

## Repo Layout

```
marketplace-ops-intelligence/
├── backend/
│   ├── api/                # FastAPI app — routers, schemas
│   ├── ingestion/          # LTA poller, weather, travel times
│   ├── validation/         # row-level schema validation
│   ├── preprocessing/      # feature engineering pipeline
│   ├── training/           # model training + evaluation
│   ├── registry/           # model version registry
│   ├── promotion/          # 5-check promotion gate
│   ├── scoring/            # batch inference engine
│   ├── recommendations/    # ops action rules engine
│   ├── monitoring/         # drift detection (PSI) + alerting
│   └── rollback/           # rollback logic + audit log
├── frontend-react/         # Vite/React/TypeScript dashboard
│   └── src/
│       ├── pages/          # Overview, ZoneRisk, ActionCenter, ModelHealth
│       ├── components/     # GlassCard, SparkLine, Badge, etc.
│       └── lib/            # API client (api.ts), utils
├── scripts/
│   ├── run_training.py     # train + register + promote
│   ├── run_scoring.py      # score all zones + write outputs
│   ├── run_monitoring.py   # drift check + alerting loop
│   └── build_training_data.py  # build dataset from real poller data
├── tests/                  # pytest suite
├── .github/workflows/      # CI, daily batch, pipeline verify
├── data/
│   ├── outputs/            # predictions, recommendations, pipeline log
│   ├── raw/                # LTA snapshots (gitignored)
│   └── registry/           # registry.json + model artifacts (gitignored)
└── API_CONTRACT.md
```

## Dashboard Pages

**Overview** — KPI cards (high-risk zones, active supply, rapid depletion, actions needed, model PSI), top risk zones, system status, high-risk trend chart

**Zone Risk Monitor** — searchable/filterable table of all 55 zones with depletion rates, risk scores, and a Singapore map view. Click any zone for supply signals, key drivers, and recommended action.

**Action Center** — prioritised intervention queue (critical → high → medium → low) with per-zone detail panel, confidence scores, and action logging

**Model Health** — AUC/Precision/Recall from model registry, PSI drift gauge, per-feature drift breakdown, run history table, one-click retraining

## Model

- **Type**: `LightGBMClassifier`
- **Target**: `supply_shortage = 1` if taxi count drops > 40% in the next hour
- **Key features**: `taxi_count`, `depletion_rate_1h`, `depletion_rate_3h`, `taxi_lag_24h`, `taxi_lag_168h`, `supply_vs_yesterday`, `is_peak_hour`, `is_raining`, `is_holiday`
- **Risk thresholds**: high ≥ 0.70 · medium ≥ 0.40 · low < 0.40
- **Promotion gate**: 5 checks — performance (F1 ≥ 0.50, AUC ≥ 0.65), regression vs active, schema compatibility, integration, stability

The API field name for the score is `delay_risk_score` (preserved for frontend compatibility).

## Run Locally

### 1. Install

```bash
make install          # creates .venv and installs Python deps
make install-frontend # installs Node deps in frontend-react/
```

Requires Python 3.11+ and Node 18+.

### 2. Train a model

```bash
make train
```

Trains on 90 days of synthetic data, runs the promotion gate, and writes the active model to `data/registry/models/`.

### 3. Score

```bash
make score
```

Scores all 55 zones, writes `data/outputs/predictions.csv` and `data/outputs/recommended_actions.csv`.

### 4. Start the API

```bash
make api
# → http://localhost:8000/api/v1
```

### 5. Start the frontend

```bash
make frontend
# → http://localhost:5173
```

The React dev server proxies `/api` to `localhost:8000` automatically.

## Live Data (optional)

To run with real LTA taxi snapshots instead of synthetic data, set your API key:

```bash
export LTA_API_KEY=your_key_here
```

Start the background poller (fetches taxi data every 5 minutes):

```bash
make poller
# logs → data/logs/poller.log
```

Start the scoring loop (scores zones every 5 minutes):

```bash
make monitor-loop
# logs → data/logs/monitor.log
```

Build a training dataset from real poller data (after 24+ hours of polling):

```bash
python scripts/build_training_data.py
make train
```

## All Commands

```bash
make install           # set up Python venv + deps
make install-frontend  # set up Node deps
make api               # start FastAPI on :8000
make frontend          # start Vite dev server on :5173
make train             # train + register + promote model
make score             # run one batch scoring pass
make monitor           # run one drift/monitoring check
make monitor-loop      # start scoring loop (every 5 min, background)
make poller            # start LTA data poller (background)
make poller-once       # run one LTA poll then exit
make test              # run pytest suite
make clean             # wipe outputs, processed data, model artifacts
```

## Tests

```bash
make test
```

Covers: validation, preprocessing, scoring, recommendations, API endpoints, drift monitoring, ingestion, rollback, model registry, and pipeline integration.

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | Every push / PR | Runs the full pytest suite |
| `verify_pipeline.yml` | Push to main | End-to-end: trains, scores, verifies output schema |
| `daily_batch.yml` | 01:00 SGT daily + manual | Trains if model missing, runs batch scoring, uploads outputs as artifact |

## API

Base URL: `http://localhost:8000/api/v1`

| Endpoint | Description |
|---|---|
| `GET /overview` | KPI cards, trend data, active alerts |
| `GET /zones` | All 55 zones with current risk scores |
| `GET /zones/{id}` | Single zone detail + score history |
| `GET /recommendations` | Prioritised action recommendations |
| `GET /pipeline/latest-run` | Most recent scoring run metadata |
| `GET /monitoring/drift` | Latest PSI drift report |
| `GET /monitoring/history` | Recent run history |
| `GET /model/status` | Active model version + training metrics |
| `GET /model/versions` | All registered model versions |
| `GET /health/services` | Pipeline component health check |
| `GET /alerts` | Active operational alerts |
| `POST /pipeline/retrain` | Trigger model retraining |

Full schema: [API_CONTRACT.md](API_CONTRACT.md)
