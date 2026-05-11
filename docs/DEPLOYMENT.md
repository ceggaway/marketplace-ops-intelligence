# Deployment Guide

This project is deployment-ready for a single-host or PaaS-style deployment
without Docker when the runtime data directory is persistent.

Auth is intentionally out of scope for this deployment guide.

## Recommended Hosting Split

Use Vercel or Netlify for the React frontend only. The FastAPI backend,
poller, monitor loop, model registry, and scored output files need a backend
host with persistent storage, such as Render, Railway, Fly.io, or a VM.

Recommended simple split:

- Vercel or Netlify: static frontend from `frontend-react/dist`.
- Render or Railway: FastAPI backend process and persistent
  `MARKETPLACE_DATA_DIR`.

## Runtime Model

Run these as separate process roles:

- `web`: FastAPI backend.
- `poller`: LTA taxi snapshot polling loop.
- `monitor`: drift/rollback monitoring loop.
- static frontend: files from `frontend-react/dist`.

The included `Procfile` defines:

```text
web: python -m uvicorn backend.api.main:app --host 0.0.0.0 --port ${PORT:-8000}
poller: python -m backend.ingestion.lta_poller
monitor: python scripts/run_monitoring.py --loop --interval ${MONITOR_INTERVAL_SEC:-300}
release: python scripts/bootstrap_model.py && python scripts/deploy_check.py --require-frontend-build
```

## Required Persistent Storage

Set `MARKETPLACE_DATA_DIR` to a persistent mounted directory. The app stores
runtime state under:

- `$MARKETPLACE_DATA_DIR/raw`
- `$MARKETPLACE_DATA_DIR/processed`
- `$MARKETPLACE_DATA_DIR/outputs`
- `$MARKETPLACE_DATA_DIR/registry`
- `$MARKETPLACE_DATA_DIR/logs`

Do not use ephemeral storage for this directory unless this is a throwaway demo.

## Required Environment Variables

```bash
MARKETPLACE_DATA_DIR=/var/lib/marketplace-ops-intelligence
CORS_ORIGINS=https://your-frontend.example.com
LTA_API_KEY=...
VITE_API_BASE_URL=https://your-api.example.com/api/v1
ENABLE_RETRAIN_ENDPOINT=false
RETRAIN_ALLOW_SYNTHETIC=false
BOOTSTRAP_ALLOW_SYNTHETIC=false
```

Optional:

```bash
MODEL_ARTIFACT_DIR=/path/to/model-artifacts
ALERT_WEBHOOK_URL=https://...
ANTHROPIC_API_KEY=...
AI_MODEL=claude-haiku-4-5-20251001
MARKETPLACE_MAX_PREDICTION_AGE_MIN=120
```

## Build

```bash
python3 -m venv .venv
make install-prod
make install-frontend
npm --prefix frontend-react run build
```

For static hosting, publish `frontend-react/dist`.

### Vercel Or Netlify Frontend Settings

Use these settings on either platform:

- Root/base directory: `frontend-react`
- Build command: `npm run build`
- Publish/output directory: `dist`
- Environment variable: `VITE_API_BASE_URL=https://your-api-domain/api/v1`

After deployment, set the backend environment variable:

```bash
CORS_ORIGINS=https://your-frontend-domain
```

## Model Artifact Bootstrap

The tracked `data/registry/registry.json` identifies the active model, but model
artifacts under `data/registry/models/` are ignored by git. A deployment must
restore the active model before serving predictions.

Preferred:

```bash
MODEL_ARTIFACT_DIR=/path/to/artifacts python scripts/bootstrap_model.py
```

`MODEL_ARTIFACT_DIR` may contain either:

- `registry.json` plus `models/<version>/...`, or
- the four files `model.pkl`, `metrics.json`, `feature_schema.json`,
  `version_meta.json` directly.

Controlled fallback:

```bash
BOOTSTRAP_TRAIN_IF_MISSING=true python scripts/bootstrap_model.py
```

Do not set `BOOTSTRAP_ALLOW_SYNTHETIC=true` for production-like deployments.

## Readiness Gate

Run before routing traffic:

```bash
python scripts/deploy_check.py --require-frontend-build
```

To require scored outputs as well:

```bash
python scripts/deploy_check.py --require-frontend-build --require-predictions
```

The check verifies writable persistent directories, active model artifacts,
frontend build output, and prediction freshness when predictions exist.

## Production Scoring Policy

Production scoring should not use synthetic fallback:

```bash
python scripts/run_scoring.py --require-live-data
```

`--allow-synthetic` is only for CI and local demos.

Optional H3 scoring is a second pass over prepared H3 feature data:

```bash
python scripts/run_scoring.py --h3
python scripts/run_scoring.py --h3 --zone
```

H3 scoring reads `data/processed/h3_supply_features.csv` and writes
`data/outputs/h3_predictions.csv`. Run it only after the H3 feature file exists;
the zone scoring path remains the production default.

## Verification

```bash
make verify
```

This runs backend tests, frontend production build, and frontend lint.
