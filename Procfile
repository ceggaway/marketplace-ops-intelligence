web: python -m uvicorn backend.api.main:app --host 0.0.0.0 --port ${PORT:-8000}
poller: python -m backend.ingestion.lta_poller
monitor: python scripts/run_monitoring.py --loop --interval ${MONITOR_INTERVAL_SEC:-300}
release: python scripts/bootstrap_model.py --train-if-missing --allow-synthetic && python scripts/run_scoring.py --allow-synthetic && python scripts/deploy_check.py
