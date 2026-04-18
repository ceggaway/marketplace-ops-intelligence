"""
ML Health API Router
====================
Endpoints consumed by the Model & Pipeline Health page.
All reads come from flat files — no database.

Endpoints:
    GET /model/status           → Active model version + metrics
    GET /model/versions         → All versions in registry
    GET /pipeline/latest-run    → Most recent scoring run metadata
    GET /monitoring/drift       → Latest drift report
    GET /monitoring/history     → Recent run history (last N runs)
    GET /alerts                 → Active system alerts
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException

from backend.api.schemas.responses import Alert, DriftReport, ModelStatus, ModelVersion, PipelineRun, ServicesHealth, ServiceStatus
from backend.registry import model_registry as registry
from backend.monitoring.metrics import get_latest_runs
from backend.monitoring.alerting import get_active_alerts

router  = APIRouter()
OUT_DIR = Path("data/outputs")


@router.get("/model/status", response_model=ModelStatus)
def get_model_status():
    meta = registry.get_active_version_meta()
    if not meta:
        return {
            "active_version":   None,
            "promoted_at":      None,
            "last_retrained_at": None,
            "training_metrics": {},
            "candidate_version": None,
            "candidate_metrics": None,
        }

    reg_data  = registry._load_registry()
    cand_id   = reg_data.get("candidate_version")
    cand_meta = registry.get_version_meta(cand_id) if cand_id else None

    training_metrics = {
        k: meta.get(k)
        for k in ["precision", "recall", "f1", "roc_auc", "train_rows", "val_rows"]
        if k in meta
    }

    return {
        "active_version":    meta.get("version_id"),
        "promoted_at":       meta.get("promoted_at"),
        "last_retrained_at": meta.get("trained_at"),
        "training_metrics":  training_metrics,
        "candidate_version": cand_id,
        "candidate_metrics": {k: cand_meta.get(k) for k in ["f1","roc_auc"]} if cand_meta else None,
    }


@router.get("/model/versions", response_model=list[ModelVersion])
def get_model_versions():
    versions = registry.list_versions()
    return versions


@router.get("/pipeline/latest-run", response_model=PipelineRun)
def get_latest_run():
    runs = get_latest_runs(n=1)
    if not runs:
        return {
            "run_id":            None,
            "timestamp":         None,
            "rows_scored":       0,
            "failed_rows":       0,
            "flagged_zones":     0,
            "drift_flag":        False,
            "rollback_status":   False,
            "run_status":        "never_run",
            "latency_ms":        0,
            "model_version":     None,
            "psi":               None,
            "logged_at":         None,
            "avg_delay_min":     None,
            "fulfilment_rate":   None,
            "total_taxi_count":  None,
        }
    return runs[-1]


@router.get("/monitoring/drift", response_model=DriftReport)
def get_drift_report():
    drift_path = OUT_DIR / "drift_report.json"
    if not drift_path.exists():
        return {
            "run_id":         "",
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "psi":            0.0,
            "drift_flag":     False,
            "drift_level":    "stable",
            "reference_mean": 0.0,
            "current_mean":   0.0,
            "reference_std":  0.0,
            "current_std":    0.0,
        }
    with open(drift_path) as f:
        return json.load(f)


@router.get("/monitoring/history", response_model=list[PipelineRun])
def get_monitoring_history(n: int = 20):
    return get_latest_runs(n=n)


@router.get("/alerts", response_model=list[Alert])
def get_alerts():
    """
    Returns active alerts merged from two sources:
    1. alerts.log  — persisted by the alerting module (drift, rollback, etc.)
    2. On-the-fly checks — high failed-row rate in the latest batch run
    Deduplicates by alert_id, keeping the most recent occurrence.
    Only alerts created within the last 24 hours are shown — older entries
    are considered resolved/stale and suppressed.
    """
    now_dt  = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    seen: dict[str, dict] = {}

    # ── Source 1: persisted alerts.log (last 24 h only) ──────────────────────
    for a in get_active_alerts(n=50):
        try:
            created = datetime.fromisoformat(a["created_at"])
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_hours = (now_dt - created).total_seconds() / 3600
            if age_hours > 24:
                continue   # suppress stale alerts
        except Exception:
            pass
        seen[a["alert_id"]] = a

    # ── Source 2: high failed-row rate (not covered by alerting module) ──────
    runs = get_latest_runs(n=1)
    if runs:
        last   = runs[-1]
        scored = last.get("rows_scored", 1) or 1
        failed = last.get("failed_rows", 0)
        if failed / scored > 0.10:
            alert_id = "HIGH_FAILED_ROWS"
            seen[alert_id] = {
                "alert_id":   alert_id,
                "zone_id":    None,
                "severity":   "medium",
                "message":    f"{failed} rows failed validation in latest batch ({failed/scored:.0%})",
                "created_at": last.get("timestamp", now_iso),
            }

    return list(seen.values())


@router.post("/pipeline/retrain")
def trigger_retrain():
    """
    Starts the training pipeline as a detached background subprocess.
    Returns immediately — training completes asynchronously (typically 1–3 min).
    """
    project_root = Path(__file__).resolve().parents[3]
    script       = project_root / "scripts" / "run_training.py"

    if not script.exists():
        raise HTTPException(status_code=500, detail="Training script not found")

    version = f"v{int(time.time())}"
    log_dir = project_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "retrain.log"

    with open(log_path, "a") as log_f:
        subprocess.Popen(
            [sys.executable, str(script), "--version", version, "--allow-synthetic"],
            cwd=str(project_root),
            stdout=log_f,
            stderr=log_f,
            start_new_session=True,
        )

    return {
        "status":  "started",
        "version": version,
        "message": f"Training started for {version}. Results will appear in Model History when complete.",
        "log":     str(log_path),
    }


@router.get("/health/services", response_model=ServicesHealth)
def get_services_health():
    """
    Checks the health of each logical service by inspecting output file
    freshness and registry state. No external processes are pinged.
    """
    now = datetime.now(timezone.utc)
    STALE_SECONDS = int(os.environ.get("SERVICE_STALE_SECONDS", "3600"))  # default: 1 h; override via env in prod

    def _file_age(path: Path) -> float | None:
        """Return seconds since last modification, or None if missing."""
        if not path.exists():
            return None
        return (now.timestamp() - path.stat().st_mtime)

    def _file_status(path: Path, label: str) -> ServiceStatus:
        age = _file_age(path)
        if age is None:
            return ServiceStatus(name=label, status="down", detail="Output file not found", last_updated=None)
        if age > STALE_SECONDS:
            ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            return ServiceStatus(name=label, status="degraded", detail=f"Last updated {int(age // 60)} min ago", last_updated=ts)
        ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return ServiceStatus(name=label, status="ok", detail=None, last_updated=ts)

    services: list[ServiceStatus] = []

    # Prediction API — predictions.csv must exist and be fresh
    services.append(_file_status(OUT_DIR / "predictions.csv", "Prediction API"))

    # Data Pipeline — pipeline.log must exist and have recent entries
    services.append(_file_status(OUT_DIR / "pipeline.log", "Data Pipeline"))

    # Feature Store — score_distribution.json written each scoring run
    services.append(_file_status(OUT_DIR / "score_distribution.json", "Feature Store"))

    # Model Serving — active model loaded in registry
    try:
        meta = registry.get_active_version_meta()
        if meta:
            services.append(ServiceStatus(name="Model Serving", status="ok",
                                          detail=f"Active: {meta.get('version_id', 'unknown')}",
                                          last_updated=None))
        else:
            services.append(ServiceStatus(name="Model Serving", status="down",
                                          detail="No active model in registry", last_updated=None))
    except Exception as exc:
        services.append(ServiceStatus(name="Model Serving", status="down",
                                      detail=str(exc), last_updated=None))

    # Drift Monitor — drift_report.json freshness
    services.append(_file_status(OUT_DIR / "drift_report.json", "Drift Monitor"))

    return ServicesHealth(services=services, checked_at=now)
