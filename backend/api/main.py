"""
FastAPI Application Entry Point
================================
Starts the API server that the Vite/React frontend calls.

Run with:
    uvicorn backend.api.main:app --reload --port 8000

Docs available at:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/redoc  (ReDoc)
"""

import os
import subprocess
import sys
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routers import operational, ml_health, ai_chat, reports
from backend.api.routers import h3 as h3_router
from backend.paths import outputs_dir, registry_dir


def _auto_bootstrap_and_score() -> None:
    """Run bootstrap + scoring once on startup if predictions are missing."""
    project_root = Path(__file__).resolve().parents[2]
    predictions = outputs_dir() / "predictions.csv"
    active = (registry_dir() / "registry.json")

    if predictions.exists():
        return

    log_dir = project_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    with open(log_dir / "startup.log", "a") as f:
        if not active.exists():
            allow_synthetic = os.environ.get("BOOTSTRAP_ALLOW_SYNTHETIC", "false").lower() in {"1", "true", "yes"}
            cmd = [sys.executable, "scripts/bootstrap_model.py"]
            if allow_synthetic:
                cmd.append("--allow-synthetic")
            subprocess.run(cmd, cwd=str(project_root), stdout=f, stderr=f)

        subprocess.run(
            [sys.executable, "scripts/run_scoring.py"],
            cwd=str(project_root), stdout=f, stderr=f,
        )

app = FastAPI(
    title="Marketplace Ops Intelligence API",
    description="Backend API for the operations command center dashboard.",
    version="1.0.0",
    on_startup=[lambda: threading.Thread(target=_auto_bootstrap_and_score, daemon=True).start()],
)

# CORS_ORIGINS env var: comma-separated list of allowed origins.
# Defaults to localhost dev ports when not set.
_raw_origins = os.environ.get("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
_allow_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(operational.router,    prefix="/api/v1", tags=["operational"])
app.include_router(ml_health.router,     prefix="/api/v1", tags=["ml-health"])
app.include_router(ai_chat.router,       prefix="/api/v1", tags=["ai-chat"])
app.include_router(reports.router,       prefix="/api/v1", tags=["reports"])
app.include_router(h3_router.router,     prefix="/api/v1", tags=["h3-spatial"])


@app.get("/health")
def health_check():
    return {"status": "ok"}
