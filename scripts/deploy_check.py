#!/usr/bin/env python
"""Deployment readiness checks for non-Docker deployments."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.paths import data_dir, logs_dir, outputs_dir, processed_dir, raw_dir, registry_dir


REQUIRED_MODEL_FILES = ["model.pkl", "metrics.json", "feature_schema.json", "version_meta.json"]


def _ok(message: str) -> None:
    print(f"OK   {message}")


def _warn(message: str) -> None:
    print(f"WARN {message}")


def _fail(failures: list[str], message: str) -> None:
    print(f"FAIL {message}")
    failures.append(message)


def _is_writable(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".write_probe"
    try:
        probe.write_text("ok")
        probe.unlink()
        return True
    except Exception:
        return False


def _load_registry(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Check deployment readiness.")
    parser.add_argument("--require-frontend-build", action="store_true")
    parser.add_argument("--require-predictions", action="store_true")
    parser.add_argument(
        "--max-prediction-age-min",
        type=int,
        default=int(os.environ.get("MARKETPLACE_MAX_PREDICTION_AGE_MIN", "120")),
    )
    args = parser.parse_args()

    failures: list[str] = []
    print(f"Data dir: {data_dir().resolve()}")

    for label, path in [
        ("data", data_dir()),
        ("raw", raw_dir()),
        ("processed", processed_dir()),
        ("outputs", outputs_dir()),
        ("registry", registry_dir()),
        ("logs", logs_dir()),
    ]:
        if _is_writable(path):
            _ok(f"{label} directory writable: {path}")
        else:
            _fail(failures, f"{label} directory is not writable: {path}")

    registry_path = registry_dir() / "registry.json"
    registry = _load_registry(registry_path)
    active = registry.get("active_version")
    if not active:
        _fail(failures, f"No active_version in {registry_path}")
    else:
        _ok(f"active model version: {active}")
        model_dir = registry_dir() / "models" / active
        missing = [name for name in REQUIRED_MODEL_FILES if not (model_dir / name).exists()]
        if missing:
            _fail(failures, f"Active model artifact incomplete at {model_dir}; missing {missing}")
        else:
            _ok(f"active model artifact complete: {model_dir}")

    frontend_dist = PROJECT_ROOT / "frontend-react" / "dist" / "index.html"
    if args.require_frontend_build:
        if frontend_dist.exists():
            _ok(f"frontend production build exists: {frontend_dist}")
        else:
            _fail(failures, "frontend-react/dist/index.html missing; run npm --prefix frontend-react run build")
    elif not frontend_dist.exists():
        _warn("frontend production build missing; pass --require-frontend-build to enforce")

    pred_path = outputs_dir() / "predictions.csv"
    if pred_path.exists():
        age_min = (datetime.now(timezone.utc).timestamp() - pred_path.stat().st_mtime) / 60
        if age_min <= args.max_prediction_age_min:
            _ok(f"predictions.csv fresh enough ({age_min:.1f} min old)")
        else:
            _fail(failures, f"predictions.csv stale ({age_min:.1f} min old > {args.max_prediction_age_min} min)")
    elif args.require_predictions:
        _fail(failures, f"predictions.csv missing at {pred_path}")
    else:
        _warn(f"predictions.csv missing at {pred_path}; first scoring run must populate it")

    if failures:
        print(f"\nDeployment check failed with {len(failures)} issue(s).")
        return 1
    print("\nDeployment check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
