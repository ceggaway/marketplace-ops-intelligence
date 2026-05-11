#!/usr/bin/env python
"""Bootstrap or verify the active model artifact for deployment."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.paths import registry_dir
from backend.registry import model_registry as registry


REQUIRED_MODEL_FILES = ["model.pkl", "metrics.json", "feature_schema.json", "version_meta.json"]


def _active_model_complete() -> bool:
    active = registry._load_registry().get("active_version")
    if not active:
        return False
    model_dir = registry_dir() / "models" / active
    return all((model_dir / name).exists() for name in REQUIRED_MODEL_FILES)


def _copy_artifacts(source: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(f"MODEL_ARTIFACT_DIR does not exist: {source}")

    registry_dir().mkdir(parents=True, exist_ok=True)
    source_registry = source / "registry.json"
    if source_registry.exists():
        shutil.copy2(source_registry, registry_dir() / "registry.json")

    source_models = source / "models"
    if source_models.exists():
        destination = registry_dir() / "models"
        destination.mkdir(parents=True, exist_ok=True)
        for item in source_models.iterdir():
            target = destination / item.name
            if item.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)
    elif all((source / name).exists() for name in REQUIRED_MODEL_FILES):
        version_meta = json.loads((source / "version_meta.json").read_text())
        active = registry._load_registry().get("active_version") or version_meta.get("version_id") or source.name
        target = registry_dir() / "models" / active
        target.mkdir(parents=True, exist_ok=True)
        for name in REQUIRED_MODEL_FILES:
            shutil.copy2(source / name, target / name)
        reg_path = registry_dir() / "registry.json"
        if not reg_path.exists():
            reg_path.write_text(json.dumps({
                "active_version": active,
                "candidate_version": None,
                "versions": {
                    active: {
                        "status": "active",
                        "trained_at": version_meta.get("trained_at"),
                        "promoted_at": version_meta.get("promoted_at"),
                        "metrics": {},
                    }
                },
            }, indent=2) + "\n")
    else:
        raise FileNotFoundError(
            "MODEL_ARTIFACT_DIR must contain registry.json + models/ or the four model files directly."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure an active model artifact exists.")
    parser.add_argument("--artifact-dir", default=os.environ.get("MODEL_ARTIFACT_DIR", ""))
    parser.add_argument("--train-if-missing", action="store_true", default=os.environ.get("BOOTSTRAP_TRAIN_IF_MISSING", "false").lower() in {"1", "true", "yes"})
    parser.add_argument("--version", default=os.environ.get("BOOTSTRAP_TRAIN_VERSION", "v_bootstrap"))
    parser.add_argument("--allow-synthetic", action="store_true", default=os.environ.get("BOOTSTRAP_ALLOW_SYNTHETIC", "false").lower() in {"1", "true", "yes"})
    args = parser.parse_args()

    if _active_model_complete():
        print("Active model artifact already present.")
        return 0

    if args.artifact_dir:
        print(f"Copying model artifacts from {args.artifact_dir}")
        _copy_artifacts(Path(args.artifact_dir))
        if _active_model_complete():
            print("Active model artifact restored.")
            return 0

    if args.train_if_missing:
        cmd = [sys.executable, "scripts/run_training.py", "--version", args.version]
        if args.allow_synthetic:
            cmd.append("--allow-synthetic")
        print("Training model because active artifact is missing:")
        print(" ".join(cmd))
        result = subprocess.run(cmd, cwd=PROJECT_ROOT)
        if result.returncode != 0:
            return result.returncode
        return 0 if _active_model_complete() else 1

    print("Active model artifact missing. Set MODEL_ARTIFACT_DIR or BOOTSTRAP_TRAIN_IF_MISSING=true.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
