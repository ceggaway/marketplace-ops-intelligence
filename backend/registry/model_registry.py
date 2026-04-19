"""
Model Registry
==============
Manages model versioning, promotion, and rollback.

Storage layout (data/registry/):
    models/
        v{n}/
            model.pkl
            metrics.json
            feature_schema.json
            version_meta.json
    registry.json   ← index: active_version, candidate_version, versions{}

Key functions:
    register(version_id)         – add a trained candidate to the index
    get_active_model()           – load + return the active model artifact
    get_active_version_meta()    – return the active version metadata dict
    promote(version_id)          – mark a candidate as active
    rollback()                   – revert to previous stable version
    list_versions()              – return all version metadata
"""

import json
import pickle
from datetime import datetime, timezone
from pathlib import Path

REGISTRY_DIR  = Path("data/registry")
REGISTRY_FILE = REGISTRY_DIR / "registry.json"
MODELS_DIR    = REGISTRY_DIR / "models"
MAX_PREVIOUS_VERSIONS = 2


def _parse_iso(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _version_sort_key(version_id: str, info: dict) -> tuple[datetime, str]:
    return (
        max(
            _parse_iso(info.get("promoted_at")),
            _parse_iso(info.get("trained_at")),
        ),
        version_id,
    )


def _metrics_are_zeroish(metrics: dict) -> bool:
    values = [metrics.get(k) for k in ("precision", "recall", "f1")]
    normalised = []
    for value in values:
        try:
            normalised.append(float(value))
        except Exception:
            normalised.append(0.0)
    roc_auc = metrics.get("roc_auc")
    return all(v <= 0.0 for v in normalised) and roc_auc in (None, 0, 0.0)


def _stale_candidate_ids(reg: dict) -> set[str]:
    candidate_version = reg.get("candidate_version")
    stale: set[str] = set()
    for vid, info in reg.get("versions", {}).items():
        if vid == candidate_version:
            continue
        if info.get("status") != "candidate":
            continue
        metrics = info.get("metrics") or {}
        if _metrics_are_zeroish(metrics):
            stale.add(vid)
    return stale


def cleanup_registry(max_previous_versions: int = MAX_PREVIOUS_VERSIONS) -> dict:
    """
    Prune stale registry entries while keeping the active model, the current
    candidate under evaluation, and a short lineage of recent previous models.
    Returns a summary of what was removed.
    """
    reg = _load_registry()
    versions = reg.get("versions", {})
    removed: list[str] = []

    stale_candidates = _stale_candidate_ids(reg)
    for vid in stale_candidates:
        versions.pop(vid, None)
        removed.append(vid)

    previous_ids = [
        vid for vid, info in versions.items()
        if info.get("status") == "previous"
    ]
    previous_ids.sort(key=lambda vid: _version_sort_key(vid, versions[vid]), reverse=True)
    for vid in previous_ids[max_previous_versions:]:
        versions.pop(vid, None)
        removed.append(vid)

    if reg.get("candidate_version") and reg["candidate_version"] not in versions:
        reg["candidate_version"] = None
    if reg.get("active_version") and reg["active_version"] not in versions:
        reg["active_version"] = None

    if removed:
        _save_registry(reg)

    return {"removed_versions": removed}


def _load_registry() -> dict:
    if not REGISTRY_FILE.exists():
        return {"active_version": None, "candidate_version": None, "versions": {}}
    with open(REGISTRY_FILE) as f:
        return json.load(f)


def _save_registry(reg: dict) -> None:
    """Write registry to disk as valid JSON (NaN/Inf → null)."""
    import math

    def _sanitise(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: _sanitise(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitise(v) for v in obj]
        return obj

    REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(REGISTRY_FILE, "w") as f:
        json.dump(_sanitise(reg), f, indent=2)


def register(version_id: str) -> None:
    """Add a trained candidate to the registry index."""
    version_dir = MODELS_DIR / version_id
    meta_path   = version_dir / "version_meta.json"
    metrics_path = version_dir / "metrics.json"

    if not meta_path.exists():
        raise FileNotFoundError(f"version_meta.json not found for {version_id}")

    with open(meta_path) as f:
        meta = json.load(f)
    with open(metrics_path) as f:
        metrics = json.load(f)

    reg = _load_registry()
    reg["versions"][version_id] = {
        "status":       "candidate",
        "trained_at":   meta.get("trained_at"),
        "promoted_at":  None,
        "metrics":      {k: metrics[k] for k in ["precision","recall","f1","roc_auc"] if k in metrics},
    }
    reg["candidate_version"] = version_id
    _save_registry(reg)
    cleanup_registry()


def get_active_model():
    """Load and return the active model artifact (pickle)."""
    reg = _load_registry()
    active = reg.get("active_version")
    if not active:
        raise RuntimeError("No active model in registry. Run promotion first.")
    model_path = MODELS_DIR / active / "model.pkl"
    if not model_path.exists():
        raise FileNotFoundError(f"model.pkl not found for {active}")
    with open(model_path, "rb") as f:
        return pickle.load(f)


def get_active_version_meta() -> dict:
    """Return the active version metadata dict (merged meta + metrics)."""
    reg = _load_registry()
    active = reg.get("active_version")
    if not active:
        return {}
    version_dir = MODELS_DIR / active
    meta = {}
    for fname in ["version_meta.json", "metrics.json", "feature_schema.json"]:
        p = version_dir / fname
        if p.exists():
            with open(p) as f:
                meta.update(json.load(f))
    meta["version_id"] = active
    return meta


def get_version_meta(version_id: str) -> dict:
    """Return metadata for any specific version."""
    version_dir = MODELS_DIR / version_id
    meta = {}
    for fname in ["version_meta.json", "metrics.json"]:
        p = version_dir / fname
        if p.exists():
            with open(p) as f:
                meta.update(json.load(f))
    meta["version_id"] = version_id
    return meta


def promote(version_id: str) -> None:
    """Mark a candidate version as active."""
    reg = _load_registry()
    if version_id not in reg["versions"]:
        raise KeyError(f"Version {version_id} not in registry")

    now = datetime.now(timezone.utc).isoformat()

    # Demote current active to 'previous'
    current_active = reg.get("active_version")
    if current_active and current_active in reg["versions"]:
        reg["versions"][current_active]["status"] = "previous"

    reg["versions"][version_id]["status"]      = "active"
    reg["versions"][version_id]["promoted_at"] = now
    reg["active_version"]    = version_id
    reg["candidate_version"] = None

    # Update version_meta.json on disk too
    meta_path = MODELS_DIR / version_id / "version_meta.json"
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
        meta["status"]      = "active"
        meta["promoted_at"] = now
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

    _save_registry(reg)
    cleanup_registry()


def rollback() -> str:
    """Roll back to the previous stable version. Returns rolled-back version id."""
    reg = _load_registry()
    previous = None
    # Find most recent 'previous' version
    for vid, info in sorted(reg["versions"].items(), reverse=True):
        if info.get("status") == "previous":
            previous = vid
            break

    if not previous:
        raise RuntimeError("No previous stable version to roll back to")

    current_active = reg.get("active_version")
    if current_active and current_active in reg["versions"]:
        reg["versions"][current_active]["status"] = "rolled_back"

    now = datetime.now(timezone.utc).isoformat()
    reg["versions"][previous]["status"]      = "active"
    reg["versions"][previous]["promoted_at"] = now
    reg["active_version"] = previous
    _save_registry(reg)
    return previous


def list_versions(include_stale: bool = False, max_previous_versions: int = MAX_PREVIOUS_VERSIONS) -> list[dict]:
    """Return version metadata sorted by recency and filtered for useful lineage."""
    reg = _load_registry()
    stale_candidates = _stale_candidate_ids(reg) if not include_stale else set()
    previous_kept: set[str] = set()
    if not include_stale:
        previous_ids = [
            vid for vid, info in reg.get("versions", {}).items()
            if info.get("status") == "previous"
        ]
        previous_ids.sort(key=lambda vid: _version_sort_key(vid, reg["versions"][vid]), reverse=True)
        previous_kept = set(previous_ids[:max_previous_versions])

    result = []
    for vid, info in reg.get("versions", {}).items():
        status = info.get("status")
        if vid in stale_candidates:
            continue
        if not include_stale and status == "previous" and vid not in previous_kept:
            continue
        entry = {"version_id": vid, **info}
        result.append(entry)
    return sorted(result, key=lambda x: _version_sort_key(x["version_id"], x), reverse=True)
