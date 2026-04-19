"""
Tests for backend/registry/model_registry.py

Covers:
- _load_registry() returns default when file missing
- register() adds version to registry and sets candidate_version
- register() raises FileNotFoundError when version_meta.json missing
- get_active_version_meta() returns empty dict when no active version
- get_active_version_meta() merges meta files correctly
- promote() marks version as active and previous active as 'previous'
- promote() raises KeyError for unknown version
- rollback() reverts to previous stable version
- rollback() raises RuntimeError when no previous version exists
- list_versions() returns the useful lineage by default
- cleanup_registry() prunes stale candidates and excess previous versions
"""

import json
import pickle
from pathlib import Path
from unittest.mock import patch

import pytest

from backend.registry import model_registry as registry


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_version(tmp_path: Path, version_id: str, metrics: dict | None = None) -> Path:
    """Create a minimal version directory with required artifact files."""
    version_dir = tmp_path / "models" / version_id
    version_dir.mkdir(parents=True, exist_ok=True)

    meta = {"version_id": version_id, "trained_at": "2024-01-01T00:00:00+00:00", "status": "candidate"}
    (version_dir / "version_meta.json").write_text(json.dumps(meta))

    m = metrics or {"precision": 0.80, "recall": 0.75, "f1": 0.77, "roc_auc": 0.88}
    (version_dir / "metrics.json").write_text(json.dumps(m))

    return version_dir


def _patch_registry(tmp_path: Path):
    """Return a context manager that redirects all registry paths to tmp_path."""
    reg_file = tmp_path / "registry.json"
    models_dir = tmp_path / "models"
    return (
        patch("backend.registry.model_registry.REGISTRY_FILE", reg_file),
        patch("backend.registry.model_registry.REGISTRY_DIR", tmp_path),
        patch("backend.registry.model_registry.MODELS_DIR", models_dir),
    )


# ── _load_registry ────────────────────────────────────────────────────────────

def test_load_registry_defaults_when_file_missing(tmp_path):
    with patch("backend.registry.model_registry.REGISTRY_FILE", tmp_path / "none.json"):
        reg = registry._load_registry()
    assert reg["active_version"] is None
    assert reg["candidate_version"] is None
    assert reg["versions"] == {}


# ── register ──────────────────────────────────────────────────────────────────

def test_register_adds_version(tmp_path):
    _make_version(tmp_path, "v1")
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        reg = registry._load_registry()
    assert "v1" in reg["versions"]
    assert reg["candidate_version"] == "v1"
    assert reg["versions"]["v1"]["status"] == "candidate"


def test_register_raises_when_meta_missing(tmp_path):
    # Create models dir but no version_meta.json
    (tmp_path / "models" / "v99").mkdir(parents=True)
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3, pytest.raises(FileNotFoundError):
        registry.register("v99")


def test_register_stores_metrics_subset(tmp_path):
    _make_version(tmp_path, "v1", metrics={"f1": 0.91, "roc_auc": 0.93, "precision": 0.88, "recall": 0.94})
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        reg = registry._load_registry()
    m = reg["versions"]["v1"]["metrics"]
    assert "f1" in m
    assert "roc_auc" in m


# ── get_active_version_meta ───────────────────────────────────────────────────

def test_get_active_version_meta_empty_when_none(tmp_path):
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        meta = registry.get_active_version_meta()
    assert meta == {}


def test_get_active_version_meta_merges_files(tmp_path):
    _make_version(tmp_path, "v2")
    reg_data = {
        "active_version": "v2",
        "candidate_version": None,
        "versions": {"v2": {"status": "active", "trained_at": None, "promoted_at": None, "metrics": {}}},
    }
    (tmp_path / "registry.json").write_text(json.dumps(reg_data))
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        meta = registry.get_active_version_meta()
    assert meta["version_id"] == "v2"
    assert "f1" in meta  # from metrics.json


# ── promote ───────────────────────────────────────────────────────────────────

def test_promote_marks_version_active(tmp_path):
    _make_version(tmp_path, "v1")
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        registry.promote("v1")
        reg = registry._load_registry()
    assert reg["active_version"] == "v1"
    assert reg["versions"]["v1"]["status"] == "active"
    assert reg["versions"]["v1"]["promoted_at"] is not None
    assert reg["candidate_version"] is None


def test_promote_demotes_previous_active(tmp_path):
    _make_version(tmp_path, "v1")
    _make_version(tmp_path, "v2")
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        registry.promote("v1")
        registry.register("v2")
        registry.promote("v2")
        reg = registry._load_registry()
    assert reg["versions"]["v1"]["status"] == "previous"
    assert reg["active_version"] == "v2"


def test_promote_raises_for_unknown_version(tmp_path):
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3, pytest.raises(KeyError):
        registry.promote("v_does_not_exist")


# ── rollback ──────────────────────────────────────────────────────────────────

def test_rollback_reverts_to_previous(tmp_path):
    _make_version(tmp_path, "v1")
    _make_version(tmp_path, "v2")
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        registry.promote("v1")
        registry.register("v2")
        registry.promote("v2")
        rolled_back = registry.rollback()
        reg = registry._load_registry()
    assert rolled_back == "v1"
    assert reg["active_version"] == "v1"
    assert reg["versions"]["v2"]["status"] == "rolled_back"


def test_rollback_raises_when_no_previous(tmp_path):
    _make_version(tmp_path, "v1")
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        registry.promote("v1")
        with pytest.raises(RuntimeError, match="No previous stable version"):
            registry.rollback()


# ── list_versions ─────────────────────────────────────────────────────────────

def test_list_versions_returns_all(tmp_path):
    _make_version(tmp_path, "v1")
    _make_version(tmp_path, "v2")
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        registry.register("v1")
        registry.register("v2")
        versions = registry.list_versions()
    ids = [v["version_id"] for v in versions]
    assert "v1" in ids
    assert "v2" in ids


def test_list_versions_sorted_descending(tmp_path):
    for vid in ["v1", "v2", "v3"]:
        _make_version(tmp_path, vid)
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        for vid in ["v1", "v2", "v3"]:
            registry.register(vid)
        versions = registry.list_versions()
    ids = [v["version_id"] for v in versions]
    assert ids == sorted(ids, reverse=True)


def test_list_versions_empty_when_registry_empty(tmp_path):
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        versions = registry.list_versions()
    assert versions == []


def test_list_versions_hides_stale_candidates_by_default(tmp_path):
    _make_version(tmp_path, "v1")
    _make_version(tmp_path, "v_good_candidate", metrics={"precision": 0.4, "recall": 0.6, "f1": 0.48, "roc_auc": 0.8})
    _make_version(tmp_path, "v_zero_candidate", metrics={"precision": 0.0, "recall": 0.0, "f1": 0.0, "roc_auc": None})
    reg_data = {
        "active_version": "v1",
        "candidate_version": "v_good_candidate",
        "versions": {
            "v1": {"status": "active", "trained_at": "2024-01-03T00:00:00+00:00", "promoted_at": "2024-01-03T01:00:00+00:00", "metrics": {"f1": 0.77}},
            "v_good_candidate": {"status": "candidate", "trained_at": "2024-01-04T00:00:00+00:00", "promoted_at": None, "metrics": {"precision": 0.4, "recall": 0.6, "f1": 0.48, "roc_auc": 0.8}},
            "v_zero_candidate": {"status": "candidate", "trained_at": "2024-01-01T00:00:00+00:00", "promoted_at": None, "metrics": {"precision": 0.0, "recall": 0.0, "f1": 0.0, "roc_auc": None}},
        },
    }
    (tmp_path / "registry.json").write_text(json.dumps(reg_data))
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        versions = registry.list_versions()
        all_versions = registry.list_versions(include_stale=True)
    ids = [v["version_id"] for v in versions]
    assert "v_good_candidate" in ids
    assert "v_zero_candidate" not in ids
    assert "v_zero_candidate" in [v["version_id"] for v in all_versions]


def test_cleanup_registry_prunes_stale_candidates_and_old_previous(tmp_path):
    for vid in ["v_active", "v_prev_1", "v_prev_2", "v_prev_3", "v_candidate", "v_stale"]:
        _make_version(tmp_path, vid)
    reg_data = {
        "active_version": "v_active",
        "candidate_version": "v_candidate",
        "versions": {
            "v_active": {"status": "active", "trained_at": "2024-01-06T00:00:00+00:00", "promoted_at": "2024-01-06T01:00:00+00:00", "metrics": {"f1": 0.8}},
            "v_prev_1": {"status": "previous", "trained_at": "2024-01-05T00:00:00+00:00", "promoted_at": "2024-01-05T01:00:00+00:00", "metrics": {"f1": 0.78}},
            "v_prev_2": {"status": "previous", "trained_at": "2024-01-04T00:00:00+00:00", "promoted_at": "2024-01-04T01:00:00+00:00", "metrics": {"f1": 0.75}},
            "v_prev_3": {"status": "previous", "trained_at": "2024-01-03T00:00:00+00:00", "promoted_at": "2024-01-03T01:00:00+00:00", "metrics": {"f1": 0.71}},
            "v_candidate": {"status": "candidate", "trained_at": "2024-01-07T00:00:00+00:00", "promoted_at": None, "metrics": {"precision": 0.5, "recall": 0.4, "f1": 0.44, "roc_auc": 0.77}},
            "v_stale": {"status": "candidate", "trained_at": "2024-01-02T00:00:00+00:00", "promoted_at": None, "metrics": {"precision": 0.0, "recall": 0.0, "f1": 0.0, "roc_auc": None}},
        },
    }
    (tmp_path / "registry.json").write_text(json.dumps(reg_data))
    p1, p2, p3 = _patch_registry(tmp_path)
    with p1, p2, p3:
        summary = registry.cleanup_registry()
        cleaned = registry._load_registry()
    assert set(summary["removed_versions"]) == {"v_stale", "v_prev_3"}
    assert "v_candidate" in cleaned["versions"]
    assert "v_stale" not in cleaned["versions"]
    assert "v_prev_3" not in cleaned["versions"]
