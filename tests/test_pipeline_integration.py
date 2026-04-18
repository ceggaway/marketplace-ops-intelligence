"""
Pipeline Integration Tests
==========================
End-to-end smoke test: generate synthetic data → preprocess → score (mocked model)
→ generate recommendations.

These tests verify that data flows correctly through the full pipeline and that
each stage produces outputs conforming to the expected schema.

Covers:
- generate_synthetic_data() → build_features() produces expected feature columns
- build_features() output has no infinities in numeric columns
- run_batch() with a mocked model produces predictions.csv with correct schema
- risk_level values are one of {high, medium, low}
- delay_risk_score (shortage probability) is in [0, 1]
- generate_recommendations() returns a DataFrame with one row per input zone
- Recommendation output contains required action columns
"""

import json
import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from backend.ingestion.loader import generate_synthetic_data
from backend.preprocessing.pipeline import build_features
from backend.recommendations.engine import generate_recommendations


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_mock_model(n_rows: int, prob: float = 0.5):
    """Return a mock sklearn-compatible model returning constant probabilities."""
    mock = MagicMock()
    probs = np.full((n_rows, 2), [1 - prob, prob])
    mock.predict_proba = MagicMock(return_value=probs)
    return mock


def _build_feature_df(days: int = 3) -> pd.DataFrame:
    """Run synthetic generation + feature engineering. Returns feature_df."""
    raw = generate_synthetic_data(days=days, seed=42)
    return build_features(raw)


# ── Ingestion → Preprocessing ─────────────────────────────────────────────────

def test_build_features_produces_required_columns():
    feature_df = _build_feature_df(days=2)
    expected = [
        "zone_id", "zone_name", "region", "timestamp",
        "taxi_count", "hour_of_day", "day_of_week", "is_peak_hour",
        "depletion_rate_1h", "supply_vs_yesterday",
        "rainfall_mm", "is_raining", "zone_type_encoded",
    ]
    for col in expected:
        assert col in feature_df.columns, f"Missing column after build_features: {col}"


def test_build_features_no_infinity_in_numerics():
    feature_df = _build_feature_df(days=2)
    numeric = feature_df.select_dtypes(include=[np.number])
    assert not np.isinf(numeric.values).any(), "Infinity found in feature DataFrame"


def test_build_features_all_55_zones_present():
    feature_df = _build_feature_df(days=2)
    assert feature_df["zone_id"].nunique() == 55


def test_build_features_drops_lag_nan_rows():
    """After build_features, taxi_lag_1h should have no NaN (first-rows dropped)."""
    feature_df = _build_feature_df(days=2)
    assert feature_df["taxi_lag_1h"].isna().sum() == 0


# ── Preprocessing → Scoring (mocked model) ───────────────────────────────────

def test_run_batch_predictions_schema(tmp_path):
    from backend.scoring.batch_scorer import run_batch

    feature_df = _build_feature_df(days=2)
    n = len(feature_df)
    mock_model = _make_mock_model(n, prob=0.55)
    mock_meta  = {"version_id": "vtest", "columns": list(feature_df.columns)}

    with patch("backend.scoring.batch_scorer.OUTPUTS_DIR", tmp_path), \
         patch("backend.scoring.batch_scorer.registry.get_active_model", return_value=mock_model), \
         patch("backend.scoring.batch_scorer.registry.get_active_version_meta", return_value=mock_meta):
        meta = run_batch(feature_df)

    predictions_path = tmp_path / "predictions.csv"
    assert predictions_path.exists()
    preds = pd.read_csv(predictions_path)
    for col in ["zone_id", "zone_name", "region", "delay_risk_score", "risk_level"]:
        assert col in preds.columns, f"Missing column in predictions: {col}"


def test_run_batch_risk_levels_valid(tmp_path):
    from backend.scoring.batch_scorer import run_batch

    feature_df = _build_feature_df(days=2)
    n = len(feature_df)
    mock_model = _make_mock_model(n, prob=0.55)
    mock_meta  = {"version_id": "vtest", "columns": list(feature_df.columns)}

    with patch("backend.scoring.batch_scorer.OUTPUTS_DIR", tmp_path), \
         patch("backend.scoring.batch_scorer.registry.get_active_model", return_value=mock_model), \
         patch("backend.scoring.batch_scorer.registry.get_active_version_meta", return_value=mock_meta):
        run_batch(feature_df)

    preds = pd.read_csv(tmp_path / "predictions.csv")
    valid_levels = {"high", "medium", "low"}
    assert set(preds["risk_level"].unique()).issubset(valid_levels)


def test_run_batch_scores_in_zero_one(tmp_path):
    from backend.scoring.batch_scorer import run_batch

    feature_df = _build_feature_df(days=2)
    n = len(feature_df)
    mock_model = _make_mock_model(n, prob=0.30)
    mock_meta  = {"version_id": "vtest", "columns": list(feature_df.columns)}

    with patch("backend.scoring.batch_scorer.OUTPUTS_DIR", tmp_path), \
         patch("backend.scoring.batch_scorer.registry.get_active_model", return_value=mock_model), \
         patch("backend.scoring.batch_scorer.registry.get_active_version_meta", return_value=mock_meta):
        run_batch(feature_df)

    preds = pd.read_csv(tmp_path / "predictions.csv")
    assert preds["delay_risk_score"].between(0.0, 1.0).all()


def test_run_batch_returns_meta_keys(tmp_path):
    from backend.scoring.batch_scorer import run_batch

    feature_df = _build_feature_df(days=2)
    n = len(feature_df)
    mock_model = _make_mock_model(n)
    mock_meta  = {"version_id": "vtest", "columns": list(feature_df.columns)}

    with patch("backend.scoring.batch_scorer.OUTPUTS_DIR", tmp_path), \
         patch("backend.scoring.batch_scorer.registry.get_active_model", return_value=mock_model), \
         patch("backend.scoring.batch_scorer.registry.get_active_version_meta", return_value=mock_meta):
        meta = run_batch(feature_df)

    for key in ["run_id", "rows_scored", "failed_rows", "flagged_zones", "run_status"]:
        assert key in meta, f"Missing meta key: {key}"


# ── Scoring → Recommendations ─────────────────────────────────────────────────

def test_generate_recommendations_row_count(tmp_path):
    preds = pd.DataFrame([
        {"zone_id": i, "zone_name": f"Zone{i}", "region": "Central",
         "delay_risk_score": 0.8 if i % 3 == 0 else 0.3,
         "risk_level": "high" if i % 3 == 0 else "low",
         "taxi_count": 50, "depletion_rate_1h": 0.2,
         "supply_vs_yesterday": 0.9, "explanation_tag": "test"}
        for i in range(1, 11)
    ])
    with patch("backend.recommendations.engine.OUTPUTS_DIR", tmp_path):
        recs = generate_recommendations(preds)
    assert len(recs) == len(preds)


def test_generate_recommendations_required_columns(tmp_path):
    preds = pd.DataFrame([{
        "zone_id": 1, "zone_name": "Orchard", "region": "Central",
        "delay_risk_score": 0.85, "risk_level": "high",
        "taxi_count": 40, "depletion_rate_1h": 0.45,
        "supply_vs_yesterday": 0.60, "explanation_tag": "rain surge",
    }])
    with patch("backend.recommendations.engine.OUTPUTS_DIR", tmp_path):
        recs = generate_recommendations(preds)
    for col in ["zone_id", "zone_name", "risk_level", "recommendation", "priority"]:
        assert col in recs.columns, f"Missing recommendation column: {col}"


def test_generate_recommendations_priority_values(tmp_path):
    preds = pd.DataFrame([
        {"zone_id": i, "zone_name": f"Z{i}", "region": "Central",
         "delay_risk_score": score, "risk_level": rl,
         "taxi_count": 50, "depletion_rate_1h": 0.1,
         "supply_vs_yesterday": 1.0, "explanation_tag": ""}
        for i, (score, rl) in enumerate([
            (0.85, "high"), (0.55, "medium"), (0.20, "low")
        ], start=1)
    ])
    with patch("backend.recommendations.engine.OUTPUTS_DIR", tmp_path):
        recs = generate_recommendations(preds)
    valid_priorities = {"critical", "high", "medium", "low"}
    assert set(recs["priority"].unique()).issubset(valid_priorities)
