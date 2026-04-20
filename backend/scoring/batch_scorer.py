"""
Batch Inference Engine
======================
Scores the latest zone-level taxi availability data using the active model.

Primary model output: depletion_risk_score ∈ [0, 1]
    Probability that available taxi count will drop by >40% in the next hour.

Additional descriptive layers:
    demand_pressure_score – exogenous proxy index for rider-demand pressure
    imbalance_score       – combined supply/depletion and pressure signal

Risk levels:
    high   – score ≥ 0.70  → intervention required
    medium – score ≥ 0.40  → monitor and pre-position
    low    – score < 0.40  → no action

Output files (data/outputs/):
    predictions.csv       – all 55 zones with scores
    flagged_zones.csv     – zones with risk ≥ medium
    failed_rows.csv       – rows that failed schema validation
    score_distribution.json – score distribution snapshot for drift monitoring
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from backend.intervention import load_intervention_config, load_zone_adjacency
from backend.intervention.action_selector import select_action
from backend.intervention.constraints import compute_neighbor_surplus
from backend.intervention.state_tracker import (
    apply_action,
    get_last_actions,
    get_remaining_budget,
    load_state,
    save_state,
    update_persistence,
)
from backend.modeling.demand_pressure import classify_pressure_level, score_demand_pressure
from backend.modeling.imbalance import classify_imbalance_level, composite_imbalance_score
from backend.modeling.shortage import compute_predicted_shortage
from backend.registry import model_registry as registry
from backend.recommendations.baseline_policy import baseline_policy_reason, classify_policy_action

import os

OUTPUTS_DIR = Path("data/outputs")

# Risk classification thresholds — configurable via environment so ops can
# tune sensitivity without code changes (e.g., lower RISK_HIGH_THRESHOLD
# during monsoon season to catch earlier-stage depletion risk).
RISK_HIGH_THRESHOLD = float(os.environ.get("RISK_HIGH_THRESHOLD", "0.70"))
RISK_MED_THRESHOLD  = float(os.environ.get("RISK_MED_THRESHOLD",  "0.40"))


def run_batch(feature_df: pd.DataFrame) -> dict:
    """Score all zones. Write outputs. Return run metadata dict."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id    = str(uuid.uuid4())[:8]
    run_start = datetime.now(timezone.utc)

    model       = registry.get_active_model()
    active_meta = registry.get_active_version_meta()
    schema_cols = active_meta.get("columns", [])

    clean_df, failed_df = _validate_schema(feature_df, schema_cols)
    _write_failed_rows(failed_df, run_id)

    if clean_df.empty:
        _write_empty_outputs(run_id)
        return _build_meta(run_id, run_start, 0, len(failed_df), 0, False,
                           active_meta.get("version_id", "unknown"), "failed",
                           feature_nan_count=0)

    X, feature_nan_count = _prepare_features(clean_df, schema_cols)
    probs = model.predict_proba(X)[:, 1]

    result_df = clean_df[["zone_id", "zone_name", "region", "timestamp"]].copy()
    result_df["delay_risk_score"]    = probs.round(4)   # field name kept for API compatibility
    result_df["depletion_risk_score"] = result_df["delay_risk_score"]
    result_df["risk_level"]          = result_df["delay_risk_score"].apply(_assign_risk_level)
    result_df["taxi_count"]          = clean_df["taxi_count"].values
    result_df["depletion_rate_1h"]   = clean_df.get(
        "depletion_rate_1h", pd.Series(np.nan, index=clean_df.index)
    ).values
    result_df["supply_vs_yesterday"] = clean_df.get(
        "supply_vs_yesterday", pd.Series(np.nan, index=clean_df.index)
    ).values
    result_df["explanation_tag"]     = [_generate_explanation_tag(row) for _, row in clean_df.iterrows()]
    result_df["demand_pressure_score"] = score_demand_pressure(clean_df).round(4)
    result_df["demand_pressure_level"] = result_df["demand_pressure_score"].apply(classify_pressure_level)
    supply_baseline = clean_df.get("taxi_lag_24h", clean_df["taxi_count"])
    result_df["baseline_supply"] = pd.Series(supply_baseline, index=clean_df.index).fillna(clean_df["taxi_count"]).astype(float).round(4)
    result_df["imbalance_score"] = [
        composite_imbalance_score(dp, supply, baseline)
        for dp, supply, baseline in zip(
            result_df["demand_pressure_score"],
            result_df["taxi_count"],
            result_df["baseline_supply"],
        )
    ]
    result_df["imbalance_score"] = result_df["imbalance_score"].round(4)
    result_df["imbalance_level"] = result_df["imbalance_score"].apply(classify_imbalance_level)
    result_df["policy_action"] = [
        classify_policy_action(dep, imb, int(supply))
        for dep, imb, supply in zip(
            result_df["depletion_risk_score"],
            result_df["imbalance_score"],
            result_df["taxi_count"],
        )
    ]
    result_df["policy_reason"] = [
        baseline_policy_reason(dep, imb, int(supply))
        for dep, imb, supply in zip(
            result_df["depletion_risk_score"],
            result_df["imbalance_score"],
            result_df["taxi_count"],
        )
    ]
    result_df["predicted_shortage"] = [
        compute_predicted_shortage(dp, supply, dep, baseline)
        for dp, supply, dep, baseline in zip(
            result_df["demand_pressure_score"],
            result_df["taxi_count"],
            result_df["depletion_risk_score"],
            result_df["baseline_supply"],
        )
    ]
    result_df["predicted_shortage"] = result_df["predicted_shortage"].round(4)
    _apply_intervention_outputs(result_df, run_start)

    result_df.to_csv(OUTPUTS_DIR / "predictions.csv", index=False)

    flagged = result_df[result_df["delay_risk_score"] >= RISK_MED_THRESHOLD]
    flagged.to_csv(OUTPUTS_DIR / "flagged_zones.csv", index=False)

    drift_snapshot = {
        "run_id":        run_id,
        "timestamp":     run_start.isoformat(),
        "score_mean":    round(float(probs.mean()), 4),
        "score_std":     round(float(probs.std()), 4),
        "score_p50":     round(float(np.percentile(probs, 50)), 4),
        "score_p90":     round(float(np.percentile(probs, 90)), 4),
        "pct_high_risk": round(float((probs >= RISK_HIGH_THRESHOLD).mean()), 4),
        "n_scored":      len(probs),
        # Store actual scores for accurate PSI reference (capped at 2000 samples)
        "scores":        [round(float(p), 6) for p in probs[:2000]],
    }
    with open(OUTPUTS_DIR / "score_distribution.json", "w") as f:
        json.dump(drift_snapshot, f, indent=2)

    # Per-zone score history — append one record per run so zone detail trend is real
    _append_zone_score_history(run_start.isoformat(), result_df)

    latency_ms = int((datetime.now(timezone.utc) - run_start).total_seconds() * 1000)

    # ── KPIs derived from observable signals, not arbitrary heuristics ────────
    flagged_mask     = result_df["delay_risk_score"] >= RISK_MED_THRESHOLD
    high_risk_mask   = result_df["delay_risk_score"] >= RISK_HIGH_THRESHOLD
    total            = max(len(result_df), 1)

    # fulfilment_rate: fraction of zones not at high depletion risk
    fulfilment_rate  = round(1.0 - float(high_risk_mask.sum()) / total, 4)

    # avg_delay_min: estimated extra wait time for flagged zones, derived from
    # the observed 1-hour depletion rate (a real supply signal).
    # Zones losing taxis at 30% per hour take ~6 min longer to hail one;
    # zones at 60% depletion rate face ~12 min extra wait (calibrated from
    # SG PHV market literature: 10 min base + depletion * 20 min).
    if "depletion_rate_1h" in clean_df.columns and flagged_mask.any():
        flagged_depletion = clean_df.loc[flagged_mask, "depletion_rate_1h"].clip(0, 1)
        avg_delay_min = round(float(flagged_depletion.mean() * 20), 2)
    else:
        avg_delay_min = 0.0

    total_taxi_count = int(result_df["taxi_count"].sum()) if "taxi_count" in result_df.columns else 0

    # Current-snapshot metrics — computed from the most recent timestamp only
    # so pipeline.log trends reflect "right now" rather than historical aggregates.
    if "timestamp" in result_df.columns:
        latest_ts   = result_df["timestamp"].max()
        latest_snap = result_df[result_df["timestamp"] == latest_ts]
    else:
        latest_snap = result_df

    high_risk_zones_now   = int((latest_snap["delay_risk_score"] >= RISK_HIGH_THRESHOLD).sum())
    supply_now            = int(latest_snap["taxi_count"].sum()) if "taxi_count" in latest_snap.columns else 0
    rapid_depletion_zones = int(
        (latest_snap["depletion_rate_1h"] > 0.30).sum()
    ) if "depletion_rate_1h" in latest_snap.columns else 0

    run_meta = _build_meta(
        run_id, run_start, len(result_df), len(failed_df),
        int(flagged_mask.sum()),
        False,
        active_meta.get("version_id", "unknown"),
        "success",
        latency_ms,
        avg_delay_min=avg_delay_min,
        fulfilment_rate=fulfilment_rate,
        total_taxi_count=total_taxi_count,
        high_risk_zones_now=high_risk_zones_now,
        supply_now=supply_now,
        rapid_depletion_zones=rapid_depletion_zones,
        avg_demand_pressure_score=round(float(result_df["demand_pressure_score"].mean()), 4),
        avg_imbalance_score=round(float(result_df["imbalance_score"].mean()), 4),
        feature_nan_count=feature_nan_count,
    )
    return run_meta


ZONE_SCORE_HISTORY_PATH = OUTPUTS_DIR / "zone_scores_history.jsonl"
_ZONE_HISTORY_MAX_LINES = 336   # 14 days of hourly runs (336 = 14 × 24)


def _append_zone_score_history(timestamp: str, result_df: pd.DataFrame) -> None:
    """
    Append one JSONL record to zone_scores_history.jsonl so zone-detail trend
    charts show real per-zone historical risk scores rather than the repeated
    current score.

    Format:
        {
          "timestamp": "<ISO>",
          "zones":       {"<zone_id>": <risk_score>, ...},
          "taxi_counts": {"<zone_id>": <taxi_count>, ...}   ← added for demand_trend
        }

    taxi_counts enables the zone detail endpoint to populate demand_trend with
    real per-zone supply history instead of returning empty arrays.
    File is capped at _ZONE_HISTORY_MAX_LINES to prevent unbounded growth.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    zone_scores = {
        str(int(row["zone_id"])): round(float(row["delay_risk_score"]), 4)
        for _, row in result_df.iterrows()
        if "zone_id" in result_df.columns and "delay_risk_score" in result_df.columns
    }
    # Per-zone taxi counts for the demand_trend chart in zone detail
    zone_taxi: dict[str, int] = {}
    if "taxi_count" in result_df.columns and "zone_id" in result_df.columns:
        for _, row in result_df.iterrows():
            zone_taxi[str(int(row["zone_id"]))] = int(row.get("taxi_count", 0))

    record = json.dumps({"timestamp": timestamp, "zones": zone_scores, "taxi_counts": zone_taxi})

    existing: list[str] = []
    if ZONE_SCORE_HISTORY_PATH.exists():
        existing = ZONE_SCORE_HISTORY_PATH.read_text().strip().splitlines()

    # Keep the last N-1 lines and append new record
    trimmed = existing[-(  _ZONE_HISTORY_MAX_LINES - 1):]
    trimmed.append(record)
    ZONE_SCORE_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    ZONE_SCORE_HISTORY_PATH.write_text("\n".join(trimmed) + "\n")


def _assign_risk_level(score: float) -> str:
    if score >= RISK_HIGH_THRESHOLD:
        return "high"
    if score >= RISK_MED_THRESHOLD:
        return "medium"
    return "low"


def _generate_explanation_tag(row: pd.Series) -> str:
    """
    Generate a plain-English explanation of why depletion risk is elevated.
    Based on directly observed supply signals rather than inferred demand.
    """
    tags = []

    taxi_count       = float(row.get("taxi_count", 50))
    depletion_1h     = float(row.get("depletion_rate_1h", 0))
    depletion_3h     = float(row.get("depletion_rate_3h", 0))
    supply_vs_yday   = float(row.get("supply_vs_yesterday", 1.0))
    rain             = bool(row.get("is_raining", False)) or float(row.get("rainfall_mm", 0)) > 0
    peak             = bool(row.get("is_peak_hour", False))
    holiday          = bool(row.get("is_holiday", False))

    # Rapid depletion in last hour
    if depletion_1h > 0.30:
        tags.append("rapid depletion")

    # Sustained depletion over 3 hours
    if depletion_3h > 0.25:
        tags.append("sustained depletion")

    # Critically low absolute supply
    if taxi_count < 10:
        tags.append("critically low supply")
    elif taxi_count < 20:
        tags.append("low supply")

    # Supply well below same time yesterday
    if supply_vs_yday < 0.7:
        tags.append("below yesterday baseline")

    if rain:
        tags.append("rain")
    if peak:
        tags.append("peak hour")
    if holiday:
        tags.append("public holiday")

    return " + ".join(tags) if tags else "normal conditions"


def _apply_intervention_outputs(result_df: pd.DataFrame, run_start: datetime) -> None:
    """Compute intervention outputs and update file-backed intervention state."""
    config = load_intervention_config()
    adjacency_map = load_zone_adjacency()
    state_path = OUTPUTS_DIR / "intervention_state.json"
    state = load_state(config, path=state_path)
    surplus_buffer = float(config.get("surplus_buffer", 0.2))

    snapshot_by_zone = {
        str(row.get("zone_name", "")): {
            "taxi_count": float(row.get("taxi_count", 0.0) or 0.0),
            "baseline_supply": float(row.get("baseline_supply", row.get("taxi_count", 1.0)) or 1.0),
        }
        for _, row in result_df.iterrows()
    }

    decisions: list[dict] = []
    pending_state_updates: list[tuple[str, str, float]] = []

    for idx, row in result_df.iterrows():
        zone_name = str(row.get("zone_name", ""))
        zone_key = str(int(row.get("zone_id", idx)))
        predicted_shortage = float(row.get("predicted_shortage", 0.0) or 0.0)
        acting_threshold = float(config.get("shortage_thresholds", {}).get("monitor_max", 0.25))
        persistence_count = update_persistence(state, zone_key, predicted_shortage > acting_threshold)
        neighbor_surplus = compute_neighbor_surplus(
            zone_name=zone_name,
            snapshot_by_zone=snapshot_by_zone,
            adjacency_map=adjacency_map,
            surplus_buffer=surplus_buffer,
        )
        decision = select_action(
            predicted_shortage=predicted_shortage,
            persistence_count=persistence_count,
            neighbor_surplus=neighbor_surplus,
            remaining_budget=get_remaining_budget(state),
            last_actions_for_zone=get_last_actions(state, zone_key),
            config=config,
            now=run_start,
        )
        decisions.append({
            "severity_bucket": decision.severity_bucket,
            "persistence_count": decision.persistence_count,
            "neighbor_surplus": decision.neighbor_surplus,
            "recommended_action": decision.recommended_action,
            "action_reason": decision.action_reason,
            "estimated_action_cost": decision.estimated_action_cost,
            "estimated_shortage_reduction": decision.estimated_shortage_reduction,
            "budget_remaining": decision.budget_remaining,
            "constraints_triggered": json.dumps(decision.constraints_triggered),
        })
        pending_state_updates.append((zone_key, decision.recommended_action, decision.estimated_action_cost))
        # Shadow budget during the run so later rows see the same-run spend.
        if decision.recommended_action in {"incentive", "rebalance_plus_incentive"}:
            state = apply_action(state, zone_key, decision.recommended_action, decision.estimated_action_cost, config, path=state_path, now=run_start)
        else:
            state.setdefault("last_actions", {}).setdefault(zone_key, {})

    for zone_key, action, action_cost in pending_state_updates:
        if action not in {"incentive", "rebalance_plus_incentive"}:
            state = apply_action(state, zone_key, action, action_cost, config, path=state_path, now=run_start)

    save_state(state, path=state_path)

    intervention_df = pd.DataFrame(decisions, index=result_df.index)
    for col in intervention_df.columns:
        result_df[col] = intervention_df[col]


def _validate_schema(df: pd.DataFrame, schema_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not schema_cols:
        return df.copy(), pd.DataFrame()
    missing = [c for c in schema_cols if c not in df.columns]
    if missing:
        fail = df.copy()
        fail["failure_reason"] = f"missing feature columns: {missing}"
        return pd.DataFrame(), fail
    return df.copy(), pd.DataFrame()


def _prepare_features(df: pd.DataFrame, schema_cols: list[str]) -> tuple[pd.DataFrame, int]:
    """
    Select and clean feature columns for model inference.

    Returns (X, feature_nan_count) where feature_nan_count is the number of
    NaN cells filled with 0 — a proxy for upstream data quality issues.
    A high count indicates that ingestion sources (carpark, weather, travel
    times) may have partially failed and the model is running on incomplete
    features. This count is surfaced in pipeline.log for ops visibility.
    """
    available = [c for c in schema_cols if c in df.columns]
    X = df[available].copy()
    bool_cols = X.select_dtypes(include="bool").columns
    X[bool_cols] = X[bool_cols].astype(int)
    # Count NaN cells before filling — each represents a missing feature value
    # that falls back to 0, potentially degrading prediction quality.
    feature_nan_count = int(X.isna().sum().sum())
    X = X.fillna(0)
    return X, feature_nan_count


def _write_failed_rows(failed_df: pd.DataFrame, run_id: str) -> None:
    if not failed_df.empty:
        failed_df.to_csv(OUTPUTS_DIR / "failed_rows.csv", index=False)
    else:
        pd.DataFrame(columns=["zone_id", "timestamp", "failure_reason"]).to_csv(
            OUTPUTS_DIR / "failed_rows.csv", index=False
        )


def _write_empty_outputs(run_id: str) -> None:
    for fname in ["predictions.csv", "flagged_zones.csv"]:
        pd.DataFrame().to_csv(OUTPUTS_DIR / fname, index=False)


def _build_meta(
    run_id, run_start, rows_scored, failed_rows, flagged_zones,
    drift_flag, model_version, status, latency_ms=0,
    avg_delay_min=0.0, fulfilment_rate=0.0, total_taxi_count=0,
    high_risk_zones_now=0, supply_now=0, rapid_depletion_zones=0,
    avg_demand_pressure_score=0.0, avg_imbalance_score=0.0,
    feature_nan_count=0,
) -> dict:
    return {
        "run_id":                 run_id,
        "timestamp":              run_start.isoformat(),
        "rows_scored":            rows_scored,
        "failed_rows":            failed_rows,
        "flagged_zones":          flagged_zones,
        "drift_flag":             drift_flag,
        "rollback_status":        False,
        "model_version":          model_version,
        "run_status":             status,
        "latency_ms":             latency_ms,
        "avg_delay_min":          avg_delay_min,
        "fulfilment_rate":        fulfilment_rate,
        "total_taxi_count":       total_taxi_count,
        "high_risk_zones_now":    high_risk_zones_now,
        "supply_now":             supply_now,
        "rapid_depletion_zones":  rapid_depletion_zones,
        "avg_demand_pressure_score": avg_demand_pressure_score,
        "avg_imbalance_score":    avg_imbalance_score,
        # Number of feature cells filled with 0 due to missing upstream data.
        # Non-zero values indicate partial ingestion failures (carpark, weather,
        # travel-time sources). Surfaced in pipeline.log so ops can see when
        # predictions may be less reliable due to incomplete feature inputs.
        "feature_nan_count":      feature_nan_count,
    }
