"""
Recommendation Engine  v2
==========================
Converts risk scores + live supply signals into actionable ops interventions.

Improvements over v1:
  1. Zone-relative thresholds  — "critically low" is calibrated per zone using
     DOW × hour baselines from snapshot history, not a global count of 5.
  2. Network-aware push logic  — before recommending driver push, checks whether
     adjacent zones are also at risk. If they are, pricing is preferred because
     it doesn't pull shared driver supply from at-risk neighbours.
  3. Root-cause classification — distinguishes weather / structural_gap /
     demand_spike / peak_pattern so the right lever is chosen.
  4. Time gating              — compares time-to-critical (ETA) against each
     intervention's realistic lag. Push cannot help if ETA < 20 min.
  5. Action alternatives      — surfaces 2 alternatives with cost/impact so the
     ops person can compare rather than just follow the top recommendation.

Intervention lag constants (minutes):
    Pricing (demand side)  →  5 min  (riders see higher fare immediately)
    Pricing (supply side)  → 20 min  (drivers physically travel into zone)
    Push notification      → 20 min  (idle driver receives + travels)
    Escalation             →  2 min  (ops manager responds to alert)
"""

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.recommendations.action_catalog import candidate_actions_for_risk
from backend.recommendations.decision_optimizer import score_candidate_action
from backend.recommendations.policy_effectiveness import (
    action_type_from_recommendation,
    effectiveness_for_context,
    load_outcome_records,
)
from backend.recommendations.zone_graph import ZONE_NAMES, get_adjacent_ids

OUTPUTS_DIR   = Path("data/outputs")
SNAPSHOT_DIR  = Path("data/raw/taxi_snapshots")

RISK_HIGH = 0.70
RISK_MED  = 0.40

# Intervention lags (minutes)
LAG_PRICING_DEMAND = 5
LAG_PRICING_SUPPLY = 20
LAG_PUSH           = 20
LAG_ESCALATION     = 2

# Thresholds
FAST_DEPLETION        = 0.30   # >30 %/hr
CRITICAL_RATIO        = 0.25   # supply < 25% of zone baseline → critical low
STRUCTURAL_GAP_RATIO  = 0.75   # supply < 75% of DOW baseline → structural gap
SURGE_THRESHOLD_RATIO = 0.90   # supply < 90% of DOW baseline → mild surge


# ── DOW baseline ─────────────────────────────────────────────────────────────

def _load_dow_baselines() -> dict:
    """
    Compute zone × day-of-week × hour average taxi_count from snapshot history.
    Returns dict keyed (zone_id, dow, hour) → {"mean": float, "n": int}.
    Falls back to empty dict if insufficient data.
    """
    if not SNAPSHOT_DIR.exists():
        return {}

    from datetime import datetime as _dt
    dfs = []
    for f in sorted(SNAPSHOT_DIR.glob("*.csv"))[-2016:]:
        try:
            ts  = _dt.strptime(f.stem, "%Y-%m-%d_%H-%M")
            dow = ts.weekday()   # 0=Mon … 6=Sun
            hour = ts.hour
            df  = pd.read_csv(f)[["zone_id", "taxi_count"]].copy()
            df["dow"]  = dow
            df["hour"] = hour
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return {}

    all_data = pd.concat(dfs, ignore_index=True)
    grouped  = (
        all_data.groupby(["zone_id", "dow", "hour"])["taxi_count"]
        .agg(mean="mean", n="count")
        .reset_index()
    )
    result: dict = {}
    for _, row in grouped.iterrows():
        key = (int(row["zone_id"]), int(row["dow"]), int(row["hour"]))
        result[key] = {"mean": float(row["mean"]), "n": int(row["n"])}
    return result


def _get_baseline(baselines: dict, zone_id: int) -> float | None:
    """Return the expected taxi_count for this zone at current DOW × hour."""
    now  = datetime.now(timezone.utc)
    key  = (zone_id, now.weekday(), now.hour)
    info = baselines.get(key)
    # Require at least 2 observations for the baseline to be meaningful
    if info and info["n"] >= 2:
        return info["mean"]
    return None


# ── Root cause ────────────────────────────────────────────────────────────────

def _classify_root_cause(
    expl_tag: str,
    depletion: float,
    vs_yesterday: float,
    vs_baseline: float | None,
) -> str:
    tag = expl_tag.lower()
    if "rain" in tag:
        return "weather"
    if vs_baseline is not None and vs_baseline < STRUCTURAL_GAP_RATIO:
        return "structural_gap"
    if depletion > FAST_DEPLETION:
        return "demand_spike"
    if "peak hour" in tag:
        return "peak_pattern"
    return "unknown"


# ── ETA + intervention window ─────────────────────────────────────────────────

def _eta_minutes(depletion: float, taxi_count: int, critical_count: int) -> int | None:
    """Estimate minutes until supply drops to critical_count at current depletion rate."""
    if depletion <= 0 or taxi_count <= critical_count:
        return None
    taxis_lost_per_min = (depletion / 60.0) * taxi_count
    if taxis_lost_per_min <= 0:
        return None
    return max(1, int((taxi_count - critical_count) / taxis_lost_per_min))


def _intervention_window(eta: int | None) -> str:
    if eta is None:
        return "unknown"
    if eta < LAG_ESCALATION:
        return "too_late"
    if eta < LAG_PUSH:
        return "tight"       # only demand-side pricing or escalation viable
    if eta < 45:
        return "moderate"    # pricing + push both viable
    return "ample"


# ── Adjacent zone risk ────────────────────────────────────────────────────────

def _adjacent_at_risk(
    zone_id: int,
    risk_map: dict[int, str],
) -> list[str]:
    """Return names of adjacent zones that are medium or high risk."""
    risky = []
    for adj_id in get_adjacent_ids(zone_id):
        level = risk_map.get(adj_id, "low")
        if level in ("medium", "high"):
            risky.append(ZONE_NAMES.get(adj_id, str(adj_id)))
    return risky


# ── Alternatives ──────────────────────────────────────────────────────────────

def _build_alternatives(
    risk_level: str,
    root_cause: str,
    window: str,
    adjacent_at_risk: list[str],
    score: float,
) -> list[dict]:
    """
    Return 2 alternative actions with cost/impact/time-to-effect so the ops
    person can compare options rather than just follow the primary recommendation.
    """
    alts: list[dict] = []

    if risk_level == "high":
        # Alt 1: surge pricing only (always viable, immediate demand effect)
        if window != "too_late":
            alts.append({
                "action":             "Surge pricing only (+$1.00–2.00)",
                "time_to_effect_min": LAG_PRICING_DEMAND,
                "cost":               "medium",  # user experience impact
                "impact":             "Reduces demand 10–20% immediately; drivers follow revenue signal within 15–20 min",
                "viable":             True,
            })
        # Alt 2: push notification only (only if adjacent zones have slack)
        push_viable = window in ("moderate", "ample") and not adjacent_at_risk
        alts.append({
            "action":             "Driver push notification only (no pricing change)",
            "time_to_effect_min": LAG_PUSH,
            "cost":               "low",
            "impact":             "Slower than pricing; effective only if drivers are idle nearby and adjacent zones are not at risk",
            "viable":             push_viable,
        })
        # Alt 3: do nothing
        alts.append({
            "action":             "Do nothing — continue monitoring",
            "time_to_effect_min": 0,
            "cost":               "none",
            "impact":             f"High risk of user-facing shortage within estimated window. Score: {score:.3f}",
            "viable":             False,
        })

    elif risk_level == "medium":
        alts.append({
            "action":             "Activate mild incentive now (+$0.50) — pre-empt escalation",
            "time_to_effect_min": LAG_PUSH,
            "cost":               "low",
            "impact":             "May prevent crossing high-risk threshold; lower cost than reactive surge",
            "viable":             True,
        })
        alts.append({
            "action":             "Do nothing — reassess in 30 min",
            "time_to_effect_min": 0,
            "cost":               "none",
            "impact":             "Risk of escalation if depletion continues; acceptable if trend is stable",
            "viable":             True,
        })

    return alts


def _recommendation_id(zone_id: int, timestamp: str, action_type: str, risk_level: str) -> str:
    raw = f"{zone_id}|{timestamp}|{action_type}|{risk_level}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _constraints_for_action(action: dict, context: dict) -> tuple[bool, list[str]]:
    constraints: list[str] = []
    window = context["intervention_window"]
    risk_level = context["risk_level"]
    has_adj_risk = context["adjacent_risk_flag"]
    is_raining = context["is_raining"]
    lag_min = int(action.get("lag_min", 0))
    eta = context.get("eta_minutes")

    if risk_level == "high" and window == "too_late":
        if action["action_id"] != "escalation":
            constraints.append("too_late_for_automated_action")

    if eta is not None and lag_min and eta < lag_min and action["action_id"] != "escalation":
        constraints.append("time_to_effect_exceeds_eta")

    if has_adj_risk and action.get("push_level") != "none":
        constraints.append("adjacent_risk_network_guardrail")

    if risk_level == "medium" and action.get("pricing_level") == "strong":
        constraints.append("surge_cap_for_medium_risk")

    if is_raining and risk_level == "high" and action["action_id"] in {"push_only", "monitor", "none"}:
        constraints.append("weather_requires_pricing_or_incentive")

    viable = len(constraints) == 0
    return viable, constraints


def _format_action_recommendation(action: dict, context: dict) -> tuple[str, str]:
    zone_name = context["zone_name"]
    root_cause = context["root_cause"].replace("_", " ")
    rider_message = " Show rider notice about elevated fares and possible delays." if action.get("rider_message") else ""
    action_id = action["action_id"]

    if action_id == "none":
        return (
            "No action required immediately. Continue standard monitoring cadence.",
            "No intervention cost; maintain baseline reliability watch.",
        )
    if action_id == "monitor":
        return (
            "Hold action for now. Reassess this zone in 15 minutes and trigger intervention if the score worsens.",
            "Low-cost watchlist posture while preserving intervention budget.",
        )
    if action_id == "mild_surge":
        return (
            f"Activate mild surge pricing (+$0.50 to $1.00) in {zone_name}. Use rider messaging to smooth demand." + rider_message,
            "Fast demand relief with moderate rider impact.",
        )
    if action_id == "strong_surge":
        return (
            f"Activate strong surge pricing (+$1.50 to $2.50) in {zone_name}. Keep it tightly time-boxed and reassess within 10 minutes." + rider_message,
            "Strongest near-term demand suppression and driver pull signal.",
        )
    if action_id == "mild_driver_incentive":
        return (
            f"Offer a mild driver incentive (+$1.00 bonus) for the next hour in {zone_name}. Target nearby idle drivers.",
            "Supply-side support with lower rider impact.",
        )
    if action_id == "strong_driver_incentive":
        return (
            f"Offer a strong driver incentive (+$2.00 bonus) for the next hour in {zone_name}. Prioritize drivers with recent service history in this zone.",
            "Higher-cost supply recovery aimed at acute shortages.",
        )
    if action_id == "push_only":
        return (
            f"Send a targeted push notification to idle drivers within 3 km of {zone_name}. Emphasize expected earnings and rapid pickup demand.",
            "Low-cost repositioning nudge when nearby slack exists.",
        )
    if action_id == "surge_plus_incentive":
        return (
            f"Activate mild surge pricing and a +$1.50 driver incentive in {zone_name}. This combines demand shaping with supply activation for {root_cause} conditions." + rider_message,
            "Balanced marketplace response using both price and incentive levers.",
        )
    if action_id == "surge_plus_push":
        return (
            f"Activate mild surge pricing and send a targeted push to nearby drivers for {zone_name}. Use this when fast demand shaping is needed but full incentives are not yet justified." + rider_message,
            "Hybrid action with quick demand relief and light supply repositioning.",
        )
    return (
        f"Escalate {zone_name} to district ops immediately. Authorize manual intervention and maximum marketplace controls.",
        "Human override for shortages that may outrun automated levers.",
    )


def _build_candidates(
    risk_level: str,
    context: dict,
    policy_records: list[dict] | None,
) -> list[dict]:
    candidates = []
    for action in candidate_actions_for_risk(risk_level):
        viable, constraints = _constraints_for_action(action, context)
        recommendation, impact = _format_action_recommendation(action, context)
        scored = score_candidate_action(action, context, records=policy_records)
        scored.update({
            "viable": viable,
            "constraints_triggered": constraints,
            "recommendation": recommendation,
            "expected_impact": impact,
        })
        candidates.append(scored)
    return candidates


def _candidate_priority(action_id: str, risk_level: str) -> str:
    if action_id == "escalation":
        return "critical"
    if risk_level == "high" and action_id in {"strong_surge", "strong_driver_incentive", "surge_plus_incentive", "surge_plus_push"}:
        return "high"
    if risk_level == "high":
        return "high"
    if risk_level == "medium":
        return "medium"
    return "low"


def _confidence_from_candidate(candidate: dict, score: float) -> float:
    base = {
        "high": 0.92,
        "medium": 0.8,
        "low": 0.68,
    }.get(candidate.get("confidence_band", "low"), 0.68)
    confidence = min(0.99, max(0.25, base + min(score, 1.0) * 0.08))
    return round(confidence, 2)


# ── Main builder ──────────────────────────────────────────────────────────────

def _build_recommendation(
    row: pd.Series,
    risk_map: dict[int, str],
    baselines: dict,
    policy_records: list[dict] | None = None,
    critical_count: int = 5,
) -> dict:
    score       = float(row.get("delay_risk_score", 0))
    risk_level  = str(row.get("risk_level", _assign_risk(score)))
    depletion   = float(row.get("depletion_rate_1h", 0) or 0)
    taxi_count  = int(row.get("taxi_count", 99) or 99)
    vs_yest     = float(row.get("supply_vs_yesterday", 1.0) or 1.0)
    expl_tag    = str(row.get("explanation_tag", "") or "")
    zone_id     = int(row.get("zone_id", 0))
    zone_name   = str(row.get("zone_name", "this zone"))
    last_updated = datetime.now(timezone.utc).isoformat()
    snapshot_ts  = str(row.get("timestamp", "") or last_updated)

    # ── Derived signals ───────────────────────────────────────────────────────
    is_raining = "rain" in expl_tag.lower()

    # DOW-calibrated baseline for this zone
    baseline_mean  = _get_baseline(baselines, zone_id)
    vs_baseline    = (taxi_count / baseline_mean) if baseline_mean else None

    # Use zone-relative critical threshold when baseline is available
    # otherwise fall back to absolute count
    zone_critical  = max(3, int(baseline_mean * CRITICAL_RATIO)) if baseline_mean else critical_count
    is_critically_low   = taxi_count < zone_critical
    is_fast_depleting   = depletion > FAST_DEPLETION
    is_structural_gap   = (vs_baseline is not None and vs_baseline < STRUCTURAL_GAP_RATIO) or \
                          (vs_baseline is None and vs_yest < STRUCTURAL_GAP_RATIO)

    # Time dynamics
    eta    = _eta_minutes(depletion, taxi_count, zone_critical)
    window = _intervention_window(eta)
    if eta is None and risk_level == "high" and taxi_count <= zone_critical:
        window = "too_late"

    # Root cause
    root_cause = _classify_root_cause(expl_tag, depletion, vs_yest, vs_baseline)

    # Network effects — adjacent zones at risk
    adj_at_risk   = _adjacent_at_risk(zone_id, risk_map)
    has_adj_risk  = len(adj_at_risk) > 0
    adj_risk_str  = ", ".join(adj_at_risk) if adj_at_risk else ""
    network_warn  = (
        f"Adjacent zones also at risk: {adj_risk_str}. "
        f"Pulling drivers here may worsen their position — prefer pricing over push."
        if has_adj_risk else ""
    )

    eta_str = f"~{eta} min" if eta is not None else "unknown"

    if risk_level == "high":
        if window == "too_late":
            issue = f"CRITICAL: supply exhaustion imminent ({eta_str}). Automated levers cannot act in time."
        elif is_raining:
            issue = "High shortage risk driven by rainfall — marketplace imbalance requires fast demand and supply control."
        elif is_critically_low:
            pct_of_normal = f"{int((taxi_count / baseline_mean) * 100)}%" if baseline_mean else f"{taxi_count} taxis"
            issue = f"Critical supply: {taxi_count} taxis ({pct_of_normal} of zone baseline) — shortage imminent{f' in {eta_str}' if eta else ''}"
        elif is_fast_depleting:
            issue = f"Rapid depletion at {depletion*100:.0f}%/hr — estimated critical shortage in {eta_str}"
        elif is_structural_gap and root_cause == "structural_gap":
            gap_pct = round((1 - (vs_baseline or vs_yest)) * 100)
            issue = f"Supply {gap_pct}% below typical level for this zone/time — structural gap detected"
        else:
            issue = f"Elevated shortage risk (score {score:.3f}) with root cause: {root_cause.replace('_', ' ')}."
    elif risk_level == "medium":
        if is_fast_depleting:
            issue = f"Moderate risk but depleting at {depletion*100:.0f}%/hr — may cross high-risk threshold{f' in {eta_str}' if eta else ''}"
        elif is_structural_gap:
            gap_pct = round((1 - (vs_baseline or vs_yest)) * 100)
            issue = f"Moderate risk with supply {gap_pct}% below typical level — structural gap forming"
        else:
            issue = f"Moderate shortage risk — conditions elevated but stable (score {score:.3f})"
    else:
        issue = "Normal operating conditions — supply and demand balanced"

    context = {
        "zone_id": zone_id,
        "zone_name": zone_name,
        "risk_level": risk_level,
        "root_cause": root_cause,
        "intervention_window": window,
        "adjacent_risk_flag": has_adj_risk,
        "is_raining": is_raining,
        "is_critically_low": is_critically_low,
        "is_fast_depleting": is_fast_depleting,
        "is_structural_gap": is_structural_gap,
        "eta_minutes": eta,
        "delay_risk_score": score,
    }

    candidates = _build_candidates(risk_level, context, policy_records or [])
    viable_candidates = [c for c in candidates if c.get("viable")]
    chosen = (
        sorted(
            viable_candidates,
            key=lambda c: (
                -float(c.get("expected_net_value", 0.0)),
                -float(c.get("expected_recovery_probability", 0.0)),
                -float(c.get("expected_supply_response_30m", 0.0)),
            ),
        )[0]
        if viable_candidates else
        sorted(candidates, key=lambda c: -float(c.get("expected_net_value", 0.0)))[0]
    )

    recommendation = chosen["recommendation"]
    impact = chosen["expected_impact"]
    priority = _candidate_priority(chosen["action_id"], risk_level)
    confidence = _confidence_from_candidate(chosen, score)
    action_type = str(chosen.get("action_type", action_type_from_recommendation(priority, recommendation)))
    recommendation_id = _recommendation_id(zone_id, snapshot_ts, action_type, risk_level)
    policy_stats = effectiveness_for_context(
        action_type=action_type,
        risk_level=risk_level,
        root_cause=root_cause,
        intervention_window=window,
        adjacent_risk_flag=has_adj_risk,
        action_id=str(chosen.get("action_id", action_type)),
        pricing_level=str(chosen.get("pricing_level", "none")),
        incentive_level=str(chosen.get("incentive_level", "none")),
        push_level=str(chosen.get("push_level", "none")),
        records=policy_records,
    )

    alternatives = []
    for candidate in sorted(
        candidates,
        key=lambda c: (
            not bool(c.get("viable")),
            -float(c.get("expected_net_value", 0.0)),
        ),
    ):
        alt = {
            "action": candidate["title"],
            "action_id": candidate["action_id"],
            "time_to_effect_min": candidate["lag_min"],
            "cost": "none" if float(candidate.get("estimated_cost_sgd", 0.0)) == 0 else ("low" if float(candidate.get("estimated_cost_sgd", 0.0)) <= 1.5 else "medium"),
            "impact": candidate["expected_impact"],
            "viable": candidate["viable"],
            "estimated_cost_sgd": round(float(candidate.get("estimated_cost_sgd", 0.0)), 2),
            "expected_supply_response_30m": round(float(candidate.get("expected_supply_response_30m", 0.0)), 2),
            "expected_recovery_probability": round(float(candidate.get("expected_recovery_probability", 0.0)), 4),
            "expected_improvement_rate": round(float(candidate.get("expected_improvement_probability", 0.0)), 4),
            "expected_score_delta": round(float(candidate.get("expected_score_delta", 0.0)), 4),
            "expected_roi": round(float(candidate.get("expected_roi", 0.0)), 4),
            "confidence_band": candidate.get("confidence_band"),
            "evidence_count": candidate.get("evidence_count"),
            "policy_rank_reason": candidate.get("policy_rank_reason"),
            "constraints_triggered": candidate.get("constraints_triggered", []),
            "winning_reason": candidate.get("winning_reason", ""),
        }
        if candidate["action_id"] != chosen["action_id"]:
            alternatives.append(alt)

    return {
        "recommendation_id":      recommendation_id,
        "zone_id":               zone_id,
        "zone_name":             zone_name,
        "region":                str(row.get("region", "")),
        "risk_level":            risk_level,
        "delay_risk_score":      round(score, 4),
        "issue_detected":        issue,
        "recommendation":        recommendation,
        "expected_impact":       impact,
        "confidence":            confidence,
        "priority":              priority,
        "explanation_tag":       expl_tag,
        "last_updated":          last_updated,
        # Gap 1: zone-relative threshold diagnostics
        "eta_minutes":           eta,
        "intervention_window":   window,
        # Gap 2: network effects
        "adjacent_risk_zones":   adj_risk_str,
        "network_warning":       network_warn,
        # Gap 3/5: root cause + alternatives
        "root_cause":            root_cause,
        "action_id":             chosen["action_id"],
        "action_type":           action_type,
        "pricing_level":         chosen.get("pricing_level", "none"),
        "incentive_level":       chosen.get("incentive_level", "none"),
        "push_level":            chosen.get("push_level", "none"),
        "expected_recovery_rate": policy_stats["recovery_rate"],
        "expected_improvement_rate": chosen["expected_improvement_probability"],
        "estimated_score_delta": chosen["expected_score_delta"],
        "confidence_band":       chosen["confidence_band"],
        "evidence_count":        chosen["evidence_count"],
        "follow_rate":           chosen["follow_rate"],
        "policy_rank_reason":    chosen["policy_rank_reason"],
        "estimated_cost_sgd":    chosen["estimated_cost_sgd"],
        "expected_supply_response_30m": chosen["expected_supply_response_30m"],
        "expected_recovery_probability": chosen["expected_recovery_probability"],
        "expected_roi":          chosen["expected_roi"],
        "decision_objective":    "reliability_first",
        "winning_reason":        (
            f"Selected {chosen['title']} because it maximized expected reliability value under current constraints."
        ),
        "constraints_triggered": json.dumps(chosen.get("constraints_triggered", [])),
        "alternative_actions":   json.dumps(alternatives),
    }


def generate_recommendations(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes predictions DataFrame (current 55-zone snapshot), returns
    recommended_actions DataFrame and writes recommended_actions.csv.
    """
    # Build zone risk map for adjacency checks (gap 2)
    risk_map: dict[int, str] = {}
    if "zone_id" in predictions_df.columns and "risk_level" in predictions_df.columns:
        for _, r in predictions_df.iterrows():
            risk_map[int(r["zone_id"])] = str(r["risk_level"])

    # Load DOW baselines once (gap 1, 5)
    baselines = _load_dow_baselines()
    policy_records = load_outcome_records()

    rows = [
        _build_recommendation(row, risk_map, baselines, policy_records=policy_records)
        for _, row in predictions_df.iterrows()
    ]
    df = pd.DataFrame(rows)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUTS_DIR / "recommended_actions.csv", index=False)
    return df


def _assign_risk(score: float) -> str:
    if score >= RISK_HIGH:
        return "high"
    if score >= RISK_MED:
        return "medium"
    return "low"
