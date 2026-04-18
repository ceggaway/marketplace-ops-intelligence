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
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

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


# ── Main builder ──────────────────────────────────────────────────────────────

def _build_recommendation(
    row: pd.Series,
    risk_map: dict[int, str],
    baselines: dict,
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
    now         = datetime.now(timezone.utc).isoformat()

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

    # ── HIGH RISK ─────────────────────────────────────────────────────────────
    if risk_level == "high":

        if window == "too_late":
            # ETA < escalation lag — automated levers cannot work
            issue          = f"CRITICAL: supply exhaustion imminent ({eta_str}). Automated levers cannot act in time."
            recommendation = (
                f"Escalate immediately to district ops manager — shortage likely within {eta_str}. "
                f"Human override required. Simultaneously activate maximum surge (+$3.00) "
                f"to suppress remaining demand and signal all nearby drivers."
            )
            impact         = "Surge suppresses demand immediately; ops override is the only viable supply-side response"
            confidence     = round(min(score + 0.05, 0.99), 2)
            priority       = "critical"

        elif is_raining:
            issue          = "High shortage risk driven by rainfall — demand surge absorbing supply network-wide"
            recommendation = (
                "Activate rain surge pricing (+$2.00/trip) across this zone. "
                "Push driver incentive notification to idle drivers within 3 km. "
                "Alert riders in-app to expect higher fares and longer wait times."
                + (f" Note: {network_warn}" if has_adj_risk else "")
            )
            impact         = "Rain surge reduces demand 15–20% immediately and attracts 10–15 nearby drivers within 20 min"
            confidence     = round(min(score + 0.10, 0.99), 2)
            priority       = "critical"

        elif is_critically_low:
            pct_of_normal = f"{int((taxi_count / baseline_mean) * 100)}%" if baseline_mean else f"{taxi_count} taxis"
            issue          = (
                f"Critical supply: {taxi_count} taxis ({pct_of_normal} of zone baseline) — "
                f"shortage imminent{f' in {eta_str}' if eta else ''}"
            )
            recommendation = (
                f"Escalate to district ops manager. "
                f"Activate $2.00 driver incentive for pickups in {zone_name}. "
                f"Push targeted notification to all idle drivers within 5 km. "
                f"Enable in-app demand nudge: suggest nearby pickup zones to waiting riders."
                + (f" ⚠ Network: {network_warn}" if has_adj_risk else "")
            )
            impact         = "Escalation + max incentive expected to mobilise 5–10 drivers within 15 min"
            confidence     = round(score, 2)
            priority       = "critical"

        elif is_fast_depleting and window == "tight":
            # Fast depletion, not much time — pricing is the only viable lever
            issue          = (
                f"Rapid depletion at {depletion*100:.0f}%/hr — "
                f"estimated critical shortage in {eta_str}. "
                f"Push notification lag ({LAG_PUSH} min) exceeds available window."
            )
            recommendation = (
                f"Activate surge pricing (+$1.50/trip) immediately — demand-side effect in {LAG_PRICING_DEMAND} min. "
                f"Push notification viable only if ETA extends past {LAG_PUSH} min. "
                f"Set 10-min reassessment alert."
                + (f" ⚠ {network_warn}" if has_adj_risk else "")
            )
            impact         = f"Pricing suppresses demand within {LAG_PRICING_DEMAND} min; shortage averted if depletion slows"
            confidence     = round(score * 0.95, 2)
            priority       = "critical"

        elif is_fast_depleting:
            # Fast depletion, window is ample — use both levers
            issue          = (
                f"Rapid depletion at {depletion*100:.0f}%/hr — "
                f"estimated critical shortage in {eta_str}"
            )
            push_note = (
                f" ⚠ Adjacent zones at risk ({adj_risk_str}) — "
                f"push may stress neighbours; prefer pricing as primary lever."
                if has_adj_risk else
                " Push notification to idle drivers in adjacent low-risk zones."
            )
            recommendation = (
                f"Activate zone driver incentive (+$1.50/trip for next hour)."
                + push_note +
                f" Escalate to ops if depletion rate does not slow within 15 min."
            )
            impact         = f"Incentive + push expected to stabilise supply within 20–30 min (ETA: {eta_str})"
            confidence     = round(score * 0.95, 2)
            priority       = "critical"

        elif is_structural_gap and root_cause == "structural_gap":
            # Supply well below DOW baseline — structural issue, not a live spike
            gap_pct = round((1 - (vs_baseline or vs_yest)) * 100)
            issue   = (
                f"Supply {gap_pct}% below typical level for this zone/time. "
                f"Root cause: {root_cause.replace('_', ' ')}. "
                f"Demand spike alone is unlikely — investigate access or driver availability."
            )
            recommendation = (
                f"Activate driver incentive (+$1.00/trip). "
                f"Check for road closures, events, or app issues affecting {zone_name}. "
                f"Push targeted re-engagement notification to drivers who served this zone "
                f"at this time last {['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][__import__('datetime').datetime.now().weekday()]}."
            )
            impact         = "Incentive + targeted driver re-engagement expected to close supply gap within 30–45 min"
            confidence     = round(score * 0.90, 2)
            priority       = "high"

        else:
            # High risk, no dominant acute signal
            issue          = (
                f"Elevated shortage risk (score {score:.3f}). "
                f"Root cause: {root_cause.replace('_', ' ')}."
            )
            recommendation = (
                "Apply mild fare adjustment (+$0.50 surge) to dampen demand. "
                "Send availability push notification to idle drivers within 3 km. "
                "Reassess in 15 min — escalate if score remains above 0.70."
                + (f" ⚠ {network_warn}" if has_adj_risk else "")
            )
            impact         = f"Mild pricing + driver nudge expected to stabilise supply within 20 min"
            confidence     = round(score * 0.85, 2)
            priority       = "high"

    # ── MEDIUM RISK ───────────────────────────────────────────────────────────
    elif risk_level == "medium":

        if is_fast_depleting:
            issue          = (
                f"Moderate risk but depleting at {depletion*100:.0f}%/hr — "
                f"may cross high-risk threshold"
                + (f" in {eta_str}" if eta else "")
            )
            recommendation = (
                "Send proactive push notification to 5–10 nearby idle drivers now — no pricing action yet. "
                "Reassess in 15 min. Activate surge immediately if score exceeds 0.70."
                + (f" ⚠ {network_warn}" if has_adj_risk else "")
            )
            impact         = "Early driver nudge can prevent escalation without pricing intervention"
            confidence     = round(score, 2)
            priority       = "medium"

        elif is_structural_gap:
            gap_pct = round((1 - (vs_baseline or vs_yest)) * 100)
            issue          = f"Moderate risk with supply {gap_pct}% below typical level — structural gap forming"
            recommendation = (
                "Monitor closely over next 30 min. "
                "If supply continues to fall, activate driver incentive before risk reaches high threshold. "
                "No immediate pricing action required."
            )
            impact         = "Early monitoring avoids reactive-only response if conditions worsen"
            confidence     = round(score, 2)
            priority       = "medium"

        else:
            issue          = f"Moderate shortage risk — conditions elevated but stable (score {score:.3f})"
            recommendation = (
                "No immediate action. Reassess in 30 min. "
                "Flag for early incentive activation if score rises above 0.65."
            )
            impact         = "Watchlist — intervene at first sign of acceleration"
            confidence     = round(score, 2)
            priority       = "medium"

    # ── LOW RISK ──────────────────────────────────────────────────────────────
    else:
        issue          = "Normal operating conditions — supply and demand balanced"
        recommendation = "No action required. Continue standard 5-min monitoring cadence."
        impact         = "N/A"
        confidence     = round(1 - score, 2)
        priority       = "low"

    # ── Alternatives ──────────────────────────────────────────────────────────
    alternatives = _build_alternatives(risk_level, root_cause, window, adj_at_risk, score)

    return {
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
        "last_updated":          now,
        # Gap 1: zone-relative threshold diagnostics
        "eta_minutes":           eta,
        "intervention_window":   window,
        # Gap 2: network effects
        "adjacent_risk_zones":   adj_risk_str,
        "network_warning":       network_warn,
        # Gap 3/5: root cause + alternatives
        "root_cause":            root_cause,
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

    rows = [
        _build_recommendation(row, risk_map, baselines)
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
