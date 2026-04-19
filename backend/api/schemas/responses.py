"""
Pydantic response schemas for the FastAPI surface.

These models intentionally reflect the responses the application actually
returns today, including a few legacy compatibility field names that are still
consumed by the frontend.
"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class TrendPoint(BaseModel):
    timestamp: datetime
    value: float


class Alert(BaseModel):
    alert_id: str
    zone_id: Optional[int] = None
    severity: Literal["high", "medium", "info"]
    message: str
    created_at: datetime


class KPIOverview(BaseModel):
    high_risk_zone_count: int       # zones at risk_level=high in current snapshot
    medium_risk_zone_count: int     # zones at risk_level=medium in current snapshot
    total_taxi_supply: int          # total taxi_count right now across all zones
    rapid_depletion_zones: int      # zones with depletion_rate_1h > 0.30
    critical_actions_count: int     # critical + high priority recommendations pending
    model_psi: float                # latest PSI from drift report
    minutes_since_last_run: int     # minutes since last scoring pipeline run
    as_of: datetime


class OverviewResponse(BaseModel):
    kpis: KPIOverview
    demand_trend: list[TrendPoint]
    delay_trend: list[TrendPoint]
    fulfilment_trend: list[TrendPoint]
    alerts: list[Alert]


class ZoneSummary(BaseModel):
    zone_id: int
    zone_name: str
    region: str
    taxi_count: int
    current_supply: int
    depletion_rate_1h: float
    supply_vs_yesterday: float
    delay_risk_score: float
    risk_level: Literal["high", "medium", "low"]
    recommendation: str
    explanation_tag: str


class ZoneDetail(ZoneSummary):
    demand_trend: list[TrendPoint]
    delay_trend: list[TrendPoint]
    risk_score_history: list[TrendPoint]


class RecommendationCard(BaseModel):
    recommendation_id: str
    zone_id: int
    zone_name: str
    region: str
    risk_level: Literal["high", "medium", "low"]
    delay_risk_score: float
    issue_detected: str
    recommendation: str
    expected_impact: Optional[str] = None
    confidence: float
    priority: Literal["critical", "high", "medium", "low"]
    explanation_tag: str
    last_updated: datetime
    # Engine v2 fields
    eta_minutes: Optional[int] = None
    intervention_window: Optional[str] = None
    adjacent_risk_zones: Optional[str] = None
    network_warning: Optional[str] = None
    root_cause: Optional[str] = None
    action_type: Optional[str] = None
    expected_recovery_rate: Optional[float] = None
    expected_improvement_rate: Optional[float] = None
    estimated_score_delta: Optional[float] = None
    confidence_band: Optional[str] = None
    evidence_count: Optional[int] = None
    follow_rate: Optional[float] = None
    policy_rank_reason: Optional[str] = None
    alternative_actions: Optional[str] = None


class ModelStatus(BaseModel):
    active_version: Optional[str] = None
    promoted_at: Optional[datetime] = None
    last_retrained_at: Optional[datetime] = None
    training_metrics: dict = Field(default_factory=dict)
    candidate_version: Optional[str] = None
    candidate_metrics: Optional[dict] = None


class ModelVersion(BaseModel):
    version_id: str
    status: str
    trained_at: Optional[datetime] = None
    promoted_at: Optional[datetime] = None
    metrics: dict = Field(default_factory=dict)


class PipelineRun(BaseModel):
    run_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    rows_scored: int
    failed_rows: int
    flagged_zones: int
    drift_flag: bool
    rollback_status: bool
    run_status: str
    latency_ms: int
    model_version: Optional[str] = None
    psi: Optional[float] = None
    logged_at: Optional[datetime] = None
    avg_delay_min: Optional[float] = None
    fulfilment_rate: Optional[float] = None
    total_taxi_count: Optional[int] = None
    supply_now: Optional[int] = None           # taxis in current zone snapshot (vs total rows scored)
    high_risk_zones_now: Optional[int] = None
    rapid_depletion_zones: Optional[int] = None


class DriftReport(BaseModel):
    run_id: str
    timestamp: datetime
    psi: float
    drift_flag: bool
    drift_level: str
    reference_mean: float
    current_mean: float
    reference_std: float
    current_std: float
    reference_n: Optional[int] = None
    current_n: Optional[int] = None
    feature_drift: Optional[dict] = None


class ServiceStatus(BaseModel):
    name: str
    status: Literal["ok", "degraded", "down"]
    detail: Optional[str] = None
    last_updated: Optional[datetime] = None


class ServicesHealth(BaseModel):
    services: list[ServiceStatus]
    checked_at: datetime


# ── Report schemas ────────────────────────────────────────────────────────────

class ZonePerformanceEntry(BaseModel):
    zone_id: int
    zone_name: str
    region: str
    mean_score: float
    pct_time_high: float
    pct_time_medium: float
    pct_time_low: float
    trend: str                        # "improving" | "stable" | "deteriorating"
    trend_delta: float                # recent mean - prior mean (negative = improving)
    observations: int


class ZonePerformanceReport(BaseModel):
    generated_at: datetime
    observation_days: int
    chronic_high_risk: list[ZonePerformanceEntry]
    most_improved: list[ZonePerformanceEntry]
    deteriorating: list[ZonePerformanceEntry]
    note: Optional[str] = None


class OutcomeEntry(BaseModel):
    recommendation_id: Optional[str] = None
    zone_id: int
    zone_name: str
    action_type: str
    priority: str
    score_at_time: float
    score_after: Optional[float] = None
    outcome: str
    logged_at: str
    followed_status: Optional[str] = None
    follow_note: Optional[str] = None


class OutcomeReport(BaseModel):
    generated_at: datetime
    total_logged: int
    total_resolved: int
    recovery_rate: float
    improvement_rate: float
    worsened_rate: float
    by_action_type: dict
    by_follow_status: dict
    top_contexts: list[dict] = Field(default_factory=list)
    by_zone: list[dict] = Field(default_factory=list)
    recent_outcomes: list[OutcomeEntry]
    sample_size_note: str


class ModelImpactReport(BaseModel):
    generated_at: datetime
    active_version: Optional[str] = None
    psi: float
    psi_level: str                    # "stable" | "moderate" | "significant"
    psi_business_impact: str
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1: Optional[float] = None
    estimated_false_positive_note: str
    version_lineage: list[dict]
    recommendation: str
