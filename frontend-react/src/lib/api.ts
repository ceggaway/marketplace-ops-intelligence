// VITE_API_BASE_URL must be set at build time for production deployments.
// In dev, Vite proxies /api/* to localhost:8000 via vite.config.ts.
// Falls back to /api/v1 (relative) so any reverse-proxy setup works without env config.
const BASE = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'

export interface TrendPoint { timestamp: string; value: number }

// severity includes 'info' to match backend Literal["high", "medium", "info"]
export interface Alert {
  alert_id: string
  zone_id?: number | null
  severity: 'high' | 'medium' | 'info'
  message: string
  created_at: string
}

export interface Kpis {
  high_risk_zone_count: number;    // zones at risk_level=high right now
  medium_risk_zone_count: number;  // zones at risk_level=medium right now
  total_taxi_supply: number;       // total taxis across all zones right now
  rapid_depletion_zones: number;   // zones losing >30% supply/hr
  critical_actions_count: number;  // critical + high priority recommendations
  model_psi: number;               // PSI drift score (0 = stable, >0.25 = alert)
  minutes_since_last_run: number;  // minutes since last scoring pipeline run
  as_of: string;
}
export interface Overview {
  kpis: Kpis; alerts: Alert[];
  demand_trend: TrendPoint[]; delay_trend: TrendPoint[]; fulfilment_trend: TrendPoint[];
}

export interface Zone {
  zone_id: number; zone_name: string; region: string;
  risk_level: 'high' | 'medium' | 'low'; delay_risk_score: number;
  depletion_risk_score?: number | null;
  demand_pressure_score?: number | null;
  demand_pressure_level?: string | null;
  imbalance_score?: number | null;
  imbalance_level?: string | null;
  policy_action?: string | null;
  policy_reason?: string | null;
  predicted_shortage?: number | null;
  severity_bucket?: string | null;
  persistence_count?: number | null;
  neighbor_surplus?: number | null;
  recommended_action?: string | null;
  action_reason?: string | null;
  estimated_action_cost?: number | null;
  estimated_shortage_reduction?: number | null;
  budget_remaining?: number | null;
  taxi_count: number; current_supply: number;
  depletion_rate_1h: number; supply_vs_yesterday: number;
  explanation_tag: string; recommendation?: string;
}
export interface ZoneDetail extends Zone {
  demand_trend: TrendPoint[]; delay_trend: TrendPoint[]; risk_score_history: TrendPoint[];
}

export interface AlternativeAction {
  action: string
  action_id?: string | null
  time_to_effect_min: number
  cost: string
  impact: string
  viable: boolean
  estimated_cost_sgd?: number | null
  expected_supply_response_30m?: number | null
  expected_recovery_probability?: number | null
  expected_improvement_rate?: number | null
  expected_score_delta?: number | null
  expected_roi?: number | null
  confidence_band?: string | null
  evidence_count?: number | null
  policy_rank_reason?: string | null
  constraints_triggered?: string[] | null
  winning_reason?: string | null
}

export interface Recommendation {
  recommendation_id: string
  zone_id: number; zone_name: string; region: string;
  risk_level: 'high' | 'medium' | 'low';
  priority: 'critical' | 'high' | 'medium' | 'low';
  delay_risk_score: number; confidence: number;
  depletion_risk_score?: number | null;
  demand_pressure_score?: number | null;
  imbalance_score?: number | null;
  imbalance_level?: string | null;
  policy_action?: string | null;
  policy_reason?: string | null;
  predicted_shortage?: number | null;
  severity_bucket?: string | null;
  persistence_count?: number | null;
  neighbor_surplus?: number | null;
  recommended_action?: string | null;
  action_reason?: string | null;
  estimated_action_cost?: number | null;
  estimated_shortage_reduction?: number | null;
  budget_remaining?: number | null;
  issue_detected: string; recommendation: string;
  expected_impact: string; explanation_tag: string;
  last_updated?: string | null;
  // Engine v2 fields
  eta_minutes?: number | null;
  intervention_window?: string | null;
  adjacent_risk_zones?: string | null;
  network_warning?: string | null;
  root_cause?: string | null;
  action_id?: string | null;
  action_type?: string | null;
  pricing_level?: string | null;
  incentive_level?: string | null;
  push_level?: string | null;
  expected_recovery_rate?: number | null;
  expected_improvement_rate?: number | null;
  estimated_score_delta?: number | null;
  confidence_band?: string | null;
  evidence_count?: number | null;
  follow_rate?: number | null;
  policy_rank_reason?: string | null;
  estimated_cost_sgd?: number | null;
  expected_supply_response_30m?: number | null;
  expected_recovery_probability?: number | null;
  expected_roi?: number | null;
  decision_objective?: string | null;
  winning_reason?: string | null;
  constraints_triggered?: string | null;
  alternative_actions?: string | null;
}

export interface TrainingMetrics {
  f1?: number; roc_auc?: number; precision?: number;
  recall?: number; train_rows?: number; val_rows?: number;
}
export interface ModelStatus {
  active_version?: string | null
  promoted_at?: string | null
  last_retrained_at?: string | null
  training_metrics: TrainingMetrics
  candidate_version?: string | null
  candidate_metrics?: Record<string, number> | null
}

export interface ModelVersion {
  version_id: string
  status: string
  trained_at?: string | null
  promoted_at?: string | null
  metrics: Record<string, number>
}

export interface LatestRun {
  run_id?: string | null
  run_status: string
  timestamp?: string | null
  rows_scored: number
  flagged_zones: number
  failed_rows: number
  latency_ms: number
  drift_flag: boolean
  rollback_status: boolean
  model_version?: string | null
  psi?: number | null
  logged_at?: string | null
  avg_delay_min?: number | null
  fulfilment_rate?: number | null
  total_taxi_count?: number | null
  supply_now?: number | null
  high_risk_zones_now?: number | null
  rapid_depletion_zones?: number | null
  avg_demand_pressure_score?: number | null
  avg_imbalance_score?: number | null
}

export interface FeatureDriftEntry {
  psi: number
  drift_level: string
  reference_mean: number
  current_mean: number
  reference_std: number
  current_std: number
}

export interface DriftReport {
  run_id: string
  timestamp: string
  psi: number
  drift_flag: boolean
  drift_level: string
  reference_mean: number
  current_mean: number
  reference_std: number
  current_std: number
  reference_n?: number | null
  current_n?: number | null
  feature_drift?: Record<string, FeatureDriftEntry> | null
}

export interface ServiceStatus {
  name: string
  status: 'ok' | 'degraded' | 'down'
  detail?: string | null
  last_updated?: string | null
}
export interface ServicesHealth {
  services: ServiceStatus[]
  checked_at: string
}

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`${BASE}${path}`)
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
  return r.json()
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
  return r.json()
}

// ── Report types ─────────────────────────────────────────────────────────────

export interface ZonePerformanceEntry {
  zone_id: number
  zone_name: string
  region: string
  mean_score: number
  pct_time_high: number
  pct_time_medium: number
  pct_time_low: number
  trend: 'improving' | 'stable' | 'deteriorating'
  trend_delta: number
  observations: number
}

export interface ZonePerformanceReport {
  generated_at: string
  observation_days: number
  chronic_high_risk: ZonePerformanceEntry[]
  most_improved: ZonePerformanceEntry[]
  deteriorating: ZonePerformanceEntry[]
  note?: string
}

export interface OutcomeEntry {
  recommendation_id?: string | null
  zone_id: number
  zone_name: string
  action_type: string
  priority: string
  score_at_time: number
  score_after: number | null
  outcome: string
  logged_at: string
  followed_status?: string | null
  follow_note?: string | null
}

export interface OutcomeReport {
  generated_at: string
  total_logged: number
  total_resolved: number
  recovery_rate: number
  improvement_rate: number
  worsened_rate: number
  by_action_type: Record<string, Record<string, number>>
  by_follow_status: Record<string, Record<string, number>>
  top_contexts: Array<{
    action_type: string
    risk_level: string
    root_cause: string
    intervention_window: string
    resolved: number
    recovery_rate: number
    improvement_rate: number
    confidence_band: string
  }>
  by_zone: { zone_id: number; zone_name: string; interventions: number; recovery_rate: number }[]
  recent_outcomes: OutcomeEntry[]
  sample_size_note: string
}

export interface RecommendationFeedbackResponse {
  status: string
  recommendation_id: string
  followed_status: 'followed' | 'not_followed'
  followed_at?: string | null
}

export interface ModelImpactReport {
  generated_at: string
  active_version: string | null
  psi: number
  psi_level: string
  psi_business_impact: string
  precision: number | null
  recall: number | null
  f1: number | null
  estimated_false_positive_note: string
  version_lineage: { version: string; status: string; trained_at: string; f1: number | null; roc_auc: number | null }[]
  recommendation: string
}

export const api = {
  overview: () => get<Overview>('/overview'),
  zones: (risk_level?: string, region?: string) => {
    const p = new URLSearchParams()
    if (risk_level) p.set('risk_level', risk_level)
    if (region) p.set('region', region)
    return get<Zone[]>(`/zones${p.toString() ? '?' + p : ''}`)
  },
  zoneDetail: (id: number) => get<ZoneDetail>(`/zones/${id}`),
  recommendations: (priority?: string) => {
    const p = priority ? `?priority=${priority}` : ''
    return get<Recommendation[]>(`/recommendations${p}`)
  },
  recommendationFeedback: (
    recommendation_id: string,
    payload: { followed_status: 'followed' | 'not_followed'; followed_by?: string; follow_note?: string },
  ) => post<RecommendationFeedbackResponse>(`/recommendations/${recommendation_id}/feedback`, payload),
  modelStatus: () => get<ModelStatus>('/model/status'),
  modelVersions: () => get<ModelVersion[]>('/model/versions'),
  latestRun: () => get<LatestRun>('/pipeline/latest-run'),
  drift: () => get<DriftReport>('/monitoring/drift'),
  history: (n = 20) => get<LatestRun[]>(`/monitoring/history?n=${n}`),
  alerts: () => get<Alert[]>('/alerts'),
  servicesHealth: () => get<ServicesHealth>('/health/services'),
  retrain: () => fetch(`${BASE}/pipeline/retrain`, { method: 'POST' }).then(r => {
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
    return r.json() as Promise<{ status: string; version: string; message: string }>
  }),
  // Reports
  reportZonePerformance: (days = 7) => get<ZonePerformanceReport>(`/reports/zone-performance?days=${days}`),
  reportOutcomes: () => get<OutcomeReport>('/reports/outcomes'),
  reportModelImpact: () => get<ModelImpactReport>('/reports/model-impact'),
}
