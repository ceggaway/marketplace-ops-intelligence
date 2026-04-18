const BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000/api/v1'

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
  taxi_count: number; current_supply: number;
  depletion_rate_1h: number; supply_vs_yesterday: number;
  explanation_tag: string; recommendation?: string;
}
export interface ZoneDetail extends Zone {
  demand_trend: TrendPoint[]; delay_trend: TrendPoint[]; risk_score_history: TrendPoint[];
}

export interface AlternativeAction {
  action: string
  time_to_effect_min: number
  cost: string
  impact: string
  viable: boolean
}

export interface Recommendation {
  zone_id: number; zone_name: string; region: string;
  risk_level: 'high' | 'medium' | 'low';
  priority: 'critical' | 'high' | 'medium' | 'low';
  delay_risk_score: number; confidence: number;
  issue_detected: string; recommendation: string;
  expected_impact: string; explanation_tag: string;
  last_updated?: string | null;
  // Engine v2 fields
  eta_minutes?: number | null;
  intervention_window?: string | null;
  adjacent_risk_zones?: string | null;
  network_warning?: string | null;
  root_cause?: string | null;
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
}
