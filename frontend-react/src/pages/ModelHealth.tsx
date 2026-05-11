import { useRef, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, Lightbulb, AlertTriangle, TrendingUp } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'
import { api } from '../lib/api'
import { fmtDate, COLORS, SECTION_LABEL, TOOLTIP_STYLE, TOOLTIP_LABEL_STYLE } from '../lib/utils'
import GlassCard from '../components/GlassCard'
import Badge from '../components/Badge'
import Spinner from '../components/Spinner'
import SparkLine from '../components/SparkLine'
import { showToast } from '../components/toast-utils'

type MetricKind = 'classification' | 'probability-error' | 'regression'

type MetricDef = {
  key: string
  label: string
  kind: MetricKind
  higherIsBetter: boolean
  target?: number
  description: string
}

const METRICS: MetricDef[] = [
  { key: 'roc_auc', label: 'ROC AUC', kind: 'classification', higherIsBetter: true, target: 0.85, description: 'Ranking quality across thresholds' },
  { key: 'f1', label: 'F1', kind: 'classification', higherIsBetter: true, target: 0.70, description: 'Balance of precision and recall' },
  { key: 'precision', label: 'Precision', kind: 'classification', higherIsBetter: true, target: 0.70, description: 'Share of fired alerts that are useful' },
  { key: 'recall', label: 'Recall', kind: 'classification', higherIsBetter: true, target: 0.70, description: 'Share of real depletion events caught' },
  { key: 'mae', label: 'MAE', kind: 'probability-error', higherIsBetter: false, description: 'Mean absolute probability error' },
  { key: 'rmse', label: 'RMSE', kind: 'probability-error', higherIsBetter: false, description: 'Penalises large probability errors' },
  { key: 'log_loss', label: 'Log Loss', kind: 'probability-error', higherIsBetter: false, description: 'Probability calibration penalty' },
  { key: 'brier_score', label: 'Brier', kind: 'probability-error', higherIsBetter: false, description: 'Mean squared probability error' },
  { key: 'r2', label: 'R2', kind: 'regression', higherIsBetter: true, description: 'Regression-style explained variance if available' },
]

const metricByKey = Object.fromEntries(METRICS.map(m => [m.key, m]))

// ── PSI circular gauge ──────────────────────────────────────────────────────
function PsiCircle({ psi }: { psi: number }) {
  const pct = Math.min(psi / 0.5, 1)
  const color = psi >= 0.25 ? '#D95252' : psi >= 0.10 ? '#C97B30' : '#3BAF73'
  const r = 52, circ = 2 * Math.PI * r
  const dash = pct * circ
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
      <svg width={130} height={130} style={{ transform: 'rotate(-90deg)' }}>
        <circle cx={65} cy={65} r={r} fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth={10} />
        <circle cx={65} cy={65} r={r} fill="none" stroke={color} strokeWidth={10}
          strokeDasharray={`${dash} ${circ}`} strokeLinecap="round" opacity={0.85} />
      </svg>
      <div style={{ marginTop: -70, textAlign: 'center', zIndex: 1, position: 'relative' }}>
        <div style={{ fontSize: '1.6rem', fontWeight: 300, color }}>{psi.toFixed(4)}</div>
        <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.40)', textTransform: 'uppercase', letterSpacing: '0.10em' }}>PSI</div>
      </div>
    </div>
  )
}

// ── Real sparkline builders ───────────────────────────────────────────────────
// Build sparkline from model versions (AUC / Precision / Recall across versions)
function makeVersionSparkPoints(
  versions: { trained_at?: string | null; metrics: Record<string, number | null> }[],
  metricKey: string,
): { timestamp: string; value: number }[] {
  return [...versions]
    .filter(v => v.trained_at && typeof v.metrics[metricKey] === 'number')
    .sort((a, b) => new Date(a.trained_at!).getTime() - new Date(b.trained_at!).getTime())
    .map(v => ({ timestamp: v.trained_at!, value: v.metrics[metricKey] as number }))
}

// Build PSI sparkline from pipeline run history (real per-run PSI values)
function makePsiSparkPoints(
  history: { timestamp?: string | null; psi?: number | null }[],
): { timestamp: string; value: number }[] {
  return [...history]
    .filter(h => h.timestamp && h.psi != null)
    .sort((a, b) => new Date(a.timestamp!).getTime() - new Date(b.timestamp!).getTime())
    .slice(-8)
    .map(h => ({ timestamp: h.timestamp!, value: h.psi! }))
}

// ── Run status badge ──────────────────────────────────────────────────────────
function runStatusBadge(status: string): { label: string; color: string } {
  if (status === 'success') return { label: 'Success', color: COLORS.low }
  if (status === 'failed')  return { label: 'Failed',  color: COLORS.high }
  return                           { label: status,    color: 'rgba(255,255,255,0.40)' }
}

// ── PSI color helper ──────────────────────────────────────────────────────────
function psiColor(psi: number) {
  if (psi >= 0.25) return COLORS.high
  if (psi >= 0.10) return COLORS.medium
  return COLORS.low
}

function metricValue(metrics: Record<string, number | null | undefined>, key: string) {
  const value = metrics?.[key]
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function formatMetric(value: number | null, key: string) {
  if (value == null) return '—'
  if (['train_rows', 'val_rows', 'n_test'].includes(key)) return value.toLocaleString()
  return value.toFixed(4)
}

function metricColor(value: number | null, metric: MetricDef) {
  if (value == null) return 'rgba(255,255,255,0.38)'
  if (metric.target == null) return COLORS.primary
  const pass = metric.higherIsBetter ? value >= metric.target : value <= metric.target
  return pass ? COLORS.low : COLORS.medium
}

function metricDelta(candidate: number | null, baseline: number | null, metric: MetricDef) {
  if (candidate == null || baseline == null) return null
  const raw = candidate - baseline
  return metric.higherIsBetter ? raw : -raw
}

export default function ModelHealth() {
  const [historyN, setHistoryN] = useState(20)
  const [versionsOpen, setVersionsOpen] = useState(false)
  const [selectedMetricKey, setSelectedMetricKey] = useState('roc_auc')
  const [comparisonMetricKey, setComparisonMetricKey] = useState('f1')
  const [performanceView, setPerformanceView] = useState<'trend' | 'compare'>('trend')
  const [compareVersionA, setCompareVersionA] = useState('')
  const [compareVersionB, setCompareVersionB] = useState('')
  const driftRef  = useRef<HTMLDivElement>(null)
  const historyRef = useRef<HTMLDivElement>(null)
  const queryClient = useQueryClient()

  const scrollTo = (ref: React.RefObject<HTMLDivElement | null>) =>
    ref.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })

  const retrainMutation = useMutation({
    mutationFn: api.retrain,
    onMutate: () => {
      showToast('Starting retraining — this will take 1–3 minutes.', 'info')
    },
    onSuccess: (data) => {
      showToast(`Retraining started (${data.version}). New run will appear in Model History when complete.`, 'success')
      // Refresh model data after a delay to pick up the new version
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['modelStatus'] })
        queryClient.invalidateQueries({ queryKey: ['modelVersions'] })
        queryClient.invalidateQueries({ queryKey: ['history'] })
      }, 10000)
    },
    onError: (err: Error) => {
      showToast(`Retraining failed to start: ${err.message}`, 'error')
    },
  })

  const handleRetrain = () => retrainMutation.mutate()

  const { data: modelStatus, isLoading: lS } = useQuery({ queryKey: ['modelStatus'], queryFn: api.modelStatus, staleTime: 30000 })
  const { isLoading: lR } = useQuery({ queryKey: ['latestRun'], queryFn: api.latestRun, staleTime: 30000 })
  const { data: drift,       isLoading: lD } = useQuery({ queryKey: ['drift'],       queryFn: api.drift,       staleTime: 30000 })
  const { data: history,     isLoading: lH } = useQuery({ queryKey: ['history', historyN], queryFn: () => api.history(historyN), staleTime: 30000 })
  const { data: alerts } = useQuery({ queryKey: ['alerts'], queryFn: api.alerts, staleTime: 30000 })
  const { data: versions } = useQuery({ queryKey: ['modelVersions'], queryFn: api.modelVersions, staleTime: 60000 })

  const isLoading = lS || lR || lD

  const psi     = drift?.psi     ?? 0.0
  const m       = (modelStatus?.training_metrics ?? {}) as Record<string, number | null>

  const auc       = metricValue(m, 'roc_auc')
  const precision = metricValue(m, 'precision')
  const recall    = metricValue(m, 'recall')
  const f1        = metricValue(m, 'f1')
  const rmse      = metricValue(m, 'rmse')
  const selectedMetric = metricByKey[selectedMetricKey] ?? METRICS[0]
  const comparisonMetric = metricByKey[comparisonMetricKey] ?? METRICS[1]
  const modelVersions = versions ?? []
  const uniqueModelVersions = [...modelVersions]
    .sort((a, b) => {
      const statusRank = (status?: string) => status === 'active' ? 0 : status === 'candidate' ? 1 : 2
      const rankDelta = statusRank(a.status) - statusRank(b.status)
      if (rankDelta !== 0) return rankDelta
      return new Date(b.trained_at ?? 0).getTime() - new Date(a.trained_at ?? 0).getTime()
    })
    .reduce<typeof modelVersions>((acc, version) => {
      if (!acc.some(existing => existing.version_id === version.version_id)) acc.push(version)
      return acc
    }, [])

  // Metric trend: one point per model version, sorted by trained_at.
  // This is intentionally close to MLflow's "compare runs by metric" workflow.
  const metricTrend = uniqueModelVersions
        .filter(v => v.trained_at && typeof v.metrics?.[selectedMetric.key] === 'number')
        .sort((a, b) => new Date(a.trained_at!).getTime() - new Date(b.trained_at!).getTime())
        .map(v => ({
          date: new Date(v.trained_at!).toLocaleDateString('en-SG', { day: '2-digit', month: 'short' }),
          version: v.version_id,
          status: v.status,
          value: v.metrics[selectedMetric.key] as number,
        }))

  // Dynamic Y-axis bounds so the chart fits real AUC values
  const metricValues = metricTrend.map(p => p.value)
  const metricPad = selectedMetric.higherIsBetter ? 0.02 : 0.01
  const metricMin = metricValues.length > 0 ? Math.max(0, Math.min(...metricValues) - metricPad) : 0
  const metricMax = metricValues.length > 0
    ? Math.min(selectedMetric.kind === 'classification' ? 1 : Math.max(...metricValues) + metricPad, Math.max(...metricValues) + metricPad)
    : 1
  const activeVersion = uniqueModelVersions.find(v => v.version_id === modelStatus?.active_version)
  const baselineVersion = activeVersion ?? uniqueModelVersions[0]
  const sortedVersions = [...uniqueModelVersions].sort((a, b) => new Date(b.trained_at ?? 0).getTime() - new Date(a.trained_at ?? 0).getTime())
  const defaultCompareA = activeVersion?.version_id ?? sortedVersions[0]?.version_id ?? ''
  const defaultCompareB = sortedVersions.find(v => v.version_id !== defaultCompareA)?.version_id ?? defaultCompareA
  const compareVersionAId = compareVersionA || defaultCompareA
  const compareVersionBId = compareVersionB || defaultCompareB
  const modelA = sortedVersions.find(v => v.version_id === compareVersionAId)
  const modelB = sortedVersions.find(v => v.version_id === compareVersionBId)
  const compareMetricRows = METRICS.map(metric => {
    const a = modelA ? metricValue(modelA.metrics, metric.key) : null
    const b = modelB ? metricValue(modelB.metrics, metric.key) : null
    return {
      metric,
      a,
      b,
      rawDelta: a != null && b != null ? b - a : null,
      qualityDelta: metricDelta(b, a, metric),
    }
  })
  const chartMetricRows = compareMetricRows.filter(row => row.a != null || row.b != null)
  const chartAData = chartMetricRows
    .filter(row => row.a != null)
    .map(row => ({ metric: row.metric.label, value: row.a as number }))
  const chartBData = chartMetricRows
    .filter(row => row.b != null)
    .map(row => ({ metric: row.metric.label, value: row.b as number }))
  const selectedCompareMetricRow = compareMetricRows.find(row => row.metric.key === selectedMetric.key)

  // Sorted run history — guard against null/undefined timestamps
  const sortedHistory = history
    ? [...history].sort((a, b) => new Date(b.timestamp ?? 0).getTime() - new Date(a.timestamp ?? 0).getTime())
    : []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── Row 0: Controls ─────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
        {/* Left: model selector + version badge */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, position: 'relative' }}>
          <div
            className="glass"
            onClick={() => setVersionsOpen(o => !o)}
            style={{
              display: 'flex', alignItems: 'center', gap: 8,
              padding: '8px 16px', borderRadius: 10, cursor: 'pointer',
              fontSize: '0.88rem', fontWeight: 500, color: 'rgba(255,255,255,0.90)',
              userSelect: 'none',
            }}
          >
            Supply Shortage Risk Model
            <span style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.45)', marginLeft: 4 }}>
              {versionsOpen ? '▴' : '▾'}
            </span>
          </div>

          {/* Version dropdown */}
          {versionsOpen && uniqueModelVersions.length > 0 && (
            <div style={{
              position: 'absolute', top: '110%', left: 0, zIndex: 200,
              background: 'rgba(6,13,26,0.97)', border: '1px solid rgba(99,140,255,0.18)',
              borderRadius: 10, minWidth: 280, boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
              backdropFilter: 'blur(12px)', overflow: 'hidden',
            }}>
              {uniqueModelVersions.map(v => {
                const isActive = v.version_id === modelStatus?.active_version
                const isCandidate = v.version_id === modelStatus?.candidate_version
                const statusColor = isActive ? COLORS.low : isCandidate ? COLORS.primary : 'rgba(255,255,255,0.28)'
                const statusLabel = isActive ? 'Production' : isCandidate ? 'Candidate' : v.status
                return (
                  <div key={v.version_id} style={{
                    padding: '10px 16px',
                    borderBottom: '1px solid rgba(255,255,255,0.06)',
                    display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12,
                  }}>
                    <div>
                      <div style={{ fontSize: '0.80rem', fontWeight: 600, color: 'rgba(255,255,255,0.88)', fontFamily: 'monospace' }}>
                        {v.version_id}
                      </div>
                      {v.trained_at && (
                        <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.38)', marginTop: 2 }}>
                          {new Date(v.trained_at).toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' })}
                        </div>
                      )}
                    </div>
                    <span style={{
                      fontSize: '0.62rem', fontWeight: 700, letterSpacing: '0.08em',
                      background: `${statusColor}18`, color: statusColor,
                      border: `1px solid ${statusColor}40`, borderRadius: 20, padding: '2px 8px',
                      whiteSpace: 'nowrap',
                    }}>
                      {statusLabel}
                    </span>
                  </div>
                )
              })}
            </div>
          )}

          <span style={{
            display: 'inline-flex', alignItems: 'center', gap: 6,
            padding: '5px 12px', borderRadius: 20,
            background: `${COLORS.low}18`, border: `1px solid ${COLORS.low}40`,
            fontSize: '0.68rem', fontWeight: 600, color: COLORS.low, letterSpacing: '0.06em',
          }}>
            {modelStatus?.active_version ? `${modelStatus.active_version} – Production` : 'No active version'}
          </span>
        </div>

        {/* Right: ML Ops label + Retrain button */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{
            padding: '4px 12px', borderRadius: 20,
            background: `${COLORS.primary}18`, border: `1px solid ${COLORS.primary}40`,
            fontSize: '0.60rem', fontWeight: 700, letterSpacing: '0.12em',
            color: COLORS.primary, textTransform: 'uppercase',
          }}>
            ML Ops
          </span>
          <button
            className="btn-primary"
            onClick={handleRetrain}
            disabled={retrainMutation.isPending}
            style={{
              display: 'flex', alignItems: 'center', gap: 7,
              padding: '8px 18px', borderRadius: 10, border: 'none',
              cursor: retrainMutation.isPending ? 'not-allowed' : 'pointer',
              fontSize: '0.82rem', fontWeight: 600,
              opacity: retrainMutation.isPending ? 0.65 : 1,
            }}
          >
            <RefreshCw size={14} style={{ animation: retrainMutation.isPending ? 'spin 1s linear infinite' : 'none' }} />
            {retrainMutation.isPending ? 'Starting…' : 'Retrain Model'}
          </button>
        </div>
      </div>

      {isLoading && <Spinner />}

      {/* ── Row 1: KPI cards ────────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, minmax(0, 1fr))', gap: 12 }}>

        {/* AUC */}
        <GlassCard accentColor={COLORS.primary} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>AUC</div>
          <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 8 }}>
            <div>
              <div style={{ fontSize: '1.55rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1.1 }}>
                {auc != null ? auc.toFixed(4) : '—'}
              </div>
              <div style={{ marginTop: 6, fontSize: '0.63rem', color: 'rgba(255,255,255,0.35)' }}>
                From active model registry
              </div>
            </div>
            {versions && versions.length > 1 && (
              <SparkLine data={makeVersionSparkPoints(versions, 'roc_auc')} color={COLORS.primary} height={34} width={64} />
            )}
          </div>
        </GlassCard>

        {/* F1 */}
        <GlassCard accentColor={metricColor(f1, metricByKey.f1)} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>F1</div>
          <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 8 }}>
            <div>
              <div style={{ fontSize: '1.55rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1.1 }}>
                {formatMetric(f1, 'f1')}
              </div>
              <div style={{ marginTop: 6, fontSize: '0.63rem', color: 'rgba(255,255,255,0.35)' }}>
                Best threshold balance
              </div>
            </div>
            {versions && versions.length > 1 && (
              <SparkLine data={makeVersionSparkPoints(versions, 'f1')} color={COLORS.low} height={34} width={64} />
            )}
          </div>
        </GlassCard>

        {/* Precision */}
        <GlassCard accentColor={COLORS.primary} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>Precision</div>
          <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 8 }}>
            <div>
              <div style={{ fontSize: '1.55rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1.1 }}>
                {precision != null ? precision.toFixed(4) : '—'}
              </div>
              <div style={{ marginTop: 6, fontSize: '0.63rem', color: 'rgba(255,255,255,0.35)' }}>
                From active model registry
              </div>
            </div>
            {versions && versions.length > 1 && (
              <SparkLine data={makeVersionSparkPoints(versions, 'precision')} color={COLORS.high} height={34} width={64} />
            )}
          </div>
        </GlassCard>

        {/* Recall */}
        <GlassCard accentColor={COLORS.primary} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>Recall</div>
          <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 8 }}>
            <div>
              <div style={{ fontSize: '1.55rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1.1 }}>
                {recall != null ? recall.toFixed(4) : '—'}
              </div>
              <div style={{ marginTop: 6, fontSize: '0.63rem', color: 'rgba(255,255,255,0.35)' }}>
                From active model registry
              </div>
            </div>
            {versions && versions.length > 1 && (
              <SparkLine data={makeVersionSparkPoints(versions, 'recall')} color={COLORS.low} height={34} width={64} />
            )}
          </div>
        </GlassCard>

        {/* RMSE */}
        <GlassCard accentColor={COLORS.medium} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>RMSE</div>
          <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 8 }}>
            <div>
              <div style={{ fontSize: '1.55rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1.1 }}>
                {formatMetric(rmse, 'rmse')}
              </div>
              <div style={{ marginTop: 6, fontSize: '0.63rem', color: 'rgba(255,255,255,0.35)' }}>
                Probability error
              </div>
            </div>
            {versions && versions.length > 1 && (
              <SparkLine data={makeVersionSparkPoints(versions, 'rmse')} color={COLORS.medium} height={34} width={64} />
            )}
          </div>
        </GlassCard>

        {/* PSI Drift */}
        <GlassCard accentColor={psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>PSI (Drift)</div>
          <div style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', gap: 8 }}>
            <div>
              <div style={{ fontSize: '1.55rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1.1 }}>
                {drift ? psi.toFixed(4) : '—'}
              </div>
              <div style={{ marginTop: 6 }}>
                {drift
                  ? <Badge
                      label={drift.drift_level === 'stable' ? 'STABLE' : drift.drift_level === 'warning' ? 'WARNING' : 'DRIFT DETECTED'}
                      color={psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low}
                    />
                  : <Badge label="NO DATA" color="rgba(255,255,255,0.28)" />
                }
              </div>
            </div>
            {history && history.length > 0 && (
              <SparkLine data={makePsiSparkPoints(history)} color={psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low} height={34} width={64} />
            )}
          </div>
        </GlassCard>

        {/* Last Trained */}
        <GlassCard accentColor={COLORS.low} hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>Last Trained</div>
          <div style={{ fontSize: '0.90rem', fontWeight: 400, color: 'rgba(255,255,255,0.88)', lineHeight: 1.3, marginBottom: 6 }}>
            {modelStatus?.last_retrained_at
              ? new Date(modelStatus.last_retrained_at).toLocaleString('en-SG', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' })
              : '—'}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            {modelStatus?.last_retrained_at
              ? <Badge label="Success" color={COLORS.low} />
              : <Badge label="Never" color="rgba(255,255,255,0.28)" />
            }
          </div>
        </GlassCard>
      </div>

      {/* ── Row 2: Drift + Performance Trend ────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>

        {/* LEFT: Data & Prediction Drift */}
        <div ref={driftRef}><GlassCard hover={false} style={{ padding: '20px 22px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 16 }}>Data &amp; Prediction Drift</div>

          <div style={{ display: 'flex', gap: 20, alignItems: 'flex-start' }}>
            {/* Gauge — ~35% */}
            <div style={{ flex: '0 0 35%', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
              <PsiCircle psi={psi} />
              {drift && (
                <div style={{
                  marginTop: 10, display: 'flex', alignItems: 'center', gap: 5,
                  padding: '5px 10px', borderRadius: 8,
                  background: `${psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low}14`,
                  border: `1px solid ${psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low}35`,
                }}>
                  <AlertTriangle size={11} color={psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low} />
                  <span style={{ fontSize: '0.62rem', color: psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low }}>
                    {psi >= 0.25 ? 'Above alert threshold (0.25)' : psi >= 0.10 ? 'Above warning threshold (0.10)' : 'Within stable range'}
                  </span>
                </div>
              )}
            </div>

            {/* Score Distribution Shift — from real DriftReport */}
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ ...SECTION_LABEL, marginBottom: 12 }}>Score Distribution Shift</div>
              {drift ? (() => {
                const rows = [
                  { label: 'Mean score',   ref: drift.reference_mean, cur: drift.current_mean },
                  { label: 'Std dev',      ref: drift.reference_std,  cur: drift.current_std  },
                ]
                const refN = drift.reference_n
                const curN = drift.current_n
                return (
                  <>
                    {rows.map(r => {
                      const delta = r.cur - r.ref
                      const absDelta = Math.abs(delta)
                      const barPct = Math.min(absDelta / 0.3, 1)  // 0.30 = full bar
                      const barColor = absDelta > 0.10 ? COLORS.high : absDelta > 0.04 ? COLORS.medium : COLORS.low
                      return (
                        <div key={r.label} style={{ marginBottom: 12 }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.70rem', marginBottom: 4 }}>
                            <span style={{ color: 'rgba(255,255,255,0.55)' }}>{r.label}</span>
                            <span style={{ color: barColor, fontWeight: 500 }}>
                              {r.ref.toFixed(3)} → {r.cur.toFixed(3)}
                              <span style={{ color: delta > 0 ? COLORS.high : COLORS.low, marginLeft: 5 }}>
                                ({delta >= 0 ? '+' : ''}{delta.toFixed(3)})
                              </span>
                            </span>
                          </div>
                          <div style={{ background: 'rgba(255,255,255,0.07)', borderRadius: 4, height: 5 }}>
                            <div style={{ width: `${barPct * 100}%`, height: '100%', background: barColor, borderRadius: 4, maxWidth: '100%' }} />
                          </div>
                        </div>
                      )
                    })}
                    {(refN != null || curN != null) && (
                      <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.35)', marginTop: 6 }}>
                        Reference n={refN ?? '—'} · Current n={curN ?? '—'}
                      </div>
                    )}
                    {drift?.feature_drift && Object.keys(drift.feature_drift).length > 0 && (
                      <div style={{ marginTop: 12 }}>
                        <div style={{ ...SECTION_LABEL, marginBottom: 8, fontSize: '0.60rem' }}>Per-Feature Drift (PSI)</div>
                        {Object.entries(drift.feature_drift).map(([feat, fd]) => (
                          <div key={feat} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.68rem', marginBottom: 4 }}>
                            <span style={{ color: 'rgba(255,255,255,0.50)', textTransform: 'capitalize' }}>{feat.replace(/_/g, ' ')}</span>
                            <span style={{ color: psiColor(fd.psi), fontWeight: 500 }}>{fd.psi.toFixed(4)} <span style={{ color: 'rgba(255,255,255,0.30)', fontWeight: 400 }}>({fd.drift_level})</span></span>
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                )
              })() : (
                <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.30)', marginTop: 8 }}>
                  No drift report available — run the scoring pipeline first.
                </div>
              )}
            </div>
          </div>

        </GlassCard></div>

        {/* RIGHT: Performance Trend */}
        <GlassCard hover={false} style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'flex-start', marginBottom: 10 }}>
            <div>
              <div style={{ ...SECTION_LABEL, marginBottom: 4 }}>Performance Trend</div>
              <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.42)' }}>
                Compare model versions by classification and error metrics.
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              <div style={{
                display: 'inline-flex',
                padding: 2,
                borderRadius: 8,
                background: 'rgba(255,255,255,0.05)',
                border: '1px solid rgba(255,255,255,0.10)',
              }}>
                {[
                  { key: 'trend', label: 'Trend' },
                  { key: 'compare', label: 'Compare Models' },
                ].map(tab => (
                  <button
                    key={tab.key}
                    onClick={() => setPerformanceView(tab.key as 'trend' | 'compare')}
                    style={{
                      border: 'none',
                      borderRadius: 6,
                      padding: '5px 9px',
                      background: performanceView === tab.key ? 'rgba(69,120,200,0.22)' : 'transparent',
                      color: performanceView === tab.key ? COLORS.primary : 'rgba(255,255,255,0.52)',
                      fontSize: '0.68rem',
                      fontWeight: 600,
                      cursor: 'pointer',
                    }}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
              <select
                value={selectedMetricKey}
                onChange={e => setSelectedMetricKey(e.target.value)}
                style={{
                  background: 'rgba(255,255,255,0.06)',
                  border: '1px solid rgba(255,255,255,0.12)',
                  color: 'rgba(255,255,255,0.82)',
                  borderRadius: 8,
                  padding: '6px 10px',
                  fontSize: '0.72rem',
                  outline: 'none',
                }}
              >
                <optgroup label="Classification">
                  {METRICS.filter(metric => metric.kind === 'classification').map(metric => (
                    <option key={metric.key} value={metric.key}>{metric.label}</option>
                  ))}
                </optgroup>
                <optgroup label="Probability / Regression">
                  {METRICS.filter(metric => metric.kind !== 'classification').map(metric => (
                    <option key={metric.key} value={metric.key}>{metric.label}</option>
                  ))}
                </optgroup>
              </select>
            </div>
          </div>

          <div style={{ display: 'flex', gap: 18, marginBottom: 12, flexWrap: 'wrap' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <svg width={20} height={10}><line x1={0} y1={5} x2={20} y2={5} stroke={COLORS.primary} strokeWidth={2} /></svg>
              <span style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.55)' }}>{selectedMetric.label}</span>
            </div>
            {selectedMetric.target != null && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <svg width={20} height={10}><line x1={0} y1={5} x2={20} y2={5} stroke="rgba(255,255,255,0.50)" strokeWidth={1.5} strokeDasharray="4 2" /></svg>
                <span style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.55)' }}>Target ({selectedMetric.target.toFixed(2)})</span>
              </div>
            )}
            <span style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.36)' }}>
              {selectedMetric.higherIsBetter ? 'Higher is better' : 'Lower is better'} · {selectedMetric.description}
            </span>
          </div>

          {performanceView === 'trend' && metricTrend.length === 0 && (
            <div style={{ height: 180, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'rgba(255,255,255,0.28)', fontSize: '0.78rem' }}>
              No {selectedMetric.label} values available for model versions.
            </div>
          )}
          {performanceView === 'trend' && metricTrend.length > 0 && <ResponsiveContainer width="100%" height={180}>
            <LineChart data={metricTrend} margin={{ top: 6, right: 12, bottom: 0, left: -10 }}>
              <XAxis
                dataKey="date"
                tick={{ fontSize: 10, fill: 'rgba(255,255,255,0.35)' }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                domain={[metricMin, metricMax]}
                tick={{ fontSize: 10, fill: 'rgba(255,255,255,0.35)' }}
                tickLine={false}
                axisLine={false}
                tickFormatter={v => Number(v).toFixed(2)}
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                formatter={(v) => [Number(v).toFixed(4), selectedMetric.label]}
              />
              {selectedMetric.target != null && (
                <ReferenceLine
                  y={selectedMetric.target}
                  stroke="rgba(255,255,255,0.45)"
                  strokeDasharray="6 3"
                  strokeWidth={1.5}
                />
              )}
              <Line
                type="monotone"
                dataKey="value"
                stroke={COLORS.primary}
                strokeWidth={2}
                dot={{ r: 3, fill: COLORS.primary, strokeWidth: 0 }}
                activeDot={{ r: 5, fill: COLORS.primary }}
              />
            </LineChart>
          </ResponsiveContainer>}

          {performanceView === 'compare' && (
            <div style={{ minHeight: 180, display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                {[
                  { label: 'Model A', value: compareVersionAId, setter: setCompareVersionA, model: modelA, color: 'rgba(148,163,184,0.95)' },
                  { label: 'Model B', value: compareVersionBId, setter: setCompareVersionB, model: modelB, color: COLORS.primary },
                ].map(item => (
                  <div key={item.label} style={{
                    padding: '9px 11px',
                    borderRadius: 9,
                    background: 'rgba(69,120,200,0.07)',
                    border: `1px solid ${item.color}44`,
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center', marginBottom: 7 }}>
                      <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.34)', textTransform: 'uppercase', letterSpacing: '0.10em' }}>
                        {item.label}
                      </div>
                      {item.model && <Badge label={item.model.status} color={item.model.status === 'active' ? COLORS.low : item.model.status === 'candidate' ? COLORS.primary : 'rgba(255,255,255,0.42)'} />}
                    </div>
                    <select
                      value={item.value}
                      onChange={e => item.setter(e.target.value)}
                      style={{
                        width: '100%',
                        background: 'rgba(255,255,255,0.06)',
                        border: '1px solid rgba(255,255,255,0.12)',
                        color: 'rgba(255,255,255,0.84)',
                        borderRadius: 8,
                        padding: '6px 8px',
                        fontSize: '0.72rem',
                        outline: 'none',
                        fontFamily: 'monospace',
                      }}
                    >
                      {sortedVersions.map(v => (
                        <option key={`${item.label}-${v.version_id}`} value={v.version_id}>
                          {v.version_id} · {v.status}
                        </option>
                      ))}
                    </select>
                    <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.32)', marginTop: 6 }}>
                      {item.model?.trained_at ? fmtDate(item.model.trained_at) : 'No version selected'}
                    </div>
                  </div>
                ))}
              </div>

              {sortedVersions.length < 2 ? (
                <div style={{ height: 180, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'rgba(255,255,255,0.28)', fontSize: '0.78rem' }}>
                  Need at least two model versions to compare.
                </div>
              ) : (
                <>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                    {[
                      { slot: 'model-a', title: modelA?.version_id ?? 'Model A', data: chartAData, color: 'rgba(148,163,184,0.95)' },
                      { slot: 'model-b', title: modelB?.version_id ?? 'Model B', data: chartBData, color: COLORS.primary },
                    ].map(chart => (
                      <div key={chart.slot} style={{ height: 170, padding: '8px 4px 2px', borderRadius: 9, background: 'rgba(255,255,255,0.025)', border: '1px solid rgba(255,255,255,0.06)' }}>
                        <div style={{ fontSize: '0.62rem', fontFamily: 'monospace', color: 'rgba(255,255,255,0.62)', margin: '0 8px 4px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {chart.title}
                        </div>
                        <ResponsiveContainer width="100%" height={140}>
                          <LineChart data={chart.data} margin={{ top: 4, right: 8, bottom: 0, left: -24 }}>
                            <XAxis dataKey="metric" tick={{ fontSize: 9, fill: 'rgba(255,255,255,0.34)' }} tickLine={false} axisLine={false} interval={0} />
                            <YAxis tick={{ fontSize: 9, fill: 'rgba(255,255,255,0.34)' }} tickLine={false} axisLine={false} tickFormatter={v => Number(v).toFixed(2)} />
                            <Tooltip
                              contentStyle={TOOLTIP_STYLE}
                              labelStyle={TOOLTIP_LABEL_STYLE}
                              formatter={(v) => [Number(v).toFixed(4), 'Metric']}
                            />
                            <Line
                              type="monotone"
                              dataKey="value"
                              stroke={chart.color}
                              strokeWidth={2}
                              dot={{ r: 3, fill: chart.color, strokeWidth: 0 }}
                              activeDot={{ r: 5, fill: chart.color }}
                            />
                          </LineChart>
                        </ResponsiveContainer>
                      </div>
                    ))}
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 8 }}>
                    {compareMetricRows
                      .filter(row => ['roc_auc', 'f1', 'precision', 'recall'].includes(row.metric.key))
                      .map(row => {
                        const color = row.qualityDelta == null || Math.abs(row.qualityDelta) < 0.0001
                          ? 'rgba(255,255,255,0.42)'
                          : row.qualityDelta > 0 ? COLORS.low : COLORS.high
                        return (
                          <div key={row.metric.key} style={{ padding: '9px 10px', borderRadius: 8, background: 'rgba(255,255,255,0.035)', border: '1px solid rgba(255,255,255,0.07)' }}>
                            <div style={{ fontSize: '0.56rem', color: 'rgba(255,255,255,0.32)', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 5 }}>
                              {row.metric.label}
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, fontSize: '0.68rem', color: 'rgba(255,255,255,0.62)' }}>
                              <span>{formatMetric(row.a, row.metric.key)}</span>
                              <span>{formatMetric(row.b, row.metric.key)}</span>
                            </div>
                            <div style={{ marginTop: 4, color, fontSize: '0.74rem', fontWeight: 700, fontVariantNumeric: 'tabular-nums' }}>
                              {row.rawDelta == null ? '—' : `${row.rawDelta >= 0 ? '+' : ''}${row.rawDelta.toFixed(4)}`}
                            </div>
                          </div>
                        )
                      })}
                  </div>
                </>
              )}
            </div>
          )}

          {/* Annotation */}
          <div style={{ marginTop: 8, fontSize: '0.65rem', color: 'rgba(255,255,255,0.42)', display: 'flex', justifyContent: 'space-between', gap: 8 }}>
            <span>{performanceView === 'trend' ? `${metricTrend.length} version${metricTrend.length === 1 ? '' : 's'} with ${selectedMetric.label}` : `${modelA?.version_id ?? 'Model A'} vs ${modelB?.version_id ?? 'Model B'}`}</span>
            <span>{selectedMetric.label} delta: <span style={{ color: selectedCompareMetricRow?.qualityDelta == null ? 'rgba(255,255,255,0.42)' : selectedCompareMetricRow.qualityDelta >= 0 ? COLORS.low : COLORS.high, fontWeight: 500 }}>{selectedCompareMetricRow?.rawDelta == null ? '—' : `${selectedCompareMetricRow.rawDelta >= 0 ? '+' : ''}${selectedCompareMetricRow.rawDelta.toFixed(4)}`}</span></span>
          </div>

        </GlassCard>
      </div>

      {/* ── Row 3: Model Version Comparison ─────────────────────────────── */}
      <GlassCard hover={false} style={{ padding: '20px 22px', overflow: 'hidden' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, marginBottom: 14 }}>
          <div>
            <div style={{ ...SECTION_LABEL, marginBottom: 4 }}>Model Version Comparison</div>
            <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)' }}>
              MLflow-style run comparison using the metrics stored in the file registry.
            </div>
          </div>
          <select
            value={comparisonMetricKey}
            onChange={e => setComparisonMetricKey(e.target.value)}
            style={{
              background: 'rgba(255,255,255,0.06)',
              border: '1px solid rgba(255,255,255,0.12)',
              color: 'rgba(255,255,255,0.82)',
              borderRadius: 8,
              padding: '6px 10px',
              fontSize: '0.72rem',
              outline: 'none',
            }}
          >
            {METRICS.map(metric => (
              <option key={metric.key} value={metric.key}>{metric.label}</option>
            ))}
          </select>
        </div>

        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.76rem' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.08)' }}>
                {[
                  'VERSION',
                  'STATUS',
                  'TRAINED',
                  'ROC AUC',
                  'F1',
                  'PRECISION',
                  'RECALL',
                  'MAE',
                  'RMSE',
                  `${comparisonMetric.label.toUpperCase()} DELTA`,
                ].map(h => (
                  <th key={h} style={{
                    padding: '8px 12px', textAlign: 'left',
                    fontSize: '0.57rem', fontWeight: 700, letterSpacing: '0.10em',
                    textTransform: 'uppercase', color: 'rgba(255,255,255,0.30)',
                    whiteSpace: 'nowrap',
                  }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedVersions.length > 0 ? sortedVersions.map(v => {
                const statusColor = v.status === 'active' ? COLORS.low : v.status === 'candidate' ? COLORS.primary : 'rgba(255,255,255,0.42)'
                const baselineMetric = baselineVersion ? metricValue(baselineVersion.metrics, comparisonMetric.key) : null
                const currentMetric = metricValue(v.metrics, comparisonMetric.key)
                const delta = metricDelta(currentMetric, baselineMetric, comparisonMetric)
                return (
                  <tr key={v.version_id} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                    <td style={{ padding: '10px 12px', fontFamily: 'monospace', color: 'rgba(255,255,255,0.78)', whiteSpace: 'nowrap' }}>
                      {v.version_id}
                    </td>
                    <td style={{ padding: '10px 12px' }}>
                      <Badge label={v.status} color={statusColor} />
                    </td>
                    <td style={{ padding: '10px 12px', color: 'rgba(255,255,255,0.45)', whiteSpace: 'nowrap' }}>
                      {v.trained_at ? fmtDate(v.trained_at) : '—'}
                    </td>
                    {['roc_auc', 'f1', 'precision', 'recall', 'mae', 'rmse'].map(key => {
                      const metric = metricByKey[key] ?? comparisonMetric
                      const value = metricValue(v.metrics, key)
                      return (
                        <td key={key} style={{ padding: '10px 12px', color: metricColor(value, metric), fontVariantNumeric: 'tabular-nums' }}>
                          {formatMetric(value, key)}
                        </td>
                      )
                    })}
                    <td style={{
                      padding: '10px 12px',
                      color: delta == null ? 'rgba(255,255,255,0.30)' : delta >= 0 ? COLORS.low : COLORS.high,
                      fontVariantNumeric: 'tabular-nums',
                      whiteSpace: 'nowrap',
                    }}>
                      {delta == null ? '—' : `${delta >= 0 ? '+' : ''}${delta.toFixed(4)}`}
                    </td>
                  </tr>
                )
              }) : (
                <tr>
                  <td colSpan={10} style={{ padding: '28px 14px', textAlign: 'center', color: 'rgba(255,255,255,0.28)', fontSize: '0.80rem' }}>
                    No model versions available — run training to populate the registry.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </GlassCard>

      {/* ── Row 4: Operational Run History ──────────────────────────────── */}
      <div ref={historyRef}><GlassCard hover={false} style={{ padding: '20px 22px', overflow: 'hidden' }}>
        {/* Section header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 18, flexWrap: 'wrap', gap: 10 }}>
          <div>
            <div style={{ ...SECTION_LABEL, marginBottom: 4 }}>Operational Run History</div>
            <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)' }}>
              Recent scoring runs and pipeline events. Model-version comparison above is the experiment-tracking view.
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.38)' }}>Last {historyN}</span>
              <input
                type="range" min={5} max={50} value={historyN}
                onChange={e => setHistoryN(Number(e.target.value))}
                style={{ accentColor: COLORS.primary, width: 90, cursor: 'pointer' }}
              />
            </div>
            <button onClick={() => setHistoryN(50)} style={{ fontSize: '0.72rem', color: COLORS.primary, background: 'none', border: 'none', cursor: 'pointer', padding: 0, whiteSpace: 'nowrap' }}>
              View All Runs →
            </button>
          </div>
        </div>

        {lH && <Spinner size={24} />}

        {!lH && (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.08)' }}>
                  {['RUN ID', 'TIMESTAMP', 'ACTIVE TAXIS', 'PSI', 'FLAGGED ZONES', 'LATENCY', 'STATUS', 'ACTION'].map(h => (
                    <th key={h} style={{
                      padding: '8px 14px', textAlign: 'left',
                      fontSize: '0.57rem', fontWeight: 700, letterSpacing: '0.11em',
                      textTransform: 'uppercase', color: 'rgba(255,255,255,0.30)',
                      whiteSpace: 'nowrap',
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedHistory.length > 0
                  ? sortedHistory.map((h, i) => {
                      const rs = runStatusBadge(h.run_status ?? 'unknown')
                      const rowPsi = h.psi != null ? h.psi : null
                      // supply_now = taxis in current snapshot; total_taxi_count = all rows scored (misleading)
                      const activeTaxis = h.supply_now != null
                        ? h.supply_now.toLocaleString()
                        : '—'
                      return (
                        <tr key={i} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                          <td style={{ padding: '10px 14px', fontFamily: 'monospace', fontSize: '0.70rem', color: 'rgba(255,255,255,0.50)' }}>
                            {String(h.run_id ?? '').slice(0, 8) || '—'}
                          </td>
                          <td style={{ padding: '10px 14px', color: 'rgba(255,255,255,0.50)', whiteSpace: 'nowrap' }}>
                            {h.timestamp ? fmtDate(h.timestamp) : '—'}
                          </td>
                          <td style={{ padding: '10px 14px', color: 'rgba(255,255,255,0.62)', fontVariantNumeric: 'tabular-nums' }}>
                            {activeTaxis}
                          </td>
                          <td style={{ padding: '10px 14px', fontWeight: 500, color: rowPsi != null ? psiColor(rowPsi) : 'rgba(255,255,255,0.30)' }}>
                            {rowPsi != null ? rowPsi.toFixed(4) : '—'}
                          </td>
                          <td style={{ padding: '10px 14px', color: h.flagged_zones ? COLORS.medium : 'rgba(255,255,255,0.42)', fontVariantNumeric: 'tabular-nums' }}>
                            {h.flagged_zones ?? '—'}
                          </td>
                          <td style={{ padding: '10px 14px', color: 'rgba(255,255,255,0.42)', fontSize: '0.70rem' }}>
                            {h.latency_ms ? `${h.latency_ms}ms` : '—'}
                          </td>
                          <td style={{ padding: '10px 14px' }}>
                            <Badge label={rs.label} color={rs.color} />
                          </td>
                          <td style={{ padding: '10px 14px' }}>
                            <button className="btn-glass" onClick={() => showToast(`Run ${String(h.run_id ?? '').slice(0,8)}: ${activeTaxis} taxis, ${h.flagged_zones} flagged, PSI ${h.psi?.toFixed(4) ?? '—'} — ${h.run_status}`, 'info')} style={{
                              fontSize: '0.65rem', padding: '4px 10px', borderRadius: 7, cursor: 'pointer',
                            }}>View Report</button>
                          </td>
                        </tr>
                      )
                    })
                  : (
                    <tr>
                      <td colSpan={8} style={{ padding: '28px 14px', textAlign: 'center', color: 'rgba(255,255,255,0.28)', fontSize: '0.80rem' }}>
                        No run history available — run the scoring pipeline to populate data
                      </td>
                    </tr>
                  )
                }
              </tbody>
            </table>
          </div>
        )}
      </GlassCard></div>

      {/* ── Row 4: Recommendation / Alerts banner ───────────────────────── */}
      {(() => {
        // Derive banner content from real API data
        const driftAlert  = alerts?.find(a => a.alert_id === 'DRIFT_ALERT')
        const rollbackAlert = alerts?.find(a => a.alert_id === 'ROLLBACK_OCCURRED')
        const failedAlert = alerts?.find(a => a.alert_id === 'HIGH_FAILED_ROWS')
        const activeAlert = driftAlert ?? rollbackAlert ?? failedAlert

        const driftFlag   = drift?.drift_flag ?? false
        const driftLevel  = drift?.drift_level ?? 'stable'
        const bannerColor = driftFlag || activeAlert?.severity === 'high'
          ? COLORS.high
          : activeAlert?.severity === 'medium'
            ? COLORS.medium
            : COLORS.low

        const title = activeAlert
          ? (driftAlert ? 'Retraining Recommended — Drift Detected'
             : rollbackAlert ? 'Model Rollback Occurred'
             : 'High Validation Failure Rate')
          : driftFlag
            ? 'Retraining Recommended — Drift Detected'
            : 'Model Healthy — No Action Required'

        const body = activeAlert
          ? activeAlert.message
          : driftFlag
            ? `PSI ${psi.toFixed(4)} exceeds drift threshold (0.25). Consider retraining to restore model accuracy.`
            : `PSI ${psi.toFixed(4)} is within acceptable bounds. Drift level: ${driftLevel}.`

        return (
          <GlassCard hover={false} style={{
            padding: '20px 24px',
            borderLeft: `4px solid ${bannerColor}`,
            display: 'flex', alignItems: 'center', gap: 20, flexWrap: 'wrap',
          }}>
            {/* Left: icon + text */}
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: 14, flex: 1, minWidth: 260 }}>
              <div style={{
                width: 38, height: 38, borderRadius: 10, flexShrink: 0,
                background: `${bannerColor}18`, border: `1px solid ${bannerColor}40`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Lightbulb size={18} color={bannerColor} />
              </div>
              <div>
                <div style={{ ...SECTION_LABEL, color: bannerColor, marginBottom: 4 }}>
                  {driftFlag || activeAlert ? 'Alert' : 'Status'}
                </div>
                <div style={{ fontSize: '0.96rem', fontWeight: 600, color: 'rgba(255,255,255,0.92)', marginBottom: 6 }}>
                  {title}
                </div>
                <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.48)', lineHeight: 1.5, maxWidth: 480 }}>
                  {body}
                </div>
              </div>
            </div>

            {/* Right: PSI stat + actions */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 20, flexShrink: 0 }}>
              <div style={{
                padding: '12px 18px', borderRadius: 12,
                background: `${bannerColor}10`, border: `1px solid ${bannerColor}30`,
                textAlign: 'center',
              }}>
                <div style={{ ...SECTION_LABEL, marginBottom: 4 }}>Current PSI</div>
                <div style={{ fontSize: '1.10rem', fontWeight: 500, color: bannerColor }}>
                  {drift ? psi.toFixed(4) : '—'}
                </div>
                <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.35)', marginTop: 2 }}>
                  {driftLevel.charAt(0).toUpperCase() + driftLevel.slice(1)}
                </div>
              </div>

              {(driftFlag || activeAlert) && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  <button
                    className="btn-primary"
                    onClick={handleRetrain}
                    disabled={retrainMutation.isPending}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 7,
                      padding: '9px 18px', borderRadius: 10, border: 'none',
                      cursor: retrainMutation.isPending ? 'not-allowed' : 'pointer',
                      fontSize: '0.82rem', fontWeight: 600, whiteSpace: 'nowrap',
                      opacity: retrainMutation.isPending ? 0.65 : 1,
                    }}
                  >
                    <TrendingUp size={14} />
                    {retrainMutation.isPending ? 'Starting…' : 'Schedule Retraining'}
                  </button>
                  <button className="btn-glass" onClick={() => scrollTo(driftRef)} style={{
                    padding: '8px 18px', borderRadius: 10, cursor: 'pointer',
                    fontSize: '0.82rem', fontWeight: 500, whiteSpace: 'nowrap',
                  }}>
                    View Details
                  </button>
                </div>
              )}
            </div>
          </GlassCard>
        )
      })()}

    </div>
  )
}
