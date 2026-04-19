import { useRef, useState, useMemo } from 'react'
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

// ── PSI circular gauge ──────────────────────────────────────────────────────
function PsiCircle({ psi }: { psi: number }) {
  const pct = Math.min(psi / 0.5, 1)
  const color = psi >= 0.25 ? '#FF4D6D' : psi >= 0.10 ? '#F59E0B' : '#10D98A'
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
  versions: { trained_at?: string | null; metrics: Record<string, number> }[],
  metricKey: string,
): { timestamp: string; value: number }[] {
  return [...versions]
    .filter(v => v.trained_at && v.metrics[metricKey] != null)
    .sort((a, b) => new Date(a.trained_at!).getTime() - new Date(b.trained_at!).getTime())
    .map(v => ({ timestamp: v.trained_at!, value: v.metrics[metricKey] }))
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

export default function ModelHealth() {
  const [historyN, setHistoryN] = useState(20)
  const [versionsOpen, setVersionsOpen] = useState(false)
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

  // Capture current time once — used to compute relative age labels (keeps render pure)
  const nowMs = useMemo(() => Date.now(), [])

  const psi     = drift?.psi     ?? 0.0
  const m       = (modelStatus?.training_metrics ?? {}) as Record<string, number>

  const auc       = m.roc_auc   ?? null
  const precision = m.precision ?? null
  const recall    = m.recall    ?? null

  // AUC trend: one point per model version, sorted by trained_at.
  // Each point shows the AUC that version achieved — real historical progression.
  const aucTrend = versions
    ? [...versions]
        .filter(v => v.trained_at && v.metrics?.roc_auc != null)
        .sort((a, b) => new Date(a.trained_at!).getTime() - new Date(b.trained_at!).getTime())
        .map(v => ({
          date: new Date(v.trained_at!).toLocaleDateString('en-SG', { day: '2-digit', month: 'short' }),
          auc:  v.metrics.roc_auc as number,
        }))
    : []

  // Dynamic Y-axis bounds so the chart fits real AUC values
  const aucValues = aucTrend.map(p => p.auc)
  const aucMin = aucValues.length > 0 ? Math.max(0,   Math.min(...aucValues) - 0.02) : 0.80
  const aucMax = aucValues.length > 0 ? Math.min(1.0, Math.max(...aucValues) + 0.02) : 1.00

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
          {versionsOpen && versions && versions.length > 0 && (
            <div style={{
              position: 'absolute', top: '110%', left: 0, zIndex: 200,
              background: 'rgba(6,13,26,0.97)', border: '1px solid rgba(99,140,255,0.18)',
              borderRadius: 10, minWidth: 280, boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
              backdropFilter: 'blur(12px)', overflow: 'hidden',
            }}>
              {versions.map(v => {
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
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12 }}>

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
            {modelStatus?.last_retrained_at && (() => {
              const diffMs = nowMs - new Date(modelStatus.last_retrained_at!).getTime()
              const diffH  = Math.floor(diffMs / 3_600_000)
              const diffD  = Math.floor(diffH / 24)
              const label  = diffD >= 1 ? `${diffD}d ago` : diffH >= 1 ? `${diffH}h ago` : 'just now'
              return <span style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.35)' }}>{label}</span>
            })()}
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

          <div style={{ marginTop: 16, borderTop: '1px solid rgba(255,255,255,0.07)', paddingTop: 12 }}>
            <button onClick={() => scrollTo(historyRef)} style={{ fontSize: '0.72rem', color: COLORS.primary, background: 'none', border: 'none', cursor: 'pointer', padding: 0, display: 'inline-flex', alignItems: 'center', gap: 4 }}>
              View Run History →
            </button>
          </div>
        </GlassCard></div>

        {/* RIGHT: Performance Trend */}
        <GlassCard hover={false} style={{ padding: '20px 22px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 8 }}>Performance Trend</div>

          {/* Legend */}
          <div style={{ display: 'flex', gap: 18, marginBottom: 14 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <svg width={20} height={10}><line x1={0} y1={5} x2={20} y2={5} stroke={COLORS.primary} strokeWidth={2} /></svg>
              <span style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.55)' }}>AUC</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <svg width={20} height={10}><line x1={0} y1={5} x2={20} y2={5} stroke="rgba(255,255,255,0.50)" strokeWidth={1.5} strokeDasharray="4 2" /></svg>
              <span style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.55)' }}>Target (0.85)</span>
            </div>
          </div>

          {aucTrend.length === 0 && (
            <div style={{ height: 180, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'rgba(255,255,255,0.28)', fontSize: '0.78rem' }}>
              No model versions trained yet — run <code style={{ fontSize: '0.72rem' }}>make train</code> first
            </div>
          )}
          {aucTrend.length > 0 && <ResponsiveContainer width="100%" height={180}>
            <LineChart data={aucTrend} margin={{ top: 6, right: 12, bottom: 0, left: -10 }}>
              <XAxis
                dataKey="date"
                tick={{ fontSize: 10, fill: 'rgba(255,255,255,0.35)' }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                domain={[aucMin, aucMax]}
                tick={{ fontSize: 10, fill: 'rgba(255,255,255,0.35)' }}
                tickLine={false}
                axisLine={false}
                tickFormatter={v => v.toFixed(2)}
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                formatter={(v) => [Number(v).toFixed(4), 'AUC']}
              />
              <ReferenceLine
                y={0.85}
                stroke="rgba(255,255,255,0.45)"
                strokeDasharray="6 3"
                strokeWidth={1.5}
              />
              <Line
                type="monotone"
                dataKey="auc"
                stroke={COLORS.primary}
                strokeWidth={2}
                dot={{ r: 3, fill: COLORS.primary, strokeWidth: 0 }}
                activeDot={{ r: 5, fill: COLORS.primary }}
              />
            </LineChart>
          </ResponsiveContainer>}

          {/* Annotation */}
          <div style={{ marginTop: 8, fontSize: '0.65rem', color: 'rgba(255,255,255,0.42)', textAlign: 'right' }}>
            Latest · AUC: <span style={{ color: COLORS.primary, fontWeight: 500 }}>{auc != null ? auc.toFixed(4) : '—'}</span>
          </div>

          <div style={{ marginTop: 12, borderTop: '1px solid rgba(255,255,255,0.07)', paddingTop: 10 }}>
            <button onClick={() => scrollTo(historyRef)} style={{ fontSize: '0.72rem', color: COLORS.primary, background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
              View Full Metrics →
            </button>
          </div>
        </GlassCard>
      </div>

      {/* ── Row 3: Run History ───────────────────────────────────────────── */}
      <div ref={historyRef}><GlassCard hover={false} style={{ padding: '20px 22px', overflow: 'hidden' }}>
        {/* Section header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 18, flexWrap: 'wrap', gap: 10 }}>
          <div>
            <div style={{ ...SECTION_LABEL, marginBottom: 4 }}>Run History</div>
            <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)' }}>Recent training runs and system events</div>
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
