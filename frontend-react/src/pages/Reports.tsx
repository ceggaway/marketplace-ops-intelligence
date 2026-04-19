/**
 * Reports — insight-driven summaries for ops managers.
 *
 * Three tabs:
 *   Zone Performance  — chronic offenders, most improved, deteriorating (7 / 14 / 30 d)
 *   Intervention Outcomes — recovery rate, by-action breakdown, recent resolved
 *   Model Impact      — PSI → plain-language business impact + version lineage
 */

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  TrendingUp, TrendingDown, Minus, AlertTriangle, CheckCircle2,
  BarChart2, Cpu, Activity, Download, RefreshCw,
} from 'lucide-react'
import { api } from '../lib/api'
import type {
  ZonePerformanceReport, ZonePerformanceEntry,
  OutcomeReport,
  ModelImpactReport,
} from '../lib/api'
import { COLORS } from '../lib/utils'
import Spinner from '../components/Spinner'

// ── Shared tokens ─────────────────────────────────────────────────────────────

const GLASS: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: '1px solid rgba(99,140,255,0.12)',
  borderRadius: 14,
}

const CARD: React.CSSProperties = {
  ...GLASS,
  padding: '18px 20px',
}

const TABS = ['Zone Performance', 'Intervention Outcomes', 'Model Impact'] as const
type TabId = typeof TABS[number]

function labelText(value: unknown, fallback = 'unknown') {
  if (value === null || value === undefined) return fallback
  const text = String(value).trim()
  return text || fallback
}

// ── Risk badge ────────────────────────────────────────────────────────────────

function TrendBadge({ trend }: { trend: string }) {
  if (trend === 'improving') return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, fontSize: '0.68rem', fontWeight: 600, color: COLORS.low, background: `${COLORS.low}18`, border: `1px solid ${COLORS.low}30`, borderRadius: 20, padding: '2px 8px' }}>
      <TrendingUp size={10} /> Improving
    </span>
  )
  if (trend === 'deteriorating') return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, fontSize: '0.68rem', fontWeight: 600, color: COLORS.high, background: `${COLORS.high}18`, border: `1px solid ${COLORS.high}30`, borderRadius: 20, padding: '2px 8px' }}>
      <TrendingDown size={10} /> Deteriorating
    </span>
  )
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, fontSize: '0.68rem', fontWeight: 600, color: 'rgba(255,255,255,0.45)', background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.12)', borderRadius: 20, padding: '2px 8px' }}>
      <Minus size={10} /> Stable
    </span>
  )
}

function ScoreBar({ value, color }: { value: number; color: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <div style={{ flex: 1, height: 5, borderRadius: 3, background: 'rgba(255,255,255,0.08)' }}>
        <div style={{ height: '100%', width: `${Math.min(100, value * 100)}%`, borderRadius: 3, background: color, transition: 'width 0.4s ease' }} />
      </div>
      <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.55)', width: 34, textAlign: 'right' }}>
        {(value * 100).toFixed(0)}%
      </span>
    </div>
  )
}

// ── Zone Performance Tab ──────────────────────────────────────────────────────

function ZoneCard({ entry, rank }: { entry: ZonePerformanceEntry; rank?: number }) {
  return (
    <div style={{ ...GLASS, padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 8 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            {rank !== undefined && (
              <span style={{ fontSize: '0.62rem', fontWeight: 700, color: 'rgba(255,255,255,0.32)', width: 18 }}>#{rank}</span>
            )}
            <span style={{ fontSize: '0.84rem', fontWeight: 600, color: 'rgba(255,255,255,0.88)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{entry.zone_name}</span>
          </div>
          <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.35)', marginTop: 2, paddingLeft: rank !== undefined ? 24 : 0 }}>{entry.region} · {entry.observations} obs</div>
        </div>
        <TrendBadge trend={entry.trend} />
      </div>

      {/* Score bars */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.38)', width: 42, flexShrink: 0 }}>High</span>
          <ScoreBar value={entry.pct_time_high} color={COLORS.high} />
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.38)', width: 42, flexShrink: 0 }}>Medium</span>
          <ScoreBar value={entry.pct_time_medium} color={COLORS.medium} />
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.38)', width: 42, flexShrink: 0 }}>Low</span>
          <ScoreBar value={entry.pct_time_low} color={COLORS.low} />
        </div>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', paddingTop: 4, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
        <div>
          <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.32)' }}>Mean score</div>
          <div style={{ fontSize: '0.80rem', fontWeight: 600, color: 'rgba(255,255,255,0.78)' }}>{(entry.mean_score * 100).toFixed(1)}%</div>
        </div>
        {entry.trend !== 'stable' && (
          <div style={{ textAlign: 'right' }}>
            <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.32)' }}>Δ recent vs prior</div>
            <div style={{ fontSize: '0.80rem', fontWeight: 600, color: entry.trend_delta > 0 ? COLORS.high : COLORS.low }}>
              {entry.trend_delta > 0 ? '+' : ''}{(entry.trend_delta * 100).toFixed(1)}pp
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function ZonePerformanceTab() {
  const [days, setDays] = useState(7)

  const { data, isLoading, isError, refetch, isFetching } = useQuery<ZonePerformanceReport>({
    queryKey: ['reportZonePerformance', days],
    queryFn: () => api.reportZonePerformance(days),
    staleTime: 120_000,
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Controls */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <div style={{ fontSize: '0.78rem', fontWeight: 600, color: 'rgba(255,255,255,0.78)' }}>Zone performance over time</div>
          <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.38)', marginTop: 2 }}>Which zones are chronic offenders, improving, or trending worse?</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {[7, 14].map(d => (
            <button key={d} onClick={() => setDays(d)} style={{
              padding: '5px 14px', borderRadius: 8, fontSize: '0.72rem', fontWeight: 500, cursor: 'pointer',
              background: days === d ? 'rgba(79,142,247,0.18)' : 'rgba(255,255,255,0.05)',
              border: `1px solid ${days === d ? 'rgba(79,142,247,0.40)' : 'rgba(255,255,255,0.10)'}`,
              color: days === d ? COLORS.primary : 'rgba(255,255,255,0.50)',
              fontFamily: 'Inter, sans-serif', transition: 'all 0.15s',
            }}>{d}d</button>
          ))}
          <button onClick={() => refetch()} style={{
            padding: '5px 10px', borderRadius: 8, fontSize: '0.72rem', cursor: 'pointer',
            background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.10)',
            color: 'rgba(255,255,255,0.50)', fontFamily: 'Inter, sans-serif', display: 'flex', alignItems: 'center', gap: 5,
          }}>
            <RefreshCw size={11} className={isFetching ? 'spin' : ''} /> Refresh
          </button>
        </div>
      </div>

      {isLoading && <div style={{ display: 'flex', justifyContent: 'center', padding: 60 }}><Spinner size={30} /></div>}
      {isError && (
        <div style={{ ...CARD, color: COLORS.high, textAlign: 'center', padding: 32 }}>
          <AlertTriangle size={20} style={{ marginBottom: 8 }} />
          <div style={{ fontSize: '0.80rem' }}>Failed to load zone performance data.</div>
        </div>
      )}

      {data && (
        <>
          {data.note && (
            <div style={{ ...CARD, padding: '12px 16px', borderColor: 'rgba(245,158,11,0.25)', background: 'rgba(245,158,11,0.06)', color: 'rgba(245,158,11,0.88)', fontSize: '0.76rem' }}>
              {data.note}
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 20 }}>
            {/* Chronic high-risk */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', background: COLORS.high, boxShadow: `0 0 6px ${COLORS.high}` }} />
                <span style={{ fontSize: '0.72rem', fontWeight: 700, color: 'rgba(255,255,255,0.70)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Chronic High-Risk</span>
              </div>
              {data.chronic_high_risk.length === 0 ? (
                <div style={{ ...GLASS, padding: '28px 16px', textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem' }}>No chronic zones in this window</div>
              ) : data.chronic_high_risk.map((e, i) => <ZoneCard key={e.zone_id} entry={e} rank={i + 1} />)}
            </div>

            {/* Most improved */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', background: COLORS.low, boxShadow: `0 0 6px ${COLORS.low}` }} />
                <span style={{ fontSize: '0.72rem', fontWeight: 700, color: 'rgba(255,255,255,0.70)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Most Improved</span>
              </div>
              {data.most_improved.length === 0 ? (
                <div style={{ ...GLASS, padding: '28px 16px', textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem' }}>No improving zones in this window</div>
              ) : data.most_improved.map((e, i) => <ZoneCard key={e.zone_id} entry={e} rank={i + 1} />)}
            </div>

            {/* Deteriorating */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', background: COLORS.medium, boxShadow: `0 0 6px ${COLORS.medium}` }} />
                <span style={{ fontSize: '0.72rem', fontWeight: 700, color: 'rgba(255,255,255,0.70)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Deteriorating</span>
              </div>
              {data.deteriorating.length === 0 ? (
                <div style={{ ...GLASS, padding: '28px 16px', textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem' }}>No deteriorating zones in this window</div>
              ) : data.deteriorating.map((e, i) => <ZoneCard key={e.zone_id} entry={e} rank={i + 1} />)}
            </div>
          </div>
        </>
      )}
    </div>
  )
}

// ── Intervention Outcomes Tab ─────────────────────────────────────────────────

function pct(n: number) { return `${(n * 100).toFixed(0)}%` }

function RateBar({ value, color, label }: { value: number; color: string; label: string }) {
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
        <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.50)' }}>{label}</span>
        <span style={{ fontSize: '0.68rem', fontWeight: 600, color }}>{pct(value)}</span>
      </div>
      <div style={{ height: 6, borderRadius: 3, background: 'rgba(255,255,255,0.08)' }}>
        <div style={{ height: '100%', width: pct(value), borderRadius: 3, background: color, transition: 'width 0.4s ease' }} />
      </div>
    </div>
  )
}

function OutcomesTab() {
  const { data, isLoading, isError, refetch, isFetching } = useQuery<OutcomeReport>({
    queryKey: ['reportOutcomes'],
    queryFn: api.reportOutcomes,
    staleTime: 120_000,
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div>
          <div style={{ fontSize: '0.78rem', fontWeight: 600, color: 'rgba(255,255,255,0.78)' }}>Observed recommendation outcomes</div>
          <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.38)', marginTop: 2 }}>How zone risk changed after recommendations were issued; read together with follow-through status.</div>
        </div>
        <button onClick={() => refetch()} style={{
          padding: '5px 10px', borderRadius: 8, fontSize: '0.72rem', cursor: 'pointer',
          background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.10)',
          color: 'rgba(255,255,255,0.50)', fontFamily: 'Inter, sans-serif', display: 'flex', alignItems: 'center', gap: 5,
        }}>
          <RefreshCw size={11} className={isFetching ? 'spin' : ''} /> Refresh
        </button>
      </div>

      {isLoading && <div style={{ display: 'flex', justifyContent: 'center', padding: 60 }}><Spinner size={30} /></div>}
      {isError && (
        <div style={{ ...CARD, color: COLORS.high, textAlign: 'center', padding: 32 }}>
          <AlertTriangle size={20} style={{ marginBottom: 8 }} />
          <div style={{ fontSize: '0.80rem' }}>Failed to load outcome data.</div>
        </div>
      )}

      {data && (
        <>
          {data.sample_size_note && (
            <div style={{ ...CARD, padding: '12px 16px', borderColor: 'rgba(245,158,11,0.25)', background: 'rgba(245,158,11,0.06)', color: 'rgba(245,158,11,0.88)', fontSize: '0.76rem' }}>
              {data.sample_size_note}
            </div>
          )}

          {/* Summary KPI row */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
            {[
              { label: 'Total interventions', value: data.total_logged, note: 'logged', color: COLORS.primary },
              { label: 'Resolved', value: data.total_resolved, note: 'assessed at 30 min', color: 'rgba(255,255,255,0.65)' },
              { label: 'Recovery rate', value: pct(data.recovery_rate), note: 'resolved outcomes below high-risk threshold', color: COLORS.low },
              { label: 'Improvement rate', value: pct(data.improvement_rate), note: 'resolved outcomes improved or recovered', color: COLORS.low },
            ].map(k => (
              <div key={k.label} style={{ ...CARD }}>
                <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.40)', marginBottom: 6 }}>{k.label}</div>
                <div style={{ fontSize: '1.40rem', fontWeight: 700, color: k.color, lineHeight: 1 }}>{k.value}</div>
                <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.30)', marginTop: 4 }}>{k.note}</div>
              </div>
            ))}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
            {/* By action type */}
            <div style={{ ...CARD, display: 'flex', flexDirection: 'column', gap: 14 }}>
              <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'rgba(255,255,255,0.70)' }}>Observed outcomes by action type</div>
              {Object.keys(data.by_action_type).length === 0 ? (
                <div style={{ textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem', padding: '24px 0' }}>No resolved outcomes yet.</div>
              ) : Object.entries(data.by_action_type).map(([action, stats]) => (
                <div key={action} style={{ borderTop: '1px solid rgba(255,255,255,0.06)', paddingTop: 12 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                    <span style={{ fontSize: '0.76rem', fontWeight: 500, color: 'rgba(255,255,255,0.72)', textTransform: 'capitalize' }}>{labelText(action).replace(/_/g, ' ')}</span>
                    <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.38)' }}>{stats.total} interventions</span>
                  </div>
                  <RateBar value={stats.recovery_rate as number} color={COLORS.low} label="Recovery rate" />
                  <div style={{ marginTop: 6 }}>
                    <RateBar value={stats.improvement_rate as number} color={COLORS.primary} label="Improvement rate" />
                  </div>
                </div>
              ))}
            </div>

            {/* Top zones by intervention count */}
            <div style={{ ...CARD, display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'rgba(255,255,255,0.70)' }}>Most intervened zones</div>
              {data.by_zone.length === 0 ? (
                <div style={{ textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem', padding: '24px 0' }}>No data yet.</div>
              ) : data.by_zone.map((z, i) => (
                <div key={z.zone_id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderBottom: i < data.by_zone.length - 1 ? '1px solid rgba(255,255,255,0.05)' : 'none' }}>
                  <span style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.32)', width: 20, textAlign: 'right' }}>#{i + 1}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '0.78rem', color: 'rgba(255,255,255,0.80)', fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{z.zone_name}</div>
                    <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.35)' }}>{z.interventions} interventions</div>
                  </div>
                  <div style={{ textAlign: 'right' }}>
                    <div style={{ fontSize: '0.76rem', fontWeight: 600, color: z.recovery_rate >= 0.5 ? COLORS.low : z.recovery_rate >= 0.3 ? COLORS.medium : COLORS.high }}>
                      {pct(z.recovery_rate)}
                    </div>
                    <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.30)' }}>recovery</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
            <div style={{ ...CARD, display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'rgba(255,255,255,0.70)' }}>Follow-through split</div>
              {Object.entries(data.by_follow_status).map(([status, stats]) => (
                <div key={status} style={{ borderTop: '1px solid rgba(255,255,255,0.06)', paddingTop: 10 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                    <span style={{ fontSize: '0.74rem', color: 'rgba(255,255,255,0.72)', textTransform: 'capitalize' }}>{labelText(status).replace(/_/g, ' ')}</span>
                    <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.38)' }}>{stats.total ?? 0} logged</span>
                  </div>
                  <RateBar value={(stats.improvement_rate as number) ?? 0} color={COLORS.primary} label="Improve / recover" />
                  <div style={{ marginTop: 6 }}>
                    <RateBar value={(stats.recovery_rate as number) ?? 0} color={COLORS.low} label="Recovery" />
                  </div>
                </div>
              ))}
            </div>

            <div style={{ ...CARD, display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'rgba(255,255,255,0.70)' }}>Strongest context buckets</div>
              {data.top_contexts.length === 0 ? (
                <div style={{ textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem', padding: '24px 0' }}>No resolved context buckets yet.</div>
              ) : data.top_contexts.map((ctx, i) => (
                <div key={`${labelText(ctx.action_type)}-${labelText(ctx.root_cause)}-${i}`} style={{ padding: '8px 0', borderBottom: i < data.top_contexts.length - 1 ? '1px solid rgba(255,255,255,0.05)' : 'none' }}>
                  <div style={{ fontSize: '0.74rem', color: 'rgba(255,255,255,0.82)', fontWeight: 500, textTransform: 'capitalize' }}>
                    {labelText(ctx.action_type).replace(/_/g, ' ')} · {labelText(ctx.root_cause).replace(/_/g, ' ')}
                  </div>
                  <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.36)', marginTop: 3 }}>
                    {labelText(ctx.risk_level)} risk · {labelText(ctx.intervention_window).replace(/_/g, ' ')} · {ctx.resolved} resolved · {labelText(ctx.confidence_band, 'low')} confidence
                  </div>
                  <div style={{ fontSize: '0.66rem', color: COLORS.primary, marginTop: 5 }}>
                    Improve / recover {pct(ctx.improvement_rate)} · recovery {pct(ctx.recovery_rate)}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Recent resolved outcomes table */}
          {data.recent_outcomes.length > 0 && (
            <div style={{ ...CARD }}>
              <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'rgba(255,255,255,0.70)', marginBottom: 14 }}>Recent resolved outcomes</div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.73rem' }}>
                  <thead>
                    <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.08)' }}>
                      {['Zone', 'Action', 'Priority', 'Followed', 'Score before', 'Score after', 'Outcome', 'Time'].map(h => (
                        <th key={h} style={{ padding: '6px 10px', textAlign: 'left', fontWeight: 600, color: 'rgba(255,255,255,0.38)', fontSize: '0.65rem', textTransform: 'uppercase', letterSpacing: '0.06em', whiteSpace: 'nowrap' }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.recent_outcomes.map((o, i) => {
                      const ocColor = o.outcome === 'recovered' ? COLORS.low : o.outcome === 'improved' ? COLORS.primary : o.outcome === 'worsened' ? COLORS.high : 'rgba(255,255,255,0.45)'
                      return (
                        <tr key={i} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                          <td style={{ padding: '8px 10px', color: 'rgba(255,255,255,0.80)', fontWeight: 500 }}>{labelText(o.zone_name, `Zone ${o.zone_id}`)}</td>
                          <td style={{ padding: '8px 10px', color: 'rgba(255,255,255,0.55)', textTransform: 'capitalize' }}>{labelText(o.action_type).replace(/_/g, ' ')}</td>
                          <td style={{ padding: '8px 10px' }}>
                            <span style={{ fontSize: '0.62rem', fontWeight: 600, textTransform: 'uppercase', color: o.priority === 'critical' ? COLORS.high : o.priority === 'high' ? COLORS.medium : 'rgba(255,255,255,0.45)' }}>{labelText(o.priority)}</span>
                          </td>
                          <td style={{ padding: '8px 10px', color: o.followed_status === 'followed' ? COLORS.low : o.followed_status === 'not_followed' ? COLORS.medium : 'rgba(255,255,255,0.32)', textTransform: 'capitalize' }}>
                            {labelText(o.followed_status).replace(/_/g, ' ')}
                          </td>
                          <td style={{ padding: '8px 10px', color: 'rgba(255,255,255,0.55)', textAlign: 'right' }}>{(o.score_at_time * 100).toFixed(1)}%</td>
                          <td style={{ padding: '8px 10px', textAlign: 'right', color: o.score_after !== null && o.score_after !== undefined && o.score_after < o.score_at_time ? COLORS.low : COLORS.high }}>
                            {o.score_after !== null && o.score_after !== undefined ? `${(o.score_after * 100).toFixed(1)}%` : '—'}
                          </td>
                          <td style={{ padding: '8px 10px' }}>
                            <span style={{ fontSize: '0.65rem', fontWeight: 600, color: ocColor, textTransform: 'capitalize' }}>{labelText(o.outcome)}</span>
                          </td>
                          <td style={{ padding: '8px 10px', color: 'rgba(255,255,255,0.32)', whiteSpace: 'nowrap' }}>
                            {o.logged_at ? new Date(o.logged_at).toLocaleString('en-SG', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

// ── Model Impact Tab ──────────────────────────────────────────────────────────

function PsiGauge({ psi }: { psi: number }) {
  const level = psi >= 0.25 ? 'significant' : psi >= 0.10 ? 'moderate' : 'stable'
  const color = psi >= 0.25 ? COLORS.high : psi >= 0.10 ? COLORS.medium : COLORS.low
  const pct_val = Math.min(100, (psi / 0.35) * 100)
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
        <div>
          <div style={{ fontSize: '2.20rem', fontWeight: 700, color, lineHeight: 1 }}>{psi.toFixed(3)}</div>
          <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.40)', marginTop: 3 }}>Population Stability Index</div>
        </div>
        <span style={{ fontSize: '0.72rem', fontWeight: 700, color, background: `${color}18`, border: `1px solid ${color}30`, borderRadius: 20, padding: '3px 12px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{level}</span>
      </div>
      {/* Track */}
      <div style={{ height: 8, borderRadius: 4, background: 'rgba(255,255,255,0.08)', position: 'relative', overflow: 'hidden' }}>
        <div style={{ position: 'absolute', left: 0, top: 0, height: '100%', width: `${pct_val}%`, borderRadius: 4, background: `linear-gradient(90deg, ${COLORS.low}, ${color})`, transition: 'width 0.5s ease' }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.58rem', color: 'rgba(255,255,255,0.28)' }}>
        <span>0 — Stable</span><span>0.10 — Moderate</span><span>0.25+ — Significant</span>
      </div>
    </div>
  )
}

function MetricRow({ label, value, note }: { label: string; value: string | null; note?: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 0', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
      <div>
        <div style={{ fontSize: '0.76rem', color: 'rgba(255,255,255,0.70)' }}>{label}</div>
        {note && <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.32)', marginTop: 2 }}>{note}</div>}
      </div>
      <span style={{ fontSize: '0.86rem', fontWeight: 600, color: value ? 'rgba(255,255,255,0.88)' : 'rgba(255,255,255,0.30)' }}>
        {value ?? '—'}
      </span>
    </div>
  )
}

function ModelImpactTab() {
  const { data, isLoading, isError, refetch, isFetching } = useQuery<ModelImpactReport>({
    queryKey: ['reportModelImpact'],
    queryFn: api.reportModelImpact,
    staleTime: 120_000,
  })

  const handleExportPredictions = () => {
    window.open('/api/v1/reports/export/predictions', '_blank')
  }
  const handleExportHistory = () => {
    window.open('/api/v1/reports/export/history', '_blank')
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div>
          <div style={{ fontSize: '0.78rem', fontWeight: 600, color: 'rgba(255,255,255,0.78)' }}>Model health in plain language</div>
          <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.38)', marginTop: 2 }}>What do drift and precision actually mean for operations?</div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={handleExportPredictions} style={{
            padding: '5px 12px', borderRadius: 8, fontSize: '0.72rem', cursor: 'pointer',
            background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.12)',
            color: 'rgba(255,255,255,0.55)', fontFamily: 'Inter, sans-serif', display: 'flex', alignItems: 'center', gap: 5,
          }}>
            <Download size={11} /> Predictions CSV
          </button>
          <button onClick={handleExportHistory} style={{
            padding: '5px 12px', borderRadius: 8, fontSize: '0.72rem', cursor: 'pointer',
            background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.12)',
            color: 'rgba(255,255,255,0.55)', fontFamily: 'Inter, sans-serif', display: 'flex', alignItems: 'center', gap: 5,
          }}>
            <Download size={11} /> History CSV
          </button>
          <button onClick={() => refetch()} style={{
            padding: '5px 10px', borderRadius: 8, fontSize: '0.72rem', cursor: 'pointer',
            background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.10)',
            color: 'rgba(255,255,255,0.50)', fontFamily: 'Inter, sans-serif', display: 'flex', alignItems: 'center', gap: 5,
          }}>
            <RefreshCw size={11} className={isFetching ? 'spin' : ''} /> Refresh
          </button>
        </div>
      </div>

      {isLoading && <div style={{ display: 'flex', justifyContent: 'center', padding: 60 }}><Spinner size={30} /></div>}
      {isError && (
        <div style={{ ...CARD, color: COLORS.high, textAlign: 'center', padding: 32 }}>
          <AlertTriangle size={20} style={{ marginBottom: 8 }} />
          <div style={{ fontSize: '0.80rem' }}>Failed to load model impact data.</div>
        </div>
      )}

      {data && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
          {/* Left column */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* PSI gauge */}
            <div style={{ ...CARD }}>
              <div style={{ fontSize: '0.68rem', fontWeight: 600, color: 'rgba(255,255,255,0.42)', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 14 }}>Data Drift</div>
              <PsiGauge psi={data.psi} />
            </div>

            {/* Business impact text */}
            <div style={{ ...CARD, padding: '16px 20px' }}>
              <div style={{ fontSize: '0.68rem', fontWeight: 600, color: 'rgba(255,255,255,0.42)', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 10 }}>Business impact</div>
              <div style={{ fontSize: '0.80rem', color: 'rgba(255,255,255,0.72)', lineHeight: 1.6 }}>{data.psi_business_impact}</div>
            </div>

            {/* False positive note */}
            <div style={{ ...CARD, padding: '16px 20px' }}>
              <div style={{ fontSize: '0.68rem', fontWeight: 600, color: 'rgba(255,255,255,0.42)', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 10 }}>Alert accuracy</div>
              <div style={{ fontSize: '0.80rem', color: 'rgba(255,255,255,0.72)', lineHeight: 1.6 }}>{data.estimated_false_positive_note}</div>
            </div>

            {/* Recommendation */}
            <div style={{ ...CARD, padding: '16px 20px', borderColor: data.psi >= 0.25 ? `${COLORS.high}30` : data.psi >= 0.10 ? `${COLORS.medium}30` : `${COLORS.low}30`, background: data.psi >= 0.25 ? `${COLORS.high}08` : data.psi >= 0.10 ? `${COLORS.medium}08` : `${COLORS.low}08` }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10 }}>
                {data.psi >= 0.10 ? <AlertTriangle size={13} color={data.psi >= 0.25 ? COLORS.high : COLORS.medium} /> : <CheckCircle2 size={13} color={COLORS.low} />}
                <span style={{ fontSize: '0.68rem', fontWeight: 700, color: data.psi >= 0.25 ? COLORS.high : data.psi >= 0.10 ? COLORS.medium : COLORS.low, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Recommendation</span>
              </div>
              <div style={{ fontSize: '0.80rem', color: 'rgba(255,255,255,0.80)', lineHeight: 1.6 }}>{data.recommendation}</div>
            </div>
          </div>

          {/* Right column */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* Model metrics */}
            <div style={{ ...CARD }}>
              <div style={{ fontSize: '0.68rem', fontWeight: 600, color: 'rgba(255,255,255,0.42)', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 4 }}>Active model</div>
              <div style={{ fontSize: '0.80rem', color: 'rgba(255,255,255,0.55)', marginBottom: 14 }}>
                {data.active_version ?? 'None'}</div>
              <MetricRow label="F1 score" value={data.f1 !== null && data.f1 !== undefined ? data.f1.toFixed(4) : null} note="Harmonic mean of precision + recall" />
              <MetricRow label="Precision" value={data.precision !== null && data.precision !== undefined ? `${(data.precision * 100).toFixed(1)}%` : null} note="Of alerts fired, % that are genuine shortages" />
              <MetricRow label="Recall" value={data.recall !== null && data.recall !== undefined ? `${(data.recall * 100).toFixed(1)}%` : null} note="Of genuine shortages, % caught by the model" />
            </div>

            {/* Version lineage */}
            <div style={{ ...CARD }}>
              <div style={{ fontSize: '0.68rem', fontWeight: 600, color: 'rgba(255,255,255,0.42)', textTransform: 'uppercase', letterSpacing: '0.10em', marginBottom: 12 }}>Version lineage</div>
              {data.version_lineage.length === 0 ? (
                <div style={{ textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.76rem', padding: '24px 0' }}>No model versions found.</div>
              ) : data.version_lineage.map((v, i) => {
                const isActive = v.status === 'active'
                return (
                  <div key={v.version} style={{ display: 'flex', alignItems: 'flex-start', gap: 10, padding: '10px 0', borderBottom: i < data.version_lineage.length - 1 ? '1px solid rgba(255,255,255,0.05)' : 'none' }}>
                    <div style={{ marginTop: 3, width: 8, height: 8, borderRadius: '50%', flexShrink: 0, background: isActive ? COLORS.low : 'rgba(255,255,255,0.20)', boxShadow: isActive ? `0 0 6px ${COLORS.low}` : 'none' }} />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                        <span style={{ fontSize: '0.76rem', fontWeight: 600, color: isActive ? 'rgba(255,255,255,0.88)' : 'rgba(255,255,255,0.55)' }}>{v.version}</span>
                        <span style={{ fontSize: '0.58rem', fontWeight: 700, color: isActive ? COLORS.low : 'rgba(255,255,255,0.32)', background: isActive ? `${COLORS.low}18` : 'rgba(255,255,255,0.06)', border: `1px solid ${isActive ? COLORS.low + '30' : 'rgba(255,255,255,0.10)'}`, borderRadius: 20, padding: '1px 7px', textTransform: 'uppercase' }}>{v.status}</span>
                      </div>
                      <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.32)', marginTop: 2 }}>
                        {v.trained_at ? new Date(v.trained_at).toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' }) : '—'}
                        {v.f1 !== null && v.f1 !== undefined ? ` · F1 ${v.f1.toFixed(4)}` : ''}
                        {v.roc_auc !== null && v.roc_auc !== undefined ? ` · AUC ${v.roc_auc.toFixed(4)}` : ''}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Page root ─────────────────────────────────────────────────────────────────

export default function Reports() {
  const [activeTab, setActiveTab] = useState<TabId>('Zone Performance')

  const TAB_ICONS: Record<TabId, React.ReactNode> = {
    'Zone Performance':       <BarChart2 size={13} />,
    'Intervention Outcomes':  <Activity size={13} />,
    'Model Impact':           <Cpu size={13} />,
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 2, background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(99,140,255,0.10)', borderRadius: 12, padding: 4, width: 'fit-content' }}>
        {TABS.map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)} style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '7px 16px', borderRadius: 9, fontSize: '0.78rem', fontWeight: 500, cursor: 'pointer',
            background: activeTab === tab ? 'rgba(79,142,247,0.18)' : 'transparent',
            border: `1px solid ${activeTab === tab ? 'rgba(79,142,247,0.35)' : 'transparent'}`,
            color: activeTab === tab ? COLORS.primary : 'rgba(255,255,255,0.45)',
            fontFamily: 'Inter, sans-serif', transition: 'all 0.15s',
          }}>
            {TAB_ICONS[tab]}
            {tab}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {activeTab === 'Zone Performance'      && <ZonePerformanceTab />}
      {activeTab === 'Intervention Outcomes' && <OutcomesTab />}
      {activeTab === 'Model Impact'          && <ModelImpactTab />}
    </div>
  )
}
