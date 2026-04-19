import { useState, useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  ArrowUpRight,
  Zap, CheckCircle, BarChart2,
  Car, TrendingDown, ShieldAlert, ListChecks, Activity as ActivityIcon,
} from 'lucide-react'
import {
  ComposedChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine,
} from 'recharts'
import { api } from '../lib/api'
import { formatNum, fmtTime, COLORS, SECTION_LABEL, TOOLTIP_STYLE, TOOLTIP_LABEL_STYLE } from '../lib/utils'
import GlassCard from '../components/GlassCard'
import Badge from '../components/Badge'
import Spinner from '../components/Spinner'
import ApiError from '../components/ApiError'
import EmptyState from '../components/EmptyState'
import SparkLine from '../components/SparkLine'

// ── helpers ────────────────────────────────────────────────────────────────

const psiColour = (v: number) =>
  v >= 0.25 ? COLORS.high : v >= 0.10 ? COLORS.medium : COLORS.low

const riskZoneColour = (n: number, total = 55) => {
  const pct = n / total
  return pct >= 0.3 ? COLORS.high : pct >= 0.1 ? COLORS.medium : COLORS.low
}

// ── sub-components ─────────────────────────────────────────────────────────

/** Tiny status dot */
function StatusDot({ color = COLORS.low }: { color?: string }) {
  return (
    <span style={{
      display: 'inline-block', width: 8, height: 8, borderRadius: '50%',
      background: color, boxShadow: `0 0 6px ${color}88`, flexShrink: 0,
    }} />
  )
}

// ── main component ─────────────────────────────────────────────────────────

const TIME_RANGES = ['Last 6 Hours', 'Last 24 Hours', 'Last 7 Days']

export default function Overview() {
  const navigate = useNavigate()
  const [timeRange, setTimeRange] = useState('Last 24 Hours')
  const [timeRangeOpen, setTimeRangeOpen] = useState(false)
  const timeRangeRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!timeRangeOpen) return
    const handler = (e: MouseEvent) => {
      if (timeRangeRef.current && !timeRangeRef.current.contains(e.target as Node)) {
        setTimeRangeOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [timeRangeOpen])

  const { data, isLoading, isError } = useQuery({
    queryKey: ['overview'],
    queryFn: api.overview,
    staleTime: 30000,
    refetchInterval: 60000,
  })

  const { data: zones } = useQuery({
    queryKey: ['zones'],
    queryFn: () => api.zones(),
    staleTime: 30000,
  })

  const { data: latestRun } = useQuery({
    queryKey: ['latestRun'],
    queryFn: api.latestRun,
    staleTime: 30000,
  })

  const { data: modelStatus } = useQuery({
    queryKey: ['modelStatus'],
    queryFn: api.modelStatus,
    staleTime: 30000,
  })

  const { data: servicesHealth } = useQuery({
    queryKey: ['servicesHealth'],
    queryFn: api.servicesHealth,
    staleTime: 60000,
    refetchInterval: 120000,
  })

  if (isLoading) return <Spinner />
  if (isError)   return <ApiError message="The overview endpoint could not be loaded." />
  if (!data)     return <EmptyState title="Overview unavailable" message="No overview payload was returned by the backend." />

  const kpis = data.kpis

  /* ── Time range filter ───────────────────────────────────────────────── */
  // Cutoff is relative to the latest data point, not wall clock — avoids Date.now() in render
  // and is more semantically correct (shows last N hours of available data).
  function filterTrend(points: { timestamp: string; value: number }[]) {
    if (timeRange === 'Last 7 Days' || points.length === 0) return points
    const hoursBack = timeRange === 'Last 6 Hours' ? 6 : 24
    const latestTs  = Math.max(...points.map(p => new Date(p.timestamp).getTime()))
    const cutoff    = latestTs - hoursBack * 3_600_000
    const filtered  = points.filter(p => new Date(p.timestamp).getTime() >= cutoff)
    // Fall back to last N entries if data is older than the cutoff (e.g. infrequent scoring)
    if (filtered.length === 0) return points.slice(-hoursBack)
    return filtered
  }

  /* ── Derived values ──────────────────────────────────────────────────── */
  const highRisk   = kpis.high_risk_zone_count
  const medRisk    = kpis.medium_risk_zone_count
  const supply     = kpis.total_taxi_supply
  const depletion  = kpis.rapid_depletion_zones
  const actions    = kpis.critical_actions_count
  const psi        = kpis.model_psi
  const minsSince  = kpis.minutes_since_last_run
  const topZones = [...(zones ?? [])].sort((a, b) => b.delay_risk_score - a.delay_risk_score).slice(0, 6)

  // Trend data for sparklines and performance chart
  const filteredFulfilment = filterTrend(data.fulfilment_trend ?? [])  // high_risk_zones_now trend
  const filteredDelay      = filterTrend(data.delay_trend ?? [])       // rapid_depletion trend
  const filteredDemand     = filterTrend(data.demand_trend ?? [])      // supply_now trend

  const trendData = filteredFulfilment.map(p => ({
    time: fmtTime(p.timestamp),
    actual: Math.round(p.value),   // high_risk_zones_now — integer count
  }))

  /* ── Insight items ───────────────────────────────────────────────────── */
  const psiInsight = psi >= 0.25
    ? { label: 'Drift Alert', color: COLORS.high, desc: 'Model PSI exceeds 0.25 — predictions may be unreliable. Retrain recommended.' }
    : psi >= 0.10
    ? { label: 'Monitoring', color: COLORS.medium, desc: 'Feature distribution shifting vs reference. Review Model Health.' }
    : { label: 'Stable', color: COLORS.low, desc: 'Model predictions are stable. No action required.' }

  const insights = [
    highRisk > 0
      ? {
          icon: ShieldAlert,
          iconColor: COLORS.high,
          title: `${highRisk} zone${highRisk !== 1 ? 's' : ''} at high shortage risk right now`,
          desc: 'Zones where the model predicts >70% chance of supply drop in the next hour. Prioritize these first.',
          badge: { label: 'Immediate Action', color: COLORS.high },
        }
      : {
          icon: CheckCircle,
          iconColor: COLORS.low,
          title: 'No zones at high shortage risk',
          desc: `All 55 zones are currently low or medium risk. ${depletion > 0 ? `Watch the ${depletion} depleting zone${depletion !== 1 ? 's' : ''} for early signs of escalation.` : 'Supply is stable across all zones.'}`,
          badge: { label: 'Stable', color: COLORS.low },
        },
    {
      icon: TrendingDown,
      iconColor: depletion > 3 ? COLORS.high : depletion > 0 ? COLORS.medium : COLORS.low,
      title: depletion > 0
        ? `${depletion} zone${depletion !== 1 ? 's' : ''} losing supply at >30%/hr`
        : 'No zones with rapid supply depletion',
      desc: depletion > 0
        ? 'Rapid depletion is the earliest leading indicator of shortage. Act within 2 hours to pre-position drivers.'
        : 'Depletion rates are normal across all zones. No pre-positioning required.',
      badge: { label: depletion > 0 ? 'Time-Sensitive' : 'Normal', color: depletion > 3 ? COLORS.high : depletion > 0 ? COLORS.medium : COLORS.low },
    },
    {
      icon: ActivityIcon,
      iconColor: psiInsight.color,
      title: `Model PSI: ${psi.toFixed(3)} — ${psi >= 0.25 ? 'Drift detected' : psi >= 0.10 ? 'Warning threshold' : 'Stable'}`,
      desc: psiInsight.desc,
      badge: { label: psiInsight.label, color: psiInsight.color },
    },
    {
      icon: Zap,
      iconColor: COLORS.primary,
      title: `${actions} critical or high-priority action${actions !== 1 ? 's' : ''} pending`,
      desc: `${formatNum(supply)} taxis active across 55 zones (~${Math.round(supply / 55)} per zone avg). Review Action Center for driver reallocation recommendations.`,
      badge: { label: 'Action Available', color: COLORS.primary },
    },
  ]

  /* ── Top recommended alert text ─────────────────────────────────────── */
  const topAlert = data.alerts?.[0]
  const actionText = topAlert
    ? topAlert.message
    : highRisk > 0
      ? `${highRisk} zone${highRisk !== 1 ? 's' : ''} at high shortage risk — prioritize driver reallocation in the highest-risk areas immediately.`
      : medRisk > 0
        ? `${medRisk} zone${medRisk !== 1 ? 's' : ''} at medium risk — monitor for escalation and pre-position drivers near depleting zones.`
        : 'All zones at low risk — supply is stable across Singapore. Continue standard monitoring cadence.'

  /* ── render ──────────────────────────────────────────────────────────── */
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* ── ROW 1: KPI CARDS ───────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12 }}>

        {/* Card 1: High-Risk Zones */}
        {(() => {
          const color = riskZoneColour(highRisk)
          return (
            <GlassCard accentColor={color} hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                  <div style={{ width: 32, height: 32, borderRadius: 8, background: `${color}18`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                    <ShieldAlert size={15} color={color} strokeWidth={1.75} />
                  </div>
                  <span style={SECTION_LABEL as React.CSSProperties}>High-Risk Zones</span>
                </div>
                <SparkLine data={filteredFulfilment} color={color} width={70} height={32} />
              </div>
              <div style={{ fontSize: '1.85rem', fontWeight: 300, color, lineHeight: 1, marginBottom: 8 }}>
                {highRisk} <span style={{ fontSize: '1rem', color: 'rgba(255,255,255,0.42)', fontWeight: 300 }}>/ 55</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{
                  fontSize: '0.65rem', fontWeight: 600, color: COLORS.medium,
                  background: `${COLORS.medium}18`, border: `1px solid ${COLORS.medium}35`,
                  borderRadius: 20, padding: '1px 7px',
                }}>{medRisk} medium</span>
                <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.35)' }}>right now</span>
              </div>
            </GlassCard>
          )
        })()}

        {/* Card 2: Active Supply */}
        {(() => {
          const avgPerZone = supply / 55
          const color = avgPerZone >= 15 ? COLORS.low : avgPerZone >= 8 ? COLORS.medium : COLORS.high
          return (
            <GlassCard accentColor={color} hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                  <div style={{ width: 32, height: 32, borderRadius: 8, background: `${color}18`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                    <Car size={15} color={color} strokeWidth={1.75} />
                  </div>
                  <span style={SECTION_LABEL as React.CSSProperties}>Active Supply</span>
                </div>
                <SparkLine data={filteredDemand} color={color} width={70} height={32} />
              </div>
              <div style={{ fontSize: '1.85rem', fontWeight: 300, color: 'rgba(255,255,255,0.92)', lineHeight: 1, marginBottom: 8 }}>
                {formatNum(supply)}
              </div>
              <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.40)' }}>
                ~{Math.round(avgPerZone)} taxis / zone avg
              </div>
            </GlassCard>
          )
        })()}

        {/* Card 3: Rapid Depletion */}
        {(() => {
          const color = depletion > 10 ? COLORS.high : depletion > 3 ? COLORS.medium : COLORS.low
          return (
            <GlassCard accentColor={color} hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                  <div style={{ width: 32, height: 32, borderRadius: 8, background: `${color}18`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                    <TrendingDown size={15} color={color} strokeWidth={1.75} />
                  </div>
                  <span style={SECTION_LABEL as React.CSSProperties}>Rapid Depletion</span>
                </div>
                <SparkLine data={filteredDelay} color={color} width={70} height={32} />
              </div>
              <div style={{ fontSize: '1.85rem', fontWeight: 300, color, lineHeight: 1, marginBottom: 8 }}>
                {depletion} <span style={{ fontSize: '1rem', color: 'rgba(255,255,255,0.42)', fontWeight: 300 }}>zones</span>
              </div>
              <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.40)' }}>
                Losing &gt;30% supply / hr — act within 2h
              </div>
            </GlassCard>
          )
        })()}

        {/* Card 4: Actions Needed */}
        {(() => {
          const color = actions > 5 ? COLORS.high : actions > 0 ? COLORS.medium : COLORS.low
          return (
            <GlassCard accentColor={color} hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                  <div style={{ width: 32, height: 32, borderRadius: 8, background: `${color}18`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                    <ListChecks size={15} color={color} strokeWidth={1.75} />
                  </div>
                  <span style={SECTION_LABEL as React.CSSProperties}>Actions Needed</span>
                </div>
              </div>
              <div style={{ fontSize: '1.85rem', fontWeight: 300, color, lineHeight: 1, marginBottom: 8 }}>
                {actions}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                {actions > 0
                  ? <button className="btn-glass" onClick={() => navigate('/actions')}
                      style={{ fontSize: '0.65rem', padding: '3px 9px' }}>
                      View all <ArrowUpRight size={10} strokeWidth={2} />
                    </button>
                  : <span style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.35)' }}>No critical actions</span>
                }
              </div>
            </GlassCard>
          )
        })()}

        {/* Card 5: Model Status */}
        {(() => {
          const color = psiColour(psi)
          const freshLabel = minsSince === 0 ? 'just now'
            : minsSince < 60 ? `${minsSince}m ago`
            : `${Math.round(minsSince / 60)}h ago`
          const driftLabel = psi >= 0.25 ? 'DRIFT' : psi >= 0.10 ? 'WARNING' : 'STABLE'
          return (
            <GlassCard accentColor={color} hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                <div style={{ width: 32, height: 32, borderRadius: 8, background: `${color}18`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                  <ActivityIcon size={15} color={color} strokeWidth={1.75} />
                </div>
                <span style={SECTION_LABEL as React.CSSProperties}>Model Status</span>
              </div>
              <div style={{ fontSize: '1.85rem', fontWeight: 300, color, lineHeight: 1, marginBottom: 8 }}>
                {psi.toFixed(3)} <span style={{ fontSize: '0.9rem', color: 'rgba(255,255,255,0.42)', fontWeight: 300 }}>PSI</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{
                  fontSize: '0.62rem', fontWeight: 700,
                  background: `${color}18`, color,
                  border: `1px solid ${color}40`,
                  borderRadius: 20, padding: '1px 7px',
                }}>{driftLabel}</span>
                <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.35)' }}>scored {freshLabel}</span>
              </div>
            </GlassCard>
          )
        })()}

      </div>

      {/* ── ROW 2: RISK SNAPSHOT + INSIGHTS ───────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '60% 40%', gap: 12 }}>

        {/* LEFT: Top Risk Zones */}
        <GlassCard hover={false} style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{ padding: '14px 18px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid rgba(99,140,255,0.10)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <span style={SECTION_LABEL as React.CSSProperties}>Top Risk Zones</span>
              <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.35)', fontWeight: 300 }}>Standardized by low / medium / high thresholds</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                {[['Low', COLORS.low], ['Medium', COLORS.medium], ['High', COLORS.high]].map(([label, color]) => (
                  <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    <div style={{ width: 8, height: 8, borderRadius: '50%', background: color as string }} />
                    <span style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.32)', fontWeight: 400 }}>{label}</span>
                  </div>
                ))}
              </div>
              <button className="btn-glass" onClick={() => navigate('/zones')} style={{ fontSize: '0.72rem', padding: '5px 11px' }}>
                View Zones <ArrowUpRight size={11} strokeWidth={2} />
              </button>
            </div>
          </div>

          <div style={{ padding: '8px 0' }}>
            {topZones.length > 0 ? topZones.map((zone, index) => (
              <div
                key={zone.zone_id}
                style={{
                  display: 'grid',
                  gridTemplateColumns: '36px 1fr auto auto',
                  alignItems: 'center',
                  gap: 12,
                  padding: '14px 18px',
                  borderBottom: index < topZones.length - 1 ? '1px solid rgba(99,140,255,0.07)' : 'none',
                }}
              >
                <div style={{
                  width: 28, height: 28, borderRadius: '50%',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  background: 'rgba(255,255,255,0.06)', color: 'rgba(255,255,255,0.62)',
                  fontSize: '0.72rem', fontWeight: 700,
                }}>
                  {index + 1}
                </div>
                <div>
                  <div style={{ fontSize: '0.82rem', fontWeight: 600, color: 'rgba(255,255,255,0.90)', marginBottom: 3 }}>
                    {zone.zone_name}
                  </div>
                  <div style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.42)' }}>
                    {zone.region} · {zone.explanation_tag || 'No explanation tag'}
                  </div>
                </div>
                <div style={{
                  padding: '3px 9px',
                  borderRadius: 999,
                  background: `${zone.risk_level === 'high' ? COLORS.high : zone.risk_level === 'medium' ? COLORS.medium : COLORS.low}18`,
                  border: `1px solid ${zone.risk_level === 'high' ? COLORS.high : zone.risk_level === 'medium' ? COLORS.medium : COLORS.low}30`,
                  color: zone.risk_level === 'high' ? COLORS.high : zone.risk_level === 'medium' ? COLORS.medium : COLORS.low,
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                }}>
                  {zone.risk_level}
                </div>
                <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'rgba(255,255,255,0.86)', fontVariantNumeric: 'tabular-nums' }}>
                  {zone.delay_risk_score.toFixed(3)}
                </div>
              </div>
            )) : (
              <div style={{ minHeight: 220, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'rgba(255,255,255,0.42)', fontSize: '0.82rem' }}>
                No zone scores available
              </div>
            )}
          </div>

          <div style={{ padding: '8px 18px', borderTop: '1px solid rgba(99,140,255,0.08)', fontSize: '0.62rem', color: 'rgba(255,255,255,0.28)', fontWeight: 300 }}>
            The full geographic view lives in Zone Risk Monitor
          </div>
        </GlassCard>

        {/* RIGHT: Top Insights */}
        <GlassCard hover={false} style={{ padding: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
          {/* Card header */}
          <div style={{ padding: '14px 18px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid rgba(99,140,255,0.10)', flexShrink: 0 }}>
            <span style={SECTION_LABEL as React.CSSProperties}>Top Insights</span>
            <button
              className="btn-glass"
              onClick={() => navigate('/actions')}
              style={{ fontSize: '0.72rem', padding: '5px 11px' }}
            >
              View All Insights <ArrowUpRight size={11} strokeWidth={2} />
            </button>
          </div>

          {/* Insight list */}
          <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
            {insights.map((item, i) => (
              <div
                key={i}
                style={{
                  display: 'flex', alignItems: 'flex-start', gap: 12,
                  padding: '14px 18px',
                  borderBottom: i < insights.length - 1 ? '1px solid rgba(99,140,255,0.07)' : 'none',
                }}
              >
                {/* Icon box */}
                <div style={{
                  width: 38, height: 38, borderRadius: 10, flexShrink: 0,
                  background: `linear-gradient(135deg, ${item.iconColor}22, ${item.iconColor}0a)`,
                  border: `1px solid ${item.iconColor}30`,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  <item.icon size={16} color={item.iconColor} strokeWidth={1.75} />
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: '0.80rem', fontWeight: 500, color: 'rgba(255,255,255,0.88)', marginBottom: 3, lineHeight: 1.35 }}>
                    {item.title}
                  </div>
                  <div style={{ fontSize: '0.70rem', fontWeight: 300, color: 'rgba(255,255,255,0.45)', lineHeight: 1.45, marginBottom: 7 }}>
                    {item.desc}
                  </div>
                  <Badge label={item.badge.label} color={item.badge.color} />
                </div>
              </div>
            ))}
          </div>
        </GlassCard>
      </div>

      {/* ── ROW 3: TREND CHART + SYSTEM STATUS + MODEL RUN ────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '55% 25% 20%', gap: 12 }}>

        {/* LEFT: Performance Trend */}
        <GlassCard hover={false} style={{ padding: '16px 18px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
            <div>
              <div style={SECTION_LABEL as React.CSSProperties}>High-Risk Zone Count Trend</div>
              <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)', fontWeight: 300, marginTop: -6, marginBottom: 10 }}>
                Number of zones at high shortage risk per scoring run
              </div>
            </div>
            <div ref={timeRangeRef} style={{ position: 'relative' }}>
              <button
                className="btn-glass"
                onClick={() => setTimeRangeOpen(o => !o)}
                style={{ fontSize: '0.70rem', padding: '4px 10px' }}
              >
                {timeRange} ▾
              </button>
              {timeRangeOpen && (
                <div style={{
                  position: 'absolute', top: 'calc(100% + 6px)', right: 0, zIndex: 100,
                  background: 'rgba(14,22,42,0.97)', border: '1px solid rgba(99,140,255,0.18)',
                  borderRadius: 10, padding: '4px 0', minWidth: 150,
                  boxShadow: '0 8px 28px rgba(0,0,0,0.55)',
                }}>
                  {TIME_RANGES.map(r => (
                    <button
                      key={r}
                      onClick={() => { setTimeRange(r); setTimeRangeOpen(false) }}
                      style={{
                        display: 'block', width: '100%', textAlign: 'left',
                        padding: '8px 14px', background: r === timeRange ? 'rgba(79,142,247,0.14)' : 'transparent',
                        border: 'none', color: r === timeRange ? 'rgba(255,255,255,0.92)' : 'rgba(255,255,255,0.56)',
                        fontSize: '0.76rem', cursor: 'pointer', fontFamily: 'Inter, sans-serif',
                        transition: 'background 0.15s',
                      }}
                      onMouseEnter={e => { if (r !== timeRange) (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,0.05)' }}
                      onMouseLeave={e => { if (r !== timeRange) (e.currentTarget as HTMLElement).style.background = 'transparent' }}
                    >
                      {r}
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Legend */}
          <div style={{ display: 'flex', gap: 16, marginBottom: 10 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: '0.65rem', color: 'rgba(255,255,255,0.45)', fontWeight: 300 }}>
              <span style={{ display: 'inline-block', width: 18, height: 2, background: COLORS.high, borderRadius: 1 }} />
              High-Risk Zones
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: '0.65rem', color: 'rgba(255,255,255,0.45)', fontWeight: 300 }}>
              <span style={{ display: 'inline-block', width: 18, height: 2, background: 'rgba(255,255,255,0.25)', borderRadius: 1, borderTop: '1px dashed rgba(255,255,255,0.25)' }} />
              Target: 0
            </div>
          </div>

          <ResponsiveContainer width="100%" height={160}>
            <ComposedChart data={trendData} margin={{ top: 4, right: 6, bottom: 0, left: -16 }}>
              <XAxis dataKey="time" tick={{ fontSize: 9, fill: 'rgba(255,255,255,0.28)', fontFamily: 'Inter' }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis
                domain={[0, 'auto']}
                allowDecimals={false}
                tickFormatter={v => String(Math.round(v))}
                tick={{ fontSize: 9, fill: 'rgba(255,255,255,0.28)', fontFamily: 'Inter' }}
                tickLine={false}
                axisLine={false}
                width={28}
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                formatter={(v) => [`${v} zones`, 'High-Risk']}
              />
              <ReferenceLine y={0} stroke="rgba(255,255,255,0.20)" strokeDasharray="5 4" strokeWidth={1.5} label={{ value: 'Target', position: 'insideTopRight', fontSize: 8, fill: 'rgba(255,255,255,0.25)' }} />
              <Line type="monotone" dataKey="actual" stroke={COLORS.high} strokeWidth={2} dot={{ r: 3, fill: COLORS.high }} name="High-Risk Zones" />
            </ComposedChart>
          </ResponsiveContainer>
        </GlassCard>

        {/* MIDDLE: System Status */}
        <GlassCard hover={false} style={{ padding: '16px 18px' }}>
          <div style={SECTION_LABEL as React.CSSProperties}>System Status</div>
          {(() => {
            const svcs = servicesHealth?.services ?? []
            const allOk = svcs.length > 0 && svcs.every(s => s.status === 'ok')
            const anyDown = svcs.some(s => s.status === 'down')
            const summaryColor = allOk ? COLORS.low : anyDown ? COLORS.high : COLORS.medium
            const summaryLabel = allOk ? 'All Services Operational' : anyDown ? 'Service Degraded' : 'Partial Degradation'
            return (
              <>
                <div style={{
                  fontSize: '0.68rem', fontWeight: 500, color: summaryColor,
                  marginBottom: 14, marginTop: -4,
                  display: 'flex', alignItems: 'center', gap: 5,
                }}>
                  <CheckCircle size={12} strokeWidth={2} color={summaryColor} />
                  {summaryLabel}
                </div>
                {svcs.length > 0
                  ? svcs.map(svc => {
                      const dotColor = svc.status === 'ok' ? COLORS.low : svc.status === 'degraded' ? COLORS.medium : COLORS.high
                      return (
                        <div key={svc.name} style={{
                          display: 'flex', alignItems: 'center', gap: 10,
                          padding: '8px 0',
                          borderBottom: '1px solid rgba(99,140,255,0.07)',
                          fontSize: '0.76rem', fontWeight: 300, color: 'rgba(255,255,255,0.72)',
                        }}>
                          <StatusDot color={dotColor} />
                          <span style={{ flex: 1 }}>{svc.name}</span>
                          {svc.detail && (
                            <span style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.32)' }}>
                              {svc.detail}
                            </span>
                          )}
                        </div>
                      )
                    })
                  : ['Prediction API', 'Data Pipeline', 'Feature Store', 'Model Serving', 'Drift Monitor'].map(name => (
                      <div key={name} style={{
                        display: 'flex', alignItems: 'center', gap: 10,
                        padding: '8px 0',
                        borderBottom: '1px solid rgba(99,140,255,0.07)',
                        fontSize: '0.76rem', fontWeight: 300, color: 'rgba(255,255,255,0.40)',
                      }}>
                        <StatusDot color="rgba(255,255,255,0.20)" />
                        {name}
                      </div>
                    ))
                }
              </>
            )
          })()}
        </GlassCard>

        {/* RIGHT: Recent Model Run */}
        <GlassCard hover={false} style={{ padding: '16px 18px', display: 'flex', flexDirection: 'column' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <div style={SECTION_LABEL as React.CSSProperties}>Recent Model Run</div>
            {latestRun && (
              <Badge
                label={latestRun.run_status === 'success' ? 'Success' : latestRun.run_status === 'never_run' ? 'No Runs' : latestRun.run_status}
                color={latestRun.run_status === 'success' ? COLORS.low : COLORS.high}
              />
            )}
          </div>

          <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.32)', fontWeight: 300, marginBottom: 2 }}>Run ID</div>
          <div style={{ fontFamily: 'monospace', fontSize: '0.68rem', color: 'rgba(255,255,255,0.62)', marginBottom: 10, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {latestRun?.run_id ?? '—'}
          </div>

          <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.32)', fontWeight: 300, marginBottom: 2 }}>Completed</div>
          <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.62)', fontWeight: 300, marginBottom: 14 }}>
            {latestRun?.timestamp
              ? new Date(latestRun.timestamp).toLocaleString('en-SG', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
              : '—'}
          </div>

          {/* Metrics 2×2 */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 14 }}>
            {[
              { label: 'AUC',       value: modelStatus?.training_metrics?.roc_auc?.toFixed(4) ?? '—' },
              { label: 'Precision', value: modelStatus?.training_metrics?.precision?.toFixed(4) ?? '—' },
            ].map(m => (
              <div key={m.label} style={{
                background: 'rgba(255,255,255,0.04)',
                border: '1px solid rgba(99,140,255,0.10)',
                borderRadius: 8, padding: '8px 10px', textAlign: 'center',
              }}>
                <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.30)', fontWeight: 400, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 3 }}>{m.label}</div>
                <div style={{ fontSize: '0.92rem', fontWeight: 400, color: COLORS.primary }}>{m.value}</div>
              </div>
            ))}
          </div>

          <button
            className="btn-glass"
            onClick={() => navigate('/health')}
            style={{ fontSize: '0.70rem', padding: '6px 10px', marginTop: 'auto', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4 }}
          >
            <BarChart2 size={12} strokeWidth={1.75} />
            View Model Health
          </button>
        </GlassCard>
      </div>

      {/* ── ROW 4: RECOMMENDED NEXT ACTION BANNER ──────────────────────────── */}
      <GlassCard hover={false} style={{
        padding: '16px 22px',
        display: 'flex', alignItems: 'center', gap: 0,
        borderLeft: `3px solid ${COLORS.primary}`,
      }}>
        {/* Left: action */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={SECTION_LABEL as React.CSSProperties}>Recommended Next Action</span>
            <span style={{
              background: `${COLORS.low}22`, color: COLORS.low,
              border: `1px solid ${COLORS.low}40`,
              borderRadius: 20, padding: '1px 8px',
              fontSize: '0.58rem', fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase',
            }}>Live</span>
          </div>
          <div style={{ fontSize: '0.88rem', fontWeight: 500, color: 'rgba(255,255,255,0.88)', lineHeight: 1.45, marginBottom: 3 }}>
            {actionText}
          </div>
          <div style={{ fontSize: '0.70rem', fontWeight: 300, color: 'rgba(255,255,255,0.40)' }}>
            Based on current shortage-risk scores and live supply signals.
          </div>
        </div>

        {/* Middle: Estimated Impact */}
        <div style={{
          borderLeft: '1px solid rgba(99,140,255,0.14)',
          borderRight: '1px solid rgba(99,140,255,0.14)',
          padding: '0 32px', margin: '0 28px', textAlign: 'center', flexShrink: 0,
        }}>
          <div style={SECTION_LABEL as React.CSSProperties}>Focus Zones</div>
          {highRisk > 0 ? (
            <>
              <div style={{ fontSize: '1.40rem', fontWeight: 300, color: COLORS.high, lineHeight: 1, marginBottom: 3 }}>
                {highRisk} high-risk
              </div>
              <div style={{ fontSize: '0.70rem', fontWeight: 300, color: 'rgba(255,255,255,0.40)' }}>
                zones need immediate review
              </div>
            </>
          ) : depletion > 0 ? (
            <>
              <div style={{ fontSize: '1.40rem', fontWeight: 300, color: COLORS.medium, lineHeight: 1, marginBottom: 3 }}>
                {depletion} depleting
              </div>
              <div style={{ fontSize: '0.70rem', fontWeight: 300, color: 'rgba(255,255,255,0.40)' }}>
                zones losing supply fast
              </div>
            </>
          ) : (
            <>
              <div style={{ fontSize: '1.40rem', fontWeight: 300, color: COLORS.low, lineHeight: 1, marginBottom: 3 }}>
                All clear
              </div>
              <div style={{ fontSize: '0.70rem', fontWeight: 300, color: 'rgba(255,255,255,0.40)' }}>
                no zones require action
              </div>
            </>
          )}
        </div>

        {/* Right: CTA */}
        <div style={{ flexShrink: 0 }}>
          <button className="btn-primary" onClick={() => navigate('/actions')}>
            Go to Action Center <ArrowUpRight size={14} strokeWidth={2} />
          </button>
        </div>
      </GlassCard>

    </div>
  )
}
