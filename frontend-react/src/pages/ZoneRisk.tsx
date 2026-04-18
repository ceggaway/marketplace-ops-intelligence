import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Search,
  Car, AlertTriangle, TrendingUp, Zap, Activity, Download,
} from 'lucide-react'
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts'
import { api } from '../lib/api'
import type { Zone } from '../lib/api'
import { riskColor, COLORS, SECTION_LABEL, TOOLTIP_STYLE } from '../lib/utils'
import GlassCard from '../components/GlassCard'
import Spinner from '../components/Spinner'
import ApiError from '../components/ApiError'
import EmptyState from '../components/EmptyState'
import SingaporeMap from '../components/SingaporeMap'
import SparkLine from '../components/SparkLine'

// ── Helper: risk pill ────────────────────────────────────────────────────────
function RiskPill({ score, level }: { score: number; level: string }) {
  const color = riskColor(level)
  return (
    <span style={{
      background: `${color}18`,
      color,
      border: `1px solid ${color}35`,
      borderRadius: 6,
      padding: '3px 8px',
      fontSize: '0.78rem',
      fontWeight: 600,
      fontVariantNumeric: 'tabular-nums',
    }}>
      {score.toFixed(3)}
    </span>
  )
}

// ── KPI chip ─────────────────────────────────────────────────────────────────
interface KpiChipProps {
  label: string
  value: React.ReactNode
  color: string
  icon: React.ReactNode
  badge?: string
  right?: React.ReactNode
}
function KpiChip({ label, value, color, icon, badge, right }: KpiChipProps) {
  return (
    <div style={{
      background: 'rgba(255,255,255,0.04)',
      border: '1px solid rgba(99,140,255,0.14)',
      borderTop: `2px solid ${color}`,
      borderRadius: 10,
      padding: '14px 16px',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 10,
      flex: 1,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <div style={{
          width: 34, height: 34, borderRadius: 8,
          background: `${color}18`,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          flexShrink: 0,
        }}>
          {icon}
        </div>
        <div>
          <div style={{ ...SECTION_LABEL, marginBottom: 2, fontSize: '0.56rem' }}>{label}</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: '1.2rem', fontWeight: 700, color: 'rgba(255,255,255,0.92)', fontVariantNumeric: 'tabular-nums' }}>
              {value}
            </span>
            {badge && (
              <span style={{
                fontSize: '0.65rem', fontWeight: 600,
                background: `${color}22`, color, borderRadius: 4,
                padding: '1px 5px', border: `1px solid ${color}33`,
              }}>{badge}</span>
            )}
          </div>
        </div>
      </div>
      {right && <div style={{ flexShrink: 0 }}>{right}</div>}
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────
export default function ZoneRisk() {
  const navigate = useNavigate()
  const [riskFilter, setRiskFilter] = useState('')
  const [regionFilter, setRegionFilter] = useState('')
  const [search, setSearch] = useState('')
  const [selectedId, setSelectedId] = useState<number | null>(null)

  // Always fetch all zones — filtering is done client-side so search, risk and
  // region all work together on the same dataset without extra API round-trips.
  const { data: zones, isLoading, isError } = useQuery({
    queryKey: ['zones'],
    queryFn: () => api.zones(),
    staleTime: 30000,
  })
  const { data: detail } = useQuery({
    queryKey: ['zone', selectedId],
    queryFn: () => api.zoneDetail(selectedId!),
    enabled: selectedId !== null,
    staleTime: 30000,
  })
  const { data: latestRun } = useQuery({
    queryKey: ['latestRun'],
    queryFn: api.latestRun,
    staleTime: 30000,
  })
  const { data: drift } = useQuery({
    queryKey: ['drift'],
    queryFn: api.drift,
    staleTime: 30000,
  })
  const { data: overview } = useQuery({
    queryKey: ['overview'],
    queryFn: api.overview,
    staleTime: 30000,
  })

  // ── Derived data ──────────────────────────────────────────────────────────
  // Counts always reflect the full unfiltered dataset — KPI chips show total picture
  const counts = zones
    ? {
        high:   zones.filter(z => z.risk_level === 'high').length,
        medium: zones.filter(z => z.risk_level === 'medium').length,
        low:    zones.filter(z => z.risk_level === 'low').length,
      }
    : { high: 0, medium: 0, low: 0 }

  const totalZones = zones?.length ?? 0
  const avgRisk = zones?.length
    ? zones.reduce((s, z) => s + z.delay_risk_score, 0) / zones.length
    : 0

  const pct = (n: number) => totalZones ? `${((n / totalZones) * 100).toFixed(1)}%` : '0%'

  const filtered = zones
    ? [...zones]
        .filter(z =>
          (!riskFilter   || z.risk_level === riskFilter) &&
          (!regionFilter || z.region === regionFilter) &&
          (!search       || z.zone_name.toLowerCase().includes(search.toLowerCase()))
        )
        .sort((a, b) => b.delay_risk_score - a.delay_risk_score)
    : []

  const selectedZone = zones?.find(z => z.zone_id === selectedId) ?? null

  const donutData = [
    { name: 'High', value: counts.high },
    { name: 'Medium', value: counts.medium },
    { name: 'Low', value: counts.low },
  ]
  const donutColors = [COLORS.high, COLORS.medium, COLORS.low]
  const highConcentration = totalZones ? Math.round((counts.high / totalZones) * 100) : 0

  // KEY DRIVERS from explanation_tag — tags use " + " as separator
  const drivers = detail?.explanation_tag && detail.explanation_tag !== 'normal conditions'
    ? detail.explanation_tag.split(/\s*\+\s*|[,·|]/).map(s => s.trim()).filter(Boolean)
    : []

  // Sparkline for avg risk chip: use high_risk_zones_now / 55 as a normalised
  // risk proxy over time. Falls back to a flat point at current avgRisk.
  const sparkData = overview?.fulfilment_trend && overview.fulfilment_trend.length > 0
    ? overview.fulfilment_trend.map(p => ({ timestamp: p.timestamp, value: p.value / 55 }))
    : [{ timestamp: '0', value: avgRisk }]

  // ── Handlers ──────────────────────────────────────────────────────────────
  const handleSelectRow = (id: number) => {
    setSelectedId(prev => (prev === id ? null : id))
  }

  const handleExportCsv = () => {
    if (filtered.length === 0) return

    const rows = filtered.map(zone => ({
      zone_id: zone.zone_id,
      zone_name: zone.zone_name,
      region: zone.region,
      risk_level: zone.risk_level,
      delay_risk_score: zone.delay_risk_score.toFixed(4),
      current_supply: zone.current_supply,
      supply_vs_yesterday: zone.supply_vs_yesterday.toFixed(4),
      depletion_rate_1h: zone.depletion_rate_1h.toFixed(4),
      explanation_tag: zone.explanation_tag,
      recommendation: zone.recommendation ?? '',
    }))

    const headers = Object.keys(rows[0])
    const csv = [
      headers.join(','),
      ...rows.map(row =>
        headers
          .map(header => {
            const value = String(row[header as keyof typeof row] ?? '')
            return `"${value.replaceAll('"', '""')}"`
          })
          .join(',')
      ),
    ].join('\n')

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    const date = new Date().toISOString().slice(0, 10)
    link.href = url
    link.download = `zones-${date}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  // ── Render ────────────────────────────────────────────────────────────────
  if (isLoading) return <Spinner />
  if (isError) return <ApiError message="The zones endpoint could not be loaded." />
  if (!zones || zones.length === 0) {
    return <EmptyState title="No zone scores available" message="Run the scoring pipeline to populate predictions before using the zone monitor." />
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── Row 1: KPI chips ───────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 12 }}>
        <KpiChip
          label="Total Zones"
          value={totalZones.toLocaleString()}
          color={COLORS.primary}
          icon={<Activity size={16} color={COLORS.primary} strokeWidth={1.75} />}
        />
        <KpiChip
          label="High Risk"
          value={counts.high}
          color={COLORS.high}
          badge={pct(counts.high)}
          icon={<AlertTriangle size={16} color={COLORS.high} strokeWidth={1.75} />}
        />
        <KpiChip
          label="Medium Risk"
          value={counts.medium}
          color={COLORS.medium}
          badge={pct(counts.medium)}
          icon={<Zap size={16} color={COLORS.medium} strokeWidth={1.75} />}
        />
        <KpiChip
          label="Low Risk"
          value={counts.low}
          color={COLORS.low}
          badge={pct(counts.low)}
          icon={<TrendingUp size={16} color={COLORS.low} strokeWidth={1.75} />}
        />
        <KpiChip
          label="Avg Risk Score"
          value={avgRisk.toFixed(3)}
          color={COLORS.primary}
          icon={<Activity size={16} color={COLORS.primary} strokeWidth={1.75} />}
          right={<SparkLine data={sparkData} color={COLORS.primary} height={36} width={80} />}
        />
      </div>

      {/* ── Row 2: Table + Map+Detail ──────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '45% 1fr', gap: 16 }}>

        {/* LEFT: Zone table panel */}
        <GlassCard hover={false} style={{ padding: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
          {/* Search + filter bar */}
          <div style={{ padding: '14px 16px 10px', display: 'flex', flexDirection: 'column', gap: 8, borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
            {/* Search row */}
            <div style={{ position: 'relative' }}>
              <Search size={13} color="rgba(255,255,255,0.35)" style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)' }} />
              <input
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Search zones..."
                style={{
                  width: '100%', boxSizing: 'border-box',
                  background: 'rgba(255,255,255,0.06)',
                  border: '1px solid rgba(99,140,255,0.14)',
                  borderRadius: 8, padding: '7px 10px 7px 30px',
                  color: 'rgba(255,255,255,0.80)', fontSize: '0.78rem',
                  outline: 'none', fontFamily: 'Inter, sans-serif',
                }}
              />
            </div>
            {/* Filter row */}
            <div style={{ display: 'flex', gap: 8 }}>
              <select
                value={riskFilter}
                onChange={e => setRiskFilter(e.target.value)}
                style={{ flex: 1, background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(99,140,255,0.14)', borderRadius: 8, padding: '6px 10px', color: 'rgba(255,255,255,0.70)', fontSize: '0.75rem', outline: 'none', fontFamily: 'Inter, sans-serif', cursor: 'pointer' }}
              >
                <option value="">All Risk Levels</option>
                <option value="high">High</option>
                <option value="medium">Medium</option>
                <option value="low">Low</option>
              </select>
              <select
                value={regionFilter}
                onChange={e => setRegionFilter(e.target.value)}
                style={{ flex: 1, background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(99,140,255,0.14)', borderRadius: 8, padding: '6px 10px', color: 'rgba(255,255,255,0.70)', fontSize: '0.75rem', outline: 'none', fontFamily: 'Inter, sans-serif', cursor: 'pointer' }}
              >
                <option value="">All Regions</option>
                {['Central', 'East', 'West', 'North', 'North-East'].map(r => (
                  <option key={r} value={r}>{r}</option>
                ))}
              </select>
              <button
                onClick={() => { setRiskFilter(''); setRegionFilter(''); setSearch('') }}
                style={{ padding: '6px 10px', borderRadius: 8, background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(99,140,255,0.14)', color: 'rgba(255,255,255,0.45)', fontSize: '0.70rem', cursor: 'pointer', fontFamily: 'Inter, sans-serif', whiteSpace: 'nowrap' }}
              >
                Clear
              </button>
              <button
                onClick={handleExportCsv}
                disabled={filtered.length === 0}
                style={{
                  display: 'inline-flex', alignItems: 'center', gap: 6,
                  padding: '6px 10px', borderRadius: 8,
                  background: filtered.length > 0 ? 'rgba(79,142,247,0.12)' : 'rgba(255,255,255,0.05)',
                  border: `1px solid ${filtered.length > 0 ? 'rgba(79,142,247,0.25)' : 'rgba(255,255,255,0.10)'}`,
                  color: filtered.length > 0 ? COLORS.primary : 'rgba(255,255,255,0.30)',
                  fontSize: '0.70rem', cursor: filtered.length > 0 ? 'pointer' : 'not-allowed',
                  fontFamily: 'Inter, sans-serif', whiteSpace: 'nowrap',
                }}
              >
                <Download size={12} />
                Export CSV
              </button>
            </div>
          </div>

          {/* Table */}
          <div style={{ overflowX: 'auto', overflowY: 'auto', flex: 1, maxHeight: '430px' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
              <thead style={{ position: 'sticky', top: 0, background: 'rgba(8,15,30,0.97)', zIndex: 1 }}>
                <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.07)' }}>
                  {['ZONE', 'REGION', 'RISK SCORE', 'SUPPLY', 'DEPLETION /HR', 'ACTION'].map(h => (
                    <th key={h} style={{
                      padding: '10px 14px',
                      textAlign: 'left',
                      ...SECTION_LABEL,
                      marginBottom: 0,
                      whiteSpace: 'nowrap',
                      fontWeight: 700,
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.map((z: Zone) => {
                  const isSelected = selectedId === z.zone_id
                  return (
                    <tr
                      key={z.zone_id}
                      onClick={() => handleSelectRow(z.zone_id)}
                      style={{
                        borderBottom: '1px solid rgba(255,255,255,0.04)',
                        background: isSelected ? 'rgba(79,142,247,0.12)' : 'transparent',
                        cursor: 'pointer',
                        transition: 'background 0.12s',
                      }}
                      onMouseEnter={e => { if (!isSelected) (e.currentTarget as HTMLElement).style.background = 'rgba(79,142,247,0.06)' }}
                      onMouseLeave={e => { if (!isSelected) (e.currentTarget as HTMLElement).style.background = 'transparent' }}
                    >
                      <td style={{ padding: '9px 14px', color: 'rgba(255,255,255,0.88)', fontWeight: 500, whiteSpace: 'nowrap', maxWidth: 130, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {z.zone_name}
                      </td>
                      <td style={{ padding: '9px 14px', color: 'rgba(255,255,255,0.42)', fontSize: '0.72rem', whiteSpace: 'nowrap' }}>
                        {z.region}
                      </td>
                      <td style={{ padding: '9px 14px' }}>
                        <RiskPill score={z.delay_risk_score} level={z.risk_level} />
                      </td>
                      <td style={{ padding: '9px 14px', color: 'rgba(255,255,255,0.60)', fontVariantNumeric: 'tabular-nums' }}>
                        {z.current_supply.toLocaleString()}
                      </td>
                      <td style={{ padding: '9px 14px', fontVariantNumeric: 'tabular-nums' }}>
                        <span style={{ color: z.depletion_rate_1h > 0.30 ? COLORS.high : z.depletion_rate_1h > 0.10 ? COLORS.medium : 'rgba(255,255,255,0.45)' }}>
                          {z.depletion_rate_1h > 0 ? `${(z.depletion_rate_1h * 100).toFixed(1)}%` : '—'}
                        </span>
                      </td>
                      <td style={{ padding: '9px 14px' }}>
                        <button
                          onClick={e => { e.stopPropagation(); handleSelectRow(z.zone_id) }}
                          style={{
                            background: 'rgba(79,142,247,0.12)',
                            border: '1px solid rgba(79,142,247,0.25)',
                            borderRadius: 5, padding: '3px 10px',
                            color: COLORS.primary, fontSize: '0.70rem',
                            cursor: 'pointer', fontFamily: 'Inter, sans-serif', fontWeight: 600,
                          }}
                        >View</button>
                      </td>
                    </tr>
                  )
                })}
                {filtered.length === 0 && (
                  <tr>
                    <td colSpan={6} style={{ padding: '28px 14px', textAlign: 'center', color: 'rgba(255,255,255,0.28)', fontSize: '0.80rem' }}>
                      No zones match your search
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

        </GlassCard>

        {/* RIGHT: Map + Detail */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {/* Map area */}
          <GlassCard hover={false} style={{ padding: '14px 16px', overflow: 'hidden', flex: '0 0 auto' }}>
            {/* Header + gradient bar */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
              <span style={{ ...SECTION_LABEL, marginBottom: 0, fontSize: '0.58rem' }}>RISK LEVEL</span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                {([['Low', COLORS.low], ['Medium', COLORS.medium], ['High', COLORS.high]] as const).map(([label, color]) => (
                  <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    <div style={{ width: 8, height: 8, borderRadius: '50%', background: color, boxShadow: `0 0 5px ${color}88` }} />
                    <span style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.45)' }}>{label}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* Map */}
            <div style={{ height: 270, borderRadius: 8, overflow: 'hidden', position: 'relative' }}>
              {zones && (
                <SingaporeMap
                  zones={zones}
                  selectedId={selectedId}
                  onSelect={setSelectedId}
                  mode="risk"
                />
              )}
              {/* Floating tooltip for selected zone */}
              {selectedZone && (
                <div style={{
                  position: 'absolute', top: 10, right: 10, zIndex: 500,
                  background: 'rgba(6,13,26,0.92)',
                  border: `1px solid ${riskColor(selectedZone.risk_level)}55`,
                  borderRadius: 8, padding: '8px 12px', minWidth: 160,
                  backdropFilter: 'blur(8px)',
                }}>
                  <div style={{ fontSize: '0.78rem', fontWeight: 600, color: 'rgba(255,255,255,0.92)', marginBottom: 2 }}>
                    {selectedZone.zone_name}
                  </div>
                  <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.42)', marginBottom: 6 }}>
                    {selectedZone.region}
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.68rem' }}>
                    <span style={{ color: 'rgba(255,255,255,0.42)' }}>Risk Score</span>
                    <span style={{ color: riskColor(selectedZone.risk_level), fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                      {selectedZone.delay_risk_score.toFixed(3)}
                    </span>
                  </div>
                </div>
              )}
            </div>
          </GlassCard>

          {/* Zone detail panel (shown when a zone is selected and detail is loaded) */}
          {selectedId && detail ? (
            <GlassCard hover={false} style={{ padding: '16px 18px', flex: 1 }}>
              {/* Zone name + badge */}
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 14 }}>
                <div>
                  <div style={{ fontSize: '1.05rem', fontWeight: 700, color: 'rgba(255,255,255,0.92)', marginBottom: 4 }}>
                    {detail.zone_name}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{
                      fontSize: '0.68rem', fontWeight: 600,
                      background: `${riskColor(detail.risk_level)}18`,
                      color: riskColor(detail.risk_level),
                      border: `1px solid ${riskColor(detail.risk_level)}30`,
                      borderRadius: 4, padding: '2px 7px',
                    }}>
                      {detail.risk_level.charAt(0).toUpperCase() + detail.risk_level.slice(1)} Risk
                    </span>
                    {detail.risk_level === 'high' && (
                      <span style={{
                        fontSize: '0.68rem', fontWeight: 600,
                        background: 'rgba(255,77,109,0.10)',
                        color: COLORS.high,
                        border: '1px solid rgba(255,77,109,0.25)',
                        borderRadius: 4, padding: '2px 7px',
                      }}>Top Priority</span>
                    )}
                  </div>
                </div>
                <div style={{
                  fontSize: '1.75rem', fontWeight: 800,
                  color: riskColor(detail.risk_level),
                  fontVariantNumeric: 'tabular-nums', lineHeight: 1,
                }}>
                  {detail.delay_risk_score.toFixed(3)}
                </div>
              </div>

              {/* KEY DRIVERS */}
              <div style={{ marginBottom: 12 }}>
                <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>KEY DRIVERS</div>
                {drivers.length > 0 ? (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {drivers.map((d, i) => (
                      <span key={i} style={{
                        fontSize: '0.70rem', fontWeight: 500,
                        background: 'rgba(79,142,247,0.10)',
                        color: COLORS.primary,
                        border: '1px solid rgba(79,142,247,0.22)',
                        borderRadius: 20, padding: '3px 10px',
                      }}>{d}</span>
                    ))}
                  </div>
                ) : (
                  <span style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.35)' }}>Normal conditions — no elevated drivers</span>
                )}
              </div>

              {/* SUPPLY SIGNALS */}
              <div style={{ marginBottom: 12 }}>
                <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>SUPPLY SIGNALS</div>
                <div style={{ display: 'flex', gap: 10 }}>
                  <div style={{
                    flex: 1, background: 'rgba(255,77,109,0.08)',
                    border: '1px solid rgba(255,77,109,0.18)',
                    borderRadius: 8, padding: '8px 12px',
                  }}>
                    <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.38)', marginBottom: 2 }}>Depletion / hr</div>
                    <div style={{ fontSize: '0.92rem', fontWeight: 700, color: detail.depletion_rate_1h > 0.30 ? COLORS.high : detail.depletion_rate_1h > 0.10 ? COLORS.medium : COLORS.low }}>
                      {detail.depletion_rate_1h > 0 ? `${(detail.depletion_rate_1h * 100).toFixed(1)}%` : '—'}
                    </div>
                  </div>
                  <div style={{
                    flex: 1, background: 'rgba(255,255,255,0.04)',
                    border: '1px solid rgba(255,255,255,0.10)',
                    borderRadius: 8, padding: '8px 12px',
                  }}>
                    <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.38)', marginBottom: 2 }}>Active Supply</div>
                    <div style={{ fontSize: '0.92rem', fontWeight: 700, color: 'rgba(255,255,255,0.80)' }}>
                      {detail.taxi_count} taxis
                    </div>
                  </div>
                </div>
              </div>

              {/* RECOMMENDED ACTION */}
              {detail.recommendation && detail.recommendation !== 'No action required' && (
                <div style={{ marginBottom: 14 }}>
                  <div style={{ ...SECTION_LABEL, marginBottom: 6 }}>RECOMMENDED ACTION</div>
                  <div style={{
                    background: 'rgba(16,217,138,0.07)',
                    border: '1px solid rgba(16,217,138,0.18)',
                    borderRadius: 8, padding: '9px 12px',
                    fontSize: '0.78rem', color: 'rgba(255,255,255,0.75)', lineHeight: 1.5,
                    display: 'flex', gap: 8,
                  }}>
                    <Zap size={13} color={COLORS.low} strokeWidth={1.75} style={{ flexShrink: 0, marginTop: 2 }} />
                    <span>{detail.recommendation}</span>
                  </div>
                </div>
              )}

              {/* CTA button */}
              <button
                onClick={() => navigate('/actions')}
                className="btn-primary"
                style={{
                  width: '100%', padding: '10px 16px',
                  background: COLORS.primary, border: 'none',
                  borderRadius: 8, color: '#fff',
                  fontSize: '0.82rem', fontWeight: 600,
                  cursor: 'pointer', fontFamily: 'Inter, sans-serif',
                  display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
                  transition: 'opacity 0.15s',
                }}
                onMouseEnter={e => (e.currentTarget.style.opacity = '0.85')}
                onMouseLeave={e => (e.currentTarget.style.opacity = '1')}
              >
                Create Intervention →
              </button>
            </GlassCard>
          ) : (
            /* Placeholder when no zone selected */
            <GlassCard hover={false} style={{ padding: '20px 18px', flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div style={{ textAlign: 'center' }}>
                <AlertTriangle size={28} color="rgba(255,255,255,0.18)" strokeWidth={1.5} style={{ marginBottom: 8 }} />
                <div style={{ fontSize: '0.80rem', color: 'rgba(255,255,255,0.35)' }}>
                  Select a zone to view details
                </div>
              </div>
            </GlassCard>
          )}
        </div>
      </div>

      {/* ── Row 3: Key Insight + Risk Distribution + Auto-Retraining ────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>

        {/* KEY INSIGHT */}
        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 12 }}>KEY INSIGHT</div>
          {counts.high > 0 ? (
            <>
              <div style={{ fontSize: '0.92rem', fontWeight: 700, color: 'rgba(255,255,255,0.90)', lineHeight: 1.4, marginBottom: 8 }}>
                {counts.high} zone{counts.high !== 1 ? 's' : ''} at high shortage risk — immediate action needed
              </div>
              <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.45)', lineHeight: 1.5, marginBottom: 16 }}>
                High-risk zones show elevated supply stress and rapid depletion. Targeted interventions in these areas can reduce overall shortage exposure.
              </div>
            </>
          ) : counts.medium > 0 ? (
            <>
              <div style={{ fontSize: '0.92rem', fontWeight: 700, color: 'rgba(255,255,255,0.90)', lineHeight: 1.4, marginBottom: 8 }}>
                {counts.medium} zone{counts.medium !== 1 ? 's' : ''} at medium risk — monitor for escalation
              </div>
              <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.45)', lineHeight: 1.5, marginBottom: 16 }}>
                No zones have crossed the high-risk threshold. Watch medium-risk zones for depletion rates exceeding 30%/hr — that is the leading indicator.
              </div>
            </>
          ) : (
            <>
              <div style={{ fontSize: '0.92rem', fontWeight: 700, color: COLORS.low, lineHeight: 1.4, marginBottom: 8 }}>
                All {totalZones} zones at low risk — supply is stable
              </div>
              <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.45)', lineHeight: 1.5, marginBottom: 16 }}>
                No shortage risk detected across all zones. Continue monitoring depletion rates as an early warning signal.
              </div>
            </>
          )}
          <button
            onClick={() => navigate('/actions')}
            style={{
              background: 'none', border: 'none', padding: 0,
              color: COLORS.primary, fontSize: '0.78rem', fontWeight: 600,
              cursor: 'pointer', fontFamily: 'Inter, sans-serif',
              display: 'flex', alignItems: 'center', gap: 4,
            }}
          >
            View Action Center →
          </button>
        </GlassCard>

        {/* RISK DISTRIBUTION */}
        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 10 }}>RISK DISTRIBUTION</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
              {/* Donut chart */}
            <div style={{ position: 'relative', width: 170, height: 170, flexShrink: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={donutData}
                    cx="50%"
                    cy="50%"
                    innerRadius={55}
                    outerRadius={80}
                    dataKey="value"
                    strokeWidth={0}
                  >
                    {donutData.map((_, i) => (
                      <Cell key={i} fill={donutColors[i]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={TOOLTIP_STYLE}
                    itemStyle={{ color: 'rgba(255,255,255,0.75)', fontSize: 11 }}
                    labelStyle={{ color: 'rgba(255,255,255,0.42)' }}
                  />
                </PieChart>
              </ResponsiveContainer>
              {/* Center label */}
              <div style={{
                position: 'absolute', top: '50%', left: '50%',
                transform: 'translate(-50%, -50%)',
                textAlign: 'center', pointerEvents: 'none',
              }}>
                <div style={{ fontSize: '1.25rem', fontWeight: 800, color: 'rgba(255,255,255,0.92)', lineHeight: 1 }}>
                  {highConcentration}%
                </div>
                <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.38)', marginTop: 2 }}>
                  Concentration
                </div>
              </div>
            </div>
            {/* Legend */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {[
                { label: 'High (≥ 0.70)', count: counts.high, color: COLORS.high },
                { label: 'Medium (0.40-0.69)', count: counts.medium, color: COLORS.medium },
                { label: 'Low (< 0.40)', count: counts.low, color: COLORS.low },
              ].map(({ label, count, color }) => (
                <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{ width: 10, height: 10, borderRadius: 3, background: color, flexShrink: 0 }} />
                  <div>
                    <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.55)' }}>{label}</div>
                    <div style={{ fontSize: '0.80rem', fontWeight: 600, color: 'rgba(255,255,255,0.85)' }}>
                      {count} zone{count !== 1 ? 's' : ''}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </GlassCard>

        {/* AUTO-RETRAINING STATUS */}
        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
            <div style={{ ...SECTION_LABEL, marginBottom: 0 }}>PIPELINE STATUS</div>
            <span style={{
              fontSize: '0.65rem', fontWeight: 600,
              background: latestRun?.run_status === 'success' ? 'rgba(16,217,138,0.12)' : 'rgba(255,77,109,0.12)',
              color: latestRun?.run_status === 'success' ? COLORS.low : COLORS.high,
              border: `1px solid ${latestRun?.run_status === 'success' ? 'rgba(16,217,138,0.22)' : 'rgba(255,77,109,0.22)'}`,
              borderRadius: 20, padding: '2px 9px',
            }}>
              {latestRun?.run_status === 'success' ? 'Healthy' : latestRun?.run_status === 'never_run' ? 'No Runs' : latestRun?.run_status ?? 'Unknown'}
            </span>
          </div>

          <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)', marginBottom: 6 }}>
            Active taxis right now
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 16 }}>
            <Car size={18} color={COLORS.primary} strokeWidth={1.75} />
            <span style={{ fontSize: '2.2rem', fontWeight: 800, color: COLORS.primary, lineHeight: 1, fontVariantNumeric: 'tabular-nums' }}>
              {overview?.kpis.total_taxi_supply ?? latestRun?.total_taxi_count ?? '—'}
            </span>
          </div>

          <div style={{
            background: 'rgba(255,255,255,0.04)',
            border: '1px solid rgba(255,255,255,0.08)',
            borderRadius: 8, padding: '10px 12px',
            fontSize: '0.72rem', color: 'rgba(255,255,255,0.45)', lineHeight: 1.5,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span>Last run</span>
              <span style={{ color: 'rgba(255,255,255,0.65)', fontWeight: 500 }}>
                {latestRun?.timestamp
                  ? new Date(latestRun.timestamp).toLocaleString('en-SG', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
                  : '—'}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <span>High-risk zones</span>
              <span style={{ color: overview?.kpis.high_risk_zone_count ? COLORS.high : COLORS.low, fontWeight: 600 }}>
                {overview?.kpis.high_risk_zone_count ?? '—'}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>PSI</span>
              <span style={{
                color: drift?.psi != null
                  ? (drift.psi >= 0.25 ? COLORS.high : drift.psi >= 0.10 ? COLORS.medium : COLORS.low)
                  : 'rgba(255,255,255,0.45)',
                fontWeight: 600,
              }}>
                {drift?.psi != null ? drift.psi.toFixed(4) : '—'}
              </span>
            </div>
          </div>
        </GlassCard>
      </div>
    </div>
  )
}
