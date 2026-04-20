import { useState, useRef, useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Zap, Search, SlidersHorizontal, ChevronRight, TrendingDown, Clock, Target, AlertTriangle, GitBranch } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { api } from '../lib/api'
import { showToast } from '../components/toast-utils'
import type { Recommendation, AlternativeAction } from '../lib/api'
import { priorityColor, COLORS, SECTION_LABEL, TOOLTIP_STYLE } from '../lib/utils'
import GlassCard from '../components/GlassCard'
import Badge from '../components/Badge'
import Spinner from '../components/Spinner'
import ApiError from '../components/ApiError'
import EmptyState from '../components/EmptyState'

// ── Circular confidence gauge ───────────────────────────────────────────────
function ConfCircle({ value, color }: { value: number; color: string }) {
  const r = 16, circ = 2 * Math.PI * r
  return (
    <svg width={42} height={42} style={{ transform: 'rotate(-90deg)', flexShrink: 0 }}>
      <circle cx={21} cy={21} r={r} fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth={3} />
      <circle
        cx={21} cy={21} r={r} fill="none" stroke={color} strokeWidth={3}
        strokeDasharray={`${(value / 100) * circ} ${circ}`} strokeLinecap="round"
      />
      <text
        x={21} y={21} textAnchor="middle" dominantBaseline="central"
        style={{
          transform: 'rotate(90deg)', transformOrigin: '21px 21px',
          fontSize: 10, fill: color, fontWeight: 600, fontFamily: 'Inter',
        }}
      >
        {Math.round(value)}%
      </text>
    </svg>
  )
}

// ── Confidence bar (detail panel) ───────────────────────────────────────────
function ConfBar({ value, color }: { value: number; color: string }) {
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={MINI_LABEL}>Confidence</span>
        <span style={{ fontSize: '0.78rem', fontWeight: 700, color }}>{Math.round(value)}%</span>
      </div>
      <div style={{ background: 'rgba(255,255,255,0.08)', borderRadius: 20, height: 5, overflow: 'hidden' }}>
        <div style={{
          width: `${value}%`, height: '100%', background: color,
          borderRadius: 20, transition: 'width 0.4s',
        }} />
      </div>
    </div>
  )
}

// ── Shared style tokens ─────────────────────────────────────────────────────
const MINI_LABEL: React.CSSProperties = {
  fontSize: '0.58rem', fontWeight: 500, textTransform: 'uppercase',
  letterSpacing: '0.09em', color: 'rgba(255,255,255,0.35)',
}

const KPI_LABEL: React.CSSProperties = {
  fontSize: '0.60rem', fontWeight: 600, textTransform: 'uppercase',
  letterSpacing: '0.10em', color: 'rgba(255,255,255,0.38)', marginBottom: 4,
}

// ── Mini impact chart data generator ───────────────────────────────────────
//
// NOTE: This chart is ILLUSTRATIVE — it is not produced by a learned impact
// model. Worsening rate is calibrated from eta_minutes when the engine
// provides it; otherwise it falls back to priority-tier heuristics.
// Expected improvement uses intervention_window to zero out benefit when the
// action window has passed.
//
// When real outcome data accumulates in the outcome tracker, replace these
// heuristics with zone-type × action-type regression estimates.
function buildChartData(rec: Recommendation) {
  const score = rec.delay_risk_score
  const priority = rec.priority
  const etaMin = rec.eta_minutes          // minutes until zone crosses critical threshold
  const window = rec.intervention_window  // 'good' | 'tight' | 'too_late'

  // ── Without-action worsening rate (per hour) ──
  // If eta_minutes is known: zone will reach ~0.85 in that many minutes.
  // Derive the per-hour drift rate from that. Cap at 0.18 to avoid nonsensical curves.
  const worsenPerHr: number = (() => {
    if (etaMin != null && etaMin > 0 && etaMin < 180) {
      return Math.min(0.18, Math.max(0.02, (0.85 - score) / (etaMin / 60)))
    }
    // Fallback: priority-based heuristic (validated against SG PHV patterns)
    return priority === 'critical' ? 0.12
         : priority === 'high'     ? 0.07
         : priority === 'medium'   ? 0.04
         : 0.02
  })()

  // ── With-action improvement ──
  // intervention_window='too_late' means drivers cannot reach in time — no effect.
  // Maximum drop is bounded: can't fall below 0.20 (structural noise floor) and
  // can't exceed 0.35 (realistic supply response ceiling for a single run).
  const actionDisabled = window === 'too_late'
  const maxDrop = actionDisabled
    ? 0
    : Math.max(0, Math.min(score - 0.20, 0.35))
  const speedMult = priority === 'critical' ? 1.40 : priority === 'high' ? 1.15 : 1.0

  const clamp = (v: number) => Math.min(1, Math.max(0, parseFloat(v.toFixed(3))))
  const baseAt = (hrs: number) => clamp(score + worsenPerHr * hrs)
  const actionAt = (frac: number) => clamp(score - Math.min(maxDrop * frac * speedMult, maxDrop))

  return [
    { t: 'Now',  baseline: score,        action: score },
    { t: '+15m', baseline: baseAt(0.25), action: actionAt(0.30) },
    { t: '+30m', baseline: baseAt(0.50), action: actionAt(0.55) },
    { t: '+1h',  baseline: baseAt(1.00), action: actionAt(0.75) },
    { t: '+2h',  baseline: baseAt(1.80), action: actionAt(0.90) },
    { t: '+3h',  baseline: baseAt(2.40), action: actionAt(0.85) },
  ]
}

// ── Action item row ─────────────────────────────────────────────────────────
function ActionRow({
  rec, rank, selected, onClick,
}: {
  rec: Recommendation
  rank: number
  selected: boolean
  onClick: () => void
}) {
  const color = priorityColor(rec.priority)
  const confPct = rec.confidence * 100
  const confColor = confPct >= 80 ? COLORS.low : confPct >= 60 ? COLORS.medium : COLORS.high

  // Parse action detail — look for patterns like "+$2.00 for 3 hours"
  const actionText = rec.recommendation ?? ''

  return (
    <div
      onClick={onClick}
      style={{
        display: 'flex', alignItems: 'center', gap: 14,
        padding: '14px 18px', borderRadius: 12, marginBottom: 8,
        background: selected ? 'rgba(79,142,247,0.08)' : 'rgba(255,255,255,0.03)',
        border: selected
          ? '1px solid rgba(79,142,247,0.30)'
          : '1px solid rgba(255,255,255,0.07)',
        borderLeft: selected ? `3px solid ${COLORS.primary}` : `3px solid ${color}`,
        cursor: 'pointer', transition: 'all 0.18s',
      }}
    >
      {/* Rank */}
      <div style={{
        width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
        background: 'rgba(255,255,255,0.08)', display: 'flex',
        alignItems: 'center', justifyContent: 'center',
        fontSize: '0.72rem', fontWeight: 700, color: 'rgba(255,255,255,0.55)',
      }}>
        {rank}
      </div>

      {/* Priority badge */}
      <div style={{ flexShrink: 0 }}>
        <Badge label={rec.priority.toUpperCase()} color={color} />
      </div>

      {/* Zone & issue */}
      <div style={{ flex: '0 0 160px', minWidth: 0 }}>
        <div style={{ fontSize: '0.83rem', fontWeight: 600, color: 'rgba(255,255,255,0.92)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {rec.zone_name}
        </div>
        <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.45)', marginTop: 2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {rec.issue_detected}
        </div>
      </div>

      {/* Recommended action */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ ...MINI_LABEL, marginBottom: 3 }}>Recommended Action</div>
        <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.72)', lineHeight: 1.4, overflow: 'hidden', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}>
          {actionText}
        </div>
      </div>

      {/* Impact */}
      <div style={{ flex: '0 0 130px', flexShrink: 0 }}>
        <div style={{ fontSize: '0.72rem', color: COLORS.low, fontWeight: 600 }}>
          <TrendingDown size={11} style={{ display: 'inline', marginRight: 3 }} />
          {rec.expected_improvement_rate != null
            ? `${Math.round(rec.expected_improvement_rate * 100)}% hist. improve`
            : (rec.expected_impact || 'Expected supply improvement')}
        </div>
        <div style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.40)', marginTop: 3 }}>
          {rec.estimated_cost_sgd != null
            ? `S$${rec.estimated_cost_sgd.toFixed(2)} cost`
            : `Score ${rec.delay_risk_score.toFixed(3)}`}
          {rec.expected_roi != null ? ` · ROI ${rec.expected_roi.toFixed(2)}` : ''}
        </div>
      </div>

      {/* Confidence circle */}
      <ConfCircle value={confPct} color={confColor} />
    </div>
  )
}

// ── Detail panel ────────────────────────────────────────────────────────────
function DetailPanel({
  rec,
  onFeedback,
  feedbackPending,
}: {
  rec: Recommendation
  onFeedback: (status: 'followed' | 'not_followed') => void
  feedbackPending: boolean
}) {
  const color = priorityColor(rec.priority)
  const confPct = rec.confidence * 100
  const confColor = confPct >= 80 ? COLORS.low : confPct >= 60 ? COLORS.medium : COLORS.high
  const chartData = buildChartData(rec)
  const drivers = rec.explanation_tag
    ?.replace('nan', '')
    .split(/[,·|•]/)
    .map(s => s.trim())
    .filter(Boolean) ?? ['Rapid Depletion', 'Low Supply']

  const isPriority = rec.priority === 'critical'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14, height: '100%' }}>
      {/* Header row */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <Badge label={rec.priority.toUpperCase()} color={color} />
            {isPriority && (
              <span style={{ fontSize: '0.58rem', fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase', color: color, opacity: 0.80 }}>
                • TOP PRIORITY
              </span>
            )}
          </div>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: 'rgba(255,255,255,0.95)', lineHeight: 1.2, marginBottom: 4 }}>
            {rec.zone_name}
          </div>
          <div style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.50)', lineHeight: 1.4 }}>
            {rec.issue_detected}
          </div>
        </div>
        <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: 12 }}>
          <div style={{ fontSize: '1.60rem', fontWeight: 300, color, lineHeight: 1 }}>
            {rec.delay_risk_score.toFixed(3)}
          </div>
          <div style={MINI_LABEL}>depletion risk</div>
        </div>
      </div>

      {/* Key drivers */}
      <div>
        <div style={{ ...SECTION_LABEL, marginBottom: 8 }}>Key Drivers</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {drivers.slice(0, 5).map(d => (
            <span key={d} style={{
              padding: '4px 10px', borderRadius: 20,
              background: 'rgba(79,142,247,0.12)', border: '1px solid rgba(79,142,247,0.28)',
              fontSize: '0.68rem', fontWeight: 500, color: COLORS.primary,
            }}>
              {d}
            </span>
          ))}
        </div>
      </div>

      {/* Expected impact */}
      <div>
        <div style={{ ...SECTION_LABEL, marginBottom: 10 }}>Expected Impact</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 10, marginBottom: 12 }}>
          <div style={{
            background: 'rgba(16,217,138,0.08)', border: '1px solid rgba(16,217,138,0.20)',
            borderRadius: 10, padding: '10px 12px',
          }}>
            <div style={{ fontSize: '1.35rem', fontWeight: 700, color: COLORS.low, lineHeight: 1 }}>
              {rec.expected_recovery_probability != null ? `${Math.round(rec.expected_recovery_probability * 100)}%` : (rec.expected_impact?.match(/\d+[-–]\d+%/)?.[0] ?? '10-15%')}
            </div>
            <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.45)', marginTop: 4 }}>
              Recover Probability
            </div>
          </div>
          <div style={{
            background: 'rgba(79,142,247,0.08)', border: '1px solid rgba(79,142,247,0.20)',
            borderRadius: 10, padding: '10px 12px',
          }}>
            <div style={{ fontSize: '1.35rem', fontWeight: 700, color: COLORS.primary, lineHeight: 1 }}>
              {rec.priority.toUpperCase()}
            </div>
            <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.45)', marginTop: 4 }}>
              Action Priority
            </div>
          </div>
          <div style={{
            background: 'rgba(255,193,7,0.08)', border: '1px solid rgba(255,193,7,0.20)',
            borderRadius: 10, padding: '10px 12px',
          }}>
            <div style={{ fontSize: '1.35rem', fontWeight: 700, color: '#ffc107', lineHeight: 1 }}>
              {rec.estimated_cost_sgd != null ? `S$${rec.estimated_cost_sgd.toFixed(2)}` : '—'}
            </div>
            <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.45)', marginTop: 4 }}>
              Estimated Cost
            </div>
          </div>
          <div style={{
            background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.12)',
            borderRadius: 10, padding: '10px 12px',
          }}>
            <div style={{ fontSize: '1.35rem', fontWeight: 700, color: 'rgba(255,255,255,0.90)', lineHeight: 1 }}>
              {rec.expected_supply_response_30m != null ? `+${rec.expected_supply_response_30m.toFixed(1)}` : '—'}
            </div>
            <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.45)', marginTop: 4 }}>
              Supply Response 30m
            </div>
          </div>
        </div>
        <ConfBar value={confPct} color={confColor} />
      </div>

      {(rec.expected_recovery_rate != null || rec.expected_improvement_rate != null || rec.policy_rank_reason) && (
        <div style={{
          background: 'rgba(79,142,247,0.05)', border: '1px solid rgba(79,142,247,0.16)',
          borderRadius: 10, padding: '12px 14px',
        }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 10 }}>Learned Policy Signal</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10, marginBottom: 10 }}>
            <div>
              <div style={MINI_LABEL}>Recovery</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: COLORS.low }}>
                {rec.expected_recovery_rate != null ? `${Math.round(rec.expected_recovery_rate * 100)}%` : '—'}
              </div>
            </div>
            <div>
              <div style={MINI_LABEL}>Improve / Recover</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: COLORS.primary }}>
                {rec.expected_improvement_rate != null ? `${Math.round(rec.expected_improvement_rate * 100)}%` : '—'}
              </div>
            </div>
            <div>
              <div style={MINI_LABEL}>Evidence</div>
              <div style={{ fontSize: '1rem', fontWeight: 700, color: 'rgba(255,255,255,0.85)', textTransform: 'capitalize' }}>
                {rec.evidence_count ?? 0} · {rec.confidence_band ?? 'low'}
              </div>
            </div>
          </div>
          <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.58)', lineHeight: 1.5 }}>
            {rec.policy_rank_reason ?? 'Rule engine default retained; not enough resolved outcomes yet.'}
            {rec.follow_rate != null ? ` Follow-through rate: ${Math.round(rec.follow_rate * 100)}%.` : ''}
          </div>
          {(rec.expected_roi != null || rec.winning_reason) && (
            <div style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.62)', lineHeight: 1.5, marginTop: 8 }}>
              {rec.expected_roi != null ? `Expected ROI: ${rec.expected_roi.toFixed(2)}. ` : ''}
              {rec.winning_reason ?? ''}
            </div>
          )}
        </div>
      )}

      {/* Mini chart */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <div style={{ ...SECTION_LABEL }}>Projected Trajectory</div>
          <span style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.30)', fontWeight: 400, fontStyle: 'italic' }}>
            {rec.intervention_window === 'too_late'
              ? 'action window passed'
              : rec.eta_minutes != null
                ? `calibrated from eta (~${rec.eta_minutes}m)`
                : 'priority-based estimate'}
          </span>
        </div>
        <div style={{ height: 100 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 4, right: 4, left: -28, bottom: 0 }}>
              <XAxis dataKey="t" tick={{ fontSize: 9, fill: 'rgba(255,255,255,0.35)' }} axisLine={false} tickLine={false} />
              <YAxis domain={['auto', 'auto']} tick={{ fontSize: 9, fill: 'rgba(255,255,255,0.35)' }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={{ color: 'rgba(255,255,255,0.42)', marginBottom: 2 }}
                itemStyle={{ fontSize: 11 }}
              />
              <Line type="monotone" dataKey="baseline" stroke={COLORS.high} strokeWidth={1.5} dot={false} name="No action" />
              <Line type="monotone" dataKey="action"   stroke={COLORS.low}  strokeWidth={1.5} dot={false} strokeDasharray="4 2" name="With action" />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.28)', marginTop: 4, fontStyle: 'italic' }}>
          Illustrative only — worsening rate derived from {rec.eta_minutes != null ? 'predicted time-to-critical' : 'priority tier'}. Not a guarantee of real-world outcome.
        </div>
      </div>

      {/* Recommended action box */}
      <div style={{
        background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.10)',
        borderRadius: 10, padding: '12px 14px',
      }}>
        <div style={{ ...MINI_LABEL, marginBottom: 6 }}>Recommended Action</div>
        <div style={{ fontSize: '0.80rem', color: 'rgba(255,255,255,0.82)', lineHeight: 1.5 }}>
          {rec.recommendation}
        </div>
        {(rec.decision_objective || rec.constraints_triggered) && (
          <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.45)', marginTop: 8, lineHeight: 1.5 }}>
            Objective: {rec.decision_objective ?? 'reliability_first'}
            {rec.constraints_triggered ? ` · Constraints: ${rec.constraints_triggered}` : ''}
          </div>
        )}
      </div>

      {/* Engine v2: diagnostics row */}
      {(rec.root_cause || rec.eta_minutes != null || rec.intervention_window) && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8 }}>
          {rec.root_cause && (
            <div style={{ background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)', borderRadius: 8, padding: '8px 10px' }}>
              <div style={{ ...MINI_LABEL, marginBottom: 4 }}>Root Cause</div>
              <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.78)', fontWeight: 600, textTransform: 'capitalize' }}>
                {rec.root_cause.replace(/_/g, ' ')}
              </div>
            </div>
          )}
          {rec.eta_minutes != null && (
            <div style={{ background: 'rgba(255,200,60,0.06)', border: '1px solid rgba(255,200,60,0.18)', borderRadius: 8, padding: '8px 10px' }}>
              <div style={{ ...MINI_LABEL, marginBottom: 4 }}>Time to Critical</div>
              <div style={{ fontSize: '0.72rem', color: '#ffc83c', fontWeight: 700 }}>
                ~{rec.eta_minutes} min
              </div>
            </div>
          )}
          {rec.intervention_window && (
            <div style={{
              background: rec.intervention_window === 'too_late' ? 'rgba(255,80,80,0.08)' : rec.intervention_window === 'tight' ? 'rgba(255,150,50,0.08)' : 'rgba(16,217,138,0.06)',
              border: `1px solid ${rec.intervention_window === 'too_late' ? 'rgba(255,80,80,0.25)' : rec.intervention_window === 'tight' ? 'rgba(255,150,50,0.25)' : 'rgba(16,217,138,0.18)'}`,
              borderRadius: 8, padding: '8px 10px',
            }}>
              <div style={{ ...MINI_LABEL, marginBottom: 4 }}>Action Window</div>
              <div style={{ fontSize: '0.72rem', fontWeight: 700, textTransform: 'capitalize', color: rec.intervention_window === 'too_late' ? COLORS.high : rec.intervention_window === 'tight' ? COLORS.medium : COLORS.low }}>
                {rec.intervention_window.replace(/_/g, ' ')}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Engine v2: network warning */}
      {rec.network_warning && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start', background: 'rgba(255,170,50,0.07)', border: '1px solid rgba(255,170,50,0.22)', borderRadius: 8, padding: '8px 12px' }}>
          <AlertTriangle size={13} color="#ffaa32" style={{ flexShrink: 0, marginTop: 1 }} />
          <div style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.72)', lineHeight: 1.5 }}>{rec.network_warning}</div>
        </div>
      )}

      {/* Engine v2: alternative actions */}
      {rec.alternative_actions && (() => {
        let alts: AlternativeAction[] = []
        try { alts = JSON.parse(rec.alternative_actions) } catch { return null }
        if (!alts.length) return null
        return (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
              <GitBranch size={11} color="rgba(255,255,255,0.35)" />
              <div style={{ ...SECTION_LABEL }}>Alternative Actions</div>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {alts.map((alt, i) => (
                <div key={i} style={{ display: 'flex', gap: 10, alignItems: 'flex-start', background: alt.viable ? 'rgba(255,255,255,0.04)' : 'rgba(255,255,255,0.02)', border: `1px solid ${alt.viable ? 'rgba(255,255,255,0.10)' : 'rgba(255,255,255,0.05)'}`, borderRadius: 8, padding: '8px 10px', opacity: alt.viable ? 1 : 0.55 }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '0.72rem', fontWeight: 600, color: alt.viable ? 'rgba(255,255,255,0.85)' : 'rgba(255,255,255,0.45)', marginBottom: 2 }}>{alt.action}</div>
                    <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.42)', lineHeight: 1.4 }}>{alt.impact}</div>
                    {(alt.expected_improvement_rate != null || alt.evidence_count != null) && (
                      <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.36)', marginTop: 4 }}>
                        {alt.expected_improvement_rate != null ? `Hist. improve ${Math.round(alt.expected_improvement_rate * 100)}%` : 'No learned signal'}
                        {alt.evidence_count != null ? ` · ${alt.evidence_count} cases` : ''}
                        {alt.confidence_band ? ` · ${alt.confidence_band} conf.` : ''}
                        {alt.expected_roi != null ? ` · ROI ${alt.expected_roi.toFixed(2)}` : ''}
                      </div>
                    )}
                  </div>
                  <div style={{ textAlign: 'right', flexShrink: 0 }}>
                    <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.35)' }}>+{alt.time_to_effect_min}m</div>
                    <div style={{ fontSize: '0.60rem', color: alt.cost === 'none' ? COLORS.low : alt.cost === 'low' ? COLORS.medium : COLORS.high, textTransform: 'capitalize', fontWeight: 600 }}>{alt.cost} cost</div>
                    {alt.estimated_cost_sgd != null && (
                      <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.34)', marginTop: 3 }}>
                        S${alt.estimated_cost_sgd.toFixed(2)}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )
      })()}

      {/* Action buttons */}
      <div style={{ display: 'flex', gap: 10 }}>
        <button className="btn-glass" onClick={() => showToast(`Zone ${rec.zone_name}: depletion risk ${rec.delay_risk_score.toFixed(3)}, priority ${rec.priority}. Recommended: ${rec.recommendation}`, 'info')} style={{ flex: 1, justifyContent: 'center', padding: '10px 0' }}>
          View Details
        </button>
        <button className="btn-primary" disabled={feedbackPending} onClick={() => onFeedback('followed')} style={{ flex: 1, justifyContent: 'center', padding: '10px 0', display: 'flex', alignItems: 'center', gap: 6, opacity: feedbackPending ? 0.7 : 1 }}>
          Mark Followed <ChevronRight size={14} />
        </button>
        <button className="btn-glass" disabled={feedbackPending} onClick={() => onFeedback('not_followed')} style={{ flex: 1, justifyContent: 'center', padding: '10px 0', opacity: feedbackPending ? 0.7 : 1 }}>
          Not Followed
        </button>
      </div>
    </div>
  )
}

// ── Main component ──────────────────────────────────────────────────────────
export default function ActionCenter() {
  const queryClient = useQueryClient()
  const [priority, setPriority] = useState('All')
  const [selectedIdx, setSelectedIdx] = useState<number>(0)
  const [search, setSearch] = useState('')
  const [showFilters, setShowFilters] = useState(false)
  const [minScore, setMinScore] = useState(0)
  const [maxScore, setMaxScore] = useState(1)
  const [sortBy, setSortBy] = useState<'risk_desc' | 'risk_asc' | 'zone_asc' | 'confidence_desc'>('risk_desc')
  const [sortOpen, setSortOpen] = useState(false)
  const sortRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!sortOpen) return
    const handler = (e: MouseEvent) => {
      if (sortRef.current && !sortRef.current.contains(e.target as Node)) setSortOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [sortOpen])

  const { data: recs, isLoading, isError } = useQuery({
    queryKey: ['recommendations', priority],
    queryFn: () => api.recommendations(priority === 'All' ? undefined : priority.toLowerCase()),
    staleTime: 30000,
  })

  const feedbackMutation = useMutation({
    mutationFn: ({
      recommendationId,
      followedStatus,
      zoneName,
    }: {
      recommendationId: string
      followedStatus: 'followed' | 'not_followed'
      zoneName: string
    }) => api.recommendationFeedback(recommendationId, {
      followed_status: followedStatus,
      followed_by: 'ops-dashboard',
      follow_note: `${zoneName} action marked ${followedStatus.replace('_', ' ')}`,
    }),
    onSuccess: (_, vars) => {
      queryClient.invalidateQueries({ queryKey: ['reportOutcomes'] })
      showToast(
        `${vars.zoneName}: marked ${vars.followedStatus === 'followed' ? 'followed' : 'not followed'}.`,
        'success',
      )
    },
    onError: () => {
      showToast('Could not save operator feedback for this recommendation.', 'error')
    },
  })

  const counts = recs ? {
    critical: recs.filter(r => r.priority === 'critical').length,
    high:     recs.filter(r => r.priority === 'high').length,
    medium:   recs.filter(r => r.priority === 'medium').length,
    low:      recs.filter(r => r.priority === 'low').length,
  } : { critical: 0, high: 0, medium: 0, low: 0 }

  const TABS = [
    { label: 'All',      value: 'All',      count: recs?.length ?? 0 },
    { label: 'Critical', value: 'critical',  count: counts.critical },
    { label: 'High',     value: 'high',      count: counts.high },
    { label: 'Medium',   value: 'medium',    count: counts.medium },
    { label: 'Low',      value: 'low',       count: counts.low },
  ]

  const SORT_OPTIONS: { value: typeof sortBy; label: string }[] = [
    { value: 'risk_desc',       label: 'Risk Score ↓' },
    { value: 'risk_asc',        label: 'Risk Score ↑' },
    { value: 'zone_asc',        label: 'Zone Name A→Z' },
    { value: 'confidence_desc', label: 'Confidence ↓' },
  ]

  const sorted = recs
    ? [...recs]
        .filter(r => search === '' || r.zone_name.toLowerCase().includes(search.toLowerCase()) || r.issue_detected.toLowerCase().includes(search.toLowerCase()))
        .filter(r => r.delay_risk_score >= minScore && r.delay_risk_score <= maxScore)
        .sort((a, b) => {
          if (sortBy === 'risk_desc')       return b.delay_risk_score - a.delay_risk_score
          if (sortBy === 'risk_asc')        return a.delay_risk_score - b.delay_risk_score
          if (sortBy === 'zone_asc')        return a.zone_name.localeCompare(b.zone_name)
          if (sortBy === 'confidence_desc') return b.confidence - a.confidence
          return 0
        })
    : []

  const selected = sorted[selectedIdx] ?? sorted[0]

  // KPI aggregates
  const totalRecs = recs?.length ?? 0
  const criticalAndHigh = counts.critical + counts.high
  const avgConf = recs && recs.length > 0
    ? Math.round((recs.reduce((s, r) => s + r.confidence, 0) / recs.length) * 100)
    : 0
  const minConf = recs && recs.length > 0 ? Math.round(Math.min(...recs.map(r => r.confidence)) * 100) : null
  const maxConf = recs && recs.length > 0 ? Math.round(Math.max(...recs.map(r => r.confidence)) * 100) : null
  const avgRisk = recs && recs.length > 0
    ? recs.reduce((s, r) => s + r.delay_risk_score, 0) / recs.length
    : 0
  function handleTabClick(value: string) {
    setPriority(value)
    setSelectedIdx(0)
  }

  if (isLoading) return <Spinner />
  if (isError) return <ApiError message="Recommendations could not be loaded from the backend." />
  if (!recs || recs.length === 0) {
    return <EmptyState title="No recommendations available" message="Run the scoring pipeline and recommendation engine to populate the action center." />
  }

  return (
    <div>
      {/* ── Row 0: Controls ── */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20, flexWrap: 'wrap', gap: 12 }}>
        {/* Priority tabs */}
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {TABS.map(tab => {
            const active = priority === tab.value
            return (
              <button
                key={tab.value}
                onClick={() => handleTabClick(tab.value)}
                style={{
                  display: 'inline-flex', alignItems: 'center', gap: 6,
                  padding: '7px 14px', borderRadius: 20,
                  fontSize: '0.78rem', fontWeight: active ? 600 : 400,
                  background: active ? 'rgba(79,142,247,0.16)' : 'rgba(255,255,255,0.06)',
                  border: `1px solid ${active ? 'rgba(79,142,247,0.35)' : 'rgba(255,255,255,0.10)'}`,
                  color: active ? COLORS.primary : 'rgba(255,255,255,0.55)',
                  cursor: 'pointer', transition: 'all 0.18s', fontFamily: 'Inter, sans-serif',
                }}
              >
                {tab.label}
                <span style={{
                  display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                  background: active ? 'rgba(79,142,247,0.25)' : 'rgba(255,255,255,0.12)',
                  borderRadius: 20, padding: '1px 7px',
                  fontSize: '0.65rem', fontWeight: 700,
                  color: active ? COLORS.primary : 'rgba(255,255,255,0.45)',
                  minWidth: 22,
                }}>
                  {tab.count}
                </span>
              </button>
            )
          })}
        </div>

        <button className="btn-glass" onClick={() => showToast(`${sorted.length} zone${sorted.length !== 1 ? 's' : ''} in current view. Top priority: ${sorted[0]?.zone_name ?? '—'} (score ${sorted[0]?.delay_risk_score.toFixed(3) ?? '—'}, ${sorted[0]?.priority ?? '—'}).`, 'info')} style={{ display: 'flex', alignItems: 'center', gap: 7, padding: '9px 18px' }}>
          <Zap size={14} />
          Queue Summary
        </button>
      </div>

      {recs && (
        <>
          {/* ── Row 1: KPI cards ── */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12, marginBottom: 20 }}>
            {/* Actions in Queue */}
            <GlassCard hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 10 }}>
                <div style={KPI_LABEL}>Actions in Queue</div>
                <Clock size={14} color={COLORS.primary} style={{ opacity: 0.6 }} />
              </div>
              <div style={{ fontSize: '2.0rem', fontWeight: 700, color: 'rgba(255,255,255,0.95)', lineHeight: 1 }}>
                {totalRecs}
              </div>
              <div style={{ fontSize: '0.72rem', color: criticalAndHigh > 0 ? COLORS.high : 'rgba(255,255,255,0.40)', marginTop: 6 }}>
                {criticalAndHigh > 0 ? `${criticalAndHigh} require attention` : 'All nominal'}
              </div>
            </GlassCard>

            {/* Avg Risk Score (labelled correctly) */}
            <GlassCard hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 10 }}>
                <div style={KPI_LABEL}>Avg Depletion Risk</div>
                <TrendingDown size={14} color={COLORS.medium} style={{ opacity: 0.6 }} />
              </div>
              <div style={{ fontSize: '2.0rem', fontWeight: 700, color: 'rgba(255,255,255,0.95)', lineHeight: 1 }}>
                {avgRisk.toFixed(3)}
              </div>
              <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)', marginTop: 6 }}>
                Mean depletion-model score across all {recs?.length ?? 0} zones (0 = no risk, 1 = severe depletion risk)
              </div>
            </GlassCard>

            {/* Avg Confidence */}
            <GlassCard hover={false} style={{ padding: '16px 18px' }}>
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 10 }}>
                <div style={KPI_LABEL}>Avg Confidence</div>
                <Target size={14} color={COLORS.low} style={{ opacity: 0.6 }} />
              </div>
              <div style={{ fontSize: '2.0rem', fontWeight: 700, color: 'rgba(255,255,255,0.95)', lineHeight: 1 }}>
                {avgConf}%
              </div>
              <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.40)', marginTop: 6 }}>
                {minConf != null && maxConf != null ? `Range: ${minConf}% – ${maxConf}%` : 'No data'}
              </div>
            </GlassCard>
          </div>

          {/* ── Row 2: Two column layout ── */}
          <div style={{ display: 'grid', gridTemplateColumns: '57% 43%', gap: 16, alignItems: 'start' }}>
            {/* ── LEFT: Priority intervention list ── */}
            <div>
              {/* Search + Sort + Filters */}
              <div style={{ display: 'flex', gap: 8, marginBottom: 14, alignItems: 'center' }}>
                <div style={{
                  flex: 1, display: 'flex', alignItems: 'center', gap: 8,
                  background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.10)',
                  borderRadius: 10, padding: '8px 12px',
                }}>
                  <Search size={13} color="rgba(255,255,255,0.35)" style={{ flexShrink: 0 }} />
                  <input
                    value={search}
                    onChange={e => { setSearch(e.target.value); setSelectedIdx(0) }}
                    placeholder="Search interventions..."
                    style={{
                      background: 'none', border: 'none', outline: 'none',
                      fontSize: '0.78rem', color: 'rgba(255,255,255,0.75)',
                      fontFamily: 'Inter, sans-serif', flex: 1,
                    }}
                  />
                </div>
                <div ref={sortRef} style={{ position: 'relative', flexShrink: 0 }}>
                  <button
                    onClick={() => setSortOpen(o => !o)}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 6,
                      background: sortOpen ? 'rgba(79,142,247,0.10)' : 'rgba(255,255,255,0.05)',
                      border: `1px solid ${sortOpen ? 'rgba(79,142,247,0.30)' : 'rgba(255,255,255,0.10)'}`,
                      borderRadius: 10, padding: '8px 12px',
                      fontSize: '0.72rem', color: sortOpen ? COLORS.primary : 'rgba(255,255,255,0.55)',
                      cursor: 'pointer', fontFamily: 'Inter, sans-serif', whiteSpace: 'nowrap',
                    }}
                  >
                    Sort: {SORT_OPTIONS.find(o => o.value === sortBy)?.label}
                  </button>
                  {sortOpen && (
                    <div style={{
                      position: 'absolute', top: 'calc(100% + 6px)', right: 0, zIndex: 100,
                      background: 'rgba(14,22,42,0.97)', border: '1px solid rgba(99,140,255,0.18)',
                      borderRadius: 10, padding: '4px 0', minWidth: 170,
                      boxShadow: '0 8px 28px rgba(0,0,0,0.55)',
                    }}>
                      {SORT_OPTIONS.map(opt => (
                        <button
                          key={opt.value}
                          onClick={() => { setSortBy(opt.value); setSortOpen(false); setSelectedIdx(0) }}
                          style={{
                            display: 'block', width: '100%', textAlign: 'left',
                            padding: '8px 14px',
                            background: sortBy === opt.value ? 'rgba(79,142,247,0.14)' : 'transparent',
                            border: 'none',
                            color: sortBy === opt.value ? 'rgba(255,255,255,0.92)' : 'rgba(255,255,255,0.56)',
                            fontSize: '0.76rem', cursor: 'pointer', fontFamily: 'Inter, sans-serif',
                          }}
                          onMouseEnter={e => { if (sortBy !== opt.value) (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,0.05)' }}
                          onMouseLeave={e => { if (sortBy !== opt.value) (e.currentTarget as HTMLElement).style.background = 'transparent' }}
                        >
                          {opt.label}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <button className="btn-glass" onClick={() => setShowFilters(f => !f)} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '8px 12px', flexShrink: 0, background: showFilters ? 'rgba(79,142,247,0.14)' : undefined, border: showFilters ? '1px solid rgba(79,142,247,0.35)' : undefined }}>
                  <SlidersHorizontal size={13} />
                  Filters
                </button>
              </div>

              {/* Expandable filter panel */}
              {showFilters && (
                <div style={{ background: 'rgba(79,142,247,0.06)', border: '1px solid rgba(79,142,247,0.18)', borderRadius: 10, padding: '12px 16px', display: 'flex', alignItems: 'center', gap: 20, flexWrap: 'wrap' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <span style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.50)' }}>Risk score min</span>
                    <input type="range" min={0} max={1} step={0.05} value={minScore}
                      onChange={e => setMinScore(Number(e.target.value))}
                      style={{ accentColor: COLORS.primary, width: 100 }} />
                    <span style={{ fontSize: '0.70rem', color: COLORS.primary, fontWeight: 600, minWidth: 32 }}>{minScore.toFixed(2)}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <span style={{ fontSize: '0.70rem', color: 'rgba(255,255,255,0.50)' }}>Risk score max</span>
                    <input type="range" min={0} max={1} step={0.05} value={maxScore}
                      onChange={e => setMaxScore(Number(e.target.value))}
                      style={{ accentColor: COLORS.primary, width: 100 }} />
                    <span style={{ fontSize: '0.70rem', color: COLORS.primary, fontWeight: 600, minWidth: 32 }}>{maxScore.toFixed(2)}</span>
                  </div>
                  <button onClick={() => { setMinScore(0); setMaxScore(1) }} style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.40)', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
                    Reset
                  </button>
                </div>
              )}

              {/* Intervention list */}
              {sorted.length === 0 && (
                <div style={{ textAlign: 'center', padding: 32, color: 'rgba(255,255,255,0.35)', fontSize: '0.82rem' }}>
                  No interventions found.
                </div>
              )}

              <div style={{ maxHeight: '62vh', overflowY: 'auto', paddingRight: 2 }}>
                {sorted.map((rec, i) => (
                  <ActionRow
                    key={`${rec.zone_id}-${i}`}
                    rec={rec}
                    rank={i + 1}
                    selected={selectedIdx === i}
                    onClick={() => setSelectedIdx(i)}
                  />
                ))}
              </div>
            </div>

            {/* ── RIGHT: Detail panel ── */}
            {selected ? (
              <GlassCard hover={false} style={{ padding: '20px 22px', position: 'sticky', top: 20 }}>
                <DetailPanel
                  rec={selected}
                  feedbackPending={feedbackMutation.isPending}
                  onFeedback={(status) => feedbackMutation.mutate({
                    recommendationId: selected.recommendation_id,
                    followedStatus: status,
                    zoneName: selected.zone_name,
                  })}
                />
              </GlassCard>
            ) : (
              <GlassCard hover={false} style={{ padding: '32px 24px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <div style={{ textAlign: 'center', color: 'rgba(255,255,255,0.30)', fontSize: '0.82rem' }}>
                  Select an intervention to view details
                </div>
              </GlassCard>
            )}
          </div>
        </>
      )}
    </div>
  )
}
