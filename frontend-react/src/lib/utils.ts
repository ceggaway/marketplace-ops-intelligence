import { clsx, type ClassValue } from 'clsx'

export function cn(...inputs: ClassValue[]) { return clsx(inputs) }

// ── Semantic colour tokens — dark navy palette ─────────────────────────────
export const COLORS = {
  high:    '#FF4D6D',
  medium:  '#F59E0B',
  low:     '#10D98A',
  primary: '#4F8EF7',
  purple:  '#A78BFA',
  brand:   '#4F8EF7',
}

export const RISK_COLOR: Record<string, string> = {
  high: COLORS.high, medium: COLORS.medium, low: COLORS.low,
}
export const PRIORITY_COLOR: Record<string, string> = {
  critical: COLORS.high, high: COLORS.medium, medium: COLORS.primary, low: COLORS.low,
}

export function riskColor(level: string)  { return RISK_COLOR[level]    ?? COLORS.primary }
export function priorityColor(p: string)  { return PRIORITY_COLOR[p]    ?? COLORS.primary }

// ── Formatting ─────────────────────────────────────────────────────────────
export function formatPct(v: number) { return `${(v * 100).toFixed(1)}%` }
export function formatNum(v: number) { return v.toLocaleString() }

export function fmtDate(s: string) {
  try { return new Date(s).toLocaleString('en-SG', { dateStyle: 'short', timeStyle: 'short' }) }
  catch { return s }
}

export function fmtTime(s: string) {
  try { return new Date(s).toLocaleTimeString('en-SG', { hour: '2-digit', minute: '2-digit' }) }
  catch { return s }
}

// ── Shared style tokens ────────────────────────────────────────────────────
export const TOOLTIP_STYLE = {
  background:   'rgba(5,10,28,0.96)',
  border:       '1px solid rgba(99,140,255,0.20)',
  borderRadius: 8,
  fontSize:     11,
  color:        'rgba(255,255,255,0.85)',
}
export const TOOLTIP_LABEL_STYLE = { color: 'rgba(255,255,255,0.42)', marginBottom: 2 }

export const SECTION_LABEL: React.CSSProperties = {
  fontSize:      '0.58rem',
  fontWeight:    600,
  letterSpacing: '0.12em',
  textTransform: 'uppercase',
  color:         'rgba(255,255,255,0.32)',
  marginBottom:  10,
}
