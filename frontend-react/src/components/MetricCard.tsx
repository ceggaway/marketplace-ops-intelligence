import type { LucideIcon } from 'lucide-react'
import GlassCard from './GlassCard'

interface Props {
  label: string; value: string; sub?: string; color: string; Icon: LucideIcon
}

export default function MetricCard({ label, value, sub, color, Icon }: Props) {
  return (
    <GlassCard accentColor={color} style={{ padding: '15px 17px 13px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 10 }}>
        {/* Circle icon — Avalon style */}
        <div style={{
          width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
          background: 'linear-gradient(135deg, rgba(255,255,255,0.14) 0%, rgba(255,255,255,0.06) 100%)',
          border: '1px solid rgba(255,255,255,0.16)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <Icon size={13} color={color} strokeWidth={1.75} />
        </div>
        <span style={{ fontSize: '0.57rem', fontWeight: 500, letterSpacing: '0.11em', textTransform: 'uppercase', color: 'rgba(255,255,255,0.42)' }}>
          {label}
        </span>
      </div>
      <div style={{ fontSize: '1.55rem', fontWeight: 400, color, lineHeight: 1, letterSpacing: '-0.02em' }}>
        {value}
      </div>
      {sub && <div style={{ fontSize: '0.63rem', color: 'rgba(255,255,255,0.35)', marginTop: 5, fontWeight: 300 }}>{sub}</div>}
    </GlassCard>
  )
}
