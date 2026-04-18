import type { LucideIcon } from 'lucide-react'

interface Props {
  label: string
  value: string | number
  color: string
  Icon: LucideIcon
}

export default function StatChip({ label, value, color, Icon }: Props) {
  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(255,255,255,0.10) 0%, rgba(255,255,255,0.05) 100%)',
      border: '1px solid rgba(255,255,255,0.14)',
      borderTop: `2px solid ${color}`,
      borderRadius: 15,
      padding: '12px 14px',
      textAlign: 'center',
      backdropFilter: 'blur(20px)',
      boxShadow: '0 8px 32px rgba(0,0,0,0.50), inset 0 1px 0 rgba(255,255,255,0.10)',
    }}>
      <div style={{ fontSize: '1.40rem', fontWeight: 400, color, lineHeight: 1.1 }}>{value}</div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 5, marginTop: 5 }}>
        <Icon size={12} color={color} strokeWidth={1.75} />
        <span style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.42)', textTransform: 'uppercase', letterSpacing: '0.09em' }}>
          {label}
        </span>
      </div>
    </div>
  )
}
