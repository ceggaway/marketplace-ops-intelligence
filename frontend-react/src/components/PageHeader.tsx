import type { LucideIcon } from 'lucide-react'
import Badge from './Badge'

interface Props {
  Icon: LucideIcon
  title: string
  subtitle: string
  badgeLabel: string
  badgeColor: string
  right?: React.ReactNode
}

export default function PageHeader({ Icon, title, subtitle, badgeLabel, badgeColor, right }: Props) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      marginBottom: 28, paddingBottom: 20, borderBottom: '1px solid rgba(255,255,255,0.08)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        {/* Avalon circle icon */}
        <div style={{
          width: 38, height: 38, borderRadius: '50%', flexShrink: 0,
          background: 'linear-gradient(135deg, rgba(255,255,255,0.14) 0%, rgba(255,255,255,0.06) 100%)',
          border: '1px solid rgba(255,255,255,0.18)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          backdropFilter: 'blur(10px)',
        }}>
          <Icon size={17} color="rgba(255,255,255,0.80)" strokeWidth={1.75} />
        </div>
        <div>
          <div style={{ fontSize: '1.05rem', fontWeight: 400, color: 'rgba(255,255,255,0.92)', letterSpacing: '-0.01em' }}>
            {title}
          </div>
          <div style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.40)', marginTop: 2, fontWeight: 300 }}>
            {subtitle}
          </div>
        </div>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        {right}
        <Badge label={badgeLabel} color={badgeColor} />
      </div>
    </div>
  )
}
