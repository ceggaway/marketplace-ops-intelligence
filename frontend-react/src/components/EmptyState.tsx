import GlassCard from './GlassCard'

interface Props {
  title: string
  message: string
}

export default function EmptyState({ title, message }: Props) {
  return (
    <GlassCard hover={false} style={{ padding: 28, textAlign: 'center' }}>
      <div style={{ fontSize: '0.98rem', fontWeight: 600, color: 'rgba(255,255,255,0.90)', marginBottom: 8 }}>
        {title}
      </div>
      <div style={{ fontSize: '0.78rem', color: 'rgba(255,255,255,0.48)', lineHeight: 1.6, maxWidth: 460, margin: '0 auto' }}>
        {message}
      </div>
    </GlassCard>
  )
}
