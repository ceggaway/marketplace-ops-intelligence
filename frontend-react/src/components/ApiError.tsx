import GlassCard from './GlassCard'
import { COLORS } from '../lib/utils'

interface Props { message?: string }

export default function ApiError({ message = 'Start the backend with make api and confirm it is serving /api/v1.' }: Props) {
  return (
    <GlassCard hover={false} style={{ padding: 24, borderLeft: `3px solid ${COLORS.high}` }}>
      <div style={{ color: COLORS.high, fontWeight: 600, marginBottom: 6, fontSize: '0.92rem' }}>
        API unavailable
      </div>
      <p style={{ color: 'rgba(255,255,255,0.70)', margin: 0, fontSize: '0.82rem', lineHeight: 1.6 }}>
        {message}
      </p>
    </GlassCard>
  )
}
