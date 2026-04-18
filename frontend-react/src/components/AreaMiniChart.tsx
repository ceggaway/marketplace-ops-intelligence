import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { fmtTime, TOOLTIP_STYLE, TOOLTIP_LABEL_STYLE } from '../lib/utils'
import type { TrendPoint } from '../lib/api'
import GlassCard from './GlassCard'

interface Props {
  data: TrendPoint[]
  color: string
  title: string
  gradientId: string   // explicit — avoids SVG ID collision when same color appears in multiple charts
  height?: number
  yLabel?: string
}

export default function AreaMiniChart({ data, color, title, gradientId, height = 150, yLabel }: Props) {
  const parsed = data.map(d => ({ value: d.value, t: fmtTime(d.timestamp) }))
  return (
    <GlassCard accentColor={color} style={{ padding: '14px 14px 6px' }}>
      <div style={{ fontSize: '0.58rem', fontWeight: 500, letterSpacing: '0.10em', textTransform: 'uppercase', color: 'rgba(255,255,255,0.38)', marginBottom: 10 }}>
        {title}
      </div>
      <ResponsiveContainer width="100%" height={height}>
        <AreaChart data={parsed} margin={{ top: 4, right: 4, bottom: 0, left: -16 }}>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor={color} stopOpacity={0.28} />
              <stop offset="95%" stopColor={color} stopOpacity={0.01} />
            </linearGradient>
          </defs>
          <XAxis dataKey="t" tick={{ fontSize: 9 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
          <YAxis tick={{ fontSize: 9 }} tickLine={false} axisLine={false} width={36} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            labelStyle={TOOLTIP_LABEL_STYLE}
            itemStyle={{ color }}
            formatter={(v) => [Number(v).toFixed(2), yLabel ?? 'value']}
          />
          <Area type="monotone" dataKey="value" stroke={color} strokeWidth={2} fill={`url(#${gradientId})`} dot={false} />
        </AreaChart>
      </ResponsiveContainer>
    </GlassCard>
  )
}
