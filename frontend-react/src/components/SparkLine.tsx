import { LineChart, Line, ResponsiveContainer } from 'recharts'
import type { TrendPoint } from '../lib/api'

interface Props {
  data: TrendPoint[]
  color: string
  height?: number
  width?: number
}

export default function SparkLine({ data, color, height = 36, width = 80 }: Props) {
  const parsed = data.map(d => ({ v: d.value }))
  return (
    <ResponsiveContainer width={width} height={height}>
      <LineChart data={parsed} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
        <Line type="monotone" dataKey="v" stroke={color} strokeWidth={1.5} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  )
}
