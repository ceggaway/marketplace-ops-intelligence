import { cn } from '../lib/utils'

interface Props {
  children: React.ReactNode
  className?: string
  accentColor?: string
  hover?: boolean
  style?: React.CSSProperties
}

export default function GlassCard({ children, className, accentColor, hover = true, style }: Props) {
  return (
    <div
      className={cn(
        'glass rounded-[15px]',
        // Avalon spec: scale(1.05) hover, not translateY
        hover && 'transition-all duration-200 hover:scale-[1.02] cursor-pointer',
        className,
      )}
      style={{
        borderTop: accentColor ? `2px solid ${accentColor}` : undefined,
        ...style,
      }}
    >
      {children}
    </div>
  )
}
