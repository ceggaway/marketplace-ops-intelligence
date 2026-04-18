interface Props { label: string; color: string; className?: string }

export default function Badge({ label, color, className }: Props) {
  return (
    <span className={className} style={{
      display: 'inline-flex', alignItems: 'center',
      background: `${color}20`, color, border: `1px solid ${color}45`,
      borderRadius: 20, padding: '2px 10px',
      fontSize: '0.60rem', fontWeight: 700, letterSpacing: '0.09em', textTransform: 'uppercase',
    }}>
      {label}
    </span>
  )
}
