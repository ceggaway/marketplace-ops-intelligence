// @keyframes spin is defined in index.css — no inline style tag needed
export default function Spinner({ size = 32 }: { size?: number }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', padding: 40 }}>
      <div style={{
        width: size, height: size, borderRadius: '50%',
        border: '2px solid rgba(249,168,192,0.20)',
        borderTopColor: '#F9A8C0',
        animation: 'spin 0.7s linear infinite',
      }} />
    </div>
  )
}
