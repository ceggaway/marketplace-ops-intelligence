import { useEffect, useState } from 'react'
import { type ToastMessage, toastListeners } from './toast-utils'

const TYPE_COLORS: Record<string, string> = {
  info:    'rgba(69,120,200,0.92)',
  success: 'rgba(59,175,115,0.92)',
  warning: 'rgba(201,123,48,0.92)',
  error:   'rgba(217,82,82,0.92)',
}

export default function ToastContainer() {
  const [toasts, setToasts] = useState<ToastMessage[]>([])

  useEffect(() => {
    const handler = (t: ToastMessage) => {
      setToasts(prev => [...prev, t])
      setTimeout(() => {
        setToasts(prev => prev.filter(x => x.id !== t.id))
      }, 4000)
    }
    toastListeners.add(handler)
    return () => { toastListeners.delete(handler) }
  }, [])

  if (toasts.length === 0) return null

  return (
    <div style={{
      position: 'fixed', bottom: 24, right: 24, zIndex: 9999,
      display: 'flex', flexDirection: 'column', gap: 10, pointerEvents: 'none',
    }}>
      {toasts.map(t => (
        <div key={t.id} style={{
          background: 'rgba(18,24,38,0.96)',
          border: `1px solid ${TYPE_COLORS[t.type ?? 'info']}55`,
          borderLeft: `3px solid ${TYPE_COLORS[t.type ?? 'info']}`,
          borderRadius: 10, padding: '11px 16px',
          fontSize: '0.80rem', color: 'rgba(255,255,255,0.88)',
          boxShadow: '0 8px 32px rgba(0,0,0,0.45)',
          maxWidth: 340, lineHeight: 1.45,
          animation: 'fadeSlideIn 0.2s ease',
          pointerEvents: 'auto',
        }}>
          {t.message}
        </div>
      ))}
      <style>{`
        @keyframes fadeSlideIn {
          from { opacity: 0; transform: translateY(8px); }
          to   { opacity: 1; transform: translateY(0); }
        }
      `}</style>
    </div>
  )
}
