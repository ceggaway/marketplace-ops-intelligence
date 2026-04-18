import { useEffect, useState } from 'react'

export interface ToastMessage {
  id: number
  message: string
  type?: 'info' | 'success' | 'warning' | 'error'
}

let _nextId = 1
const _listeners: Set<(t: ToastMessage) => void> = new Set()

export function showToast(message: string, type: ToastMessage['type'] = 'info') {
  const toast: ToastMessage = { id: _nextId++, message, type }
  _listeners.forEach(fn => fn(toast))
}

const TYPE_COLORS: Record<string, string> = {
  info:    'rgba(79,142,247,0.92)',
  success: 'rgba(16,217,138,0.92)',
  warning: 'rgba(245,158,11,0.92)',
  error:   'rgba(255,77,109,0.92)',
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
    _listeners.add(handler)
    return () => { _listeners.delete(handler) }
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
