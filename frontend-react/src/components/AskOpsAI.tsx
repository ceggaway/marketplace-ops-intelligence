import { useState, useRef, useEffect, type KeyboardEvent } from 'react'
import { X, Send, Bot, User, Loader, Sparkles, RefreshCw } from 'lucide-react'
import { COLORS } from '../lib/utils'

interface Message {
  role: 'user' | 'assistant'
  content: string
  ts: Date
}

interface Props {
  open: boolean
  onClose: () => void
}

const SUGGESTED = [
  'Which zones need immediate action?',
  'Summarise current supply risk',
  'What is the model confidence right now?',
  'Any signs of drift or model issues?',
]

function TypingDots() {
  return (
    <div style={{ display: 'flex', gap: 4, padding: '4px 2px', alignItems: 'center' }}>
      {[0, 1, 2].map(i => (
        <div
          key={i}
          style={{
            width: 6, height: 6, borderRadius: '50%',
            background: 'rgba(79,142,247,0.55)',
            animation: `bounce 1.2s ease-in-out ${i * 0.2}s infinite`,
          }}
        />
      ))}
      <style>{`
        @keyframes bounce {
          0%, 80%, 100% { transform: translateY(0); opacity: 0.5; }
          40%            { transform: translateY(-5px); opacity: 1; }
        }
      `}</style>
    </div>
  )
}

function MessageBubble({ msg }: { msg: Message }) {
  const isUser = msg.role === 'user'

  // Render markdown-lite: bold (**text**) and code blocks (```...```)
  const renderContent = (text: string) => {
    const parts = text.split(/(```[\s\S]*?```|\*\*[^*]+\*\*)/g)
    return parts.map((part, i) => {
      if (part.startsWith('```') && part.endsWith('```')) {
        const code = part.slice(3, -3).trim()
        return (
          <pre key={i} style={{
            background: 'rgba(0,0,0,0.40)', border: '1px solid rgba(255,255,255,0.10)',
            borderRadius: 8, padding: '10px 12px', fontSize: '0.68rem',
            fontFamily: 'monospace', overflowX: 'auto', whiteSpace: 'pre-wrap',
            color: 'rgba(255,255,255,0.70)', marginTop: 8, marginBottom: 4,
          }}>{code}</pre>
        )
      }
      if (part.startsWith('**') && part.endsWith('**')) {
        return <strong key={i} style={{ color: 'rgba(255,255,255,0.92)', fontWeight: 600 }}>{part.slice(2, -2)}</strong>
      }
      return <span key={i}>{part}</span>
    })
  }

  return (
    <div style={{
      display: 'flex', gap: 8,
      flexDirection: isUser ? 'row-reverse' : 'row',
      alignItems: 'flex-start', marginBottom: 16,
    }}>
      {/* Avatar */}
      <div style={{
        width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
        background: isUser
          ? 'rgba(255,255,255,0.10)'
          : 'linear-gradient(135deg, rgba(79,142,247,0.35) 0%, rgba(123,92,247,0.35) 100%)',
        border: `1px solid ${isUser ? 'rgba(255,255,255,0.14)' : 'rgba(79,142,247,0.35)'}`,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>
        {isUser
          ? <User size={13} color="rgba(255,255,255,0.55)" />
          : <Bot size={13} color={COLORS.primary} />}
      </div>

      {/* Bubble */}
      <div style={{
        maxWidth: '78%',
        background: isUser
          ? 'rgba(79,142,247,0.14)'
          : 'rgba(255,255,255,0.05)',
        border: `1px solid ${isUser ? 'rgba(79,142,247,0.28)' : 'rgba(255,255,255,0.09)'}`,
        borderRadius: isUser ? '14px 4px 14px 14px' : '4px 14px 14px 14px',
        padding: '10px 13px',
      }}>
        <div style={{
          fontSize: '0.78rem', color: 'rgba(255,255,255,0.82)',
          lineHeight: 1.55, whiteSpace: 'pre-wrap',
        }}>
          {renderContent(msg.content)}
        </div>
        <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.25)', marginTop: 6, textAlign: isUser ? 'right' : 'left' }}>
          {msg.ts.toLocaleTimeString('en-SG', { hour: '2-digit', minute: '2-digit' })}
        </div>
      </div>
    </div>
  )
}

export default function AskOpsAI({ open, onClose }: Props) {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput]       = useState('')
  const [loading, setLoading]   = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef  = useRef<HTMLTextAreaElement>(null)

  // Welcome message on first open
  useEffect(() => {
    if (open && messages.length === 0) {
      setMessages([{
        role: 'assistant',
        content: "Hi, I'm Ops AI. I have access to your live zone data, risk scores, recommendations, and model health.\n\nWhat would you like to know?",
        ts: new Date(),
      }])
    }
    if (open) setTimeout(() => inputRef.current?.focus(), 300)
  }, [open])

  // Scroll to bottom on new message
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length, loading])

  async function handleSend(text?: string) {
    const msg = (text ?? input).trim()
    if (!msg || loading) return

    const userMsg: Message = { role: 'user', content: msg, ts: new Date() }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)

    try {
      const res = await fetch('/api/v1/ai/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: msg,
          history: messages.map(m => ({ role: m.role, content: m.content })),
        }),
      })
      const data = await res.json()
      setMessages(prev => [...prev, { role: 'assistant', content: data.reply, ts: new Date() }])
    } catch {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Could not reach the AI service. Check that the API is running.',
        ts: new Date(),
      }])
    } finally {
      setLoading(false)
    }
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  function handleReset() {
    setMessages([{
      role: 'assistant',
      content: "Conversation cleared. What would you like to know?",
      ts: new Date(),
    }])
  }

  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: 'fixed', inset: 0, zIndex: 200,
          background: 'rgba(0,0,0,0.40)',
          backdropFilter: 'blur(3px)',
          opacity: open ? 1 : 0,
          pointerEvents: open ? 'auto' : 'none',
          transition: 'opacity 0.25s',
        }}
      />

      {/* Sidebar panel */}
      <div style={{
        position: 'fixed', top: 0, right: 0, height: '100vh', width: 420,
        zIndex: 201,
        transform: open ? 'translateX(0)' : 'translateX(100%)',
        transition: 'transform 0.28s cubic-bezier(0.32,0,0.15,1)',
        background: 'rgba(7,14,28,0.98)',
        borderLeft: '1px solid rgba(79,142,247,0.18)',
        display: 'flex', flexDirection: 'column',
        boxShadow: '-12px 0 48px rgba(0,0,0,0.65)',
      }}>

        {/* ── Header ─────────────────────────────────────────── */}
        <div style={{
          padding: '16px 18px', borderBottom: '1px solid rgba(79,142,247,0.12)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{
              width: 34, height: 34, borderRadius: 10, flexShrink: 0,
              background: 'linear-gradient(135deg, rgba(79,142,247,0.30) 0%, rgba(123,92,247,0.25) 100%)',
              border: '1px solid rgba(79,142,247,0.35)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <Sparkles size={15} color={COLORS.primary} />
            </div>
            <div>
              <div style={{ fontSize: '0.88rem', fontWeight: 600, color: 'rgba(255,255,255,0.92)', lineHeight: 1 }}>Ops AI</div>
              <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.35)', marginTop: 2 }}>Live data · Singapore Taxi Ops</div>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            <button
              onClick={handleReset}
              title="Clear chat"
              style={{
                width: 28, height: 28, borderRadius: 7, border: '1px solid rgba(255,255,255,0.10)',
                background: 'rgba(255,255,255,0.05)', color: 'rgba(255,255,255,0.40)',
                cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}
            >
              <RefreshCw size={12} />
            </button>
            <button
              onClick={onClose}
              style={{
                width: 28, height: 28, borderRadius: 7, border: '1px solid rgba(255,255,255,0.10)',
                background: 'rgba(255,255,255,0.05)', color: 'rgba(255,255,255,0.40)',
                cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}
            >
              <X size={13} />
            </button>
          </div>
        </div>

        {/* ── Messages ───────────────────────────────────────── */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '18px 16px 8px' }}>
          {messages.map((msg, i) => (
            <MessageBubble key={i} msg={msg} />
          ))}
          {loading && (
            <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start', marginBottom: 16 }}>
              <div style={{
                width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
                background: 'linear-gradient(135deg, rgba(79,142,247,0.35) 0%, rgba(123,92,247,0.35) 100%)',
                border: '1px solid rgba(79,142,247,0.35)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Loader size={13} color={COLORS.primary} style={{ animation: 'spin 1s linear infinite' }} />
              </div>
              <div style={{
                background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.09)',
                borderRadius: '4px 14px 14px 14px', padding: '10px 13px',
              }}>
                <TypingDots />
              </div>
            </div>
          )}
          <div ref={bottomRef} />
        </div>

        {/* ── Suggested prompts (only when 1 message / fresh) ── */}
        {messages.length <= 1 && !loading && (
          <div style={{ padding: '0 16px 10px', flexShrink: 0 }}>
            <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.28)', marginBottom: 8, letterSpacing: '0.08em', textTransform: 'uppercase', fontWeight: 600 }}>
              Suggested
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
              {SUGGESTED.map(s => (
                <button
                  key={s}
                  onClick={() => handleSend(s)}
                  style={{
                    textAlign: 'left', padding: '7px 12px', borderRadius: 8,
                    background: 'rgba(79,142,247,0.07)', border: '1px solid rgba(79,142,247,0.18)',
                    color: 'rgba(255,255,255,0.62)', fontSize: '0.74rem',
                    cursor: 'pointer', fontFamily: 'Inter, sans-serif',
                    transition: 'all 0.15s',
                  }}
                  onMouseEnter={e => {
                    (e.currentTarget as HTMLElement).style.background = 'rgba(79,142,247,0.14)'
                    ;(e.currentTarget as HTMLElement).style.color = 'rgba(255,255,255,0.85)'
                  }}
                  onMouseLeave={e => {
                    (e.currentTarget as HTMLElement).style.background = 'rgba(79,142,247,0.07)'
                    ;(e.currentTarget as HTMLElement).style.color = 'rgba(255,255,255,0.62)'
                  }}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* ── Input ──────────────────────────────────────────── */}
        <div style={{
          padding: '10px 14px 16px', borderTop: '1px solid rgba(255,255,255,0.07)',
          flexShrink: 0,
        }}>
          <div style={{
            display: 'flex', gap: 8, alignItems: 'flex-end',
            background: 'rgba(255,255,255,0.05)',
            border: '1px solid rgba(79,142,247,0.22)',
            borderRadius: 12, padding: '8px 10px 8px 14px',
          }}>
            <textarea
              ref={inputRef}
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about zones, supply, actions…"
              rows={1}
              style={{
                flex: 1, background: 'none', border: 'none', outline: 'none',
                color: 'rgba(255,255,255,0.85)', fontSize: '0.80rem',
                fontFamily: 'Inter, sans-serif', resize: 'none',
                maxHeight: 120, overflowY: 'auto', lineHeight: 1.5,
              }}
              onInput={e => {
                const el = e.currentTarget
                el.style.height = 'auto'
                el.style.height = Math.min(el.scrollHeight, 120) + 'px'
              }}
            />
            <button
              onClick={() => handleSend()}
              disabled={!input.trim() || loading}
              style={{
                width: 32, height: 32, borderRadius: 8, flexShrink: 0,
                background: input.trim() && !loading ? COLORS.primary : 'rgba(255,255,255,0.08)',
                border: 'none', cursor: input.trim() && !loading ? 'pointer' : 'not-allowed',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                transition: 'background 0.18s',
              }}
            >
              <Send size={14} color={input.trim() && !loading ? '#fff' : 'rgba(255,255,255,0.25)'} />
            </button>
          </div>
          <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.22)', marginTop: 6, textAlign: 'center' }}>
            Enter to send · Shift+Enter for newline
          </div>
        </div>
      </div>

      <style>{`
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </>
  )
}
