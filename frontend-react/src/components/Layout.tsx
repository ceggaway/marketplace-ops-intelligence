import { useState, useRef, useEffect } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { LayoutDashboard, Map, Zap, Activity, Bell, Database, BarChart2, Cpu, MessageSquare, Headphones, X } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../lib/api'
import type { Alert } from '../lib/api'
import { COLORS } from '../lib/utils'
import { showToast } from './Toast'
import ToastContainer from './Toast'
import AskOpsAI from './AskOpsAI'

// Module-level — not recreated each render
const NOISE_URL = `url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E")`

const PAGE_META: Record<string, { title: string; sub: string }> = {
  '/':        { title: 'Overview',           sub: 'Live performance, key insights, and what\'s next.' },
  '/zones':   { title: 'Zone Risk Monitor',  sub: 'Understand risk by location and take action where it matters most.' },
  '/actions': { title: 'Action Center',      sub: 'Prioritized interventions that drive results.' },
  '/health':  { title: 'Model Health',       sub: 'Monitor performance, detect drift, and keep models reliable.' },
}

const NAV = [
  { to: '/',        Icon: LayoutDashboard, label: 'Overview',      sub: 'Live KPIs & Insights' },
  { to: '/zones',   Icon: Map,             label: 'Zone Risk',      sub: 'Risk scoring & heatmap' },
  { to: '/actions', Icon: Zap,             label: 'Action Center',  sub: 'Prioritized interventions' },
  { to: '/health',  Icon: Activity,        label: 'Model Health',   sub: 'PSI, drift & performance' },
]

const TOOLS = [
  { Icon: Cpu,      label: 'Simulation',   badge: 'New', msg: 'Simulation engine coming soon — build what-if scenarios for driver reallocation.' },
  { Icon: BarChart2, label: 'Reports',      badge: undefined, msg: 'Scheduled reports will be available in a future release.' },
  { Icon: Database, label: 'Data Explorer', badge: undefined, msg: 'Data Explorer is under development — direct SQL query access coming soon.' },
]

function NavItem({ to, Icon, label, sub }: typeof NAV[0]) {
  return (
    <NavLink to={to} end={to === '/'} style={({ isActive }) => ({
      display: 'flex', alignItems: 'center', gap: 10,
      padding: '9px 10px', borderRadius: 10, marginBottom: 1,
      background: isActive ? 'rgba(79,142,247,0.14)' : 'transparent',
      border: `1px solid ${isActive ? 'rgba(79,142,247,0.30)' : 'transparent'}`,
      color: isActive ? 'rgba(255,255,255,0.92)' : 'rgba(255,255,255,0.46)',
      textDecoration: 'none', transition: 'all 0.18s ease',
    })}>
      {({ isActive }) => (
        <>
          <Icon size={14} strokeWidth={1.75} color={isActive ? COLORS.primary : 'rgba(255,255,255,0.42)'} />
          <div>
            <div style={{ fontSize: '0.82rem', fontWeight: isActive ? 500 : 400, lineHeight: 1.2 }}>{label}</div>
            <div style={{ fontSize: '0.58rem', color: 'rgba(255,255,255,0.28)', marginTop: 1 }}>{sub}</div>
          </div>
        </>
      )}
    </NavLink>
  )
}

export default function Layout({ children }: { children: React.ReactNode }) {
  const { pathname } = useLocation()
  const meta = PAGE_META[pathname] ?? PAGE_META['/']
  const now = new Date().toLocaleString('en-SG', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' })

  const [bellOpen, setBellOpen]   = useState(false)
  const [chatOpen, setChatOpen]   = useState(false)
  const bellRef = useRef<HTMLDivElement>(null)

  const { data: alerts = [] } = useQuery<Alert[]>({
    queryKey: ['alerts'],
    queryFn: api.alerts,
    staleTime: 60000,
    refetchInterval: 120000,
  })

  useEffect(() => {
    if (!bellOpen) return
    const handler = (e: MouseEvent) => {
      if (bellRef.current && !bellRef.current.contains(e.target as Node)) setBellOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [bellOpen])

  return (
    <div style={{ display: 'flex', minHeight: '100vh', background: '#060D1A', position: 'relative' }}>

      {/* ── Background — dark navy storm cloud ──────────────────────────── */}
      <div style={{ position: 'fixed', inset: 0, pointerEvents: 'none', zIndex: 0 }}>
        <div style={{ position: 'absolute', inset: 0, background: '#060D1A' }} />
        {/* Storm cloud bloom — top portion */}
        <div style={{
          position: 'absolute', inset: 0,
          background: `
            radial-gradient(ellipse 100% 55% at 50% 0%,   rgba(20,40,90,0.90) 0%, rgba(10,20,55,0.60) 35%, transparent 65%),
            radial-gradient(ellipse 70%  45% at 20% 0%,   rgba(30,55,120,0.70) 0%, transparent 50%),
            radial-gradient(ellipse 60%  40% at 80% 0%,   rgba(25,45,100,0.65) 0%, transparent 50%),
            radial-gradient(ellipse 50%  30% at 50% 15%,  rgba(40,70,160,0.40) 0%, transparent 55%),
            radial-gradient(ellipse 80%  60% at 50% 100%, rgba(8,15,40,0.80)   0%, transparent 60%)
          `,
        }} />
        {/* Subtle blue glow accent */}
        <div style={{
          position: 'absolute', inset: 0,
          background: 'radial-gradient(ellipse 40% 30% at 50% 0%, rgba(79,142,247,0.12) 0%, transparent 60%)',
        }} />
        {/* Vignette */}
        <div style={{
          position: 'absolute', inset: 0,
          background: 'radial-gradient(ellipse 85% 85% at 50% 50%, transparent 45%, rgba(0,0,0,0.55) 100%)',
        }} />
        {/* Noise grain */}
        <div style={{
          position: 'absolute', inset: 0, opacity: 0.18,
          backgroundImage: NOISE_URL, backgroundSize: '256px 256px',
          mixBlendMode: 'overlay',
        }} />
      </div>

      {/* ── Sidebar ─────────────────────────────────────────────────────── */}
      <aside className="glass-sidebar" style={{ position: 'fixed', top: 0, left: 0, height: '100vh', width: 215, zIndex: 50, display: 'flex', flexDirection: 'column' }}>

        {/* Brand */}
        <div style={{ padding: '18px 16px 14px', borderBottom: '1px solid rgba(99,140,255,0.10)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
            {/* Hexagon-style logo mark */}
            <div style={{
              width: 32, height: 32, borderRadius: 8, flexShrink: 0,
              background: 'linear-gradient(135deg, #4F8EF7 0%, #7B5CF7 100%)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              boxShadow: '0 0 12px rgba(79,142,247,0.40)',
            }}>
              <Activity size={15} color="#fff" strokeWidth={2} />
            </div>
            <div>
              <div style={{ fontSize: '0.82rem', fontWeight: 600, color: '#fff', letterSpacing: '0.04em', lineHeight: 1 }}>OpsIQ</div>
              <div style={{ fontSize: '0.52rem', color: 'rgba(255,255,255,0.36)', letterSpacing: '0.12em', textTransform: 'uppercase', marginTop: 2 }}>ML Intelligence · SG</div>
            </div>
          </div>
        </div>

        {/* Dashboards nav */}
        <nav style={{ padding: '12px 8px', flex: 1, overflowY: 'auto' }}>
          <div style={{ fontSize: '0.52rem', fontWeight: 600, letterSpacing: '0.14em', textTransform: 'uppercase', color: 'rgba(255,255,255,0.26)', padding: '0 8px 8px' }}>
            Dashboards
          </div>
          {NAV.map(n => <NavItem key={n.to} {...n} />)}

          {/* Tools section */}
          <div style={{ fontSize: '0.52rem', fontWeight: 600, letterSpacing: '0.14em', textTransform: 'uppercase', color: 'rgba(255,255,255,0.26)', padding: '18px 8px 8px' }}>
            Tools
          </div>
          {TOOLS.map(({ Icon, label, badge, msg }) => (
            <button key={label} onClick={() => showToast(msg, 'info')} style={{
              display: 'flex', alignItems: 'center', gap: 10, width: '100%',
              padding: '9px 10px', borderRadius: 10, marginBottom: 1,
              background: 'transparent', border: '1px solid transparent',
              color: 'rgba(255,255,255,0.38)', cursor: 'pointer',
              fontFamily: 'Inter, sans-serif', textAlign: 'left',
              transition: 'all 0.18s',
            }}
              onMouseEnter={e => (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,0.04)'}
              onMouseLeave={e => (e.currentTarget as HTMLElement).style.background = 'transparent'}
            >
              <Icon size={14} strokeWidth={1.75} />
              <span style={{ fontSize: '0.82rem', fontWeight: 400 }}>{label}</span>
              {badge && (
                <span style={{ marginLeft: 'auto', fontSize: '0.52rem', fontWeight: 600, color: COLORS.low, background: `${COLORS.low}18`, border: `1px solid ${COLORS.low}30`, borderRadius: 20, padding: '1px 7px' }}>
                  {badge}
                </span>
              )}
            </button>
          ))}
        </nav>

        {/* AI assistant card */}
        <div style={{ margin: '8px 10px', background: 'linear-gradient(135deg, rgba(79,142,247,0.14) 0%, rgba(123,92,247,0.10) 100%)', border: '1px solid rgba(79,142,247,0.22)', borderRadius: 12, padding: '14px 14px 12px' }}>
          <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.42)', marginBottom: 4 }}>Need clarity fast?</div>
          <div style={{ fontSize: '0.88rem', fontWeight: 600, color: '#fff', marginBottom: 3 }}>Ask Ops AI</div>
          <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.45)', lineHeight: 1.5, marginBottom: 10 }}>Get instant insights and recommendations.</div>
          <button
            onClick={() => setChatOpen(true)}
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              background: 'rgba(79,142,247,0.20)', border: '1px solid rgba(79,142,247,0.35)',
              borderRadius: 8, padding: '6px 12px', color: COLORS.primary,
              fontSize: '0.72rem', fontWeight: 500, cursor: 'pointer', fontFamily: 'Inter, sans-serif',
            }}>
            <MessageSquare size={11} /> Chat with AI →
          </button>
        </div>

        {/* Footer */}
        <div style={{ padding: '10px 10px 14px', borderTop: '1px solid rgba(99,140,255,0.08)' }}>
          <button
            onClick={() => showToast('For support, email opsiq-support@company.com or raise a ticket in the internal portal.', 'info')}
            style={{
              display: 'flex', alignItems: 'center', gap: 9, width: '100%',
              padding: '8px 10px', borderRadius: 10, background: 'transparent',
              border: '1px solid transparent', color: 'rgba(255,255,255,0.38)',
              cursor: 'pointer', fontFamily: 'Inter, sans-serif',
            }}>
            <Headphones size={13} strokeWidth={1.75} />
            <span style={{ fontSize: '0.80rem' }}>Support</span>
          </button>
        </div>
      </aside>

      {/* ── Content area ────────────────────────────────────────────────── */}
      <div style={{ marginLeft: 215, flex: 1, display: 'flex', flexDirection: 'column', minHeight: '100vh', position: 'relative', zIndex: 10 }}>

        {/* Top bar */}
        <header className="glass-topbar" style={{ position: 'sticky', top: 0, zIndex: 40, padding: '0 28px', height: 62, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16 }}>
          {/* Page title */}
          <div style={{ minWidth: 0 }}>
            <div style={{ fontSize: '1.10rem', fontWeight: 600, color: '#fff', letterSpacing: '-0.01em', lineHeight: 1.1 }}>{meta.title}</div>
            <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.36)', marginTop: 2 }}>{meta.sub}</div>
          </div>

          {/* Header controls */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            {/* Bell */}
            <div ref={bellRef} style={{ position: 'relative' }}>
              <button className="btn-glass" onClick={() => setBellOpen(o => !o)} style={{ padding: '7px 9px', borderRadius: 8 }}>
                <Bell size={14} strokeWidth={1.75} />
              </button>
              {alerts.length > 0 && (
                <div style={{ position: 'absolute', top: 5, right: 5, width: 7, height: 7, borderRadius: '50%', background: COLORS.high, border: '1.5px solid #060D1A' }} />
              )}
              {bellOpen && (
                <div style={{
                  position: 'absolute', top: 'calc(100% + 8px)', right: 0, zIndex: 200,
                  width: 340, maxHeight: 420, overflowY: 'auto',
                  background: 'rgba(14,22,42,0.97)', border: '1px solid rgba(99,140,255,0.18)',
                  borderRadius: 12, boxShadow: '0 8px 32px rgba(0,0,0,0.55)',
                }}>
                  <div style={{ padding: '12px 14px 10px', borderBottom: '1px solid rgba(99,140,255,0.10)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', position: 'sticky', top: 0, background: 'rgba(14,22,42,0.97)' }}>
                    <div>
                      <span style={{ fontSize: '0.80rem', fontWeight: 600, color: 'rgba(255,255,255,0.88)' }}>Notifications</span>
                      {alerts.length > 0 && (
                        <span style={{ marginLeft: 8, fontSize: '0.62rem', fontWeight: 700, color: COLORS.high, background: `${COLORS.high}18`, border: `1px solid ${COLORS.high}30`, borderRadius: 20, padding: '1px 7px' }}>
                          {alerts.length}
                        </span>
                      )}
                    </div>
                    <button onClick={() => setBellOpen(false)} style={{ background: 'none', border: 'none', color: 'rgba(255,255,255,0.35)', cursor: 'pointer', padding: 2 }}>
                      <X size={13} />
                    </button>
                  </div>
                  {alerts.length === 0 ? (
                    <div style={{ padding: '28px 14px', textAlign: 'center', color: 'rgba(255,255,255,0.32)', fontSize: '0.78rem' }}>
                      No active alerts
                    </div>
                  ) : (
                    alerts.map((a: Alert, i: number) => {
                      const ac = a.severity === 'high' ? COLORS.high : a.severity === 'medium' ? COLORS.medium : COLORS.primary
                      return (
                        <div key={a.alert_id} style={{
                          padding: '11px 14px',
                          borderBottom: i < alerts.length - 1 ? '1px solid rgba(255,255,255,0.05)' : 'none',
                          display: 'flex', gap: 10, alignItems: 'flex-start',
                        }}>
                          <div style={{ width: 7, height: 7, borderRadius: '50%', background: ac, marginTop: 6, flexShrink: 0, boxShadow: `0 0 5px ${ac}88` }} />
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{ fontSize: '0.76rem', color: 'rgba(255,255,255,0.82)', lineHeight: 1.45, marginBottom: 4 }}>
                              {a.message}
                            </div>
                            <div style={{ fontSize: '0.62rem', color: 'rgba(255,255,255,0.32)' }}>
                              {new Date(a.created_at).toLocaleString('en-SG', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}
                            </div>
                          </div>
                          <span style={{ fontSize: '0.58rem', fontWeight: 700, color: ac, background: `${ac}18`, border: `1px solid ${ac}30`, borderRadius: 4, padding: '2px 6px', flexShrink: 0, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                            {a.severity}
                          </span>
                        </div>
                      )
                    })
                  )}
                </div>
              )}
            </div>

            {/* System status */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(99,140,255,0.12)', borderRadius: 8, padding: '6px 12px' }}>
              <div style={{ width: 7, height: 7, borderRadius: '50%', background: COLORS.low, boxShadow: `0 0 6px ${COLORS.low}` }} />
              <span style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.62)' }}>System Healthy</span>
            </div>

            {/* Date/time */}
            <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.38)', whiteSpace: 'nowrap' }}>{now}</div>
          </div>
        </header>

        {/* Page content */}
        <main style={{ flex: 1, padding: '28px 32px 64px', overflowY: 'auto' }}>
          {children}
        </main>
      </div>
      <AskOpsAI open={chatOpen} onClose={() => setChatOpen(false)} />
      <ToastContainer />
    </div>
  )
}
