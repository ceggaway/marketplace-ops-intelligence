import { Activity, BarChart3, Bell, Brain, CheckCircle2, Database, GitBranch, Hexagon, LineChart, MapPin, RefreshCw, ShieldCheck, Zap } from 'lucide-react'
import GlassCard from '../components/GlassCard'
import Badge from '../components/Badge'
import { SECTION_LABEL } from '../lib/utils'

type Tone = 'blue' | 'green' | 'amber' | 'purple' | 'red' | 'slate'

const OPS_PALETTE = {
  ink: '#101820',
  panel: 'rgba(19,25,31,0.82)',
  panelSoft: 'rgba(28,33,38,0.66)',
  border: 'rgba(210,201,188,0.14)',
  steel: '#7F91A3',
  teal: '#4F9386',
  olive: '#8F9B61',
  ochre: '#B7833D',
  brick: '#B86655',
  plum: '#8A728D',
}

const TONES: Record<Tone, { bg: string; border: string; text: string }> = {
  blue:   { bg: 'rgba(127,145,163,0.13)', border: 'rgba(127,145,163,0.36)', text: OPS_PALETTE.steel },
  green:  { bg: 'rgba(79,147,134,0.13)', border: 'rgba(79,147,134,0.38)', text: OPS_PALETTE.teal },
  amber:  { bg: 'rgba(183,131,61,0.13)', border: 'rgba(183,131,61,0.40)', text: OPS_PALETTE.ochre },
  purple: { bg: 'rgba(138,114,141,0.13)', border: 'rgba(138,114,141,0.34)', text: OPS_PALETTE.plum },
  red:    { bg: 'rgba(184,102,85,0.12)', border: 'rgba(184,102,85,0.38)', text: OPS_PALETTE.brick },
  slate:  { bg: OPS_PALETTE.panelSoft, border: OPS_PALETTE.border, text: 'rgba(232,226,218,0.64)' },
}

const FLOW_STEPS = [
  { title: 'Live Mobility Inputs', sub: 'LTA taxi availability, zone metadata, weather, calendar and time signals', tone: 'amber' as Tone },
  { title: 'Ingestion & Validation', sub: 'Schema checks, bad-row isolation and live snapshot storage', tone: 'blue' as Tone },
  { title: 'Feature Pipeline', sub: 'Lag features, depletion rates, demand-pressure proxy, supply-gap and imbalance indicators', tone: 'green' as Tone },
  { title: 'Supply-Depletion Risk Model', sub: 'Predicts zone-level probability of sharp available-supply decline', tone: 'purple' as Tone },
  { title: 'Policy Engine', sub: 'Ranks monitoring, ops alerts, driver nudges, rebalancing and incentive recommendations', tone: 'amber' as Tone },
  { title: 'Dashboards & Alerts', sub: 'Overview, Zone Risk, Action Center, Model Health and Reports', tone: 'blue' as Tone },
  { title: 'Monitoring & Rollback', sub: 'PSI drift, model metrics, promotion gates and previous stable model restore', tone: 'red' as Tone },
]

const ARCH_COLUMNS = [
  {
    title: 'Data Layer',
    tone: 'amber' as Tone,
    items: ['data/raw live snapshots', 'data/processed features', 'data/outputs predictions'],
  },
  {
    title: 'ML Layer',
    tone: 'purple' as Tone,
    items: ['training/trainer.py', 'registry/model_registry.py', 'scoring/batch_scorer.py'],
  },
  {
    title: 'Ops Layer',
    tone: 'green' as Tone,
    items: ['recommendation engine', 'policy effectiveness', 'outcome tracker'],
  },
  {
    title: 'API & UI',
    tone: 'blue' as Tone,
    items: ['FastAPI /api/v1', 'React dashboards', 'reports and model health'],
  },
]

const audienceHeaderStyle = {
  display: 'flex',
  alignItems: 'center',
  gap: 9,
  marginBottom: 10,
  minHeight: 18,
}

const dashboardItemStyle = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: 10,
  minHeight: 54,
}

function StageColumn({ title, sub, tone = 'blue', index }: { title: string; sub: string; tone?: Tone; index: number }) {
  const t = TONES[tone]
  return (
    <div style={{
      minHeight: 148,
      padding: '14px 12px',
      borderLeft: `3px solid ${t.text}`,
      background: index % 2 === 0 ? 'rgba(255,255,255,0.026)' : 'rgba(255,255,255,0.014)',
      borderTop: `1px solid ${OPS_PALETTE.border}`,
      borderBottom: `1px solid ${OPS_PALETTE.border}`,
    }}>
      <div style={{
        fontSize: '0.56rem',
        fontWeight: 700,
        letterSpacing: '0.10em',
        color: t.text,
        marginBottom: 16,
      }}>
        STAGE {String(index).padStart(2, '0')}
      </div>
      <div style={{ fontSize: '0.82rem', fontWeight: 700, color: 'rgba(239,234,226,0.90)', lineHeight: 1.25, marginBottom: 8 }}>{title}</div>
      <div style={{ fontSize: '0.64rem', color: 'rgba(232,226,218,0.56)', lineHeight: 1.45 }}>{sub}</div>
    </div>
  )
}

function MatrixRow({ title, tone, items }: { title: string; tone: Tone; items: string[] }) {
  const t = TONES[tone]
  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: '132px minmax(0, 1fr)',
      gap: 16,
      alignItems: 'center',
      padding: '12px 0',
      borderTop: `1px solid ${OPS_PALETTE.border}`,
    }}>
      <div style={{ color: 'rgba(239,234,226,0.86)', fontWeight: 700, fontSize: '0.74rem', display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ width: 8, height: 8, borderRadius: 999, background: t.text }} />
        {title}
      </div>
      <div style={{ display: 'flex', gap: 7, flexWrap: 'wrap' }}>
        {items.map(item => (
          <span key={item} style={{
            fontSize: '0.60rem',
            color: 'rgba(232,226,218,0.62)',
            lineHeight: 1.25,
            fontFamily: item.includes('.') || item.includes('/') ? 'monospace' : undefined,
            background: 'rgba(255,255,255,0.035)',
            border: `1px solid ${OPS_PALETTE.border}`,
            borderRadius: 999,
            padding: '4px 8px',
          }}>
            {item}
          </span>
        ))}
      </div>
    </div>
  )
}

function LedgerTable({ rows }: { rows: Array<{ event: string; owner: string; output: string; tone: Tone }> }) {
  return (
    <div style={{ borderTop: `1px solid ${OPS_PALETTE.border}` }}>
      <div style={{
        display: 'grid',
        gridTemplateColumns: '1.05fr 0.7fr 1.25fr',
        gap: 14,
        padding: '8px 0',
        fontSize: '0.54rem',
        fontWeight: 700,
        letterSpacing: '0.11em',
        textTransform: 'uppercase',
        color: 'rgba(232,226,218,0.36)',
      }}>
        <span>Event</span>
        <span>Owner</span>
        <span>Output</span>
      </div>
      {rows.map((row) => {
        const t = TONES[row.tone]
        return (
          <div key={row.event} style={{
            display: 'grid',
            gridTemplateColumns: '1.05fr 0.7fr 1.25fr',
            gap: 14,
            alignItems: 'baseline',
            padding: '10px 0',
            borderTop: `1px solid ${OPS_PALETTE.border}`,
          }}>
            <div style={{ color: t.text, fontSize: '0.68rem', fontWeight: 700 }}>{row.event}</div>
            <div style={{ color: 'rgba(232,226,218,0.54)', fontSize: '0.66rem' }}>{row.owner}</div>
            <div style={{ color: 'rgba(232,226,218,0.64)', fontSize: '0.66rem', lineHeight: 1.45 }}>{row.output}</div>
          </div>
        )
      })}
    </div>
  )
}

function MetricTile({ icon: Icon, label, value }: { icon: typeof Activity; label: string; value: string }) {
  return (
    <GlassCard hover={false} style={{ padding: '15px 16px', minHeight: 92 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 9, marginBottom: 10 }}>
        <div style={{
          width: 28, height: 28, borderRadius: 8,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: 'rgba(79,147,134,0.12)',
          border: '1px solid rgba(79,147,134,0.24)',
        }}>
          <Icon size={14} color={OPS_PALETTE.teal} strokeWidth={1.8} />
        </div>
        <div style={{ fontSize: '0.58rem', textTransform: 'uppercase', letterSpacing: '0.10em', color: 'rgba(255,255,255,0.34)', fontWeight: 700 }}>{label}</div>
      </div>
      <div style={{ fontSize: '0.88rem', color: 'rgba(255,255,255,0.82)', lineHeight: 1.45 }}>{value}</div>
    </GlassCard>
  )
}

function PipelineDiagram() {
  const diagramCardStyle = {
    background: `linear-gradient(135deg, ${OPS_PALETTE.panel} 0%, rgba(13,18,23,0.92) 100%)`,
    border: `1px solid ${OPS_PALETTE.border}`,
    boxShadow: '0 18px 40px rgba(0,0,0,0.32)',
  }

  return (
    <GlassCard hover={false} style={{ ...diagramCardStyle, padding: '0' }}>
      <div style={{ padding: '20px 22px 16px', borderBottom: `1px solid ${OPS_PALETTE.border}` }}>
        <div style={{ ...SECTION_LABEL, marginBottom: 8 }}>Operating Model</div>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 18, alignItems: 'flex-end' }}>
          <div style={{ fontSize: '1.05rem', fontWeight: 700, color: 'rgba(239,234,226,0.90)' }}>
            Supply risk scoring and response
          </div>
          <div style={{ fontSize: '0.66rem', color: 'rgba(232,226,218,0.52)', maxWidth: 470, lineHeight: 1.45, textAlign: 'right' }}>
            Daily scoring is score-only. Retraining is a controlled lifecycle step with registry promotion gates.
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: `repeat(${FLOW_STEPS.length}, minmax(112px, 1fr))`, overflowX: 'auto' }}>
        {FLOW_STEPS.map((step, idx) => (
          <StageColumn key={step.title} {...step} index={idx + 1} />
        ))}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(360px, 0.9fr) minmax(420px, 1.1fr)', gap: 28, padding: '18px 22px 22px', alignItems: 'start' }}>
        <div>
          <div style={{ ...SECTION_LABEL, marginBottom: 2 }}>Ownership Matrix</div>
          {ARCH_COLUMNS.map((col) => (
            <MatrixRow key={col.title} title={col.title} tone={col.tone} items={col.items} />
          ))}
          <div style={{
            marginTop: 14,
            paddingTop: 12,
            borderTop: `1px dashed ${TONES.red.border}`,
            display: 'grid',
            gridTemplateColumns: 'minmax(0, 1fr) auto',
            gap: 14,
            alignItems: 'center',
            fontSize: '0.66rem',
            color: 'rgba(232,226,218,0.54)',
          }}>
            <span>Drift or failed checks</span>
            <span style={{ color: OPS_PALETTE.brick, fontWeight: 700 }}>rollback to previous stable model</span>
          </div>
        </div>

        <div>
          <div style={{ ...SECTION_LABEL, marginBottom: 2 }}>Run Ledger</div>
          <LedgerTable
            rows={[
              { event: 'Scheduled scoring', owner: 'Batch job', output: 'loads active model and current feature set', tone: 'slate' },
              { event: 'Zone scoring', owner: 'ML layer', output: 'predictions, severity buckets and recommendation context', tone: 'amber' },
              { event: 'Decision review', owner: 'Ops layer', output: 'ranked operational recommendations and follow-through status', tone: 'green' },
              { event: 'Impact reporting', owner: 'API & UI', output: 'trend, model health, monitoring logs and exportable reports', tone: 'purple' },
            ]}
          />
        </div>
      </div>
    </GlassCard>
  )
}

export default function ProjectBrief() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.35fr) minmax(280px, 0.65fr)', gap: 18 }}>
        <GlassCard hover={false} style={{ padding: '22px 24px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 8 }}>Project Summary</div>
          <h1 style={{ margin: 0, color: 'rgba(255,255,255,0.94)', fontSize: '1.55rem', lineHeight: 1.2, letterSpacing: 0 }}>
            Marketplace Ops Intelligence for Singapore taxi supply risk
          </h1>
          <p style={{ margin: '12px 0 0', color: 'rgba(255,255,255,0.62)', fontSize: '0.88rem', lineHeight: 1.7, maxWidth: 900 }}>
            OpsIQ predicts where available taxi supply may become constrained, explains the operational drivers, and turns model output into ranked, human-reviewed recommendations for marketplace operators. The product combines live mobility inputs, feature engineering, supply-depletion risk scoring, demand-pressure proxy signals, recommendation policies, model monitoring, and reporting in one workflow.
          </p>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 16 }}>
            <Badge label="FastAPI backend" color={OPS_PALETTE.steel} />
            <Badge label="React dashboards" color={OPS_PALETTE.teal} />
            <Badge label="Model registry" color={OPS_PALETTE.plum} />
            <Badge label="Drift monitoring" color={OPS_PALETTE.ochre} />
          </div>
        </GlassCard>

        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 12 }}>What It Produces</div>
          {[
            'Zone-level supply-depletion risk scores',
            'Available-supply shortage severity buckets',
            'Prioritized intervention recommendations',
            'Model performance, drift and rollback status',
            'Operational reports for trends, outcomes and model impact',
          ].map(item => (
            <div key={item} style={{ display: 'flex', gap: 9, alignItems: 'flex-start', marginBottom: 11 }}>
              <CheckCircle2 size={15} color={OPS_PALETTE.teal} strokeWidth={1.8} style={{ marginTop: 1, flexShrink: 0 }} />
              <div style={{ fontSize: '0.78rem', color: 'rgba(255,255,255,0.68)', lineHeight: 1.45 }}>{item}</div>
            </div>
          ))}
        </GlassCard>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14 }}>
        <MetricTile icon={Database} label="Inputs" value="Live taxi supply snapshots, engineered zone features, weather/time signals and model registry state." />
        <MetricTile icon={Brain} label="Model" value="Supply-depletion risk classifier with versioned artifacts and promotion gates." />
        <MetricTile icon={Zap} label="Actions" value="Policy-ranked interventions based on risk, persistence, cost, neighbour surplus, cooldowns and expected uplift." />
        <MetricTile icon={ShieldCheck} label="Reliability" value="PSI drift checks, failed-row handling, monitoring logs and rollback path." />
      </div>

      <PipelineDiagram />

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 14 }}>
        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={audienceHeaderStyle}>
            <MapPin size={16} color={OPS_PALETTE.steel} strokeWidth={1.9} style={{ flexShrink: 0 }} />
            <div style={{ ...SECTION_LABEL, lineHeight: 1 }}>For Operators</div>
          </div>
          <div style={{ color: 'rgba(255,255,255,0.62)', fontSize: '0.78rem', lineHeight: 1.65 }}>
            See which zones need attention, why the model believes available supply is at risk, and which human-reviewed intervention is expected to help fastest.
          </div>
        </GlassCard>
        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={audienceHeaderStyle}>
            <LineChart size={16} color={OPS_PALETTE.teal} strokeWidth={1.9} style={{ flexShrink: 0 }} />
            <div style={{ ...SECTION_LABEL, lineHeight: 1 }}>For Analysts</div>
          </div>
          <div style={{ color: 'rgba(255,255,255,0.62)', fontSize: '0.78rem', lineHeight: 1.65 }}>
            Compare zone performance, recommendation outcomes, model versions, drift signals and historical metric movement from the Reports and Model Health views.
          </div>
        </GlassCard>
        <GlassCard hover={false} style={{ padding: '18px 20px' }}>
          <div style={audienceHeaderStyle}>
            <GitBranch size={16} color={OPS_PALETTE.ochre} strokeWidth={1.9} style={{ flexShrink: 0 }} />
            <div style={{ ...SECTION_LABEL, lineHeight: 1 }}>For Deployment</div>
          </div>
          <div style={{ color: 'rgba(255,255,255,0.62)', fontSize: '0.78rem', lineHeight: 1.65 }}>
            The app separates frontend, API, scoring, monitoring and model bootstrap so production jobs can run without retraining on every batch.
          </div>
        </GlassCard>
      </div>

      <GlassCard hover={false} style={{ padding: '18px 20px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12, alignItems: 'center' }}>
          {[
            { icon: Activity, label: 'Overview', text: 'live KPIs' },
            { icon: MapPin, label: 'Zone Risk', text: 'where supply risk is building' },
            { icon: Bell, label: 'Action Center', text: 'what action to consider' },
            { icon: RefreshCw, label: 'Model Health', text: 'whether the model can be trusted' },
            { icon: BarChart3, label: 'Reports', text: 'what changed over time' },
          ].map(({ icon: Icon, label, text }) => (
            <div key={label} style={dashboardItemStyle}>
              <Icon size={18} color={OPS_PALETTE.steel} strokeWidth={1.7} style={{ flexShrink: 0 }} />
              <div style={{ minWidth: 0 }}>
                <div style={{ fontSize: '0.74rem', color: 'rgba(255,255,255,0.78)', fontWeight: 700, lineHeight: 1.1 }}>{label}</div>
                <div style={{ fontSize: '0.60rem', color: 'rgba(255,255,255,0.35)', marginTop: 4, lineHeight: 1.15 }}>{text}</div>
              </div>
            </div>
          ))}
        </div>
      </GlassCard>

      {/* ── H3 Hex-Level Spatial Granularity ──────────────────────────────── */}
      <GlassCard hover={false} style={{ padding: '22px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
          <div style={{ width: 36, height: 36, borderRadius: 9, background: 'rgba(138,114,141,0.13)', border: `1px solid ${TONES.purple.border}`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
            <Hexagon size={18} color={OPS_PALETTE.plum} strokeWidth={1.75} />
          </div>
          <div>
            <div style={{ fontSize: '1.02rem', fontWeight: 700, color: 'rgba(255,255,255,0.92)', marginBottom: 2 }}>H3 Hex-Level Spatial Granularity</div>
            <div style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.35)' }}>Optional layer on top of the existing zone-level pipeline · Uber H3 resolution 8 · ~0.46 km² per cell</div>
          </div>
        </div>

        <div style={{ fontSize: '0.78rem', color: 'rgba(255,255,255,0.60)', lineHeight: 1.65, marginBottom: 18 }}>
          The standard pipeline aggregates taxi availability into planning zones (~30 areas). The H3 layer
          re-indexes the same GPS point data into a uniform hexagonal grid — each hex is roughly 460 m across —
          giving a higher-resolution view of where supply stress is concentrated. Zone-level scores remain the
          authoritative signal; H3 adds a sub-zone drill-down to distinguish whether a problem is spread across
          an entire zone or confined to a specific pocket (e.g. a single MRT interchange or event venue).
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 14, marginBottom: 18 }}>
          {[
            {
              title: 'How it works',
              body: 'Each taxi GPS fix is mapped to an H3 cell using its lat/lon. Cells are aggregated by timestamp to produce per-cell taxi counts, then feature-engineered (lags, rolling windows, depletion rate, demand-pressure proxy) before scoring.',
              tone: 'purple' as Tone,
            },
            {
              title: 'Action policy',
              body: 'H3 actions are conservative: low/moderate → monitor; high → ops alert + driver comms; severe → ops alert + driver comms + incentive. Rebalancing only fires when neighbor surplus exceeds the configured threshold to avoid over-dispatching.',
              tone: 'amber' as Tone,
            },
            {
              title: 'Sparse cell caveat',
              body: 'Cells with median taxi count below min_taxis_per_cell (default: 3) are flagged as sparse. Scores in sparse cells are directional — they indicate a possible gap but carry higher uncertainty. The dashboard shows a warning badge on all sparse cells.',
              tone: 'red' as Tone,
            },
          ].map(({ title, body, tone }) => {
            const t = TONES[tone]
            return (
              <div key={title} style={{ background: t.bg, border: `1px solid ${t.border}`, borderRadius: 10, padding: '14px 16px' }}>
                <div style={{ fontSize: '0.72rem', fontWeight: 700, color: t.text, marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.06em' }}>{title}</div>
                <div style={{ fontSize: '0.72rem', color: 'rgba(255,255,255,0.58)', lineHeight: 1.55 }}>{body}</div>
              </div>
            )
          })}
        </div>

        <div style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.08)', borderRadius: 8, padding: '12px 16px' }}>
          <div style={{ ...SECTION_LABEL, marginBottom: 8 }}>HOW TO RUN H3 SCORING</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {[
              { cmd: 'python scripts/run_scoring.py --h3', desc: 'Score prepared H3 features from data/processed/h3_supply_features.csv' },
              { cmd: 'python scripts/run_scoring.py --h3 --zone', desc: 'Run zone scoring and H3 scoring in one pass' },
              { cmd: 'GET /api/v1/h3/heatmap', desc: 'Fetch all H3 cells with polygon boundaries for map rendering' },
              { cmd: 'GET /api/v1/h3/cells?severity_bucket=severe', desc: 'Filter H3 cells by severity bucket' },
            ].map(({ cmd, desc }) => (
              <div key={cmd} style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
                <code style={{ fontSize: '0.68rem', fontFamily: 'monospace', color: OPS_PALETTE.plum, background: 'rgba(138,114,141,0.12)', padding: '2px 7px', borderRadius: 4, whiteSpace: 'nowrap', flexShrink: 0 }}>{cmd}</code>
                <span style={{ fontSize: '0.68rem', color: 'rgba(255,255,255,0.40)' }}>{desc}</span>
              </div>
            ))}
          </div>
        </div>
      </GlassCard>
    </div>
  )
}
