import { ArrowDown, ArrowRight, ExternalLink } from 'lucide-react'
import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { PageHeader } from '../components/PageHeader'
import { REPOSITORY_URL } from './mode'

function Node({ title, detail, tone = 'ink' }: { title: string; detail: string; tone?: 'ink' | 'accent' | 'amber' }) {
  const ring = { ink: 'border-ink-600', accent: 'border-accent-500/60', amber: 'border-amber-500/50' }[tone]
  return (
    <div className={`rounded-lg border ${ring} bg-ink-850 px-3 py-2.5`}>
      <p className="text-sm font-semibold text-ink-100">{title}</p>
      <p className="mt-0.5 text-xs leading-relaxed text-ink-400">{detail}</p>
    </div>
  )
}

function Arrow() {
  return (
    <div className="flex items-center justify-center text-ink-500" aria-hidden>
      <ArrowDown className="h-4 w-4 md:hidden" />
      <ArrowRight className="hidden h-4 w-4 md:block" />
    </div>
  )
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="card p-5" aria-label={title}>
      <h2 className="mb-3 text-sm font-semibold text-ink-100">{title}</h2>
      <div className="space-y-3 text-sm leading-relaxed text-ink-300">{children}</div>
    </section>
  )
}

const GUARANTEES: [string, string][] = [
  ['Dispatched only when the broker has it', 'The job row and its dispatch message are written in one PostgreSQL transaction (transactional outbox). A job becomes QUEUED only after RabbitMQ confirms the publish.'],
  ['Each delivery runs at most once', 'Workers record an inbox entry per message and check the attempt number, so a redelivered message is a no-op.'],
  ['Stale actors cannot write', 'Results are accepted only from the worker holding the live attempt; the scheduler writes under a fencing token from its leader lease.'],
  ['Failures retry with backoff', 'FAILED → RETRYING → QUEUED, with exponential backoff, until the attempt budget runs out.'],
  ['Final failures are kept, not lost', 'Exhausted or permanent failures are dead-lettered: the job keeps its history and error, and can be retried on its own or in bulk.'],
  ['Lost workers are detected', 'Workers heartbeat; when one goes quiet, the scheduler fails and retries its running jobs. On SIGTERM a worker drains and hands unfinished work back.'],
]

const TRY: { to: string; label: string; detail: string }[] = [
  { to: '/jobs?create=1', label: 'Submit a job', detail: 'Pick a type and watch PENDING → QUEUED → RUNNING → COMPLETED.' },
  { to: '/jobs?q=rotate-certificates', label: 'Automatic retry', detail: 'These fail once, back off, and succeed on attempt 2. Open one to see the timeline.' },
  { to: '/jobs?dead_lettered=1', label: 'Dead-letter recovery', detail: 'Filter by an error such as 503, then retry all matching. Permanent errors fail again.' },
  { to: '/workers', label: 'Worker fleet', detail: 'Capacity, load, and a worker that stopped heartbeating and is marked unhealthy.' },
  { to: '/queue', label: 'Queue', detail: 'Waiting work by priority and dispatch status.' },
  { to: '/executions', label: 'Execution history', detail: 'Every attempt, with worker, duration and error.' },
]

export function AboutPage() {
  return (
    <>
      <PageHeader
        title="About NEXUS"
        description="A distributed job orchestration platform: durable background jobs, dispatched through RabbitMQ, executed by a worker fleet, with every attempt recorded."
        actions={
          <a href={REPOSITORY_URL} target="_blank" rel="noreferrer" className="btn-ghost shrink-0 whitespace-nowrap">
            Source code <ExternalLink className="h-3.5 w-3.5" aria-hidden />
          </a>
        }
      />
      <div className="grid gap-6">
        <Section title="Why it exists">
          <p>
            Background work tends to fail quietly: a message is lost between the database and the queue, a job runs twice after a
            redelivery, a worker dies mid-task, or a failure is retried forever with no one noticing. NEXUS makes each job's
            lifecycle durable and observable, so you can answer “did it run, where, how many times, and why did it fail?”.
          </p>
        </Section>

        <Section title="Architecture">
          <div className="grid items-stretch gap-2 md:grid-cols-[1fr_auto_1fr_auto_1fr]">
            <Node title="Dashboard" detail="React + TypeScript. This app." tone="accent" />
            <Arrow />
            <Node title="API" detail="FastAPI. Validates, authenticates per tenant, writes the job and its outbox message in one transaction." />
            <Arrow />
            <Node title="PostgreSQL" detail="Jobs, transitions, executions, outbox, inbox, worker registry, leader lease." />
          </div>
          <div className="grid items-stretch gap-2 md:grid-cols-[1fr_auto_1fr_auto_1fr]">
            <Node title="Scheduler" detail="Leader-elected. Relays the outbox with publisher confirms, re-dispatches retries, detects dead workers, prunes resolved dead letters." />
            <Arrow />
            <Node title="RabbitMQ" detail="nexus.jobs (priority queue) and nexus.dead-letter." />
            <Arrow />
            <Node title="Workers (×N)" detail="Register, heartbeat, consume up to their capacity, run registered handlers with a timeout, report results." />
          </div>
          <Node title="Monitoring" detail="Prometheus scrapes fleet-wide metrics computed from PostgreSQL and RabbitMQ; 11 alert rules; a provisioned Grafana dashboard." tone="amber" />
        </Section>

        <Section title="Job lifecycle">
          <pre className="overflow-x-auto rounded-lg bg-ink-950 p-4 font-mono text-xs leading-relaxed text-ink-200">{`PENDING ──► QUEUED ──► RUNNING ──► COMPLETED
   │          ▲           │
   │          │           ▼
   │       RETRYING ◄── FAILED   automatic while attempts remain;
   │                      │      dead-lettered when exhausted or permanent
   └──────────┴───────────┴────► CANCELLED`}</pre>
        </Section>

        <Section title="What the platform guarantees">
          <dl className="grid gap-4 sm:grid-cols-2">
            {GUARANTEES.map(([term, detail]) => (
              <div key={term}>
                <dt className="font-medium text-ink-100">{term}</dt>
                <dd className="mt-1 text-ink-400">{detail}</dd>
              </div>
            ))}
          </dl>
        </Section>

        <Section title="Explore this demo">
          <ul className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {TRY.map((t) => (
              <li key={t.to}>
                <Link to={t.to} className="block rounded-lg border border-ink-700 p-3 transition hover:border-accent-500/60 hover:bg-ink-800/50">
                  <span className="font-medium text-accent-400">{t.label} →</span>
                  <span className="mt-1 block text-xs text-ink-400">{t.detail}</span>
                </Link>
              </li>
            ))}
          </ul>
        </Section>

        <Section title="What is simulated here">
          <p>
            This page is a static build hosted on GitHub Pages, so there is no API, database, broker or worker behind it. The
            dashboard's API client is pointed at an in-browser simulator that answers the same endpoints. It seeds 24 hours of
            history and advances jobs with the same state machine, retry backoff and dead-letter rules as the backend.
            Outcomes are decided by the simulator: no job here is executed by a real worker. Data lives in memory and resets
            when you reload.
          </p>
          <p>
            To run the real system (PostgreSQL, RabbitMQ, Redis, scheduler, two workers, Prometheus and Grafana) locally:{' '}
            <code className="rounded bg-ink-950 px-1.5 py-0.5 font-mono text-xs text-ink-200">docker compose up --build</code>. See
            the README in the repository.
          </p>
        </Section>

        <Section title="Built with">
          <div className="flex flex-wrap gap-2">
            {['Python 3.12', 'FastAPI', 'SQLAlchemy (async)', 'Alembic', 'PostgreSQL', 'RabbitMQ', 'Redis', 'React 19', 'TypeScript', 'Vite', 'Tailwind CSS', 'TanStack Query', 'Prometheus', 'Grafana', 'Docker Compose', 'GitHub Actions', 'Vercel'].map((t) => (
              <span key={t} className="rounded-md bg-ink-800 px-2 py-1 font-mono text-xs text-ink-200">{t}</span>
            ))}
          </div>
        </Section>
      </div>
    </>
  )
}
