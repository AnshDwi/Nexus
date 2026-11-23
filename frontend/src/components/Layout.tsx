import clsx from 'clsx'
import {
  Activity,
  BookOpen,
  FlaskConical,
  Gauge,
  History,
  Layers,
  ListChecks,
  Menu,
  Server,
  Settings as SettingsIcon,
  X,
} from 'lucide-react'
import { useState, type ReactNode } from 'react'
import { NavLink } from 'react-router-dom'
import { DemoBanner } from '../demo/DemoBanner'
import { useReady, useSystem } from '../lib/queries'

const NAV = [
  { to: '/', label: 'Overview', icon: Gauge, end: true },
  { to: '/jobs', label: 'Jobs', icon: ListChecks },
  { to: '/workers', label: 'Workers', icon: Server },
  { to: '/queue', label: 'Queue', icon: Layers },
  { to: '/executions', label: 'Execution History', icon: History },
  { to: '/settings', label: 'Settings', icon: SettingsIcon },
  ...(__NEXUS_DEMO__ ? [{ to: '/about', label: 'About NEXUS', icon: BookOpen }] : []),
]

function ConnectionIndicator() {
  const ready = useReady()
  const system = useSystem()
  const ok = ready.data?.status === 'ok' && !system.error
  const label = ready.isLoading ? 'Connecting…' : __NEXUS_DEMO__ ? 'Simulated API' : ok ? 'API connected' : 'API degraded'
  return (
    <div className="flex items-center gap-2 text-xs text-ink-300" title={label}>
      <span className={clsx('h-2 w-2 rounded-full', ready.isLoading ? 'bg-ink-400' : __NEXUS_DEMO__ ? 'bg-amber-400' : ok ? 'bg-emerald-400' : 'bg-rose-400')} aria-hidden />
      <span className="hidden sm:inline">{label}</span>
      {system.data && (
        <span className="hidden rounded-md bg-ink-800 px-2 py-0.5 font-mono md:inline">
          {system.data.tenant_id} · {system.data.environment}
        </span>
      )}
    </div>
  )
}

function Sidebar({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <nav aria-label="Main" className="flex h-full flex-col gap-1 p-3">
      <div className="mb-6 flex items-center gap-2.5 px-2 pt-2">
        <div className="grid h-8 w-8 place-items-center rounded-lg bg-gradient-to-br from-accent-500 to-indigo-600 font-bold text-white shadow-lg shadow-accent-500/20">
          N
        </div>
        <div>
          <p className="text-sm font-semibold tracking-wide text-ink-100">NEXUS</p>
          <p className="text-[11px] text-ink-400">Developer Platform</p>
        </div>
      </div>
      {NAV.map(({ to, label, icon: Icon, end }) => (
        <NavLink
          key={to}
          to={to}
          end={end}
          onClick={onNavigate}
          className={({ isActive }) =>
            clsx(
              'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition',
              isActive ? 'bg-accent-500/15 text-accent-400' : 'text-ink-300 hover:bg-ink-800 hover:text-ink-100',
            )
          }
        >
          <Icon className="h-4 w-4" aria-hidden />
          {label}
        </NavLink>
      ))}
      <div className="mt-auto flex items-center gap-2 rounded-lg border border-ink-700 bg-ink-850 px-3 py-2 text-xs text-ink-400">
        {__NEXUS_DEMO__ ? (
          <><FlaskConical className="h-3.5 w-3.5" aria-hidden /> Demo · simulated in browser</>
        ) : (
          <><Activity className="h-3.5 w-3.5" aria-hidden /> Live · refreshes every 3s</>
        )}
      </div>
    </nav>
  )
}

export function Layout({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="flex h-full">
      <aside className="hidden w-60 shrink-0 border-r border-ink-700 bg-ink-950/60 lg:block">
        <Sidebar />
      </aside>
      {open && (
        <div className="fixed inset-0 z-40 lg:hidden">
          <div className="absolute inset-0 bg-ink-950/70" onClick={() => setOpen(false)} aria-hidden />
          <aside className="relative h-full w-64 border-r border-ink-700 bg-ink-900">
            <button type="button" className="btn-ghost absolute right-2 top-2 p-1.5" aria-label="Close menu" onClick={() => setOpen(false)}>
              <X className="h-4 w-4" />
            </button>
            <Sidebar onNavigate={() => setOpen(false)} />
          </aside>
        </div>
      )}
      <div className="flex min-w-0 flex-1 flex-col">
        {__NEXUS_DEMO__ && <DemoBanner />}
        <header className="flex h-14 items-center justify-between gap-3 border-b border-ink-700 bg-ink-900/80 px-4 backdrop-blur sm:px-6">
          <button type="button" className="btn-ghost p-1.5 lg:hidden" aria-label="Open menu" onClick={() => setOpen(true)}>
            <Menu className="h-5 w-5" />
          </button>
          <div className="lg:hidden font-semibold">NEXUS</div>
          <div className="ml-auto">
            <ConnectionIndicator />
          </div>
        </header>
        <main className="min-w-0 flex-1 overflow-y-auto px-4 py-6 sm:px-6 lg:px-8">
          <div className="mx-auto max-w-7xl">{children}</div>
        </main>
      </div>
    </div>
  )
}
