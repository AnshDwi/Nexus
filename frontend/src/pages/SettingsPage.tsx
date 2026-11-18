import { CheckCircle2, CircleSlash, XCircle } from 'lucide-react'
import { ConnectionForm } from '../components/ConnectionForm'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'
import { useToast } from '../components/Toast'
import { useConnection } from '../lib/connection'
import { useSystem } from '../lib/queries'

function CheckRow({ name, value }: { name: string; value: string }) {
  const Icon = value === 'ok' ? CheckCircle2 : value === 'not_configured' ? CircleSlash : XCircle
  const tone = value === 'ok' ? 'text-emerald-400' : value === 'not_configured' ? 'text-ink-400' : 'text-rose-400'
  return (
    <li className="flex items-center justify-between py-2.5">
      <span className="text-sm capitalize text-ink-200">{name}</span>
      <span className={`flex items-center gap-1.5 text-sm ${tone}`}><Icon className="h-4 w-4" aria-hidden />{value.replace('_', ' ')}</span>
    </li>
  )
}

export function SettingsPage() {
  const { data, isLoading, error, refetch } = useSystem()
  const { disconnect } = useConnection()
  const { notify } = useToast()
  return (
    <>
      <PageHeader title="Settings" description="API connection, system health and active configuration." />
      <div className="grid gap-6 lg:grid-cols-2">
        <section className="card p-5" aria-label="API connection">
          <h2 className="mb-4 text-sm font-semibold text-ink-100">API connection</h2>
          <ConnectionForm />
          <button type="button" className="btn-ghost mt-3 text-xs" onClick={() => { disconnect(); notify({ tone: 'success', title: 'API key removed from this browser' }) }}>
            Sign out
          </button>
        </section>
        <section className="card p-5" aria-label="System health">
          <h2 className="mb-2 text-sm font-semibold text-ink-100">System health</h2>
          {isLoading ? <LoadingState rows={3} /> : error || !data ? <ErrorState error={error} onRetry={() => void refetch()} /> : (
            <>
              <ul className="divide-y divide-ink-700">
                {Object.entries(data.checks).map(([k, v]) => <CheckRow key={k} name={k} value={v} />)}
              </ul>
              <dl className="mt-4 grid grid-cols-2 gap-3 text-sm">
                <div><dt className="text-xs text-ink-400">Version</dt><dd className="font-mono">{data.version}</dd></div>
                <div><dt className="text-xs text-ink-400">Environment</dt><dd>{data.environment}</dd></div>
                <div><dt className="text-xs text-ink-400">Tenant</dt><dd className="font-mono">{data.tenant_id}</dd></div>
                <div><dt className="text-xs text-ink-400">Messaging</dt><dd>{data.event_bus}</dd></div>
              </dl>
            </>
          )}
        </section>
        {data && (
          <section className="card p-5 lg:col-span-2" aria-label="Configuration">
            <h2 className="mb-3 text-sm font-semibold text-ink-100">Configuration</h2>
            <p className="mb-3 text-xs text-ink-400">Non-secret runtime settings reported by the API.</p>
            <div className="mb-4 flex flex-wrap gap-2">
              {data.job_types.map((t) => <span key={t} className="rounded-md bg-ink-800 px-2 py-1 font-mono text-xs text-ink-200">{t}</span>)}
            </div>
            <dl className="grid gap-x-6 gap-y-2 sm:grid-cols-2">
              {Object.entries(data.config).map(([k, v]) => (
                <div key={k} className="flex justify-between gap-4 border-b border-ink-800 py-1.5 text-sm">
                  <dt className="text-ink-400">{k.replaceAll('_', ' ')}</dt>
                  <dd className="truncate font-mono text-ink-200">{Array.isArray(v) ? v.join(', ') || '—' : String(v)}</dd>
                </div>
              ))}
            </dl>
          </section>
        )}
      </div>
    </>
  )
}
