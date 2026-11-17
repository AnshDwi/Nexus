import { useQueryClient } from '@tanstack/react-query'
import { CheckCircle2, CircleSlash, XCircle } from 'lucide-react'
import { useState, type FormEvent } from 'react'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'
import { useToast } from '../components/Toast'
import { ApiError, NexusClient } from '../lib/api'
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

export function ConnectionForm({ compact = false }: { compact?: boolean }) {
  const { connection, setConnection } = useConnection()
  const [baseUrl, setBaseUrl] = useState(connection.baseUrl)
  const [apiKey, setApiKey] = useState(connection.apiKey)
  const [status, setStatus] = useState<{ ok: boolean; message: string } | null>(null)
  const [testing, setTesting] = useState(false)
  const qc = useQueryClient()

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setTesting(true)
    setStatus(null)
    try {
      const info = await new NexusClient({ baseUrl, apiKey }).system()
      setConnection({ baseUrl, apiKey })
      await qc.invalidateQueries()
      setStatus({ ok: true, message: `Connected as tenant “${info.tenant_id}” (${info.environment})` })
    } catch (err) {
      setStatus({ ok: false, message: err instanceof ApiError ? err.message : 'Connection failed' })
    } finally {
      setTesting(false)
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div>
        <label className="label" htmlFor="api-base">API base URL</label>
        <input id="api-base" className="input" value={baseUrl} placeholder="(same origin)" onChange={(e) => setBaseUrl(e.target.value)} />
        {!compact && <p className="mt-1 text-xs text-ink-400">Leave empty when the dashboard and API share an origin.</p>}
      </div>
      <div>
        <label className="label" htmlFor="api-key">API key</label>
        <input id="api-key" className="input font-mono" type="password" autoComplete="off" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="nx_…" />
        {!compact && <p className="mt-1 text-xs text-ink-400">Stored in this browser only. Keys are tenant-scoped.</p>}
      </div>
      {status && (
        <p role="status" className={`text-sm ${status.ok ? 'text-emerald-300' : 'text-rose-300'}`}>{status.message}</p>
      )}
      <button type="submit" className="btn-primary w-full sm:w-auto" disabled={testing || !apiKey}>
        {testing ? 'Testing…' : 'Save & test connection'}
      </button>
    </form>
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
