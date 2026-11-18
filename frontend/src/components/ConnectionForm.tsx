import { useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { ApiError, NexusClient } from '../lib/api'
import { useConnection } from '../lib/connection'

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
