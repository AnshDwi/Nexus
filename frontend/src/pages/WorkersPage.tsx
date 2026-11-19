import { WorkerStatusBadge } from '../components/Badges'
import { PageHeader } from '../components/PageHeader'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import { formatDateTime, formatRelative } from '../lib/format'
import { useWorkers } from '../lib/queries'

export function WorkersPage() {
  const { data, isLoading, error, refetch } = useWorkers()
  return (
    <>
      <PageHeader title="Workers" description="Registered workers, their heartbeats and current workload." />
      <div className="card overflow-hidden">
        {isLoading ? (
          <LoadingState label="Loading workers" />
        ) : error ? (
          <ErrorState error={error} onRetry={() => void refetch()} />
        ) : !data || data.length === 0 ? (
          <EmptyState title="No workers registered" description="Start a worker: python -m nexus.daemon worker" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[760px]">
              <thead className="border-b border-ink-700 bg-ink-900/50">
                <tr>
                  <th className="th">Worker</th><th className="th">Status</th><th className="th">Last heartbeat</th>
                  <th className="th">Workload</th><th className="th">Host</th><th className="th">Registered</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-700">
                {data.map((w) => (
                  <tr key={w.worker_id}>
                    <td className="td font-mono text-xs">{w.worker_id}</td>
                    <td className="td"><WorkerStatusBadge status={w.status} /></td>
                    <td className="td">
                      <div>{formatRelative(w.last_heartbeat_at)}</div>
                      <div className="text-xs text-ink-400">{w.heartbeat_age_seconds}s ago</div>
                    </td>
                    <td className="td">
                      <div className="flex items-center gap-2">
                        <div className="h-1.5 w-24 overflow-hidden rounded-full bg-ink-700" role="progressbar" aria-valuenow={w.running_jobs} aria-valuemax={w.capacity_slots} aria-label="Worker utilisation">
                          <div className="h-full rounded-full bg-accent-500" style={{ width: `${Math.min(w.utilization * 100, 100)}%` }} />
                        </div>
                        <span className="tabular-nums text-xs">{w.running_jobs}/{w.capacity_slots}</span>
                      </div>
                    </td>
                    <td className="td text-xs text-ink-300">
                      {w.capabilities.cpu_cores} CPU · {Math.round(w.capabilities.memory_mb / 1024)} GB · {w.capabilities.architecture} · {w.capabilities.region}
                    </td>
                    <td className="td text-ink-300">{formatDateTime(w.registered_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </>
  )
}
