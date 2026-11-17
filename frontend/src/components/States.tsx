import { AlertTriangle, Inbox, RefreshCw } from 'lucide-react'
import type { ReactNode } from 'react'
import { ApiError } from '../lib/api'

export function LoadingState({ rows = 4, label = 'Loading' }: { rows?: number; label?: string }) {
  return (
    <div role="status" aria-label={label} className="space-y-3 p-4">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="h-9 animate-pulse rounded-lg bg-ink-800" />
      ))}
      <span className="sr-only">{label}…</span>
    </div>
  )
}

export function EmptyState({
  title,
  description,
  action,
}: {
  title: string
  description?: string
  action?: ReactNode
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 px-6 py-14 text-center">
      <div className="rounded-full bg-ink-800 p-3 text-ink-400">
        <Inbox className="h-6 w-6" aria-hidden />
      </div>
      <div>
        <p className="font-medium text-ink-100">{title}</p>
        {description && <p className="mt-1 text-sm text-ink-400">{description}</p>}
      </div>
      {action}
    </div>
  )
}

export function ErrorState({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const message = error instanceof ApiError ? error.message : 'Something went wrong'
  const requestId = error instanceof ApiError ? error.requestId : null
  return (
    <div role="alert" className="flex flex-col items-center gap-3 px-6 py-12 text-center">
      <div className="rounded-full bg-rose-500/10 p-3 text-rose-300">
        <AlertTriangle className="h-6 w-6" aria-hidden />
      </div>
      <div>
        <p className="font-medium text-ink-100">{message}</p>
        {requestId && <p className="mt-1 font-mono text-xs text-ink-400">request {requestId}</p>}
      </div>
      {onRetry && (
        <button type="button" className="btn-secondary" onClick={onRetry}>
          <RefreshCw className="h-4 w-4" aria-hidden /> Try again
        </button>
      )}
    </div>
  )
}
