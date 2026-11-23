import clsx from 'clsx'
import type { LucideIcon } from 'lucide-react'
import type { ReactNode } from 'react'

export function StatCard({
  label,
  value,
  icon: Icon,
  tone = 'default',
  hint,
}: {
  label: string
  value: ReactNode
  icon: LucideIcon
  tone?: 'default' | 'blue' | 'green' | 'red' | 'amber'
  hint?: ReactNode
}) {
  const tones = {
    default: 'text-ink-300 bg-ink-800',
    blue: 'text-blue-300 bg-blue-500/10',
    green: 'text-emerald-300 bg-emerald-500/10',
    red: 'text-rose-300 bg-rose-500/10',
    amber: 'text-amber-300 bg-amber-500/10',
  }
  return (
    <div className="card p-4" data-testid={`stat-${label.toLowerCase().replace(/\s+/g, '-')}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs font-medium uppercase tracking-wide text-ink-400">{label}</p>
          <p className="mt-2 text-2xl font-semibold tabular-nums text-ink-100">{value}</p>
        </div>
        <div className={clsx('rounded-lg p-2', tones[tone])}>
          <Icon className="h-4 w-4" aria-hidden />
        </div>
      </div>
      {hint && <div className="mt-2 text-xs text-ink-400">{hint}</div>}
    </div>
  )
}
