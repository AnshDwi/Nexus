import { useQueryClient } from '@tanstack/react-query'
import { ExternalLink, FlaskConical, RotateCcw } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useToast } from '../components/Toast'
import { resetDemo } from '.'
import { REPOSITORY_URL } from './mode'

/** Always-visible notice that the dashboard is running on simulated data. */
export function DemoBanner() {
  const qc = useQueryClient()
  const { notify } = useToast()
  return (
    <div
      role="note"
      aria-label="Demo mode"
      className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-amber-500/30 bg-amber-500/10 px-4 py-2 text-xs text-amber-200 sm:px-6"
    >
      <span className="order-1 flex items-center gap-1.5 font-semibold uppercase tracking-wide text-amber-300">
        <FlaskConical className="h-3.5 w-3.5" aria-hidden /> Demo mode
      </span>
      {/* Full-width second row on phones; inline between label and links from md up. */}
      <span className="order-3 min-w-0 basis-full md:order-2 md:basis-0 md:flex-1">
        Simulated data in your browser: no backend, no real workers. Jobs follow the real NEXUS state machine, so
        retries, dead-letters and recovery behave as they would in production.
      </span>
      <span className="order-2 ml-auto flex items-center gap-3 md:order-3 md:ml-0">
        <Link to="/about" className="font-medium text-amber-100 underline-offset-2 hover:underline">
          How it works
        </Link>
        <a href={REPOSITORY_URL} target="_blank" rel="noreferrer" className="flex items-center gap-1 font-medium text-amber-100 underline-offset-2 hover:underline">
          Source <ExternalLink className="h-3 w-3" aria-hidden />
        </a>
        <button
          type="button"
          className="flex items-center gap-1 font-medium text-amber-100 underline-offset-2 hover:underline"
          onClick={() => {
            resetDemo()
            void qc.invalidateQueries()
            notify({ tone: 'success', title: 'Demo data reset', description: 'The simulation restarted from its seeded state.' })
          }}
        >
          <RotateCcw className="h-3.5 w-3.5" aria-hidden /> Reset
        </button>
      </span>
    </div>
  )
}
