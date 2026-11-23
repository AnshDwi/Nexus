import { ChevronLeft, ChevronRight } from 'lucide-react'

export function Pagination({
  page,
  pages,
  total,
  onPage,
}: {
  page: number
  pages: number
  total: number
  onPage: (p: number) => void
}) {
  return (
    <div className="flex items-center justify-between gap-3 border-t border-ink-700 px-4 py-3 text-sm text-ink-400">
      <span>
        {total} result{total === 1 ? '' : 's'}
      </span>
      <div className="flex items-center gap-2">
        <button type="button" className="btn-secondary px-2 py-1" disabled={page <= 1} onClick={() => onPage(page - 1)} aria-label="Previous page">
          <ChevronLeft className="h-4 w-4" />
        </button>
        <span className="tabular-nums text-ink-300">
          Page {page} of {pages}
        </span>
        <button type="button" className="btn-secondary px-2 py-1" disabled={page >= pages} onClick={() => onPage(page + 1)} aria-label="Next page">
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}
