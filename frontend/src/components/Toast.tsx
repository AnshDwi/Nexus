import clsx from 'clsx'
import { CheckCircle2, XCircle, X } from 'lucide-react'
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'

type Tone = 'success' | 'error'
interface Toast {
  id: number
  tone: Tone
  title: string
  description?: string
}

const ToastContext = createContext<{ notify: (t: Omit<Toast, 'id'>) => void } | null>(null)

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([])
  const dismiss = useCallback((id: number) => setToasts((all) => all.filter((t) => t.id !== id)), [])
  const notify = useCallback(
    (t: Omit<Toast, 'id'>) => {
      const id = Date.now() + Math.random()
      setToasts((all) => [...all, { ...t, id }])
      setTimeout(() => dismiss(id), 5000)
    },
    [dismiss],
  )
  const value = useMemo(() => ({ notify }), [notify])
  return (
    <ToastContext.Provider value={value}>
      {children}
      <div aria-live="polite" className="pointer-events-none fixed bottom-4 right-4 z-50 flex w-80 flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            role="status"
            className={clsx(
              'pointer-events-auto flex items-start gap-3 rounded-lg border p-3 shadow-xl backdrop-blur',
              t.tone === 'success' ? 'border-emerald-500/30 bg-ink-850/95' : 'border-rose-500/30 bg-ink-850/95',
            )}
          >
            {t.tone === 'success' ? (
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-400" aria-hidden />
            ) : (
              <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-rose-400" aria-hidden />
            )}
            <div className="min-w-0 flex-1">
              <p className="text-sm font-medium text-ink-100">{t.title}</p>
              {t.description && <p className="mt-0.5 break-words text-xs text-ink-300">{t.description}</p>}
            </div>
            <button type="button" aria-label="Dismiss" className="text-ink-400 hover:text-ink-100" onClick={() => dismiss(t.id)}>
              <X className="h-4 w-4" />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}

// eslint-disable-next-line react-refresh/only-export-components
export function useToast() {
  const ctx = useContext(ToastContext)
  if (!ctx) throw new Error('useToast must be used inside ToastProvider')
  return ctx
}
