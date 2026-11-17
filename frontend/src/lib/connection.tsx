import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'
import { NexusClient, type Connection } from './api'

const STORAGE_KEY = 'nexus.connection'

// The API key is entered by the user and kept in this browser only; it is
// never bundled into the build.
function load(): Connection {
  const fallback: Connection = { baseUrl: import.meta.env.VITE_API_BASE_URL ?? '', apiKey: '' }
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return fallback
    const parsed = JSON.parse(raw) as Partial<Connection>
    return { baseUrl: parsed.baseUrl ?? fallback.baseUrl, apiKey: parsed.apiKey ?? '' }
  } catch {
    return fallback
  }
}

interface ConnectionContextValue {
  connection: Connection
  client: NexusClient
  setConnection: (c: Connection) => void
  disconnect: () => void
}

const ConnectionContext = createContext<ConnectionContextValue | null>(null)

export function ConnectionProvider({
  children,
  initial,
}: {
  children: ReactNode
  initial?: Connection
}) {
  const [connection, setState] = useState<Connection>(() => initial ?? load())
  const setConnection = useCallback((c: Connection) => {
    setState(c)
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(c))
    } catch {
      /* storage unavailable: keep in memory */
    }
  }, [])
  const disconnect = useCallback(() => {
    setState((c) => ({ ...c, apiKey: '' }))
    try {
      localStorage.removeItem(STORAGE_KEY)
    } catch {
      /* ignore */
    }
  }, [])
  const client = useMemo(() => new NexusClient(connection), [connection])
  const value = useMemo(
    () => ({ connection, client, setConnection, disconnect }),
    [connection, client, setConnection, disconnect],
  )
  return <ConnectionContext.Provider value={value}>{children}</ConnectionContext.Provider>
}

// eslint-disable-next-line react-refresh/only-export-components
export function useConnection(): ConnectionContextValue {
  const ctx = useContext(ConnectionContext)
  if (!ctx) throw new Error('useConnection must be used inside ConnectionProvider')
  return ctx
}
