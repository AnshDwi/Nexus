import { QueryClientProvider } from '@tanstack/react-query'
import { lazy, Suspense, useState, type ReactNode } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { ToastProvider } from './components/Toast'
import type { Connection } from './lib/api'
import { makeQueryClient } from './lib/queryClient'
import { ConnectionProvider, useConnection } from './lib/connection'
import { ConnectPage } from './pages/ConnectPage'
import { LoadingState } from './components/States'

// Route-level code splitting: the chart library only loads with Overview.
const OverviewPage = lazy(() => import('./pages/OverviewPage').then((m) => ({ default: m.OverviewPage })))
const JobsPage = lazy(() => import('./pages/JobsPage').then((m) => ({ default: m.JobsPage })))
const WorkersPage = lazy(() => import('./pages/WorkersPage').then((m) => ({ default: m.WorkersPage })))
const QueuePage = lazy(() => import('./pages/QueuePage').then((m) => ({ default: m.QueuePage })))
const ExecutionsPage = lazy(() => import('./pages/ExecutionsPage').then((m) => ({ default: m.ExecutionsPage })))
const SettingsPage = lazy(() => import('./pages/SettingsPage').then((m) => ({ default: m.SettingsPage })))
// Demo builds only. __NEXUS_DEMO__ is replaced by a literal at build time
// (vite.config.ts), so other builds emit no About chunk and no simulator.
const AboutPage = __NEXUS_DEMO__ ? lazy(() => import('./demo/AboutPage').then((m) => ({ default: m.AboutPage }))) : null


export function AppRoutes() {
  const { connection } = useConnection()
  if (!connection.apiKey) return <ConnectPage />
  return (
    <Layout>
      <Suspense fallback={<LoadingState rows={6} />}>
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/jobs" element={<JobsPage />} />
        <Route path="/jobs/:jobId" element={<JobsPage />} />
        <Route path="/workers" element={<WorkersPage />} />
        <Route path="/queue" element={<QueuePage />} />
        <Route path="/executions" element={<ExecutionsPage />} />
        <Route path="/settings" element={<SettingsPage />} />
        {AboutPage && <Route path="/about" element={<AboutPage />} />}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
      </Suspense>
    </Layout>
  )
}

export function Providers({ children, connection }: { children: ReactNode; connection?: Connection }) {
  const [client] = useState(makeQueryClient)
  return (
    <QueryClientProvider client={client}>
      <ConnectionProvider initial={connection}>
        <ToastProvider>{children}</ToastProvider>
      </ConnectionProvider>
    </QueryClientProvider>
  )
}

export default function App() {
  return (
    <Providers>
      {/* BASE_URL is "/" normally and "/Nexus/" in the GitHub Pages build. */}
      <BrowserRouter basename={import.meta.env.BASE_URL.replace(/\/$/, '') || '/'}>
        <AppRoutes />
      </BrowserRouter>
    </Providers>
  )
}
