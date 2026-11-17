import { ConnectionForm } from './SettingsPage'

export function ConnectPage() {
  return (
    <div className="grid min-h-full place-items-center bg-[radial-gradient(ellipse_at_top,rgba(59,130,246,0.15),transparent_60%)] p-6">
      <div className="card w-full max-w-md p-6">
        <div className="mb-6 flex items-center gap-3">
          <div className="grid h-10 w-10 place-items-center rounded-xl bg-gradient-to-br from-accent-500 to-indigo-600 text-lg font-bold text-white">N</div>
          <div>
            <h1 className="text-lg font-semibold">Connect to NEXUS</h1>
            <p className="text-sm text-ink-400">Enter an API key to open the dashboard.</p>
          </div>
        </div>
        <ConnectionForm compact />
        <p className="mt-4 text-xs text-ink-400">Local development key: <code className="font-mono text-ink-200">dev-key</code> (see .env.example).</p>
      </div>
    </div>
  )
}
