/**
 * The whole app as the GitHub Pages build runs it: demo mode on, served under
 * /Nexus/. Runs with `npm run test:demo` (vitest --mode pages); the default
 * test run builds without demo mode, so it skips this file.
 */
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from '../App'
import { resetDemo } from '.'

function open(path: string) {
  window.history.pushState({}, '', path)
  return render(<App />)
}

describe.skipIf(!__NEXUS_DEMO__)('demo build (GitHub Pages)', () => {
  let network: ReturnType<typeof vi.spyOn>
  beforeEach(() => {
    resetDemo()
    // Vitest always reports BASE_URL "/"; the Pages build sets "/Nexus/"
    // (asset paths in the built files are checked by scripts/check-pages-build.mjs).
    vi.stubEnv('BASE_URL', '/Nexus/')
    network = vi.spyOn(globalThis, 'fetch')
  })
  afterEach(() => {
    expect(network).not.toHaveBeenCalled() // nothing ever leaves the browser
    vi.unstubAllEnvs()
    window.history.pushState({}, '', '/')
  })

  it('opens straight into the dashboard with a clear demo notice', async () => {
    open('/Nexus/')
    expect(screen.queryByText('Connect to NEXUS')).not.toBeInTheDocument()
    const notice = screen.getByRole('note', { name: 'Demo mode' })
    expect(notice).toHaveTextContent(/Simulated data in your browser: no backend, no real workers/)
    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(await screen.findByText('Simulated API')).toBeInTheDocument()
    expect(await screen.findByText(/demo · demo/)).toBeInTheDocument() // tenant · environment
  })

  it('renders deep links, including a job detail, and redirects unknown paths', async () => {
    open('/Nexus/jobs?dead_lettered=1')
    expect(await screen.findByRole('heading', { name: 'Jobs' })).toBeInTheDocument()
    const rows = await screen.findAllByText('dead-lettered')
    expect(rows.length).toBeGreaterThan(0)
    await userEvent.click(screen.getAllByRole('row')[1])
    const dialog = await screen.findByRole('dialog')
    expect(within(dialog).getByText('Job details')).toBeInTheDocument()
    expect(window.location.pathname).toMatch(/^\/Nexus\/jobs\/[0-9a-f-]{36}$/)
  })

  it('redirects an unknown route to the overview', async () => {
    open('/Nexus/does-not-exist')
    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(window.location.pathname).toMatch(/^\/Nexus\/?$/) // Pages serves /Nexus and /Nexus/
  })

  it('retries dead-lettered jobs in bulk against the simulator', async () => {
    const user = userEvent.setup()
    open('/Nexus/jobs?dead_lettered=1')
    await user.type(await screen.findByRole('textbox', { name: 'Filter by last error' }), '503')
    const button = await screen.findByRole('button', { name: /Retry all matching \(\d+\)/ })
    await user.click(button)
    await user.click(screen.getByRole('button', { name: 'Confirm retry' }))
    expect(await screen.findByText(/^Retrying \d+ jobs?$/)).toBeInTheDocument()
  })

  it('explains the system and what is simulated on the About page', async () => {
    open('/Nexus/about')
    expect(await screen.findByRole('heading', { name: 'About NEXUS' })).toBeInTheDocument()
    expect(screen.getByRole('region', { name: 'Architecture' })).toHaveTextContent('RabbitMQ')
    expect(screen.getByRole('region', { name: 'What is simulated here' })).toHaveTextContent('no job here is executed by a real worker')
    expect(screen.getByRole('link', { name: /Dead-letter recovery/ })).toHaveAttribute('href', '/Nexus/jobs?dead_lettered=1')
  })

  it('shows no API key form or sign-out in Settings', async () => {
    open('/Nexus/settings')
    expect(await screen.findByText(/talks to a simulator running in your browser/)).toBeInTheDocument()
    expect(screen.queryByLabelText('API key')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Sign out' })).not.toBeInTheDocument()
    expect((await screen.findAllByText('simulated')).length).toBe(3)
  })
})
