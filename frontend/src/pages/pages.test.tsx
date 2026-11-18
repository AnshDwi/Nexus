import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { MemoryRouter } from 'react-router-dom'
import { render } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { AppRoutes, Providers } from '../App'
import { makeJob } from '../test/fixtures'
import { renderWithApp } from '../test/render'
import { API, API_KEY, db, server } from '../test/server'
import { ExecutionsPage } from './ExecutionsPage'
import { OverviewPage } from './OverviewPage'
import { QueuePage } from './QueuePage'
import { SettingsPage } from './SettingsPage'
import { WorkersPage } from './WorkersPage'

describe('pages', () => {
  it('Overview shows the platform summary', async () => {
    renderWithApp(<OverviewPage />)
    expect(await screen.findByTestId('stat-total-jobs')).toHaveTextContent('42')
    expect(screen.getByTestId('stat-running')).toHaveTextContent('3')
    expect(screen.getByTestId('stat-failed')).toHaveTextContent('4')
    expect(screen.getByTestId('stat-worker-health')).toHaveTextContent('2/2')
    expect(screen.getByText('nightly-report')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: '2 dead-lettered' })).toHaveAttribute('href', '/jobs?dead_lettered=1')
  })

  it('Overview shows an error state with retry when the API fails', async () => {
    server.use(http.get(`${API}/metrics/summary`, () => HttpResponse.json({ detail: { code: 'internal_error', message: 'internal server error' } }, { status: 500 })))
    renderWithApp(<OverviewPage />)
    expect(await screen.findByRole('alert', {}, { timeout: 5000 })).toHaveTextContent('internal server error')
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument()
  })

  it('Workers lists heartbeat and workload', async () => {
    renderWithApp(<WorkersPage />)
    expect(await screen.findByText('healthy')).toBeInTheDocument()
    expect(screen.getByText('1/4')).toBeInTheDocument()
  })

  it('Queue shows depth and dispatch status', async () => {
    db.jobs = [makeJob({ name: 'waiting', state: 'queued' })]
    renderWithApp(<QueuePage />)
    expect(await screen.findByText('waiting')).toBeInTheDocument()
    expect(screen.getByText('Confirmed by broker')).toBeInTheDocument()
    expect(screen.getByTestId('stat-dead-lettered')).toHaveTextContent('0')
  })

  it('Execution history shows failure reasons', async () => {
    renderWithApp(<ExecutionsPage />)
    expect(await screen.findByText('RuntimeError: boom')).toBeInTheDocument()
    expect(screen.getByText('1.23 s')).toBeInTheDocument()
    expect(screen.getByText('retry')).toBeInTheDocument()
  })

  it('Settings reports system health and configuration', async () => {
    renderWithApp(<SettingsPage />)
    expect(await screen.findByText('not configured')).toBeInTheDocument()
    expect(screen.getAllByText('ok')).toHaveLength(2)
    expect(screen.getByText('rabbitmq', { selector: 'dd' })).toBeInTheDocument()
  })
})

describe('connection flow', () => {
  it('asks for an API key, validates it against the API, then opens the dashboard', async () => {
    const user = userEvent.setup()
    render(
      <Providers connection={{ baseUrl: 'http://nexus.test', apiKey: '' }}>
        <MemoryRouter><AppRoutes /></MemoryRouter>
      </Providers>,
    )
    expect(screen.getByRole('heading', { name: 'Connect to NEXUS' })).toBeInTheDocument()
    await user.type(screen.getByLabelText('API key'), 'wrong')
    await user.click(screen.getByRole('button', { name: /save & test/i }))
    expect(await screen.findByText('invalid or missing credentials')).toBeInTheDocument()

    await user.clear(screen.getByLabelText('API key'))
    await user.type(screen.getByLabelText('API key'), API_KEY)
    await user.click(screen.getByRole('button', { name: /save & test/i }))
    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(JSON.parse(localStorage.getItem('nexus.connection')!)).toMatchObject({ apiKey: API_KEY })
  })
})
