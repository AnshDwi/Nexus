import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { makeJob } from '../test/fixtures'
import { renderWithApp } from '../test/render'
import { db } from '../test/server'
import { JobsPage } from './JobsPage'

describe('<JobsPage>', () => {
  it('shows an empty state', async () => {
    renderWithApp(<JobsPage />, { route: '/jobs' })
    expect(await screen.findByText('No jobs yet')).toBeInTheDocument()
  })

  it('lists jobs and filters by status through the API', async () => {
    db.jobs = [makeJob({ name: 'build-api', state: 'running' }), makeJob({ name: 'broken', state: 'failed' })]
    const user = userEvent.setup()
    renderWithApp(<JobsPage />, { route: '/jobs' })
    expect(await screen.findByText('build-api')).toBeInTheDocument()
    expect(screen.getByText('broken')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'failed' }))
    await waitFor(() => expect(screen.queryByText('build-api')).not.toBeInTheDocument())
    expect(screen.getByText('broken')).toBeInTheDocument()
    const last = new URL(db.requests.filter((r) => r.url.includes('/jobs?')).at(-1)!.url)
    expect(last.searchParams.getAll('status')).toEqual(['failed'])
  })

  it('filters dead-lettered jobs and marks them in the table', async () => {
    db.jobs = [
      makeJob({ name: 'exhausted', state: 'failed', dead_lettered_at: new Date().toISOString() }),
      makeJob({ name: 'fine', state: 'completed' }),
    ]
    const user = userEvent.setup()
    renderWithApp(<JobsPage />, { route: '/jobs' })
    expect(await screen.findByText('fine')).toBeInTheDocument()
    expect(screen.getByText('dead-lettered')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Dead-lettered' }))
    await waitFor(() => expect(screen.queryByText('fine')).not.toBeInTheDocument())
    expect(screen.getByText('exhausted')).toBeInTheDocument()
    const last = new URL(db.requests.filter((r) => r.url.includes('/jobs?')).at(-1)!.url)
    expect(last.searchParams.get('dead_lettered')).toBe('true')
  })

  it('cancels an eligible job and retries a failed one', async () => {
    db.jobs = [makeJob({ name: 'waiting', state: 'queued' }), makeJob({ name: 'broken', state: 'failed' })]
    const user = userEvent.setup()
    renderWithApp(<JobsPage />, { route: '/jobs' })

    await user.click(await screen.findByRole('button', { name: 'Cancel waiting' }))
    expect(await screen.findByText('Cancelled waiting')).toBeInTheDocument()
    expect(db.jobs[0].state).toBe('cancelled')

    expect(screen.queryByRole('button', { name: 'Retry waiting' })).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Retry broken' }))
    expect(await screen.findByText('Retrying broken')).toBeInTheDocument()
    expect(db.jobs[1].state).toBe('retrying')
  })

  it('opens job details with execution history', async () => {
    const job = makeJob({ name: 'with-history', state: 'failed', last_error: 'RuntimeError: boom' })
    db.jobs = [job]
    renderWithApp(<JobsPage />, { route: `/jobs/${job.job_id}`, path: '/jobs/:jobId' })
    const dialog = await screen.findByRole('dialog', { name: 'Job details' })
    expect(await within(dialog).findByText(/Last error:/)).toBeInTheDocument()
    expect(within(dialog).getByText('dispatch confirmed by broker')).toBeInTheDocument()
    expect(within(dialog).getByRole('button', { name: /retry/i })).toBeInTheDocument()
  })

  it('creates a job from the dialog', async () => {
    const user = userEvent.setup()
    renderWithApp(<JobsPage />, { route: '/jobs?create=1' })
    const dialog = await screen.findByRole('dialog', { name: 'Create job' })
    await user.type(within(dialog).getByLabelText('Name'), 'from-ui')
    await user.click(within(dialog).getByRole('button', { name: /submit job/i }))
    expect(await screen.findByText('Job submitted')).toBeInTheDocument()
    expect(db.jobs[0].name).toBe('from-ui')
  })
})
