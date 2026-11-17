import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { renderWithApp } from '../test/render'
import { db } from '../test/server'
import { validateJobForm } from '../lib/jobForm'
import { CreateJobForm } from './CreateJobForm'

describe('validateJobForm', () => {
  const ok = { name: 'a', jobType: 'echo', priority: 'normal' as const, payload: '{}', maxAttempts: '3', timeoutSeconds: '60' }
  it('accepts a valid form', () => {
    expect(validateJobForm(ok)).toEqual({ errors: {}, payload: {} })
  })
  it('rejects bad input', () => {
    const { errors } = validateJobForm({ ...ok, name: ' ', payload: '[1]', maxAttempts: '0', timeoutSeconds: 'x' })
    expect(errors).toMatchObject({
      name: 'Name is required',
      payload: 'Payload must be a JSON object',
      maxAttempts: 'Between 1 and 20',
      timeoutSeconds: 'At least 1 second',
    })
    expect(validateJobForm({ ...ok, payload: '{oops' }).errors.payload).toBe('Payload is not valid JSON')
  })
})

describe('<CreateJobForm>', () => {
  it('shows validation errors without calling the API', async () => {
    const user = userEvent.setup()
    renderWithApp(<CreateJobForm onCreated={vi.fn()} onCancel={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /submit job/i }))
    expect(await screen.findByText('Name is required')).toBeInTheDocument()
    expect(db.requests.some((r) => r.method === 'POST')).toBe(false)
  })

  it('submits a job with an idempotency key and reports success', async () => {
    const user = userEvent.setup()
    const onCreated = vi.fn()
    renderWithApp(<CreateJobForm onCreated={onCreated} onCancel={vi.fn()} />)
    await user.type(screen.getByLabelText('Name'), 'nightly-report')
    await user.selectOptions(screen.getByLabelText('Priority'), 'critical')
    await waitFor(() => expect(screen.getByRole('option', { name: 'test.fail' })).toBeInTheDocument())
    await user.click(screen.getByRole('button', { name: /submit job/i }))
    await waitFor(() => expect(onCreated).toHaveBeenCalledOnce())
    const post = db.requests.find((r) => r.method === 'POST')!
    expect(post.headers.get('Idempotency-Key')).toBeTruthy()
    expect(await post.json()).toMatchObject({ name: 'nightly-report', priority: 'critical', payload: { message: 'hello' } })
    expect(db.jobs[0].name).toBe('nightly-report')
  })

  it('surfaces a server-side rejection', async () => {
    const user = userEvent.setup()
    renderWithApp(<CreateJobForm onCreated={vi.fn()} onCancel={vi.fn()} />)
    await waitFor(() => expect(screen.getByRole('option', { name: 'test.fail' })).toBeInTheDocument())
    // Force an unknown job type through the select.
    const select = screen.getByLabelText('Job type') as HTMLSelectElement
    const option = document.createElement('option')
    option.value = 'unknown'
    option.textContent = 'unknown'
    select.appendChild(option)
    await user.selectOptions(select, 'unknown')
    await user.type(screen.getByLabelText('Name'), 'bad')
    await user.click(screen.getByRole('button', { name: /submit job/i }))
    expect(await screen.findByRole('alert')).toHaveTextContent("unknown job type 'unknown'")
  })
})
