import { useState, type FormEvent } from 'react'
import { ApiError } from '../lib/api'
import { newIdempotencyKey } from '../lib/format'
import { useCreateJob, useSystem } from '../lib/queries'
import { validateJobForm, type FormErrors, type JobFormState } from '../lib/jobForm'
import { JOB_PRIORITIES, type Job } from '../lib/types'

const DEFAULTS: JobFormState = {
  name: '',
  jobType: 'echo',
  priority: 'normal',
  payload: '{\n  "message": "hello"\n}',
  maxAttempts: '3',
  timeoutSeconds: '60',
}

export function CreateJobForm({ onCreated, onCancel }: { onCreated: (job: Job) => void; onCancel: () => void }) {
  const system = useSystem()
  const create = useCreateJob()
  const [form, setForm] = useState<JobFormState>(DEFAULTS)
  const [errors, setErrors] = useState<FormErrors>({})
  // One key per form instance: double-clicks or network retries create one job.
  const [idempotencyKey] = useState(newIdempotencyKey)
  const jobTypes = system.data?.job_types ?? ['echo']

  const set = (key: keyof JobFormState) => (value: string) => setForm((f) => ({ ...f, [key]: value }))

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    const { errors: found, payload } = validateJobForm(form)
    setErrors(found)
    if (Object.keys(found).length > 0 || !payload) return
    try {
      const job = await create.mutateAsync({
        idempotencyKey,
        input: {
          name: form.name.trim(),
          job_type: form.jobType,
          priority: form.priority,
          payload,
          max_attempts: Number(form.maxAttempts),
          timeout_seconds: Number(form.timeoutSeconds),
        },
      })
      onCreated(job)
    } catch (err) {
      setErrors({ form: err instanceof ApiError ? err.message : 'Could not create the job' })
    }
  }

  const field = (key: keyof JobFormState) =>
    errors[key] ? (
      <p className="mt-1 text-xs text-rose-300" id={`${key}-error`}>
        {errors[key]}
      </p>
    ) : null

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      {errors.form && (
        <div role="alert" className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">
          {errors.form}
        </div>
      )}
      <div>
        <label className="label" htmlFor="job-name">Name</label>
        <input
          id="job-name"
          className="input"
          value={form.name}
          onChange={(e) => set('name')(e.target.value)}
          placeholder="nightly-report"
          aria-invalid={Boolean(errors.name)}
          aria-describedby={errors.name ? 'name-error' : undefined}
          autoFocus
        />
        {field('name')}
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div>
          <label className="label" htmlFor="job-type">Job type</label>
          <select id="job-type" className="input" value={form.jobType} onChange={(e) => set('jobType')(e.target.value)}>
            {jobTypes.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
          {field('jobType')}
        </div>
        <div>
          <label className="label" htmlFor="job-priority">Priority</label>
          <select
            id="job-priority"
            className="input"
            value={form.priority}
            onChange={(e) => set('priority')(e.target.value)}
          >
            {JOB_PRIORITIES.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </div>
      </div>
      <div>
        <label className="label" htmlFor="job-payload">Payload (JSON)</label>
        <textarea
          id="job-payload"
          className="input min-h-28 font-mono text-xs"
          value={form.payload}
          onChange={(e) => set('payload')(e.target.value)}
          aria-invalid={Boolean(errors.payload)}
          spellCheck={false}
        />
        {field('payload')}
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="label" htmlFor="job-attempts">Max attempts</label>
          <input id="job-attempts" className="input" inputMode="numeric" value={form.maxAttempts} onChange={(e) => set('maxAttempts')(e.target.value)} />
          {field('maxAttempts')}
        </div>
        <div>
          <label className="label" htmlFor="job-timeout">Timeout (s)</label>
          <input id="job-timeout" className="input" inputMode="numeric" value={form.timeoutSeconds} onChange={(e) => set('timeoutSeconds')(e.target.value)} />
          {field('timeoutSeconds')}
        </div>
      </div>
      <div className="flex justify-end gap-2 pt-2">
        <button type="button" className="btn-secondary" onClick={onCancel}>Cancel</button>
        <button type="submit" className="btn-primary" disabled={create.isPending}>
          {create.isPending ? 'Submitting…' : 'Submit job'}
        </button>
      </div>
    </form>
  )
}
