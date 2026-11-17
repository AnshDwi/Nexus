import type { JobPriority } from './types'

export interface JobFormState {
  name: string
  jobType: string
  priority: JobPriority
  payload: string
  maxAttempts: string
  timeoutSeconds: string
}

export type FormErrors = Partial<Record<keyof JobFormState | 'form', string>>

/** Client-side validation; the API re-validates everything. */
export function validateJobForm(f: JobFormState): { errors: FormErrors; payload?: Record<string, unknown> } {
  const errors: FormErrors = {}
  if (!f.name.trim()) errors.name = 'Name is required'
  else if (f.name.length > 200) errors.name = 'Name must be 200 characters or fewer'
  if (!f.jobType) errors.jobType = 'Choose a job type'
  let payload: Record<string, unknown> | undefined
  try {
    const parsed: unknown = f.payload.trim() ? JSON.parse(f.payload) : {}
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      errors.payload = 'Payload must be a JSON object'
    } else {
      payload = parsed as Record<string, unknown>
    }
  } catch {
    errors.payload = 'Payload is not valid JSON'
  }
  const attempts = Number(f.maxAttempts)
  if (!Number.isInteger(attempts) || attempts < 1 || attempts > 20) errors.maxAttempts = 'Between 1 and 20'
  const timeout = Number(f.timeoutSeconds)
  if (!Number.isInteger(timeout) || timeout < 1) errors.timeoutSeconds = 'At least 1 second'
  return { errors, payload }
}

