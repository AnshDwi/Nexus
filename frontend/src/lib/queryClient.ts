import { QueryClient } from '@tanstack/react-query'
import { ApiError } from './api'

export function makeQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Do not hammer the API with retries on auth/validation errors.
        retry: (count, error) => !(error instanceof ApiError && error.status >= 400 && error.status < 500) && count < 2,
        staleTime: 1000,
      },
    },
  })
}
