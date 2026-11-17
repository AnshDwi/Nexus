import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useConnection } from './connection'
import type { CreateJobInput, JobFilters } from './types'

const LIVE = 3000 // dashboards refresh every 3s so status changes show up

export function useSummary() {
  const { client, connection } = useConnection()
  return useQuery({
    queryKey: ['summary', connection.baseUrl],
    queryFn: () => client.summary(),
    refetchInterval: LIVE,
  })
}

export function useJobs(filters: JobFilters) {
  const { client, connection } = useConnection()
  return useQuery({
    queryKey: ['jobs', connection.baseUrl, filters],
    queryFn: () => client.listJobs(filters),
    refetchInterval: LIVE,
    placeholderData: keepPreviousData,
  })
}

export function useJob(jobId: string | null) {
  const { client, connection } = useConnection()
  return useQuery({
    queryKey: ['job', connection.baseUrl, jobId],
    queryFn: () => client.getJob(jobId as string),
    enabled: Boolean(jobId),
    refetchInterval: LIVE,
  })
}

export function useWorkers() {
  const { client, connection } = useConnection()
  return useQuery({ queryKey: ['workers', connection.baseUrl], queryFn: () => client.workers(), refetchInterval: LIVE })
}

export function useQueueSnapshot() {
  const { client, connection } = useConnection()
  return useQuery({ queryKey: ['queue', connection.baseUrl], queryFn: () => client.queue(), refetchInterval: LIVE })
}

export function useExecutions(params: { page: number; pageSize: number; status?: string }) {
  const { client, connection } = useConnection()
  return useQuery({
    queryKey: ['executions', connection.baseUrl, params],
    queryFn: () => client.executions(params),
    refetchInterval: LIVE,
    placeholderData: keepPreviousData,
  })
}

export function useSystem() {
  const { client, connection } = useConnection()
  return useQuery({
    queryKey: ['system', connection.baseUrl, connection.apiKey],
    queryFn: () => client.system(),
    refetchInterval: 10000,
    retry: false,
  })
}

export function useReady() {
  const { client, connection } = useConnection()
  return useQuery({ queryKey: ['ready', connection.baseUrl], queryFn: () => client.ready(), refetchInterval: 10000 })
}

function useInvalidateJobs() {
  const qc = useQueryClient()
  return () =>
    Promise.all(
      ['jobs', 'job', 'summary', 'queue', 'executions'].map((k) => qc.invalidateQueries({ queryKey: [k] })),
    )
}

export function useCreateJob() {
  const { client } = useConnection()
  const invalidate = useInvalidateJobs()
  return useMutation({
    mutationFn: ({ input, idempotencyKey }: { input: CreateJobInput; idempotencyKey: string }) =>
      client.createJob(input, idempotencyKey),
    onSuccess: invalidate,
  })
}

export function useCancelJob() {
  const { client } = useConnection()
  const invalidate = useInvalidateJobs()
  return useMutation({
    mutationFn: ({ jobId, reason }: { jobId: string; reason?: string }) => client.cancelJob(jobId, reason),
    onSuccess: invalidate,
  })
}

export function useRetryJob() {
  const { client } = useConnection()
  const invalidate = useInvalidateJobs()
  return useMutation({ mutationFn: (jobId: string) => client.retryJob(jobId), onSuccess: invalidate })
}
