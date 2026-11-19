/**
 * Demo mode entry point. Nothing runs at import time: the store is created
 * on the first request, so a non-demo build can drop this module entirely.
 */
import { createDemoFetch } from './server'
import { DemoStore } from './store'

let store: DemoStore | null = null
let handler: typeof fetch | null = null

function demoStore(): DemoStore {
  store ??= new DemoStore()
  return store
}

/** `fetch` replacement that serves the NEXUS API from the in-browser simulator. */
export const demoFetch: typeof fetch = (input, init) => {
  handler ??= createDemoFetch(demoStore())
  return handler(input, init)
}

/** Start the simulation again from the seeded data set. */
export function resetDemo(): void {
  demoStore().reset()
}
