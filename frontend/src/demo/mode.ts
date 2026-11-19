/**
 * Demo mode is a build-time switch (`VITE_DEMO_MODE=true` in .env.pages, used
 * by `npm run build:pages`). Vite replaces `__NEXUS_DEMO__` with a literal, so
 * a normal build drops every demo branch and the simulator itself.
 */
export const DEMO_MODE: boolean = __NEXUS_DEMO__

/** Placeholder credential for the in-browser demo backend. Not a real key. */
export const DEMO_API_KEY = 'demo'

export const REPOSITORY_URL = 'https://github.com/AnshDwi/Nexus'
