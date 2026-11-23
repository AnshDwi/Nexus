/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string
  /** "true" builds the self-contained demo (GitHub Pages). */
  readonly VITE_DEMO_MODE?: string
  readonly BASE_URL: string
}
interface ImportMeta {
  readonly env: ImportMetaEnv
}

/** true only in the GitHub Pages demo build; replaced by a literal (vite.config.ts `define`). */
declare const __NEXUS_DEMO__: boolean
