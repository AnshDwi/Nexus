/// <reference types="vitest/config" />
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

// The dev server (`npm run dev`) and the preview server used by the Docker
// image (`vite preview`) both proxy /api to the NEXUS API, so the dashboard
// is same-origin and needs no CORS configuration locally.
const apiProxy = {
  '/api': { target: process.env.NEXUS_API_PROXY ?? 'http://localhost:8000', changeOrigin: true },
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), 'VITE_')
  return {
  // "/" everywhere except the GitHub Pages demo (`--mode pages`, .env.pages),
  // which is served from https://anshdwi.github.io/Nexus/.
  base: env.VITE_BASE_PATH || '/',
  // Replaced by a bare true/false literal everywhere, so non-demo builds
  // tree-shake the simulator (src/demo) out completely.
  define: { __NEXUS_DEMO__: JSON.stringify(env.VITE_DEMO_MODE === 'true') },
  plugins: [react(), tailwindcss()],
  server: { port: 5173, proxy: apiProxy },
  preview: { port: 5173, proxy: apiProxy },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    css: false,
    restoreMocks: true,
  },
  }
})
