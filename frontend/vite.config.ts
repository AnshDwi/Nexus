/// <reference types="vitest/config" />
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// The dev server (`npm run dev`) and the preview server used by the Docker
// image (`vite preview`) both proxy /api to the NEXUS API, so the dashboard
// is same-origin and needs no CORS configuration locally.
const apiProxy = {
  '/api': { target: process.env.NEXUS_API_PROXY ?? 'http://localhost:8000', changeOrigin: true },
}

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: { port: 5173, proxy: apiProxy },
  preview: { port: 5173, proxy: apiProxy },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    css: false,
    restoreMocks: true,
  },
})
