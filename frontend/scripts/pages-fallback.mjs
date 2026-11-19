// GitHub Pages serves 404.html for unknown paths. Making it a copy of the app
// shell lets deep links such as /Nexus/jobs/<id> load the SPA, whose router
// then renders the right page (a refresh never shows a blank screen).
import { copyFileSync, existsSync } from 'node:fs'

const index = new URL('../dist/index.html', import.meta.url)
if (!existsSync(index)) {
  console.error('dist/index.html not found: run the build first')
  process.exit(1)
}
copyFileSync(index, new URL('../dist/404.html', import.meta.url))
console.log('dist/404.html written (SPA fallback for GitHub Pages)')
