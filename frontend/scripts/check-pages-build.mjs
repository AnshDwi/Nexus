// Checks the GitHub Pages build in dist/ before it is published:
//   * every asset referenced by index.html is under the /Nexus/ base and exists
//   * 404.html (the SPA fallback) is identical to index.html
//   * the demo is really built in (and therefore needs no backend)
//   * no localhost URLs, backend credentials or secret-looking strings ship
// Exits non-zero with a list of problems.
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import { join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const BASE = '/Nexus/'
const dist = fileURLToPath(new URL('../dist/', import.meta.url))
const problems = []
const fail = (msg) => problems.push(msg)

if (!existsSync(join(dist, 'index.html'))) {
  console.error('dist/index.html missing: run `npm run build:pages` first')
  process.exit(1)
}

const files = []
;(function walk(dir) {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name)
    if (statSync(p).isDirectory()) walk(p)
    else files.push(p)
  }
})(dist)

const index = readFileSync(join(dist, 'index.html'), 'utf8')
if (!existsSync(join(dist, '404.html')) || readFileSync(join(dist, '404.html'), 'utf8') !== index) {
  fail('404.html must be an exact copy of index.html (SPA fallback)')
}

const refs = [...index.matchAll(/(?:src|href)="([^"]+)"/g)].map((m) => m[1])
if (refs.length === 0) fail('index.html references no assets')
for (const ref of refs) {
  if (/^https?:\/\//.test(ref)) continue
  if (!ref.startsWith(BASE)) {
    fail(`index.html references ${ref} outside ${BASE}`)
    continue
  }
  if (!existsSync(join(dist, ref.slice(BASE.length)))) fail(`index.html references missing file ${ref}`)
}
if (!refs.some((r) => r.endsWith('favicon.svg'))) fail('favicon is not linked')

const text = files.filter((f) => /\.(js|css|html|json|svg)$/.test(f))
const bundle = text.map((f) => [relative(dist, f), readFileSync(f, 'utf8')])
const js = bundle.filter(([f]) => f.endsWith('.js')).map(([, c]) => c).join('\n')
if (!js.includes('in-browser simulator')) fail('demo simulator is missing: was the build made with --mode pages?')

const forbidden = [
  // React Router parses paths against a placeholder "http://localhost" origin
  // and never requests it; a local backend address always has a port or an IP.
  [/https?:\/\/localhost:\d+|\b(127\.0\.0\.1|0\.0\.0\.0)\b/, 'local backend address'],
  [/7e9f8fd111802be56c379d597842e29b2cebd35ff2133d431a49fa556a18704e/, 'local API key hash'],
  [/local-(cron-secret|metrics-token)/, 'local service secret'],
  [/(postgres(ql)?|amqps?|redis):\/\/[^\s'"`]+/, 'backend connection URL'],
  [/\b(API_KEYS|CRON_SECRET|METRICS_TOKEN|DATABASE_URL|RABBITMQ_URL|VERCEL_TOKEN|POSTGRES_PASSWORD)\b/, 'backend secret variable name'],
  [/\b(ghp|gho|github_pat)_[A-Za-z0-9_]{20,}/, 'GitHub token'],
  [/\bsk-[A-Za-z0-9]{20,}/, 'API secret key'],
  [/AKIA[0-9A-Z]{16}/, 'AWS access key'],
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----/, 'private key'],
]
for (const [file, content] of bundle) {
  for (const [re, what] of forbidden) {
    const m = content.match(re)
    if (m) fail(`${file}: ${what}: ${m[0].slice(0, 60)}`)
  }
}

if (problems.length) {
  console.error(`Pages build check failed (${problems.length}):\n  - ${problems.join('\n  - ')}`)
  process.exit(1)
}
console.log(`Pages build OK: ${files.length} files, ${refs.length} index.html references under ${BASE}, SPA fallback present, no secrets or localhost URLs.`)
