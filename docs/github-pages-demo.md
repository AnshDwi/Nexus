# GitHub Pages live demo

NEXUS publishes a self-contained demo of its dashboard to GitHub Pages at:

**https://anshdwi.github.io/Nexus/**

Only the static dashboard is published. GitHub Pages serves static files and cannot run the API, the scheduler, workers, PostgreSQL, RabbitMQ or Redis. The demo build therefore points the dashboard at an **in-browser simulator** instead of a backend. Nothing about the real system changes; the demo is a separate build of the same frontend.

---

## 1. One-time GitHub setting

1. Open the repository on GitHub and go to **Settings → Pages**.
2. Under **Build and deployment → Source**, choose **GitHub Actions**. Don't choose "Deploy from a branch".
3. Leave everything else as it is. No branch, folder or custom domain is needed.

The first deployment creates a `github-pages` environment (**Settings → Environments**). The workflow only deploys from `main`. If you add deployment-branch rules to that environment, make sure `main` is allowed.

No secrets are needed: the workflow uses the short-lived token GitHub issues to each run.

## 2. When it deploys

The workflow is `.github/workflows/pages.yml`.

| Event | What happens |
|---|---|
| Pull request that touches `frontend/**` or the workflow | Build and verify only, no deployment |
| Push to `main` touching those paths (that is, after a PR is reviewed and merged) | Build, verify, **deploy**, smoke-test the public URL |
| Manual run (**Actions → Pages demo → Run workflow**) on `main` | Same as a push to `main` |
| Pushes to any other branch | Nothing |

**Build job** (permissions: `contents: read`):
1. `npm ci`, then typecheck, lint, the normal test suite and the demo test suite (`npm run test:demo`).
2. `npm run build:pages`, which:
   - builds with `--mode pages`;
   - writes `404.html`;
   - runs `scripts/check-pages-build.mjs`.
3. Uploads `frontend/dist` with `actions/upload-pages-artifact`.

**Deploy job** (permissions: `pages: write`, `id-token: write`; only on `main`):
1. `actions/configure-pages` reads the site configuration. The job stops if the site path isn't `/Nexus`.
2. `actions/deploy-pages` publishes the artifact.
3. A smoke test fetches the public URL, its main script, and a deep link (`/Nexus/jobs`), and fails the run if any of them doesn't serve the dashboard.

The workflow runs independently of `ci.yml`, which is unchanged.

## 3. Base URL configuration

A project site is served from a sub-path, `/<repository>/`. Everything that depends on it comes from one file, `frontend/.env.pages`:

```
VITE_DEMO_MODE=true
VITE_BASE_PATH=/Nexus/
```

It is used in three places:
- **`vite.config.ts`** sets `base` from `VITE_BASE_PATH`. Asset URLs and the favicon in `index.html` become `/Nexus/...`.
- **`App.tsx`** gives React Router `basename={import.meta.env.BASE_URL}`, so `/Nexus/jobs` maps to the `/jobs` route.
- **The SPA fallback.** GitHub Pages serves `404.html` for paths that don't exist as files. `scripts/pages-fallback.mjs` makes `404.html` a copy of `index.html`. So a refresh or a shared link such as `/Nexus/jobs/<id>` loads the app, and the router shows the right page instead of a blank screen. The response status is 404, but browsers render it normally. This is the standard approach for single-page apps on GitHub Pages.

**If the repository is renamed:**
1. Change `VITE_BASE_PATH` in `frontend/.env.pages`.
2. Change the expected `/Nexus` in the workflow's "Check the site path" step.
3. Change `BASE` in `frontend/scripts/check-pages-build.mjs`.

All other builds are unaffected and keep base `/`: `npm run dev`, `npm run build`, the Docker image and the Vercel project.

## 4. Demo-mode architecture

```
pages / hooks (unchanged) ──► NexusClient (unchanged) ──► fetch
                                                           │
                       normal build: window.fetch ─────────┴──► NEXUS API
                       demo build:   demoFetch  ──► DemoStore (in memory, in the browser)
```

- **Swapping the transport.** `NexusClient` already accepts a `fetch` implementation. In the demo build, `lib/connection.tsx` gives it `demoFetch` (`src/demo/server.ts`), which answers the same `/api/v1/...` routes with the same JSON shapes, status codes and error bodies. Every page, query hook and error path runs exactly as against the real API. No page component contains demo logic.
- **The simulator.** `src/demo/store.ts` seeds a deterministic 24-hour history (about 170 jobs, four workers, one of which stopped heartbeating). It then advances jobs with the same state machine as `src/nexus/domain/job.py`:
  - PENDING → QUEUED → RUNNING → COMPLETED;
  - RUNNING → FAILED → RETRYING → QUEUED with exponential backoff;
  - dead-lettering when attempts run out or the error is permanent;
  - a manual retry gives a fresh attempt budget;
  - worker capacity is respected.

  Each simulated transition is marked "(simulated)". Background traffic arrives every few seconds so the dashboard is never static.
- **Isolation.** All demo code lives in `src/demo/`. It is switched on by a build-time constant (`__NEXUS_DEMO__`, from `VITE_DEMO_MODE`), which Vite replaces with a literal. So a normal build contains none of it; `check-pages-build.mjs` and the bundle check in the tests confirm this.
- **Visibility.** A banner on every page says the data is simulated. The status pill reads "Simulated API", and Settings shows each dependency as "simulated". An **About NEXUS** page explains the real architecture and what the demo simulates.
- **Privacy.** No request leaves the browser. There are no API keys, credentials or backend URLs in the build; `check-pages-build.mjs` fails the build if any appear. Data lives in memory and resets on reload or with **Reset** in the banner.

## 5. What works in the demo

| Area | In the demo |
|---|---|
| Overview: totals, job states, 24 h execution trend, recent activity, worker health | ✅ from simulated data |
| Jobs: list, search, status/priority filters, pagination | ✅ |
| Create job (all registered types: `echo`, `http.webhook`, `test.sleep`, `test.flaky`, `test.fail`) | ✅ runs through the simulated lifecycle |
| Job detail: state history, execution attempts, payload, result, error | ✅ |
| Cancel; retry; automatic retry with backoff | ✅ |
| Dead-lettered view, filter by last error, retry selected, retry all matching | ✅ |
| Workers: capacity, load, heartbeat age, unhealthy-worker detection | ✅ |
| Queue by priority and dispatch status; execution history | ✅ |
| Settings: health checks and configuration | ✅ shown as "simulated" |

The layout is responsive: the sidebar becomes a menu and the banner stacks on phones ([screenshot](screenshots/demo-mobile.png)).

## 6. What needs the real backend

These are not part of the demo. Run `docker compose up --build` (see the README) to use them.

- Real job execution: handlers running on worker processes, real webhooks.
- Persistence across reloads, multiple users, API keys and tenant isolation.
- The RabbitMQ broker: publisher confirms, the real `nexus.dead-letter` queue, redelivery.
- Scheduler leader election and fencing, dead-worker recovery, graceful worker drain.
- Prometheus metrics, alert rules and the Grafana dashboard.
- The REST API itself (`/api/docs`), rate limiting, idempotency stored in the database.

## 7. Building and checking locally

```bash
cd frontend
npm ci
npm run build:pages            # dist/ for GitHub Pages, verified by check-pages-build.mjs
npm run test:demo              # simulator + whole-app tests under /Nexus/
npx vite --mode pages          # dev server in demo mode: http://localhost:5173/Nexus/
```

`vite preview` doesn't emulate GitHub Pages' `404.html` behaviour. To test a refresh on a deep link, serve `dist/` from any static server that serves `/Nexus/` and returns `404.html` for unknown paths.

## 8. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Deploy job fails with "Get Pages site failed" or "Not Found" | Pages isn't enabled with **Source: GitHub Actions** (step 1) |
| Deploy job is rejected by environment protection | Allow `main` under **Settings → Environments → github-pages → Deployment branches** |
| "Check the site path" step fails | The repository was renamed, or a custom domain was set; update the base path (section 3) |
| Blank page with 404s for `/assets/...` | The site was built without `--mode pages`; use `npm run build:pages` |
| Old version still shown | GitHub Pages caches for up to about 10 minutes; hard-refresh |
