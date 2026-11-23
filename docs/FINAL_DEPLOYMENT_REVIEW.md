# NEXUS: final production deployment review

Review date: 20 Nov 2026 · Branch `production-readiness` (PR #3) · Reviewer: Claude (independent pass over the code, configuration and running stack, not just the documentation).

## 1. Executive summary

**Verdict: READY WITH CONDITIONS** (section 12).

The single-server production stack works as designed. Every job finished, with exactly one completed execution recorded, through:
- crashes and restarts of the API, scheduler and workers;
- a broker outage, and database and Redis restarts;
- a simulated server reboot;
- a failed deployment;
- a failed restore.

Only ports 80/443 face the internet.

The review still found real problems, now fixed and re-tested:
- **Critical:** the dashboard returned `403 Blocked request` on any real domain. Earlier tests used `localhost`, which hid it.
- **High:**
  - backups were world-readable;
  - two dependencies had known vulnerabilities;
  - a failed deploy could leave the system pointing at an unproven version;
  - `deploy` reported success while Caddy was crash-looping;
  - Redis ran as root.

What is **not** yet proven is anything that needs your real server and domain: a Let's Encrypt certificate and its renewal, the provider's network, and off-site backups. Those are the conditions.

## 2. Architecture review

One VM runs `compose.production.yml`:
- Caddy is the only public entry (80/443, automatic HTTPS);
- behind it: the dashboard, API, scheduler, workers (scaled with `WORKER_REPLICAS`), PostgreSQL, RabbitMQ, Redis, Prometheus and Grafana;
- operations go through `deploy/production/nexus.sh`.

Assessment:
- **Sound for a portfolio or low-volume production service.** Durable state is in PostgreSQL (the outbox and inbox are the source of truth). The broker carries delivery only: queued jobs whose message is lost are re-dispatched by the scheduler after `job_redispatch_after_seconds` (900 s). That recovery is in code (`stale_dispatch_for_update`) but wasn't drilled.
- **Single point of failure:** one server. Accepted for this use (Medium, section 7).
- **The dashboard is served by `vite preview`** (Node) rather than a static file server. It works and is now host-checked, but it's an extra process; serving `dist/` from Caddy would be leaner (Low).

**Git and PR structure (verified):**
- **Branch and state:** current branch `production-readiness`; no uncommitted work.
- **PR #2 (Pages demo)** was **merged into `nexus-product`** on 19 Nov (`dde12ce`). It isn't in `main`, so the Pages demo is **not deployed yet**: the Pages workflow deploys only from `main`.
- **PR #3's base** was `pages-demo`, which is identical to `nexus-product` after #2's merge. It is now **retargeted to `nexus-product`** (same single commit, same diff). PR #3 depends on #1 being merged before it reaches `main`.
- **PR #3's contents:**
  - 1 commit, 13 files before this review, all production-related;
  - **no** frontend or Pages-demo changes, except two review fixes: `vite.config.ts` (preview `allowedHosts`) and a lint fix in `pages.yml`;
  - no duplicate commits;
  - merges into `nexus-product` and into `main` without conflicts (checked with `git merge-tree`).

## 3. Security findings

Verified against the running stack, with a realistic domain over HTTPS:

| Area | Finding |
|---|---|
| Exposed ports | Only Caddy on 0.0.0.0 (80, 443/tcp+udp). Prometheus `127.0.0.1:9090` and the RabbitMQ console `127.0.0.1:15672` are host-local. A scan of the host's external IP found 80/443 open and 5432, 5672, 6379, 8000, 8081, 9090, 15672, 3000 and 5173 closed |
| PostgreSQL / Redis / RabbitMQ | Internal network only; strong random passwords (`openssl rand -hex 32`); Redis requires a password, and the RabbitMQ guest user isn't created |
| Grafana | Public at `/grafana/` behind a login; anonymous access and sign-up off; random admin password |
| Prometheus | Not public (127.0.0.1); scrapes the API with a bearer token |
| Caddy routing | `/api/v1/internal/*` → 404 (verified for `metrics` and `maintenance`); `/api/*` → API; `/grafana/*` → Grafana; everything else → dashboard. HTTP → HTTPS 308 |
| HTTPS | TLS 1.3 verified with Caddy's internal CA; HSTS, nosniff, `X-Frame-Options: DENY`, Referrer-Policy and Permissions-Policy present. **Let's Encrypt issuance and renewal are untested** (needs a real domain) |
| API authentication | No key or a wrong key → 401; a valid key → 200. Keys are 192-bit random, stored only as SHA-256 hashes. Production **refuses** the public `dev-key`, `local-*` secrets, default passwords, an empty `API_KEYS`, `DEBUG`, and `*` or http CORS (unit-tested, and verified at runtime) |
| Authorization / tenants | Every query is tenant-scoped; another tenant's job → 404 (integration-tested). **No roles**: any key has full rights in its tenant (Low) |
| Rate limiting | Per tenant (reads 600/min, writes 120/min), shared through Redis, with a per-process fallback on Redis errors. Unauthenticated 401s aren't rate-limited, which is acceptable given the key strength (Low) |
| Secrets | Generated on the server by `nexus.sh init`; `.env.production` is mode 600 and git-ignored. Secrets are passed as container environment variables, so anyone with Docker access can read them; Docker access is root-equivalent anyway (Low) |
| Containers | App processes run as uid 10001, the dashboard as 1000, Prometheus as 65534, and **Redis now as `redis` (it was root)**. No privileged containers, no added capabilities, **no Docker socket mounts**; `no-new-privileges` on every service. Caddy runs as root (official image default) |
| File permissions | `.env.production` 600. **Backups were 666 (world-readable and writable); now the `backups/` directory is 700 and files are 600**, owned by the operator |
| Sensitive logging | A grep of all container logs found the API key, all three data-store passwords and the metrics token in **0** lines; no `Authorization` headers are logged |
| Dependencies | `pip-audit` initially found **3 known vulnerabilities**: `certifi` 2026.6.17 and `multidict` 6.7.1. **Fixed**: upgraded to certifi 2026.10.23 and multidict 7.2.1, now 0 known; integration tests pass on the upgrade. `npm audit`: 0 |
| Public API docs | `/api/docs` and the OpenAPI schema are public (Low; they reveal no secrets) |

## 4. Reliability findings

**Tested:** the scenarios below were deliberately triggered and checked. **Assumed:** these follow from the design or code but weren't triggered.

| Scenario | Status | Evidence |
|---|---|---|
| API crash (process killed) | ✅ Tested | Host `kill -9` of the API, scheduler and a worker: all auto-restarted and healthy in **6 s**; new jobs completed |
| Worker crash | ✅ Tested | Drill: SIGKILL both workers → heartbeat timeout → jobs retried → one completed execution each |
| Scheduler crash | ✅ Tested | Killed with the API above; it restarted and reacquired leadership; jobs kept flowing |
| API restart | ✅ Tested | Drill: 10 jobs, one completed execution each |
| PostgreSQL restart | ✅ Tested | Drill |
| RabbitMQ restart / outage | ✅ Tested | Drill: jobs submitted **while RabbitMQ was stopped** stayed PENDING in the outbox, then ran once it returned |
| Redis restart | ✅ Tested | Drill |
| Graceful worker restart | ✅ Tested | Drill: unfinished work handed back, then retried |
| Full stop/start | ✅ Tested | Drill: every job kept and completed |
| Docker daemon restart (≈ reboot) | ✅ Tested | Daemon stopped and started: all 10 services back **healthy in 12 s** with no manual action; 3 in-flight jobs marked `lost`, retried, and completed (one completed execution each) |
| Real VPS reboot | ⚠️ Assumed | Covered by the daemon test plus `restart: unless-stopped`, if Docker is enabled at boot (the `get.docker.com` install does that). Verify once on the server |
| `docker kill` / `docker stop` | ℹ️ By design | Docker treats these as manual stops and won't restart the container until `nexus.sh deploy` or `compose up`. Use `docker restart`, not `kill`, by hand |
| Failed migration / failed deploy | ✅ Tested | A deliberately failing migration: `deploy` exited 1, restored the previous version (healthy), left the recorded version unchanged; the schema stayed at the old head |
| Deploy with a broken proxy | ✅ Fixed and tested | Caddy now has a health check, so `deploy` fails instead of reporting success |
| Invalid configuration | ✅ Tested | A dev key in production → `Refusing to start …`; insecure defaults covered by unit tests |
| Missing secrets | ✅ Tested | `compose` refuses with `required variable … is missing` (also a CI step) |
| Corrupt, truncated or empty backup | ✅ Tested | Refused up front with "nothing was changed"; services untouched |
| Restore failing partway | ✅ Tested | Valid archive with a bad owner: `pg_restore` failed, the **single transaction rolled back** (data intact), and the services were restarted automatically |
| Disk exhaustion | ⚠️ Partly mitigated | Mitigations: log rotation (50 MB per container); Prometheus retention 15 d; old images pruned (last 3 versions kept, tested); `deploy` refuses under 3 GB free; RabbitMQ's built-in disk alarm. **Not tested; no disk alerting** (no node exporter). If PostgreSQL's disk fills, writes fail until space is freed |
| Expired HTTPS certificate | ⚠️ Assumed | Caddy renews automatically about 30 days before expiry and persists certificates in `caddy_data`. Untested here (no public domain) |
| Out of memory | ✅ Mitigated | Per-service `mem_limit`; no OOM kills during any drill; peak observed well under the limits |
| Broker volume lost | ⚠️ Assumed | Queued jobs are re-dispatched after 15 min (code); not drilled |

## 5. Backup and recovery findings

| Check | Result |
|---|---|
| Backups never silently empty | ✅ An empty or failed `pg_dump` is deleted and reported (`die`) |
| Password-protected database | ✅ `PGPASSWORD` over TCP (it failed before the earlier fix, then was drilled) |
| Safe file names | ✅ Generated by the script; the label is validated (`[A-Za-z0-9._-]`) |
| Can't overwrite unrelated files | ✅ Writes to a `.partial` file then moves it into `backups/`; Postgres no longer mounts any host directory |
| Restore refuses invalid files | ✅ Missing, empty, truncated and corrupt files refused (`pg_restore --list` check) |
| Restore failure doesn't leave the system stopped | ✅ An `EXIT` trap restarts services; drilled with a failing restore |
| Consistency | ✅ `pg_dump` takes one transactional snapshot; restore is `--single-transaction`. RabbitMQ and Redis aren't backed up; they hold no data that PostgreSQL can't rebuild (outbox and redispatch) |
| Permissions | ✅ Directory 700, files 600 (they were 666) |
| **Survives server loss** | ❌ **No.** Backups are stored **only on the same VPS**. There is **no disaster recovery** until backups are copied off the server and a restore from that copy has been tested. Documented in PRODUCTION_DEPLOYMENT.md §9, and a condition below |

## 6. Resource assessment

Measured on the running stack (Grafana excluded; it typically uses 100–200 MiB):

| Service | Idle memory | Limit |
|---|---|---|
| RabbitMQ | 112–155 MiB | 768 MiB |
| API | ~71 MiB | 512 MiB |
| Scheduler / each worker | ~59 MiB | 384 / 768 MiB |
| Dashboard | ~57–61 MiB | 384 MiB |
| PostgreSQL | 29–41 MiB | 1 GiB |
| Prometheus | ~22 MiB | 768 MiB |
| Caddy / Redis | ~13 / ~4 MiB | 256 / 256 MiB |
| **Total** | **≈ 0.5 GiB** (≈ 0.7 GiB with Grafana) | ceilings only |

CPU use is a few percent of one core when idle. Images take about 4.3 GB on disk, and pruning keeps three versions.

**Is 2 vCPU / 4 GB sufficient?** **Yes**, with ample headroom at portfolio or low-volume load:
- builds (Python dependencies, npm and the Vite build) run on the server during `deploy` and briefly add about 1 GB;
- the sustained job throughput is bounded by `WORKER_REPLICAS × WORKER_CAPACITY` (8 concurrent jobs by default), not by the server.

**Minimums:**
- **2 GB RAM** works with 2 GB of swap (documented), but builds get slow.
- **40 GB disk** is fine with pruning. Keep backups pruned (the cron line in the guide) and copy them off-server.

## 7. Findings by severity

| # | Severity | Finding | Status |
|---|---|---|---|
| C1 | **Critical** | Dashboard returned `403 Blocked request` for any non-localhost domain (Vite preview host check) | **Fixed**: `preview.allowedHosts` from `NEXUS_ALLOWED_HOSTS` = `NEXUS_DOMAIN`; verified 200 over HTTPS |
| H1 | High | Backup files world-readable and writable (666), containing every job payload | **Fixed**: streamed to the host, dir 700 / files 600; verified |
| H2 | High | 3 known vulnerabilities (`certifi`, `multidict`) | **Fixed**: upgraded; `pip-audit` clean; integration tests pass |
| H3 | High | Failed deploy recorded the new version before success, so later commands could switch to a broken image | **Fixed**: recorded only on success; automatic fallback to the previous version; drilled |
| H4 | High | Caddy had no health check, so `deploy` reported success while the public entry was down | **Fixed**: health check on Caddy's local admin API |
| H5 | High | Redis ran as root (the entrypoint only drops privileges for a bare `redis-server` command) | **Fixed**: `user: redis`; verified |
| H6 | High | **No off-site backups**, so no disaster recovery | **Open, condition**: needs your storage; procedure documented |
| M1 | Medium | No memory limits or `no-new-privileges` | **Fixed**: per-service limits, `no-new-privileges` everywhere |
| M2 | Medium | Disk growth from images and backups | **Partly fixed**: image pruning (last 3), 3 GB free-space check before deploy; backup pruning is a documented cron line. No disk alerting |
| M3 | Medium | No alert notifications: 11 rules evaluate, but nothing pages you | Open: add Alertmanager or Grafana contact points, plus an external uptime check on `/api/v1/ready` |
| M4 | Medium | Single server is a single point of failure | Accepted for this use |
| M5 | Medium | Let's Encrypt issuance and renewal untested | Open, condition: verify on first deploy (`nexus.sh smoke`) |
| L1 | Low | Base images pinned by tag, not digest (`postgres:16-alpine`, `rabbitmq:3.13-…`, `caddy:2-alpine`, …); RabbitMQ 3.13 isn't the newest series | Plan an upgrade and digest pinning |
| L2 | Low | No per-key roles (full rights within a tenant) | Future |
| L3 | Low | `/api/docs` public; 401s not rate-limited; secrets visible to Docker-group users via `docker inspect` | Accepted |
| L4 | Low | Dashboard served by `vite preview` instead of a static server | Future: serve `dist/` from Caddy |
| L5 | Low | No CPU limits | Future |

## 8. Fixes implemented in this review

- **`frontend/vite.config.ts`:** preview `allowedHosts` from `NEXUS_ALLOWED_HOSTS` (C1).
- **`compose.production.yml`:**
  - `NEXUS_ALLOWED_HOSTS` set from the domain (C1);
  - Caddy health check (H4);
  - `user: redis` (H5);
  - `mem_limit` and `no-new-privileges` on every service (M1);
  - Postgres no longer mounts `./backups` (H1).
- **`deploy/production/nexus.sh`:**
  - backups streamed with `umask 077`, a 700 directory, `.partial` then move, and a validated label (H1);
  - restore over stdin, with a pre-check and a trap (H1);
  - version recorded only on success, plus automatic fallback (H3);
  - 3 GB disk check and image pruning (M2).
- **`uv.lock`:** `certifi` 2026.10.23, `multidict` 7.2.1 (H2).
- **`.github/workflows/ci.yml`:** new **`production-stack`** job that deploys `compose.production.yml` on the **official images** (including Grafana) with `nexus.sh`. It then runs the smoke check and all 10 failure drills over HTTPS, and a backup/restore.
- **`.github/workflows/pages.yml`:** shellcheck lint fix (unused loop variable).
- **Docs:** PRODUCTION_DEPLOYMENT.md updated (backup handling, deploy fallback, pruning, limits, the off-site backup warning); this review.
- **PR #3 retargeted** from `pages-demo` to `nexus-product` (metadata only).

## 9. Tests executed

**Locally (this sandbox), with substitute images** (Docker Hub is blocked here, so PostgreSQL, RabbitMQ, Redis, Python, Node, Caddy and Prometheus were stand-ins built from Ubuntu packages or official release binaries; Grafana wasn't run). All of these were run after the fixes:
- Fresh-clone production deploy with domain `nexus.example.com` and Caddy `tls internal`:
  - every service healthy;
  - HTTPS endpoint probe: dashboard and deep link 200; internal endpoints 404; no or bad key 401, valid 200; HSTS and the other headers present; TLS 1.3.
- **Resilience drills: 10/10 passed over HTTPS** (282 s).
- Crash via host `kill -9` (API, scheduler, worker): recovered in 6 s.
- Docker daemon restart: recovered in 12 s; in-flight jobs completed (one completed execution each).
- Failed migration deploy: fallback worked.
- Backup and restore: correct permissions; corrupt, empty, truncated and missing files refused; a restore failing partway rolled back and services were restarted.
- Image pruning across 4 deploys: last 3 kept.
- Runtime refusal of the dev key; missing-secret refusal.
- Port scan; container privilege inspection; secrets-in-logs grep.

**Locally, no containers:**
- Python: ruff and mypy clean; **unit 325 passed**; **integration 72 passed** (real PostgreSQL, RabbitMQ and Redis on the host); `pip-audit` clean after the fix.
- Frontend: typecheck and lint clean; **50 tests passed** (6 demo-only tests skipped by design); **22 demo-mode tests passed**; normal and Pages builds OK; `npm audit` 0.
- Tooling: `shellcheck`, `actionlint`, `caddy validate`, and both compose configs rendered.

**GitHub Actions, official images** (PR #3, commit `2c20d7e`, all 10 checks green). The **`production-stack`** job (5 min 21 s):
- pulled the official `postgres:16-alpine`, `rabbitmq:3.13-management-alpine`, `redis:7-alpine`, `caddy:2-alpine`, `prom/prometheus:v3.5.0` and `grafana/grafana:12.2.1`;
- deployed with `nexus.sh deploy`; **all 11 containers healthy, Grafana included**;
- `nexus.sh smoke` over HTTPS: dashboard ok, internal endpoints hidden, **Grafana ok**;
- **failure drills: 10 passed** (226 s);
- backup: mode `-rw-------`, 40 KB; restore OK.

The other checks:
- `production-config`: compose renders and refuses without secrets; shellcheck; `caddy validate`; promtool;
- `compose-e2e`: development stack, e2e tests, demo seed, Grafana provisioning, graceful shutdown;
- `test`, `integration`, `frontend`, `vercel-build`;
- the Pages workflow `build`.

The deploy jobs were correctly skipped, since this isn't `main`.

So the production stack has now run on **both** substitute images (locally) and the **official images** (GitHub Actions).

**Not yet run anywhere:**
- a real VPS;
- real DNS and Let's Encrypt issuance or renewal;
- an actual reboot of a VM;
- a disk-full scenario;
- broker volume loss;
- a restore from an **off-site** backup copy;
- load or performance testing beyond the drills.

## 10. Remaining risks

1. **No disaster recovery** until backups go off the server and a restore from that copy has been tested (H6).
2. **First-deploy unknowns:** certificate issuance (DNS and firewall), and the provider's network and IPv6.
3. **Nobody is told about failures:** alerts aren't routed anywhere (M3), and there's no disk monitoring (M2).
4. **One server** (M4): a host failure means downtime until you restore elsewhere.
5. **Base-image drift** (L1): tags can change between deploys.

## 11. Deployment checklist

Before buying:
- [ ] Decide on a provider: 2 vCPU / 4 GB / 40 GB, Ubuntu 24.04 (≈ €6/month class)
- [ ] A domain you control (≈ €10/year), with an A record you can set
- [ ] Off-site backup storage, for example any S3-compatible bucket or another machine you can `scp` to

On the server (PRODUCTION_DEPLOYMENT.md §4):
- [ ] Firewall: only 22, 80/tcp, 443/tcp and 443/udp open
- [ ] `nexus.sh init <domain> <email>`; store the printed API key
- [ ] `nexus.sh deploy` exits 0; `nexus.sh status` shows everything healthy
- [ ] `nexus.sh smoke` passes over **real HTTPS**, which proves the certificate (closes M5)
- [ ] Run the failure drills once, with test handlers on, then turn them off and redeploy
- [ ] Reboot the server once (`sudo reboot`); confirm `status` is all healthy afterwards
- [ ] Set up the daily backup cron **and** the off-site copy; restore that copy once on a scratch machine or a fresh install (closes H6)
- [ ] Point an external uptime check at `https://<domain>/api/v1/ready`
- [ ] Optional: Grafana alert contact point (M3)

Must stay private: PostgreSQL 5432, RabbitMQ 5672/15672, Redis 6379, API 8000, daemon health 8081, Prometheus 9090, Grafana 3000 (it is published only through Caddy's `/grafana/`), dashboard 5173. Must be open: 80 and 443 (plus SSH). No external managed services are required.

## 12. Final verdict

### READY WITH CONDITIONS

The stack is ready to deploy publicly for a portfolio or low-volume service, on these conditions:
1. **Before relying on the data:** set up off-site backups and test a restore from them. Until then, the deployment has **no disaster recovery**.
2. **On the first deploy:** confirm real HTTPS with `nexus.sh smoke`, and do one real reboot check.

Passing tests don't make the system "production-safe" by themselves. What the evidence supports:
- Every critical and high issue found in this review is fixed and re-tested, except off-site backups, which need your storage.
- The failure modes that matter most for a job platform (crashes, restarts, broker outage, failed deploys and restores) were triggered and handled without losing or duplicating a job.
