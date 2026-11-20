# Production deployment

This guide runs the **real** NEXUS backend on a server: API, PostgreSQL, RabbitMQ, Redis, the scheduler, worker processes, Prometheus and Grafana. Jobs are submitted through the API, executed by real workers, stored in PostgreSQL, retried and recovered. (The GitHub Pages site is a separate, simulated demo; see [github-pages-demo.md](github-pages-demo.md).)

It is written for a first deployment. Every command is meant to be copied as-is, replacing only `nexus.example.com`, `you@example.com` and `SERVER_IP`.

**Contents**
1. [Recommended architecture](#1-recommended-architecture)
2. [Hosting options and cost](#2-hosting-options-and-cost)
3. [What runs where](#3-what-runs-where)
4. [Step by step: first deployment](#4-step-by-step-first-deployment)
5. [Configuration and secrets](#5-configuration-and-secrets)
6. [Database, broker and Redis](#6-database-broker-and-redis)
7. [Deployment order and migrations](#7-deployment-order-and-migrations)
8. [Verifying health](#8-verifying-health)
9. [Updates, backups and rollback](#9-updates-backups-and-rollback)
10. [Monitoring](#10-monitoring)
11. [Troubleshooting](#11-troubleshooting)
12. [Readiness checklist](#12-readiness-checklist)

---

## 1. Recommended architecture

**One Linux server (2 vCPU, 4 GB RAM) running `compose.production.yml` with Docker Compose.**

```
Internet ──443/80──► Caddy (automatic HTTPS)
                       ├── /            ► dashboard (React build)
                       ├── /api/*       ► api (FastAPI)          ── /api/v1/internal/* is blocked
                       └── /grafana/*   ► grafana (login)
private network:  api ─ scheduler ─ worker ×N ─ postgres ─ rabbitmq ─ redis ─ prometheus
127.0.0.1 only:   Prometheus :9090, RabbitMQ console :15672   (reach them with an SSH tunnel)
```

Why one server rather than a platform per service:
- **The scheduler and workers are long-running processes that must never sleep.** The scheduler polls continuously, and workers keep broker connections and send heartbeats every 5 seconds. Platforms that suspend idle services, or bill each always-on process separately, either stop processing jobs or cost several times more (section 2).
- **It's the configuration that was tested.** The failure drills (section 8) ran against exactly this file.
- **It's small.** Measured on the running stack: the whole system idles at about 500 MiB RAM without Grafana (Grafana adds roughly 100–200 MiB) and a few percent of one CPU. 4 GB leaves room for building images on the server and for job spikes.
- **It moves cleanly.** Nothing is provider-specific, so you can move to any other server later with a backup and restore.

## 2. Hosting options and cost

Prices were checked in November 2026 and change often, so confirm them before buying.

| Option | Runs workers 24/7? | Est. monthly cost | Notes |
|---|---|---|---|
| **Small VPS + this guide** (e.g. Hetzner CX23: 2 vCPU / 4 GB / 40 GB) | ✅ | **≈ €5.5–6** (CX23 €5.49 + IPv4) | Cheapest real option and the most realistic portfolio setup. Hetzner's cost-optimized plans have intermittently been out of stock in 2026; any provider with 2 vCPU / 4 GB and Ubuntu works the same way |
| **Railway** (Hobby plan) | ✅ (services don't sleep) | ≈ $8–15 (est.) | $5/month includes $5 of usage; RAM $10/GB-month, CPU $20/vCPU-month. Easiest UI, but you recreate ~10 services by hand, and Railway can't mount this repo's Prometheus/Grafana/Caddy config files without custom images |
| **Fly.io** | ✅ | ≈ $25+ | No free tier for new accounts; shared-cpu-1x 256 MB is $2.19/month per machine. You'd run ~9 machines plus volumes and an IPv4; managed Postgres alone is far more |
| **Render** | ✅ only on paid instances | ≈ $60+ | Background workers have no free instance ($7/month each). Free web services spin down after 15 minutes idle; free Postgres expires. API, scheduler, 2 workers, RabbitMQ, Prometheus and Grafana at $7 each, plus Postgres and Key Value |
| Oracle Cloud Always Free (Ampere A1) | ⚠️ | €0 | Now 2 OCPU / 12 GB. Capacity is often unavailable, and Oracle may reclaim instances it considers idle (95th-percentile CPU below 20% over 7 days). NEXUS idles far below that |
| Free managed mix (Neon + CloudAMQP free + Upstash + free compute) | ❌ | €0 | **Doesn't keep running.** The scheduler queries PostgreSQL every second, so Neon's compute never scales to zero. At 0.25 CU that's ~180 CU-hours/month, above the Free plan's 100, after which compute is suspended until next month. CloudAMQP's free broker (1M messages/month, 20 connections, 100 queues) would itself be fine |
| Vercel (existing [vercel-deployment.md](vercel-deployment.md)) | ⚠️ Daemons are beta, production deployments only | Pro plan + external broker | Kept as an alternative; needs a managed RabbitMQ such as CloudAMQP |

**Recommendation:**
- **Cheapest practical:** a VPS at about €6/month.
- **Easiest:** Railway at about $8–15/month, with more porting work.
- **Best for a portfolio:** the VPS. It's one public HTTPS URL with the real stack, Grafana included, for the price of a coffee.

## 3. What runs where

All on the one server, from `compose.production.yml`:

| Service | Kind | Restart | Persistent data | Health check | Public? |
|---|---|---|---|---|---|
| `caddy` | Reverse proxy, TLS | always | `caddy_data` (certificates) | admin API `:2019/config/` (container-local) | **80, 443** |
| `dashboard` | Static React build served by Node | always | none | `GET /` | via Caddy |
| `api` | FastAPI | always | none | `GET /api/v1/ready` (checks PostgreSQL, RabbitMQ, Redis) | via Caddy (`/api`) |
| `scheduler` | Long-running daemon (leader-elected) | always | none | `:8081/health` (stalls → restart) | no |
| `worker` ×`WORKER_REPLICAS` | Long-running daemons | always | none | `:8081/health` | no |
| `migrate` | One-shot `alembic upgrade head` | never | none | exit code | no |
| `postgres` | Database | always | `postgres_data` | `pg_isready` | no |
| `rabbitmq` | Broker | always | `rabbitmq_data` | `rabbitmq-diagnostics ping` | 127.0.0.1:15672 |
| `redis` | Rate-limit counters (password, AOF) | always | `redis_data` | `redis-cli ping` | no |
| `prometheus` | Metrics, 11 alert rules | always | `prometheus_data` (15 days) | `/-/ready` | 127.0.0.1:9090 |
| `grafana` | Dashboards (login required) | always | `grafana_data` | `/api/health` | via Caddy (`/grafana`) |

**Startup order** is enforced by health checks:
1. `postgres`, `rabbitmq` and `redis` become healthy.
2. `migrate` runs and exits 0.
3. `api`, `scheduler` and `worker` start.
4. `dashboard`, `prometheus` and `grafana` start once the API is healthy.
5. `caddy` starts last.

**Scaling workers:** set `WORKER_REPLICAS` (and `WORKER_CAPACITY`, the concurrent jobs per worker), then `nexus.sh deploy`. Prometheus discovers the replicas automatically.

**Logs** are JSON lines (`request_id`, `correlation_id`, `job_id`, `attempt`), rotated at 10 MB × 5 files per container. They contain no API keys, passwords or tokens; this was checked against the running stack.

**Containers:**
- **Memory limits** cap a runaway process, so it can't take the whole server down. Examples: API 512 MB, worker 768 MB, PostgreSQL 1 GB; see `mem_limit` in `compose.production.yml`.
- **`no-new-privileges`** is set on every service.
- **Non-root:** the app processes run as uid 10001 and the dashboard as uid 1000.
- **No Docker socket access:** nothing mounts `/var/run/docker.sock`.

## 4. Step by step: first deployment

### 4.1 Get a server and a domain
1. **Create a server:** Ubuntu 24.04, 2 vCPU, 4 GB RAM, 40 GB disk, with an SSH key, at any provider (section 2). Note its public IP, `SERVER_IP`.
2. **Point a name at it:** at your DNS provider, create an **A record** `nexus.example.com → SERVER_IP` (and AAAA for IPv6 if you have it). Caddy needs this record to get the HTTPS certificate.

### 4.2 Prepare the server (once)
```bash
ssh root@SERVER_IP
apt-get update && apt-get -y upgrade
curl -fsSL https://get.docker.com | sh              # Docker Engine + Compose v2
adduser --disabled-password --gecos "" nexus
usermod -aG docker nexus
mkdir -p /home/nexus/.ssh && cp ~/.ssh/authorized_keys /home/nexus/.ssh/ && chown -R nexus:nexus /home/nexus/.ssh
ufw allow OpenSSH && ufw allow 80/tcp && ufw allow 443/tcp && ufw allow 443/udp && ufw --force enable
exit
```
The firewall only needs SSH, 80 and 443. Every other port in the stack is either internal or bound to 127.0.0.1.

### 4.3 Install NEXUS
```bash
ssh nexus@SERVER_IP
git clone https://github.com/AnshDwi/Nexus.git && cd Nexus
git checkout production-readiness           # until it is merged into main
deploy/production/nexus.sh init nexus.example.com you@example.com
```
`init` writes `.env.production` (mode 600, ignored by git) with fresh random passwords and tokens, and **prints your first API key once**. Save it in a password manager.

### 4.4 Deploy
```bash
deploy/production/nexus.sh deploy
```
This command:
1. backs up the database (skipped on the first run);
2. builds images tagged with the current git commit;
3. runs migrations;
4. starts everything and waits until every health check passes;
5. checks the public URL.

The first build takes a few minutes. The first certificate can take up to a minute after DNS resolves.

### 4.5 Use it
- **Dashboard:** `https://nexus.example.com`. Paste your API key.
- **API:** `curl -H "Authorization: Bearer $KEY" https://nexus.example.com/api/v1/system`
- **Grafana:** `https://nexus.example.com/grafana/`. User `admin`; the password is `GRAFANA_ADMIN_PASSWORD` in `.env.production`.

Submit a job:
```bash
KEY=nx_...   # your key
curl -s -X POST https://nexus.example.com/api/v1/jobs \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"name":"hello","job_type":"echo","payload":{"msg":"hi"}}'
```

**Optional: populate a showcase** with real jobs, including retries and dead letters. This needs the test job types, so turn them on only for the duration:
```bash
sed -i 's/^TASK_ENABLE_TEST_HANDLERS=.*/TASK_ENABLE_TEST_HANDLERS=true/' .env.production
deploy/production/nexus.sh deploy
deploy/production/nexus.sh compose run --rm --no-deps -e NEXUS_API_KEY="$KEY" api \
  python -m nexus.demo seed --api-url http://api:8000 --wait --allow-non-local
sed -i 's/^TASK_ENABLE_TEST_HANDLERS=.*/TASK_ENABLE_TEST_HANDLERS=false/' .env.production
deploy/production/nexus.sh deploy
```

## 5. Configuration and secrets

All configuration lives in `.env.production`. The template is [`deploy/production/env.example`](../deploy/production/env.example), with every variable explained.

| Variable | Required | What it is |
|---|---|---|
| `NEXUS_DOMAIN`, `ACME_EMAIL` | ✅ | Public hostname and Let's Encrypt contact |
| `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD`, `REDIS_PASSWORD` | ✅ | Data-store passwords (internal network only) |
| `API_KEYS` | ✅ | `tenant:sha256(key),…`. Add keys with `nexus.sh new-key <tenant>` |
| `CRON_SECRET`, `METRICS_TOKEN` | ✅ | Protect `/api/v1/internal/*` (also blocked at Caddy) |
| `GRAFANA_ADMIN_PASSWORD` | ✅ | Grafana `admin` login |
| `WORKER_REPLICAS`, `WORKER_CAPACITY` | | Worker processes and concurrent jobs per process (2 × 4) |
| `WEBHOOK_ALLOWED_HOSTS` | | Hosts the `http.webhook` job type may call (https only) |
| `CORS_ALLOWED_ORIGINS` | | Only for a dashboard on another origin (https) |
| `TASK_ENABLE_TEST_HANDLERS` | | `false` in production; `true` only for drills or the showcase |

**Two layers of protection against unsafe configuration:**
- **Compose** refuses to start if any required variable is missing (`required variable … is missing a value`).
- **The API, scheduler and workers** refuse to start in `production` if any of these hold:
  - database or broker passwords are the built-in defaults;
  - `API_KEYS` is empty or contains the public development key `dev-key`;
  - `CRON_SECRET` or `METRICS_TOKEN` is a published local default;
  - `DEBUG` is on;
  - a CORS origin is `*` or isn't https.

**Secrets checklist**
- [ ] `.env.production` exists only on the server, mode 600 (`ls -l .env.production` → `-rw-------`)
- [ ] `git status` doesn't list `.env.production`, `backups/`, `.nexus-version` or `.deploy-history` (all git-ignored)
- [ ] Every secret was generated by `nexus.sh init` / `openssl rand -hex 32`; nothing reused from `.env.example` or the README
- [ ] API keys are stored in a password manager; only their hashes are in `API_KEYS`
- [ ] Backups (which contain job payloads) are copied off the server to private storage only
- [ ] To rotate a secret: edit `.env.production`, then `nexus.sh deploy`. **Exception:** the PostgreSQL, RabbitMQ and Redis passwords only apply to *new* data volumes. To change one on a running system:
  - PostgreSQL: run `ALTER USER` (`nexus.sh compose exec postgres psql -U nexus -c "ALTER USER nexus PASSWORD '…'"`) first;
  - RabbitMQ: `rabbitmqctl change_password nexus …`;
  - Redis: reads it at start, so a deploy is enough.

## 6. Database, broker and Redis

All three run in containers with named volumes, so data survives restarts, deploys and server reboots.

- **PostgreSQL 16.** Database `nexus`, owned by user `nexus`. Migrations create every table. `nexus.sh backup` streams a consistent `pg_dump` (custom format, compressed) from the container to `./backups/` on the host:
  - the directory is mode 700 and each file mode 600, because dumps contain every job payload;
  - an empty or failed dump is deleted and reported, never kept;
  - file names are generated by the script (timestamp plus a validated label), so a backup can't overwrite another file.
- **RabbitMQ 3.13.** Its node name is pinned (`hostname: nexus-rabbitmq`), so queues survive container re-creation. NEXUS declares its own topology at startup:
  - `nexus.events` exchange;
  - `nexus.jobs` priority queue;
  - `nexus.tasks`;
  - `nexus.dlx` → `nexus.dead-letter`.

  Messages are persistent and published with confirms. The management UI is at 127.0.0.1:15672 (SSH tunnel, section 10).
- **Redis 7.** Password-protected with append-only persistence. It holds only rate-limit counters, so losing it is harmless; the API falls back to per-process limits if it's unreachable.

**Using managed services instead** (optional): point the app at them instead of the containers.
- **Variables:** set `DATABASE_URL`, `RABBITMQ_URL` (`amqps://…`) and `REDIS_URL` (`rediss://…`) in the app services' environment.
- **Compose:** remove the corresponding containers in a `NEXUS_COMPOSE_OVERRIDE` file.
- **Caveat:** check the free-tier limits in section 2 first.

## 7. Deployment order and migrations

`nexus.sh deploy` always does, in order:
1. check that at least 3 GB of disk is free, then `pg_dump` to `backups/nexus-<time>-before-<version>.dump`. If either step fails, the deploy stops before changing anything;
2. build images `nexus-app:<commit>` and `nexus-dashboard:<commit>`;
3. run `migrate` (`alembic upgrade head`, one-shot; app services wait for it to succeed);
4. replace `api`, `scheduler` and `worker`. Workers receive SIGTERM and **drain**: they stop taking jobs, give running jobs up to 25 s to finish, hand the rest back to be retried, and deregister. Nothing is lost (verified, section 8);
5. wait for all health checks, then run the smoke test.

**If any of this fails** (a migration error, or a service that won't become healthy), `deploy`:
- puts `api`, `scheduler`, `worker` and `dashboard` back on the previous version;
- leaves the recorded version unchanged;
- exits with an error naming the logs to read.

This was drilled with a deliberately failing migration: the previous version stayed up and healthy, and the schema was unchanged.

**Image pruning:** after a successful deploy, images older than the last 3 deployed versions are removed, so repeated deploys don't fill the disk. Rollback is therefore possible to any of the last 3 versions.

**Migration rules for future changes:**
- **Additive only.** Migrations must be additive and backwards compatible: add columns or tables; remove them only in a later release. Old containers can briefly run against the new schema during step 4, and a rollback (section 9) runs old code against the new schema.
- **Safe to re-run.** `alembic upgrade head` is idempotent (verified by the drill).
- **One at a time.** Only the `migrate` container runs migrations, so they never run concurrently.
- **Manual check:** `nexus.sh compose run --rm migrate alembic current` should print the same revision as `alembic heads`.

## 8. Verifying health

```bash
deploy/production/nexus.sh status          # every container should be "healthy"
deploy/production/nexus.sh smoke           # public URL: readiness, dashboard, Grafana, internal endpoints hidden
curl -s https://nexus.example.com/api/v1/ready
# {"status":"ok","postgres":"ok","redis":"ok","rabbitmq":"ok"}   (503 with details if a dependency is down)
deploy/production/nexus.sh logs worker     # JSON logs; Ctrl-C to stop
```

**Failure drills** (`tests/e2e/test_production_resilience.py`, marker `resilience`) restart and kill real containers while real jobs run. They're destructive, so run them before going live or on a staging server:
```bash
sed -i 's/^TASK_ENABLE_TEST_HANDLERS=.*/TASK_ENABLE_TEST_HANDLERS=true/' .env.production && deploy/production/nexus.sh deploy
curl -LsSf https://astral.sh/uv/install.sh | sh && source ~/.local/bin/env
NEXUS_E2E_API_URL=https://nexus.example.com NEXUS_E2E_API_KEY="$KEY" uv run pytest -m resilience -v
sed -i 's/^TASK_ENABLE_TEST_HANDLERS=.*/TASK_ENABLE_TEST_HANDLERS=false/' .env.production && deploy/production/nexus.sh deploy
```

They check:
1. every dependency and container is healthy;
2. jobs survive an API restart;
3. jobs submitted **while RabbitMQ is down** stay PENDING (stored in the outbox) and run when it returns;
4. a graceful worker restart hands unfinished work back, which is then retried;
5. SIGKILLed workers are detected via heartbeat timeout and their jobs retried;
6. PostgreSQL and Redis restarts are survived;
7. retries and dead letters work, including a bulk retry;
8. a full stop/start keeps every job;
9. migrations are idempotent;
10. logs are structured and traceable.

Each job must finish **exactly once**.

## 9. Updates, backups and rollback

**Update to a newer version:**
```bash
cd ~/Nexus && git pull && deploy/production/nexus.sh deploy
```

**Backups:**
```bash
deploy/production/nexus.sh backup                     # now
crontab -e                                            # daily at 03:00, keep 14 days:
0 3 * * * cd /home/nexus/Nexus && deploy/production/nexus.sh backup >/dev/null && find backups -name '*.dump' -mtime +14 -delete
scp nexus@SERVER_IP:Nexus/backups/*.dump ./nexus-backups/   # copy off the server regularly
```

**Rollback:**
```bash
deploy/production/nexus.sh rollback            # to the previously deployed version
deploy/production/nexus.sh rollback 7ec4a73    # to a specific version (its image must still exist)
```
Rollback restarts `api`, `scheduler`, `worker` and `dashboard` on the older images. It **does not downgrade the database schema**; with additive migrations (section 7), older code runs fine on a newer schema. If a migration itself was the problem, restore the backup taken just before that deploy:
```bash
ls backups/                                                      # nexus-<time>-before-<version>.dump
deploy/production/nexus.sh restore backups/nexus-…-before-<bad-version>.dump
deploy/production/nexus.sh rollback <previous-version>
```
`restore` first checks that the file is a valid dump; it refuses empty, truncated or corrupt files without touching anything. It then stops the app services and restores in a **single transaction**. If the restore fails partway, the database is left exactly as it was. Either way, the services are always started again. Everything after the backup's timestamp is lost. All of these paths were drilled, including a restore that fails partway.

> ⚠️ **Backups on the same server are not disaster recovery.** If the server or its disk is lost, `./backups` is lost with it. Copy backups off the server regularly (the `scp` line above, or `rclone` to object storage) and test a restore from the off-site copy once. Until you have done that, treat this deployment as having no disaster recovery.

## 10. Monitoring

- **Grafana:** `https://nexus.example.com/grafana/` (login). The *NEXUS — Job platform* dashboard is the home page.
- **Prometheus:** not public. Open a tunnel from your computer, then visit http://localhost:9090:
  ```bash
  ssh -N -L 9090:127.0.0.1:9090 -L 15672:127.0.0.1:15672 nexus@SERVER_IP
  ```
  The same tunnel serves the RabbitMQ console at http://localhost:15672 (user `nexus`, `RABBITMQ_PASSWORD`).
- **Alerts:** 11 rules are loaded and visible in Prometheus → Alerts. To be notified, add an Alertmanager or import the rules into Grafana alerting with a contact point (e-mail or Slack). That isn't configured by default.
- **Uptime:** point a free external uptime checker at `https://nexus.example.com/api/v1/ready`.

## 11. Troubleshooting

| Symptom | Likely cause → fix |
|---|---|
| `required variable X is missing a value` | `.env.production` lacks X; add it (see `env.example`) |
| API, scheduler or workers restart; logs say `Refusing to start in environment 'production': …` | The message names the problem (default password, dev key, empty `API_KEYS`…); fix it in `.env.production` and deploy |
| Browser shows a certificate error, or Caddy logs `challenge failed` | DNS doesn't point at the server yet, or ports 80/443 are blocked. Check `dig +short nexus.example.com` and the firewall/cloud security group |
| `502` from Caddy | The target container is down or still starting: `nexus.sh status`, then `nexus.sh logs api` |
| Jobs stay `pending` | RabbitMQ unreachable, or the scheduler is down: `nexus.sh status`; `curl …/api/v1/ready` names the failing dependency |
| Jobs stay `queued` | No healthy worker: check the dashboard's Workers page and `nexus.sh logs worker` |
| A worker shows `unhealthy` in the dashboard | Its heartbeat stopped; its jobs are retried automatically. If it was replaced, the old entry is forgotten after a while |
| `migrate` exited non-zero, so nothing started | `nexus.sh logs migrate`; fix, then deploy again. The database is unchanged, because Alembic runs each migration in a transaction |
| RabbitMQ logs `disk alarm` / publishing blocked | Disk almost full: `df -h`, prune old images (`docker image prune -a`), delete old backups |
| Server out of memory during `deploy` | The build runs alongside the stack: add swap (`fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile`) or use a 4 GB server |
| Lost the API key | `nexus.sh new-key <tenant>`, put the new entry in `API_KEYS`, deploy |

## 12. Readiness checklist

The first column was verified in the sandbox, against `compose.production.yml` built from this branch. The sandbox used stand-in base images (Docker Hub was unavailable there), and Grafana was verified separately in CI. The second column is what remains for you.

| Item | Verified | Left to do |
|---|---|---|
| Repository ready | ✅ Branch `production-readiness`; CI validates the production config | Merge when happy |
| Production configuration ready | ✅ `compose.production.yml`, Caddyfile, production Prometheus config, env template | n/a |
| Environment variables documented | ✅ Section 5 and `deploy/production/env.example` | n/a |
| Secrets removed from repository | ✅ No secrets committed; production refuses dev keys and default passwords (unit-tested); `.env.production` git-ignored | Keep it that way |
| Database ready | ✅ Containerized with a volume; backup/restore drilled | Create the server |
| Broker ready | ✅ Containerized with a volume; broker-outage drill passed | Create the server |
| Redis ready | ✅ Password, AOF; restart drill passed | Create the server |
| API deployable | ✅ Healthy behind Caddy; internal endpoints blocked | Domain + DNS |
| Worker deployable | ✅ 2 replicas; drain, kill and restart drills passed | n/a |
| Scheduler deployable | ✅ Healthy; dead-worker recovery verified | n/a |
| Monitoring deployable | ✅ Prometheus scrapes API, scheduler and both workers (DNS discovery), 11 rules OK. Grafana verified in CI on the official image | Optional: alert notifications |
| Migration process verified | ✅ Runs before the apps, idempotent, backup taken first | n/a |
| Health checks verified | ✅ Every container healthy; `/ready` reports each dependency; 10/10 failure drills | Re-run drills on the real server before going live |
| Rollback plan documented | ✅ Section 9; deploy → v2 → rollback → restore drilled | n/a |

**Not yet verified:**
- a real public deployment (real DNS, a Let's Encrypt certificate, the provider's network);
- the official base images on the production compose (CI covers them for the development compose).

The first `nexus.sh deploy` and `nexus.sh smoke` on your server close both.

---

<sub>Price sources (checked November 2026):
[Hetzner price adjustment](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/) ·
[Railway pricing](https://docs.railway.com/pricing) ·
[Fly.io pricing](https://fly.io/pricing) ·
[Render pricing](https://render.com/pricing) ·
[Neon plans](https://neon.com/docs/introduction/plans) ·
[CloudAMQP plans](https://www.cloudamqp.com/plans.html) ·
[Oracle Always Free](https://docs.oracle.com/en-us/iaas/Content/FreeTier/resourceref.htm)</sub>
