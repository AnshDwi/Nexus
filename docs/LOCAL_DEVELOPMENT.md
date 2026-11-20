# Local development

Docker Compose is the primary way to run NEXUS. Everything runs on your machine, nothing costs money, and nothing is exposed beyond `localhost`.

**You need:**
- Docker with Compose v2 (Docker Desktop, or Docker Engine plus the compose plugin);
- about 2 GB of free RAM and 5 GB of disk.

**Only for running tests and editing code on the host:**
- [uv](https://docs.astral.sh/uv/) for Python 3.12;
- Node 22.

## Start

```bash
git clone https://github.com/AnshDwi/Nexus.git
cd Nexus
cp .env.example .env              # optional; every value has a local default
docker compose up --build -d      # add --wait to block until everything is healthy
```

The first run builds two images and pulls the infrastructure images (a few minutes). Later runs take about 30 seconds. `docker compose ps` should list every service as `healthy`, and `migrate` as `exited (0)`.

| What | Where | Credentials |
|---|---|---|
| Dashboard | http://localhost:5173 | API key `dev-key` |
| API docs (Swagger UI) | http://localhost:8000/api/docs | "Authorize" → `dev-key` |
| API base | http://localhost:8000/api/v1 | header `Authorization: Bearer dev-key` |
| Grafana | http://localhost:3000 | anonymous viewer; NEXUS dashboard is the home page |
| Prometheus | http://localhost:9090 | none (alerts under *Alerts*) |
| RabbitMQ console | http://localhost:15672 | `nexus` / `nexus` |
| PostgreSQL | `localhost:5432`, db `nexus` | `nexus` / `nexus` |
| Redis | `localhost:6379` | none |

All ports bind to `127.0.0.1`. If one is already taken on your machine, change it in `.env` (`API_HOST_PORT`, `DASHBOARD_HOST_PORT`, `POSTGRES_HOST_PORT`, …).

Prometheus and Grafana start because `.env` contains `COMPOSE_PROFILES=observability`. Delete that line for a lighter stack (about 250 MB less RAM).

## How it starts (read `docker-compose.yml` alongside this)

```
postgres ─┐
rabbitmq ─┼─(healthy)─► migrate (alembic upgrade head, exits 0)
redis ────┘                  │
                             ▼
              api · scheduler · worker (capacity 4) · worker-small (capacity 2)
                             │ api healthy
                             ▼
              dashboard · prometheus ─► grafana
```

Each arrow is a `depends_on` with `condition: service_healthy` or `service_completed_successfully`, so nothing starts before what it needs:
- the API is only "healthy" when `/api/v1/ready` can reach PostgreSQL, RabbitMQ and Redis;
- the scheduler and workers report health on port 8081 inside their containers.

All services share one Docker network (`nexus_default`) and find each other by service name (`postgres`, `rabbitmq`, `api`, …).

## Everyday commands

| Task | Command |
|---|---|
| Status | `docker compose ps` |
| Logs (all / one service, follow) | `docker compose logs -f` · `docker compose logs -f worker` |
| Only job-related log lines | `docker compose logs worker \| grep '"job_id"'` |
| Restart a service | `docker compose restart api` |
| Rebuild after code changes | `docker compose up --build -d` |
| Run a shell in a service | `docker compose exec api sh` |
| SQL console | `docker compose exec postgres psql -U nexus` |
| Redis console | `docker compose exec redis redis-cli` |
| Queue depths | `docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers` |
| **Stop** (keeps all data) | `docker compose --profile "*" down` |
| Pause without removing containers | `docker compose stop` · resume with `docker compose start` |

Use `--profile "*"` with `down`. A plain `docker compose down` only stops services whose profile is active, so a Grafana or Prometheus started with another profile setting would keep running and keep the network alive.

`stop`/`down` send SIGTERM. Workers then drain: they stop taking jobs, give running jobs up to 25 s, hand the rest back for retry, and deregister. No job is lost.

## Demo data

```bash
docker compose --profile demo run --rm seed
```

The seed submits a scenario of real jobs **through the API**:
- one that succeeds;
- one that fails once, then succeeds on retry;
- one that fails permanently;
- one that exhausts its retries and is dead-lettered;
- eight load jobs spread over both workers.

It then checks every outcome and prints "Demo verified". Run it as often as you like; each run uses new names. It refuses to run against a non-local environment.

## Reset (deletes all local data)

```bash
scripts/reset-local.sh            # lists the volumes it will delete and asks you to type "reset"
docker compose up --build -d      # start again from scratch
```

The script runs `docker compose --profile "*" down --volumes --remove-orphans` after confirmation. It only touches the local project (`nexus`), never the production stack (`nexus-prod`).

## Tests

| Suite | Command | Needs | Touches your dev data? |
|---|---|---|---|
| Backend unit (325) | `uv sync && uv run pytest` | uv | no |
| Lint and types | `uv run ruff check . && uv run ruff format --check . && uv run mypy src` | uv | no |
| Backend integration (72) | `scripts/test-integration.sh` | running stack, uv | **no**: separate database `nexus_test`, RabbitMQ vhost `nexus-test`, Redis db 15 |
| End to end (8) | `uv run pytest -m e2e` | running stack | adds jobs (named `e2e-…`) |
| Frontend (50 + 22 demo-mode) | `cd frontend && npm ci && npm test && npm run test:demo` | Node 22 | no |
| Frontend types, lint, build | `cd frontend && npm run typecheck && npm run lint && npm run build` | Node 22 | no |
| Production failure drills | see [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md#8-verifying-health) | production compose | destructive, so separate stack |

Don't run `uv run pytest -m integration` directly against the dev stack. Those tests truncate tables and purge queues; `scripts/test-integration.sh` exists to point them at their own database and vhost.

## Working on the code

- **Backend:** edit `src/nexus/...`, then `docker compose up --build -d api scheduler worker worker-small`. The API, scheduler and workers share one image, so rebuild all four.
- **Dashboard with hot reload:**
  ```bash
  cd frontend && npm ci && npm run dev
  ```
  This opens http://localhost:5173 and proxies `/api` to the API container on :8000. Stop the `dashboard` container first (`docker compose stop dashboard`), because both use port 5173.
- **Database migrations:**
  ```bash
  uv run alembic revision --autogenerate -m "describe change"
  ```
  The running stack's database is on `localhost:5432` and `.env` points there. Review the generated file, then `docker compose up --build -d` (the `migrate` service applies it) or `uv run alembic upgrade head`.
- **New job type:** add a handler in `src/nexus/infrastructure/execution/handlers.py` and register it in `build_registry`.

## Troubleshooting

| Symptom | Cause → fix |
|---|---|
| `Bind for 127.0.0.1:5432 failed: port is already allocated` | Something else uses the port, often a local PostgreSQL. Set `POSTGRES_HOST_PORT=5433` (or the matching variable) in `.env` |
| `dependency failed to start: container nexus-migrate-1 exited (1)` | A migration failed: `docker compose logs migrate` |
| A service stays `health: starting` and then `unhealthy` | `docker compose logs <service>`. For the API, `curl localhost:8000/api/v1/ready` names the failing dependency |
| Dashboard says "Invalid or missing API key" | Use `dev-key` (Settings → API connection) |
| Jobs stay `queued` | No healthy worker: `docker compose ps worker worker-small`, `docker compose logs worker` |
| Jobs stay `pending` | Broker unreachable or the scheduler is down. Jobs wait safely in the outbox and dispatch when it's back |
| `network nexus_default has active endpoints` on `down` | A profile service is still running; use `docker compose --profile "*" down` |
| Everything is odd after branch switches | `docker compose up --build -d`; if the schema history diverged, `scripts/reset-local.sh` |
