#!/usr/bin/env bash
# Operate the NEXUS production stack (compose.production.yml) on one server.
#
#   nexus.sh init <domain> <acme-email>  create .env.production with random secrets
#   nexus.sh new-key <tenant>            print a new API key and its API_KEYS entry
#   nexus.sh deploy                      backup, build this commit, migrate, start, verify
#   nexus.sh status                      containers, health and versions
#   nexus.sh smoke                       check the public URL end to end
#   nexus.sh backup                      pg_dump to ./backups/
#   nexus.sh restore <file>              restore a backup (stops app services first)
#   nexus.sh rollback [version]          start the previously deployed version again
#   nexus.sh logs [service]              follow logs
#   nexus.sh stop                        graceful stop (workers drain; data kept)
#   nexus.sh compose <args>              any docker compose command, same settings
#
# Run from the repository root on the server. Requires Docker with Compose v2,
# git, curl and openssl.
set -euo pipefail

cd "$(dirname "$0")/../.."
ENV_FILE=".env.production"
HISTORY=".deploy-history"
VERSION_FILE=".nexus-version" # the version currently deployed
# NEXUS_COMPOSE_OVERRIDE: optional extra compose file for site-specific tweaks.
# Every command runs the deployed version's images, never an implicit "latest".
compose() {
  NEXUS_VERSION="${NEXUS_VERSION:-$(cat "$VERSION_FILE" 2>/dev/null || echo latest)}" \
  docker compose --env-file "$ENV_FILE" -f compose.production.yml \
    ${NEXUS_COMPOSE_OVERRIDE:+-f "$NEXUS_COMPOSE_OVERRIDE"} "$@"
}
die() { echo "error: $*" >&2; exit 1; }
need_env() {
  [ -f "$ENV_FILE" ] || die "$ENV_FILE not found: run '$0 init <domain> <email>' first"
  if grep -qE '^[A-Z_]+=.*CHANGE-ME' "$ENV_FILE"; then die "$ENV_FILE still contains CHANGE-ME values"; fi
}
env_value() { grep -E "^$1=" "$ENV_FILE" | tail -1 | cut -d= -f2-; }
secret() { openssl rand -hex 32; }

new_key() {
  local tenant="${1:?usage: new-key <tenant>}"
  [[ "$tenant" =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]] || die "tenant must be lowercase letters, digits, - or _"
  local key hash
  key="nx_$(openssl rand -hex 24)"
  hash=$(printf '%s' "$key" | openssl dgst -sha256 -r | cut -d' ' -f1)
  echo "API key for tenant '$tenant' (shown once, store it in a password manager):"
  echo "  $key"
  echo "Add this entry to API_KEYS in $ENV_FILE (comma-separated), then run: $0 deploy"
  echo "  $tenant:$hash"
}

cmd_init() {
  local domain="${1:?usage: init <domain> <acme-email>}" email="${2:?usage: init <domain> <acme-email>}"
  [ -f "$ENV_FILE" ] && die "$ENV_FILE already exists; edit it instead"
  local key hash
  key="nx_$(openssl rand -hex 24)"
  hash=$(printf '%s' "$key" | openssl dgst -sha256 -r | cut -d' ' -f1)
  sed -e "s|^NEXUS_DOMAIN=.*|NEXUS_DOMAIN=$domain|" \
      -e "s|^ACME_EMAIL=.*|ACME_EMAIL=$email|" \
      -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$(secret)|" \
      -e "s|^RABBITMQ_PASSWORD=.*|RABBITMQ_PASSWORD=$(secret)|" \
      -e "s|^REDIS_PASSWORD=.*|REDIS_PASSWORD=$(secret)|" \
      -e "s|^API_KEYS=.*|API_KEYS=admin:$hash|" \
      -e "s|^CRON_SECRET=.*|CRON_SECRET=$(secret)|" \
      -e "s|^METRICS_TOKEN=.*|METRICS_TOKEN=$(secret)|" \
      -e "s|^GRAFANA_ADMIN_PASSWORD=.*|GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 12)|" \
      deploy/production/env.example > "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "Wrote $ENV_FILE (mode 600). Your first API key, tenant 'admin' (shown once):"
  echo "  $key"
  echo "Grafana: user admin, password in $ENV_FILE (GRAFANA_ADMIN_PASSWORD)."
}

cmd_backup() {
  need_env
  local label="${1:-manual}" file tmp
  [[ "$label" =~ ^[A-Za-z0-9._-]+$ ]] || die "invalid backup label"
  # Backups hold every job payload: private directory, private files.
  mkdir -p backups && chmod 700 backups
  if ! compose ps --status running postgres | grep -q postgres; then
    echo "postgres is not running; skipping backup"
    return 0
  fi
  file="backups/nexus-$(date -u +%Y%m%dT%H%M%SZ)-$label.dump"
  tmp="$file.partial"
  # pg_dump runs inside the container (one consistent snapshot) and streams to
  # a host file created with mode 600; nothing is written inside the container.
  # shellcheck disable=SC2016 # $POSTGRES_* expand inside the container
  if ! (umask 077 && compose exec -T postgres sh -c \
        'PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "$tmp") \
      || [ ! -s "$tmp" ]; then
    rm -f "$tmp"
    die "backup failed; nothing was changed"
  fi
  mv "$tmp" "$file"
  echo "backup: $file ($(du -h "$file" | cut -f1))"
}

cmd_restore() {
  need_env
  local file="${1:?usage: restore backups/<file>.dump}"
  [ -f "$file" ] && [ -s "$file" ] || die "$file not found or empty"
  # shellcheck disable=SC2016 # $POSTGRES_* expand inside the container
  compose exec -T postgres pg_restore --list < "$file" > /dev/null \
    || die "$file is not a readable pg_dump archive; nothing was changed"
  echo "Stopping API, scheduler and workers; restoring $(basename "$file") ..."
  compose stop api scheduler worker
  # Bring the services back whatever happens below.
  trap 'compose up -d --wait api scheduler worker >/dev/null' EXIT
  # shellcheck disable=SC2016 # $POSTGRES_* expand inside the container
  compose exec -T postgres sh -c \
    'PGPASSWORD="$POSTGRES_PASSWORD" pg_restore -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists --single-transaction' \
    < "$file"
  echo "restored $(basename "$file"); restarting services"
}

cmd_smoke() {
  need_env
  local domain base
  domain=$(env_value NEXUS_DOMAIN)
  base="https://$domain"
  [[ "$domain" == :* ]] && base="http://localhost${domain}"
  echo "checking $base"
  curl -fsS "$base/api/v1/ready" && echo
  curl -fsS -o /dev/null "$base/" && echo "dashboard: ok"
  local internal
  internal=$(curl -s -o /dev/null -w '%{http_code}' "$base/api/v1/internal/metrics")
  [ "$internal" = 404 ] || die "/api/v1/internal/metrics is publicly reachable (HTTP $internal)"
  echo "internal endpoints hidden: ok"
  curl -fsS -o /dev/null "$base/grafana/api/health" && echo "grafana: ok"
}

cmd_deploy() {
  need_env
  [ "$(stat -c %a "$ENV_FILE")" = 600 ] || { chmod 600 "$ENV_FILE"; echo "fixed $ENV_FILE permissions (600)"; }
  local version previous free_gb
  version=$(git rev-parse --short HEAD)
  previous=$(cat "$VERSION_FILE" 2>/dev/null || true)
  free_gb=$(df -Pk . | awk 'NR==2 {print int($4/1048576)}')
  [ "$free_gb" -ge 3 ] || die "only ${free_gb} GB free disk; need 3 GB to build and deploy safely"
  [ -z "$(git status --porcelain --untracked-files=no)" ] || echo "warning: uncommitted changes are included in $version"
  echo "== deploying $version"
  cmd_backup "before-$version"
  export NEXUS_VERSION="$version"
  compose build
  # `up --wait` runs the one-shot migration first (every app service depends
  # on it) and returns only when every health check passes.
  if ! compose up -d --remove-orphans --wait --wait-timeout 300; then
    echo "== deploy of $version FAILED" >&2
    compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}' >&2 || true
    if [ -n "$previous" ] && docker image inspect "nexus-app:$previous" >/dev/null 2>&1; then
      echo "== restoring the previous version $previous" >&2
      NEXUS_VERSION="$previous" compose up -d --no-build --wait --wait-timeout 300 \
        api scheduler worker dashboard >&2 || true
    fi
    die "deploy failed; see 'nexus.sh logs migrate' / 'nexus.sh logs api'. Still running: ${previous:-nothing}"
  fi
  echo "$version" > "$VERSION_FILE"
  echo "$(date -u +%FT%TZ) $version" >> "$HISTORY"
  prune_images
  compose ps --format 'table {{.Service}}\t{{.Status}}'
  echo "== deployed $version"
  ( cmd_smoke ) || echo "warning: smoke check failed (DNS or certificate not ready yet?)"
}

# Keep the images of the last 3 deployed versions (for rollback); remove older ones.
prune_images() {
  local keep
  keep=$(awk '{print $2}' "$HISTORY" | awk '!seen[$0]++' | tail -3 | tr '\n' ' ')
  for repo in nexus-app nexus-dashboard; do
    docker image ls "$repo" --format '{{.Tag}}' | while read -r tag; do
      case " $keep " in *" $tag "*) ;; *) docker image rm "$repo:$tag" >/dev/null 2>&1 || true ;; esac
    done
  done
  docker builder prune -f --filter until=168h >/dev/null 2>&1 || true
}

cmd_rollback() {
  need_env
  local target="${1:-}"
  if [ -z "$target" ]; then
    [ -f "$HISTORY" ] || die "no deploy history"
    target=$(awk '{print $2}' "$HISTORY" | uniq | tail -2 | head -1)
    [ "$target" != "$(awk '{print $2}' "$HISTORY" | tail -1)" ] || die "no previous version recorded"
  fi
  docker image inspect "nexus-app:$target" >/dev/null 2>&1 || die "image nexus-app:$target not found (build it: git checkout $target && $0 deploy)"
  echo "== rolling back to $target (database schema is NOT downgraded; see docs/PRODUCTION_DEPLOYMENT.md)"
  echo "$target" > "$VERSION_FILE"
  compose up -d --no-build --wait --wait-timeout 300 api scheduler worker dashboard
  echo "$(date -u +%FT%TZ) $target rollback" >> "$HISTORY"
  compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}'
}

cmd_status() {
  need_env
  compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}'
  [ -f "$HISTORY" ] && { echo; echo "recent deployments:"; tail -5 "$HISTORY"; }
}

case "${1:-}" in
  init) shift; cmd_init "$@" ;;
  new-key) shift; new_key "$@" ;;
  deploy) cmd_deploy ;;
  status) cmd_status ;;
  smoke) cmd_smoke ;;
  backup) cmd_backup ;;
  restore) shift; cmd_restore "$@" ;;
  rollback) shift; cmd_rollback "$@" ;;
  logs) shift; need_env; compose logs -f --tail 200 "$@" ;;
  stop) need_env; compose stop ;;
  compose) shift; need_env; compose "$@" ;;
  *) sed -n '2,15p' "$0"; exit 1 ;;
esac
