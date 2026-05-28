#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DEPLOY_TARGET="${DEPLOY_TARGET:-remote}"
DEPLOY_HOST="${DEPLOY_HOST:-}"
DEPLOY_USER="${DEPLOY_USER:-}"
DEPLOY_PORT="${DEPLOY_PORT:-}"
DEPLOY_PATH="${DEPLOY_PATH:-/srv/agentic-data-platform/dev/current}"
DEPLOY_PROJECT_NAME="${DEPLOY_PROJECT_NAME:-agentic-data-shared dev}"
DEPLOY_RUN_TESTS="${DEPLOY_RUN_TESTS:-1}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.dev.yml}"
SSH_KEY_PATH="${SSH_KEY_PATH:-}"

log() {
  printf '[deploy-dev] %s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

compose() {
  docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

run_compose_smoke() {
  local compose_config_output
  compose_config_output="$(mktemp /tmp/agentic-data-shared dev-compose.XXXXXX.yml)"

  log "Validating Compose file"
  compose config >"$compose_config_output"
  rm -f "$compose_config_output"

  log "Starting shared development services"
  compose up -d postgres redis minio

  if [[ "$DEPLOY_RUN_TESTS" == "1" ]]; then
    log "Running application smoke checks"
    compose run --rm app
  fi

  log "Current service status"
  compose ps
}

deploy_local() {
  require_command docker
  cd "$ROOT_DIR"
  run_compose_smoke
}

deploy_remote() {
  require_command ssh
  require_command rsync

  if [[ -z "$DEPLOY_HOST" || -z "$DEPLOY_USER" ]]; then
    printf 'DEPLOY_HOST and DEPLOY_USER are required for remote deployment.\n' >&2
    exit 1
  fi

  local remote="${DEPLOY_USER}@${DEPLOY_HOST}"
  local ssh_args=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new)

  if [[ -n "$DEPLOY_PORT" ]]; then
    ssh_args+=(-p "$DEPLOY_PORT")
  fi

  if [[ -n "$SSH_KEY_PATH" ]]; then
    ssh_args+=(-i "$SSH_KEY_PATH")
  fi

  log "Preparing remote path ${DEPLOY_PATH}"
  ssh "${ssh_args[@]}" "$remote" "mkdir -p '$DEPLOY_PATH'"

  log "Syncing repository to ${remote}:${DEPLOY_PATH}"
  rsync -az --delete \
    --exclude '.git' \
    --include '.env.example' \
    --exclude '.env' \
    --exclude '.env.*' \
    --exclude 'AGENTS.md' \
    --exclude 'MEMORY.md' \
    --exclude '.venv' \
    --exclude '__pycache__' \
    --exclude '*.egg-info' \
    --exclude '.pytest_cache' \
    --exclude '.ruff_cache' \
    --exclude '.mypy_cache' \
    --exclude '/node_modules' \
    --exclude '/dist' \
    --exclude '/build' \
    --exclude '/.runtime' \
    --exclude '/data' \
    --exclude '/artifacts' \
    --exclude '/outputs' \
    --exclude '/logs' \
    -e "ssh ${ssh_args[*]}" \
    "${ROOT_DIR}/" "${remote}:${DEPLOY_PATH}/"

  log "Running remote Compose deployment"
  local remote_script
  remote_script=$(cat <<EOF
set -euo pipefail
cd "$DEPLOY_PATH"
compose_config_output=\$(mktemp /tmp/agentic-data-shared dev-compose.XXXXXX.yml)
trap 'rm -f "\$compose_config_output"' EXIT
docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" config >"\$compose_config_output"
docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" up -d postgres redis minio
if [[ "$DEPLOY_RUN_TESTS" == "1" ]]; then
  docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" run --rm app
fi
docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" ps
EOF
)
  ssh "${ssh_args[@]}" "$remote" "bash -se" <<<"$remote_script"
}

case "$DEPLOY_TARGET" in
  local)
    deploy_local
    ;;
  remote)
    deploy_remote
    ;;
  *)
    printf 'Unknown DEPLOY_TARGET: %s. Use "local" or "remote".\n' "$DEPLOY_TARGET" >&2
    exit 1
    ;;
esac
