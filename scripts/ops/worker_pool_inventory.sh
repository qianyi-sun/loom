#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/ops/worker_pool_inventory.sh HOSTFILE

Collect non-destructive inventory from candidate remote worker hosts.

HOSTFILE contains one SSH target per line. Blank lines and lines starting
with '#' are ignored. The script does not scan networks; operators provide
the exact hosts they are allowed to inspect.

Required environment:
  LOOM_WORKER_CONTROL_PLANE_URL   Control Plane URL reachable by workers
  LOOM_WORKER_GATEWAY_URL         LLM Gateway URL reachable by workers
  LOOM_WORKER_MINIO_ENDPOINT      MinIO S3 endpoint reachable by workers

Optional environment:
  SSH_CONNECT_TIMEOUT             SSH timeout in seconds (default: 5)
  SSH_OPTS                        Extra ssh options, shell-split by ssh

Example:
  LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:8080 \
  LOOM_WORKER_GATEWAY_URL=http://control-node:9100 \
  LOOM_WORKER_MINIO_ENDPOINT=http://control-node:9000 \
    scripts/ops/worker_pool_inventory.sh worker-hosts.txt
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

hostfile=${1:-}
if [[ -z "$hostfile" ]]; then
  usage >&2
  exit 2
fi
if [[ ! -r "$hostfile" ]]; then
  echo "error: hostfile is not readable: $hostfile" >&2
  exit 2
fi

: "${LOOM_WORKER_CONTROL_PLANE_URL:?set LOOM_WORKER_CONTROL_PLANE_URL}"
: "${LOOM_WORKER_GATEWAY_URL:?set LOOM_WORKER_GATEWAY_URL}"
: "${LOOM_WORKER_MINIO_ENDPOINT:?set LOOM_WORKER_MINIO_ENDPOINT}"

ssh_timeout=${SSH_CONNECT_TIMEOUT:-5}

quote() {
  printf '%q' "$1"
}

run_remote() {
  local host=$1
  local cp_url gw_url minio_url
  cp_url=$(quote "$LOOM_WORKER_CONTROL_PLANE_URL")
  gw_url=$(quote "$LOOM_WORKER_GATEWAY_URL")
  minio_url=$(quote "$LOOM_WORKER_MINIO_ENDPOINT")

  # shellcheck disable=SC2086 # SSH_OPTS is intentionally operator-supplied.
  ssh -o BatchMode=yes -o ConnectTimeout="$ssh_timeout" ${SSH_OPTS:-} "$host" "bash -s" <<REMOTE
set -euo pipefail
export LOOM_WORKER_CONTROL_PLANE_URL=$cp_url
export LOOM_WORKER_GATEWAY_URL=$gw_url
export LOOM_WORKER_MINIO_ENDPOINT=$minio_url

probe_url() {
  local name=\$1
  local url=\$2
  if curl -fsS --max-time 3 "\$url" >/dev/null; then
    echo "\$name=ok"
  else
    echo "\$name=failed"
  fi
}

echo "host=\$(hostname)"
printf 'ips='
hostname -I 2>/dev/null | tr ' ' ',' | sed 's/,\$//'
echo "cpus=\$(nproc 2>/dev/null || echo unknown)"
lscpu 2>/dev/null | awk -F: '/^Model name/ {gsub(/^ +/, "", \$2); print "cpu_model=" \$2; exit}' || true
free -m 2>/dev/null | awk '/^Mem:/ {print "mem_total_mib=" \$2 " mem_available_mib=" \$7} /^Swap:/ {print "swap_total_mib=" \$2 " swap_free_mib=" \$4}' || true
df -h / 2>/dev/null | awk 'NR==2 {print "root_disk_size=" \$2 " root_disk_used=" \$3 " root_disk_avail=" \$4 " root_disk_use_pct=" \$5}' || true
if command -v docker >/dev/null 2>&1; then
  docker info --format 'docker_version={{.ServerVersion}} docker_cpus={{.NCPU}} docker_mem_bytes={{.MemTotal}}' 2>/dev/null || echo docker_info=failed
else
  echo docker=missing
fi
probe_url control_plane "\${LOOM_WORKER_CONTROL_PLANE_URL%/}/healthz"
probe_url gateway "\${LOOM_WORKER_GATEWAY_URL%/}/healthz"
probe_url minio "\${LOOM_WORKER_MINIO_ENDPOINT%/}/minio/health/live"
REMOTE
}

while IFS= read -r host || [[ -n "$host" ]]; do
  host=${host%%#*}
  host=$(printf '%s' "$host" | tr -d '\r' | xargs)
  [[ -z "$host" ]] && continue
  echo "## $host"
  if ! run_remote "$host"; then
    echo "ssh=failed"
  fi
done < "$hostfile"
