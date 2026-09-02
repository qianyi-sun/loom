#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/ops/worker_pool_slurm_submit.sh PLAN_CSV --env-file PATH --repo-dir PATH \
  --sandbox-identity NAME --candidate-sha SHA --container-cpus N \
  --container-memory-mib N --container-pids N [--dry-run|--yes]

Submit one remote Loom worker Slurm job for every included row emitted by
worker_pool_plan.py. The command defaults to --dry-run and prints the sbatch
commands it would submit. Pass --yes to submit.

Required:
  PLAN_CSV       CSV from scripts/ops/worker_pool_plan.py
  --env-file     Untracked remote-worker env file path available on each node
  --repo-dir     Loom checkout path available on each node
  --sandbox-identity  Lowercase environment/sandbox identity
  --candidate-sha     Exact 40-character lowercase candidate SHA
  --container-cpus    Positive per-container CPU ceiling
  --container-memory-mib  Positive per-container memory ceiling in MiB
  --container-pids    Positive per-container PID ceiling

Optional:
  --partition    Slurm partition name
  --time         Slurm time limit, default 7-00:00:00
  --yes          Submit jobs
  --dry-run      Print jobs without submitting (default)
USAGE
}

plan_csv=${1:-}
if [[ -z "$plan_csv" || "$plan_csv" == "-h" || "$plan_csv" == "--help" ]]; then
  usage
  [[ "$plan_csv" == "-h" || "$plan_csv" == "--help" ]] && exit 0
  exit 2
fi
shift

env_file=
repo_dir=
partition=
time_limit="7-00:00:00"
submit=0
sandbox_identity=
candidate_sha=
container_cpus=
container_memory_mib=
container_pids=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      env_file=${2:-}
      shift 2
      ;;
    --repo-dir)
      repo_dir=${2:-}
      shift 2
      ;;
    --partition)
      partition=${2:-}
      shift 2
      ;;
    --time)
      time_limit=${2:-}
      shift 2
      ;;
    --sandbox-identity)
      sandbox_identity=${2:-}
      shift 2
      ;;
    --candidate-sha)
      candidate_sha=${2:-}
      shift 2
      ;;
    --container-cpus)
      container_cpus=${2:-}
      shift 2
      ;;
    --container-memory-mib)
      container_memory_mib=${2:-}
      shift 2
      ;;
    --container-pids)
      container_pids=${2:-}
      shift 2
      ;;
    --yes)
      submit=1
      shift
      ;;
    --dry-run)
      submit=0
      shift
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -r "$plan_csv" ]]; then
  echo "error: plan CSV is not readable: $plan_csv" >&2
  exit 2
fi
if [[ -z "$env_file" ]]; then
  echo "error: --env-file is required" >&2
  exit 2
fi
if [[ -z "$repo_dir" ]]; then
  echo "error: --repo-dir is required" >&2
  exit 2
fi
if [[ ! "$sandbox_identity" =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]]; then
  echo "error: --sandbox-identity must be a lowercase Slurm sandbox identity" >&2
  exit 2
fi
if [[ ! "$candidate_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: --candidate-sha must be an exact 40-character lowercase SHA" >&2
  exit 2
fi
if [[ ! "$container_cpus" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] || [[ "$container_cpus" =~ ^0+([.]0+)?$ ]]; then
  echo "error: --container-cpus must be positive" >&2
  exit 2
fi
if [[ ! "$container_memory_mib" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: --container-memory-mib must be positive" >&2
  exit 2
fi
if [[ ! "$container_pids" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: --container-pids must be positive" >&2
  exit 2
fi

quote() {
  printf '%q' "$1"
}

emit_slurm_script() {
  local repo_q=$1
  local env_q=$2
  local concurrency=$3

  cat <<SLURM
#!/usr/bin/env bash
set -euo pipefail
cd ${repo_q}
export LOOM_WORKER_MAX_CONCURRENT=${concurrency}
export LOOM_WORKER_SLURM_JOB_ID="\$SLURM_JOB_ID"
export LOOM_WORKER_COMPOSE_PROJECT="loom-\${LOOM_WORKER_SANDBOX_IDENTITY}-\${LOOM_WORKER_CANDIDATE_SHA:0:12}-\${SLURM_JOB_ID}"
compose_args=(--project-name "\$LOOM_WORKER_COMPOSE_PROJECT" --env-file ${env_q} -f deploy/docker-compose.remote-worker.yml)

cleanup() {
  status=\${1:-\$?}
  trap - EXIT INT TERM
  if [[ -n "\${compose_pid:-}" ]]; then
    kill "\$compose_pid" 2>/dev/null || true
    wait "\$compose_pid" 2>/dev/null || true
  fi
  cleanup_status=0
  docker compose "\${compose_args[@]}" down --remove-orphans || cleanup_status=\$?
  for volume_suffix in remote_worker_trajectories remote_worker_benchmarks; do
    volume_name="\${LOOM_WORKER_COMPOSE_PROJECT}_\${volume_suffix}"
    if docker volume inspect "\$volume_name" >/dev/null 2>&1; then
      volume_status=0
      docker volume rm "\$volume_name" || volume_status=\$?
      if [[ "\$cleanup_status" -eq 0 && "\$volume_status" -ne 0 ]]; then
        cleanup_status=\$volume_status
      fi
    fi
  done
  if [[ "\$status" -eq 0 && "\$cleanup_status" -ne 0 ]]; then
    status=\$cleanup_status
  fi
  exit "\$status"
}

trap cleanup EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM
docker compose "\${compose_args[@]}" up --build &
compose_pid=\$!
wait "\$compose_pid"
SLURM
}

submit_row() {
  local host=$1
  local cpus=$2
  local mem_mib=$3
  local concurrency=$4
  local job_name safe_host env_q repo_q
  safe_host=${host//[^A-Za-z0-9_-]/-}
  job_name="loom-worker-${safe_host}"
  env_q=$(quote "$env_file")
  repo_q=$(quote "$repo_dir")

  local -a sbatch_args
  sbatch_args=(--job-name="$job_name" --nodelist="$host" --time="$time_limit")
  if [[ -n "$partition" ]]; then
    sbatch_args+=(--partition="$partition")
  fi
  if [[ -n "$cpus" ]]; then
    sbatch_args+=(--cpus-per-task="$cpus")
  fi
  if [[ -n "$mem_mib" ]]; then
    sbatch_args+=(--mem="${mem_mib}M")
  fi
  sbatch_args+=(--export="ALL,LOOM_WORKER_MAX_CONCURRENT=${concurrency},LOOM_REMOTE_WORKER_ENV_FILE=${env_file},LOOM_REMOTE_WORKER_REPO_DIR=${repo_dir},LOOM_WORKER_SANDBOX_IDENTITY=${sandbox_identity},LOOM_WORKER_CANDIDATE_SHA=${candidate_sha},LOOM_WORKER_CONTAINER_CPUS=${container_cpus},LOOM_WORKER_CONTAINER_MEMORY_MIB=${container_memory_mib},LOOM_WORKER_CONTAINER_PIDS=${container_pids}")

  if [[ "$submit" -eq 0 ]]; then
    printf 'sbatch'
    printf ' %q' "${sbatch_args[@]}"
    printf " <<'SLURM'\n"
    emit_slurm_script "$repo_q" "$env_q" "$concurrency"
    printf "SLURM\n"
    return
  fi

  emit_slurm_script "$repo_q" "$env_q" "$concurrency" | sbatch "${sbatch_args[@]}"
}

tail -n +2 "$plan_csv" | while IFS=, read -r host status cpus mem_total_mib _docker_cpus recommended_concurrency _reason; do
  [[ -z "$host" ]] && continue
  [[ "$status" == "include" ]] || continue
  [[ "$recommended_concurrency" =~ ^[0-9]+$ ]] || continue
  [[ "$recommended_concurrency" -gt 0 ]] || continue
  submit_row "$host" "$cpus" "$mem_total_mib" "$recommended_concurrency"
done
