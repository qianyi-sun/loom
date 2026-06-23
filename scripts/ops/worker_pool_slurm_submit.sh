#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/ops/worker_pool_slurm_submit.sh PLAN_CSV --env-file PATH --repo-dir PATH [--dry-run|--yes]

Submit one remote Loom worker Slurm job for every included row emitted by
worker_pool_plan.py. The command defaults to --dry-run and prints the sbatch
commands it would submit. Pass --yes to submit.

Required:
  PLAN_CSV       CSV from scripts/ops/worker_pool_plan.py
  --env-file     Untracked remote-worker env file path available on each node
  --repo-dir     Loom checkout path available on each node

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

quote() {
  printf '%q' "$1"
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
  sbatch_args=(--job-name="$job_name" --nodelist="$host" --exclusive --time="$time_limit")
  if [[ -n "$partition" ]]; then
    sbatch_args+=(--partition="$partition")
  fi
  if [[ -n "$cpus" ]]; then
    sbatch_args+=(--cpus-per-task="$cpus")
  fi
  if [[ -n "$mem_mib" ]]; then
    sbatch_args+=(--mem="${mem_mib}M")
  fi
  sbatch_args+=(--export="ALL,LOOM_WORKER_MAX_CONCURRENT=${concurrency},LOOM_REMOTE_WORKER_ENV_FILE=${env_file},LOOM_REMOTE_WORKER_REPO_DIR=${repo_dir}")

  if [[ "$submit" -eq 0 ]]; then
    printf 'sbatch'
    printf ' %q' "${sbatch_args[@]}"
    cat <<DRYRUN
 <<'SLURM'
#!/usr/bin/env bash
set -euo pipefail
cd ${repo_q}
export LOOM_WORKER_MAX_CONCURRENT=${concurrency}
docker compose --env-file ${env_q} -f deploy/docker-compose.remote-worker.yml up --build
SLURM
DRYRUN
    return
  fi

  sbatch "${sbatch_args[@]}" <<SLURM
#!/usr/bin/env bash
set -euo pipefail
cd ${repo_q}
export LOOM_WORKER_MAX_CONCURRENT=${concurrency}
docker compose --env-file ${env_q} -f deploy/docker-compose.remote-worker.yml up --build
SLURM
}

tail -n +2 "$plan_csv" | while IFS=, read -r host status cpus mem_total_mib _docker_cpus recommended_concurrency _reason; do
  [[ -z "$host" ]] && continue
  [[ "$status" == "include" ]] || continue
  [[ "$recommended_concurrency" =~ ^[0-9]+$ ]] || continue
  [[ "$recommended_concurrency" -gt 0 ]] || continue
  submit_row "$host" "$cpus" "$mem_total_mib" "$recommended_concurrency"
done
