#!/usr/bin/env bash
# Converge the inert Phase 1 Slurm prerequisites for dynamic task-image builders.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LOOM_DEFAULT_POLICY_PATH="$REPO_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
LOOM_DEFAULT_STATE_ROOT="/var/lib/loom-task-builder/slurm-authority"
LOOM_POLICY_PATH="${LOOM_POLICY_PATH:-$LOOM_DEFAULT_POLICY_PATH}"
LOOM_STATE_ROOT="${LOOM_STATE_ROOT:-$LOOM_DEFAULT_STATE_ROOT}"
LOOM_STATE_OWNER="${LOOM_STATE_OWNER:-root}"
LOOM_STATE_GROUP="${LOOM_STATE_GROUP:-root}"
LOOM_CONTROLLER_HOST="${LOOM_CONTROLLER_HOST:-$(hostname -s)}"
LOOM_HOST_ARCH="${LOOM_HOST_ARCH:-$(uname -m)}"

loom_builder_slurm_error() {
  echo "error: $*" >&2
  return 1
}

loom_builder_slurm_load_policy() {
  local cluster_id="$1"
  local output
  local values=()

  if [[ ! -f "$LOOM_POLICY_PATH" || -L "$LOOM_POLICY_PATH" ]]; then
    loom_builder_slurm_error "prerequisite policy is unavailable"
    return
  fi
  if ! output="$(python3 - "$LOOM_POLICY_PATH" "$cluster_id" <<'PY'
import pathlib
import sys
import tomllib

policy_path = pathlib.Path(sys.argv[1])
cluster_id = sys.argv[2]
if policy_path.stat().st_size > 2 * 1024 * 1024:
    raise SystemExit("policy is too large")
policy = tomllib.loads(policy_path.read_text(encoding="utf-8"))
if policy.get("schema") != "loom.task-image-builder-prerequisites/v1":
    raise SystemExit("invalid policy schema")
if policy.get("production_certification_allowed") is not False:
    raise SystemExit("Phase 1 policy is not inert")
if policy.get("certified_nodes") != []:
    raise SystemExit("Phase 1 policy certifies nodes")

clusters = [item for item in policy.get("clusters", []) if item.get("id") == cluster_id]
if len(clusters) != 1:
    raise SystemExit("cluster policy is not unique")
cluster = clusters[0]
identity = policy["identity"]
resources = policy["resource_profile"]

if identity.get("user") != "loom-builder" or identity.get("group") != "loom-task-builder":
    raise SystemExit("builder identity is not exact")
if (
    resources.get("cpus") != 8
    or resources.get("memory_mib") != 32768
    or resources.get("wall_time") != "02:00:00"
    or resources.get("max_jobs_per_user") != 1
    or resources.get("max_submit_jobs_per_user") != 1
):
    raise SystemExit("builder resource profile is not exact")
if (
    cluster.get("trial_priority_tier") != 100
    or cluster.get("builder_priority_tier") != 200
    or cluster["builder_priority_tier"] <= cluster["trial_priority_tier"]
):
    raise SystemExit("builder partition priority is not exact")
if (
    cluster.get("builder_partition") != "loom-task-builder"
    or cluster.get("slurm_account") != "loom-task-builder"
    or cluster.get("slurm_qos") != "loom-task-image-builder"
):
    raise SystemExit("builder Slurm identity is not exact")

values = (
    cluster["slurm_cluster"],
    cluster["architecture"],
    cluster["controller"],
    cluster["trial_partition"],
    str(cluster["trial_priority_tier"]),
    cluster["builder_partition"],
    str(cluster["builder_priority_tier"]),
    ",".join(cluster["builder_nodes"]),
    cluster["builder_nodes_expression"],
    cluster["trial_partition_anchor"],
    cluster["builder_partition_line"],
    cluster["slurm_account"],
    cluster["slurm_qos"],
    cluster["slurm_config"],
    cluster["slurm_config_owner"],
    cluster["slurm_config_group"],
    cluster["slurm_config_mode"],
    identity["user"],
    str(resources["cpus"]),
    str(resources["memory_mib"]),
    resources["wall_time"],
    str(resources["max_jobs_per_user"]),
    str(resources["max_submit_jobs_per_user"]),
)
for value in values:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise SystemExit("policy contains an unsafe value")
    print(value)
PY
  )"; then
    loom_builder_slurm_error "prerequisite policy validation failed"
    return
  fi
  mapfile -t values <<<"$output"
  if [[ "${#values[@]}" -ne 23 ]]; then
    loom_builder_slurm_error "prerequisite policy output is incomplete"
    return
  fi

  LOOM_SLURM_CLUSTER="${values[0]}"
  LOOM_EXPECTED_ARCH="${values[1]}"
  LOOM_EXPECTED_CONTROLLER="${values[2]}"
  LOOM_TRIAL_PARTITION="${values[3]}"
  LOOM_TRIAL_TIER="${values[4]}"
  LOOM_BUILDER_PARTITION="${values[5]}"
  LOOM_BUILDER_TIER="${values[6]}"
  LOOM_EXPECTED_NODES_CSV="${values[7]}"
  LOOM_BUILDER_NODES_EXPRESSION="${values[8]}"
  LOOM_TRIAL_PARTITION_ANCHOR="${values[9]}"
  LOOM_BUILDER_PARTITION_LINE="${values[10]}"
  LOOM_SLURM_ACCOUNT="${values[11]}"
  LOOM_SLURM_QOS="${values[12]}"
  LOOM_SLURM_CONFIG="${values[13]}"
  LOOM_SLURM_CONFIG_OWNER="${values[14]}"
  LOOM_SLURM_CONFIG_GROUP="${values[15]}"
  LOOM_SLURM_CONFIG_MODE="${values[16]}"
  LOOM_BUILDER_USER="${values[17]}"
  LOOM_BUILDER_CPUS="${values[18]}"
  LOOM_BUILDER_MEMORY_MIB="${values[19]}"
  LOOM_BUILDER_WALL="${values[20]}"
  LOOM_BUILDER_MAX_JOBS="${values[21]}"
  LOOM_BUILDER_MAX_SUBMIT="${values[22]}"
  LOOM_BACKUP="$LOOM_STATE_ROOT/slurm.conf.before-loom-task-builder"

  if [[ "$LOOM_HOST_ARCH" != "$LOOM_EXPECTED_ARCH" ]]; then
    loom_builder_slurm_error "controller architecture does not match cluster policy"
    return
  fi
  if [[ "$LOOM_CONTROLLER_HOST" != "$LOOM_EXPECTED_CONTROLLER" ]]; then
    loom_builder_slurm_error "controller hostname does not match cluster policy"
    return
  fi
}

loom_builder_slurm_validate_controller() {
  local live_config
  if ! live_config="$(scontrol show config)"; then
    loom_builder_slurm_error "Slurm controller readback is unavailable"
    return
  fi
  if ! grep -Eq "^ClusterName[[:space:]]*=[[:space:]]*$LOOM_SLURM_CLUSTER$" \
    <<<"$live_config"; then
    loom_builder_slurm_error "Slurm cluster readback does not match policy"
    return
  fi
  if ! grep -Eq \
    "^SlurmctldHost\[0\][[:space:]]*=[[:space:]]*$LOOM_EXPECTED_CONTROLLER(\(|$)" \
    <<<"$live_config"; then
    loom_builder_slurm_error "Slurm controller readback does not match policy"
    return
  fi
}

loom_builder_slurm_validate_backup() {
  if [[ -e "$LOOM_STATE_ROOT" || -L "$LOOM_STATE_ROOT" ]]; then
    if [[ -L "$LOOM_STATE_ROOT" || ! -d "$LOOM_STATE_ROOT" \
      || "$(stat -c '%U:%G:%a:%F' "$LOOM_STATE_ROOT")" \
        != "$LOOM_STATE_OWNER:$LOOM_STATE_GROUP:755:directory" ]]; then
      loom_builder_slurm_error "Slurm authority state directory is unsafe"
      return
    fi
  fi
  if [[ ! -e "$LOOM_BACKUP" && ! -L "$LOOM_BACKUP" ]]; then
    return 0
  fi
  if [[ -L "$LOOM_BACKUP" || ! -f "$LOOM_BACKUP" \
    || "$(stat -c '%U:%G:%a:%F' "$LOOM_BACKUP")" \
      != "$LOOM_STATE_OWNER:$LOOM_STATE_GROUP:600:regular file" ]]; then
    loom_builder_slurm_error "Slurm configuration backup is unsafe"
    return
  fi
  if [[ "$LOOM_PARTITION_CONVERGED" == "0" ]]; then
    if ! cmp -s "$LOOM_BACKUP" "$LOOM_SLURM_CONFIG"; then
      loom_builder_slurm_error "Slurm configuration backup drift is unsafe"
      return
    fi
  fi
}

loom_builder_slurm_validate_durable_config() {
  local anchor_count builder_count expected_mode named_count
  expected_mode="${LOOM_SLURM_CONFIG_MODE#0}"
  if [[ -L "$LOOM_SLURM_CONFIG" || ! -f "$LOOM_SLURM_CONFIG" \
    || "$(stat -c '%U:%G:%a:%F' "$LOOM_SLURM_CONFIG")" \
      != "$LOOM_SLURM_CONFIG_OWNER:$LOOM_SLURM_CONFIG_GROUP:$expected_mode:regular file" ]]; then
    loom_builder_slurm_error "Slurm configuration metadata is unsafe"
    return
  fi
  anchor_count="$(grep -Fxc "$LOOM_TRIAL_PARTITION_ANCHOR" "$LOOM_SLURM_CONFIG" || true)"
  builder_count="$(grep -Fxc "$LOOM_BUILDER_PARTITION_LINE" "$LOOM_SLURM_CONFIG" || true)"
  named_count="$(grep -Ec \
    "^PartitionName=$LOOM_BUILDER_PARTITION([[:space:]]|$)" \
    "$LOOM_SLURM_CONFIG" || true)"
  if [[ "$anchor_count" != "1" ]]; then
    loom_builder_slurm_error "trial partition anchor drift is unsafe"
    return
  fi
  if [[ "$builder_count" == "0" && "$named_count" == "0" ]]; then
    LOOM_PARTITION_CONVERGED=0
  elif [[ "$builder_count" == "1" && "$named_count" == "1" ]]; then
    LOOM_PARTITION_CONVERGED=1
  else
    loom_builder_slurm_error "builder partition drift is unsafe"
    return
  fi
  loom_builder_slurm_validate_backup
}

loom_builder_slurm_state_nodes() {
  local line="$1"
  local partition="$2"
  local field nodes_expression state token
  if ! state="$(scontrol show partition "$partition" -o)"; then
    loom_builder_slurm_error "live partition readback is unavailable for $partition"
    return
  fi
  for token in $line; do
    if [[ " $state " != *" $token "* ]]; then
      loom_builder_slurm_error "live partition readback drift for $partition"
      return
    fi
  done
  nodes_expression=""
  for field in $state; do
    case "$field" in
      Nodes=*) nodes_expression="${field#Nodes=}" ;;
    esac
  done
  if [[ -z "$nodes_expression" ]]; then
    loom_builder_slurm_error "live partition node readback is incomplete for $partition"
    return
  fi
  if ! scontrol show hostnames "$nodes_expression"; then
    loom_builder_slurm_error "live partition node expansion failed for $partition"
    return
  fi
}

loom_builder_slurm_validate_live_partitions() {
  local builder_nodes expected_nodes trial_nodes
  expected_nodes="$(tr ',' '\n' <<<"$LOOM_EXPECTED_NODES_CSV")"
  if ! trial_nodes="$(loom_builder_slurm_state_nodes \
    "$LOOM_TRIAL_PARTITION_ANCHOR" "$LOOM_TRIAL_PARTITION")"; then
    return 1
  fi
  if [[ "$trial_nodes" != "$expected_nodes" ]]; then
    loom_builder_slurm_error "trial partition node-set drift is unsafe"
    return
  fi
  if [[ "$LOOM_PARTITION_CONVERGED" == "0" ]]; then
    return 0
  fi
  if ! builder_nodes="$(loom_builder_slurm_state_nodes \
    "$LOOM_BUILDER_PARTITION_LINE" "$LOOM_BUILDER_PARTITION")"; then
    return 1
  fi
  if [[ "$builder_nodes" != "$expected_nodes" || "$builder_nodes" != "$trial_nodes" ]]; then
    loom_builder_slurm_error "builder and trial partition node sets do not overlap exactly"
    return
  fi
  if (( LOOM_BUILDER_TIER <= LOOM_TRIAL_TIER )); then
    loom_builder_slurm_error "builder partition priority does not dominate trial priority"
    return
  fi
}

loom_builder_slurm_read_accounting() {
  local desired_association desired_qos legacy_qos
  if ! LOOM_ACCOUNT_ROW="$(timeout 30 sacctmgr --noheader --parsable2 \
    show account where "name=$LOOM_SLURM_ACCOUNT" format=Account </dev/null)"; then
    loom_builder_slurm_error "Slurm account readback is unavailable"
    return
  fi
  if ! LOOM_QOS_ROW="$(timeout 30 sacctmgr --noheader --parsable2 \
    show qos where "name=$LOOM_SLURM_QOS" \
    format=Name,Flags,Priority,MaxJobsPU,MaxSubmitJobsPU,MaxWall,GrpTRES </dev/null)"; then
    loom_builder_slurm_error "Slurm QoS readback is unavailable"
    return
  fi
  if ! LOOM_ASSOCIATION_ROW="$(timeout 30 sacctmgr --noheader --parsable2 \
    show association where "cluster=$LOOM_SLURM_CLUSTER" \
    "account=$LOOM_SLURM_ACCOUNT" "user=$LOOM_BUILDER_USER" \
    "partition=$LOOM_BUILDER_PARTITION" \
    format=Cluster,Account,User,Partition,QOS,DefaultQOS </dev/null)"; then
    loom_builder_slurm_error "Slurm association readback is unavailable"
    return
  fi

  desired_qos="$LOOM_SLURM_QOS|DenyOnLimit|0|$LOOM_BUILDER_MAX_JOBS|$LOOM_BUILDER_MAX_SUBMIT|$LOOM_BUILDER_WALL|cpu=$LOOM_BUILDER_CPUS,mem=${LOOM_BUILDER_MEMORY_MIB}M,node=1|"
  legacy_qos="$LOOM_SLURM_QOS|DenyOnLimit|0|1|1|04:00:00||"
  desired_association="$LOOM_SLURM_CLUSTER|$LOOM_SLURM_ACCOUNT|$LOOM_BUILDER_USER|$LOOM_BUILDER_PARTITION|$LOOM_SLURM_QOS|$LOOM_SLURM_QOS|"

  if [[ -z "$LOOM_ACCOUNT_ROW" ]]; then
    LOOM_ACCOUNT_CONVERGED=0
  elif [[ "$LOOM_ACCOUNT_ROW" == "$LOOM_SLURM_ACCOUNT|" ]]; then
    LOOM_ACCOUNT_CONVERGED=1
  else
    loom_builder_slurm_error "Slurm account drift is unsafe"
    return
  fi
  if [[ "$LOOM_QOS_ROW" == "$legacy_qos" ]]; then
    LOOM_QOS_CONVERGED=0
  elif [[ "$LOOM_QOS_ROW" == "$desired_qos" ]]; then
    LOOM_QOS_CONVERGED=1
  else
    loom_builder_slurm_error "Slurm QoS drift is unsafe"
    return
  fi
  if [[ -z "$LOOM_ASSOCIATION_ROW" ]]; then
    LOOM_ASSOCIATION_CONVERGED=0
  elif [[ "$LOOM_ASSOCIATION_ROW" == "$desired_association" ]]; then
    LOOM_ASSOCIATION_CONVERGED=1
  else
    loom_builder_slurm_error "Slurm association drift is unsafe"
    return
  fi
}

loom_builder_slurm_preflight() {
  local cluster_id="$1"
  loom_builder_slurm_load_policy "$cluster_id"
  loom_builder_slurm_validate_controller
  loom_builder_slurm_validate_durable_config
  loom_builder_slurm_validate_live_partitions
  loom_builder_slurm_read_accounting
}

loom_builder_slurm_restore_config() {
  local failure="$1"
  if ! install -o "$LOOM_SLURM_CONFIG_OWNER" -g "$LOOM_SLURM_CONFIG_GROUP" \
    -m "$LOOM_SLURM_CONFIG_MODE" "$LOOM_BACKUP" "$LOOM_SLURM_CONFIG"; then
    loom_builder_slurm_error "$failure; failed to restore backup on disk"
    return
  fi
  if ! scontrol reconfigure; then
    loom_builder_slurm_error \
      "$failure; restored backup on disk, but Slurm rejected the restored backup reconfigure"
    return
  fi
  loom_builder_slurm_error "$failure; restored backup"
}

loom_builder_slurm_add_partition() {
  local temporary
  if ! install -d -o "$LOOM_STATE_OWNER" -g "$LOOM_STATE_GROUP" \
    -m 0755 "$LOOM_STATE_ROOT"; then
    loom_builder_slurm_error "cannot create Slurm authority state directory"
    return
  fi
  if [[ ! -e "$LOOM_BACKUP" && ! -L "$LOOM_BACKUP" ]]; then
    if ! install -o "$LOOM_STATE_OWNER" -g "$LOOM_STATE_GROUP" -m 0600 \
      "$LOOM_SLURM_CONFIG" "$LOOM_BACKUP"; then
      loom_builder_slurm_error "cannot create Slurm configuration backup"
      return
    fi
  fi
  temporary="$(mktemp "$(dirname "$LOOM_SLURM_CONFIG")/.slurm.conf.XXXXXX")"
  if ! awk -v anchor="$LOOM_TRIAL_PARTITION_ANCHOR" \
    -v builder="$LOOM_BUILDER_PARTITION_LINE" \
    '{ print; if ($0 == anchor) print builder }' \
    "$LOOM_SLURM_CONFIG" >"$temporary"; then
    unlink "$temporary"
    loom_builder_slurm_error "cannot render builder partition configuration"
    return
  fi
  if ! chown "$LOOM_SLURM_CONFIG_OWNER:$LOOM_SLURM_CONFIG_GROUP" "$temporary" \
    || ! chmod "$LOOM_SLURM_CONFIG_MODE" "$temporary" \
    || ! mv "$temporary" "$LOOM_SLURM_CONFIG"; then
    unlink "$temporary" 2>/dev/null || true
    loom_builder_slurm_error "cannot install builder partition configuration"
    return
  fi
  if ! scontrol reconfigure; then
    loom_builder_slurm_restore_config "Slurm rejected the builder partition update"
    return
  fi
  LOOM_PARTITION_CONVERGED=1
  if ! loom_builder_slurm_validate_live_partitions; then
    loom_builder_slurm_restore_config "live builder partition readback failed"
    return
  fi
}

loom_builder_slurm_apply_accounting() {
  if [[ "$LOOM_ACCOUNT_CONVERGED" == "0" ]]; then
    if ! sacctmgr --immediate add account "name=$LOOM_SLURM_ACCOUNT" \
      "cluster=$LOOM_SLURM_CLUSTER" \
      description="Loom allocation-scoped task image builders" organization=loom; then
      loom_builder_slurm_error "failed to add the builder Slurm account"
      return
    fi
  fi
  if [[ "$LOOM_QOS_CONVERGED" == "0" ]]; then
    if ! sacctmgr --immediate modify qos where "name=$LOOM_SLURM_QOS" set \
      Flags=DenyOnLimit Priority=0 \
      "MaxJobsPU=$LOOM_BUILDER_MAX_JOBS" \
      "MaxSubmitJobsPU=$LOOM_BUILDER_MAX_SUBMIT" \
      "MaxWall=$LOOM_BUILDER_WALL" \
      "GrpTRES=cpu=$LOOM_BUILDER_CPUS,mem=${LOOM_BUILDER_MEMORY_MIB}M,node=1"; then
      loom_builder_slurm_error "failed to narrow the builder Slurm QoS"
      return
    fi
  fi
  if [[ "$LOOM_ASSOCIATION_CONVERGED" == "0" ]]; then
    if ! sacctmgr --immediate add user "name=$LOOM_BUILDER_USER" \
      "account=$LOOM_SLURM_ACCOUNT" "cluster=$LOOM_SLURM_CLUSTER" \
      "partition=$LOOM_BUILDER_PARTITION" "qos=$LOOM_SLURM_QOS" \
      "defaultqos=$LOOM_SLURM_QOS"; then
      loom_builder_slurm_error "failed to add the bounded builder Slurm association"
      return
    fi
  fi
}

loom_builder_slurm_report() {
  local cluster_id="$1"
  printf '{"blockers":["phase2_guard_provider_release_missing"],"certified_nodes":[],"cluster_id":"%s","production_certification_allowed":false,"state":"prerequisites_converged"}\n' \
    "$cluster_id"
}

loom_builder_slurm_check() {
  local cluster_id="$1"
  loom_builder_slurm_preflight "$cluster_id"
  if [[ "$LOOM_PARTITION_CONVERGED" != "1" \
    || "$LOOM_ACCOUNT_CONVERGED" != "1" \
    || "$LOOM_QOS_CONVERGED" != "1" \
    || "$LOOM_ASSOCIATION_CONVERGED" != "1" ]]; then
    loom_builder_slurm_error "task-image builder Slurm prerequisites are not converged"
    return
  fi
  loom_builder_slurm_report "$cluster_id"
}

loom_builder_slurm_apply() {
  local cluster_id="$1"
  loom_builder_slurm_preflight "$cluster_id"
  if [[ "$LOOM_PARTITION_CONVERGED" == "0" ]]; then
    loom_builder_slurm_add_partition
  fi
  loom_builder_slurm_apply_accounting
  loom_builder_slurm_validate_durable_config
  loom_builder_slurm_validate_live_partitions
  loom_builder_slurm_read_accounting
  if [[ "$LOOM_PARTITION_CONVERGED" != "1" \
    || "$LOOM_ACCOUNT_CONVERGED" != "1" \
    || "$LOOM_QOS_CONVERGED" != "1" \
    || "$LOOM_ASSOCIATION_CONVERGED" != "1" ]]; then
    loom_builder_slurm_error "task-image builder Slurm prerequisite readback did not converge"
    return
  fi
  loom_builder_slurm_report "$cluster_id"
}

loom_builder_slurm_main() {
  if [[ "$#" -ne 2 || ( "$1" != "check" && "$1" != "apply" ) ]]; then
    echo "usage: sudo $0 {check|apply} <cluster-id>" >&2
    return 2
  fi
  if [[ "$LOOM_POLICY_PATH" != "$LOOM_DEFAULT_POLICY_PATH" \
    || "$LOOM_STATE_ROOT" != "$LOOM_DEFAULT_STATE_ROOT" \
    || "$LOOM_STATE_OWNER" != "root" \
    || "$LOOM_STATE_GROUP" != "root" \
    || "$LOOM_CONTROLLER_HOST" != "$(hostname -s)" \
    || "$LOOM_HOST_ARCH" != "$(uname -m)" ]]; then
    loom_builder_slurm_error "test overrides are forbidden in the direct converger CLI"
    return
  fi
  if [[ "$1" == "apply" && "$(id -u)" -ne 0 ]]; then
    loom_builder_slurm_error "Slurm prerequisite convergence requires controller root"
    return
  fi
  "loom_builder_slurm_$1" "$2"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  loom_builder_slurm_main "$@"
fi
