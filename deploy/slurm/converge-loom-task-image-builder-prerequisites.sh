#!/usr/bin/env bash
# Converge the inert Phase 1 Slurm prerequisites for dynamic task-image builders.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LOOM_DEFAULT_POLICY_PATH="$REPO_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
LOOM_DEFAULT_STATE_ROOT="/var/lib/loom-task-builder/slurm-authority"
LOOM_SLURM_READBACK="$REPO_ROOT/scripts/ops/task_image_builder_slurm_readback.py"
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
legacy = policy["legacy_guard"]

if (
    identity.get("user") != "loom-builder"
    or identity.get("uid") != 993
    or identity.get("group") != "loom-task-builder"
    or identity.get("gid") != 980
    or identity.get("home") != "/nonexistent"
    or identity.get("shell") != "/usr/sbin/nologin"
):
    raise SystemExit("builder identity is not exact")
if legacy != {
    "qos": "loom-task-image-builder",
    "reservation": "loom-task-image-builder",
    "account": "loom-staging",
    "user": "loom-rollout",
    "max_jobs_per_user": 1,
    "max_submit_jobs_per_user": 1,
    "max_wall": "04:00:00",
}:
    raise SystemExit("legacy builder guard is not exact")
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
    or not isinstance(cluster.get("slurm_qos"), str)
    or not cluster["slurm_qos"]
):
    raise SystemExit("builder Slurm identity is not exact")
if cluster["slurm_qos"] == legacy["qos"]:
    raise SystemExit("rootless QoS collides with legacy")
for key in (
    "legacy_base_qos",
    "legacy_reservation_node",
    "legacy_reservation_partition",
):
    if not isinstance(cluster.get(key), str) or not cluster[key]:
        raise SystemExit("legacy cluster guard is not exact")

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
    identity["group"],
    str(identity["uid"]),
    str(identity["gid"]),
    identity["home"],
    identity["shell"],
    str(resources["cpus"]),
    str(resources["memory_mib"]),
    resources["wall_time"],
    str(resources["max_jobs_per_user"]),
    str(resources["max_submit_jobs_per_user"]),
    legacy["qos"],
    legacy["reservation"],
    legacy["account"],
    legacy["user"],
    str(legacy["max_jobs_per_user"]),
    str(legacy["max_submit_jobs_per_user"]),
    legacy["max_wall"],
    cluster["legacy_base_qos"],
    cluster["legacy_reservation_node"],
    cluster["legacy_reservation_partition"],
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
  if [[ "${#values[@]}" -ne 38 ]]; then
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
  LOOM_BUILDER_GROUP="${values[18]}"
  LOOM_BUILDER_UID="${values[19]}"
  LOOM_BUILDER_GID="${values[20]}"
  LOOM_BUILDER_HOME="${values[21]}"
  LOOM_BUILDER_SHELL="${values[22]}"
  LOOM_BUILDER_CPUS="${values[23]}"
  LOOM_BUILDER_MEMORY_MIB="${values[24]}"
  LOOM_BUILDER_WALL="${values[25]}"
  LOOM_BUILDER_MAX_JOBS="${values[26]}"
  LOOM_BUILDER_MAX_SUBMIT="${values[27]}"
  LOOM_LEGACY_QOS="${values[28]}"
  LOOM_LEGACY_RESERVATION="${values[29]}"
  LOOM_LEGACY_ACCOUNT="${values[30]}"
  LOOM_LEGACY_USER="${values[31]}"
  LOOM_LEGACY_MAX_JOBS="${values[32]}"
  LOOM_LEGACY_MAX_SUBMIT="${values[33]}"
  LOOM_LEGACY_WALL="${values[34]}"
  LOOM_LEGACY_BASE_QOS="${values[35]}"
  LOOM_LEGACY_RESERVATION_NODE="${values[36]}"
  LOOM_LEGACY_RESERVATION_PARTITION="${values[37]}"
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

loom_builder_slurm_validate_controller_identity() {
  local passwd_by_name passwd_by_id group_by_name group_by_id supplementary
  local passwd_name _passwd uid gid _gecos home shell passwd_extra
  local group_name _group_password group_gid _members group_extra

  if ! passwd_by_name="$(getent passwd "$LOOM_BUILDER_USER")" \
    || ! passwd_by_id="$(getent passwd "$LOOM_BUILDER_UID")" \
    || ! group_by_name="$(getent group "$LOOM_BUILDER_GROUP")" \
    || ! group_by_id="$(getent group "$LOOM_BUILDER_GID")" \
    || ! supplementary="$(id -G "$LOOM_BUILDER_USER")"; then
    loom_builder_slurm_error "controller builder identity is unavailable or unsafe"
    return
  fi
  if [[ -z "$passwd_by_name" || "$passwd_by_name" == *$'\n'* \
    || "$passwd_by_name" != "$passwd_by_id" \
    || -z "$group_by_name" || "$group_by_name" == *$'\n'* \
    || "$group_by_name" != "$group_by_id" ]]; then
    loom_builder_slurm_error "controller builder identity is unavailable or unsafe"
    return
  fi
  IFS=: read -r passwd_name _passwd uid gid _gecos home shell passwd_extra \
    <<<"$passwd_by_name"
  IFS=: read -r group_name _group_password group_gid _members group_extra \
    <<<"$group_by_name"
  if [[ "$passwd_name" != "$LOOM_BUILDER_USER" \
    || "$uid" != "$LOOM_BUILDER_UID" \
    || "$gid" != "$LOOM_BUILDER_GID" \
    || "$home" != "$LOOM_BUILDER_HOME" \
    || "$shell" != "$LOOM_BUILDER_SHELL" \
    || -n "$passwd_extra" \
    || "$group_name" != "$LOOM_BUILDER_GROUP" \
    || "$group_gid" != "$LOOM_BUILDER_GID" \
    || -n "$group_extra" \
    || "$supplementary" != "$LOOM_BUILDER_GID" ]]; then
    loom_builder_slurm_error "controller builder identity is unavailable or unsafe"
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

loom_builder_slurm_read_legacy_fingerprint() {
  local qos_raw association_raw reservation_raw
  local qos_json association_json reservation_json checksum fingerprint remainder

  if ! qos_raw="$(timeout 30 sacctmgr --noheader --parsable2 \
    show qos where "name=$LOOM_LEGACY_QOS" \
    format=Name,Flags,Priority,MaxJobsPU,MaxSubmitJobsPU,MaxWall,GrpTRES \
    </dev/null)" \
    || ! association_raw="$(timeout 30 sacctmgr --noheader --parsable2 \
      show association where "cluster=$LOOM_SLURM_CLUSTER" \
      "account=$LOOM_LEGACY_ACCOUNT" "user=$LOOM_LEGACY_USER" \
      format=Cluster,Account,User,QOS,DefaultQOS </dev/null)" \
    || ! reservation_raw="$(timeout 30 scontrol show reservation \
      "$LOOM_LEGACY_RESERVATION" -o </dev/null)"; then
    loom_builder_slurm_error "legacy builder readback is unavailable"
    return
  fi
  if ! qos_json="$(printf '%s' "$qos_raw" | python3 "$LOOM_SLURM_READBACK" qos \
    --name "$LOOM_LEGACY_QOS" --flags DenyOnLimit --priority 0 \
    --max-jobs "$LOOM_LEGACY_MAX_JOBS" \
    --max-submit "$LOOM_LEGACY_MAX_SUBMIT" --max-wall "$LOOM_LEGACY_WALL" \
    --group-tres '')" \
    || ! association_json="$(printf '%s' "$association_raw" \
      | python3 "$LOOM_SLURM_READBACK" association \
        --cluster "$LOOM_SLURM_CLUSTER" --account "$LOOM_LEGACY_ACCOUNT" \
        --user "$LOOM_LEGACY_USER" \
        --qos "$LOOM_LEGACY_BASE_QOS,$LOOM_LEGACY_QOS" \
        --default-qos "$LOOM_LEGACY_BASE_QOS")" \
    || ! reservation_json="$(printf '%s' "$reservation_raw" \
      | python3 "$LOOM_SLURM_READBACK" reservation \
        --name "$LOOM_LEGACY_RESERVATION" \
        --node "$LOOM_LEGACY_RESERVATION_NODE" --node-count 1 \
        --partition "$LOOM_LEGACY_RESERVATION_PARTITION" \
        --users "$LOOM_LEGACY_USER" --accounts "$LOOM_LEGACY_ACCOUNT" \
        --state ACTIVE --flags IGNORE_JOBS,SPEC_NODES)"; then
    loom_builder_slurm_error "legacy builder readback is invalid"
    return
  fi
  if ! checksum="$(
    printf '%s\n%s\n%s\n' "$qos_json" "$association_json" "$reservation_json" \
      | sha256sum
  )"; then
    loom_builder_slurm_error "legacy builder fingerprint is unavailable"
    return
  fi
  read -r fingerprint remainder <<<"$checksum"
  if [[ ! "$fingerprint" =~ ^[0-9a-f]{64}$ || -z "$remainder" ]]; then
    loom_builder_slurm_error "legacy builder fingerprint is unavailable"
    return
  fi
  printf '%s\n' "$fingerprint"
}

loom_builder_slurm_capture_legacy_fingerprint() {
  if ! LOOM_LEGACY_FINGERPRINT="$(loom_builder_slurm_read_legacy_fingerprint)"; then
    return 1
  fi
}

loom_builder_slurm_verify_legacy_fingerprint() {
  local current
  if ! current="$(loom_builder_slurm_read_legacy_fingerprint)"; then
    return 1
  fi
  if [[ "$current" != "$LOOM_LEGACY_FINGERPRINT" ]]; then
    loom_builder_slurm_error "legacy builder fingerprint changed during convergence"
    return
  fi
}

loom_builder_slurm_read_accounting() {
  local account_json qos_json association_json
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

  if ! account_json="$(printf '%s' "$LOOM_ACCOUNT_ROW" \
    | python3 "$LOOM_SLURM_READBACK" account --name "$LOOM_SLURM_ACCOUNT" \
      --allow-absent)"; then
    loom_builder_slurm_error "rootless account readback drift is unsafe"
    return
  fi
  if ! qos_json="$(printf '%s' "$LOOM_QOS_ROW" \
    | python3 "$LOOM_SLURM_READBACK" qos --name "$LOOM_SLURM_QOS" \
      --flags DenyOnLimit --priority 0 --max-jobs "$LOOM_BUILDER_MAX_JOBS" \
      --max-submit "$LOOM_BUILDER_MAX_SUBMIT" --max-wall "$LOOM_BUILDER_WALL" \
      --group-tres "cpu=$LOOM_BUILDER_CPUS,mem=${LOOM_BUILDER_MEMORY_MIB}M,node=1" \
      --allow-absent)"; then
    loom_builder_slurm_error "rootless QoS readback drift is unsafe"
    return
  fi
  if ! association_json="$(printf '%s' "$LOOM_ASSOCIATION_ROW" \
    | python3 "$LOOM_SLURM_READBACK" association \
      --cluster "$LOOM_SLURM_CLUSTER" --account "$LOOM_SLURM_ACCOUNT" \
      --user "$LOOM_BUILDER_USER" --partition "$LOOM_BUILDER_PARTITION" \
      --qos "$LOOM_SLURM_QOS" --default-qos "$LOOM_SLURM_QOS" \
      --allow-absent)"; then
    loom_builder_slurm_error "rootless association readback drift is unsafe"
    return
  fi

  if [[ "$account_json" == "null" ]]; then
    LOOM_ACCOUNT_CONVERGED=0
  else
    LOOM_ACCOUNT_CONVERGED=1
  fi
  if [[ "$qos_json" == "null" ]]; then
    LOOM_QOS_CONVERGED=0
  else
    LOOM_QOS_CONVERGED=1
  fi
  if [[ "$association_json" == "null" ]]; then
    LOOM_ASSOCIATION_CONVERGED=0
  else
    LOOM_ASSOCIATION_CONVERGED=1
  fi
}

loom_builder_slurm_preflight() {
  local cluster_id="$1"
  loom_builder_slurm_load_policy "$cluster_id"
  loom_builder_slurm_validate_controller
  loom_builder_slurm_validate_controller_identity
  loom_builder_slurm_validate_durable_config
  loom_builder_slurm_validate_live_partitions
  loom_builder_slurm_capture_legacy_fingerprint
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
    if ! sacctmgr --immediate add qos "name=$LOOM_SLURM_QOS" \
      flags=DenyOnLimit Priority=0 \
      "MaxJobsPU=$LOOM_BUILDER_MAX_JOBS" \
      "MaxSubmitJobsPU=$LOOM_BUILDER_MAX_SUBMIT" \
      "MaxWall=$LOOM_BUILDER_WALL" \
      "GrpTRES=cpu=$LOOM_BUILDER_CPUS,mem=${LOOM_BUILDER_MEMORY_MIB}M,node=1"; then
      loom_builder_slurm_error "failed to add the rootless builder Slurm QoS"
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
  loom_builder_slurm_verify_legacy_fingerprint
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
