#!/usr/bin/env bash
# Install only the inert task-image-builder submission identity on a Slurm controller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LOOM_DEFAULT_POLICY_PATH="$REPO_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
LOOM_POLICY_PATH="${LOOM_POLICY_PATH:-$LOOM_DEFAULT_POLICY_PATH}"
LOOM_PASSWD_FILE="${LOOM_PASSWD_FILE:-/etc/passwd}"
LOOM_GROUP_FILE="${LOOM_GROUP_FILE:-/etc/group}"
LOOM_CONTROLLER_HOST="${LOOM_CONTROLLER_HOST:-$(hostname -s)}"
LOOM_HOST_ARCH="${LOOM_HOST_ARCH:-$(uname -m)}"

loom_controller_identity_error() {
  echo "error: $*" >&2
  return 1
}

loom_controller_identity_load_policy() {
  local cluster_id="$1"
  local output
  local values=()

  if [[ ! -f "$LOOM_POLICY_PATH" || -L "$LOOM_POLICY_PATH" ]]; then
    loom_controller_identity_error "prerequisite policy is unavailable"
    return
  fi
  if ! output="$(python3 - "$LOOM_POLICY_PATH" "$cluster_id" <<'PY'
import pathlib
import re
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
if policy.get("unconditional_blockers") != ["phase2_guard_provider_release_missing"]:
    raise SystemExit("Phase 1 blocker is not exact")

identity = policy.get("identity")
if identity != {
    "user": "loom-builder",
    "group": "loom-task-builder",
    "uid": 993,
    "gid": 980,
    "subid_start": 3000000,
    "subid_count": 65536,
    "home": "/nonexistent",
    "shell": "/usr/sbin/nologin",
    "forbidden_supplementary_groups": ["docker", "root", "sudo"],
}:
    raise SystemExit("builder identity policy is not exact")
clusters = [item for item in policy.get("clusters", []) if item.get("id") == cluster_id]
if len(clusters) != 1:
    raise SystemExit("cluster policy is not unique")
cluster = clusters[0]
values = (
    cluster.get("slurm_cluster"),
    cluster.get("architecture"),
    cluster.get("controller"),
    identity["user"],
    identity["group"],
    str(identity["uid"]),
    str(identity["gid"]),
    identity["home"],
    identity["shell"],
)
for value in values:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[A-Za-z0-9._/-]+", value) is None
        or "\n" in value
        or "\r" in value
    ):
        raise SystemExit("policy contains an unsafe controller identity value")
    print(value)
PY
  )"; then
    loom_controller_identity_error "prerequisite policy validation failed"
    return
  fi
  mapfile -t values <<<"$output"
  if [[ "${#values[@]}" -ne 9 ]]; then
    loom_controller_identity_error "prerequisite policy output is incomplete"
    return
  fi

  LOOM_SLURM_CLUSTER="${values[0]}"
  LOOM_EXPECTED_ARCH="${values[1]}"
  LOOM_EXPECTED_CONTROLLER="${values[2]}"
  LOOM_BUILDER_USER="${values[3]}"
  LOOM_BUILDER_GROUP="${values[4]}"
  LOOM_BUILDER_UID="${values[5]}"
  LOOM_BUILDER_GID="${values[6]}"
  LOOM_BUILDER_HOME="${values[7]}"
  LOOM_BUILDER_SHELL="${values[8]}"

  if [[ "$LOOM_HOST_ARCH" != "$LOOM_EXPECTED_ARCH" ]]; then
    loom_controller_identity_error "controller architecture does not match cluster policy"
    return
  fi
  if [[ "$LOOM_CONTROLLER_HOST" != "$LOOM_EXPECTED_CONTROLLER" ]]; then
    loom_controller_identity_error "controller hostname does not match cluster policy"
    return
  fi
}

loom_controller_identity_validate_slurm() {
  local live_config
  if ! live_config="$(timeout 30 scontrol show config </dev/null)" \
    || [[ -z "$live_config" || ${#live_config} -gt 1048576 ]]; then
    loom_controller_identity_error "Slurm controller readback is unavailable"
    return
  fi
  if ! grep -Eq "^ClusterName[[:space:]]*=[[:space:]]*$LOOM_SLURM_CLUSTER$" \
    <<<"$live_config"; then
    loom_controller_identity_error "Slurm controller readback does not match cluster policy"
    return
  fi
  if ! grep -Eq \
    "^SlurmctldHost\[0\][[:space:]]*=[[:space:]]*$LOOM_EXPECTED_CONTROLLER(\(|$)" \
    <<<"$live_config"; then
    loom_controller_identity_error "Slurm controller readback does not match cluster policy"
    return
  fi
}

loom_controller_identity_optional_getent() {
  local database="$1"
  local key="$2"
  local result
  local status

  if result="$(getent "$database" "$key")"; then
    if [[ -z "$result" || "$result" == *$'\n'* || ${#result} -gt 65536 ]]; then
      loom_controller_identity_error "builder identity lookup is ambiguous"
      return 1
    fi
    printf '%s' "$result"
    return 0
  else
    status=$?
  fi
  if [[ "$status" -ne 2 ]]; then
    loom_controller_identity_error "builder identity lookup failed"
    return 1
  fi
  return 0
}

loom_controller_identity_preflight() {
  local passwd_by_name passwd_by_id group_by_name group_by_id supplementary
  local passwd_name _passwd uid gid _gecos home shell passwd_extra
  local group_name _group_password group_gid members group_extra
  local group_scan_status

  if [[ ! -f "$LOOM_PASSWD_FILE" || -L "$LOOM_PASSWD_FILE" \
    || ! -f "$LOOM_GROUP_FILE" || -L "$LOOM_GROUP_FILE" ]]; then
    loom_controller_identity_error "local identity databases are unavailable"
    return
  fi
  if awk -F: -v user="$LOOM_BUILDER_USER" '
      NF >= 4 {
        count = split($4, members, ",")
        for (member_index = 1; member_index <= count; member_index += 1) {
          if (members[member_index] == user) found = 1
        }
      }
      END { exit !found }
    ' "$LOOM_GROUP_FILE"; then
    loom_controller_identity_error "builder has forbidden supplementary group membership"
    return
  else
    group_scan_status=$?
    if [[ "$group_scan_status" -ne 1 ]]; then
      loom_controller_identity_error "local group membership scan failed"
      return
    fi
  fi

  passwd_by_name="$(loom_controller_identity_optional_getent passwd "$LOOM_BUILDER_USER")"
  passwd_by_id="$(loom_controller_identity_optional_getent passwd "$LOOM_BUILDER_UID")"
  group_by_name="$(loom_controller_identity_optional_getent group "$LOOM_BUILDER_GROUP")"
  group_by_id="$(loom_controller_identity_optional_getent group "$LOOM_BUILDER_GID")"

  LOOM_CONTROLLER_USER_PRESENT=0
  if [[ -n "$passwd_by_name" || -n "$passwd_by_id" ]]; then
    if [[ -z "$passwd_by_name" || "$passwd_by_name" != "$passwd_by_id" ]]; then
      loom_controller_identity_error "builder user name or UID conflict"
      return
    fi
    IFS=: read -r passwd_name _passwd uid gid _gecos home shell passwd_extra \
      <<<"$passwd_by_name"
    if [[ "$passwd_name" != "$LOOM_BUILDER_USER" \
      || "$uid" != "$LOOM_BUILDER_UID" \
      || "$gid" != "$LOOM_BUILDER_GID" \
      || "$home" != "$LOOM_BUILDER_HOME" \
      || "$shell" != "$LOOM_BUILDER_SHELL" \
      || -n "$passwd_extra" ]]; then
      loom_controller_identity_error "builder user name or UID conflict"
      return
    fi
    if ! supplementary="$(id -G "$LOOM_BUILDER_USER")" \
      || [[ "$supplementary" != "$LOOM_BUILDER_GID" ]]; then
      loom_controller_identity_error "builder has forbidden supplementary group membership"
      return
    fi
    LOOM_CONTROLLER_USER_PRESENT=1
  fi

  LOOM_CONTROLLER_GROUP_PRESENT=0
  if [[ -n "$group_by_name" || -n "$group_by_id" ]]; then
    if [[ -z "$group_by_name" || "$group_by_name" != "$group_by_id" ]]; then
      loom_controller_identity_error "builder group name or GID conflict"
      return
    fi
    IFS=: read -r group_name _group_password group_gid members group_extra \
      <<<"$group_by_name"
    if [[ "$group_name" != "$LOOM_BUILDER_GROUP" \
      || "$group_gid" != "$LOOM_BUILDER_GID" \
      || -n "$members" \
      || -n "$group_extra" ]]; then
      loom_controller_identity_error "builder group name or GID conflict"
      return
    fi
    LOOM_CONTROLLER_GROUP_PRESENT=1
  fi
}

loom_controller_identity_preflight_all() {
  local cluster_id="$1"
  loom_controller_identity_load_policy "$cluster_id"
  loom_controller_identity_validate_slurm
  loom_controller_identity_preflight
}

loom_controller_identity_report() {
  printf '%s\n' \
    '{"certified_nodes":[],"production_certification_allowed":false,"state":"controller_identity_prepared"}'
}

loom_controller_identity_check() {
  local cluster_id="$1"
  loom_controller_identity_preflight_all "$cluster_id"
  if [[ "$LOOM_CONTROLLER_USER_PRESENT" != "1" \
    || "$LOOM_CONTROLLER_GROUP_PRESENT" != "1" ]]; then
    loom_controller_identity_error "controller builder identity is incomplete"
    return
  fi
  loom_controller_identity_report
}

loom_controller_identity_apply() {
  local cluster_id="$1"
  loom_controller_identity_preflight_all "$cluster_id"
  if [[ "$LOOM_CONTROLLER_GROUP_PRESENT" != "1" ]]; then
    groupadd --system --gid "$LOOM_BUILDER_GID" "$LOOM_BUILDER_GROUP"
  fi
  if [[ "$LOOM_CONTROLLER_USER_PRESENT" != "1" ]]; then
    useradd --system --uid "$LOOM_BUILDER_UID" --gid "$LOOM_BUILDER_GROUP" \
      --home-dir "$LOOM_BUILDER_HOME" --shell "$LOOM_BUILDER_SHELL" \
      --no-create-home "$LOOM_BUILDER_USER"
  fi
  loom_controller_identity_preflight
  if [[ "$LOOM_CONTROLLER_USER_PRESENT" != "1" \
    || "$LOOM_CONTROLLER_GROUP_PRESENT" != "1" ]]; then
    loom_controller_identity_error "controller builder identity did not converge"
    return
  fi
  loom_controller_identity_report
}

loom_controller_identity_main() {
  if [[ "$#" -ne 2 || ( "$1" != "check" && "$1" != "apply" ) ]]; then
    echo "usage: sudo $0 {check|apply} <cluster-id>" >&2
    return 2
  fi
  if [[ "$1" == "apply" && "$(id -u)" -ne 0 ]]; then
    loom_controller_identity_error "controller identity installation requires root"
    return
  fi
  if [[ "$LOOM_POLICY_PATH" != "$LOOM_DEFAULT_POLICY_PATH" \
    || "$LOOM_PASSWD_FILE" != "/etc/passwd" \
    || "$LOOM_GROUP_FILE" != "/etc/group" \
    || "$LOOM_CONTROLLER_HOST" != "$(hostname -s)" \
    || "$LOOM_HOST_ARCH" != "$(uname -m)" ]]; then
    loom_controller_identity_error "test overrides are forbidden in the direct installer CLI"
    return
  fi
  "loom_controller_identity_$1" "$2"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  loom_controller_identity_main "$@"
fi
