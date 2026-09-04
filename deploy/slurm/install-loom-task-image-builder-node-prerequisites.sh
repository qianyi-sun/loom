#!/usr/bin/env bash
# Install the inert, allocation-scoped provider release prerequisites.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LOOM_DEFAULT_POLICY_PATH="$REPO_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
LOOM_DEFAULT_HOST_RELEASE_MANIFEST="$REPO_ROOT/deploy/task-image-builder/host-release-v2.json"
LOOM_DEFAULT_PROVIDER_INSTALLER="$REPO_ROOT/scripts/ops/install_task_image_builder_provider_release.py"
LOOM_POLICY_PATH="${LOOM_POLICY_PATH:-$LOOM_DEFAULT_POLICY_PATH}"
LOOM_HOST_RELEASE_MANIFEST="${LOOM_HOST_RELEASE_MANIFEST:-$LOOM_DEFAULT_HOST_RELEASE_MANIFEST}"
LOOM_PROVIDER_INSTALLER="${LOOM_PROVIDER_INSTALLER:-$LOOM_DEFAULT_PROVIDER_INSTALLER}"
LOOM_STAGE_ROOT="${LOOM_STAGE_ROOT:-/}"
LOOM_PASSWD_FILE="${LOOM_PASSWD_FILE:-/etc/passwd}"
LOOM_GROUP_FILE="${LOOM_GROUP_FILE:-/etc/group}"
LOOM_SUBUID_FILE="${LOOM_SUBUID_FILE:-/etc/subuid}"
LOOM_SUBGID_FILE="${LOOM_SUBGID_FILE:-/etc/subgid}"
LOOM_HOST_ARCH="${LOOM_HOST_ARCH:-$(uname -m)}"
LOOM_SKIP_HOST_CHECKS="${LOOM_SKIP_HOST_CHECKS:-0}"

loom_node_error() {
  echo "error: $*" >&2
  return 1
}

loom_node_load_policy() {
  local cluster_id="$1"
  local slurm_node="$2"
  local values=()
  local expected_nodes=()
  local expected_node inventory_match=0
  if [[ ! -f "$LOOM_POLICY_PATH" || -L "$LOOM_POLICY_PATH" ]]; then
    loom_node_error "prerequisite policy is unavailable"
    return
  fi
  if [[ ! -f "$LOOM_HOST_RELEASE_MANIFEST" || -L "$LOOM_HOST_RELEASE_MANIFEST" ]]; then
    loom_node_error "host release manifest is unavailable"
    return
  fi
  if [[ ! -f "$LOOM_PROVIDER_INSTALLER" || -L "$LOOM_PROVIDER_INSTALLER" ]]; then
    loom_node_error "provider installer is unavailable"
    return
  fi
  mapfile -t values < <(
    python3 - "$LOOM_POLICY_PATH" "$LOOM_HOST_RELEASE_MANIFEST" "$cluster_id" <<'PY'
import json
import pathlib
import sys
import tomllib

policy_path = pathlib.Path(sys.argv[1])
host_release_path = pathlib.Path(sys.argv[2])
cluster_id = sys.argv[3]
policy = tomllib.loads(policy_path.read_text(encoding="utf-8"))
if policy.get("schema") != "loom.task-image-builder-prerequisites/v1":
    raise SystemExit("invalid policy schema")
if policy.get("production_certification_allowed") is not False:
    raise SystemExit("Phase 2C policy is not inert")
if policy.get("certified_nodes") != []:
    raise SystemExit("Phase 2C policy certifies nodes")
if policy.get("unconditional_blockers") != ["phase2_guard_provider_release_missing"]:
    raise SystemExit("Phase 2C blocker set is invalid")
host_release = json.loads(host_release_path.read_text(encoding="utf-8"))
if host_release.get("schema") != "loom.task-image-builder-host-release/v2":
    raise SystemExit("invalid host release schema")
if host_release.get("release") != "host-release-v2":
    raise SystemExit("invalid host release identity")
if policy.get("host_release_manifest") != host_release_path.name:
    raise SystemExit("policy does not bind the host release manifest")
identity = policy["identity"]
clusters = [item for item in policy["clusters"] if item["id"] == cluster_id]
if len(clusters) != 1:
    raise SystemExit("cluster policy is not unique")
cluster = clusters[0]
values = (
    cluster["architecture"],
    ",".join(cluster["builder_nodes"]),
    identity["user"],
    identity["group"],
    str(identity["uid"]),
    str(identity["gid"]),
    str(identity["subid_start"]),
    str(identity["subid_count"]),
    identity["home"],
    identity["shell"],
    ",".join(identity["forbidden_supplementary_groups"]),
)
for value in values:
    if "\n" in value or "\r" in value:
        raise SystemExit("policy contains an unsafe value")
    print(value)
PY
  ) || {
    loom_node_error "prerequisite policy validation failed"
    return
  }
  if [[ "${#values[@]}" -ne 11 ]]; then
    loom_node_error "prerequisite policy output is incomplete"
    return
  fi
  LOOM_EXPECTED_ARCH="${values[0]}"
  LOOM_EXPECTED_NODES="${values[1]}"
  LOOM_BUILDER_USER="${values[2]}"
  LOOM_BUILDER_GROUP="${values[3]}"
  LOOM_BUILDER_UID="${values[4]}"
  LOOM_BUILDER_GID="${values[5]}"
  LOOM_SUBID_START="${values[6]}"
  LOOM_SUBID_COUNT="${values[7]}"
  LOOM_BUILDER_HOME="${values[8]}"
  LOOM_BUILDER_SHELL="${values[9]}"
  LOOM_FORBIDDEN_GROUPS="${values[10]}"
  if [[ "$LOOM_HOST_ARCH" != "$LOOM_EXPECTED_ARCH" ]]; then
    loom_node_error "host architecture does not match cluster policy"
    return
  fi
  IFS=, read -r -a expected_nodes <<<"$LOOM_EXPECTED_NODES"
  for expected_node in "${expected_nodes[@]}"; do
    if [[ "$expected_node" == "$slurm_node" ]]; then
      inventory_match=1
    fi
  done
  if [[ "$inventory_match" != "1" ]]; then
    loom_node_error "Slurm node is outside the cluster builder inventory"
    return
  fi
  LOOM_SLURM_NODE="$slurm_node"
}

loom_node_verify_slurm_identity() {
  local slurm_node="$1"
  local state field node_name="" node_addr="" node_hostname=""
  local node_name_count=0 node_addr_count=0 node_hostname_count=0
  local resolved local_addresses local_hostnames short_name canonical_name aliases

  if ! state="$(timeout 30 scontrol show node "$slurm_node" -o </dev/null 2>/dev/null)" \
    || [[ -z "$state" || "$state" == *$'\n'* || ${#state} -gt 65536 ]]; then
    loom_node_error "Slurm node identity does not match the local host"
    return
  fi
  for field in $state; do
    case "$field" in
      NodeName=*)
        node_name="${field#NodeName=}"
        node_name_count=$((node_name_count + 1))
        ;;
      NodeAddr=*)
        node_addr="${field#NodeAddr=}"
        node_addr_count=$((node_addr_count + 1))
        ;;
      NodeHostName=*)
        node_hostname="${field#NodeHostName=}"
        node_hostname_count=$((node_hostname_count + 1))
        ;;
    esac
  done
  if [[ "$node_name_count" -ne 1 || "$node_addr_count" -ne 1 \
    || "$node_hostname_count" -ne 1 || -z "$node_name" \
    || -z "$node_addr" || -z "$node_hostname" ]]; then
    loom_node_error "Slurm node identity does not match the local host"
    return
  fi
  if ! resolved="$(getent ahosts "$node_addr" 2>/dev/null)" \
    || ! local_addresses="$(ip -o address show scope global 2>/dev/null)" \
    || ! short_name="$(hostname -s 2>/dev/null)" \
    || ! canonical_name="$(hostname -f 2>/dev/null)" \
    || ! aliases="$(hostname -A 2>/dev/null)"; then
    loom_node_error "Slurm node identity does not match the local host"
    return
  fi
  local_hostnames="$short_name $canonical_name $aliases"
  if [[ ${#resolved} -gt 65536 || ${#local_addresses} -gt 65536 \
    || ${#local_hostnames} -gt 65536 ]]; then
    loom_node_error "Slurm node identity does not match the local host"
    return
  fi
  if ! python3 - "$slurm_node" "$node_name" "$node_hostname" \
    "$resolved" "$local_addresses" "$local_hostnames" <<'PY'
import ipaddress
import sys

expected_name, observed_name, node_hostname, resolved_raw, local_raw, hostnames_raw = (
    sys.argv[1:]
)
if observed_name != expected_name:
    raise SystemExit(1)

try:
    resolved = {
        ipaddress.ip_address(line.split()[0])
        for line in resolved_raw.splitlines()
        if line.split()
    }
    local = {
        ipaddress.ip_interface(fields[3]).ip
        for line in local_raw.splitlines()
        if len(fields := line.split()) >= 4 and fields[2] in {"inet", "inet6"}
    }
except ValueError:
    raise SystemExit(1) from None

hostnames = {item.casefold() for item in hostnames_raw.split() if item}
if not resolved or not resolved.issubset(local):
    raise SystemExit(1)
if node_hostname.casefold() not in hostnames:
    raise SystemExit(1)
PY
  then
    loom_node_error "Slurm node identity does not match the local host"
    return
  fi
}

loom_node_validate_provider_release() {
  local bundle_dir="$1"
  local release_sha
  if [[ ! -d "$bundle_dir" || -L "$bundle_dir" ]]; then
    loom_node_error "provider release directory is unavailable"
    return
  fi
  if ! release_sha="$(python3 - "$bundle_dir" "$LOOM_EXPECTED_ARCH" "$LOOM_PROVIDER_INSTALLER" <<'PY'
import os
import pathlib
import sys

bundle = pathlib.Path(sys.argv[1]).resolve(strict=True)
architecture = sys.argv[2]
installer = pathlib.Path(sys.argv[3]).resolve(strict=True)
repository = installer.parents[2]
sys.path[:0] = [str(repository), str(repository / "src")]

from scripts.ops.task_image_builder_provider_release import verify_release_directory

verify_release_directory(
    bundle,
    expected_release_sha256=bundle.name,
    expected_architecture=architecture,
    expected_uid=os.geteuid(),
)
print(bundle.name)
PY
  )"; then
    loom_node_error "provider release verification failed"
    return
  fi
  if [[ "$release_sha" != "${bundle_dir##*/}" ]]; then
    loom_node_error "provider release identity is inconsistent"
    return
  fi
  LOOM_PROVIDER_RELEASE_SHA256="$release_sha"
}

loom_node_provider_stage_args() {
  local args=(
    "$LOOM_PROVIDER_INSTALLER"
    "--bundle" "$1"
    "--release-sha256" "$LOOM_PROVIDER_RELEASE_SHA256"
    "--architecture" "$LOOM_EXPECTED_ARCH"
    "--root" "$LOOM_STAGE_ROOT"
  )
  if [[ "$LOOM_STAGE_ROOT" == "/" ]]; then
    args+=("--live")
  fi
  printf '%s\n' "${args[@]}"
}

loom_node_verify_staged_provider_release() {
  local bundle_dir="$1"
  mapfile -t stage_args < <(loom_node_provider_stage_args "$bundle_dir")
  if ! python3 "${stage_args[@]}" --verify-staged >/dev/null; then
    loom_node_error "staged provider release is missing"
    return
  fi
}

loom_node_stage_provider_release() {
  local bundle_dir="$1"
  mapfile -t stage_args < <(loom_node_provider_stage_args "$bundle_dir")
  if ! python3 "${stage_args[@]}" >/dev/null; then
    loom_node_error "provider release staging failed"
    return
  fi
}

loom_node_validate_subid_file() {
  local file="$1"
  local label="$2"
  if [[ ! -f "$file" || -L "$file" ]]; then
    loom_node_error "$label database is unavailable"
    return
  fi
  awk -F: -v user="$LOOM_BUILDER_USER" -v start="$LOOM_SUBID_START" \
    -v count="$LOOM_SUBID_COUNT" -v label="$label" '
      NF != 0 && NF != 3 { print "error: " label " database is malformed" > "/dev/stderr"; exit 1 }
      NF == 3 {
        row_start = $2 + 0
        row_end = row_start + $3 - 1
        wanted_end = start + count - 1
        if ($1 == user) {
          seen += 1
          if ($2 != start || $3 != count) {
            print "error: " label " mapping conflict" > "/dev/stderr"
            exit 1
          }
        } else if (row_start <= wanted_end && row_end >= start) {
          print "error: " label " range conflict" > "/dev/stderr"
          exit 1
        }
      }
      END {
        if (seen > 1) {
          print "error: duplicate " label " mapping conflict" > "/dev/stderr"
          exit 1
        }
      }
    ' "$file"
}

loom_node_identity_preflight() {
  local group_name group_members group_row passwd_row unused_gid unused_password
  passwd_row="$(awk -F: -v user="$LOOM_BUILDER_USER" '$1 == user {print}' "$LOOM_PASSWD_FILE")"
  if awk -F: -v user="$LOOM_BUILDER_USER" -v uid="$LOOM_BUILDER_UID" \
    '$1 != user && $3 == uid {found=1} END {exit !found}' "$LOOM_PASSWD_FILE"; then
    loom_node_error "builder UID conflict"
    return
  fi
  if [[ -n "$passwd_row" && "$passwd_row" != \
    "$LOOM_BUILDER_USER:x:$LOOM_BUILDER_UID:$LOOM_BUILDER_GID::$LOOM_BUILDER_HOME:$LOOM_BUILDER_SHELL" ]]; then
    loom_node_error "builder identity conflict"
    return
  fi
  group_row="$(awk -F: -v group="$LOOM_BUILDER_GROUP" '$1 == group {print}' "$LOOM_GROUP_FILE")"
  if awk -F: -v group="$LOOM_BUILDER_GROUP" -v gid="$LOOM_BUILDER_GID" \
    '$1 != group && $3 == gid {found=1} END {exit !found}' "$LOOM_GROUP_FILE"; then
    loom_node_error "builder GID conflict"
    return
  fi
  if [[ -n "$group_row" && "$group_row" != "$LOOM_BUILDER_GROUP:x:$LOOM_BUILDER_GID:" ]]; then
    loom_node_error "builder group conflict"
    return
  fi
  while IFS=: read -r group_name unused_password unused_gid group_members; do
    if [[ "$group_name" != "$LOOM_BUILDER_GROUP" \
      && ",$group_members," == *",$LOOM_BUILDER_USER,"* ]]; then
      loom_node_error "builder has forbidden supplementary group membership"
      return
    fi
  done <"$LOOM_GROUP_FILE"
  loom_node_validate_subid_file "$LOOM_SUBUID_FILE" "subuid"
  loom_node_validate_subid_file "$LOOM_SUBGID_FILE" "subgid"
}

loom_node_identity_complete() {
  grep -qxF "$LOOM_BUILDER_USER:x:$LOOM_BUILDER_UID:$LOOM_BUILDER_GID::$LOOM_BUILDER_HOME:$LOOM_BUILDER_SHELL" \
    "$LOOM_PASSWD_FILE" \
    && grep -qxF "$LOOM_BUILDER_GROUP:x:$LOOM_BUILDER_GID:" "$LOOM_GROUP_FILE" \
    && grep -qxF "$LOOM_BUILDER_USER:$LOOM_SUBID_START:$LOOM_SUBID_COUNT" "$LOOM_SUBUID_FILE" \
    && grep -qxF "$LOOM_BUILDER_USER:$LOOM_SUBID_START:$LOOM_SUBID_COUNT" "$LOOM_SUBGID_FILE"
}

loom_node_append_subid() {
  local file="$1"
  local directory temporary
  if grep -q "^$LOOM_BUILDER_USER:" "$file"; then
    return
  fi
  directory="$(dirname "$file")"
  temporary="$(mktemp "$directory/.loom-subid.XXXXXX")"
  {
    cat "$file"
    printf '%s:%s:%s\n' "$LOOM_BUILDER_USER" "$LOOM_SUBID_START" "$LOOM_SUBID_COUNT"
  } >"$temporary"
  chmod --reference="$file" "$temporary"
  chown --reference="$file" "$temporary"
  mv "$temporary" "$file"
}

loom_node_apply_identity() {
  if ! grep -q "^$LOOM_BUILDER_GROUP:" "$LOOM_GROUP_FILE"; then
    groupadd --system --gid "$LOOM_BUILDER_GID" "$LOOM_BUILDER_GROUP"
  fi
  if ! grep -q "^$LOOM_BUILDER_USER:" "$LOOM_PASSWD_FILE"; then
    useradd --system --uid "$LOOM_BUILDER_UID" --gid "$LOOM_BUILDER_GROUP" \
      --home-dir "$LOOM_BUILDER_HOME" --shell "$LOOM_BUILDER_SHELL" \
      --no-create-home "$LOOM_BUILDER_USER"
  fi
  loom_node_append_subid "$LOOM_SUBUID_FILE"
  loom_node_append_subid "$LOOM_SUBGID_FILE"
  loom_node_identity_preflight
  if ! loom_node_identity_complete; then
    loom_node_error "builder identity did not converge"
    return
  fi
}

loom_node_host_checks() {
  local helper mode options
  if [[ "$LOOM_SKIP_HOST_CHECKS" == "1" ]]; then
    return
  fi
  for helper in newuidmap newgidmap; do
    local path
    path="$(command -v "$helper" || true)"
    if [[ -z "$path" ]]; then
      loom_node_error "$helper is unavailable"
      return
    fi
    mode="$(stat -c '%u:%a' "$path")"
    if [[ "$mode" != 0:4??? && "$mode" != 0:6??? ]]; then
      loom_node_error "$helper is not setuid root"
      return
    fi
  done
  if [[ "$(stat -fc '%T' /sys/fs/cgroup)" != "cgroup2fs" ]]; then
    loom_node_error "cgroup v2 is unavailable"
    return
  fi
  for helper in cpu cpuset io memory pids; do
    if [[ " $(< /sys/fs/cgroup/cgroup.controllers) " != *" $helper "* ]]; then
      loom_node_error "required cgroup controller is unavailable"
      return
    fi
  done
  if [[ "$(sysctl -n kernel.unprivileged_userns_clone 2>/dev/null || true)" != "1" ]]; then
    loom_node_error "unprivileged user namespaces are unavailable"
    return
  fi
  if ! findmnt -n -o FSTYPE,OPTIONS /sys/fs/bpf \
    | grep -Eq '^bpf[[:space:]].*(mode=700|mode=0700)'; then
    loom_node_error "root-only bpffs is unavailable"
    return
  fi
  options="$(findmnt -n -T /var/lib/loom-task-builder -o FSTYPE,OPTIONS 2>/dev/null || true)"
  if [[ "$options" != ext4* || "$options" != *prjquota* ]]; then
    loom_node_error "project-quota builder storage is unavailable"
    return
  fi
  for option in ConstrainCores ConstrainRAMSpace ConstrainSwapSpace ConstrainDevices; do
    if ! grep -Eq "^$option[[:space:]]*=[[:space:]]*yes$" /etc/slurm/cgroup.conf; then
      loom_node_error "Slurm cgroup constraint $option is disabled"
      return
    fi
  done
}

loom_node_check() {
  local cluster_id="$1"
  local slurm_node="$2"
  local bundle_dir="$3"
  loom_node_load_policy "$cluster_id" "$slurm_node"
  loom_node_verify_slurm_identity "$slurm_node"
  loom_node_validate_provider_release "$bundle_dir"
  loom_node_identity_preflight
  if ! loom_node_identity_complete; then
    loom_node_error "builder identity prerequisites are incomplete"
    return
  fi
  loom_node_verify_staged_provider_release "$bundle_dir"
  loom_node_host_checks
  printf '{"certified_nodes":[],"cluster_id":"%s","production_certification_allowed":false,"state":"prepared"}\n' \
    "$cluster_id"
}

loom_node_apply() {
  local cluster_id="$1"
  local slurm_node="$2"
  local bundle_dir="$3"
  loom_node_load_policy "$cluster_id" "$slurm_node"
  loom_node_verify_slurm_identity "$slurm_node"
  loom_node_validate_provider_release "$bundle_dir"
  loom_node_identity_preflight
  loom_node_host_checks
  loom_node_apply_identity
  loom_node_stage_provider_release "$bundle_dir"
  loom_node_check "$cluster_id" "$slurm_node" "$bundle_dir"
}

loom_node_main() {
  if [[ "$#" -ne 4 || ( "$1" != "check" && "$1" != "apply" ) ]]; then
    echo "usage: sudo $0 {check|apply} <cluster-id> <slurm-node-name> <offline-provider-release-directory>" >&2
    return 2
  fi
  if [[ "$LOOM_POLICY_PATH" != "$LOOM_DEFAULT_POLICY_PATH" \
    || "$LOOM_HOST_RELEASE_MANIFEST" != "$LOOM_DEFAULT_HOST_RELEASE_MANIFEST" \
    || "$LOOM_PROVIDER_INSTALLER" != "$LOOM_DEFAULT_PROVIDER_INSTALLER" \
    || "$LOOM_STAGE_ROOT" != "/" \
    || "$LOOM_PASSWD_FILE" != "/etc/passwd" \
    || "$LOOM_GROUP_FILE" != "/etc/group" \
    || "$LOOM_SUBUID_FILE" != "/etc/subuid" \
    || "$LOOM_SUBGID_FILE" != "/etc/subgid" \
    || "$LOOM_HOST_ARCH" != "$(uname -m)" \
    || "$LOOM_SKIP_HOST_CHECKS" != "0" ]]; then
    loom_node_error "test overrides are forbidden in the direct installer CLI"
    return
  fi
  if [[ "$(id -u)" -ne 0 ]]; then
    loom_node_error "node prerequisite staging requires root"
    return
  fi
  "loom_node_$1" "$2" "$3" "$4"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  loom_node_main "$@"
fi
