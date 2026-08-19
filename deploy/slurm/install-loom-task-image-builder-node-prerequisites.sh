#!/usr/bin/env bash
# Install the inert, allocation-scoped rootless builder node prerequisites.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LOOM_DEFAULT_POLICY_PATH="$REPO_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
LOOM_DEFAULT_RUNTIME_MANIFEST="$REPO_ROOT/deploy/task-image-builder/rootless-runtime-v1.json"
LOOM_POLICY_PATH="${LOOM_POLICY_PATH:-$LOOM_DEFAULT_POLICY_PATH}"
LOOM_RUNTIME_MANIFEST="${LOOM_RUNTIME_MANIFEST:-$LOOM_DEFAULT_RUNTIME_MANIFEST}"
LOOM_INSTALL_BASE="${LOOM_INSTALL_BASE:-/opt/loom-task-builder}"
LOOM_PASSWD_FILE="${LOOM_PASSWD_FILE:-/etc/passwd}"
LOOM_GROUP_FILE="${LOOM_GROUP_FILE:-/etc/group}"
LOOM_SUBUID_FILE="${LOOM_SUBUID_FILE:-/etc/subuid}"
LOOM_SUBGID_FILE="${LOOM_SUBGID_FILE:-/etc/subgid}"
LOOM_INSTALL_OWNER="${LOOM_INSTALL_OWNER:-root}"
LOOM_INSTALL_GROUP="${LOOM_INSTALL_GROUP:-root}"
LOOM_HOST_ARCH="${LOOM_HOST_ARCH:-$(uname -m)}"
LOOM_SKIP_HOST_CHECKS="${LOOM_SKIP_HOST_CHECKS:-0}"
LOOM_RELEASE_NAME="rootless-runtime-v1"

loom_node_error() {
  echo "error: $*" >&2
  return 1
}

loom_node_load_policy() {
  local cluster_id="$1"
  local slurm_node="$2"
  local values=()
  if [[ ! -f "$LOOM_POLICY_PATH" || -L "$LOOM_POLICY_PATH" ]]; then
    loom_node_error "prerequisite policy is unavailable"
    return
  fi
  if [[ ! -f "$LOOM_RUNTIME_MANIFEST" || -L "$LOOM_RUNTIME_MANIFEST" ]]; then
    loom_node_error "runtime manifest is unavailable"
    return
  fi
  mapfile -t values < <(
    python3 - "$LOOM_POLICY_PATH" "$LOOM_RUNTIME_MANIFEST" "$cluster_id" <<'PY'
import json
import pathlib
import sys
import tomllib

policy_path = pathlib.Path(sys.argv[1])
manifest_path = pathlib.Path(sys.argv[2])
cluster_id = sys.argv[3]
policy = tomllib.loads(policy_path.read_text(encoding="utf-8"))
if policy.get("schema") != "loom.task-image-builder-prerequisites/v1":
    raise SystemExit("invalid policy schema")
if policy.get("production_certification_allowed") is not False:
    raise SystemExit("Phase 1 policy is not inert")
if policy.get("certified_nodes") != []:
    raise SystemExit("Phase 1 policy certifies nodes")
identity = policy["identity"]
clusters = [item for item in policy["clusters"] if item["id"] == cluster_id]
if len(clusters) != 1:
    raise SystemExit("cluster policy is not unique")
cluster = clusters[0]
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if manifest.get("schema") != "loom.task-image-builder-rootless-runtime/v1":
    raise SystemExit("invalid runtime manifest schema")
if manifest.get("release") != "rootless-runtime-v1":
    raise SystemExit("invalid runtime release")
if policy.get("runtime", {}).get("manifest") != manifest_path.name:
    raise SystemExit("policy does not bind the runtime manifest")
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
  if [[ ",$LOOM_EXPECTED_NODES," != *",$slurm_node,"* ]]; then
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

loom_node_runtime() {
  local mode="$1"
  local artifact_dir="$2"
  python3 - "$mode" "$LOOM_RUNTIME_MANIFEST" "$LOOM_HOST_ARCH" \
    "$artifact_dir" "$LOOM_INSTALL_BASE" "$LOOM_INSTALL_OWNER" \
    "$LOOM_INSTALL_GROUP" <<'PY'
import grp
import hashlib
import json
import os
import pathlib
import pwd
import shutil
import stat
import sys
import tarfile
import tempfile

mode, manifest_name, architecture, artifact_name, install_name, owner_name, group_name = (
    sys.argv[1:]
)
manifest_path = pathlib.Path(manifest_name)
artifact_dir = pathlib.Path(artifact_name)
install_base = pathlib.Path(install_name)

def fail(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)

def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()

try:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    release = manifest["architectures"][architecture]
except (OSError, KeyError, json.JSONDecodeError):
    fail("runtime manifest does not contain the host architecture")

expected_artifacts = {item["name"]: item["sha256"] for item in release["artifacts"]}
try:
    actual_entries = list(artifact_dir.iterdir())
except OSError:
    fail("artifact directory is unavailable")
if {item.name for item in actual_entries} != set(expected_artifacts):
    fail("artifact set does not match the runtime manifest")
for item in actual_entries:
    if item.is_symlink():
        fail("artifact symlink is forbidden")
    if not item.is_file():
        fail("artifact must be a regular file")
    if item.stat().st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        fail("artifact has an unsafe mode")
    if digest(item.read_bytes()) != expected_artifacts[item.name]:
        fail("artifact digest does not match the runtime manifest")

buildkit_names = [name for name in expected_artifacts if name.startswith("buildkit-")]
rootlesskit_names = [name for name in expected_artifacts if name.startswith("rootlesskit-")]
slirp_names = [name for name in expected_artifacts if name.startswith("slirp4netns-")]
fuse_names = [name for name in expected_artifacts if name.startswith("fuse-overlayfs-")]
if any(len(items) != 1 for items in (buildkit_names, rootlesskit_names, slirp_names, fuse_names)):
    fail("runtime artifact roles are ambiguous")

selected: dict[str, bytes] = {}
archive_specs = (
    (
        artifact_dir / buildkit_names[0],
        {
            "bin/buildctl": "buildctl",
            "bin/buildkit-runc": "buildkit-runc",
            "bin/buildkitd": "buildkitd",
        },
    ),
    (
        artifact_dir / rootlesskit_names[0],
        {"rootlessctl": "rootlessctl", "rootlesskit": "rootlesskit"},
    ),
)
for archive_path, members in archive_specs:
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            by_name = {member.name: member for member in archive.getmembers()}
            for member_name, binary_name in members.items():
                member = by_name.get(member_name)
                if member is None or not member.isfile():
                    fail("runtime archive is missing a regular selected binary")
                stream = archive.extractfile(member)
                if stream is None:
                    fail("runtime archive selected binary cannot be read")
                selected[binary_name] = stream.read()
    except (OSError, tarfile.TarError):
        fail("runtime archive is invalid")
selected["slirp4netns"] = (artifact_dir / slirp_names[0]).read_bytes()
selected["fuse-overlayfs"] = (artifact_dir / fuse_names[0]).read_bytes()
expected_binaries = release["binaries"]
if set(selected) != set(expected_binaries):
    fail("selected runtime binary set does not match the manifest")
if any(digest(payload) != expected_binaries[name] for name, payload in selected.items()):
    fail("selected runtime binary digest does not match the manifest")

receipt = {
    "schema": "loom.task-image-builder-installed-runtime/v1",
    "release": manifest["release"],
    "architecture": architecture,
    "manifest_sha256": digest(manifest_path.read_bytes()),
    "binary_sha256": expected_binaries,
}
receipt_payload = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n"
release_dir = install_base / "releases" / manifest["release"]
current = install_base / "current"

def verify_installed() -> bool:
    if not release_dir.exists():
        return False
    if release_dir.is_symlink() or not release_dir.is_dir():
        fail("installed release drift is unsafe")
    binary_dir = release_dir / "bin"
    try:
        entries = list(binary_dir.iterdir())
    except OSError:
        fail("installed release drift is incomplete")
    if {item.name for item in entries} != set(expected_binaries):
        fail("installed release drift changed the binary set")
    for item in entries:
        if item.is_symlink() or not item.is_file():
            fail("installed release drift changed a binary type")
        if digest(item.read_bytes()) != expected_binaries[item.name]:
            fail("installed release drift changed a binary digest")
    receipt_path = release_dir / "receipt.json"
    if receipt_path.is_symlink() or not receipt_path.is_file():
        fail("installed release drift removed its receipt")
    if receipt_path.read_bytes() != receipt_payload:
        fail("installed release drift changed its receipt")
    if current.is_symlink():
        if current.resolve() != release_dir.resolve():
            fail("installed release drift changed the current link")
    elif current.exists():
        fail("installed release drift changed the current link type")
    else:
        fail("installed release drift removed the current link")
    return True

if release_dir.exists():
    verify_installed()
    print(json.dumps({"release": manifest["release"], "state": "present"}, sort_keys=True))
    raise SystemExit(0)
if mode in {"check", "validate"}:
    if mode == "check":
        fail("installed rootless runtime is missing")
    print(json.dumps({"release": manifest["release"], "state": "validated"}, sort_keys=True))
    raise SystemExit(0)
if mode != "install":
    fail("runtime operation is invalid")

try:
    owner = pwd.getpwnam(owner_name).pw_uid
    group = grp.getgrnam(group_name).gr_gid
except KeyError:
    fail("installation owner or group is unavailable")
releases = install_base / "releases"
releases.mkdir(parents=True, mode=0o755, exist_ok=True)
stage = pathlib.Path(tempfile.mkdtemp(prefix=f".{manifest['release']}.", dir=releases))
try:
    binary_dir = stage / "bin"
    binary_dir.mkdir(mode=0o755)
    for name, payload in selected.items():
        target = binary_dir / name
        with target.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        target.chmod(0o755)
        os.chown(target, owner, group)
    receipt_path = stage / "receipt.json"
    with receipt_path.open("xb") as handle:
        handle.write(receipt_payload)
        handle.flush()
        os.fsync(handle.fileno())
    receipt_path.chmod(0o644)
    os.chown(receipt_path, owner, group)
    os.chown(binary_dir, owner, group)
    os.chown(stage, owner, group)
    stage.rename(release_dir)
except BaseException:
    shutil.rmtree(stage, ignore_errors=True)
    raise
if current.exists() or current.is_symlink():
    fail("installed release drift occupied the current link")
temporary_link = install_base / f".current.{os.getpid()}"
temporary_link.symlink_to(pathlib.Path("releases") / manifest["release"])
temporary_link.replace(current)
verify_installed()
print(json.dumps({"release": manifest["release"], "state": "installed"}, sort_keys=True))
PY
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
  if [[ "$options" != ext4* && "$options" != xfs* ]] \
    || [[ "$options" != *prjquota* && "$options" != *pquota* ]]; then
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
  local artifact_dir="$3"
  loom_node_load_policy "$cluster_id" "$slurm_node"
  loom_node_verify_slurm_identity "$slurm_node"
  loom_node_runtime validate "$artifact_dir" >/dev/null
  loom_node_identity_preflight
  if ! loom_node_identity_complete; then
    loom_node_error "builder identity prerequisites are incomplete"
    return
  fi
  loom_node_runtime check "$artifact_dir" >/dev/null
  loom_node_host_checks
  printf '{"certified_nodes":[],"cluster_id":"%s","production_certification_allowed":false,"state":"prepared"}\n' \
    "$cluster_id"
}

loom_node_apply() {
  local cluster_id="$1"
  local slurm_node="$2"
  local artifact_dir="$3"
  loom_node_load_policy "$cluster_id" "$slurm_node"
  loom_node_verify_slurm_identity "$slurm_node"
  loom_node_runtime validate "$artifact_dir" >/dev/null
  loom_node_identity_preflight
  loom_node_apply_identity
  loom_node_runtime install "$artifact_dir" >/dev/null
  loom_node_check "$cluster_id" "$slurm_node" "$artifact_dir"
}

loom_node_main() {
  if [[ "$#" -ne 4 || ( "$1" != "check" && "$1" != "apply" ) ]]; then
    echo "usage: sudo $0 {check|apply} <cluster-id> <slurm-node-name> <offline-artifact-directory>" >&2
    return 2
  fi
  if [[ "$LOOM_POLICY_PATH" != "$LOOM_DEFAULT_POLICY_PATH" \
    || "$LOOM_RUNTIME_MANIFEST" != "$LOOM_DEFAULT_RUNTIME_MANIFEST" \
    || "$LOOM_INSTALL_BASE" != "/opt/loom-task-builder" \
    || "$LOOM_PASSWD_FILE" != "/etc/passwd" \
    || "$LOOM_GROUP_FILE" != "/etc/group" \
    || "$LOOM_SUBUID_FILE" != "/etc/subuid" \
    || "$LOOM_SUBGID_FILE" != "/etc/subgid" \
    || "$LOOM_INSTALL_OWNER" != "root" \
    || "$LOOM_INSTALL_GROUP" != "root" \
    || "$LOOM_HOST_ARCH" != "$(uname -m)" \
    || "$LOOM_SKIP_HOST_CHECKS" != "0" ]]; then
    loom_node_error "test overrides are forbidden in the direct installer CLI"
    return
  fi
  if [[ "$1" == "apply" && "$(id -u)" -ne 0 ]]; then
    loom_node_error "node prerequisite installation requires root"
    return
  fi
  "loom_node_$1" "$2" "$3" "$4"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  loom_node_main "$@"
fi
