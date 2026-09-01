# Personal-development native builder runtime rollout

This runbook stages and activates the dedicated native `linux/arm64` personal
candidate builder on GB10 host `gx10-01c7`. It complements the measured OLDLAB
Kubernetes builder runtime; it does not replace it. The transaction creates no
Loom Task, Trial, Worker, Slurm job, reservation, or executable capacity. The
executable-new-capacity ceiling remains exactly `0`.

Repository presence is not operational authority. Use only an exact merged,
protected-CI-approved source commit, schema-4 trusted release, reviewed native
operational plan, owner-only evidence directory, and separately authorized
runtime window. There is no QEMU path and no runc fallback. Stop at the first
unexpected byte, identity, route, process, image, namespace, grant, or capacity
observation.

The ordering is intentional:

1. capture read-only before-state;
2. stage and verify all root-owned bytes while both dedicated services are
   inactive;
3. activate only the dedicated daemon, converge exact current and previous
   images, and run the disposable two-container conformance;
4. stop the daemon, stage the private key and agent unit while inactive, then
   reactivate the daemon and start the agent;
5. continue with the native acceptance runbook, which applies management only
   after it observes the agent service active and then requires fresh signed
   zero-grant readiness before any owner deployment.

Secret values are never printed, placed in command arguments, copied into the
repository, or included in evidence. Paths, ownership, modes, public-key
digests, and bounded Secret key-name inventories may be recorded. Private-key
and CA bytes or digests are prohibited in evidence and issue comments.

## 1. Bind the exact source, release, profile, and evidence authority

Run all blocks in one Bash session from the repository root. Replace every
placeholder. The evidence root must already exist outside the repository and
must be an owner-owned mode-`0700` directory.

```bash
set -euo pipefail
umask 077
export LC_ALL=C
test "$(id -u)" != 0

merged_source_sha='<merged-40-lowercase-hex>'
trusted_release_artifact='<absolute-trusted-release-artifact-directory>'
trusted_release="$trusted_release_artifact/trusted-release.json"
trusted_release_sha256='<trusted-release-64-lowercase-hex>'
previous_trusted_release='<absolute-previous-trusted-release.json-or-empty>'
previous_trusted_release_sha256='<previous-trusted-release-64-lowercase-hex-or-empty>'
runtime_window_id='<authorized-native-runtime-window-id>'
reviewed_kubeconfig='<absolute-owner-only-mode-0600-kubeconfig>'
evidence_root='<absolute-existing-owner-only-evidence-root-outside-repository>'
gb10_dns_observer='<read-only-gb10-dns-observer-ssh-target>'
slurm_observer='<read-only-slurm-observer-ssh-target>'

runtime_profile='deploy/personal-dev-native-builder/runtime-profile-v1.json'
runtime_profile_sha256='c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d'
prepared_control_profile='<absolute-owner-only-prepared-schema-3-profile.toml>'
prepared_control_profile_sha256='<prepared-profile-64-lowercase-hex>'
archive_url='https://storage.googleapis.com/gvisor/releases/release/20260810/aarch64/gvisor.tar.bz2'
archive_sha512='dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7'
rollback_shadow_manifest='<absolute-byte-reviewed-schema-4-shadow-manifest>'
rollback_shadow_sha256='<rollback-shadow-64-lowercase-hex>'

repository_root="$(pwd -P)"
loom_python="$repository_root/.venv/bin/python"
loom_cli() {
  env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONSAFEPATH=1 \
    PYTHONPATH="$repository_root/src" "$loom_python" -m loom_cli "$@"
}
verify_loom_cli_source() {
  env PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONSAFEPATH=1 \
    PYTHONPATH="$repository_root/src" "$loom_python" - "$repository_root" <<'PY'
import sys
from pathlib import Path

import loom
import loom_cli

root = Path(sys.argv[1]).resolve(strict=True)
expected_loom = root / "src" / "loom" / "__init__.py"
expected_loom_cli = root / "src" / "loom_cli" / "__init__.py"
observed_loom = Path(loom.__file__).resolve(strict=True)
observed_loom_cli = Path(loom_cli.__file__).resolve(strict=True)
if observed_loom != expected_loom or observed_loom_cli != expected_loom_cli:
    raise SystemExit(1)
PY
}
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir="$evidence_root/${timestamp}-${merged_source_sha}"
authority_source_sha="$merged_source_sha"
native_authority_git=(
  /usr/bin/env -i HOME=/nonexistent PATH=/usr/bin:/bin LC_ALL=C
  GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
  /usr/bin/git --no-replace-objects -C "$repository_root"
)
authority_source_tree="$(
  "${native_authority_git[@]}" rev-parse --verify "$merged_source_sha^{tree}"
)"
native_authority_client=(
  "$loom_python"
  "$repository_root/scripts/ops/personal_dev_native_builder_runtime_authority_client.py"
)
native_authority_privileged_client=(
  /usr/local/libexec/loom-personal-dev-native-builder-runtime-authority-material-client
)
native_authority_local_archive_root="/run/loom-personal-dev-native-builder-runtime-authority-archives"
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
ssh_run() {
  local target="$1"
  local remote_command
  shift
  remote_command="$(python3 - "$@" <<'PY'
import shlex
import sys

if len(sys.argv) < 2:
    raise SystemExit(1)
sys.stdout.write(shlex.join(sys.argv[1:]))
PY
)"
  test -n "$remote_command"
  ssh "${ssh_options[@]}" "$target" -- "$remote_command"
}
native_authority_stage_agent() {
  local request_id="$1"
  shift
  sudo -n -- "${native_authority_privileged_client[@]}" stage-agent \
    --authority-source-sha "$authority_source_sha" \
    --authority-source-tree "$authority_source_tree" \
    --request-id "$request_id" \
    --runtime-profile-sha256 "$runtime_profile_sha256" \
    --schema-version 1 \
    "$@"
}
validate_native_authority_receipt() {
  local operation="$1"
  local request_id="$2"
  local receipt state state_sha256
  receipt="$(cat)"
  test -n "$receipt" || return 1
  printf '%s' "$receipt" | jq -e \
    --arg operation "$operation" --arg request_id "$request_id" \
    --arg source "$authority_source_sha" --arg tree "$authority_source_tree" \
    --arg profile "$runtime_profile_sha256" '
      def exact_fields($expected):
        if type == "object" then keys == $expected else false end;
      def hex40: type == "string" and test("^[0-9a-f]{40}$");
      def hex64: type == "string" and test("^[0-9a-f]{64}$");
      def uuid:
        type == "string" and
        test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
      def nonnegative_integer: type == "number" and . >= 0 and floor == .;
      def https_origin:
        type == "string" and startswith("https://") and
        (index("\r") | not) and (index("\n") | not) and (index("\u0000") | not);
      def agent_image:
        type == "string" and
        test("^ghcr\\.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:[0-9a-f]{64}$");
      def builder_image:
        type == "string" and
        test("^ghcr\\.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$");
      def conformance:
        exact_fields([
          "architecture","buildkit_sandbox_id","client_sandbox_id",
          "cross_provider_network","foreign_to_provider","host_to_provider",
          "managed_containers_after","managed_networks_after","platform",
          "private_control_plane","public_https","runtime","schema","status"
        ]) and
        .architecture == "arm64" and
        (.buildkit_sandbox_id | hex64) and
        (.client_sandbox_id | hex64) and
        .cross_provider_network == "denied" and
        .foreign_to_provider == "denied" and
        .host_to_provider == "denied" and
        .managed_containers_after == 0 and .managed_networks_after == 0 and
        .platform == "linux/arm64" and .private_control_plane == "denied" and
        .public_https == "allowed" and .runtime == "runsc-personal-dev-native" and
        .schema == "loom-personal-dev-native-builder-conformance-v1" and
        .status == "passed";
      def public_state:
        (
          if .phase == "prepared" then
            exact_fields([
              "authority_source_sha","authority_source_tree","conformance",
              "current_agent","current_builder","current_revision","phase",
              "previous_agent","previous_builder","previous_revision",
              "public_store_origin","runtime_profile_sha256","schema"
            ])
          elif .phase == "staged" or .phase == "active" then
            exact_fields([
              "agent_instance_id","agent_key_id","authority_source_sha",
              "authority_source_tree","conformance","current_agent",
              "current_builder","current_revision","phase","previous_agent",
              "previous_builder","previous_revision","public_key_sha256",
              "public_store_origin","runtime_profile_sha256","schema",
              "service_origin"
            ])
          else false
          end
        ) and
        .schema == "loom.personal-dev-native-builder-runtime-authority-state.v1" and
        .authority_source_sha == $source and .authority_source_tree == $tree and
        .runtime_profile_sha256 == $profile and
        (.current_agent | agent_image) and (.current_builder | builder_image) and
        (.current_revision | hex40) and (.public_store_origin | https_origin) and
        (
          (.previous_agent == "" and .previous_builder == "" and .previous_revision == "") or
          ((.previous_agent | agent_image) and (.previous_builder | builder_image) and
           (.previous_revision | hex40) and .previous_revision != .current_revision)
        ) and
        (
          if .phase == "prepared" then true else
            (.agent_instance_id | uuid) and
            (.agent_key_id | type == "string" and test("^[a-z][a-z0-9._-]{0,63}$")) and
            (.public_key_sha256 | hex64) and (.service_origin | https_origin)
          end
        ) and
        (.conformance | conformance);
      exact_fields([
        "agent_service","architecture","authority_source_sha","authority_source_tree",
        "dockerd_service","executable_new_capacity","host_name",
        "managed_containers","managed_networks","nft_table","operation","phase",
        "request_id","runtime_profile_sha256","schema","state","state_sha256"
      ]) and
      .schema == "loom.personal-dev-native-builder-runtime-authority-receipt.v1" and
      .operation == $operation and .request_id == $request_id and
      .authority_source_sha == $source and .authority_source_tree == $tree and
      .runtime_profile_sha256 == $profile and .host_name == "gx10-01c7" and
      .architecture == "aarch64" and .executable_new_capacity == 0 and
      (.agent_service == "active" or .agent_service == "inactive") and
      (.dockerd_service == "active" or .dockerd_service == "inactive") and
      (.nft_table == "present" or .nft_table == "absent") and
      (.managed_containers | nonnegative_integer) and
      (.managed_networks == null or (.managed_networks | nonnegative_integer)) and
      (
        if $operation == "status" then true
        elif $operation == "prepare" then .phase == "prepared"
        elif $operation == "stage-agent" then .phase == "staged"
        elif $operation == "activate" then .phase == "active"
        elif $operation == "remove" then .phase == "inert"
        else false
        end
      ) and
      (
        if .phase == "inert" then .state == null and .state_sha256 == ""
        else
          (.state | public_state) and (.state_sha256 | hex64) and
          .state.phase == .phase
        end
      )
    ' >/dev/null || return 1
  if test "$(printf '%s' "$receipt" | jq -er .phase)" != inert; then
    state_sha256="$(printf '%s' "$receipt" | jq -er .state_sha256)" || return 1
    state="$(printf '%s' "$receipt" | jq -a -cS -j .state)" || return 1
    test "$(printf '%s\n' "$state" | sha256sum | awk '{print $1}')" = "$state_sha256" || return 1
  fi
  printf '%s' "$receipt" | jq -a -cS -j -e . || return 1
}
native_authority_request() {
  local operation="$1"
  local request_id="$2"
  local output="$3"
  shift 3
  [[ "$request_id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  case "$operation" in
    status|prepare|stage-agent|activate|remove) ;;
    *) exit 1 ;;
  esac
  if ! {
    {
    if test "$operation" = stage-agent; then
      native_authority_stage_agent "$request_id" "$@"
    else
      "${native_authority_client[@]}" "$operation" \
        --authority-source-sha "$authority_source_sha" \
        --authority-source-tree "$authority_source_tree" \
        --request-id "$request_id" \
        --runtime-profile-sha256 "$runtime_profile_sha256" \
        --schema-version 1 \
        "$@"
    fi
  } | sudo -n -- /usr/bin/ssh -F /dev/null \
    -o HostName=207.35.188.227 \
    -o Port=2221 \
    -o User=qianyi \
    -o IdentityFile=/var/lib/loom-staging-rollout/gb10-deploy-ed25519 \
    -o IdentitiesOnly=yes \
    -o PubkeyAuthentication=yes \
    -o PreferredAuthentications=publickey \
    -o GSSAPIAuthentication=no \
    -o HostbasedAuthentication=no \
    -o PasswordAuthentication=no \
    -o KbdInteractiveAuthentication=no \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=yes \
    -o UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts \
    -o GlobalKnownHostsFile=/dev/null \
    -o UpdateHostKeys=no \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o ConnectTimeout=10 \
    trt-gb10-1 \
    'sudo -n -- /usr/local/libexec/loom-personal-dev-native-builder-runtime-authority' \
    | jq -cS -j -s '
      if length == 1 and (.[0] | type) == "object" then .[0]
      else error("authority receipt cardinality") end
    ' | validate_native_authority_receipt "$operation" "$request_id"
  } > "$output"; then
    rm -f -- "$output"
    return 1
  fi
  chmod 0600 "$output"
}
native_authority_stage_archive() {
  (
    local request_id="$1"
    local source="$2"
    local local_dir="$native_authority_local_archive_root/$request_id"
    local local_archive="$local_dir/gvisor-release-20260810.0-aarch64.tar.bz2"
    local remote_dir="/var/tmp/loom-personal-dev-native-builder/$request_id"
    local remote_archive="$remote_dir/gvisor-release-20260810.0-aarch64.tar.bz2"
    local operation_status
    [[ "$request_id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
    test -f "$source" && test ! -L "$source"
    test "$(stat -c %u "$source")" = "$(id -u)"
    test "$(stat -c %a "$source")" = 600
    cleanup_native_authority_local_archive() {
      local primary_status=$?
      local file_status=0
      local directory_status=0
      trap - EXIT
      trap '' HUP INT TERM
      if sudo -n -- /usr/bin/rm -f -- "$local_archive"; then
        :
      else
        file_status=$?
      fi
      if sudo -n -- /usr/bin/rmdir -- "$local_dir"; then
        :
      else
        directory_status=$?
      fi
      if test "$primary_status" -ne 0; then
        exit "$primary_status"
      fi
      if test "$file_status" -ne 0; then
        exit "$file_status"
      fi
      exit "$directory_status"
    }
    trap cleanup_native_authority_local_archive EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    sudo -n -- /usr/bin/install -d -m 0700 "$local_dir"
    test ! -e "$local_archive"
    if /bin/cat -- "$source" \
      | sudo -n -- /usr/bin/install -m 0600 /dev/stdin "$local_archive"; then
      :
    else
      operation_status=$?
      exit "$operation_status"
    fi
    test "$(sudo -n -- /usr/bin/sha512sum "$local_archive" | awk '{print $1}')" \
      = "$archive_sha512"
    if {
      printf 'mkdir %s\n' "$remote_dir"
      printf 'chmod 700 %s\n' "$remote_dir"
      printf 'put %s %s\n' "$local_archive" "$remote_archive"
      printf 'chmod 600 %s\n' "$remote_archive"
    } | sudo -n -- /usr/bin/sftp -b - -F /dev/null \
      -o HostName=207.35.188.227 \
      -o Port=2221 \
      -o User=qianyi \
      -o IdentityFile=/var/lib/loom-staging-rollout/gb10-deploy-ed25519 \
      -o IdentitiesOnly=yes \
      -o PubkeyAuthentication=yes \
      -o PreferredAuthentications=publickey \
      -o GSSAPIAuthentication=no \
      -o HostbasedAuthentication=no \
      -o PasswordAuthentication=no \
      -o KbdInteractiveAuthentication=no \
      -o BatchMode=yes \
      -o StrictHostKeyChecking=yes \
      -o UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts \
      -o GlobalKnownHostsFile=/dev/null \
      -o UpdateHostKeys=no \
      -o ServerAliveInterval=30 \
      -o ServerAliveCountMax=3 \
      -o ConnectTimeout=10 \
      trt-gb10-1; then
      :
    else
      operation_status=$?
      exit "$operation_status"
    fi
  )
}
native_authority_remove_staged_archive() {
  local request_id="$1"
  local remote_dir="/var/tmp/loom-personal-dev-native-builder/$request_id"
  local remote_archive="$remote_dir/gvisor-release-20260810.0-aarch64.tar.bz2"
  [[ "$request_id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  {
    printf 'rm %s\n' "$remote_archive"
    printf 'rmdir %s\n' "$remote_dir"
  } | sudo -n -- /usr/bin/sftp -b - -F /dev/null \
    -o HostName=207.35.188.227 \
    -o Port=2221 \
    -o User=qianyi \
    -o IdentityFile=/var/lib/loom-staging-rollout/gb10-deploy-ed25519 \
    -o IdentitiesOnly=yes \
    -o PubkeyAuthentication=yes \
    -o PreferredAuthentications=publickey \
    -o GSSAPIAuthentication=no \
    -o HostbasedAuthentication=no \
    -o PasswordAuthentication=no \
    -o KbdInteractiveAuthentication=no \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=yes \
    -o UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts \
    -o GlobalKnownHostsFile=/dev/null \
    -o UpdateHostKeys=no \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o ConnectTimeout=10 \
    trt-gb10-1
}
native_authority_prepare() {
  (
    local request_id="$1"
    local source="$2"
    local output="$3"
    shift 3
    cleanup_native_authority_remote_archive() {
      local primary_status=$?
      local cleanup_status=0
      trap - EXIT HUP INT TERM
      trap '' HUP INT TERM
      if native_authority_remove_staged_archive "$request_id"; then
        :
      else
        cleanup_status=$?
      fi
      if test "$primary_status" -ne 0; then
        exit "$primary_status"
      fi
      exit "$cleanup_status"
    }
    trap cleanup_native_authority_remote_archive EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    native_authority_stage_archive "$request_id" "$source"
    native_authority_request prepare "$request_id" "$output" "$@"
  )
}
new_native_authority_request_id() {
  python3 - <<'PY'
from uuid import uuid4

print(uuid4())
PY
}
validate_native_authority_transport_config() {
  local config target
  config="$repository_root/deploy/worker-pools/gb10/ssh_config"
  test -f "$config" && test ! -L "$config"
  target="$(/usr/bin/ssh -G -F "$config" trt-gb10-1)"
  test "$(awk '$1 == "hostname" { print $2; exit }' <<< "$target")" = 207.35.188.227
  test "$(awk '$1 == "port" { print $2; exit }' <<< "$target")" = 2221
  test "$(awk '$1 == "user" { print $2; exit }' <<< "$target")" = qianyi
  test -z "$(awk '$1 == "proxyjump" { print $2; exit }' <<< "$target")"
  test -z "$(awk '$1 == "proxycommand" { print $2; exit }' <<< "$target")"
  test "$(awk '$1 == "identityfile" { print $2; exit }' <<< "$target")" = \
    /var/lib/loom-staging-rollout/gb10-deploy-ed25519
  test "$(awk '$1 == "userknownhostsfile" { print $2; exit }' <<< "$target")" = \
    /etc/loom/staging-rollout-gb10-known-hosts
  test "$(awk '$1 == "globalknownhostsfile" { print $2; exit }' <<< "$target")" = /dev/null
  test "$(awk '$1 == "identitiesonly" { print $2; exit }' <<< "$target")" = yes
  test "$(awk '$1 == "pubkeyauthentication" { print $2; exit }' <<< "$target")" = true
  test "$(awk '$1 == "passwordauthentication" { print $2; exit }' <<< "$target")" = no
  test "$(awk '$1 == "stricthostkeychecking" { print $2; exit }' <<< "$target")" = true
  test "$(awk '$1 == "updatehostkeys" { print $2; exit }' <<< "$target")" = false
}

test "$merged_source_sha" != '<merged-40-lowercase-hex>'
test "$trusted_release_sha256" != '<trusted-release-64-lowercase-hex>'
test "$runtime_window_id" != '<authorized-native-runtime-window-id>'
test "$("${native_authority_git[@]}" rev-parse --show-toplevel)" = \
  "$repository_root"
test "$("${native_authority_git[@]}" rev-parse --verify HEAD^{commit})" = \
  "$merged_source_sha"
test -z "$("${native_authority_git[@]}" status --porcelain=v1 --untracked-files=all)"
test -x "$loom_python"
test -x "${native_authority_client[1]}"
verify_loom_cli_source
test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = \
  "$runtime_profile_sha256"
test "$authority_source_tree" = "$(
  "${native_authority_git[@]}" rev-parse --verify "$merged_source_sha^{tree}"
)"
validate_native_authority_transport_config

for path in "$trusted_release" "$reviewed_kubeconfig" "$prepared_control_profile" \
  "$rollback_shadow_manifest"; do
  test -f "$path"
  test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 600
  test "$(stat -c %h "$path")" = 1
done
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
test "$(jq -er .source_sha "$trusted_release")" = "$merged_source_sha"
test "$(jq -er .schema_version "$trusted_release")" = 4
test "$(sha256sum "$rollback_shadow_manifest" | awk '{print $1}')" = \
  "$rollback_shadow_sha256"
test "$(sha256sum "$prepared_control_profile" | awk '{print $1}')" = \
  "$prepared_control_profile_sha256"

test -d "$evidence_root"
test ! -L "$evidence_root"
test "$(realpath -e "$evidence_root")" = "$evidence_root"
test "$(stat -c %u "$evidence_root")" = "$(id -u)"
test "$(stat -c %a "$evidence_root")" = 700
case "$evidence_root/" in
  "$repository_root"/*) exit 1 ;;
esac
case "$repository_root/" in
  "$evidence_root"/*) exit 1 ;;
esac
test ! -e "$evidence_dir"
install -d -m 0700 "$evidence_dir"

prepared_binding="$(python3 - "$prepared_control_profile" \
  "$runtime_profile_sha256" <<'PY'
import ipaddress
import json
import re
import sys
import tomllib
import uuid
from pathlib import Path
from urllib.parse import urlsplit

value = tomllib.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
native = value.get("native_builder")
network = value.get("network")
identities = value.get("identities")
if (
    not isinstance(native, dict)
    or not isinstance(network, dict)
    or not isinstance(identities, dict)
):
    raise SystemExit(1)
management = urlsplit(network.get("public_origin", ""))
public_store = urlsplit(native.get("public_store_origin", ""))
cidrs = native.get("public_store_endpoint_cidrs")
if (
    value.get("schema_version") != 3
    or native.get("prepared") is not True
    or native.get("host_name") != "gx10-01c7"
    or native.get("runtime_profile_sha256") != sys.argv[2]
    or native.get("provider") != "gb10-gvisor-docker-v1"
    or native.get("platform") != "linux/arm64"
    or native.get("protocol_version") != 1
    or native.get("max_concurrency") != 2
    or identities.get("native_builder_public_secret")
    != "loom-personal-dev-native-builder-public"
    or not isinstance(native.get("agent_instance_id"), str)
    or str(uuid.UUID(native["agent_instance_id"])) != native["agent_instance_id"]
    or not isinstance(native.get("agent_key_id"), str)
    or re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", native["agent_key_id"]) is None
    or not isinstance(native.get("public_key_sha256"), str)
    or re.fullmatch(r"[0-9a-f]{64}", native["public_key_sha256"]) is None
    or native["public_key_sha256"] == "0" * 64
    or not isinstance(cidrs, list)
    or not cidrs
    or management.scheme != "https"
    or not management.hostname
    or management.path not in {"", "/"}
    or management.username
    or management.password
    or management.query
    or management.fragment
    or public_store.scheme != "https"
    or not public_store.hostname
    or public_store.hostname == management.hostname
    or public_store.port not in {None, 443}
    or public_store.path not in {"", "/"}
    or public_store.username
    or public_store.password
    or public_store.query
    or public_store.fragment
):
    raise SystemExit(1)
parsed_cidrs = [ipaddress.ip_network(item, strict=True) for item in cidrs]
canonical_cidrs = [
    str(item)
    for item in sorted(
        parsed_cidrs, key=lambda item: (item.version, int(item.network_address))
    )
]
if (
    cidrs != canonical_cidrs
    or len(set(cidrs)) != len(cidrs)
    or any(not item.is_global or item.prefixlen != item.max_prefixlen for item in parsed_cidrs)
):
    raise SystemExit(1)
print(json.dumps({
    "agent_instance_id": native["agent_instance_id"],
    "agent_key_id": native["agent_key_id"],
    "management_origin": network["public_origin"],
    "native_builder_public_secret": identities["native_builder_public_secret"],
    "public_key_sha256": native["public_key_sha256"],
    "public_store_endpoint_cidrs": cidrs,
    "public_store_origin": native["public_store_origin"],
}, sort_keys=True, separators=(",", ":")))
PY
)"
agent_instance_id="$(jq -er .agent_instance_id <<< "$prepared_binding")"
agent_key_id="$(jq -er .agent_key_id <<< "$prepared_binding")"
expected_public_key_sha256="$(jq -er .public_key_sha256 <<< "$prepared_binding")"
reviewed_management_origin="$(jq -er .management_origin <<< "$prepared_binding")"
native_builder_public_secret="$(jq -er .native_builder_public_secret <<< "$prepared_binding")"
reviewed_public_store_origin="$(jq -er .public_store_origin <<< "$prepared_binding")"
reviewed_public_store_cidrs="$(jq -r '.public_store_endpoint_cidrs[]' <<< "$prepared_binding")"
printf '%s\n' "$prepared_binding" > "$evidence_dir/prepared-profile-binding.json"
chmod 0600 "$evidence_dir/prepared-profile-binding.json"

current_agent="$(jq -er .images.personal_dev_native_builder_agent "$trusted_release")"
current_builder="$(jq -er .images.personal_dev_builder "$trusted_release")"
current_revision="$(jq -er .source_sha "$trusted_release")"
[[ "$current_agent" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:[0-9a-f]{64}$ ]]
[[ "$current_builder" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$ ]]

previous_args=(
  --previous-agent ''
  --previous-builder ''
  --previous-revision ''
)
if test -n "$previous_trusted_release"; then
  [[ "$previous_trusted_release_sha256" =~ ^[0-9a-f]{64}$ ]]
  test -f "$previous_trusted_release"
  test ! -L "$previous_trusted_release"
  test "$(realpath -e "$previous_trusted_release")" = "$previous_trusted_release"
  test "$(stat -c %u "$previous_trusted_release")" = "$(id -u)"
  test "$(stat -c %a "$previous_trusted_release")" = 600
  test "$(stat -c %h "$previous_trusted_release")" = 1
  test "$(sha256sum "$previous_trusted_release" | awk '{print $1}')" = \
    "$previous_trusted_release_sha256"
  test "$(jq -er .schema_version "$previous_trusted_release")" = 4
  previous_agent="$(jq -er .images.personal_dev_native_builder_agent "$previous_trusted_release")"
  previous_builder="$(jq -er .images.personal_dev_builder "$previous_trusted_release")"
  previous_revision="$(jq -er .source_sha "$previous_trusted_release")"
  [[ "$previous_agent" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:[0-9a-f]{64}$ ]]
  [[ "$previous_builder" =~ ^ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$ ]]
  [[ "$previous_revision" =~ ^[0-9a-f]{40}$ ]]
  test "$previous_revision" != "$current_revision"
  previous_args=(
    --previous-agent "$previous_agent"
    --previous-builder "$previous_builder"
    --previous-revision "$previous_revision"
  )
else
  test -z "$previous_trusted_release_sha256"
fi

jq -cnS \
  --arg source "$merged_source_sha" \
  --arg tree "$authority_source_tree" \
  --arg release "$trusted_release_sha256" \
  --arg previous_release "$previous_trusted_release_sha256" \
  --arg profile "$runtime_profile_sha256" \
  --arg prepared_profile "$prepared_control_profile_sha256" \
  --arg archive "$archive_sha512" \
  --arg window "$runtime_window_id" \
  '{archive_sha512:$archive,prepared_profile_sha256:$prepared_profile,
    profile_sha256:$profile,
    source_sha:$source,source_tree:$tree,
    trusted_release_sha256:$release,
    previous_trusted_release_sha256:$previous_release,window_id:$window}' \
  > "$evidence_dir/immutable-inputs.json"
chmod 0600 "$evidence_dir/immutable-inputs.json"
```

Before the runtime window opens, a direct-root administrator on the protected
operator host must install the policy-bound material client from the same
approved authority inventory and provision exactly these fixed inputs:

- `/etc/loom/personal-dev-native-builder-authority-material/agent-ed25519`, a
  root-owned, root-group, single-link regular file with mode `0400` and exactly
  32 bytes;
- `/etc/loom/personal-dev-native-builder-authority-material/service-ca.pem`, a
  root-owned, root-group, single-link regular file with mode `0444` and a size
  from 1 byte through 1 MiB.

The material client validates the complete installed inventory, opens those
fixed files without following links, and gives the FD-only encoder distinct
descriptors numbered at least `3`. The operator does not provide either
pathname. Its local `sudo` authorization is part of the separately provisioned
protected-operator-host transport authority; it is not granted by the GB10
authority sudoers asset, which remains limited to the empty-argument remote
broker. Neither CA bytes nor a CA digest may be recorded; private-key bytes and
digests are prohibited by the same evidence rule.

```bash
sudo -n -- "${native_authority_privileged_client[@]}" emit-public-key \
  --expected-public-key-sha256 "$expected_public_key_sha256" >/dev/null
```

## 2. Capture read-only before-state

The database snapshot records counts only. PostgreSQL credentials remain inside
the existing Postgres container. The Slurm observer account must be read-only;
only `scontrol show` and `squeue` are permitted. This procedure contains no
Slurm mutation and no task submission.

```bash
kubeconfig="$evidence_dir/kubeconfig"
install -m 0600 "$reviewed_kubeconfig" "$kubeconfig"
test "$(sha256sum "$reviewed_kubeconfig" | awk '{print $1}')" = \
  "$(sha256sum "$kubeconfig" | awk '{print $1}')"

capture_host() {
  local output="$1"
  local request_id
  request_id="$(new_native_authority_request_id)"
  native_authority_request status "$request_id" "$output"
}

capture_slurm() {
  local output="$1"
  local temporary="$output.tmp"
  ssh_run "$slurm_observer" scontrol show nodes --json \
    | jq -cS . > "$output"
  ssh_run "$slurm_observer" squeue --json \
    | jq -cS . > "$temporary"
  chmod 0600 "$output" "$temporary"
  jq -cnS \
    --slurpfile nodes "$output" \
    --slurpfile queue "$temporary" \
    '{nodes:$nodes[0],queue:$queue[0]}' > "$output.merged"
  mv "$output.merged" "$output"
  rm -f "$temporary"
}

assert_no_loom_slurm_jobs() {
  jq -e '[.queue.jobs[]? |
    select(((.name // "") | ascii_downcase | startswith("loom")))] |
    length == 0' "$1" >/dev/null
}

postgres_pod="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pod \
  -l app=loom-dev-postgres -o json | jq -er '
    [.items[] | select(.status.phase == "Running") | .metadata.name] |
    if length == 1 then .[0] else error("postgres cardinality") end')"

read_count() {
  local sql="$1"
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
    -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "$1"' \
    sh "$sql"
}

capture_database_counts() {
  local output="$1" grants=null grant_table
  local tasks workers
  tasks="$(read_count 'SELECT count(*) FROM tasks')"
  workers="$(read_count 'SELECT count(*) FROM workers')"
  grant_table="$(read_count "SELECT to_regclass('public.personal_dev_native_build_grants') IS NOT NULL")"
  if test "$grant_table" = t; then
    grants="$(read_count "SELECT count(*) FROM personal_dev_native_build_grants WHERE state IN ('queued','running')")"
  fi
  jq -cnS --argjson grants "$grants" --argjson tasks "$tasks" \
    --argjson workers "$workers" \
    '{active_native_grants:$grants,tasks:$tasks,workers:$workers}' > "$output"
  chmod 0600 "$output"
}

capture_namespaces() {
  kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
    | jq -cS '[.items[].metadata.name |
        select(startswith("loom-dev-") or startswith("loom-build-"))] | sort' \
    > "$1"
  chmod 0600 "$1"
}

capture_host "$evidence_dir/before-host.json"
capture_slurm "$evidence_dir/before-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/before-slurm.json"
capture_database_counts "$evidence_dir/before-database-counts.json"
capture_namespaces "$evidence_dir/before-namespaces.json"
loom_cli admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  > "$evidence_dir/before-capacity.status.json"
chmod 0600 "$evidence_dir/before-capacity.status.json"
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/before-capacity.status.json" >/dev/null
```

The personal control-plane status used later must include the exact canonical
fragments `"manager_ceiling":0` and `"worker_available":false`; neither
host activation nor agent registration may weaken them.

## 3. Prove the current public-store DNS/CIDR binding

The prepared profile and operational plan must contain the same exact HTTPS
origin on external port 443 and sorted public `/32` or `/128` endpoints. The
prepared shadow must already own the TLS Ingress while routing it to the
selectorless disabled Service; only an acceptance or operational manifest may
route it to MinIO port 9000 and open the matching ingress rule. Port 9001 must
remain absent. Resolve again through the read-only GB10 observer at the start
of the window; guessed,
private, broad, stale, or extra addresses are a stop condition.

```bash
public_store_host="$(python3 - "$reviewed_public_store_origin" <<'PY'
import sys
from urllib.parse import urlsplit

value = urlsplit(sys.argv[1])
if (
    value.scheme != "https"
    or not value.hostname
    or value.port not in {None, 443}
    or value.path not in {"", "/"}
):
    raise SystemExit(1)
if value.username or value.password or value.query or value.fragment:
    raise SystemExit(1)
print(value.hostname)
PY
)"

dns_raw="$evidence_dir/public-store-dns.raw"
{
  ssh_run "$gb10_dns_observer" getent ahostsv4 "$public_store_host" || true
  ssh_run "$gb10_dns_observer" getent ahostsv6 "$public_store_host" || true
} > "$dns_raw"
chmod 0600 "$dns_raw"

normalize_public_store_cidrs() {
  python3 -c 'import ipaddress,sys
values=[]
for line in sys.stdin:
    address=ipaddress.ip_address(line.strip())
    if address.version == 6 and address.ipv4_mapped is not None:
        address=address.ipv4_mapped
    if not address.is_global:
        raise SystemExit(1)
    values.append(f"{address}/{address.max_prefixlen}")
if not values:
    raise SystemExit(1)
networks=[ipaddress.ip_network(value,strict=True) for value in set(values)]
print("\n".join(
    str(item) for item in sorted(
        networks,key=lambda item:(item.version,int(item.network_address))
    )
))'
}

observed_public_store_cidrs="$(awk '{print $1}' "$dns_raw" | sort -u \
  | normalize_public_store_cidrs)"
test "$observed_public_store_cidrs" = "$reviewed_public_store_cidrs"
jq -cnS --arg host "$public_store_host" \
  --arg origin "$reviewed_public_store_origin" \
  --arg cidrs "$observed_public_store_cidrs" \
  '{host:$host,origin:$origin,public_store_endpoint_cidrs:($cidrs|split("\n"))}' \
  > "$evidence_dir/public-store-dns.json"
chmod 0600 "$evidence_dir/public-store-dns.json"
rm -f "$dns_raw"
```

## 4. Download and stage data, then prepare through the fixed authority

Download the public gVisor archive without privilege. An unprivileged read feeds
a fixed root-private local copy whose digest is checked before root SFTP opens
it; root never reopens the operator pathname. The data-only upload authenticates
as `qianyi` and creates no remote shell. The fixed broker opens the resulting
owner-only remote file, copies it into its root-private state directory,
verifies the digest, and performs preflight, installation, release convergence,
the fixed two-container conformance asset, and compensation back to inert state.

```bash
archive="$evidence_dir/gvisor-release-20260810.0-aarch64.tar.bz2"
archive_part="$archive.part"
curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
  --connect-timeout 10 --max-time 1800 --max-filesize 1073741824 \
  --output "$archive_part" "$archive_url"
chmod 0600 "$archive_part"
test "$(sha512sum "$archive_part" | awk '{print $1}')" = "$archive_sha512"
mv -T "$archive_part" "$archive"

prepare_request_id="$(new_native_authority_request_id)"
prepare_archive="/var/tmp/loom-personal-dev-native-builder/$prepare_request_id/gvisor-release-20260810.0-aarch64.tar.bz2"
native_authority_prepare "$prepare_request_id" "$archive" \
  "$evidence_dir/runtime-prepare.json" \
  --archive-path "$prepare_archive" \
  --archive-sha512 "$archive_sha512" \
  --current-agent "$current_agent" \
  --current-builder "$current_builder" \
  --current-revision "$current_revision" \
  "${previous_args[@]}" \
  --public-store-origin "$reviewed_public_store_origin"
prepared_state_sha256="$(jq -er .state_sha256 "$evidence_dir/runtime-prepare.json")"
jq -e '.phase == "prepared" and .agent_service == "inactive" and
  .dockerd_service == "inactive" and .nft_table == "absent" and
  .managed_containers == 0 and .managed_networks == null' \
  "$evidence_dir/runtime-prepare.json" >/dev/null
```

## 5. Review the prepared public receipt

The canonical `prepare` receipt is the complete host evidence boundary for
preflight, the sealed installer, current/previous image convergence, and the
fixed KVM-gVisor conformance asset. It records only public runtime identities,
state, and conformance result; it never records host commands, logs, or secret
material. No operator-supplied source or executable byte crosses this boundary.

## 6. Stage the agent and activate it through the fixed authority

`stage-agent` accepts only the prepared-state digest and public agent identity.
The sealed installed material client opens the two fixed protected inputs and
passes distinct descriptors to the FD-only encoder. The values and their
digests are not in the client header, environment, command arguments, receipt,
evidence, or staging directory.

```bash
stage_request_id="$(new_native_authority_request_id)"
native_authority_request stage-agent "$stage_request_id" \
  "$evidence_dir/agent-stage.json" \
  --expected-state-sha256 "$prepared_state_sha256" \
  --agent-image "$current_agent" \
  --builder-image "$current_builder" \
  --service-origin "$reviewed_management_origin" \
  --agent-instance-id "$agent_instance_id" \
  --agent-key-id "$agent_key_id" \
  --expected-public-key-sha256 "$expected_public_key_sha256"
staged_state_sha256="$(jq -er .state_sha256 "$evidence_dir/agent-stage.json")"
jq -e '.phase == "staged" and .agent_service == "inactive" and
  .dockerd_service == "inactive" and .nft_table == "absent" and
  .managed_containers == 0 and .managed_networks == null' \
  "$evidence_dir/agent-stage.json" >/dev/null

emit_public_key() {
  sudo -n -- "${native_authority_privileged_client[@]}" emit-public-key \
    --expected-public-key-sha256 "$expected_public_key_sha256"
}

emit_public_key \
  | kubectl --kubeconfig "$kubeconfig" --namespace loom-dev create secret generic \
      "$native_builder_public_secret" \
      --from-file=public-key=/dev/stdin --dry-run=client -o yaml \
  | kubectl --kubeconfig "$kubeconfig" apply --server-side \
      --field-manager=loom-personal-dev-native-builder-public-key -f - \
      > "$evidence_dir/native-builder-public-key.apply.txt"
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get secret \
  "$native_builder_public_secret" \
  -o 'go-template={{range $key, $value := .data}}{{$key}}{{"\n"}}{{end}}')" = \
  public-key
chmod 0600 "$evidence_dir/native-builder-public-key.apply.txt"

activate_request_id="$(new_native_authority_request_id)"
native_authority_request activate "$activate_request_id" \
  "$evidence_dir/runtime-activate.json" \
  --expected-state-sha256 "$staged_state_sha256"
jq -e '.phase == "active" and .agent_service == "active" and
  .dockerd_service == "active" and .nft_table == "present" and
  .managed_containers == 0 and .managed_networks == 0' \
  "$evidence_dir/runtime-activate.json" >/dev/null

capture_host "$evidence_dir/after-host.json"
```

The agent is now active before management activation, but signed durable
readiness is not yet possible if the current management release has native mode
disabled. That is expected. Continue immediately with
`personal-dev-native-builder-acceptance.md`; its `signed-zero-grant-readiness`
gate occurs after management apply and before any owner request. The runtime
transaction is not accepted until that gate passes.

## 7. Capture after-state and seal evidence

Run this block after the acceptance runbook has reached signed zero-grant
readiness. The Slurm and Task/Worker counts must be unchanged, active native
grants and dynamic namespaces must be zero, the capacity manager remains at
ceiling zero, and both host units have their exact active identity.

```bash
capture_slurm "$evidence_dir/after-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/after-slurm.json"
capture_database_counts "$evidence_dir/after-database-counts.json"
capture_namespaces "$evidence_dir/after-namespaces.json"
loom_cli admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  > "$evidence_dir/after-capacity.status.json"
chmod 0600 "$evidence_dir/after-capacity.status.json"

jq -e --slurpfile before "$evidence_dir/before-database-counts.json" '
  .tasks == $before[0].tasks and .workers == $before[0].workers and
  .active_native_grants == 0' "$evidence_dir/after-database-counts.json" >/dev/null
jq -e '. == []' "$evidence_dir/after-namespaces.json" >/dev/null
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/after-capacity.status.json" >/dev/null

(
  cd "$evidence_dir"
  find . -maxdepth 1 -type f ! -name kubeconfig \
    ! -name evidence-index.sha256 -printf '%P\n' \
    | LC_ALL=C sort \
    | while IFS= read -r file; do sha256sum "$file"; done \
    > evidence-index.sha256
  chmod 0600 evidence-index.sha256
)
```

The evidence index is sanitized. Secret values are never sealed, uploaded, or
included in review evidence. Keep the kubeconfig and raw operator credentials
outside the index.

## Rollback to the exact inert shadow

Rollback is permitted only with zero active grants and no personal/build
namespace. First apply the exact reviewed shadow to disable new provider claims.
The fixed `remove` transition stops the agent before the dedicated daemon,
removes only the exact nft table, and removes only byte-identical managed
runtime files. The dedicated image cache and system identities are retained as
inert state: removal never recursively deletes Docker data or accounts. Its
canonical receipt reports the resulting inert public state and never restarts
or alters the primary Docker daemon.

```bash
rollback_shadow_recheck="$evidence_dir/rollback-shadow.recheck.yaml"
install -m 0600 "$rollback_shadow_manifest" "$rollback_shadow_recheck"
cmp -s "$rollback_shadow_recheck" "$rollback_shadow_manifest"
test "$(sha256sum "$rollback_shadow_recheck" | awk '{print $1}')" = \
  "$rollback_shadow_sha256"

capture_database_counts "$evidence_dir/rollback-pre-counts.json"
jq -e '.active_native_grants == 0' \
  "$evidence_dir/rollback-pre-counts.json" >/dev/null
capture_namespaces "$evidence_dir/rollback-pre-namespaces.json"
jq -e '. == []' "$evidence_dir/rollback-pre-namespaces.json" >/dev/null

kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$rollback_shadow_manifest" \
  > "$evidence_dir/rollback-shadow.diff.txt" 2>&1 || rollback_diff_status=$?
test "${rollback_diff_status:-0}" -eq 0 || test "$rollback_diff_status" -eq 1
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$rollback_shadow_manifest" \
  > "$evidence_dir/rollback-shadow.apply.txt"

capture_host "$evidence_dir/runtime-remove-preflight.json"
remove_state_sha256="$(jq -er .state_sha256 "$evidence_dir/runtime-remove-preflight.json")"
remove_request_id="$(new_native_authority_request_id)"
native_authority_request remove "$remove_request_id" \
  "$evidence_dir/runtime-remove.json" \
  --expected-state-sha256 "$remove_state_sha256"
jq -e '.phase == "inert" and .agent_service == "inactive" and
  .dockerd_service == "inactive" and .nft_table == "absent" and
  .managed_containers == 0 and .managed_networks == null and
  .state == null and .state_sha256 == ""' \
  "$evidence_dir/runtime-remove.json" >/dev/null

loom_cli admin personal-dev-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  --file deploy/dev-fleet/personal-dev-control-plane.toml \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$evidence_dir/rollback-shadow.status.json"
chmod 0600 "$evidence_dir"/rollback-*
```

If a grant, managed container, network, namespace, changed byte, or unexpected
unit remains, stop. Do not broaden selectors or improvise cleanup.
