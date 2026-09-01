# Personal-development native builder two-owner acceptance

This runbook proves a release-bound multi-person development environment with
native OLDLAB `linux/amd64` Kubernetes Jobs and native GB10 `linux/arm64`
grants. Two owners deploy and update different arbitrary local source trees,
exercise all cross-owner denials, and clean up through authenticated Loom APIs.
Candidate bytes run only under `runsc-personal-dev` or
`runsc-personal-dev-native`; there is no QEMU or runc fallback.

This is application-candidate acceptance. It contains no task submission and
no Slurm mutation. Personal workers remain unavailable and the
executable-new-capacity ceiling remains exactly `0`.
Every accepted control-plane status therefore contains the canonical fragments
`"manager_ceiling":0` and `"worker_available":false`.

The authority order is mandatory: activate the agent while native management
is disabled; apply one expiring schema-3 acceptance plan; complete the full
two-owner lifecycle; clean up and reapply the exact inert shadow; verify a
canonical schema-v3 result; then derive and apply the schema-2 operational
plan. No operational plan exists before verification. Secret values are never
printed, stored in evidence, passed as command arguments, or copied into the
repository.

## 1. Bind immutable inputs and owner sessions

Complete the native runtime runbook through agent activation first. Use one
non-root shell. Every referenced input is an owner-only, reviewed file.

```bash
set -euo pipefail
umask 077
export LC_ALL=C
test "$(id -u)" != 0

repository_root="$(pwd -P)"
merged_source_sha='<merged-40-lowercase-hex>'
trusted_release='<absolute-owner-only-trusted-release.json>'
trusted_release_sha256='<trusted-release-64-lowercase-hex>'
profile='<absolute-owner-only-prepared-schema-3-profile.toml>'
profile_sha256='<prepared-profile-64-lowercase-hex>'
acceptance_plan='<absolute-owner-only-native-acceptance-plan-v3.json>'
acceptance_plan_sha256='<native-acceptance-plan-64-lowercase-hex>'
rollback_shadow_manifest='<absolute-byte-reviewed-schema-4-shadow-manifest>'
rollback_shadow_sha256='<rollback-shadow-64-lowercase-hex>'
runtime_evidence='<absolute-active-native-runtime-evidence-directory>'
runtime_profile_sha256='c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d'
archive_sha512='dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7'

reviewed_kubeconfig='<absolute-owner-only-mode-0600-kubeconfig>'
evidence_root='<absolute-existing-owner-only-evidence-root-outside-repository>'
slurm_observer='<read-only-slurm-observer-ssh-target>'
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
native_authority_client=(
  "$loom_python"
  "$repository_root/scripts/ops/personal_dev_native_builder_runtime_authority_client.py"
)
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
    status) ;;
    *) return 1 ;;
  esac
  if ! "${native_authority_client[@]}" "$operation" \
    --authority-source-sha "$authority_source_sha" \
    --authority-source-tree "$authority_source_tree" \
    --request-id "$request_id" \
    --runtime-profile-sha256 "$runtime_profile_sha256" \
    --schema-version 1 \
    "$@" | sudo -n -- /usr/bin/ssh -F /dev/null \
    -o HostName=192.168.20.12 \
    -o Port=22 \
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
    -o 'ProxyCommand=/usr/bin/ssh -F /dev/null -o HostName=207.35.188.227 -o Port=2221 -o User=qianyi -o IdentityFile=/var/lib/loom-staging-rollout/gb10-deploy-ed25519 -o IdentitiesOnly=yes -o PubkeyAuthentication=yes -o PreferredAuthentications=publickey -o GSSAPIAuthentication=no -o HostbasedAuthentication=no -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts -o GlobalKnownHostsFile=/dev/null -o UpdateHostKeys=no -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -W "[%h]:%p" trt-gb10-1' \
    trt-gb10-2 \
    'sudo -n -- /usr/local/libexec/loom-personal-dev-native-builder-runtime-authority' \
    | jq -cS -j -s '
      if length == 1 and (.[0] | type) == "object" then .[0]
      else error("authority receipt cardinality") end
    ' | validate_native_authority_receipt "$operation" "$request_id" > "$output"; then
    rm -f -- "$output"
    return 1
  fi
  chmod 0600 "$output"
}
new_native_authority_request_id() {
  python3 - <<'PY'
from uuid import uuid4

print(uuid4())
PY
}
validate_native_authority_transport_config() {
  local config target jump
  config="$repository_root/deploy/worker-pools/gb10/ssh_config"
  test -f "$config" && test ! -L "$config"
  target="$(/usr/bin/ssh -G -F "$config" trt-gb10-2)"
  jump="$(/usr/bin/ssh -G -F "$config" trt-gb10-1)"
  test "$(awk '$1 == "hostname" { print $2; exit }' <<< "$target")" = 192.168.20.12
  test "$(awk '$1 == "port" { print $2; exit }' <<< "$target")" = 22
  test "$(awk '$1 == "user" { print $2; exit }' <<< "$target")" = qianyi
  test "$(awk '$1 == "proxyjump" { print $2; exit }' <<< "$target")" = trt-gb10-1
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
  test "$(awk '$1 == "hostname" { print $2; exit }' <<< "$jump")" = 207.35.188.227
  test "$(awk '$1 == "port" { print $2; exit }' <<< "$jump")" = 2221
  test "$(awk '$1 == "user" { print $2; exit }' <<< "$jump")" = qianyi
}
trusted_launcher_profile='<absolute-owner-only-trusted-launcher-profile.json>'
scanner_finding_policy='<absolute-owner-only-scanner-finding-policy.json>'
backup_restore_evidence='<absolute-owner-only-backup-restore-evidence.json>'
bound_evidence_args=(
  --source-root "$repository_root"
  --trusted-launcher-profile-file "$trusted_launcher_profile"
  --scanner-finding-policy-file "$scanner_finding_policy"
  --backup-restore-evidence-file "$backup_restore_evidence"
)

owner_0_xdg='<absolute-mode-0700-owner-0-xdg-config-root>'
owner_1_xdg='<absolute-mode-0700-owner-1-xdg-config-root>'
owner_0_source='<absolute-owner-0-source-root>'
owner_1_source='<absolute-owner-1-source-root>'
owner_0_update_source='<absolute-owner-0-updated-source-root>'
owner_1_update_source='<absolute-owner-1-updated-source-root>'
owner_0_name='<owner-0-personal-name>'
owner_1_name='<owner-1-personal-name>'

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir="$evidence_root/${timestamp}-native-two-owner-$merged_source_sha"
authority_source_sha="$merged_source_sha"
authority_source_tree="$(git rev-parse HEAD^{tree})"
test "$(git rev-parse --show-toplevel)" = "$repository_root"
test "$(git rev-parse HEAD)" = "$merged_source_sha"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test -x "$loom_python"
test -x "${native_authority_client[1]}"
verify_loom_cli_source
test "$authority_source_tree" = "$(git rev-parse HEAD^{tree})"
validate_native_authority_transport_config
for path in "$trusted_release" "$profile" "$acceptance_plan" \
  "$rollback_shadow_manifest" "$reviewed_kubeconfig" \
  "$trusted_launcher_profile" "$scanner_finding_policy" \
  "$backup_restore_evidence"; do
  test -f "$path" && test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 600
  test "$(stat -c %h "$path")" = 1
done
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = "$trusted_release_sha256"
test "$(sha256sum "$profile" | awk '{print $1}')" = "$profile_sha256"
test "$(sha256sum "$acceptance_plan" | awk '{print $1}')" = "$acceptance_plan_sha256"
test "$(sha256sum "$rollback_shadow_manifest" | awk '{print $1}')" = "$rollback_shadow_sha256"
test "$(jq -er .schema_version "$trusted_release")" = 4
test "$(jq -er .schema_version "$acceptance_plan")" = 3
test "$(jq -er .source.commit "$acceptance_plan")" = "$merged_source_sha"
test "$(jq -er .release.trusted_release_sha256 "$acceptance_plan")" = "$trusted_release_sha256"
test "$(jq -er .release.shadow_manifest_sha256 "$acceptance_plan")" = "$rollback_shadow_sha256"
test "$(jq -er .native_builder.runtime_profile_sha256 "$acceptance_plan")" = "$runtime_profile_sha256"
test "$(jq -er .native_builder.provider "$acceptance_plan")" = gb10-gvisor-docker-v1
test "$(jq -er .native_builder.platform "$acceptance_plan")" = linux/arm64
test "$(jq -er .native_builder.max_concurrency "$acceptance_plan")" = 2
test "$(jq -er .manager.executable_new_capacity_ceiling "$acceptance_plan")" = 0
test "$(jq -er '.acceptance_owners | length' "$acceptance_plan")" = 2
now_epoch="$(date -u +%s)"
test "$(date -u -d "$(jq -er .window.started_at "$acceptance_plan")" +%s)" -le "$now_epoch"
test "$now_epoch" -lt "$(date -u -d "$(jq -er .window.expires_at "$acceptance_plan")" +%s)"

test -d "$runtime_evidence" && test ! -L "$runtime_evidence"
test "$(realpath -e "$runtime_evidence")" = "$runtime_evidence"
test "$(stat -c %u "$runtime_evidence")" = "$(id -u)"
test "$(stat -c %a "$runtime_evidence")" = 700
jq -e --arg source "$merged_source_sha" --arg release "$trusted_release_sha256" \
  --arg profile "$runtime_profile_sha256" --arg prepared "$profile_sha256" \
  --arg archive "$archive_sha512" '
  .source_sha == $source and .trusted_release_sha256 == $release and
  .profile_sha256 == $profile and .prepared_profile_sha256 == $prepared and
  .archive_sha512 == $archive' "$runtime_evidence/immutable-inputs.json" >/dev/null

test -d "$evidence_root" && test ! -L "$evidence_root"
test "$(realpath -e "$evidence_root")" = "$evidence_root"
test "$(stat -c %u "$evidence_root")" = "$(id -u)"
test "$(stat -c %a "$evidence_root")" = 700
case "$evidence_root/" in "$repository_root"/*) exit 1 ;; esac
test ! -e "$evidence_dir"
install -d -m 0700 "$evidence_dir"
kubeconfig="$evidence_dir/kubeconfig"
install -m 0600 "$reviewed_kubeconfig" "$kubeconfig"

for path in "$owner_0_xdg" "$owner_1_xdg"; do
  test -d "$path" && test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 700
done
test "$(realpath -e "$owner_0_xdg")" != "$(realpath -e "$owner_1_xdg")"
for config in "$owner_0_xdg/loom/config.toml" "$owner_1_xdg/loom/config.toml"; do
  test -f "$config" && test ! -L "$config"
  test "$(stat -c %a "$config")" = 600
  test "$(stat -c %h "$config")" = 1
done
test "$(stat -c %d:%i "$owner_0_xdg/loom/config.toml")" != \
  "$(stat -c %d:%i "$owner_1_xdg/loom/config.toml")"
declare -A source_roots=()
for path in "$owner_0_source" "$owner_1_source" \
  "$owner_0_update_source" "$owner_1_update_source"; do
  test -d "$path" && test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(realpath -e "$path")" != "$repository_root"
  source_roots["$(realpath -e "$path")"]=1
done
test "${#source_roots[@]}" -eq 4
```

Use source trees whose trusted Docker builds take long enough to observe real
overlap. Do not add artificial network dependencies or task invocations.

## 2. Define read-only boundaries and status interlocks

```bash
postgres_pod="$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get pod \
  -l app=loom-dev-postgres -o json | jq -er '
  [.items[] | select(.status.phase == "Running") | .metadata.name] |
  if length == 1 then .[0] else error("postgres cardinality") end')"
read_count() {
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev exec "$postgres_pod" \
    -c postgres -- /bin/sh -euc \
    'exec psql -AtX --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "$1"' \
    sh "$1"
}
capture_counts() {
  local output="$1" grants=null grant_table tasks workers
  grant_table="$(read_count "SELECT to_regclass('public.personal_dev_native_build_grants') IS NOT NULL")"
  if test "$grant_table" = t; then
    grants="$(read_count "SELECT count(*) FROM personal_dev_native_build_grants WHERE state IN ('queued','running')")"
  fi
  tasks="$(read_count 'SELECT count(*) FROM tasks')"
  workers="$(read_count 'SELECT count(*) FROM workers')"
  jq -cnS --argjson grants "$grants" --argjson tasks "$tasks" \
    --argjson workers "$workers" \
    '{active_native_grants:$grants,tasks:$tasks,workers:$workers}' > "$output"
  chmod 0600 "$output"
}
capture_namespaces() {
  kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
    | jq -cS '[.items[].metadata.name |
      select(startswith("loom-dev-") or startswith("loom-build-"))] | sort' > "$1"
  chmod 0600 "$1"
}
capture_slurm() {
  local output="$1" queue="$1.queue"
  ssh_run "$slurm_observer" scontrol show nodes --json | jq -cS . > "$output"
  ssh_run "$slurm_observer" squeue --json | jq -cS . > "$queue"
  jq -cnS --slurpfile nodes "$output" --slurpfile jobs "$queue" \
    '{nodes:$nodes[0],queue:$jobs[0]}' > "$output.merged"
  mv "$output.merged" "$output" && rm -f "$queue" && chmod 0600 "$output"
}
assert_no_loom_slurm_jobs() {
  jq -e '[.queue.jobs[]? | select(((.name // "") | ascii_downcase |
    startswith("loom")))] | length == 0' "$1" >/dev/null
}
assert_canonical_json() {
  local source="$1" canonical
  canonical="$(mktemp "$evidence_dir/canonical.XXXXXX")"
  jq -cS -j . "$source" > "$canonical"
  cmp -s "$source" "$canonical"
  rm -f "$canonical"
}
assert_canonical_json_line() {
  local source="$1" canonical
  canonical="$(mktemp "$evidence_dir/canonical-line.XXXXXX")"
  jq -cS . "$source" > "$canonical"
  cmp -s "$source" "$canonical"
  rm -f "$canonical"
}
acceptance_status() {
  loom_cli admin personal-dev-control-plane status-acceptance \
    --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" \
    --trusted-release-file "$trusted_release" \
    --trusted-release-sha256 "$trusted_release_sha256" \
    --acceptance-plan-file "$acceptance_plan" \
    --acceptance-plan-sha256 "$acceptance_plan_sha256" \
    "${bound_evidence_args[@]}" > "$1"
  chmod 0600 "$1"
  jq -e '.ready == true and .blockers == [] and .manager_ceiling == 0 and
    .worker_available == false and any(.components[];
      .name == "native-builder" and .observed == 1 and .ready == true)' "$1" >/dev/null
}

capture_counts "$evidence_dir/before-database-counts.json"
capture_namespaces "$evidence_dir/before-namespaces.json"
capture_slurm "$evidence_dir/before-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/before-slurm.json"
loom_cli admin capacity-control-plane status --namespace loom-dev \
  --kubeconfig "$kubeconfig" > "$evidence_dir/before-capacity.status.json"
chmod 0600 "$evidence_dir/before-capacity.status.json"
jq -e '.active_native_grants == null or .active_native_grants == 0' \
  "$evidence_dir/before-database-counts.json" >/dev/null
jq -e '. == []' "$evidence_dir/before-namespaces.json" >/dev/null
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/before-capacity.status.json" >/dev/null
```

## 3. Apply only the expiring schema-3 acceptance plane

```bash
pre_management_status_request_id="$(new_native_authority_request_id)"
native_authority_request status "$pre_management_status_request_id" \
  "$evidence_dir/agent-active-pre-management.json"
jq -e '.phase == "active" and .agent_service == "active" and
  .dockerd_service == "active" and .nft_table == "present" and
  .managed_containers == 0 and .managed_networks == 0' \
  "$evidence_dir/agent-active-pre-management.json" >/dev/null

shadow_recheck="$evidence_dir/preflight-shadow.yaml"
loom_cli admin personal-dev-control-plane render --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$shadow_recheck" 2> "$evidence_dir/preflight-shadow.render.json"
chmod 0600 "$shadow_recheck" "$evidence_dir/preflight-shadow.render.json"
cmp -s "$shadow_recheck" "$rollback_shadow_manifest"

acceptance_manifest="$evidence_dir/native-acceptance.rendered.yaml"
loom_cli admin personal-dev-control-plane render-acceptance \
  --file "$profile" --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --acceptance-plan-file "$acceptance_plan" \
  --acceptance-plan-sha256 "$acceptance_plan_sha256" \
  "${bound_evidence_args[@]}" \
  > "$acceptance_manifest" 2> "$evidence_dir/native-acceptance.render.json"
chmod 0600 "$acceptance_manifest" "$evidence_dir/native-acceptance.render.json"
acceptance_manifest_sha256="$(sha256sum "$acceptance_manifest" | awk '{print $1}')"
acceptance_diff_rc=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane -f "$acceptance_manifest" \
  > "$evidence_dir/native-acceptance.server-side-diff.txt" 2>&1 || acceptance_diff_rc=$?
test "$acceptance_diff_rc" -eq 0 || test "$acceptance_diff_rc" -eq 1
chmod 0600 "$evidence_dir/native-acceptance.server-side-diff.txt"
capture_counts "$evidence_dir/pre-apply-database-counts.json"
jq -e '.active_native_grants == null or .active_native_grants == 0' \
  "$evidence_dir/pre-apply-database-counts.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane -f "$acceptance_manifest" \
  > "$evidence_dir/native-acceptance.server-side-apply.txt"
chmod 0600 "$evidence_dir/native-acceptance.server-side-apply.txt"
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s

signed_readiness="$evidence_dir/signed-zero-grant-readiness.status.json"
readiness_deadline=$((SECONDS + 180))
until acceptance_status "$signed_readiness"; do
  test "$SECONDS" -lt "$readiness_deadline"
  sleep 2
done
capture_counts "$evidence_dir/signed-zero-grant-readiness.counts.json"
jq -e '.active_native_grants == 0' \
  "$evidence_dir/signed-zero-grant-readiness.counts.json" >/dev/null
acceptance_status "$evidence_dir/pre-deploy.status.json"
```

Run section 7 of the runtime runbook now. It seals the host transaction while
signed readiness is fresh, active grants are zero, and no dynamic namespace
exists. Return here only after its evidence index is complete; do not start an
owner request early.

## 4. Authenticate two distinct owners

```bash
owner_0_whoami="$evidence_dir/owner-0.whoami.json"
owner_1_whoami="$evidence_dir/owner-1.whoami.json"
XDG_CONFIG_HOME="$owner_0_xdg" loom_cli auth whoami --format json | jq -cS . > "$owner_0_whoami"
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli auth whoami --format json | jq -cS . > "$owner_1_whoami"
chmod 0600 "$owner_0_whoami" "$owner_1_whoami"
for whoami in "$owner_0_whoami" "$owner_1_whoami"; do
  jq -e '.auth_kind == "bearer" and .credential_type == "user_owned_api_token" and
    .principal_type == "team" and .role == null' "$whoami" >/dev/null
done
owner_0_principal="$(jq -cS '{user_id,team_id}' "$owner_0_whoami")"
owner_1_principal="$(jq -cS '{user_id,team_id}' "$owner_1_whoami")"
test "$owner_0_principal" != "$owner_1_principal"
test "$owner_0_principal" = "$(jq -cS .acceptance_owners[0] "$acceptance_plan")"
test "$owner_1_principal" = "$(jq -cS .acceptance_owners[1] "$acceptance_plan")"
```

## 5. Run concurrent initial deploys and capture native overlap

Start both source-fresh requests before either wait. The overlap must contain
two simultaneous amd64 Jobs and two simultaneous arm64 grants plus two
BuildKit/client pairs.

```bash
( XDG_CONFIG_HOME="$owner_0_xdg" loom_cli service up \
    --environment "dev-$owner_0_name" --source-root "$owner_0_source" \
    --min-slots 0 --max-slots 2 ) > "$evidence_dir/owner-0.deploy-v1.txt" 2>&1 &
owner_0_deploy_pid=$!
( XDG_CONFIG_HOME="$owner_1_xdg" loom_cli service up \
    --environment "dev-$owner_1_name" --source-root "$owner_1_source" \
    --min-slots 0 --max-slots 2 ) > "$evidence_dir/owner-1.deploy-v1.txt" 2>&1 &
owner_1_deploy_pid=$!
raw_jobs="$evidence_dir/simultaneous-amd64-jobs.raw.json"
raw_grants="$evidence_dir/simultaneous-arm64-grants.raw.json"
native_overlap_status="$evidence_dir/simultaneous-arm64-status.json"
overlap_deadline=$((SECONDS + 600))
while true; do
  jobs_before="$evidence_dir/jobs-before.tmp"
  jobs_after="$evidence_dir/jobs-after.tmp"
  kubectl --kubeconfig "$kubeconfig" get jobs --all-namespaces \
    -l loom.dev/platform=amd64 -o json | jq -cS '[.items[] |
    select(.status.active == 1) | {candidate:.metadata.labels["loom.dev/candidate"],
    name:.metadata.name,namespace:.metadata.namespace,
    runtime_class:.spec.template.spec.runtimeClassName,uid:.metadata.uid}] |
    sort_by(.candidate)' > "$jobs_before"
  if jq -e 'length == 2 and all(.[];
      .runtime_class == "loom-personal-dev-builder")' "$jobs_before" >/dev/null; then
    read_count "SELECT coalesce(jsonb_agg(jsonb_build_object(
      'candidate',left(c.candidate_sha,12),'grant_id',g.id,'platform',g.platform,
      'provider',g.provider,'state',g.state) ORDER BY left(c.candidate_sha,12))::text,'[]')
      FROM personal_dev_native_build_grants g JOIN personal_dev_candidates c
      ON c.id=g.candidate_id WHERE g.state='running'" | jq -cS . > "$raw_grants"
    kubectl --kubeconfig "$kubeconfig" get jobs --all-namespaces \
      -l loom.dev/platform=amd64 -o json | jq -cS '[.items[] |
      select(.status.active == 1) | {candidate:.metadata.labels["loom.dev/candidate"],
      name:.metadata.name,namespace:.metadata.namespace,
      runtime_class:.spec.template.spec.runtimeClassName,uid:.metadata.uid}] |
      sort_by(.candidate)' > "$jobs_after"
    if jq -e 'length == 2 and all(.[]; .platform == "linux/arm64" and
        .provider == "gb10-gvisor-docker-v1" and .state == "running")' \
        "$raw_grants" >/dev/null && cmp -s "$jobs_before" "$jobs_after"; then
      mv "$jobs_after" "$raw_jobs" && rm -f "$jobs_before"
      break
    fi
  fi
  rm -f "$jobs_before" "$jobs_after" "$raw_grants"
  test "$SECONDS" -lt "$overlap_deadline"
  sleep 1
done
native_overlap_status_request_id="$(new_native_authority_request_id)"
native_authority_request status "$native_overlap_status_request_id" \
  "$native_overlap_status"
jq -e '.phase == "active" and .agent_service == "active" and
  .dockerd_service == "active" and .nft_table == "present" and
  .managed_containers == 4 and .managed_networks == 2' \
  "$native_overlap_status" >/dev/null
chmod 0600 "$raw_jobs" "$raw_grants"
owner_0_rc=0; owner_1_rc=0
wait "$owner_0_deploy_pid" || owner_0_rc=$?
wait "$owner_1_deploy_pid" || owner_1_rc=$?
test "$owner_0_rc" -eq 0 && test "$owner_1_rc" -eq 0
chmod 0600 "$evidence_dir/owner-0.deploy-v1.txt" "$evidence_dir/owner-1.deploy-v1.txt"

owner_0_initial="$evidence_dir/owner-0.initial.json"
owner_1_initial="$evidence_dir/owner-1.initial.json"
XDG_CONFIG_HOME="$owner_0_xdg" loom_cli dev status "$owner_0_name" --format json | jq -cS . > "$owner_0_initial"
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli dev status "$owner_1_name" --format json | jq -cS . > "$owner_1_initial"
chmod 0600 "$owner_0_initial" "$owner_1_initial"
for status in "$owner_0_initial" "$owner_1_initial"; do
  jq -e '.status=="ready" and .application_status=="ready" and
    .capacity_prepared==true and .min_slots==0 and .max_slots==2 and
    .worker_available==false' "$status" >/dev/null
done
owner_0_initial_candidate="$(jq -er .candidate_sha "$owner_0_initial")"
owner_1_initial_candidate="$(jq -er .candidate_sha "$owner_1_initial")"
test "$owner_0_initial_candidate" != "$owner_1_initial_candidate"
simultaneous_jobs="$evidence_dir/simultaneous-amd64-jobs.json"
simultaneous_grants="$evidence_dir/simultaneous-arm64-grants.json"
simultaneous_containers="$evidence_dir/simultaneous-arm64-containers.json"
jq -cS --arg owner0 "${owner_0_initial_candidate:0:12}" \
  --arg owner1 "${owner_1_initial_candidate:0:12}" '
  [$owner0,$owner1] as $order | [$order[] as $candidate | .[] |
  select(.candidate==$candidate)]' "$raw_jobs" > "$simultaneous_jobs"
jq -cS --arg owner0 "${owner_0_initial_candidate:0:12}" \
  --arg owner1 "${owner_1_initial_candidate:0:12}" '
  [$owner0,$owner1] as $order | [$order[] as $candidate | .[] |
  select(.candidate==$candidate)]' "$raw_grants" > "$simultaneous_grants"
initial_native_runtime="$evidence_dir/initial-native-runtime-evidence.jsonl"
: > "$initial_native_runtime"
for candidate in "$owner_0_initial_candidate" "$owner_1_initial_candidate"; do
  read_count "SELECT jsonb_build_object('grant_id',g.id,
    'evidence',g.runtime_evidence_json)::text FROM personal_dev_native_build_grants g
    JOIN personal_dev_candidates c ON c.id=g.candidate_id
    WHERE c.candidate_sha='$candidate' AND g.state='succeeded'" \
    | jq -cS . >> "$initial_native_runtime"
done
jq -cS --slurpfile grants "$simultaneous_grants" -s '
  . as $evidence | [$grants[0][] as $grant |
    ($evidence[] | select(.grant_id == $grant.grant_id)) as $runtime |
    {grant_id:$grant.grant_id,id:$runtime.evidence.buildkit_container_id,
     image:($runtime.evidence.builder_image|split("@")[1]),
     platform:$runtime.evidence.platform,role:"buildkit",
     runtime_name:$runtime.evidence.runtime_name},
    {grant_id:$grant.grant_id,id:$runtime.evidence.client_container_id,
     image:($runtime.evidence.builder_image|split("@")[1]),
     platform:$runtime.evidence.platform,role:"client",
     runtime_name:$runtime.evidence.runtime_name}]' \
  "$initial_native_runtime" > "$simultaneous_containers"
jq -e 'length==2' "$simultaneous_jobs" >/dev/null
jq -e 'length==2' "$simultaneous_grants" >/dev/null
jq -e 'length==4 and
  all(.[]; .platform=="linux/arm64" and .runtime_name=="runsc-personal-dev-native") and
  ([.[].id]|unique|length==4)' "$simultaneous_containers" >/dev/null
chmod 0600 "$initial_native_runtime" "$simultaneous_jobs" "$simultaneous_grants" \
  "$simultaneous_containers"
acceptance_status "$evidence_dir/after-initial.status.json"
```

## 6. Update both owners and prove native publication

```bash
( XDG_CONFIG_HOME="$owner_0_xdg" loom_cli service up \
    --environment "dev-$owner_0_name" --source-root "$owner_0_update_source" \
    --min-slots 0 --max-slots 3 ) > "$evidence_dir/owner-0.deploy-v2.txt" 2>&1 &
owner_0_update_pid=$!
( XDG_CONFIG_HOME="$owner_1_xdg" loom_cli service up \
    --environment "dev-$owner_1_name" --source-root "$owner_1_update_source" \
    --min-slots 0 --max-slots 4 ) > "$evidence_dir/owner-1.deploy-v2.txt" 2>&1 &
owner_1_update_pid=$!
owner_0_update_rc=0; owner_1_update_rc=0
wait "$owner_0_update_pid" || owner_0_update_rc=$?
wait "$owner_1_update_pid" || owner_1_update_rc=$?
test "$owner_0_update_rc" -eq 0 && test "$owner_1_update_rc" -eq 0
chmod 0600 "$evidence_dir/owner-0.deploy-v2.txt" "$evidence_dir/owner-1.deploy-v2.txt"
owner_0_updated="$evidence_dir/owner-0.updated.json"
owner_1_updated="$evidence_dir/owner-1.updated.json"
XDG_CONFIG_HOME="$owner_0_xdg" loom_cli dev status "$owner_0_name" --format json | jq -cS . > "$owner_0_updated"
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli dev status "$owner_1_name" --format json | jq -cS . > "$owner_1_updated"
chmod 0600 "$owner_0_updated" "$owner_1_updated"
jq -e '.status=="ready" and .min_slots==0 and .max_slots==3 and .worker_available==false' "$owner_0_updated" >/dev/null
jq -e '.status=="ready" and .min_slots==0 and .max_slots==4 and .worker_available==false' "$owner_1_updated" >/dev/null
owner_0_candidate="$(jq -er .candidate_sha "$owner_0_updated")"
owner_1_candidate="$(jq -er .candidate_sha "$owner_1_updated")"
[[ "$owner_0_candidate" =~ ^[0-9a-f]{64}$ ]]
[[ "$owner_1_candidate" =~ ^[0-9a-f]{64}$ ]]
test "$owner_0_candidate" != "$owner_0_initial_candidate"
test "$owner_1_candidate" != "$owner_1_initial_candidate"
test "$owner_0_candidate" != "$owner_1_candidate"
acceptance_status "$evidence_dir/after-updates.status.json"

candidate_publications="$evidence_dir/candidate-publications.jsonl"
native_runtime="$evidence_dir/native-runtime-evidence.jsonl"
: > "$candidate_publications"; : > "$native_runtime"
for candidate in "$owner_0_candidate" "$owner_1_candidate"; do
  read_count "SELECT jsonb_build_object('archive_sha256',archive_sha256,
    'candidate_sha',candidate_sha,'image_manifest_digest',image_manifest_digest,
    'publication',publication_json)::text FROM personal_dev_candidates
    WHERE candidate_sha='$candidate'" | jq -cS . >> "$candidate_publications"
  read_count "SELECT jsonb_build_object('candidate_sha',c.candidate_sha,
    'evidence',g.runtime_evidence_json)::text FROM personal_dev_native_build_grants g
    JOIN personal_dev_candidates c ON c.id=g.candidate_id
    WHERE c.candidate_sha='$candidate' AND g.state='succeeded'" | jq -cS '
    {buildkit_container_id:.evidence.buildkit_container_id,
     buildkit_running:.evidence.buildkit_running,candidate_sha:.candidate_sha,
     client_container_id:.evidence.client_container_id,
     client_exit_code:.evidence.client_exit_code,
     client_oom_killed:.evidence.client_oom_killed,emulated:false,
     fallback_used:false,platform:.evidence.platform,provider:.evidence.provider,
     runtime_name:.evidence.runtime_name}' >> "$native_runtime"
done
chmod 0600 "$candidate_publications" "$native_runtime"
test "$(wc -l < "$candidate_publications")" = 2
test "$(wc -l < "$native_runtime")" = 2
test "$(jq -r .archive_sha256 "$candidate_publications" | sort -u | wc -l)" = 2
jq -e '.platform=="linux/arm64" and .provider=="gb10-gvisor-docker-v1" and
  .runtime_name=="runsc-personal-dev-native" and
  .client_container_id!=.buildkit_container_id and .client_exit_code==0 and
  .client_oom_killed==false and .buildkit_running==true and
  .emulated==false and .fallback_used==false' "$native_runtime" >/dev/null
test "$(jq -s '[.[]|.buildkit_container_id,.client_container_id]|unique|length' "$native_runtime")" = 4
test "$(jq -s --slurpfile overlap "$simultaneous_containers" '
  ([.[]|.buildkit_container_id,.client_container_id]-[$overlap[0][].id])|length' \
  "$native_runtime")" = 4

native_indexes="$evidence_dir/native-indexes.jsonl"
: > "$native_indexes"
for candidate in "$owner_0_candidate" "$owner_1_candidate"; do
  for component in service web; do
    reference="$(jq -er --arg candidate "$candidate" --arg component "$component" '
      select(.candidate_sha==$candidate)|.publication.images[$component].index' \
      "$candidate_publications")"
    [[ "$reference" =~ ^ghcr\.io/qianyi-sun/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$ ]]
    inspect="$evidence_dir/index-$(printf '%s' "$reference" | sha256sum | awk '{print $1}').json"
    docker buildx imagetools inspect --raw "$reference" > "$inspect"
    chmod 0600 "$inspect"
    manifest_sha256="$(sha256sum "$inspect" | awk '{print $1}')"
    test "$reference" = "${reference%@sha256:*}@sha256:$manifest_sha256"
    jq -e '.mediaType=="application/vnd.oci.image.index.v1+json" and
      ([.manifests[].platform|(.os+"/"+.architecture)]|sort)==
      ["linux/amd64","linux/arm64"]' "$inspect" >/dev/null
    jq -cS -n --arg candidate_sha "$candidate" --arg component "$component" \
      --arg manifest_sha256 "$manifest_sha256" --arg reference "$reference" '
      {candidate_sha:$candidate_sha,component:$component,
       manifest_sha256:$manifest_sha256,
       platforms:["linux/amd64","linux/arm64"],reference:$reference}' \
      >> "$native_indexes"
  done
done
chmod 0600 "$native_indexes"
test "$(wc -l < "$native_indexes")" = 4
```

The OLDLAB Job specs bind RuntimeClass `loom-personal-dev-builder`; the signed
GB10 completions bind `runsc-personal-dev-native`. `docker buildx imagetools
inspect --raw` preserves the exact digest-bearing index bytes.

## 7. Prove routes and all six cross-owner denials

```bash
for field in subject_id subject_incarnation identity.environment identity.namespace \
  identity.database identity.task_bucket identity.trajectories_bucket \
  identity.artifacts_bucket identity.route_host identity.worker_control_plane_host \
  identity.worker_gateway_host identity.route_path identity.worker_pool; do
  test "$(jq -er ".$field" "$owner_0_updated")" != "$(jq -er ".$field" "$owner_1_updated")"
done
for status in "$owner_0_updated" "$owner_1_updated"; do
  route_host="$(jq -er .identity.route_host "$status")"
  [[ "$route_host" =~ ^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$ ]]
  route_output="$evidence_dir/route-$route_host.json"
  curl --fail --silent --show-error --proto '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 30 "https://$route_host/api/v1/health" \
    | jq -cS . > "$route_output"
  jq -e '.status=="ok"' "$route_output" >/dev/null
  chmod 0600 "$route_output"
done

denials_jsonl="$evidence_dir/cross-owner-denials.jsonl"
: > "$denials_jsonl"; chmod 0600 "$denials_jsonl"
probe_cross_owner_denial() {
  local actor_xdg="$1" actor_candidate="$2" target_xdg="$3" target_name="$4"
  local actor_index="$5" target_index="$6" operation="$7"
  local prefix="$evidence_dir/denial-$actor_index-$target_index-$operation"
  local before="$prefix.before.json" after="$prefix.after.json"
  local stdout="$prefix.stdout" stderr="$prefix.stderr" expected="$prefix.expected"
  local expected_receipt target_epoch rc=0
  XDG_CONFIG_HOME="$target_xdg" loom_cli dev status "$target_name" --format json | jq -cS . > "$before"
  target_epoch="$(jq -er '.operation_epoch|select(type=="number" and .>0)' "$before")"
  case "$operation" in
    read)
      expected_receipt='{"error_code":"resource_hidden","http_method":"GET","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_read"}'
      XDG_CONFIG_HOME="$actor_xdg" loom_cli dev status "$target_name" \
        --format json --expected-hidden-denial > "$stdout" 2> "$stderr" || rc=$? ;;
    update)
      expected_receipt='{"error_code":"resource_hidden","http_method":"PUT","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_update"}'
      XDG_CONFIG_HOME="$actor_xdg" loom_cli service up \
        --environment "dev-$target_name" --candidate "$actor_candidate" \
        --expected-operation-epoch 1 --min-slots 0 --quiet \
        --expected-hidden-denial > "$stdout" 2> "$stderr" || rc=$? ;;
    destroy)
      expected_receipt='{"error_code":"resource_hidden","http_method":"DELETE","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_destroy"}'
      XDG_CONFIG_HOME="$actor_xdg" loom_cli dev destroy "$target_name" \
        --format json --expected-operation-epoch "$target_epoch" \
        --expected-hidden-denial > "$stdout" 2> "$stderr" || rc=$? ;;
    *) return 2 ;;
  esac
  test "$rc" -eq 1 && test ! -s "$stdout"
  printf '%s\n' "$expected_receipt" > "$expected"
  cmp -s "$stderr" "$expected" && rm -f "$expected"
  XDG_CONFIG_HOME="$target_xdg" loom_cli dev status "$target_name" --format json | jq -cS . > "$after"
  cmp -s "$before" "$after"; chmod 0600 "$before" "$after" "$stdout" "$stderr"
  jq -cS -n \
    --arg actor_team_id "$(jq -r ".acceptance_owners[$actor_index].team_id" "$acceptance_plan")" \
    --arg actor_user_id "$(jq -r ".acceptance_owners[$actor_index].user_id" "$acceptance_plan")" \
    --arg operation "$operation" --arg stderr_sha256 "$(sha256sum "$stderr"|awk '{print $1}')" \
    --arg stdout_sha256 "$(sha256sum "$stdout"|awk '{print $1}')" \
    --arg target_after_sha256 "$(sha256sum "$after"|awk '{print $1}')" \
    --arg target_before_sha256 "$(sha256sum "$before"|awk '{print $1}')" \
    --arg target_environment "$target_name" \
    --arg target_team_id "$(jq -r ".acceptance_owners[$target_index].team_id" "$acceptance_plan")" \
    --arg target_user_id "$(jq -r ".acceptance_owners[$target_index].user_id" "$acceptance_plan")" '
    {actor_team_id:$actor_team_id,actor_user_id:$actor_user_id,exit_code:1,
     operation:$operation,stderr_sha256:$stderr_sha256,stdout_sha256:$stdout_sha256,
     target_after_sha256:$target_after_sha256,target_before_sha256:$target_before_sha256,
     target_environment:$target_environment,target_team_id:$target_team_id,
     target_user_id:$target_user_id}' >> "$denials_jsonl"
}
probe_cross_owner_denial "$owner_0_xdg" "$owner_0_candidate" "$owner_1_xdg" "$owner_1_name" 0 1 read
probe_cross_owner_denial "$owner_0_xdg" "$owner_0_candidate" "$owner_1_xdg" "$owner_1_name" 0 1 update
probe_cross_owner_denial "$owner_0_xdg" "$owner_0_candidate" "$owner_1_xdg" "$owner_1_name" 0 1 destroy
probe_cross_owner_denial "$owner_1_xdg" "$owner_1_candidate" "$owner_0_xdg" "$owner_0_name" 1 0 read
probe_cross_owner_denial "$owner_1_xdg" "$owner_1_candidate" "$owner_0_xdg" "$owner_0_name" 1 0 update
probe_cross_owner_denial "$owner_1_xdg" "$owner_1_candidate" "$owner_0_xdg" "$owner_0_name" 1 0 destroy
test "$(wc -l < "$denials_jsonl")" = 6
acceptance_status "$evidence_dir/after-denials.status.json"
```

## 8. Complete authenticated owner cleanup

Owner 0 deletes normally. Owner 1 proves retained-data deletion, same-subject
redeploy with a new incarnation, and final normal deletion.

```bash
owner_0_destroyed="$evidence_dir/owner-0.destroyed.json"
owner_1_destroyed="$evidence_dir/owner-1.destroyed.json"
owner_1_redeployed="$evidence_dir/owner-1.redeployed.json"
owner_1_final_destroyed="$evidence_dir/owner-1.final-destroyed.json"
retained_subject_id="$(jq -er .subject_id "$owner_1_updated")"
retained_incarnation="$(jq -er .subject_incarnation "$owner_1_updated")"
XDG_CONFIG_HOME="$owner_0_xdg" loom_cli dev destroy "$owner_0_name" \
  --format json | jq -cS . > "$owner_0_destroyed"
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli dev destroy "$owner_1_name" \
  --keep-data --format json | jq -cS . > "$owner_1_destroyed"
chmod 0600 "$owner_0_destroyed" "$owner_1_destroyed"
jq -e '.status=="deleted" and .keep_data==false' "$owner_0_destroyed" >/dev/null
jq -e '.status=="deleted" and .keep_data==true' "$owner_1_destroyed" >/dev/null
owner_1_redeploy_epoch="$(jq -er '.operation_epoch|select(type=="number" and .>0)' "$owner_1_destroyed")"
acceptance_status "$evidence_dir/after-destroy.status.json"
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli service up \
  --environment "dev-$owner_1_name" --source-root "$owner_1_update_source" \
  --expected-operation-epoch "$owner_1_redeploy_epoch" --min-slots 0 --max-slots 2 \
  > "$evidence_dir/owner-1.redeploy.txt" 2>&1
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli dev status "$owner_1_name" --format json | jq -cS . > "$owner_1_redeployed"
chmod 0600 "$evidence_dir/owner-1.redeploy.txt" "$owner_1_redeployed"
jq -e --arg subject "$retained_subject_id" --arg incarnation "$retained_incarnation" '
  .status=="ready" and .subject_id==$subject and .subject_incarnation!=$incarnation and
  .deployment_generation==1 and .worker_available==false and .min_slots==0 and
  .max_slots==2' "$owner_1_redeployed" >/dev/null
acceptance_status "$evidence_dir/after-redeploy.status.json"
XDG_CONFIG_HOME="$owner_1_xdg" loom_cli dev destroy "$owner_1_name" \
  --format json | jq -cS . > "$owner_1_final_destroyed"
chmod 0600 "$owner_1_final_destroyed"
jq -e '.status=="deleted" and .keep_data==false' "$owner_1_final_destroyed" >/dev/null

cleanup_deadline=$((SECONDS + 300))
while true; do
  capture_counts "$evidence_dir/final-zero-grants.json"
  capture_namespaces "$evidence_dir/final-zero-namespaces.json"
  if jq -e '.active_native_grants==0' "$evidence_dir/final-zero-grants.json" >/dev/null &&
    jq -e '.==[]' "$evidence_dir/final-zero-namespaces.json" >/dev/null; then break; fi
  test "$SECONDS" -lt "$cleanup_deadline"; sleep 2
done
jq -cS '{tasks}' "$evidence_dir/final-zero-grants.json" > "$evidence_dir/final-zero-tasks.json"
jq -cS '{workers}' "$evidence_dir/final-zero-grants.json" > "$evidence_dir/final-zero-workers.json"
jq -e --slurpfile before "$evidence_dir/before-database-counts.json" '
  .tasks==$before[0].tasks and .workers==$before[0].workers and
  .active_native_grants==0' "$evidence_dir/final-zero-grants.json" >/dev/null
capture_slurm "$evidence_dir/after-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/after-slurm.json"
loom_cli admin capacity-control-plane status --namespace loom-dev \
  --kubeconfig "$kubeconfig" > "$evidence_dir/final-capacity.status.json"
jq -e '.=={executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/final-capacity.status.json" >/dev/null
chmod 0600 "$evidence_dir/final-zero-"*.json "$evidence_dir/final-capacity.status.json"
acceptance_status "$evidence_dir/pre-rollback.status.json"
```

## 9. Reapply and verify the exact inert shadow

```bash
shadow_recheck_after="$evidence_dir/shadow-recheck-after.yaml"
loom_cli admin personal-dev-control-plane render --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$shadow_recheck_after" 2> "$evidence_dir/shadow-recheck-after.render.json"
chmod 0600 "$shadow_recheck_after" "$evidence_dir/shadow-recheck-after.render.json"
cmp -s "$shadow_recheck_after" "$rollback_shadow_manifest"
rollback_diff_rc=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane -f "$rollback_shadow_manifest" \
  > "$evidence_dir/rollback.server-side-diff.txt" 2>&1 || rollback_diff_rc=$?
test "$rollback_diff_rc" -eq 0 || test "$rollback_diff_rc" -eq 1
chmod 0600 "$evidence_dir/rollback.server-side-diff.txt"
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane -f "$rollback_shadow_manifest" \
  > "$evidence_dir/rollback.server-side-apply.txt"
chmod 0600 "$evidence_dir/rollback.server-side-apply.txt"
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
rollback_status_raw="$evidence_dir/rollback-shadow.status.raw.json"
rollback_status="$evidence_dir/rollback-shadow.status.json"
loom_cli admin personal-dev-control-plane status --namespace loom-dev \
  --kubeconfig "$kubeconfig" --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" > "$rollback_status_raw"
jq -cS -j . "$rollback_status_raw" > "$rollback_status"
rm -f "$rollback_status_raw"
chmod 0600 "$rollback_status"; assert_canonical_json "$rollback_status"
jq -e '.schema=="loom-personal-dev-control-plane-status-v1" and .mode=="shadow" and
  .ready==true and .blockers==[] and .manager_ceiling==0 and
  .worker_available==false and all(.components[];.ready==true)' "$rollback_status" >/dev/null
rollback_shadow_status_sha256="$(sha256sum "$rollback_status" | awk '{print $1}')"
capture_counts "$evidence_dir/inert-database-counts.json"
capture_namespaces "$evidence_dir/inert-namespaces.json"
jq -e '.active_native_grants==0' "$evidence_dir/inert-database-counts.json" >/dev/null
jq -e '.==[]' "$evidence_dir/inert-namespaces.json" >/dev/null
```

## 10. Seal and verify canonical schema-v3 evidence

`jq -cS -j` is mandatory: canonical JSON has no trailing newline.

```bash
project_snapshot() {
  jq -cS -j '{application_status,candidate_sha,capacity_prepared,capacity_status,
    deployment_generation,identity:{environment:.identity.environment,
    namespace:.identity.namespace,database:.identity.database,
    task_bucket:.identity.task_bucket,trajectories_bucket:.identity.trajectories_bucket,
    artifacts_bucket:.identity.artifacts_bucket,route_host:.identity.route_host,
    worker_control_plane_host:.identity.worker_control_plane_host,
    worker_gateway_host:.identity.worker_gateway_host,route_path:.identity.route_path,
    worker_pool:.identity.worker_pool},keep_data,max_slots,min_slots,name,
    operation_epoch,owner_team_id,owner_user_id,status,subject_id,
    subject_incarnation,worker_available}' "$1" > "$2"
  chmod 0600 "$2"; assert_canonical_json "$2"
}
owner_0_initial_selected="$evidence_dir/owner-0.initial.selected.json"
owner_0_updated_selected="$evidence_dir/owner-0.updated.selected.json"
owner_0_destroyed_selected="$evidence_dir/owner-0.destroyed.selected.json"
owner_1_initial_selected="$evidence_dir/owner-1.initial.selected.json"
owner_1_updated_selected="$evidence_dir/owner-1.updated.selected.json"
owner_1_destroyed_selected="$evidence_dir/owner-1.destroyed.selected.json"
owner_1_redeployed_selected="$evidence_dir/owner-1.redeployed.selected.json"
owner_1_final_destroyed_selected="$evidence_dir/owner-1.final-destroyed.selected.json"
project_snapshot "$owner_0_initial" "$owner_0_initial_selected"
project_snapshot "$owner_0_updated" "$owner_0_updated_selected"
project_snapshot "$owner_0_destroyed" "$owner_0_destroyed_selected"
project_snapshot "$owner_1_initial" "$owner_1_initial_selected"
project_snapshot "$owner_1_updated" "$owner_1_updated_selected"
project_snapshot "$owner_1_destroyed" "$owner_1_destroyed_selected"
project_snapshot "$owner_1_redeployed" "$owner_1_redeployed_selected"
project_snapshot "$owner_1_final_destroyed" "$owner_1_final_destroyed_selected"

native_zero_capacity="$evidence_dir/native-zero-capacity.json"
jq -cS -j -n \
  --argjson tasks_before "$(jq -er .tasks "$evidence_dir/before-database-counts.json")" \
  --argjson tasks_after "$(jq -er .tasks "$evidence_dir/final-zero-grants.json")" \
  --argjson workers_before "$(jq -er .workers "$evidence_dir/before-database-counts.json")" \
  --argjson workers_after "$(jq -er .workers "$evidence_dir/final-zero-grants.json")" '
  {active_native_grants:0,dynamic_namespace_count:0,
  executable_new_capacity_ceiling:0,loom_slurm_jobs_after:0,
  loom_slurm_jobs_before:0,tasks_after:$tasks_after,tasks_before:$tasks_before,
  worker_available:false,workers_after:$workers_after,workers_before:$workers_before}' \
  > "$native_zero_capacity"
chmod 0600 "$native_zero_capacity"

acceptance_result="$evidence_dir/acceptance-result-v3.json"
jq -cS -j -n \
  --arg acceptance_manifest_sha256 "$acceptance_manifest_sha256" \
  --arg acceptance_plan_sha256 "$acceptance_plan_sha256" \
  --arg after_denials "$(sha256sum "$evidence_dir/after-denials.status.json"|awk '{print $1}')" \
  --arg after_destroy "$(sha256sum "$evidence_dir/after-destroy.status.json"|awk '{print $1}')" \
  --arg after_initial "$(sha256sum "$evidence_dir/after-initial.status.json"|awk '{print $1}')" \
  --arg after_redeploy "$(sha256sum "$evidence_dir/after-redeploy.status.json"|awk '{print $1}')" \
  --arg after_updates "$(sha256sum "$evidence_dir/after-updates.status.json"|awk '{print $1}')" \
  --arg pre_deploy "$(sha256sum "$evidence_dir/pre-deploy.status.json"|awk '{print $1}')" \
  --arg pre_rollback "$(sha256sum "$evidence_dir/pre-rollback.status.json"|awk '{print $1}')" \
  --arg release_sha256 "$trusted_release_sha256" \
  --arg rollback_shadow "$rollback_shadow_status_sha256" \
  --arg shadow_manifest_sha256 "$rollback_shadow_sha256" \
  --arg after_slurm "$(sha256sum "$evidence_dir/after-slurm.json"|awk '{print $1}')" \
  --arg before_slurm "$(sha256sum "$evidence_dir/before-slurm.json"|awk '{print $1}')" \
  --arg candidate_publications "$(sha256sum "$candidate_publications"|awk '{print $1}')" \
  --arg final_capacity "$(sha256sum "$evidence_dir/final-capacity.status.json"|awk '{print $1}')" \
  --arg final_zero_grants "$(sha256sum "$evidence_dir/final-zero-grants.json"|awk '{print $1}')" \
  --arg final_zero_namespaces "$(sha256sum "$evidence_dir/final-zero-namespaces.json"|awk '{print $1}')" \
  --arg final_zero_tasks "$(sha256sum "$evidence_dir/final-zero-tasks.json"|awk '{print $1}')" \
  --arg final_zero_workers "$(sha256sum "$evidence_dir/final-zero-workers.json"|awk '{print $1}')" \
  --arg native_runtime_sha256 "$(sha256sum "$native_runtime"|awk '{print $1}')" \
  --arg simultaneous_containers "$(sha256sum "$simultaneous_containers"|awk '{print $1}')" \
  --arg simultaneous_grants "$(sha256sum "$simultaneous_grants"|awk '{print $1}')" \
  --arg simultaneous_jobs "$(sha256sum "$simultaneous_jobs"|awk '{print $1}')" \
  --slurpfile denials "$denials_jsonl" --slurpfile completions "$native_runtime" \
  --slurpfile indexes "$native_indexes" --slurpfile containers "$simultaneous_containers" \
  --slurpfile grants "$simultaneous_grants" --slurpfile jobs "$simultaneous_jobs" \
  --slurpfile o0d "$owner_0_destroyed_selected" --slurpfile o0i "$owner_0_initial_selected" \
  --slurpfile o0u "$owner_0_updated_selected" --slurpfile o1d "$owner_1_destroyed_selected" \
  --slurpfile o1f "$owner_1_final_destroyed_selected" --slurpfile o1i "$owner_1_initial_selected" \
  --slurpfile o1r "$owner_1_redeployed_selected" --slurpfile o1u "$owner_1_updated_selected" \
  --slurpfile zero "$native_zero_capacity" '
  {acceptance_manifest_sha256:$acceptance_manifest_sha256,
  acceptance_plan_sha256:$acceptance_plan_sha256,cross_owner_denials:$denials,
  native:{completions:$completions,evidence_sha256s:{after_slurm:$after_slurm,
  before_slurm:$before_slurm,candidate_publications:$candidate_publications,
  final_capacity:$final_capacity,final_zero_grants:$final_zero_grants,
  final_zero_namespaces:$final_zero_namespaces,final_zero_tasks:$final_zero_tasks,
  final_zero_workers:$final_zero_workers,native_runtime:$native_runtime_sha256,
  simultaneous_containers:$simultaneous_containers,
  simultaneous_grants:$simultaneous_grants,simultaneous_jobs:$simultaneous_jobs},
  indexes:$indexes,overlap:{amd64_jobs:$jobs[0],arm64_containers:$containers[0],
  arm64_grants:$grants[0]},zero_capacity:$zero[0]},
  owners:[{destroyed:$o0d[0],final_destroyed:null,initial:$o0i[0],
  redeployed:null,updated:$o0u[0]},{destroyed:$o1d[0],final_destroyed:$o1f[0],
  initial:$o1i[0],redeployed:$o1r[0],updated:$o1u[0]}],
  release_sha256:$release_sha256,
  schema:"loom-personal-dev-zero-capacity-acceptance-result-v3",
  shadow_manifest_sha256:$shadow_manifest_sha256,
  status_sha256s:{after_denials:$after_denials,after_destroy:$after_destroy,
  after_initial:$after_initial,after_redeploy:$after_redeploy,
  after_updates:$after_updates,pre_deploy:$pre_deploy,
  pre_rollback:$pre_rollback,rollback_shadow:$rollback_shadow}}' > "$acceptance_result"
chmod 0600 "$acceptance_result"; assert_canonical_json "$acceptance_result"
acceptance_result_sha256="$(sha256sum "$acceptance_result"|awk '{print $1}')"
acceptance_verification="$evidence_dir/acceptance-result-verification.json"
loom_cli admin personal-dev-control-plane verify-acceptance-result \
  --acceptance-plan-file "$acceptance_plan" \
  --acceptance-plan-sha256 "$acceptance_plan_sha256" \
  --acceptance-result-file "$acceptance_result" \
  --acceptance-result-sha256 "$acceptance_result_sha256" \
  --acceptance-manifest-sha256 "$acceptance_manifest_sha256" \
  --rollback-shadow-manifest-file "$rollback_shadow_manifest" \
  --rollback-shadow-status-file "$rollback_status" > "$acceptance_verification"
chmod 0600 "$acceptance_verification"
assert_canonical_json_line "$acceptance_verification"
jq -e --arg result "$acceptance_result_sha256" --arg rollback "$rollback_shadow_status_sha256" '
  .schema=="loom-personal-dev-zero-capacity-acceptance-verification-v1" and
  .verified==true and .native == true and .owner_count==2 and
  .cross_owner_denial_count==6 and .acceptance_result_sha256==$result and
  .rollback_shadow_status_sha256==$rollback' "$acceptance_verification" >/dev/null
```

## 11. Derive and apply durable operational authority

Only the verified result may authorize this plan. Rendering is the strict
load/validation gate; review the server-side diff before apply.

```bash
operational_plan="$evidence_dir/native-operational-plan.json"
approved_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -cS -j --arg acceptance_result_sha256 "$acceptance_result_sha256" \
  --arg approved_at "$approved_at" \
  --arg rollback_evidence_sha256 "$rollback_shadow_status_sha256" '
  del(.acceptance_owners,.window)|.schema_version=2|
  .approval={acceptance_result_sha256:$acceptance_result_sha256,
  approved_at:$approved_at,rollback_evidence_sha256:$rollback_evidence_sha256}' \
  "$acceptance_plan" > "$operational_plan"
chmod 0600 "$operational_plan"; assert_canonical_json "$operational_plan"
operational_plan_sha256="$(sha256sum "$operational_plan"|awk '{print $1}')"
test "$(jq -er .schema_version "$operational_plan")" = 2
test "$(jq -er .approval.acceptance_result_sha256 "$operational_plan")" = "$acceptance_result_sha256"
test "$(jq -er .approval.rollback_evidence_sha256 "$operational_plan")" = "$rollback_shadow_status_sha256"
operational_manifest="$evidence_dir/native-operational.rendered.yaml"
loom_cli admin personal-dev-control-plane render-operational \
  --file "$profile" --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${bound_evidence_args[@]}" > "$operational_manifest" \
  2> "$evidence_dir/native-operational.render.json"
chmod 0600 "$operational_manifest" "$evidence_dir/native-operational.render.json"
operational_diff_rc=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane -f "$operational_manifest" \
  > "$evidence_dir/native-operational.server-side-diff.txt" 2>&1 || operational_diff_rc=$?
test "$operational_diff_rc" -eq 0 || test "$operational_diff_rc" -eq 1
chmod 0600 "$evidence_dir/native-operational.server-side-diff.txt"
capture_counts "$evidence_dir/pre-operational-apply.counts.json"
jq -e '.active_native_grants==0' "$evidence_dir/pre-operational-apply.counts.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane -f "$operational_manifest" \
  > "$evidence_dir/native-operational.server-side-apply.txt"
chmod 0600 "$evidence_dir/native-operational.server-side-apply.txt"
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
loom_cli admin personal-dev-control-plane status-operational \
  --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${bound_evidence_args[@]}" > "$evidence_dir/final-operational.status.json"
chmod 0600 "$evidence_dir/final-operational.status.json"
jq -e '.ready==true and .blockers==[] and .manager_ceiling==0 and
  .worker_available==false and any(.components[];.name=="native-builder" and
  .observed==1 and .ready==true)' "$evidence_dir/final-operational.status.json" >/dev/null
capture_counts "$evidence_dir/final-operational.counts.json"
capture_namespaces "$evidence_dir/final-operational.namespaces.json"
capture_slurm "$evidence_dir/final-operational.slurm.json"
jq -e --slurpfile before "$evidence_dir/before-database-counts.json" '
  .active_native_grants==0 and .tasks==$before[0].tasks and
  .workers==$before[0].workers' "$evidence_dir/final-operational.counts.json" >/dev/null
jq -e '.==[]' "$evidence_dir/final-operational.namespaces.json" >/dev/null
assert_no_loom_slurm_jobs "$evidence_dir/final-operational.slurm.json"
```

This restores the exact operational state only after acceptance is proven.

## 12. Seal sanitized evidence

```bash
(
  cd "$evidence_dir"
  find . -maxdepth 1 -type f ! -name kubeconfig \
    ! -name evidence-index.sha256 -printf '%P\n' | LC_ALL=C sort \
    | while IFS= read -r file; do sha256sum "$file"; done \
    > evidence-index.sha256
  chmod 0600 evidence-index.sha256
)
```

Secret values are never part of `evidence-index.sha256`.

## Failure rollback

Before an owner request, reapply only the exact reviewed shadow and record
shadow status. After an owner request, first wait for any started CLI process,
then clean up through each owner's authenticated `loom dev destroy`, prove zero
grants and namespaces, and reapply the exact shadow. Never derive an operational
plan after failed acceptance or verification. Follow the runtime runbook to
stop the agent before the dedicated daemon. Never improvise a namespace, grant,
container, image, Slurm, Task, Worker, or capacity mutation.
