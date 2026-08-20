# Personal-development builder runtime rollout

This runbook installs and proves the measured KVM gVisor runtime used by
personal-development builders. It is a protected infrastructure rollout, not a
personal-development deployment. The executable-new-capacity ceiling remains exactly `0`.
Builder preparation and lifecycle flags remain false, and activation replicas
remain zero. There may be no `loom-dev-<owner>` namespace and no `loom-build-*` namespace.

The only eligible Kubernetes nodes are `trt-eai-oldlab-2` through
`trt-eai-oldlab-5`. The control-plane node is never eligible. Roll one agent at
a time in that order. Cordon it, but do not drain it. A failure on one node stops the fleet rollout.
Never force a PodDisruptionBudget, delete a Longhorn resource, or continue on a
partially verified node.

Repository presence does not authorize this procedure. Run it only from the
exact merged, CI-approved commit during the recorded #1280 runtime window.
Retain owner-only evidence and never record kubeconfig bytes, Secret values, or
unbounded command output.

## 1. Bind the source and owner-only evidence

Run every command block in the same Bash session. Replace placeholders before
the window opens. The reviewed kubeconfig must be flattened and self-contained.

```bash
set -euo pipefail
umask 077
test "$(id -u)" != 0

merged_source_sha='<merged-40-lowercase-hex>'
window_id='<approved-issue-1280-runtime-window-id>'
reviewed_kubeconfig='<absolute-mode-0600-reviewed-kubeconfig>'
evidence_root='<absolute-existing-owner-only-evidence-root-outside-repository>'
profile_source=deploy/dev-fleet/personal-dev-builder-runtime-profile.json
runtime_class_source=deploy/dev-fleet/personal-dev-builder-runtime-class.yaml
installer_source=scripts/ops/install_personal_dev_builder_runtime.py
profile_module_source=scripts/ops/personal_dev_builder_runtime_profile.py
profile_sha256=880b7c79013e38b016046c732209574d48d6ae5a008164906f9951ba27765b76
profile_label_a=880b7c79013e38b016046c732209574d
profile_label_b=48d6ae5a008164906f9951ba27765b76
archive_url=https://storage.googleapis.com/gvisor/releases/release/20260810/x86_64/gvisor.tar.bz2
archive_sha512=3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
repository_root="$(pwd -P)"
evidence_dir="$evidence_root/${timestamp}-${merged_source_sha}"

test "$merged_source_sha" != '<merged-40-lowercase-hex>'
test "$window_id" != '<approved-issue-1280-runtime-window-id>'
test "$reviewed_kubeconfig" != '<absolute-mode-0600-reviewed-kubeconfig>'
test "$evidence_root" != \
  '<absolute-existing-owner-only-evidence-root-outside-repository>'
test "$(git rev-parse --show-toplevel)" = "$repository_root"
test "$(git rev-parse HEAD)" = "$merged_source_sha"
worktree_status="$(git status --porcelain=v1 --untracked-files=all)"
test -z "$worktree_status"
[[ "$evidence_root" == /* ]]
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
test "$(realpath -e "$evidence_dir")" = "$evidence_dir"
test "$(stat -c %u "$evidence_dir")" = "$(id -u)"
test "$(stat -c %a "$evidence_dir")" = 700
worktree_status="$(git status --porcelain=v1 --untracked-files=all)"
test -z "$worktree_status"

for source in \
  "$profile_source" \
  "$runtime_class_source" \
  "$installer_source" \
  "$profile_module_source"
do
  test -f "$source"
  test ! -L "$source"
  test "$(stat -c %h "$source")" = 1
done
test "$(sha256sum "$profile_source" | awk '{print $1}')" = "$profile_sha256"
runtime_class_sha256="$(sha256sum "$runtime_class_source" | awk '{print $1}')"
installer_sha256="$(sha256sum "$installer_source" | awk '{print $1}')"
profile_module_sha256="$(sha256sum "$profile_module_source" | awk '{print $1}')"

test -f "$reviewed_kubeconfig"
test ! -L "$reviewed_kubeconfig"
test "$(realpath -e "$reviewed_kubeconfig")" = "$reviewed_kubeconfig"
test "$(stat -c %u "$reviewed_kubeconfig")" = "$(id -u)"
test "$(stat -c %a "$reviewed_kubeconfig")" = 600
test "$(stat -c %h "$reviewed_kubeconfig")" = 1
kubeconfig_source_sha256="$(sha256sum "$reviewed_kubeconfig" | awk '{print $1}')"
kubeconfig="$evidence_dir/kubeconfig"
install -m 0600 "$reviewed_kubeconfig" "$kubeconfig"
test "$(sha256sum "$reviewed_kubeconfig" | awk '{print $1}')" = \
  "$kubeconfig_source_sha256"
test "$(sha256sum "$kubeconfig" | awk '{print $1}')" = \
  "$kubeconfig_source_sha256"

install -m 0600 "$profile_source" "$evidence_dir/runtime-profile.json"
install -m 0600 "$runtime_class_source" "$evidence_dir/runtime-class.yaml"
test "$(sha256sum "$evidence_dir/runtime-class.yaml" | awk '{print $1}')" = \
  "$runtime_class_sha256"
printf '%s\n' "$window_id" > "$evidence_dir/window-id.txt"
printf '%s\n' "$merged_source_sha" > "$evidence_dir/source-sha.txt"
printf '%s\n' "$(git rev-parse HEAD^{tree})" > "$evidence_dir/source-tree.txt"
sha256sum \
  "$evidence_dir/runtime-profile.json" \
  "$evidence_dir/runtime-class.yaml" \
  "$installer_source" \
  "$profile_module_source" \
  > "$evidence_dir/public-inputs.sha256"

PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python - \
  "$evidence_dir/runtime-profile.json" "$profile_sha256" \
  "$evidence_dir/runtime-class.yaml" "$kubeconfig" <<'PY'
import sys
from pathlib import Path

import yaml

from scripts.ops.personal_dev_builder_runtime_profile import (
    load_runtime_profile,
    render_runtime_class,
)
from loom_cli.personal_dev_control_plane_cmd import _load_safe_kubeconfig

profile = load_runtime_profile(Path(sys.argv[1]))
if profile.sha256 != sys.argv[2]:
    raise SystemExit("profile digest mismatch")
with Path(sys.argv[3]).open("rb") as stream:
    runtime_class = yaml.safe_load(stream)
if runtime_class != render_runtime_class(profile):
    raise SystemExit("RuntimeClass does not match the measured profile")
if _load_safe_kubeconfig(Path(sys.argv[4])) is None:
    raise SystemExit("kubeconfig is not owner-only, flattened, and self-contained")
PY
```

Do not display or upload `$kubeconfig`. Only its reviewed hash and inode
properties may appear in sanitized issue evidence.

## 2. Download the exact archive without root

The archive is gVisor `release-20260810.0`, tag commit
`5ceb9a5fd5750d6c73dd166441f28306039300d0`. Download it as the unprivileged
operator. The node-local preflight independently streams and verifies the
archive, its five regular-file members, required `gvisor-bin` directory entry,
modes, sizes, and hashes before installation.

```bash
archive="$evidence_dir/gvisor-release-20260810.0.tar.bz2"
archive_part="$archive.part"
test ! -e "$archive"
test ! -e "$archive_part"
curl --fail --location --silent --show-error \
  --proto '=https' --tlsv1.2 \
  --connect-timeout 10 --max-time 1800 --max-filesize 1073741824 \
  --output "$archive_part" "$archive_url"
chmod 0600 "$archive_part"
test "$(stat -c %u "$archive_part")" = "$(id -u)"
test "$(stat -c %a "$archive_part")" = 600
test "$(stat -c %h "$archive_part")" = 1
test "$(sha512sum "$archive_part" | awk '{print $1}')" = "$archive_sha512"
mv -T "$archive_part" "$archive"
test "$(sha512sum "$archive" | awk '{print $1}')" = "$archive_sha512"
printf '%s  %s\n' "$archive_sha512" "$(basename "$archive")" \
  > "$evidence_dir/archive.sha512"
```

## 3. Define and re-run the global stop conditions

These checks are observation-only. Secret key names are reviewed; Secret
values are never emitted to stdout or evidence. The expected management Secret
inventory is the same exact inventory required by the inert management shadow.

```bash
nodes=(
  trt-eai-oldlab-2
  trt-eai-oldlab-3
  trt-eai-oldlab-4
  trt-eai-oldlab-5
)
declare -A ssh_targets=(
  [trt-eai-oldlab-2]='<ssh-user>@trt-eai-oldlab-2'
  [trt-eai-oldlab-3]='<ssh-user>@trt-eai-oldlab-3'
  [trt-eai-oldlab-4]='<ssh-user>@trt-eai-oldlab-4'
  [trt-eai-oldlab-5]='<ssh-user>@trt-eai-oldlab-5'
)
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)

secret_keys() {
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace loom-dev get secret "$1" \
    -o 'go-template={{range $key, $value := .data}}{{$key}}{{"\n"}}{{end}}' \
    | LC_ALL=C sort
}

assert_secret_key_inventory() {
  local expected_management
  expected_management="$(printf '%s\n' \
    admin-secrets.toml \
    capacity-lifecycle-ca.pem \
    capacity-lifecycle-certificate.pem \
    capacity-lifecycle-private-key.pem \
    capacity-lifecycle-token \
    capacity-reporter-ca.pem \
    capacity-reporter-certificate.pem \
    capacity-reporter-private-key.pem \
    config.json \
    dev-instance-database-admin-url \
    minio-access-key \
    minio-secret-key \
    postgres-database \
    postgres-password \
    postgres-user \
    secret-store-master-key \
    svc-db-url \
    | LC_ALL=C sort)"
  test "$(secret_keys loom-personal-dev-management)" = "$expected_management"
  test "$(secret_keys loom-personal-dev-activation-public)" = public-key
  test "$(secret_keys loom-personal-dev-activation-agent)" = private-key
}

assert_no_runtimeclass_consumers() {
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    get pods --all-namespaces -o json \
    | jq -e '[.items[] | select(.spec.runtimeClassName == "loom-personal-dev-builder")] | length == 0' \
      >/dev/null
}

assert_runtimeclass_absent() {
  local observed
  observed="$(kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    get runtimeclass.node.k8s.io/loom-personal-dev-builder \
    --ignore-not-found -o name)"
  test -z "$observed"
}

assert_smoke_namespace_owned() {
  kubectl --kubeconfig "$kubeconfig" get namespace loom-runtime-smoke -o json \
    | jq -e --arg source "$merged_source_sha" '
        .metadata.name == "loom-runtime-smoke" and
        .metadata.labels["app.kubernetes.io/managed-by"] == "loom-personal-dev-runtime-smoke" and
        .metadata.annotations["loom.dev/runtime-rollout-source-sha"] == $source and
        .metadata.labels["pod-security.kubernetes.io/audit"] == "restricted" and
        .metadata.labels["pod-security.kubernetes.io/audit-version"] == "v1.36" and
        .metadata.labels["pod-security.kubernetes.io/enforce"] == "restricted" and
        .metadata.labels["pod-security.kubernetes.io/enforce-version"] == "v1.36" and
        .metadata.labels["pod-security.kubernetes.io/warn"] == "restricted" and
        .metadata.labels["pod-security.kubernetes.io/warn-version"] == "v1.36"
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-runtime-smoke \
    get pods -o json \
    | jq -e --arg source "$merged_source_sha" '
        .items | length <= 5 and all(.[];
          (.metadata.name == "buildkit-conformance" or
           (.metadata.name | test("^gvisor-smoke-[2-5]$"))) and
          .metadata.labels["app.kubernetes.io/managed-by"] == "loom-personal-dev-runtime-smoke" and
          .metadata.annotations["loom.dev/runtime-rollout-source-sha"] == $source)
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-runtime-smoke \
    get configmaps -o json \
    | jq -e --arg source "$merged_source_sha" '
        all(.items[];
          .metadata.name == "kube-root-ca.crt" or
          (.metadata.name == "buildkit-conformance" and
           .metadata.labels["app.kubernetes.io/managed-by"] == "loom-personal-dev-runtime-smoke" and
           .metadata.annotations["loom.dev/runtime-rollout-source-sha"] == $source))
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-runtime-smoke \
    get networkpolicies.networking.k8s.io -o json \
    | jq -e --arg source "$merged_source_sha" '
        .items | length <= 2 and all(.[];
          (.metadata.name == "build-egress" or .metadata.name == "default-deny") and
          .metadata.labels["app.kubernetes.io/managed-by"] == "loom-personal-dev-runtime-smoke" and
          .metadata.annotations["loom.dev/runtime-rollout-source-sha"] == $source)
      ' \
      >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-runtime-smoke \
    get serviceaccounts -o json \
    | jq -e '
        .items | length <= 1 and all(.[];
          .metadata.name == "default" and
          ((.imagePullSecrets // []) | length == 0) and
          ((.secrets // []) | length == 0))
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-runtime-smoke \
    get deployments.apps,statefulsets.apps,daemonsets.apps,jobs.batch,cronjobs.batch,persistentvolumeclaims,services,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io,resourcequotas,limitranges,ingresses.networking.k8s.io \
    -o json | jq -e '.items | length == 0' >/dev/null
  local secret_names
  secret_names="$(kubectl --kubeconfig "$kubeconfig" \
    --namespace loom-runtime-smoke get secrets -o name)"
  test -z "$secret_names"
}

assert_smoke_namespace_absent() {
  local observed
  observed="$(kubectl --kubeconfig "$kubeconfig" \
    get namespace loom-runtime-smoke --ignore-not-found -o name)"
  test -z "$observed"
}

assert_runtime_receipt() {
  local receipt="$1" operation="$2" state="$3"
  test -f "$receipt"
  test ! -L "$receipt"
  if test "$operation" = preflight; then
    test "$state" = -
    jq -e --arg archive "$archive_sha512" --arg profile "$profile_sha256" '
        . == {
          archive_sha512: $archive,
          operation: "preflight",
          profile_sha256: $profile,
          release: "release-20260810.0"
        }
      ' "$receipt" >/dev/null
  else
    jq -e --arg operation "$operation" --arg profile "$profile_sha256" \
      --arg state "$state" '
        . == {
          operation: $operation,
          profile_sha256: $profile,
          release: "release-20260810.0",
          state: $state
        }
      ' "$receipt" >/dev/null
  fi
}

assert_pod_continuity() {
  local snapshot="$1" table="$2" namespace name uid
  test -f "$snapshot"
  test ! -L "$snapshot"
  test ! -e "$table"
  jq -r '
      if type != "array" or
         any(.[];
           (keys | sort) != ["name", "namespace", "uid"] or
           (.name | type) != "string" or
           (.namespace | type) != "string" or
           (.uid | type) != "string" or
           (.name | test("[\\t\\n]")) or
           (.namespace | test("[\\t\\n]")) or
           (.uid | test("[\\t\\n]")))
      then error("invalid Pod continuity snapshot")
      else .[] | [.namespace, .name, .uid] | @tsv
      end
    ' "$snapshot" > "$table"
  chmod 0600 "$table"
  while IFS=$'\t' read -r namespace name uid; do
    kubectl --kubeconfig "$kubeconfig" --namespace "$namespace" \
      wait --for=condition=Ready --timeout=5m "pod/$name"
    test "$(kubectl --kubeconfig "$kubeconfig" --namespace "$namespace" \
      get "pod/$name" -o jsonpath='{.metadata.uid}')" = "$uid"
  done < "$table"
}

capture_runtime_node_state() {
  local node="$1" destination="$2"
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  test ! -e "$destination"
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -cS '
        {
          apiVersion,
          kind,
          metadata: {
            annotations: {
              "loom.dev/personal-dev-runtime-profile-sha256":
                .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"]
            },
            labels: {
              "loom.dev/personal-dev-runtime-profile-a":
                .metadata.labels["loom.dev/personal-dev-runtime-profile-a"],
              "loom.dev/personal-dev-runtime-profile-b":
                .metadata.labels["loom.dev/personal-dev-runtime-profile-b"]
            },
            name: .metadata.name
          },
          spec: {unschedulable: (.spec.unschedulable // false)},
          status: {
            conditions: [
              .status.conditions[]? |
              select(.type == "Ready" or .type == "DiskPressure") |
              {status, type}
            ] | sort_by(.type)
          }
        }
      ' > "$destination"
  chmod 0600 "$destination"
  jq -e --arg node "$node" '
      .apiVersion == "v1" and .kind == "Node" and .metadata.name == $node
    ' "$destination" >/dev/null
}

assert_node_cordon_state() {
  local node="$1" expected="$2"
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  case "$expected" in
    true|false) ;;
    *) return 1 ;;
  esac
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -e --argjson expected "$expected" \
      '(.spec.unschedulable // false) == $expected' >/dev/null
}

remove_node_runtime_identity() {
  local node="$1"
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -e --arg a "$profile_label_a" --arg b "$profile_label_b" \
      --arg digest "$profile_sha256" '
        (.metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == null or
         .metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == $a) and
        (.metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == null or
         .metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == $b) and
        (.metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == null or
         .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == $digest)
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" patch "node/$node" --type=merge \
    --patch '{"metadata":{"labels":{"loom.dev/personal-dev-runtime-profile-a":null,"loom.dev/personal-dev-runtime-profile-b":null},"annotations":{"loom.dev/personal-dev-runtime-profile-sha256":null}}}' \
    >/dev/null
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -e '
        (.metadata.labels["loom.dev/personal-dev-runtime-profile-a"] | not) and
        (.metadata.labels["loom.dev/personal-dev-runtime-profile-b"] | not) and
        (.metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] | not)
      ' >/dev/null
}

assert_remote_staging() {
  local node="$1" target remote_stage remote_uid remote_gid observed_inventory
  local expected_inventory staged_dir staged_path
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  target="${ssh_targets[$node]}"
  remote_stage="$(< "$evidence_dir/$node.remote-stage.txt")"
  [[ "$remote_stage" =~ ^/tmp/loom-personal-dev-runtime\.[A-Za-z0-9]{8}$ ]]
  remote_uid="$(ssh "${ssh_options[@]}" "$target" /usr/bin/id -u)"
  remote_gid="$(ssh "${ssh_options[@]}" "$target" /usr/bin/id -g)"
  [[ "$remote_uid" =~ ^[0-9]+$ ]]
  [[ "$remote_gid" =~ ^[0-9]+$ ]]
  test "$remote_uid" != 0
  expected_inventory="$(printf '%s\n' \
    gvisor-release-20260810.0.tar.bz2 \
    personal-dev-builder-runtime-profile.json \
    scripts \
    scripts/ops \
    scripts/ops/install_personal_dev_builder_runtime.py \
    scripts/ops/personal_dev_builder_runtime_profile.py)"
  observed_inventory="$(ssh "${ssh_options[@]}" "$target" \
    "/usr/bin/find '$remote_stage' -xdev -mindepth 1 -printf '%P\\n' | LC_ALL=C /usr/bin/sort")"
  test "$observed_inventory" = "$expected_inventory"
  for staged_dir in \
    "$remote_stage" \
    "$remote_stage/scripts" \
    "$remote_stage/scripts/ops"
  do
    test "$(ssh "${ssh_options[@]}" "$target" \
      /usr/bin/env LC_ALL=C /usr/bin/stat -c '%F:%u:%g:%a' "$staged_dir")" = \
      "directory:$remote_uid:$remote_gid:700"
  done
  for staged_path in \
    "$remote_stage/personal-dev-builder-runtime-profile.json" \
    "$remote_stage/gvisor-release-20260810.0.tar.bz2" \
    "$remote_stage/scripts/ops/install_personal_dev_builder_runtime.py" \
    "$remote_stage/scripts/ops/personal_dev_builder_runtime_profile.py"
  do
    test "$(ssh "${ssh_options[@]}" "$target" \
      /usr/bin/env LC_ALL=C /usr/bin/stat -c '%F:%u:%g:%a:%h' "$staged_path")" = \
      "regular file:$remote_uid:$remote_gid:600:1"
  done
}

assert_node_staging() {
  local node="$1" target root_stage staged_dir staged_path
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  target="${ssh_targets[$node]}"
  root_stage="$(< "$evidence_dir/$node.root-stage.txt")"
  test "$root_stage" = \
    "/root/loom-personal-dev-builder-runtime-rollout/$merged_source_sha/$node"
  for staged_dir in \
    /root/loom-personal-dev-builder-runtime-rollout \
    "/root/loom-personal-dev-builder-runtime-rollout/$merged_source_sha" \
    "$root_stage" \
    "$root_stage/scripts" \
    "$root_stage/scripts/ops"
  do
    test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- \
      /usr/bin/env LC_ALL=C /usr/bin/stat \
      -c '%F:%u:%g:%a' "$staged_dir")" = directory:0:0:700
  done
  test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/sha256sum \
    "$root_stage/personal-dev-builder-runtime-profile.json" | awk '{print $1}')" = \
    "$profile_sha256"
  test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/sha512sum \
    "$root_stage/gvisor-release-20260810.0.tar.bz2" | awk '{print $1}')" = \
    "$archive_sha512"
  test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/sha256sum \
    "$root_stage/scripts/ops/install_personal_dev_builder_runtime.py" | awk '{print $1}')" = \
    "$installer_sha256"
  test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/sha256sum \
    "$root_stage/scripts/ops/personal_dev_builder_runtime_profile.py" | awk '{print $1}')" = \
  "$profile_module_sha256"
  for staged_path in \
    "$root_stage/personal-dev-builder-runtime-profile.json" \
    "$root_stage/scripts/ops/install_personal_dev_builder_runtime.py" \
    "$root_stage/scripts/ops/personal_dev_builder_runtime_profile.py"
  do
    test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- \
      /usr/bin/env LC_ALL=C /usr/bin/stat \
      -c '%F:%u:%g:%a:%h' "$staged_path")" = \
      'regular file:0:0:400:1'
  done
  test "$(ssh "${ssh_options[@]}" "$target" sudo -n -- \
    /usr/bin/env LC_ALL=C /usr/bin/stat \
    -c '%F:%u:%g:%a:%h' \
    "$root_stage/gvisor-release-20260810.0.tar.bz2")" = \
    "regular file:0:0:600:1"
}

k3s_agent_invocation_id() {
  local node="$1" target invocation
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  target="${ssh_targets[$node]}"
  invocation="$(ssh "${ssh_options[@]}" "$target" sudo -n -- \
    /usr/bin/systemctl show --property=InvocationID --value k3s-agent)"
  [[ "$invocation" =~ ^[0-9a-f]{32}$ ]]
  printf '%s\n' "$invocation"
}

await_fresh_node_lease() {
  local node="$1" prefix="$2" baseline current attempt
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  test ! -e "$prefix.lease-at-restart.txt"
  test ! -e "$prefix.lease-fresh.txt"
  baseline="$(kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace kube-node-lease get "lease.coordination.k8s.io/$node" \
    -o jsonpath='{.spec.renewTime}')"
  test -n "$baseline"
  printf '%s\n' "$baseline" > "$prefix.lease-at-restart.txt"
  current="$baseline"
  for attempt in {1..60}; do
    sleep 2
    current="$(kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
      --namespace kube-node-lease get "lease.coordination.k8s.io/$node" \
      -o jsonpath='{.spec.renewTime}')"
    test -n "$current"
    if test "$current" != "$baseline"; then
      break
    fi
  done
  test "$current" != "$baseline"
  printf '%s\n' "$current" > "$prefix.lease-fresh.txt"
}

cleanup_node_staging() {
  local node="$1" target remote_stage root_stage root_stage_base root_stage_parent
  case "$node" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  target="${ssh_targets[$node]}"
  remote_stage="$(< "$evidence_dir/$node.remote-stage.txt")"
  [[ "$remote_stage" =~ ^/tmp/loom-personal-dev-runtime\.[A-Za-z0-9]{8}$ ]]
  root_stage_base=/root/loom-personal-dev-builder-runtime-rollout
  root_stage_parent="$root_stage_base/$merged_source_sha"
  root_stage="$(< "$evidence_dir/$node.root-stage.txt")"
  test "$root_stage" = "$root_stage_parent/$node"
  assert_remote_staging "$node"
  assert_node_staging "$node"
  ssh "${ssh_options[@]}" "$target" \
    "sudo -n -- /usr/bin/rm -rf -- '$root_stage' && sudo -n -- /usr/bin/test ! -e '$root_stage' && sudo -n -- /usr/bin/rmdir '$root_stage_parent' '$root_stage_base' && /usr/bin/rm -rf -- '$remote_stage' && /usr/bin/test ! -e '$remote_stage'"
  printf '%s\n' "$node staging=absent" \
    > "$evidence_dir/$node.staging-cleanup.txt"
}

assert_dns_hostname_identity() {
  local observed="$1" expected="$2" normalized
  case "$expected" in
    trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
    *) return 1 ;;
  esac
  test -n "$observed"
  normalized="$(printf '%s' "$observed" \
    | LC_ALL=C tr '[:upper:]' '[:lower:]')"
  test "$normalized" = "$expected"
}

assert_global_stop_conditions() {
  local capacity_status="$evidence_dir/capacity-status.latest.json"
  local expected_agent_names
  expected_agent_names='["trt-eai-oldlab-2","trt-eai-oldlab-3","trt-eai-oldlab-4","trt-eai-oldlab-5"]'

  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get nodes -o json \
    | jq -e --argjson agents "$expected_agent_names" '
        ([.items[].metadata.name] | sort) as $names |
        ($names - $agents) as $control_names |
        .items | length == 5 and
        ($agents - $names | length) == 0 and
        ($control_names | length) == 1 and
        all(.[];
          any(.status.conditions[]?; .type == "Ready" and .status == "True") and
          any(.status.conditions[]?; .type == "DiskPressure" and .status == "False")) and
        all(.[] | select(.metadata.name as $name | ($agents | index($name)) != null);
          .metadata.labels["node-role.kubernetes.io/control-plane"] == null) and
        all(.[] | select(.metadata.name == $control_names[0]);
          .metadata.labels["node-role.kubernetes.io/control-plane"] != null and
          .metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == null and
          .metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == null and
          .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == null)
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace longhorn-system get deployments.apps,daemonsets.apps -o json \
    | jq -e '
        ([.items[] | select(.kind == "Deployment")]) as $deployments |
        ([.items[] | select(.kind == "DaemonSet")]) as $daemon_sets |
        ($deployments | length) > 0 and
        ($daemon_sets | length) > 0 and
        all($deployments[];
          (.spec.replicas | type) == "number" and
          .spec.replicas > 0 and
          .status.observedGeneration == .metadata.generation and
          .status.updatedReplicas == .spec.replicas and
          .status.readyReplicas == .spec.replicas and
          .status.availableReplicas == .spec.replicas and
          (.status.unavailableReplicas // 0) == 0) and
        all($daemon_sets[];
          .status.desiredNumberScheduled > 0 and
          .status.observedGeneration == .metadata.generation and
          .status.updatedNumberScheduled == .status.desiredNumberScheduled and
          .status.numberReady == .status.desiredNumberScheduled and
          .status.numberAvailable == .status.desiredNumberScheduled and
          (.status.numberUnavailable // 0) == 0)
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace longhorn-system get pods -o json \
    | jq -e '
        .items as $pods |
        ($pods | length) > 0 and
        any($pods[]; .status.phase == "Running") and
        all($pods[] |
          select(.status.phase != "Succeeded" and .status.phase != "Failed");
          .status.phase == "Running" and
          (.status.containerStatuses | type == "array" and length > 0) and
          all(.status.containerStatuses[]; .ready == true))
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace longhorn-system get volumes.longhorn.io -o json \
    | jq -e '.items | length > 0 and all(.[]; .status.robustness == "healthy")' \
      >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get namespaces -o json \
    | jq -e '[.items[].metadata.name | select(startswith("loom-dev-") or startswith("loom-build-"))] | length == 0' \
      >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    get deployments.apps --all-namespaces -o json \
    | jq -e '
        [.items[] |
          select(.metadata.namespace == "loom-dev" or
                 (.metadata.namespace | startswith("loom-dev-")) or
                 (.metadata.namespace | startswith("loom-build-"))) |
          select(
            .metadata.name == "loom-worker" or
            (.metadata.name | test("^loom-worker-g[1-9][0-9]*$")) or
            .metadata.labels.app == "loom-worker" or
            .spec.template.metadata.labels.app == "loom-worker" or
            any(.spec.template.spec.containers[]?;
              .name == "worker" or
              ((.image? // "") | test("/loom-worker@sha256:[0-9a-f]{64}$")) or
              any(.env[]?;
                .name == "LOOM_SVC_K8S_WORKER_ENABLED" and .value == "true")))
        ] | length == 0
      ' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s --namespace loom-dev \
    get deployments.apps,statefulsets.apps,jobs.batch,persistentvolumeclaims,serviceaccounts,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io,services,pods,ingresses.networking.k8s.io,networkpolicies.networking.k8s.io \
    --selector app.kubernetes.io/managed-by=loom-personal-dev-control-plane \
    -o json | jq -e '.items | length == 0' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    get clusterroles.rbac.authorization.k8s.io,clusterrolebindings.rbac.authorization.k8s.io \
    --selector app.kubernetes.io/managed-by=loom-personal-dev-control-plane \
    -o json | jq -e '.items | length == 0' >/dev/null
  assert_secret_key_inventory
  uv run --no-sync loom admin capacity-control-plane status \
    --namespace loom-dev --kubeconfig "$kubeconfig" > "$capacity_status"
  chmod 0600 "$capacity_status"
  test "$(tr -d '\n' < "$capacity_status")" = \
    '{"executable_new_capacity_ceiling":0,"status":"ready"}'
}

ssh_host_evidence="$evidence_dir/ssh-host-identities.txt"
test ! -e "$ssh_host_evidence"
for node in "${nodes[@]}"; do
  target="${ssh_targets[$node]}"
  [[ "$target" =~ ^[A-Za-z_][A-Za-z0-9._-]*@trt-eai-oldlab-[2-5]$ ]]
  test "${target#*@}" = "$node"
  observed_hostname="$(ssh "${ssh_options[@]}" "$target" /bin/hostname -f)"
  assert_dns_hostname_identity "$observed_hostname" "$node"
  printf '%s\n' "$observed_hostname" >> "$ssh_host_evidence"
done
chmod 0600 "$ssh_host_evidence"
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent
for node in "${nodes[@]}"; do
  assert_node_cordon_state "$node" false
done
kubectl --kubeconfig "$kubeconfig" get nodes -o json \
  | jq -cS '[.items[] | {conditions: [.status.conditions[] | select(.type == "Ready" or .type == "DiskPressure") | {status, type}], name: .metadata.name, unschedulable: (.spec.unschedulable // false)}] | sort_by(.name)' \
  > "$evidence_dir/nodes.baseline.json"
kubectl --kubeconfig "$kubeconfig" --namespace longhorn-system \
  get volumes.longhorn.io -o json \
  | jq -cS '[.items[] | {name: .metadata.name, robustness: .status.robustness, state: .status.state}] | sort_by(.name)' \
  > "$evidence_dir/longhorn-volumes.baseline.json"
kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
  | jq -cS '[.items[].metadata.name] | sort' \
  > "$evidence_dir/namespaces.baseline.json"
{
  printf '%s\n' '[loom-personal-dev-management]'
  secret_keys loom-personal-dev-management
  printf '%s\n' '[loom-personal-dev-activation-public]'
  secret_keys loom-personal-dev-activation-public
  printf '%s\n' '[loom-personal-dev-activation-agent]'
  secret_keys loom-personal-dev-activation-agent
} > "$evidence_dir/secret-keys.baseline.txt"
chmod 0600 \
  "$evidence_dir/nodes.baseline.json" \
  "$evidence_dir/longhorn-volumes.baseline.json" \
  "$evidence_dir/namespaces.baseline.json" \
  "$evidence_dir/secret-keys.baseline.txt"
```

The RuntimeClass check above requires the class to be absent before the first node
mutation. An existing class is drift; do not replace it in place.

## 4. Roll agents 2 through 5 sequentially

Complete this whole section for one node, including uncordon and the repeated
global checks, before selecting the next node. The remote staging directory is
unprivileged and contains only public release inputs. Privilege is used only to
freeze those inputs in root-owned staging, run the audited installer, restart
`k3s-agent`, and remove the exact temporary root-owned staging tree.

```bash
node='<one-node-from-the-nodes-array-in-order>'
target="${ssh_targets[$node]}"
case "$node" in
  trt-eai-oldlab-2|trt-eai-oldlab-3|trt-eai-oldlab-4|trt-eai-oldlab-5) ;;
  *) exit 1 ;;
esac
observed_hostname="$(ssh "${ssh_options[@]}" "$target" /bin/hostname -f)"
assert_dns_hostname_identity "$observed_hostname" "$node"
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent
assert_node_cordon_state "$node" false

root_stage_base=/root/loom-personal-dev-builder-runtime-rollout
root_stage_parent="$root_stage_base/$merged_source_sha"
root_stage="$root_stage_parent/$node"
ssh "${ssh_options[@]}" "$target" \
  sudo -n -- /usr/bin/test ! -e "$root_stage_base"

remote_stage="$(ssh "${ssh_options[@]}" "$target" \
  'umask 077; /usr/bin/mktemp -d /tmp/loom-personal-dev-runtime.XXXXXXXX')"
[[ "$remote_stage" =~ ^/tmp/loom-personal-dev-runtime\.[A-Za-z0-9]{8}$ ]]
printf '%s\n' "$remote_stage" > "$evidence_dir/$node.remote-stage.txt"
ssh "${ssh_options[@]}" "$target" \
  "/usr/bin/install -d -m 0700 '$remote_stage/scripts/ops'"
scp "${ssh_options[@]}" -- \
  "$profile_source" \
  "$archive" \
  "$target:$remote_stage/"
scp "${ssh_options[@]}" -- \
  "$installer_source" \
  "$profile_module_source" \
  "$target:$remote_stage/scripts/ops/"
ssh "${ssh_options[@]}" "$target" \
  "/usr/bin/chmod 0600 '$remote_stage/personal-dev-builder-runtime-profile.json' '$remote_stage/gvisor-release-20260810.0.tar.bz2' '$remote_stage/scripts/ops/install_personal_dev_builder_runtime.py' '$remote_stage/scripts/ops/personal_dev_builder_runtime_profile.py'"
assert_remote_staging "$node"
printf '%s\n' "$root_stage" > "$evidence_dir/$node.root-stage.txt"
ssh "${ssh_options[@]}" "$target" \
  "sudo -n -- /usr/bin/test ! -e '$root_stage_base' && sudo -n -- /usr/bin/install -d -o root -g root -m 0700 '$root_stage/scripts/ops' && sudo -n -- /usr/bin/install -o root -g root -m 0400 '$remote_stage/personal-dev-builder-runtime-profile.json' '$root_stage/personal-dev-builder-runtime-profile.json' && sudo -n -- /usr/bin/install -o root -g root -m 0600 '$remote_stage/gvisor-release-20260810.0.tar.bz2' '$root_stage/gvisor-release-20260810.0.tar.bz2' && sudo -n -- /usr/bin/install -o root -g root -m 0400 '$remote_stage/scripts/ops/install_personal_dev_builder_runtime.py' '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' && sudo -n -- /usr/bin/install -o root -g root -m 0400 '$remote_stage/scripts/ops/personal_dev_builder_runtime_profile.py' '$root_stage/scripts/ops/personal_dev_builder_runtime_profile.py'"
assert_node_staging "$node"

kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
  get pods --all-namespaces --field-selector "spec.nodeName=$node" -o json \
  | jq -cS '[.items[] | select(.status.phase == "Running") | {name: .metadata.name, namespace: .metadata.namespace, uid: .metadata.uid}] | sort_by(.namespace, .name)' \
  > "$evidence_dir/$node.pods.before.json"

kubectl --kubeconfig "$kubeconfig" cordon "$node"
assert_node_cordon_state "$node" true
ssh "${ssh_options[@]}" "$target" \
  "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' preflight --profile '$root_stage/personal-dev-builder-runtime-profile.json' --archive '$root_stage/gvisor-release-20260810.0.tar.bz2'" \
  > "$evidence_dir/$node.preflight.json"
assert_runtime_receipt "$evidence_dir/$node.preflight.json" preflight -
ssh "${ssh_options[@]}" "$target" \
  "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' install --profile '$root_stage/personal-dev-builder-runtime-profile.json' --archive '$root_stage/gvisor-release-20260810.0.tar.bz2'" \
  > "$evidence_dir/$node.install.json"
assert_runtime_receipt "$evidence_dir/$node.install.json" install staged
ssh "${ssh_options[@]}" "$target" \
  "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' verify-staged --profile '$root_stage/personal-dev-builder-runtime-profile.json'" \
  > "$evidence_dir/$node.verify-staged.json"
assert_runtime_receipt \
  "$evidence_dir/$node.verify-staged.json" verify-staged staged

k3s_agent_invocation_id "$node" \
  > "$evidence_dir/$node.k3s-invocation.before.txt"
ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/systemctl restart k3s-agent
ssh "${ssh_options[@]}" "$target" sudo -n -- \
  /usr/bin/systemctl is-active --quiet k3s-agent
k3s_agent_invocation_id "$node" \
  > "$evidence_dir/$node.k3s-invocation.after.txt"
test "$(< "$evidence_dir/$node.k3s-invocation.after.txt")" != \
  "$(< "$evidence_dir/$node.k3s-invocation.before.txt")"
await_fresh_node_lease "$node" "$evidence_dir/$node.forward"
kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready \
  --timeout=5m "node/$node"
kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
  | jq -e '
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      any(.status.conditions[]?; .type == "DiskPressure" and .status == "False")
    ' >/dev/null
ssh "${ssh_options[@]}" "$target" \
  "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' verify-active --profile '$root_stage/personal-dev-builder-runtime-profile.json'" \
  > "$evidence_dir/$node.verify-active.json"
assert_runtime_receipt \
  "$evidence_dir/$node.verify-active.json" verify-active active

assert_pod_continuity "$evidence_dir/$node.pods.before.json" \
  "$evidence_dir/$node.pods.before.tsv"

kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
  | jq -e '
      (.metadata.labels["loom.dev/personal-dev-runtime-profile-a"] | not) and
      (.metadata.labels["loom.dev/personal-dev-runtime-profile-b"] | not) and
      (.metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] | not)
    ' >/dev/null
kubectl --kubeconfig "$kubeconfig" label "node/$node" \
  "loom.dev/personal-dev-runtime-profile-a=$profile_label_a" \
  "loom.dev/personal-dev-runtime-profile-b=$profile_label_b" \
  --overwrite=false
kubectl --kubeconfig "$kubeconfig" annotate "node/$node" \
  "loom.dev/personal-dev-runtime-profile-sha256=$profile_sha256" \
  --overwrite=false
capture_runtime_node_state "$node" "$evidence_dir/$node.node-after.json"
jq -e --arg a "$profile_label_a" --arg b "$profile_label_b" \
  --arg digest "$profile_sha256" '
    .metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == $a and
    .metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == $b and
    .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == $digest
  ' "$evidence_dir/$node.node-after.json" >/dev/null
assert_global_stop_conditions
kubectl --kubeconfig "$kubeconfig" uncordon "$node"
assert_node_cordon_state "$node" false
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent
```

Repeat with the next array entry only after the current node is Ready,
uncordoned, byte-verified, and the global stop conditions pass. Do not run these
blocks concurrently.

### Per-node rollback

If any command after cordon fails, stop. Keep that node cordoned and preserve
all evidence. Re-enter the strict session, restore the variables from sections
1 and 3, recover both staging paths from the node's owner-only evidence files,
and run only this exact rollback. `remove` refuses to touch nonidentical files.

```bash
node='<failed-node>'
target="${ssh_targets[$node]}"
remote_stage="$(< "$evidence_dir/$node.remote-stage.txt")"
[[ "$remote_stage" =~ ^/tmp/loom-personal-dev-runtime\.[A-Za-z0-9]{8}$ ]]
root_stage="$(< "$evidence_dir/$node.root-stage.txt")"
test "$root_stage" = \
  "/root/loom-personal-dev-builder-runtime-rollout/$merged_source_sha/$node"
assert_node_staging "$node"

assert_no_runtimeclass_consumers
assert_runtimeclass_absent
kubectl --kubeconfig "$kubeconfig" cordon "$node"
assert_node_cordon_state "$node" true
test ! -e "$evidence_dir/$node.rollback-pods.before.json"
kubectl --kubeconfig "$kubeconfig" \
  get pods --all-namespaces --field-selector "spec.nodeName=$node" -o json \
  | jq -cS '[.items[] | select(.status.phase == "Running") | {name: .metadata.name, namespace: .metadata.namespace, uid: .metadata.uid}] | sort_by(.namespace, .name)' \
  > "$evidence_dir/$node.rollback-pods.before.json"
remove_node_runtime_identity "$node"
if test -s "$evidence_dir/$node.install.json"; then
  assert_runtime_receipt "$evidence_dir/$node.install.json" install staged
  ssh "${ssh_options[@]}" "$target" \
    "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' verify-staged --profile '$root_stage/personal-dev-builder-runtime-profile.json'" \
    > "$evidence_dir/$node.rollback-verify-staged.json"
  assert_runtime_receipt \
    "$evidence_dir/$node.rollback-verify-staged.json" verify-staged staged
  ssh "${ssh_options[@]}" "$target" \
    "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' remove --profile '$root_stage/personal-dev-builder-runtime-profile.json'" \
    > "$evidence_dir/$node.remove.json"
  assert_runtime_receipt "$evidence_dir/$node.remove.json" remove absent
  k3s_agent_invocation_id "$node" \
    > "$evidence_dir/$node.rollback-k3s-invocation.before.txt"
  ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/systemctl restart k3s-agent
  ssh "${ssh_options[@]}" "$target" sudo -n -- \
    /usr/bin/systemctl is-active --quiet k3s-agent
  k3s_agent_invocation_id "$node" \
    > "$evidence_dir/$node.rollback-k3s-invocation.after.txt"
  test "$(< "$evidence_dir/$node.rollback-k3s-invocation.after.txt")" != \
    "$(< "$evidence_dir/$node.rollback-k3s-invocation.before.txt")"
  await_fresh_node_lease "$node" "$evidence_dir/$node.rollback"
else
  ssh "${ssh_options[@]}" "$target" \
    "sudo -n -- /usr/bin/test ! -e /opt/loom/gvisor/release-20260810.0 && sudo -n -- /usr/bin/test ! -e /etc/loom/personal-dev-builder-runtime-profile.json && sudo -n -- /usr/bin/test ! -e /etc/containerd/runsc-personal-dev.toml && sudo -n -- /usr/bin/test ! -e /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl && sudo -n -- /usr/bin/test ! -e /usr/local/bin/containerd-shim-runsc-v1"
fi
kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready \
  --timeout=5m "node/$node"
assert_pod_continuity "$evidence_dir/$node.pods.before.json" \
  "$evidence_dir/$node.rollback-original-pods.before.tsv"
assert_pod_continuity "$evidence_dir/$node.rollback-pods.before.json" \
  "$evidence_dir/$node.rollback-pods.before.tsv"
kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
  | jq -e '
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      any(.status.conditions[]?; .type == "DiskPressure" and .status == "False") and
      (.metadata.labels["loom.dev/personal-dev-runtime-profile-a"] | not) and
      (.metadata.labels["loom.dev/personal-dev-runtime-profile-b"] | not) and
      (.metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] | not)
    ' >/dev/null
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent
kubectl --kubeconfig "$kubeconfig" uncordon "$node"
assert_node_cordon_state "$node" false
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent
cleanup_node_staging "$node"
exit 1
```

Never remove a mismatched installation manually. Preserve it and escalate.

## 5. Diff and apply the exact RuntimeClass

The RuntimeClass stays absent while the four nodes restart. After all four
active verifications and labels pass, review one complete server-side diff.
The class contains the exact handler, full profile annotation, Linux/amd64
selector, and both digest halves:

- `loom.dev/personal-dev-runtime-profile-a=880b7c79013e38b016046c732209574d`
- `loom.dev/personal-dev-runtime-profile-b=48d6ae5a008164906f9951ba27765b76`
- `loom.dev/personal-dev-runtime-profile-sha256=880b7c79013e38b016046c732209574d48d6ae5a008164906f9951ba27765b76`

```bash
runtime_class="$evidence_dir/runtime-class.yaml"
runtime_diff="$evidence_dir/runtime-class.server-side-diff.txt"
test ! -e "$runtime_diff"
test "$(sha256sum "$runtime_class" | awk '{print $1}')" = \
  "$runtime_class_sha256"
for node in "${nodes[@]}"; do
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -e --arg a "$profile_label_a" --arg b "$profile_label_b" \
      --arg digest "$profile_sha256" '
        .spec.unschedulable != true and
        .metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == $a and
        .metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == $b and
        .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == $digest
      ' >/dev/null
done
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent

runtime_diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-builder-runtime \
  -f "$runtime_class" > "$runtime_diff" 2>&1 || runtime_diff_status=$?
test "$runtime_diff_status" -eq 1
chmod 0600 "$runtime_diff"
```

Byte-review the complete diff now. It must contain only creation of the exact
RuntimeClass. After review, calculate its SHA-256 independently and paste that
digest below. This separate block is the apply boundary.

```bash
reviewed_runtime_diff_sha256='<reviewed-runtime-diff-sha256>'
test "$reviewed_runtime_diff_sha256" != '<reviewed-runtime-diff-sha256>'
[[ "$reviewed_runtime_diff_sha256" =~ ^[0-9a-f]{64}$ ]]
test "$(sha256sum "$runtime_diff" | awk '{print $1}')" = \
  "$reviewed_runtime_diff_sha256"
test "$(sha256sum "$runtime_class" | awk '{print $1}')" = \
  "$runtime_class_sha256"
assert_global_stop_conditions
assert_no_runtimeclass_consumers
assert_runtimeclass_absent
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-builder-runtime \
  -f "$runtime_class"
kubectl --kubeconfig "$kubeconfig" get \
  runtimeclass.node.k8s.io/loom-personal-dev-builder -o json \
  > "$evidence_dir/runtime-class.live.json"
jq -e --arg digest "$profile_sha256" --arg a "$profile_label_a" \
  --arg b "$profile_label_b" '
    .apiVersion == "node.k8s.io/v1" and
    .kind == "RuntimeClass" and
    .metadata.name == "loom-personal-dev-builder" and
    .metadata.annotations["loom.dev/runtime-profile-sha256"] == $digest and
    .handler == "runsc-personal-dev" and
    .scheduling.nodeSelector == {
      "kubernetes.io/arch": "amd64",
      "kubernetes.io/os": "linux",
      "loom.dev/personal-dev-runtime-profile-a": $a,
      "loom.dev/personal-dev-runtime-profile-b": $b
    } and
    ((.scheduling | keys) - ["nodeSelector", "tolerations"] | length == 0) and
    (.scheduling.tolerations // []) == [] and
    (has("overhead") | not)
  ' "$evidence_dir/runtime-class.live.json" >/dev/null
assert_global_stop_conditions
assert_no_runtimeclass_consumers
```

Any unexpected diff, toleration, overhead, selector, annotation, or handler is
a stop condition. Do not use `--force-conflicts`.

## 6. Prove gVisor on every node

Use one reviewed digest-pinned image with `/bin/sh`, `id`, and `grep`; the
multi-architecture Python base already pinned by this repository is suitable.
The temporary namespace is `loom-runtime-smoke`, never a personal or builder
namespace. The Pod is bound directly to each reviewed node and still names the
exact RuntimeClass. `/proc/gvisor/kernel_is_gvisor` is the in-sandbox marker.

```bash
smoke_namespace=loom-runtime-smoke
smoke_image=python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7
smoke_namespace_manifest="$evidence_dir/runtime-smoke-namespace.json"
smoke_network_policies="$evidence_dir/runtime-smoke-network-policies.json"
assert_smoke_namespace_absent
jq -n --arg namespace "$smoke_namespace" --arg source "$merged_source_sha" '{
  apiVersion: "v1",
  kind: "Namespace",
  metadata: {
    name: $namespace,
    labels: {
      "app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke",
      "pod-security.kubernetes.io/audit": "restricted",
      "pod-security.kubernetes.io/audit-version": "v1.36",
      "pod-security.kubernetes.io/enforce": "restricted",
      "pod-security.kubernetes.io/enforce-version": "v1.36",
      "pod-security.kubernetes.io/warn": "restricted",
      "pod-security.kubernetes.io/warn-version": "v1.36"
    },
    annotations: {"loom.dev/runtime-rollout-source-sha": $source}
  }
}' > "$smoke_namespace_manifest"
chmod 0600 "$smoke_namespace_manifest"
kubectl --kubeconfig "$kubeconfig" create -f "$smoke_namespace_manifest"
assert_smoke_namespace_owned

jq -n --arg namespace "$smoke_namespace" --arg source "$merged_source_sha" '{
  apiVersion: "v1",
  kind: "List",
  items: [
    {
      apiVersion: "networking.k8s.io/v1",
      kind: "NetworkPolicy",
      metadata: {
        name: "default-deny",
        namespace: $namespace,
        labels: {"app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke"},
        annotations: {"loom.dev/runtime-rollout-source-sha": $source}
      },
      spec: {podSelector: {}, policyTypes: ["Ingress", "Egress"]}
    },
    {
      apiVersion: "networking.k8s.io/v1",
      kind: "NetworkPolicy",
      metadata: {
        name: "build-egress",
        namespace: $namespace,
        labels: {"app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke"},
        annotations: {"loom.dev/runtime-rollout-source-sha": $source}
      },
      spec: {
        podSelector: {},
        policyTypes: ["Egress"],
        egress: [{
          ports: [
            {protocol: "UDP", port: 53},
            {protocol: "TCP", port: 53},
            {protocol: "TCP", port: 443}
          ]
        }]
      }
    }
  ]
}' > "$smoke_network_policies"
chmod 0600 "$smoke_network_policies"
kubectl --kubeconfig "$kubeconfig" create -f "$smoke_network_policies"
kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
  get networkpolicies.networking.k8s.io -o json \
  | jq -e '[.items[].metadata.name] | sort == ["build-egress", "default-deny"]' \
    >/dev/null
assert_smoke_namespace_owned

for node in "${nodes[@]}"; do
  pod="gvisor-smoke-${node##*-}"
  manifest="$evidence_dir/$pod.json"
  jq -n --arg namespace "$smoke_namespace" --arg node "$node" \
    --arg pod "$pod" --arg image "$smoke_image" \
    --arg source "$merged_source_sha" '{
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: $pod,
        namespace: $namespace,
        labels: {"app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke"},
        annotations: {"loom.dev/runtime-rollout-source-sha": $source}
      },
      spec: {
        activeDeadlineSeconds: 300,
        automountServiceAccountToken: false,
        nodeName: $node,
        restartPolicy: "Never",
        runtimeClassName: "loom-personal-dev-builder",
        terminationGracePeriodSeconds: 30,
        securityContext: {
          runAsNonRoot: true,
          runAsUser: 65532,
          runAsGroup: 65532,
          seccompProfile: {type: "RuntimeDefault"}
        },
        containers: [{
          name: "smoke",
          image: $image,
          imagePullPolicy: "IfNotPresent",
          command: ["/bin/sh", "-ceu", "--"],
          args: ["test -f /proc/gvisor/kernel_is_gvisor; test \"$(id -u)\" = 65532; test \"$(grep ^CapEff: /proc/self/status | cut -f2)\" = 0000000000000000; printf \"gvisor-marker=present uid=65532 capeff=0000000000000000\\n\""],
          resources: {
            requests: {cpu: "10m", memory: "32Mi"},
            limits: {cpu: "250m", memory: "128Mi"}
          },
          securityContext: {
            allowPrivilegeEscalation: false,
            capabilities: {drop: ["ALL"]},
            readOnlyRootFilesystem: true,
            runAsNonRoot: true
          }
        }]
      }
    }' > "$manifest"
  chmod 0600 "$manifest"
  kubectl --kubeconfig "$kubeconfig" create -f "$manifest"
  kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
    wait --for=jsonpath='{.status.phase}'=Succeeded --timeout=5m "pod/$pod"
  kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
    get "pod/$pod" -o json > "$evidence_dir/$pod.live.json"
  jq -e --arg image "$smoke_image" --arg node "$node" \
    --arg source "$merged_source_sha" --arg a "$profile_label_a" \
    --arg b "$profile_label_b" '
      .status.phase == "Succeeded" and
      .spec.nodeName == $node and
      .spec.runtimeClassName == "loom-personal-dev-builder" and
      .spec.nodeSelector == {
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/os": "linux",
        "loom.dev/personal-dev-runtime-profile-a": $a,
        "loom.dev/personal-dev-runtime-profile-b": $b
      } and
      .spec.automountServiceAccountToken == false and
      .metadata.labels["app.kubernetes.io/managed-by"] == "loom-personal-dev-runtime-smoke" and
      .metadata.annotations["loom.dev/runtime-rollout-source-sha"] == $source and
      .spec.containers[0].image == $image and
      .spec.containers[0].securityContext.allowPrivilegeEscalation == false and
      .spec.containers[0].securityContext.capabilities.drop == ["ALL"] and
      .spec.containers[0].securityContext.readOnlyRootFilesystem == true and
      .spec.containers[0].securityContext.runAsNonRoot == true
    ' "$evidence_dir/$pod.live.json" >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
    logs --limit-bytes=1048576 "$pod" > "$evidence_dir/$pod.log"
  test "$(tr -d '\n' < "$evidence_dir/$pod.log")" = \
    'gvisor-marker=present uid=65532 capeff=0000000000000000'
  assert_smoke_namespace_owned
  assert_global_stop_conditions
done
```

## 7. Prove rootless BuildKit amd64 and arm64 on the canary

Use the separately reviewed immutable `personal_dev_builder` image digest; do
not substitute a tag. Its rootless BuildKit contains
`buildkit-qemu-aarch64`. The conformance builds a digest-pinned base through a
`RUN` step for both `linux/amd64` and `linux/arm64`. The step checks the actual
`uname -m`; the verifier then reads each OCI config and requires the requested
platform metadata.

```bash
builder_image='<reviewed-personal-dev-builder-image@sha256:64-lowercase-hex>'
test "$builder_image" != \
  '<reviewed-personal-dev-builder-image@sha256:64-lowercase-hex>'
[[ "$builder_image" =~ ^ghcr\.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$ ]]
base_image=python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7
buildkit_script="$evidence_dir/buildkit-conformance.sh"
cat > "$buildkit_script" <<'SH'
#!/bin/sh
set -eu
test -f /proc/gvisor/kernel_is_gvisor
test -x /usr/bin/buildctl-daemonless.sh
test -x /usr/bin/buildkit-qemu-aarch64
mkdir -p "$HOME" "$XDG_RUNTIME_DIR"
chmod 0700 "$HOME" "$XDG_RUNTIME_DIR"

build_one() {
  platform="$1"
  expected_uname="$2"
  expected_arch="$3"
  directory="/workspace/${expected_arch}"
  output="/workspace/${expected_arch}.oci.tar"
  metadata="/workspace/${expected_arch}.metadata.json"
  mkdir -p "$directory"
  chmod 0700 "$directory"
  cat > "$directory/Dockerfile" <<EOF
FROM ${BASE_IMAGE}
RUN actual="\$(uname -m)"; printf 'loom-uname=%s\\n' "\$actual"; test "\$actual" = "${expected_uname}"
EOF
  BUILDKITD_FLAGS="--oci-worker-no-process-sandbox --oci-worker-snapshotter=native" \
    /usr/bin/buildctl-daemonless.sh build \
      --frontend=dockerfile.v0 \
      --local "context=$directory" \
      --local "dockerfile=$directory" \
      --opt "platform=$platform" \
      --output "type=oci,dest=$output" \
      --metadata-file "$metadata" \
      --progress=plain
  python3 - "$output" "$platform" "$expected_arch" <<'PY'
import json
import sys
import tarfile

archive, platform, expected_arch = sys.argv[1:]
with tarfile.open(archive, mode="r:") as bundle:
    index = json.load(bundle.extractfile("index.json"))

    def document(digest: str):
        algorithm, value = digest.split(":", 1)
        if algorithm != "sha256":
            raise SystemExit("unexpected OCI digest")
        return json.load(bundle.extractfile(f"blobs/sha256/{value}"))

    pending = list(index["manifests"])
    configs = []
    while pending:
        value = document(pending.pop()["digest"])
        if "manifests" in value:
            pending.extend(value["manifests"])
        elif "config" in value:
            configs.append(document(value["config"]["digest"]))
    if len(configs) != 1:
        raise SystemExit("unexpected OCI config count")
    config = configs[0]
    if config.get("os") != "linux" or config.get("architecture") != expected_arch:
        raise SystemExit("OCI platform mismatch")
print(json.dumps({"architecture": expected_arch, "platform": platform}, sort_keys=True))
PY
}

build_one linux/amd64 x86_64 amd64
build_one linux/arm64 aarch64 arm64
SH
chmod 0600 "$buildkit_script"

buildkit_configmap="$evidence_dir/buildkit-conformance.configmap.json"
kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
  create configmap buildkit-conformance \
  --from-file=run.sh="$buildkit_script" \
  --dry-run=client -o json \
  | jq --arg source "$merged_source_sha" '
      .metadata.labels["app.kubernetes.io/managed-by"] = "loom-personal-dev-runtime-smoke" |
      .metadata.annotations["loom.dev/runtime-rollout-source-sha"] = $source
    ' > "$buildkit_configmap"
chmod 0600 "$buildkit_configmap"
kubectl --kubeconfig "$kubeconfig" create -f "$buildkit_configmap"
assert_smoke_namespace_owned

jq -n --arg namespace "$smoke_namespace" --arg image "$builder_image" \
  --arg base "$base_image" --arg source "$merged_source_sha" '{
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      name: "buildkit-conformance",
      namespace: $namespace,
      labels: {"app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke"},
      annotations: {"loom.dev/runtime-rollout-source-sha": $source}
    },
    spec: {
      activeDeadlineSeconds: 1200,
      automountServiceAccountToken: false,
      nodeName: "trt-eai-oldlab-2",
      restartPolicy: "Never",
      runtimeClassName: "loom-personal-dev-builder",
      terminationGracePeriodSeconds: 30,
      securityContext: {
        runAsNonRoot: true,
        runAsUser: 1000,
        runAsGroup: 1000,
        fsGroup: 1000,
        seccompProfile: {type: "RuntimeDefault"}
      },
      containers: [{
        name: "buildkit",
        image: $image,
        imagePullPolicy: "IfNotPresent",
        command: ["/bin/sh", "/var/run/loom-conformance/run.sh"],
        env: [
          {name: "BASE_IMAGE", value: $base},
          {name: "HOME", value: "/workspace/home"},
          {name: "TMPDIR", value: "/tmp"},
          {name: "XDG_RUNTIME_DIR", value: "/workspace/run"}
        ],
        resources: {
          requests: {cpu: "500m", memory: "1Gi", "ephemeral-storage": "4Gi"},
          limits: {cpu: "4", memory: "8Gi", "ephemeral-storage": "20Gi"}
        },
        securityContext: {
          allowPrivilegeEscalation: false,
          capabilities: {drop: ["ALL"]},
          readOnlyRootFilesystem: true,
          runAsNonRoot: true
        },
        volumeMounts: [
          {name: "script", mountPath: "/var/run/loom-conformance", readOnly: true},
          {name: "workspace", mountPath: "/workspace"},
          {name: "tmp", mountPath: "/tmp"}
        ]
      }],
      volumes: [
        {name: "script", configMap: {name: "buildkit-conformance", defaultMode: 292}},
        {name: "workspace", emptyDir: {sizeLimit: "20Gi"}},
        {name: "tmp", emptyDir: {sizeLimit: "2Gi"}}
      ]
    }
  }' > "$evidence_dir/buildkit-conformance.pod.json"
chmod 0600 "$evidence_dir/buildkit-conformance.pod.json"
kubectl --kubeconfig "$kubeconfig" create \
  -f "$evidence_dir/buildkit-conformance.pod.json"
kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
  wait --for=jsonpath='{.status.phase}'=Succeeded --timeout=20m \
  pod/buildkit-conformance
kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
  get pod/buildkit-conformance -o json \
  > "$evidence_dir/buildkit-conformance.live.json"
jq -e --arg image "$builder_image" --arg source "$merged_source_sha" \
  --arg a "$profile_label_a" --arg b "$profile_label_b" '
    .status.phase == "Succeeded" and
    .spec.nodeName == "trt-eai-oldlab-2" and
    .spec.runtimeClassName == "loom-personal-dev-builder" and
    .spec.nodeSelector == {
      "kubernetes.io/arch": "amd64",
      "kubernetes.io/os": "linux",
      "loom.dev/personal-dev-runtime-profile-a": $a,
      "loom.dev/personal-dev-runtime-profile-b": $b
    } and
    .spec.automountServiceAccountToken == false and
    .metadata.labels["app.kubernetes.io/managed-by"] == "loom-personal-dev-runtime-smoke" and
    .metadata.annotations["loom.dev/runtime-rollout-source-sha"] == $source and
    .spec.containers[0].image == $image and
    .spec.containers[0].securityContext.allowPrivilegeEscalation == false and
    .spec.containers[0].securityContext.capabilities.drop == ["ALL"] and
    .spec.containers[0].securityContext.readOnlyRootFilesystem == true and
    .spec.containers[0].securityContext.runAsNonRoot == true
  ' "$evidence_dir/buildkit-conformance.live.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace "$smoke_namespace" \
  logs --limit-bytes=8388608 pod/buildkit-conformance \
  > "$evidence_dir/buildkit-conformance.log"
grep -F 'loom-uname=x86_64' "$evidence_dir/buildkit-conformance.log" >/dev/null
grep -F 'loom-uname=aarch64' "$evidence_dir/buildkit-conformance.log" >/dev/null
grep -F '"architecture": "amd64", "platform": "linux/amd64"' \
  "$evidence_dir/buildkit-conformance.log" >/dev/null
grep -F '"architecture": "arm64", "platform": "linux/arm64"' \
  "$evidence_dir/buildkit-conformance.log" >/dev/null
assert_global_stop_conditions
```

If either platform fails, retain its Pod and logs, remove the RuntimeClass from
eligibility as described below, and stop. Do not substitute a native runc Pod.

## 8. Close or roll back the rollout

On success, delete only the temporary smoke namespace after all Pod and build
evidence is captured. Re-verify the RuntimeClass, nodes, capacity ceiling,
Secret key inventory, Longhorn, and absence of personal/build namespaces.

```bash
assert_smoke_namespace_owned
kubectl --kubeconfig "$kubeconfig" delete namespace "$smoke_namespace" --wait=true
assert_smoke_namespace_absent
assert_global_stop_conditions
assert_no_runtimeclass_consumers
kubectl --kubeconfig "$kubeconfig" get \
  runtimeclass.node.k8s.io/loom-personal-dev-builder -o json \
  > "$evidence_dir/runtime-class.final.json"
jq -e --arg digest "$profile_sha256" --arg a "$profile_label_a" \
  --arg b "$profile_label_b" '
    .apiVersion == "node.k8s.io/v1" and
    .kind == "RuntimeClass" and
    .metadata.name == "loom-personal-dev-builder" and
    .metadata.annotations["loom.dev/runtime-profile-sha256"] == $digest and
    .handler == "runsc-personal-dev" and
    .scheduling.nodeSelector == {
      "kubernetes.io/arch": "amd64",
      "kubernetes.io/os": "linux",
      "loom.dev/personal-dev-runtime-profile-a": $a,
      "loom.dev/personal-dev-runtime-profile-b": $b
    } and
    ((.scheduling | keys) - ["nodeSelector", "tolerations"] | length == 0) and
    (.scheduling.tolerations // []) == [] and
    (has("overhead") | not)
  ' "$evidence_dir/runtime-class.final.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace longhorn-system \
  get volumes.longhorn.io -o json \
  | jq -cS '[.items[] | {name: .metadata.name, robustness: .status.robustness, state: .status.state}] | sort_by(.name)' \
  > "$evidence_dir/longhorn-volumes.final.json"
kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
  | jq -cS '[.items[].metadata.name] | sort' \
  > "$evidence_dir/namespaces.final.json"
{
  printf '%s\n' '[loom-personal-dev-management]'
  secret_keys loom-personal-dev-management
  printf '%s\n' '[loom-personal-dev-activation-public]'
  secret_keys loom-personal-dev-activation-public
  printf '%s\n' '[loom-personal-dev-activation-agent]'
  secret_keys loom-personal-dev-activation-agent
} > "$evidence_dir/secret-keys.final.txt"
for node in "${nodes[@]}"; do
  capture_runtime_node_state "$node" "$evidence_dir/$node.final.json"
  jq -e --arg a "$profile_label_a" --arg b "$profile_label_b" \
    --arg digest "$profile_sha256" '
      .spec.unschedulable != true and
      .metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == $a and
      .metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == $b and
      .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == $digest and
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      any(.status.conditions[]?; .type == "DiskPressure" and .status == "False")
    ' "$evidence_dir/$node.final.json" >/dev/null
  cleanup_node_staging "$node"
done
(
  cd "$evidence_dir"
  find . -maxdepth 1 -type f ! -name evidence.sha256 -print0 \
    | sort -z | xargs -0 sha256sum
) > "$evidence_dir/evidence.sha256"
chmod 0600 "$evidence_dir"/*
```

Publish only sanitized source, profile, RuntimeClass, node, and evidence hashes
to #1280. Do not publish the kubeconfig, its contents, Secret values, remote
command output, or the builder image pull credential.

For fleet rollback after the RuntimeClass exists, first require that no Pod in
any namespace names `loom-personal-dev-builder`. Then delete exactly that
RuntimeClass, remove the exact two labels and one annotation from agents 2–5,
and run the per-node rollback in reverse order. Keep each node cordoned until
`remove`, the `k3s-agent` restart, Ready/DiskPressure, Pod continuity, and
Longhorn checks all pass.

```bash
smoke_namespace_name="$(kubectl --kubeconfig "$kubeconfig" \
  get namespace "$smoke_namespace" --ignore-not-found -o name)"
if test -n "$smoke_namespace_name"
then
  assert_smoke_namespace_owned
  kubectl --kubeconfig "$kubeconfig" delete namespace "$smoke_namespace" --wait=true
fi
assert_smoke_namespace_absent
kubectl --kubeconfig "$kubeconfig" get pods --all-namespaces -o json \
  | jq -e '[.items[] | select(.spec.runtimeClassName == "loom-personal-dev-builder")] | length == 0' \
    >/dev/null
kubectl --kubeconfig "$kubeconfig" get \
  runtimeclass.node.k8s.io/loom-personal-dev-builder -o json \
  | jq -e --arg digest "$profile_sha256" --arg a "$profile_label_a" \
    --arg b "$profile_label_b" '
      .apiVersion == "node.k8s.io/v1" and
      .kind == "RuntimeClass" and
      .metadata.name == "loom-personal-dev-builder" and
      .metadata.annotations["loom.dev/runtime-profile-sha256"] == $digest and
      .handler == "runsc-personal-dev" and
      .scheduling.nodeSelector == {
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/os": "linux",
        "loom.dev/personal-dev-runtime-profile-a": $a,
        "loom.dev/personal-dev-runtime-profile-b": $b
      } and
      ((.scheduling | keys) - ["nodeSelector", "tolerations"] | length == 0) and
      (.scheduling.tolerations // []) == [] and
      (has("overhead") | not)
    ' >/dev/null
kubectl --kubeconfig "$kubeconfig" delete \
  runtimeclass.node.k8s.io/loom-personal-dev-builder --wait=true
assert_runtimeclass_absent
assert_no_runtimeclass_consumers
for node in trt-eai-oldlab-5 trt-eai-oldlab-4 trt-eai-oldlab-3 trt-eai-oldlab-2; do
  assert_runtimeclass_absent
  assert_no_runtimeclass_consumers
  target="${ssh_targets[$node]}"
  remote_stage="$(< "$evidence_dir/$node.remote-stage.txt")"
  [[ "$remote_stage" =~ ^/tmp/loom-personal-dev-runtime\.[A-Za-z0-9]{8}$ ]]
  root_stage="$(< "$evidence_dir/$node.root-stage.txt")"
  test "$root_stage" = \
    "/root/loom-personal-dev-builder-runtime-rollout/$merged_source_sha/$node"
  assert_node_staging "$node"
  kubectl --kubeconfig "$kubeconfig" cordon "$node"
  assert_node_cordon_state "$node" true
  kubectl --kubeconfig "$kubeconfig" \
    get pods --all-namespaces --field-selector "spec.nodeName=$node" -o json \
    | jq -cS '[.items[] | select(.status.phase == "Running") | {name: .metadata.name, namespace: .metadata.namespace, uid: .metadata.uid}] | sort_by(.namespace, .name)' \
    > "$evidence_dir/$node.rollback-pods.before.json"
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -e --arg a "$profile_label_a" --arg b "$profile_label_b" \
      --arg digest "$profile_sha256" '
        .metadata.labels["loom.dev/personal-dev-runtime-profile-a"] == $a and
        .metadata.labels["loom.dev/personal-dev-runtime-profile-b"] == $b and
        .metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] == $digest
      ' >/dev/null
  remove_node_runtime_identity "$node"
  ssh "${ssh_options[@]}" "$target" \
    "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' verify-staged --profile '$root_stage/personal-dev-builder-runtime-profile.json'" \
    > "$evidence_dir/$node.fleet-rollback-verify-staged.json"
  assert_runtime_receipt \
    "$evidence_dir/$node.fleet-rollback-verify-staged.json" verify-staged staged
  ssh "${ssh_options[@]}" "$target" \
    "sudo -n -- /usr/bin/env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH='$root_stage' /usr/bin/python3 '$root_stage/scripts/ops/install_personal_dev_builder_runtime.py' remove --profile '$root_stage/personal-dev-builder-runtime-profile.json'" \
    > "$evidence_dir/$node.fleet-remove.json"
  assert_runtime_receipt "$evidence_dir/$node.fleet-remove.json" remove absent
  k3s_agent_invocation_id "$node" \
    > "$evidence_dir/$node.fleet-rollback-k3s-invocation.before.txt"
  ssh "${ssh_options[@]}" "$target" sudo -n -- /usr/bin/systemctl restart k3s-agent
  ssh "${ssh_options[@]}" "$target" sudo -n -- \
    /usr/bin/systemctl is-active --quiet k3s-agent
  k3s_agent_invocation_id "$node" \
    > "$evidence_dir/$node.fleet-rollback-k3s-invocation.after.txt"
  test "$(< "$evidence_dir/$node.fleet-rollback-k3s-invocation.after.txt")" != \
    "$(< "$evidence_dir/$node.fleet-rollback-k3s-invocation.before.txt")"
  await_fresh_node_lease "$node" "$evidence_dir/$node.fleet-rollback"
  kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready \
    --timeout=5m "node/$node"
  assert_pod_continuity "$evidence_dir/$node.rollback-pods.before.json" \
    "$evidence_dir/$node.fleet-rollback-pods.before.tsv"
  kubectl --kubeconfig "$kubeconfig" get "node/$node" -o json \
    | jq -e '
        any(.status.conditions[]?; .type == "Ready" and .status == "True") and
        any(.status.conditions[]?; .type == "DiskPressure" and .status == "False") and
        (.metadata.labels["loom.dev/personal-dev-runtime-profile-a"] | not) and
        (.metadata.labels["loom.dev/personal-dev-runtime-profile-b"] | not) and
        (.metadata.annotations["loom.dev/personal-dev-runtime-profile-sha256"] | not)
      ' >/dev/null
  assert_global_stop_conditions
  kubectl --kubeconfig "$kubeconfig" uncordon "$node"
  assert_node_cordon_state "$node" false
  assert_global_stop_conditions
  assert_runtimeclass_absent
  assert_no_runtimeclass_consumers
  cleanup_node_staging "$node"
done
assert_runtimeclass_absent
```

Never change the global manager ceiling, start a pool executor, submit a Slurm
job, enable a personal controller, or reuse this evidence for the later
management-shadow apply. That apply needs a fresh trusted release, render,
server-side diff, and explicit #1280 shadow window.
