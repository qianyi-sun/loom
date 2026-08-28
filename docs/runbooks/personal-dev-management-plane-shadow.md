# Personal-development management-plane shadow rehearsal

This runbook renders, deploys, observes, and rolls back the inert shared
personal-development management plane in `loom-dev`. It is a shadow rehearsal:
personal mutations remain disabled, the activation agent remains at zero
replicas, and physical capacity unchanged is a mandatory boundary.

Repository merge and render success do not authorize a live change. The
server-side apply steps below may run only in the explicit issue #1280 shadow
window, with the reviewed kubeconfig, release evidence, Secret provisioning,
rollback artifact, and global-capacity zero-ceiling shadow already approved.
This runbook contains no personal application deployment or physical-capacity
transition.

When the reviewed release advances to a schema on which the predecessor
service cannot start, use the
[incompatible-schema transition](personal-dev-schema-transition.md). Reapplying
this shadow runbook alone is not a database rollback and must not cross an
incompatible schema boundary.

## Stop conditions

Before rendering, and again immediately before apply or rollback:
stop if any `loom-dev-<owner>` namespace exists. Also stop for an unapproved
source commit or tree, a mutable or mismatched image, a changed trusted-release
file, a noncanonical evidence record, missing Secret keys, a nonzero
capacity-manager ceiling, an unexpected package-owned resource, a failed
migration, an unbound PVC, a nonzero activation replica count, or an
unavailable rollback artifact. Stop if any `loom-build-*` namespace exists;
the restricted builder is disabled throughout this rehearsal.

Do not improvise cleanup. Never delete PVCs, databases, buckets, migration
evidence, Secrets, or namespaces as part of this rehearsal. Retain the current
zero-capacity state and escalate when a stop condition is reached.

## 1. Prepare owner-only evidence

Use one exact CI-approved source commit and tree. The trusted-release document
must be the canonical, current-user-owned mode-`0600` file produced by the
protected image release for that source. It binds immutable digests for the
service, builder, activation agent, PostgreSQL, MinIO, and MinIO client.
Run every command block below in the same Bash session; the first block enables
strict error and pipeline handling for the entire rehearsal.

```bash
set -euo pipefail
umask 077
evidence_dir='artifacts/personal-dev/management-shadow/<approved-window-id>'
profile=deploy/dev-fleet/personal-dev-control-plane.toml
trusted_release="$evidence_dir/trusted-release.json"
trusted_release_sha256='<reviewed-64-lowercase-hex>'
shadow_render="$evidence_dir/personal-management-shadow.yaml"
render_evidence="$evidence_dir/personal-management-shadow.render.json"
status_evidence="$evidence_dir/personal-management-shadow.status.json"
rollback_status_evidence="$evidence_dir/rollback-shadow.status.json"
previous_shadow_render="$evidence_dir/previous-reviewed-shadow.yaml"
previous_shadow_sha256='<previous-reviewed-64-lowercase-hex>'
previous_profile="$evidence_dir/previous-reviewed-profile.toml"
previous_trusted_release="$evidence_dir/previous-reviewed-trusted-release.json"
previous_trusted_release_sha256='<previous-reviewed-64-lowercase-hex>'
kubeconfig=/absolute/path/to/reviewed-kubeconfig

install -d -m 0700 "$evidence_dir"
test ! -e "$shadow_render"
test ! -e "$render_evidence"
test ! -e "$status_evidence"
test "$(git rev-parse --show-toplevel)" = "$(pwd -P)"
export PYTHONPATH=src:.
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test -f "$trusted_release" && test ! -L "$trusted_release"
test "$(stat -c %u "$trusted_release")" = "$(id -u)"
test "$(stat -c %a "$trusted_release")" = 600
test "$(stat -c %h "$trusted_release")" = 1
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
repository_source_sha="$(git rev-parse HEAD)"
repository_source_tree="$(git rev-parse HEAD^{tree})"
test "$repository_source_sha" = "$(jq -r .source_sha "$trusted_release")"
test "$repository_source_tree" = "$(jq -r .source_tree "$trusted_release")"
test -f "$profile" && test ! -L "$profile"
test "$(stat -c %u "$profile")" = "$(id -u)"
test "$(stat -c %h "$profile")" = 1
test "$(stat -c %s "$profile")" -gt 0
test "$(stat -c %s "$profile")" -le 1048576
test -f "$kubeconfig" && test ! -L "$kubeconfig"
test "$(realpath -e "$kubeconfig")" = "$kubeconfig"
test "$(stat -c %u "$kubeconfig")" = "$(id -u)"
test "$(stat -c %a "$kubeconfig")" = 600
test "$(stat -c %h "$kubeconfig")" = 1
test "$(stat -c %s "$kubeconfig")" -gt 0
test "$(stat -c %s "$kubeconfig")" -le 1048576
profile_sha256="$(sha256sum "$profile" | awk '{print $1}')"
kubeconfig_sha256="$(sha256sum "$kubeconfig" | awk '{print $1}')"
kubeconfig_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$kubeconfig")"

assert_reviewed_kubeconfig() {
  test -f "$kubeconfig" && test ! -L "$kubeconfig"
  test "$(realpath -e "$kubeconfig")" = "$kubeconfig"
  test "$(stat -c %u "$kubeconfig")" = "$(id -u)"
  test "$(stat -c %a "$kubeconfig")" = 600
  test "$(stat -c %h "$kubeconfig")" = 1
  test "$(stat -c %s "$kubeconfig")" -gt 0
  test "$(stat -c %s "$kubeconfig")" -le 1048576
  test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$kubeconfig")" = \
    "$kubeconfig_identity"
  test "$(sha256sum "$kubeconfig" | awk '{print $1}')" = "$kubeconfig_sha256"
}

assert_no_dynamic_namespaces() {
  local forbidden_namespaces
  forbidden_namespaces="$(
    kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
      | jq -r '.items[].metadata.name |
          select(startswith("loom-dev-") or startswith("loom-build-"))'
  )"
  test -z "$forbidden_namespaces"
}

capacity_shadow_status() {
  uv run --no-sync loom admin capacity-control-plane status \
    --namespace loom-dev \
    --kubeconfig "$kubeconfig"
}

assert_zero_capacity() {
  local capacity_status
  capacity_status="$(capacity_shadow_status)"
  test "$capacity_status" = \
    '{"executable_new_capacity_ceiling":0,"status":"ready"}'
}

assert_status_input_safety() {
  local checked_profile="$1"
  local checked_release="$2"
  local checked_release_sha256="$3"
  local preflight_status
  local status_rc=0
  local valid=0
  preflight_status="$(mktemp "$evidence_dir/preflight-status.XXXXXX.json")"
  uv run --no-sync loom admin personal-dev-control-plane status \
    --namespace loom-dev \
    --kubeconfig "$kubeconfig" \
    --file "$checked_profile" \
    --trusted-release-file "$checked_release" \
    --trusted-release-sha256 "$checked_release_sha256" \
    > "$preflight_status" || status_rc=$?
  if { test "$status_rc" -eq 0 || test "$status_rc" -eq 1; } &&
    jq -e '.schema == "loom-personal-dev-control-plane-status-v1" and
           (.blockers | index("kube_context_invalid") | not)' \
      "$preflight_status" >/dev/null; then
    valid=1
  fi
  rm -f "$preflight_status"
  test "$valid" -eq 1
}
```

Keep the previous reviewed shadow YAML, profile, trusted-release file, trusted
release SHA-256, render evidence, and YAML SHA-256 in the same owner-only
evidence set. If this is the first shadow installation and no previous
manifest exists, rollback means retaining the current inert state and
escalating; it never means deleting shared storage.

## 2. Render and bind exact bytes

Render into temporary owner-only files. The command validates every input
before stdout and emits YAML only to stdout plus one canonical evidence record
to stderr. Publish neither file if rendering fails.

```bash
render_tmp="$(mktemp "$evidence_dir/personal-shadow.XXXXXX.yaml")"
render_evidence_tmp="$(mktemp "$evidence_dir/personal-shadow.XXXXXX.json")"

if ! uv run --no-sync loom admin personal-dev-control-plane render \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$render_tmp" 2> "$render_evidence_tmp"; then
  rm -f "$render_tmp" "$render_evidence_tmp"
  exit 1
fi

chmod 0600 "$render_tmp" "$render_evidence_tmp"
mv "$render_tmp" "$shadow_render"
mv "$render_evidence_tmp" "$render_evidence"
sha256sum "$shadow_render" > "$shadow_render.sha256"
chmod 0600 "$shadow_render.sha256"
shadow_render_sha256="$(sha256sum "$shadow_render" | awk '{print $1}')"

assert_current_shadow_artifacts() {
  test -z "$(git status --porcelain=v1 --untracked-files=all)"
  test "$(git rev-parse HEAD)" = "$(jq -r .source_sha "$trusted_release")"
  test "$(git rev-parse HEAD^{tree})" = "$(jq -r .source_tree "$trusted_release")"
  test -f "$profile" && test ! -L "$profile"
  test "$(stat -c %u "$profile")" = "$(id -u)"
  test "$(stat -c %h "$profile")" = 1
  test "$(stat -c %s "$profile")" -gt 0
  test "$(stat -c %s "$profile")" -le 1048576
  test "$(sha256sum "$profile" | awk '{print $1}')" = "$profile_sha256"
  test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
    "$trusted_release_sha256"
  test -f "$shadow_render" && test ! -L "$shadow_render"
  test "$(stat -c %u "$shadow_render")" = "$(id -u)"
  test "$(stat -c %a "$shadow_render")" = 600
  test "$(stat -c %h "$shadow_render")" = 1
  test "$(sha256sum "$shadow_render" | awk '{print $1}')" = \
    "$shadow_render_sha256"
  assert_status_input_safety \
    "$profile" "$trusted_release" "$trusted_release_sha256"
}

render_identity_set() {
  uv run --no-sync python - "$1" <<'PY'
import json
import re
import sys
from pathlib import Path

import yaml

migration_name = re.compile(r"loom-personal-dev-migrate-[0-9a-f]{16}-[0-9a-f]{16}")
documents = list(yaml.safe_load_all(Path(sys.argv[1]).read_text(encoding="utf-8")))
identities = set()
for document in documents:
    if not isinstance(document, dict) or not isinstance(document.get("metadata"), dict):
        raise SystemExit("manifest resource identity is invalid")
    api_version = document.get("apiVersion")
    kind = document.get("kind")
    name = document["metadata"].get("name")
    namespace = document["metadata"].get("namespace", "")
    if not all(isinstance(value, str) and value for value in (api_version, kind, name)):
        raise SystemExit("manifest resource identity is invalid")
    if not isinstance(namespace, str):
        raise SystemExit("manifest resource identity is invalid")
    if kind == "Namespace" or (kind == "Job" and migration_name.fullmatch(name)):
        continue
    identity = (api_version, kind, namespace, name)
    if identity in identities:
        raise SystemExit("manifest resource identity is duplicated")
    identities.add(identity)
for identity in sorted(identities):
    print(json.dumps(identity, separators=(",", ":"), ensure_ascii=True))
PY
}

live_identity_set() {
  uv run --no-sync python - "$1" "$2" <<'PY'
import json
import re
import sys
from pathlib import Path

managed_by = "loom-personal-dev-control-plane"
migration_name = re.compile(r"loom-personal-dev-migrate-[0-9a-f]{16}-[0-9a-f]{16}")
generated_pvc = re.compile(r"data-loom-dev-(?:postgres|minio)-0")
derived_pod_controllers = {"Job", "ReplicaSet", "StatefulSet"}
identities = set()
for path_index, path_value in enumerate(sys.argv[1:]):
    document = json.loads(Path(path_value).read_text(encoding="utf-8"))
    items = document.get("items") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or not set(document).issubset({"apiVersion", "kind", "metadata", "items"})
        or document.get("apiVersion") != "v1"
        or document.get("kind") != "List"
        or not isinstance(items, list)
        or len(items) > 4096
    ):
        raise SystemExit("live resource inventory is invalid")
    expected_namespace = "loom-dev" if path_index == 0 else ""
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("metadata"), dict):
            raise SystemExit("live resource identity is invalid")
        metadata = item["metadata"]
        api_version = item.get("apiVersion")
        kind = item.get("kind")
        name = metadata.get("name")
        namespace = metadata.get("namespace", "")
        labels = metadata.get("labels")
        if (
            not all(isinstance(value, str) and value for value in (api_version, kind, name))
            or namespace != expected_namespace
            or not isinstance(labels, dict)
            or labels.get("app.kubernetes.io/managed-by") != managed_by
        ):
            raise SystemExit("live resource identity is invalid")
        if kind == "Job" and migration_name.fullmatch(name):
            continue
        if kind == "PersistentVolumeClaim" and generated_pvc.fullmatch(name):
            continue
        owners = metadata.get("ownerReferences", [])
        if kind == "Pod" and isinstance(owners, list) and any(
            isinstance(owner, dict)
            and owner.get("controller") is True
            and owner.get("kind") in derived_pod_controllers
            for owner in owners
        ):
            continue
        identity = (api_version, kind, namespace, name)
        if identity in identities:
            raise SystemExit("live resource identity is duplicated")
        identities.add(identity)
for identity in sorted(identities):
    print(json.dumps(identity, separators=(",", ":"), ensure_ascii=True))
PY
}

assert_live_identity_delta() {
  local previous_identities="$1"
  local live_identities="$2"
  local allowed_missing_identities="$3"
  local live_missing_identities="$4"
  local live_unexpected_identities="$5"
  LC_ALL=C comm -23 "$previous_identities" "$live_identities" \
    > "$live_missing_identities"
  LC_ALL=C comm -13 "$previous_identities" "$live_identities" \
    > "$live_unexpected_identities"
  test ! -s "$live_unexpected_identities" || return 1
  if test -s "$live_missing_identities"; then
    test -z "$(LC_ALL=C comm -23 \
      "$live_missing_identities" "$allowed_missing_identities")" || return 1
  fi
}

assert_forward_identity_contract() {
  local allowed_missing_identities
  local current_identities
  local live_cluster_inventory
  local live_identities
  local live_missing_identities
  local live_namespaced_inventory
  local live_unexpected_identities
  local previous_only_identities
  local previous_identities
  allowed_missing_identities="$(mktemp "$evidence_dir/forward-allowed-missing.XXXXXX.txt")"
  current_identities="$(mktemp "$evidence_dir/forward-current-identities.XXXXXX.txt")"
  previous_identities="$(mktemp "$evidence_dir/forward-previous-identities.XXXXXX.txt")"
  live_identities="$(mktemp "$evidence_dir/forward-live-identities.XXXXXX.txt")"
  live_missing_identities="$(mktemp "$evidence_dir/forward-live-missing.XXXXXX.txt")"
  live_namespaced_inventory="$(mktemp "$evidence_dir/forward-live-namespaced.XXXXXX.json")"
  live_cluster_inventory="$(mktemp "$evidence_dir/forward-live-cluster.XXXXXX.json")"
  live_unexpected_identities="$(mktemp "$evidence_dir/forward-live-unexpected.XXXXXX.txt")"
  previous_only_identities="$(mktemp "$evidence_dir/forward-previous-only.XXXXXX.txt")"
  printf '%s\n' \
    '["apps/v1","Deployment","loom-dev","loom-personal-dev-web"]' \
    '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-acme-http01-ingress"]' \
    '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-capacity-manager-ingress"]' \
    '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-web-ingress"]' \
    '["v1","Service","loom-dev","loom-personal-dev-web"]' \
    > "$allowed_missing_identities"
  render_identity_set "$shadow_render" > "$current_identities"
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get \
    deployments.apps,statefulsets.apps,jobs.batch,persistentvolumeclaims,pods,\
serviceaccounts,roles.rbac.authorization.k8s.io,\
rolebindings.rbac.authorization.k8s.io,services,\
ingresses.networking.k8s.io,networkpolicies.networking.k8s.io \
    --namespace loom-dev \
    --selector app.kubernetes.io/managed-by=loom-personal-dev-control-plane \
    --output=json > "$live_namespaced_inventory"
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get \
    clusterroles.rbac.authorization.k8s.io,\
clusterrolebindings.rbac.authorization.k8s.io,\
validatingadmissionpolicies.admissionregistration.k8s.io,\
validatingadmissionpolicybindings.admissionregistration.k8s.io \
    --selector app.kubernetes.io/managed-by=loom-personal-dev-control-plane \
    --output=json > "$live_cluster_inventory"
  for inventory in "$live_namespaced_inventory" "$live_cluster_inventory"; do
    test "$(stat -c %s "$inventory")" -gt 0
    test "$(stat -c %s "$inventory")" -le 4194304
  done
  live_identity_set "$live_namespaced_inventory" "$live_cluster_inventory" \
    > "$live_identities"
  chmod 0600 "$allowed_missing_identities" "$current_identities" \
    "$previous_identities" "$live_identities" "$live_missing_identities" \
    "$live_namespaced_inventory" "$live_cluster_inventory" \
    "$live_unexpected_identities" "$previous_only_identities"
  if test -e "$previous_shadow_render" || test -L "$previous_shadow_render"; then
    test -f "$previous_shadow_render" && test ! -L "$previous_shadow_render"
    test "$(stat -c %u "$previous_shadow_render")" = "$(id -u)"
    test "$(stat -c %a "$previous_shadow_render")" = 600
    test "$(stat -c %h "$previous_shadow_render")" = 1
    test "$(sha256sum "$previous_shadow_render" | awk '{print $1}')" = \
      "$previous_shadow_sha256"
    render_identity_set "$previous_shadow_render" > "$previous_identities"
    LC_ALL=C comm -23 "$previous_identities" "$current_identities" \
      > "$previous_only_identities"
    test ! -s "$previous_only_identities"
    test -z "$(LC_ALL=C comm -13 \
      "$previous_identities" "$current_identities" \
      | LC_ALL=C comm -23 - "$allowed_missing_identities")"
    assert_live_identity_delta "$current_identities" "$live_identities" \
      "$allowed_missing_identities" "$live_missing_identities" \
      "$live_unexpected_identities"
  else
    test ! -s "$live_identities"
  fi
  rm -f "$allowed_missing_identities" "$current_identities" \
    "$previous_identities" "$live_identities" "$live_missing_identities" \
    "$live_namespaced_inventory" "$live_cluster_inventory" \
    "$live_unexpected_identities" "$previous_only_identities"
}

remove_forward_only_web_resources_for_rollback() {
  local previous_identities
  local web_identity_count
  previous_identities="$(mktemp "$evidence_dir/rollback-previous-identities.XXXXXX.txt")"
  render_identity_set "$previous_shadow_render" > "$previous_identities"
  chmod 0600 "$previous_identities"
  web_identity_count=0
  for identity in \
    '["apps/v1","Deployment","loom-dev","loom-personal-dev-web"]' \
    '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-web-ingress"]' \
    '["v1","Service","loom-dev","loom-personal-dev-web"]'; do
    if grep -Fqx -- "$identity" "$previous_identities"; then
      web_identity_count=$((web_identity_count + 1))
    fi
  done
  if test "$web_identity_count" -eq 0; then
    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev delete \
      deployment.apps/loom-personal-dev-web \
      service/loom-personal-dev-web \
      networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress \
      --ignore-not-found --wait=true --timeout=300s \
      > "$evidence_dir/rollback-web-cleanup.txt"
    chmod 0600 "$evidence_dir/rollback-web-cleanup.txt"
  else
    test "$web_identity_count" -eq 3
  fi
  rm -f "$previous_identities"
}

assert_forward_storage_lineage_contract() {
  local -a guard_arguments
  local live_storage_inventory
  live_storage_inventory="$(mktemp "$evidence_dir/forward-live-storage.XXXXXX.json")"
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get \
    statefulset.apps/loom-dev-postgres \
    statefulset.apps/loom-dev-minio \
    persistentvolumeclaim/data-loom-dev-postgres-0 \
    persistentvolumeclaim/data-loom-dev-minio-0 \
    persistentvolumeclaim/loom-personal-dev-scanner-cache \
    --namespace loom-dev \
    --ignore-not-found \
    --output=json > "$live_storage_inventory"
  if test ! -s "$live_storage_inventory"; then
    jq -cn '{apiVersion:"v1",items:[],kind:"List"}' > "$live_storage_inventory"
  fi
  test "$(stat -c %s "$live_storage_inventory")" -gt 0
  test "$(stat -c %s "$live_storage_inventory")" -le 4194304
  chmod 0600 "$live_storage_inventory"
  guard_arguments=(
    --current "$shadow_render"
    --live-inventory "$live_storage_inventory"
  )
  if test -e "$previous_shadow_render" || test -L "$previous_shadow_render"; then
    test -f "$previous_shadow_render" && test ! -L "$previous_shadow_render"
    test "$(stat -c %u "$previous_shadow_render")" = "$(id -u)"
    test "$(stat -c %a "$previous_shadow_render")" = 600
    test "$(stat -c %h "$previous_shadow_render")" = 1
    test "$(sha256sum "$previous_shadow_render" | awk '{print $1}')" = \
      "$previous_shadow_sha256"
    guard_arguments+=(--previous "$previous_shadow_render")
  fi
  uv run --no-sync python -m loom.personal_dev_storage_lineage_guard \
    "${guard_arguments[@]}"
  rm -f "$live_storage_inventory"
}

jq -e \
  --arg release "$trusted_release_sha256" \
  --arg yaml "$(sha256sum "$shadow_render" | awk '{print $1}')" \
  '.schema == "loom-personal-dev-control-plane-render-v1" and
   .mode == "shadow" and
   .resource_count == 38 and
   .release_sha256 == $release and
   .yaml_sha256 == $yaml and
   (.input_sha256 | test("^[0-9a-f]{64}$")) and
   (.source_sha | test("^[0-9a-f]{40}$")) and
   (.source_tree | test("^[0-9a-f]{40}$"))' \
  "$render_evidence"
```

Byte-review the YAML and canonical evidence. Confirm the render contains only
the shared Namespace, storage, migration, management API, existing Web SPA,
inert activation agent, RBAC, admission policy, and NetworkPolicy resources
described in the architecture. The renderer does not include Secret values or
a personal runtime namespace.

## 3. Recheck live read-only boundaries

The selected kubeconfig is explicit and canonical. Record its current context,
prove that no personal namespace exists, and require the separate global
capacity shadow to report ready at ceiling zero before opening the change
window. It must be flattened and self-contained, with no external credential
paths or exec credential plugins; status enforces those requirements and gives
every command a read-only anonymous snapshot of the exact validated bytes.

```bash
assert_reviewed_kubeconfig
kubectl --kubeconfig "$kubeconfig" config current-context \
  > "$evidence_dir/kube-context.txt"

assert_no_dynamic_namespaces

capacity_shadow_status \
  > "$evidence_dir/capacity-shadow.status.json"
test "$(tr -d '\n' < "$evidence_dir/capacity-shadow.status.json")" = \
  '{"executable_new_capacity_ceiling":0,"status":"ready"}'
chmod 0600 "$evidence_dir/kube-context.txt" \
  "$evidence_dir/capacity-shadow.status.json"
```

The capacity check is observation only. It does not prepare an execution epoch,
start a controller-local executor, or change a ceiling.

## 4. Provision the three Secrets through the approved channel

The renderer never creates credential values. At this point, provision or
verify the three pre-reviewed Secrets through the approved Secret channel.
Do not place values in shell arguments, logs, YAML, or the evidence directory.

- `loom-personal-dev-management` has exactly the scalar keys
  `postgres-user`, `postgres-password`, `postgres-database`, `svc-db-url`,
  `dev-instance-database-admin-url`, `minio-access-key`, `minio-secret-key`, and
  `secret-store-master-key`, plus file keys `admin-secrets.toml`, `config.json`,
  `capacity-lifecycle-token`, `capacity-lifecycle-ca.pem`,
  `capacity-lifecycle-certificate.pem`, `capacity-lifecycle-private-key.pem`,
  `capacity-reporter-ca.pem`, `capacity-reporter-certificate.pem`, and
  `capacity-reporter-private-key.pem`.
- `loom-personal-dev-activation-public` has only `public-key`.
- `loom-personal-dev-activation-agent` has only `private-key`.

Stop unless the approved channel confirms the exact key inventory and the
private activation key remains isolated from the management Pod.

## 5. Diff and apply only in the issue #1280 shadow window

Open and record the approved issue #1280 shadow window before the first apply.
Review the complete server-side diff. Any deletion, PVC replacement, personal
namespace, mutable image, enabled personal flag, nonzero activation replica,
or capacity resource outside the reviewed shadow is a stop condition. A first
installation also requires no existing package-owned top-level resource. An
upgrade requires the previous-image rollback manifest and new forward manifest
to have the same non-derived identity set. Live state must either match it or
be missing only a bounded subset of the reviewed additive identities: the two
#1573/#1576 NetworkPolicies and the Web Deployment, Service, and ingress
NetworkPolicy introduced by #1591. Every live-only identity, deletion,
replacement, or other missing identity remains a stop condition. The rollback
path explicitly removes only the three non-storage Web identities when the
previous manifest predates them; server-side apply is never treated as pruning.
Before asking the API server for a diff, an upgrade also requires the new and
previous-reviewed PostgreSQL and MinIO `volumeClaimTemplates`, live
StatefulSet templates, and generated Bound PVCs to agree after normalization of
only the documented Kubernetes API defaults and Longhorn binding fields. A
first installation requires all five exact shared-storage identities to be
absent. Any immutable claim-template, generated-claim, or forbidden acceptance
metadata drift is a stop condition; never recreate a StatefulSet or PVC to
bypass it. The same live guard is rerun immediately before apply.

```bash
assert_current_shadow_artifacts
assert_forward_storage_lineage_contract
assert_forward_identity_contract
test ! -e "$evidence_dir/server-side-diff.txt"
diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$shadow_render" \
  > "$evidence_dir/server-side-diff.txt" 2>&1 || diff_status=$?
test "$diff_status" -eq 0 || test "$diff_status" -eq 1
chmod 0600 "$evidence_dir/server-side-diff.txt"

assert_current_shadow_artifacts
assert_forward_storage_lineage_contract
assert_forward_identity_contract
assert_no_dynamic_namespaces
assert_zero_capacity
assert_reviewed_kubeconfig
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$shadow_render" \
  > "$evidence_dir/server-side-apply.txt"
chmod 0600 "$evidence_dir/server-side-apply.txt"
```

This is the only state-changing section. It applies only the byte-reviewed
shadow manifest. It does not enable personal lifecycle mutations or change
physical capacity.

## 6. Wait for storage, migration, and management readiness

```bash
kubectl --kubeconfig "$kubeconfig" -n loom-dev wait \
  --for=jsonpath='{.status.phase}'=Bound --timeout=600s \
  pvc -l app.kubernetes.io/managed-by=loom-personal-dev-control-plane

kubectl --kubeconfig "$kubeconfig" rollout status statefulset/loom-dev-postgres \
  --namespace loom-dev --timeout=600s
kubectl --kubeconfig "$kubeconfig" rollout status statefulset/loom-dev-minio \
  --namespace loom-dev --timeout=600s
kubectl --kubeconfig "$kubeconfig" -n loom-dev wait \
  --for=condition=complete --timeout=900s \
  job -l app=loom-personal-dev-migration
kubectl --kubeconfig "$kubeconfig" rollout status deployment/loom-personal-dev-management \
  --namespace loom-dev --timeout=300s
kubectl --kubeconfig "$kubeconfig" rollout status deployment/loom-personal-dev-web \
  --namespace loom-dev --timeout=300s

test "$(
  kubectl --kubeconfig "$kubeconfig" -n loom-dev get \
    deployment/loom-personal-dev-activation-agent \
    -o jsonpath='{.spec.replicas}'
)" = 0
```

The retained immutable migration Jobs are evidence. Status validates every
retained terminal Job/Pod pair and still requires the exact current trusted
migration to complete with exactly one Succeeded Pod. Each current or
historical Pod must carry both exact Job-name labels and the exact controller
owner reference, including the live Job UID; the Job, Pod, immutable images,
digests, template, and pod security boundary must all agree. Each retained Job
must be ownerless, and neither Job nor Pod may be pending deletion or carry a
finalizer; otherwise the purported durable evidence can disappear or depends
on an untrusted lifecycle controller. Retained evidence is admitted only by
the closed migration-v1 contract: the exact Alembic
command, Loom service digest repository, Secret-backed database environment,
single container, resource envelope, security contexts, default service
account without token mounting, `/tmp` emptyDir, and Kubernetes 1.36
API-materialized Job/Pod defaults. Scheduler-assigned `Pod.spec.nodeName` is
evidence of placement; forced placement fields in the Job template are
forbidden.

Do not loosen migration-v1 or delete retained evidence when the renderer or
Kubernetes defaults change. Add a separately tested later contract before
applying the new shape so old evidence keeps its original meaning. The
inventory remains bounded by the observer's 4 MiB response and 4,096-item
limits; a malformed, failed, running, orphaned, duplicated, unpaired,
mutable-image, or security-widened pair blocks readiness.

## 7. Capture canonical shadow status

Status requires the same trusted render inputs. It compares every current
package-owned live object with the locally rendered expected object, checks
generated Pods and PVCs, proves both personal flags false, proves activation at
zero, and executes only the manager's read-only mTLS observation command.

```bash
uv run --no-sync loom admin personal-dev-control-plane status \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$status_evidence"
chmod 0600 "$status_evidence"

canonical_status="$(mktemp "$evidence_dir/status.XXXXXX.json")"
jq -cS . "$status_evidence" > "$canonical_status"
cmp -s "$canonical_status" "$status_evidence"
rm -f "$canonical_status"

jq -e \
  --arg input "$(jq -r .input_sha256 "$render_evidence")" \
  --arg release "$trusted_release_sha256" \
  '.schema == "loom-personal-dev-control-plane-status-v1" and
   .mode == "shadow" and .ready == true and .blockers == [] and
   .manager_ceiling == 0 and .input_sha256 == $input and
   .release_sha256 == $release and
   .worker_available == false and
   any(.components[]; .name == "personal-workers" and
       .observed == 0 and .ready == true) and
   all(.components[]; .ready == true)' \
  "$status_evidence"
sha256sum "$status_evidence" > "$status_evidence.sha256"
chmod 0600 "$status_evidence.sha256"
```

The successful canonical shape is:

```json
{"blockers":[],"components":[{"name":"cluster-resources","observed":10,"ready":true},{"name":"manager","observed":1,"ready":true},{"name":"namespaced-resources","observed":34,"ready":true},{"name":"namespaces","observed":1,"ready":true},{"name":"personal-workers","observed":0,"ready":true},{"name":"runtime-class","observed":1,"ready":true},{"name":"web","observed":1,"ready":true}],"input_sha256":"<render-input-sha256>","manager_ceiling":0,"mode":"shadow","ready":true,"release_sha256":"<trusted-release-sha256>","schema":"loom-personal-dev-control-plane-status-v1","worker_available":false}
```

The namespaced observed count may include retained successful migration
evidence after later upgrades or rollbacks. The observer validates every pair
within its bounded response and item limits; all component names, blocker
codes, and digest fields remain bounded.

## 8. Roll back without deleting durable state

Rollback is another issue #1280 window action. Stop if any
`loom-dev-<owner>` namespace exists, and stop if any `loom-build-*` namespace
exists. Verify the previous manifest, its matching trusted release, and its
recorded SHA-256 before continuing.
Reapply the previous reviewed shadow with the same field manager; do not
synthesize a replacement manifest from live state.

Stop unless the previous management image is explicitly proven
schema-compatible with the current database state. This rehearsal does not
downgrade schema, restore a database, or infer compatibility from a completed
historical Job.

Server-side apply does not remove objects absent from the older manifest.
Before mutation, the shared forward interlock therefore permits only the exact
reviewed additive identities and forbids every removal or live-only identity.
If the previous manifest predates #1591, rollback explicitly deletes only the
stateless Web Deployment, Service, and ingress NetworkPolicy after reapplying
that manifest. PVCs, databases, buckets, Secrets, migration evidence, and every
other identity remain untouched. Any different identity delta is a rollback
stop condition; this runbook never guesses which live object is safe to delete.

```bash
test -f "$previous_shadow_render" && test ! -L "$previous_shadow_render"
test "$(stat -c %u "$previous_shadow_render")" = "$(id -u)"
test "$(stat -c %a "$previous_shadow_render")" = 600
test "$(stat -c %h "$previous_shadow_render")" = 1
test "$(sha256sum "$previous_shadow_render" | awk '{print $1}')" = "$previous_shadow_sha256"
test -f "$previous_profile" && test ! -L "$previous_profile"
test "$(stat -c %u "$previous_profile")" = "$(id -u)"
test "$(stat -c %h "$previous_profile")" = 1
test "$(stat -c %s "$previous_profile")" -gt 0
test "$(stat -c %s "$previous_profile")" -le 1048576
test -f "$previous_trusted_release" && test ! -L "$previous_trusted_release"
test "$(stat -c %u "$previous_trusted_release")" = "$(id -u)"
test "$(stat -c %a "$previous_trusted_release")" = 600
test "$(stat -c %h "$previous_trusted_release")" = 1
test "$(sha256sum "$previous_trusted_release" | awk '{print $1}')" = \
  "$previous_trusted_release_sha256"
previous_profile_sha256="$(sha256sum "$previous_profile" | awk '{print $1}')"

assert_previous_shadow_artifacts() {
  test -f "$previous_shadow_render" && test ! -L "$previous_shadow_render"
  test "$(stat -c %u "$previous_shadow_render")" = "$(id -u)"
  test "$(stat -c %a "$previous_shadow_render")" = 600
  test "$(stat -c %h "$previous_shadow_render")" = 1
  test "$(sha256sum "$previous_shadow_render" | awk '{print $1}')" = \
    "$previous_shadow_sha256"
  test -f "$previous_profile" && test ! -L "$previous_profile"
  test "$(stat -c %u "$previous_profile")" = "$(id -u)"
  test "$(stat -c %h "$previous_profile")" = 1
  test "$(stat -c %s "$previous_profile")" -gt 0
  test "$(stat -c %s "$previous_profile")" -le 1048576
  test "$(sha256sum "$previous_profile" | awk '{print $1}')" = \
    "$previous_profile_sha256"
  test "$(sha256sum "$previous_trusted_release" | awk '{print $1}')" = \
    "$previous_trusted_release_sha256"
  assert_status_input_safety \
    "$previous_profile" "$previous_trusted_release" \
    "$previous_trusted_release_sha256"
}

previous_shadow_render_tmp="$(mktemp "$evidence_dir/previous-shadow.XXXXXX.yaml")"
previous_render_evidence_tmp="$(mktemp "$evidence_dir/previous-shadow.XXXXXX.json")"
if ! uv run --no-sync loom admin personal-dev-control-plane render \
  --file "$previous_profile" \
  --trusted-release-file "$previous_trusted_release" \
  --trusted-release-sha256 "$previous_trusted_release_sha256" \
  > "$previous_shadow_render_tmp" 2> "$previous_render_evidence_tmp"; then
  rm -f "$previous_shadow_render_tmp" "$previous_render_evidence_tmp"
  exit 1
fi
chmod 0600 "$previous_shadow_render_tmp" "$previous_render_evidence_tmp"
if ! cmp -s "$previous_shadow_render_tmp" "$previous_shadow_render"; then
  rm -f "$previous_shadow_render_tmp" "$previous_render_evidence_tmp"
  exit 1
fi
rm -f "$previous_shadow_render_tmp" "$previous_render_evidence_tmp"

forbidden_namespaces="$(
  kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
    | jq -r '.items[].metadata.name |
        select(startswith("loom-dev-") or startswith("loom-build-"))'
)"
test -z "$forbidden_namespaces"

assert_forward_storage_lineage_contract
test ! -e "$evidence_dir/rollback-server-side-diff.txt"
rollback_diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$previous_shadow_render" \
  > "$evidence_dir/rollback-server-side-diff.txt" 2>&1 || rollback_diff_status=$?
test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1
chmod 0600 "$evidence_dir/rollback-server-side-diff.txt"
assert_current_shadow_artifacts
assert_previous_shadow_artifacts
assert_forward_storage_lineage_contract
assert_forward_identity_contract
assert_no_dynamic_namespaces
assert_zero_capacity
assert_reviewed_kubeconfig
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$previous_shadow_render" \
  > "$evidence_dir/rollback-apply.txt"
chmod 0600 "$evidence_dir/rollback-apply.txt"
remove_forward_only_web_resources_for_rollback
```

Wait for the previous storage, migration, and management objects, then bind the
read-only result to the previous profile and trusted release:

```bash
kubectl --kubeconfig "$kubeconfig" rollout status statefulset/loom-dev-postgres \
  --namespace loom-dev --timeout=600s
kubectl --kubeconfig "$kubeconfig" rollout status statefulset/loom-dev-minio \
  --namespace loom-dev --timeout=600s
kubectl --kubeconfig "$kubeconfig" -n loom-dev wait \
  --for=condition=complete --timeout=900s \
  job -l app=loom-personal-dev-migration
kubectl --kubeconfig "$kubeconfig" rollout status deployment/loom-personal-dev-management \
  --namespace loom-dev --timeout=300s

uv run --no-sync loom admin personal-dev-control-plane status \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$previous_profile" \
  --trusted-release-file "$previous_trusted_release" \
  --trusted-release-sha256 "$previous_trusted_release_sha256" \
  > "$rollback_status_evidence"
chmod 0600 "$rollback_status_evidence"
jq -e '.schema == "loom-personal-dev-control-plane-status-v1" and
       .mode == "shadow" and .ready == true and .blockers == [] and
       .manager_ceiling == 0 and .worker_available == false and
       any(.components[]; .name == "personal-workers" and
           .observed == 0 and .ready == true) and
       all(.components[]; .ready == true)' \
  "$rollback_status_evidence"
sha256sum "$rollback_status_evidence" > "$rollback_status_evidence.sha256"
chmod 0600 "$rollback_status_evidence.sha256"
```

Retain both migration histories and all PVCs. Because server-side apply is per
resource, an interrupted rollback can leave a mixed but inert shadow version.
Stop, capture status, and diagnose; do not widen authority or remove storage.

Finally record hashes for the non-secret evidence set without printing any
credential content:

```bash
sha256sum "$shadow_render" "$render_evidence" "$status_evidence" \
  "$evidence_dir/capacity-shadow.status.json" \
  > "$evidence_dir/final-evidence.sha256"
chmod 0600 "$evidence_dir/final-evidence.sha256"
```

Shadow readiness proves only the shared management foundation and the separate
zero-ceiling capacity boundary. Acceptance enablement, single-owner application
deployment, and physical worker execution require later, independently
reviewed interlocks.
