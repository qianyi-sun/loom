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

## Stop conditions

Before rendering, and again immediately before apply or rollback:
stop if any `loom-dev-<owner>` namespace exists. Also stop for an unapproved
source commit or tree, a mutable or mismatched image, a changed trusted-release file, a
noncanonical evidence record, missing Secret keys, a nonzero capacity-manager
ceiling, an unexpected package-owned resource, a failed migration, an unbound
PVC, a nonzero activation replica count, or an unavailable rollback artifact.

Do not improvise cleanup. Never delete PVCs, databases, buckets, migration
evidence, Secrets, or namespaces as part of this rehearsal. Retain the current
zero-capacity state and escalate when a stop condition is reached.

## 1. Prepare owner-only evidence

Use one exact CI-approved source commit and tree. The trusted-release document
must be the canonical, current-user-owned mode-`0600` file produced by the
protected image release for that source. It binds immutable digests for the
service, builder, activation agent, PostgreSQL, MinIO, and MinIO client.

```bash
umask 077
evidence_dir=artifacts/personal-dev/management-shadow
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
test -f "$trusted_release" && test ! -L "$trusted_release"
test "$(stat -c %u "$trusted_release")" = "$(id -u)"
test "$(stat -c %a "$trusted_release")" = 600
test "$(stat -c %h "$trusted_release")" = 1
test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
test -f "$profile" && test ! -L "$profile"
test -f "$kubeconfig" && test ! -L "$kubeconfig"
test "$(realpath -e "$kubeconfig")" = "$kubeconfig"
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

jq -e \
  --arg release "$trusted_release_sha256" \
  --arg yaml "$(sha256sum "$shadow_render" | awk '{print $1}')" \
  '.schema == "loom-personal-dev-control-plane-render-v1" and
   .mode == "shadow" and
   .resource_count == 32 and
   .release_sha256 == $release and
   .yaml_sha256 == $yaml and
   (.input_sha256 | test("^[0-9a-f]{64}$")) and
   (.source_sha | test("^[0-9a-f]{40}$")) and
   (.source_tree | test("^[0-9a-f]{40}$"))' \
  "$render_evidence"
```

Byte-review the YAML and canonical evidence. Confirm the render contains only
the shared Namespace, storage, migration, management service, inert activation
agent, RBAC, admission policy, and NetworkPolicy resources described in the
architecture. The renderer does not include Secret values or a personal
application.

## 3. Recheck live read-only boundaries

The selected kubeconfig is explicit and canonical. Record its current context,
prove that no personal namespace exists, and require the separate global
capacity shadow to report ready at ceiling zero before opening the change
window.

```bash
kubectl --kubeconfig "$kubeconfig" config current-context \
  > "$evidence_dir/kube-context.txt"

personal_namespaces="$(
  kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
    | jq -r '.items[].metadata.name | select(startswith("loom-dev-"))'
)"
test -z "$personal_namespaces"

uv run --no-sync loom admin capacity-control-plane status \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
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
or capacity resource outside the reviewed shadow is a stop condition.

```bash
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$shadow_render"

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

test "$(
  kubectl --kubeconfig "$kubeconfig" -n loom-dev get \
    deployment/loom-personal-dev-activation-agent \
    -o jsonpath='{.spec.replicas}'
)" = 0
```

The retained immutable migration Jobs are evidence. Status accepts only a
bounded history of successful terminal Job/Pod pairs and still requires the
exact current trusted migration to complete.

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
   all(.components[]; .ready == true)' \
  "$status_evidence"
sha256sum "$status_evidence" > "$status_evidence.sha256"
chmod 0600 "$status_evidence.sha256"
```

The successful canonical shape is:

```json
{"blockers":[],"components":[{"name":"cluster-resources","observed":10,"ready":true},{"name":"manager","observed":1,"ready":true},{"name":"namespaced-resources","observed":27,"ready":true},{"name":"namespaces","observed":1,"ready":true},{"name":"runtime-class","observed":1,"ready":true}],"input_sha256":"<render-input-sha256>","manager_ceiling":0,"mode":"shadow","ready":true,"release_sha256":"<trusted-release-sha256>","schema":"loom-personal-dev-control-plane-status-v1"}
```

The namespaced observed count may include bounded retained successful migration
evidence after a later upgrade or rollback. All component names, blocker codes,
and digest fields remain bounded.

## 8. Roll back without deleting state

Rollback is another issue #1280 window action. Stop if any
`loom-dev-<owner>` namespace exists. Verify the previous manifest, its matching
trusted release, and its recorded SHA-256 before continuing.
Reapply the previous reviewed shadow with the same field manager; do not
synthesize a replacement manifest from live state.

Stop unless the previous management image is explicitly proven
schema-compatible with the current database state. This rehearsal does not
downgrade schema, restore a database, or infer compatibility from a completed
historical Job.

```bash
test -f "$previous_shadow_render" && test ! -L "$previous_shadow_render"
test "$(stat -c %u "$previous_shadow_render")" = "$(id -u)"
test "$(stat -c %a "$previous_shadow_render")" = 600
test "$(stat -c %h "$previous_shadow_render")" = 1
test "$(sha256sum "$previous_shadow_render" | awk '{print $1}')" = "$previous_shadow_sha256"
test -f "$previous_profile" && test ! -L "$previous_profile"
test -f "$previous_trusted_release" && test ! -L "$previous_trusted_release"
test "$(stat -c %u "$previous_trusted_release")" = "$(id -u)"
test "$(stat -c %a "$previous_trusted_release")" = 600
test "$(stat -c %h "$previous_trusted_release")" = 1
test "$(sha256sum "$previous_trusted_release" | awk '{print $1}')" = \
  "$previous_trusted_release_sha256"

personal_namespaces="$(
  kubectl --kubeconfig "$kubeconfig" get namespaces -o json \
    | jq -r '.items[].metadata.name | select(startswith("loom-dev-"))'
)"
test -z "$personal_namespaces"

kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$previous_shadow_render"
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$previous_shadow_render" \
  > "$evidence_dir/rollback-apply.txt"
chmod 0600 "$evidence_dir/rollback-apply.txt"
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
       .manager_ceiling == 0 and all(.components[]; .ready == true)' \
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
zero-ceiling capacity boundary. Acceptance enablement, two-owner application
deployment, and physical worker execution require later, independently
reviewed interlocks.
