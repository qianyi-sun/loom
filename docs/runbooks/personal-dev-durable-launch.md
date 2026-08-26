# Personal-development durable zero-capacity launch

This runbook enables the personal application plane as a durable operational
service after the complete single-owner zero-capacity acceptance has finished and
returned the shared management plane to its byte-reviewed inert shadow. It does
not extend the acceptance window and does not leave an acceptance manifest
running. The durable manifest is generated from a separate operational plan by
`render-operational` and observed by `status-operational`.

The launch remains deliberately non-executable. The global manager's executable
new-capacity ceiling stays exactly `0`, `worker_available` stays `false`, no
personal worker may exist, and this procedure submits no task. A successful
source build, personal application route, or operational status is not evidence
that OLDLAB, GB10, a model, or a task executor is available. #906/#822 retain
authority over physical task capacity and any nonzero ceiling.

Repository merge, a completed acceptance result, DNS configuration, and a
successful render are necessary but not sufficient live authority. Execute the
mutable sections only inside an explicit #1280 durable-launch window with the
exact candidate, operator, rollback manifest, and evidence paths reviewed in
advance.

## Stop conditions and rollback boundary

Stop before apply if any of these conditions holds:

- the full `personal-dev-zero-capacity-acceptance.md` procedure did not finish,
  including final owner destroy and inert-shadow rollback;
- the operational plan does not bind the exact canonical acceptance result and
  the retained shadow-rollback evidence;
- source, trusted-release, operational-plan, kubeconfig, backup/restore,
  RuntimeClass, scanner, Secret-key inventory, DNS, TLS, or Ingress evidence
  differs from the reviewed bytes;
- the live shared plane is not the exact ready shadow, a `loom-dev-*` or
  `loom-build-*` namespace exists, or a personal worker exists;
- the global-manager authority, execution state or epoch, configuration-epoch
  floor, observer principal, or executable ceiling differs from the
  operational plan; or
- a server-side diff removes or replaces storage, changes resource identity or
  authority, admits a mutable image, adds a worker or capacity actuator, or
  touches an unrelated namespace.

Never delete a PVC or namespace directly. If a personal environment exists at
rollback time, retire it through its authenticated manager-first destroy path.
Do not apply the shadow until every personal namespace and bounded builder
sandbox is gone. If normal retirement cannot complete, keep the operational
plane at ceiling `0`, retain its data and evidence, and stop without improvised
cleanup.

## 1. Bind the accepted release and owner-only evidence

Use one Bash session. Substitute only paths and identities approved for the
launch window. The checkout must be detached at the exact merged `dev` commit
bound by the trusted release and operational plan.

```bash
set -euo pipefail
umask 077

repo="<absolute-clean-detached-loom-checkout>"
profile="$repo/deploy/dev-fleet/personal-dev-control-plane.toml"
loom_cli="$repo/.venv/bin/loom"
trusted_release="<absolute-owner-only-trusted-release.json>"
trusted_release_sha256="<reviewed-trusted-release-sha256>"
operational_plan="<absolute-owner-only-operational-plan.json>"
operational_plan_sha256="<reviewed-operational-plan-sha256>"
acceptance_result="<absolute-owner-only-acceptance-result.json>"
rollback_evidence="<absolute-owner-only-acceptance-shadow-rollback-evidence>"
backup_restore_evidence="<absolute-owner-only-backup-restore-evidence>"
trusted_launcher_profile="<absolute-owner-only-source-derived-trusted-launcher-profile>"
scanner_finding_policy="<absolute-owner-only-source-derived-scanner-finding-policy>"
kubeconfig="<absolute-reviewed-self-contained-mode-0600-kubeconfig>"
expected_kube_context="<reviewed-context>"

test -x "$loom_cli"

evidence_dir="<new-absolute-owner-only-durable-launch-evidence-directory>"
install -d -m 0700 "$evidence_dir"
operational_render="$evidence_dir/reviewed-operational.yaml"
operational_render_evidence="$evidence_dir/reviewed-operational.render.json"
shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"
shadow_render_evidence="$evidence_dir/reviewed-rollback-shadow.render.json"
pre_launch_status="$evidence_dir/pre-launch-shadow.status.json"
post_launch_status="$evidence_dir/post-launch-operational.status.json"
final_launch_status="$evidence_dir/final-operational.status.json"
rollback_status="$evidence_dir/rollback-shadow.status.json"

assert_owner_only_file() {
  local path="$1"
  test -f "$path" && test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 600
  test "$(stat -c %h "$path")" = 1
}

for path in \
  "$trusted_release" \
  "$operational_plan" \
  "$acceptance_result" \
  "$rollback_evidence" \
  "$backup_restore_evidence" \
  "$trusted_launcher_profile" \
  "$scanner_finding_policy" \
  "$kubeconfig"; do
  assert_owner_only_file "$path"
done

test "$(sha256sum "$trusted_release" | awk '{print $1}')" = "$trusted_release_sha256"
test "$(sha256sum "$operational_plan" | awk '{print $1}')" = "$operational_plan_sha256"
test "$(sha256sum "$acceptance_result" | awk '{print $1}')" = \
  "$(jq -r .approval.acceptance_result_sha256 "$operational_plan")"
test "$(sha256sum "$rollback_evidence" | awk '{print $1}')" = \
  "$(jq -r .approval.rollback_evidence_sha256 "$operational_plan")"
test "$(sha256sum "$backup_restore_evidence" | awk '{print $1}')" = \
  "$(jq -r .storage.backup_restore_evidence_sha256 "$operational_plan")"
test "$(sha256sum "$trusted_launcher_profile" | awk '{print $1}')" = \
  "$(jq -r .builder.trusted_launcher_profile_sha256 "$operational_plan")"
test "$(sha256sum "$scanner_finding_policy" | awk '{print $1}')" = \
  "$(jq -r .builder.scanner_finding_policy_sha256 "$operational_plan")"

operational_evidence_args=(
  --source-root "$repo"
  --trusted-launcher-profile-file "$trusted_launcher_profile"
  --scanner-finding-policy-file "$scanner_finding_policy"
  --backup-restore-evidence-file "$backup_restore_evidence"
)

cd "$repo"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test "$(git rev-parse HEAD)" = "$(jq -r .source.commit "$operational_plan")"
test "$(git rev-parse 'HEAD^{tree}')" = "$(jq -r .source.tree "$operational_plan")"
test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = \
  "$expected_kube_context"
```

The operational plan is canonical, non-secret control evidence. It must bind
the same exact release and shadow manifest accepted by the implementation,
plus the final acceptance result, shadow-rollback evidence, manager boundary,
principals, quotas, RuntimeClass, scanner, publisher, registry, activation key,
schema head, and storage evidence. `approved_at` is an audit timestamp, not an
expiry. Choosing a distant acceptance expiry is not an operational plan.

## 2. Prove the accepted procedure ended in the exact shadow

The acceptance result must be canonical and report the exact acceptance owner,
two-environment create/update and isolation, destroy, retained-data redeploy,
and final cleanup. The
acceptance runbook's last apply and status must prove the byte-reviewed shadow,
not an expired or still-active acceptance manifest.

```bash
test "$(jq -r .schema "$acceptance_result")" = \
  "loom-personal-dev-zero-capacity-acceptance-result-v1"
test "$(jq -r .release_sha256 "$acceptance_result")" = "$trusted_release_sha256"
test "$(jq -r .shadow_manifest_sha256 "$acceptance_result")" = \
  "$(jq -r .release.shadow_manifest_sha256 "$operational_plan")"

"$loom_cli" admin personal-dev-control-plane status \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$pre_launch_status"
chmod 0600 "$pre_launch_status"
jq -e '
  .schema == "loom-personal-dev-control-plane-status-v1" and
  .mode == "shadow" and .ready == true and .blockers == [] and
  .manager_ceiling == 0 and all(.components[]; .ready == true)
' "$pre_launch_status" >/dev/null

kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get namespaces -o json \
  | jq -e '[.items[].metadata.name |
      select(startswith("loom-dev-") or startswith("loom-build-"))] |
      length == 0' >/dev/null
kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get deployments -A -o json \
  | jq -e '[.items[] |
      select(
        .metadata.namespace == "loom-dev" or
        (.metadata.namespace | startswith("loom-dev-")) or
        (.metadata.namespace | startswith("loom-build-"))) |
      select(
        .metadata.name == "loom-worker" or
        ((.metadata.name // "") | test("^loom-worker-g[1-9][0-9]*$")) or
        .metadata.labels.app == "loom-worker" or
        .spec.template.metadata.labels.app == "loom-worker" or
        any(.spec.template.spec.containers[]?;
          .name == "worker" or
          ((.image // "") | test("/loom-worker(@sha256:|$)")) or
          any(.env[]?;
            .name == "LOOM_SVC_K8S_WORKER_ENABLED" and .value == "true")))] |
      length == 0' >/dev/null
```

Also re-run the acceptance runbook's exact Secret-key inventory,
backup/restore, storage-lineage, migration, scanner-cache, RuntimeClass,
global-manager, source, and kubeconfig interlocks. Do not shorten them to the
illustrative observations above.

## 3. Prove DNS, TLS, and management ingress before enablement

The shadow already renders the management Ingress, so DNS and certificate
readiness can be established without enabling personal mutation. Record the
reviewed DNS answers and require the exact cert-manager Certificate and Ingress
host before the operational apply.

```bash
management_host="$(python3 -c '
import sys, tomllib
from urllib.parse import urlsplit
with open(sys.argv[1], "rb") as stream:
    print(urlsplit(tomllib.load(stream)["network"]["public_origin"]).hostname)
' "$profile")"
test -n "$management_host"
solver_port="$(python3 -c '
import sys, tomllib
with open(sys.argv[1], "rb") as stream:
    print(tomllib.load(stream)["network"]["acme_http01_solver_port"])
' "$profile")"
test "$solver_port" = 8089

getent ahosts "$management_host" > "$evidence_dir/management.dns.txt"
test -s "$evidence_dir/management.dns.txt"
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  networkpolicy/loom-personal-dev-acme-http01-ingress -o json \
  > "$evidence_dir/management.acme-http01-network-policy.json"
jq -e --argjson port "$solver_port" '
  .spec.podSelector.matchLabels == {"acme.cert-manager.io/http01-solver":"true"} and
  .spec.policyTypes == ["Ingress"] and
  (.spec | has("egress") | not) and
  (.spec.ingress[0] | has("from") | not) and
  .spec.ingress[0].ports == [{port:$port,protocol:"TCP"}]
' "$evidence_dir/management.acme-http01-network-policy.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  ingress/loom-personal-dev-management -o json \
  > "$evidence_dir/management.ingress.json"
jq -e --arg host "$management_host" '
  .spec.rules == [{host:$host,http:.spec.rules[0].http}] and
  .spec.tls[0].hosts == [$host]
' "$evidence_dir/management.ingress.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  certificate/loom-personal-dev-management-tls -o json \
  > "$evidence_dir/management.certificate.json"
jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' \
  "$evidence_dir/management.certificate.json" >/dev/null
curl --fail --silent --show-error --proto '=https' --tlsv1.2 \
  "https://$management_host/api/v1/health" \
  > "$evidence_dir/management.shadow-health.json"
```

Do not use `--resolve`, `-k`, an alternate CA, or a local port-forward as DNS,
certificate, or public-route evidence.

## 4. Render and byte-review operational and rollback manifests

Render both manifests before mutation. The operational render must be the exact
trusted release with lifecycle and restricted builder enabled, Kubernetes
worker disabled, one activation replica, the operational-plan binding, and no
capacity actuator or worker. The shadow render must be byte-identical to the
rollback digest in the operational plan.

```bash
operational_tmp="$(mktemp "$evidence_dir/operational.XXXXXX.yaml")"
operational_evidence_tmp="$(mktemp "$evidence_dir/operational.XXXXXX.json")"
if ! "$loom_cli" admin personal-dev-control-plane \
  render-operational \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${operational_evidence_args[@]}" \
  > "$operational_tmp" 2> "$operational_evidence_tmp"; then
  exit 1
fi
chmod 0600 "$operational_tmp" "$operational_evidence_tmp"
mv "$operational_tmp" "$operational_render"
mv "$operational_evidence_tmp" "$operational_render_evidence"

shadow_tmp="$(mktemp "$evidence_dir/shadow.XXXXXX.yaml")"
shadow_evidence_tmp="$(mktemp "$evidence_dir/shadow.XXXXXX.json")"
if ! "$loom_cli" admin personal-dev-control-plane render \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$shadow_tmp" 2> "$shadow_evidence_tmp"; then
  exit 1
fi
chmod 0600 "$shadow_tmp" "$shadow_evidence_tmp"
mv "$shadow_tmp" "$shadow_render"
mv "$shadow_evidence_tmp" "$shadow_render_evidence"

operational_render_sha256="$(sha256sum "$operational_render" | awk '{print $1}')"
shadow_render_sha256="$(sha256sum "$shadow_render" | awk '{print $1}')"
test "$shadow_render_sha256" = \
  "$(jq -r .release.shadow_manifest_sha256 "$operational_plan")"
jq -e --arg plan "$operational_plan_sha256" --arg yaml "$operational_render_sha256" '
  .schema == "loom-personal-dev-control-plane-render-v1" and
  .mode == "operational" and .operational_plan_sha256 == $plan and
  .yaml_sha256 == $yaml
' "$operational_render_evidence" >/dev/null
jq -e --arg yaml "$shadow_render_sha256" '
  .schema == "loom-personal-dev-control-plane-render-v1" and
  .mode == "shadow" and .yaml_sha256 == $yaml
' "$shadow_render_evidence" >/dev/null
```

Byte-review the complete YAML and canonical render records. Record a structural
comparison proving that resource identities, storage classes, requests, claim
templates, RBAC/admission scope, images, and network boundaries are unchanged
except for the reviewed operational binding, lifecycle/builder flags,
readiness contract, and activation replica.

## 5. Diff and apply the durable operational manifest

```bash
diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$operational_render" \
  > "$evidence_dir/operational.server-side-diff.txt" 2>&1 || diff_status=$?
test "$diff_status" -eq 0 || test "$diff_status" -eq 1
chmod 0600 "$evidence_dir/operational.server-side-diff.txt"

# Re-run every source, file-identity, release, storage, Secret, RuntimeClass,
# manager-ceiling, namespace, worker, DNS, TLS, and diff interlock immediately
# before this apply.
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$operational_render" \
  > "$evidence_dir/operational.server-side-apply.txt"
chmod 0600 "$evidence_dir/operational.server-side-apply.txt"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-activation-agent --timeout=300s

"$loom_cli" admin personal-dev-control-plane \
  status-operational \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${operational_evidence_args[@]}" \
  > "$post_launch_status"
chmod 0600 "$post_launch_status"
jq -e --arg plan "$operational_plan_sha256" '
  .schema == "loom-personal-dev-control-plane-status-v1" and
  .mode == "operational" and .operational_plan_sha256 == $plan and
  .ready == true and .blockers == [] and .application_ready == true and
  .capacity_publication_ready == true and .manager_ceiling == 0 and
  .worker_available == false and all(.components[]; .ready == true)
' "$post_launch_status" >/dev/null
```

An apply record, Ready Deployment, or HTTP `200` alone does not authorize
leaving the plane enabled. The canonical `status-operational` result owns the
Kubernetes and global-manager boundary; the next section owns the external
route behavior.

## 6. Verify one operational owner and the stable routes without a task

Use one reviewed authenticated owner session and a small arbitrary source
snapshot. This is a personal application launch smoke, not another substitute
for the completed single-owner acceptance. Do not submit a TaskSet, Batch, Trial,
Slurm job, or model call.

```bash
owner_xdg="<absolute-mode-0700-launch-owner-xdg-config-root>"
owner_source="<absolute-small-arbitrary-source-root>"
owner_name="<reviewed-launch-smoke-name>"
assert_owner_only_file "$owner_xdg/loom/config.toml"

XDG_CONFIG_HOME="$owner_xdg" loom auth whoami \
  > "$evidence_dir/launch-owner.whoami.txt"
XDG_CONFIG_HOME="$owner_xdg" loom service up \
  --environment "dev-$owner_name" \
  --source-root "$owner_source" \
  --min-slots 0 \
  --max-slots 2 \
  > "$evidence_dir/launch-owner.deploy.txt" 2>&1
XDG_CONFIG_HOME="$owner_xdg" loom dev status "$owner_name" --format json \
  > "$evidence_dir/launch-owner.status.json"
chmod 0600 "$evidence_dir/launch-owner.status.json"
jq -e '
  .status == "ready" and .application_status == "ready" and
  .capacity_prepared == true and .worker_available == false and
  .min_slots == 0
' "$evidence_dir/launch-owner.status.json" >/dev/null

for url in \
  "https://$owner_name.dev.yylx.world/api/v1/health" \
  "https://$owner_name.dev.yylx.world/" \
  "https://cp-$owner_name.dev.yylx.world/healthz" \
  "https://gw-$owner_name.dev.yylx.world/healthz"; do
  curl --fail --silent --show-error --proto '=https' --tlsv1.2 "$url" \
    >> "$evidence_dir/launch-owner.routes.txt"
done
chmod 0600 "$evidence_dir/launch-owner.routes.txt"
```

Record DNS answers and certificate chains for the management, application,
Control Plane, and Gateway hosts. Cross-check that the stable Ingress points to
the acknowledged deployment generation. A route check through a port-forward,
an insecure TLS option, or a candidate-generation Service is not stable-route
evidence.

Retire the launch-smoke environment through the same normal path so the
rollback path remains immediately executable:

```bash
XDG_CONFIG_HOME="$owner_xdg" loom dev destroy "$owner_name" --format json \
  > "$evidence_dir/launch-owner.destroy.json"
chmod 0600 "$evidence_dir/launch-owner.destroy.json"
jq -e '.status == "deleted"' "$evidence_dir/launch-owner.destroy.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get namespaces -o json \
  | jq -e '[.items[].metadata.name |
      select(startswith("loom-dev-") or startswith("loom-build-"))] |
      length == 0' >/dev/null
```

## 7. Record the steady operational result

Re-run `status-operational`, the manager-ceiling and worker inventories, DNS/TLS
checks, Secret-key inventory, scanner init, and a server-side diff against the
reviewed operational manifest. The final diff must be empty.

```bash
"$loom_cli" admin personal-dev-control-plane \
  status-operational \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${operational_evidence_args[@]}" \
  > "$final_launch_status"
chmod 0600 "$final_launch_status"
jq -e '
  .mode == "operational" and .ready == true and .blockers == [] and
  .manager_ceiling == 0 and .worker_available == false
' "$final_launch_status" >/dev/null

final_diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$operational_render" \
  > "$evidence_dir/final-operational.server-side-diff.txt" 2>&1 \
  || final_diff_status=$?
test "$final_diff_status" -eq 0
test ! -s "$evidence_dir/final-operational.server-side-diff.txt"

jq -cS -n \
  --arg acceptance_result "$(sha256sum "$acceptance_result" | awk '{print $1}')" \
  --arg operational_plan "$operational_plan_sha256" \
  --arg operational_manifest "$operational_render_sha256" \
  --arg release "$trusted_release_sha256" \
  --arg rollback_manifest "$shadow_render_sha256" \
  '{acceptance_result_sha256:$acceptance_result,
    operational_manifest_sha256:$operational_manifest,
    operational_plan_sha256:$operational_plan,
    release_sha256:$release,
    rollback_manifest_sha256:$rollback_manifest,
    schema:"loom-personal-dev-durable-launch-result-v1",
    worker_available:false,
    executable_new_capacity_ceiling:0}' \
  > "$evidence_dir/durable-launch-result.json"
chmod 0600 "$evidence_dir/durable-launch-result.json"
sha256sum "$evidence_dir/durable-launch-result.json" \
  > "$evidence_dir/durable-launch-result.sha256"
chmod 0600 "$evidence_dir/durable-launch-result.sha256"
```

Retain the exact trusted release, operational plan, completed acceptance and
rollback evidence, backup/restore record, both reviewed manifests and render
records, diffs, applies, statuses, DNS/TLS/route records, owner smoke records,
canonical launch result, and hashes. The final live state may remain
operational only while the continuous boundary remains ceiling `0`,
`worker_available=false`, and no personal worker exists.

## 8. Byte-reviewed rollback to inert shadow

Use this path for any launch failure or later authorized operational shutdown.
First stop new user activity at the owning interface and inventory every
personal environment. Each owner must use `loom dev destroy`, or the explicitly
approved authenticated manager-first administrative equivalent, and wait for
durable deletion. Never substitute `kubectl delete namespace`.

After all personal and builder namespaces are gone, re-render the shadow from
the same profile and trusted release and require byte equality with the file
reviewed before operational apply:

```bash
shadow_recheck="$(mktemp "$evidence_dir/shadow-recheck.XXXXXX.yaml")"
shadow_recheck_evidence="$(mktemp "$evidence_dir/shadow-recheck.XXXXXX.json")"
"$loom_cli" admin personal-dev-control-plane render \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$shadow_recheck" 2> "$shadow_recheck_evidence"
cmp -s "$shadow_recheck" "$shadow_render"

kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get namespaces -o json \
  | jq -e '[.items[].metadata.name |
      select(startswith("loom-dev-") or startswith("loom-build-"))] |
      length == 0' >/dev/null

rollback_diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$shadow_render" \
  > "$evidence_dir/rollback.server-side-diff.txt" 2>&1 \
  || rollback_diff_status=$?
test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1

# Re-run every source, release, file-identity, storage, Secret, RuntimeClass,
# manager-ceiling, namespace, worker, and rollback-diff interlock here.
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$shadow_render" \
  > "$evidence_dir/rollback.server-side-apply.txt"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  deployment/loom-personal-dev-activation-agent \
  -o jsonpath='{.spec.replicas}')" = 0

"$loom_cli" admin personal-dev-control-plane status \
  --namespace loom-dev \
  --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  > "$rollback_status"
chmod 0600 "$rollback_status"
jq -e '
  .mode == "shadow" and .ready == true and .blockers == [] and
  .manager_ceiling == 0 and all(.components[]; .ready == true)
' "$rollback_status" >/dev/null
```

The rollback leaves the shared database, object storage, scanner cache, and
evidence intact while disabling lifecycle and builder and scaling activation to
zero. It does not activate, restart, or reconfigure either physical pool and
does not establish task capacity.
