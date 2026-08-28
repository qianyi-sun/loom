# Personal-development multi-owner durable zero-capacity launch

This runbook enables the personal application plane as a durable operational
service after the complete two-owner zero-capacity acceptance has finished and
returned the shared management plane to its byte-reviewed inert shadow. The
schema-v1 single-owner record remains historical compatibility, but before a second person is onboarded the final multi-person launch requires a verified schema-v2 result. It does
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
mutable sections only inside a separately reviewed multi-owner durable-launch window with the
exact candidate, operator, rollback manifest, and evidence paths reviewed in
advance.

## Stop conditions and rollback boundary

Stop before apply if any of these conditions holds:

- the full `personal-dev-concurrent-owner-zero-capacity-acceptance.md` procedure did not finish,
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
validated_repository_root() {
  local requested="$1"
  local current
  local git_root
  test "${requested#/}" != "$requested"
  test -d "$requested" && test ! -L "$requested"
  test "$(realpath -e -- "$requested")" = "$requested"
  current="$(pwd -P)"
  test "$current" = "$requested"
  git_root="$(git -C "$requested" rev-parse --show-toplevel)"
  test "${git_root#/}" != "$git_root"
  test -d "$git_root" && test ! -L "$git_root"
  test "$(realpath -e -- "$git_root")" = "$git_root"
  test "$git_root" = "$requested"
  printf '%s\n' "$git_root"
}
repo="$(validated_repository_root "$repo")"
profile="$repo/deploy/dev-fleet/personal-dev-control-plane.toml"
loom_cli="$repo/.venv/bin/loom"
python_cli="$repo/.venv/bin/python"
trusted_release="<absolute-owner-only-trusted-release.json>"
trusted_release_sha256="<reviewed-trusted-release-sha256>"
operational_plan="<absolute-owner-only-operational-plan.json>"
operational_plan_sha256="<reviewed-operational-plan-sha256>"
acceptance_plan="<absolute-owner-only-acceptance-plan.json>"
acceptance_plan_sha256="<reviewed-v2-acceptance-plan-sha256>"
acceptance_result="<absolute-owner-only-acceptance-result-v2.json>"
acceptance_result_sha256="<reviewed-v2-acceptance-result-sha256>"
acceptance_manifest_sha256="<reviewed-acceptance-manifest-sha256>"
acceptance_rollback_manifest="<absolute-owner-only-acceptance-rollback-shadow-manifest>"
rollback_evidence="<absolute-owner-only-acceptance-shadow-rollback-evidence>"
backup_restore_evidence="<absolute-owner-only-backup-restore-evidence>"
trusted_launcher_profile="<absolute-owner-only-source-derived-trusted-launcher-profile>"
scanner_finding_policy="<absolute-owner-only-source-derived-scanner-finding-policy>"
kubeconfig="<absolute-reviewed-self-contained-mode-0600-kubeconfig>"
expected_kube_context="<reviewed-context>"

test -x "$loom_cli"
test -x "$python_cli"

evidence_dir="<new-absolute-owner-only-durable-launch-evidence-directory>"
assert_owner_controlled_evidence_parent() {
  local directory="$1"
  local caller
  local child=""
  local child_owner
  local current
  local mode
  local owner
  caller="$(id -u)"
  test "${directory#/}" != "$directory"
  test -d "$directory" && test ! -L "$directory"
  test "$(realpath -e -- "$directory")" = "$directory"
  test "$(stat -c %u -- "$directory")" = "$caller"
  mode="$(stat -c %a -- "$directory")"
  test $((8#$mode & 8#022)) -eq 0
  current="$directory"
  while true; do
    test -d "$current" && test ! -L "$current"
    test "$(realpath -e -- "$current")" = "$current"
    owner="$(stat -c %u -- "$current")"
    if test "$owner" != 0 && test "$owner" != "$caller"; then
      return 1
    fi
    mode="$(stat -c %a -- "$current")"
    if test $((8#$mode & 8#022)) -ne 0; then
      test $((8#$mode & 8#1000)) -ne 0
      test -n "$child"
      child_owner="$(stat -c %u -- "$child")"
      if test "$child_owner" != 0 && test "$child_owner" != "$caller"; then
        return 1
      fi
    fi
    test "$current" = / && break
    child="$current"
    current="$(dirname -- "$current")"
  done
}

prepare_new_evidence_dir() {
  local path="$1"
  local parent
  local parent_identity
  local path_identity
  test "${path#/}" != "$path"
  test "$(realpath -m -- "$path")" = "$path"
  case "$path" in
    "$repo"|"$repo"/*) return 1 ;;
  esac
  parent="$(dirname -- "$path")"
  assert_owner_controlled_evidence_parent "$parent"
  parent_identity="$(stat -c '%d:%i:%f:%u:%g' -- "$parent")"
  test ! -e "$path" && test ! -L "$path"
  test "$(stat -c '%d:%i:%f:%u:%g' -- "$parent")" = "$parent_identity"
  mkdir -m 0700 -- "$path"
  test "$(stat -c '%d:%i:%f:%u:%g' -- "$parent")" = "$parent_identity"
  test -d "$path" && test ! -L "$path"
  test "$(realpath -e -- "$path")" = "$path"
  test "$(stat -c %u -- "$path")" = "$(id -u)"
  test "$(stat -c %a -- "$path")" = 700
  path_identity="$(stat -c '%d:%i:%f:%u:%g:%h' -- "$path")"
  test -z "$(find "$path" -mindepth 1 -maxdepth 1 -print -quit)"
  test "$(stat -c '%d:%i:%f:%u:%g:%h' -- "$path")" = "$path_identity"
  test "$(stat -c '%d:%i:%f:%u:%g' -- "$parent")" = "$parent_identity"
  assert_owner_controlled_evidence_parent "$parent"
}
prepare_new_evidence_dir "$evidence_dir"
operational_render="$evidence_dir/reviewed-operational.yaml"
operational_render_evidence="$evidence_dir/reviewed-operational.render.json"
shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"
shadow_render_evidence="$evidence_dir/reviewed-rollback-shadow.render.json"
pre_launch_status="$evidence_dir/pre-launch-shadow.status.json"
post_launch_status="$evidence_dir/post-launch-operational.status.json"
final_launch_status="$evidence_dir/final-operational.status.json"
rollback_status="$evidence_dir/rollback-shadow.status.json"
acceptance_verification="$evidence_dir/acceptance-result-verification.json"

reviewed_public_origin() {
  "$python_cli" - "$1" <<'PY'
import sys
import tomllib
from pathlib import Path

with Path(sys.argv[1]).open("rb") as stream:
    value = tomllib.load(stream)["network"]["public_origin"]
if not isinstance(value, str) or not value or value != value.strip():
    raise SystemExit("invalid network.public_origin")
print(value)
PY
}

reviewed_server="$(reviewed_public_origin "$profile")"

assert_owner_only_file() {
  local path="$1"
  local maximum_bytes="${2:-4194304}"
  test -f "$path" && test ! -L "$path" || return 1
  test "$(realpath -e -- "$path")" = "$path" || return 1
  test "$(stat -c %u -- "$path")" = "$(id -u)" || return 1
  test "$(stat -c %a -- "$path")" = 600 || return 1
  test "$(stat -c %h -- "$path")" = 1 || return 1
  test "$(stat -c %s -- "$path")" -gt 0 || return 1
  test "$(stat -c %s -- "$path")" -le "$maximum_bytes" || return 1
}

declare -A reviewed_file_identity=()
declare -A reviewed_file_sha256=()
declare -A reviewed_file_maximum=()

capture_file_binding() {
  local path="$1"
  local maximum_bytes="$2"
  local identity
  local sha256
  assert_owner_only_file "$path" "$maximum_bytes" || return 1
  identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$path")"
  sha256="$(sha256sum -- "$path" | awk '{print $1}')"
  assert_owner_only_file "$path" "$maximum_bytes" || return 1
  test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$path")" = "$identity" || return 1
  reviewed_file_identity["$path"]="$identity"
  reviewed_file_sha256["$path"]="$sha256"
  reviewed_file_maximum["$path"]="$maximum_bytes"
}

assert_file_binding() {
  local path="$1"
  local maximum_bytes="${reviewed_file_maximum[$path]-}"
  test -n "$maximum_bytes" || return 1
  assert_owner_only_file "$path" "$maximum_bytes" || return 1
  test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' -- "$path")" = \
    "${reviewed_file_identity[$path]}" || return 1
  test "$(sha256sum -- "$path" | awk '{print $1}')" = \
    "${reviewed_file_sha256[$path]}" || return 1
}

external_artifacts=(
  "$trusted_release"
  "$operational_plan"
  "$acceptance_plan"
  "$acceptance_result"
  "$acceptance_rollback_manifest"
  "$rollback_evidence"
  "$backup_restore_evidence"
  "$trusted_launcher_profile"
  "$scanner_finding_policy"
)
for path in \
  "${external_artifacts[@]}"; do
  capture_file_binding "$path" 16777216
done
capture_file_binding "$kubeconfig" 1048576

test "$(sha256sum "$trusted_release" | awk '{print $1}')" = "$trusted_release_sha256"
test "$(sha256sum "$operational_plan" | awk '{print $1}')" = "$operational_plan_sha256"
test "$(sha256sum "$acceptance_plan" | awk '{print $1}')" = "$acceptance_plan_sha256"
test "$(sha256sum "$acceptance_result" | awk '{print $1}')" = "$acceptance_result_sha256"
test "$acceptance_result_sha256" = \
  "$(jq -r .approval.acceptance_result_sha256 "$operational_plan")"
acceptance_rollback_manifest_sha256="$(sha256sum "$acceptance_rollback_manifest" | awk '{print $1}')"
test "$acceptance_rollback_manifest_sha256" = \
  "$(jq -r .release.shadow_manifest_sha256 "$acceptance_plan")"
test "$acceptance_rollback_manifest_sha256" = \
  "$(jq -r .shadow_manifest_sha256 "$acceptance_result")"
test "$acceptance_rollback_manifest_sha256" = \
  "$(jq -r .release.shadow_manifest_sha256 "$operational_plan")"
rollback_shadow_status_sha256="$(sha256sum "$rollback_evidence" | awk '{print $1}')"
test "$rollback_shadow_status_sha256" = \
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
source_commit="$(jq -r .source.commit "$operational_plan")"
source_tree="$(jq -r .source.tree "$operational_plan")"

assert_exact_source() {
  test "$(pwd -P)" = "$repo" || return 1
  test "$(git rev-parse --show-toplevel)" = "$repo" || return 1
  if git symbolic-ref --quiet HEAD >/dev/null; then
    return 1
  fi
  test -z "$(git status --porcelain=v1 --untracked-files=all)" || return 1
  test "$(git rev-parse HEAD)" = "$source_commit" || return 1
  test "$(git rev-parse 'HEAD^{tree}')" = "$source_tree" || return 1
}

assert_reviewed_kubeconfig() {
  assert_file_binding "$kubeconfig" || return 1
  test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = \
    "$expected_kube_context" || return 1
}

assert_server_side_diff_unchanged() {
  local manifest="$1"
  local reviewed_diff="$2"
  local label="$3"
  local recheck
  local diff_status=0
  case "$label" in
    operational|rollback) ;;
    *) return 1 ;;
  esac
  test -f "$reviewed_diff" && test ! -L "$reviewed_diff" || return 1
  test "$(realpath -e -- "$reviewed_diff")" = "$reviewed_diff" || return 1
  test "$(stat -c %u -- "$reviewed_diff")" = "$(id -u)" || return 1
  test "$(stat -c %a -- "$reviewed_diff")" = 600 || return 1
  test "$(stat -c %h -- "$reviewed_diff")" = 1 || return 1
  test "$(stat -c %s -- "$reviewed_diff")" -le 16777216 || return 1
  recheck="$(mktemp "$evidence_dir/$label.diff-recheck.XXXXXX.txt")"
  kubectl --kubeconfig "$kubeconfig" diff --server-side \
    --field-manager=loom-personal-dev-control-plane \
    -f "$manifest" > "$recheck" 2>&1 || diff_status=$?
  if ! test "$diff_status" -eq 0 && ! test "$diff_status" -eq 1; then
    rm -f -- "$recheck"
    return 1
  fi
  chmod 0600 "$recheck"
  if ! cmp -s "$recheck" "$reviewed_diff"; then
    rm -f -- "$recheck"
    return 1
  fi
  rm -f -- "$recheck"
}

assert_exact_source
assert_reviewed_kubeconfig
```

The operational plan is canonical, non-secret control evidence. It must bind
the same exact release and shadow manifest accepted by the implementation,
plus the final acceptance result, shadow-rollback evidence, manager boundary,
principals, quotas, RuntimeClass, scanner, publisher, registry, activation key,
schema head, and storage evidence. `approved_at` is an audit timestamp, not an
expiry. Choosing a distant acceptance expiry is not an operational plan.

## 2. Prove the accepted procedure ended in the exact shadow

The acceptance result must be canonical v2 evidence for two exact owners, six
ordered cross-owner denials, concurrent create/update, normal and retained-data
destroy, retained-name redeploy, final cleanup, and inert rollback. Run the
strict read-only verifier before any operational render or apply. The
acceptance runbook's last apply and status must prove the byte-reviewed shadow,
not an expired or still-active acceptance manifest. The verifier independently
binds every denial digest to the canonical target GET, PUT, or DELETE HTTP 404
receipt; nonempty arbitrary stderr is not acceptance evidence. It also binds
the retained exact rollback-manifest bytes to the result and requires every
top-level rendered resource's render-input and trusted-release annotations to
match the observed rollback status and accepted release.

```bash
test "$(jq -r .schema "$acceptance_result")" = \
  "loom-personal-dev-zero-capacity-acceptance-result-v2"
test "$(jq -r .release_sha256 "$acceptance_result")" = "$trusted_release_sha256"
test "$(jq -r .shadow_manifest_sha256 "$acceptance_result")" = \
  "$(jq -r .release.shadow_manifest_sha256 "$operational_plan")"

"$loom_cli" admin personal-dev-control-plane verify-acceptance-result \
  --acceptance-plan-file "$acceptance_plan" \
  --acceptance-plan-sha256 "$acceptance_plan_sha256" \
  --acceptance-result-file "$acceptance_result" \
  --acceptance-result-sha256 "$acceptance_result_sha256" \
  --acceptance-manifest-sha256 "$acceptance_manifest_sha256" \
  --rollback-shadow-manifest-file "$acceptance_rollback_manifest" \
  --rollback-shadow-status-file "$rollback_evidence" \
  > "$acceptance_verification"
chmod 0600 "$acceptance_verification"
jq -e \
  --arg result "$acceptance_result_sha256" \
  --arg rollback_shadow_status_sha256 "$rollback_shadow_status_sha256" '
  .schema == "loom-personal-dev-zero-capacity-acceptance-verification-v1" and
  .verified == true and .owner_count == 2 and
  .cross_owner_denial_count == 6 and
  .acceptance_result_sha256 == $result and
  .rollback_shadow_status_sha256 == $rollback_shadow_status_sha256
' "$acceptance_verification" >/dev/null

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

## 3. Prove DNS, TLS, Web, and API ingress before enablement

The shadow already renders the public Web and API Ingress, so DNS, certificate,
and account-action route readiness can be established without enabling personal
mutation. Record the reviewed DNS answers and require the exact cert-manager
Certificate, Web NetworkPolicy, and Ingress path split before the operational
apply. The Web image is the existing Loom SPA; this package does not implement
a second account, setup, or reset flow.

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

assert_web_api_route_contract() {
  local route_frontend route_health route_ingress route_network_policy
  local route_reset_page route_setup_page
  route_network_policy="$(mktemp "$evidence_dir/web-policy-recheck.XXXXXX.json")"
  route_ingress="$(mktemp "$evidence_dir/web-ingress-recheck.XXXXXX.json")"
  route_frontend="$(mktemp "$evidence_dir/frontend-config-recheck.XXXXXX.json")"
  route_health="$(mktemp "$evidence_dir/api-health-recheck.XXXXXX.json")"
  route_reset_page="$(mktemp "$evidence_dir/reset-spa-recheck.XXXXXX.html")"
  route_setup_page="$(mktemp "$evidence_dir/setup-spa-recheck.XXXXXX.html")"

  if ! kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace loom-dev get networkpolicy/loom-personal-dev-web-ingress \
    -o json > "$route_network_policy"; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! jq -e '
    .spec.podSelector == {"matchLabels":{"app":"loom-personal-dev-web"}} and
    .spec.policyTypes == ["Ingress"] and
    (.spec | has("egress") | not) and
    (.spec.ingress[0] | has("from") | not) and
    .spec.ingress == [{ports:[{port:8080,protocol:"TCP"}]}]
  ' "$route_network_policy" >/dev/null; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace loom-dev get ingress/loom-personal-dev-management \
    -o json > "$route_ingress"; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! jq -e --arg host "$management_host" '
    (.spec | keys | sort == ["ingressClassName","rules","tls"]) and
    (.metadata.annotations | keys | sort) as $keys | ($keys == ["cert-manager.io/cluster-issuer","loom.dev/render-input-sha256","loom.dev/trusted-release-sha256","nginx.ingress.kubernetes.io/proxy-body-size","nginx.ingress.kubernetes.io/proxy-read-timeout"] or $keys == ["cert-manager.io/cluster-issuer","loom.dev/operational-plan-sha256","loom.dev/render-input-sha256","loom.dev/trusted-release-sha256","nginx.ingress.kubernetes.io/proxy-body-size","nginx.ingress.kubernetes.io/proxy-read-timeout"]) and .metadata.annotations["cert-manager.io/cluster-issuer"] == "letsencrypt-prod" and .metadata.annotations["nginx.ingress.kubernetes.io/proxy-body-size"] == "512m" and .metadata.annotations["nginx.ingress.kubernetes.io/proxy-read-timeout"] == "300" and
    .spec.ingressClassName == "nginx" and
    .spec.rules == [{host:$host,http:{paths:[
      {backend:{service:{name:"loom-personal-dev-management",port:{number:8090}}},path:"/api",pathType:"Prefix"},
      {backend:{service:{name:"loom-personal-dev-web",port:{number:80}}},path:"/",pathType:"Prefix"}
    ]}}] and .spec.tls == [{hosts:[$host],secretName:"loom-personal-dev-management-tls"}]
  ' "$route_ingress" >/dev/null; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
    "$reviewed_server/api/v1/health" > "$route_health"; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! jq -e '.status == "ok"' "$route_health" >/dev/null; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
    "$reviewed_server/loom-frontend-config.json" > "$route_frontend"; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! jq -e --arg origin "$reviewed_server" '
    .environment == "development" and .routePath == "" and
    .apiBase == "" and .apiRouteBase == ($origin + "/api")
  ' "$route_frontend" >/dev/null; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
    "$reviewed_server/auth/reset" > "$route_reset_page" ||
    ! grep -Fq '<div id="root">' "$route_reset_page"; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  if ! curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
    "$reviewed_server/auth/setup" > "$route_setup_page" ||
    ! grep -Fq '<div id="root">' "$route_setup_page"; then
    rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
    return 1
  fi
  rm -f -- "$route_network_policy" "$route_ingress" "$route_frontend" "$route_health" "$route_reset_page" "$route_setup_page"
}

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
  networkpolicy/loom-personal-dev-management-ingress -o json \
  > "$evidence_dir/management.network-policy.json"
jq -e '
  .spec.podSelector.matchLabels == {"app":"loom-personal-dev-management"} and
  .spec.policyTypes == ["Ingress"] and
  (.spec | has("egress") | not) and
  (.spec.ingress[0] | has("from") | not) and
  .spec.ingress == [{ports:[{port:8090,protocol:"TCP"}]}]
' "$evidence_dir/management.network-policy.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  networkpolicy/loom-personal-dev-web-ingress -o json \
  > "$evidence_dir/web.network-policy.json"
jq -e '
  .spec.podSelector == {"matchLabels":{"app":"loom-personal-dev-web"}} and
  .spec.policyTypes == ["Ingress"] and
  (.spec | has("egress") | not) and
  (.spec.ingress[0] | has("from") | not) and
  .spec.ingress == [{ports:[{port:8080,protocol:"TCP"}]}]
' "$evidence_dir/web.network-policy.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  ingress/loom-personal-dev-management -o json \
  > "$evidence_dir/management.ingress.json"
jq -e --arg host "$management_host" '
  (.spec | keys | sort == ["ingressClassName","rules","tls"]) and
  .metadata.annotations == {
    "cert-manager.io/cluster-issuer":"letsencrypt-prod",
    "nginx.ingress.kubernetes.io/proxy-body-size":"512m",
    "nginx.ingress.kubernetes.io/proxy-read-timeout":"300"
  } and
  .spec.ingressClassName == "nginx" and
  .spec.rules == [{host:$host,http:{paths:[
    {backend:{service:{name:"loom-personal-dev-management",port:{number:8090}}},path:"/api",pathType:"Prefix"},
    {backend:{service:{name:"loom-personal-dev-web",port:{number:80}}},path:"/",pathType:"Prefix"}
  ]}}] and .spec.tls == [{hosts:[$host],secretName:"loom-personal-dev-management-tls"}]
' "$evidence_dir/management.ingress.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
  certificate/loom-personal-dev-management-tls -o json \
  > "$evidence_dir/management.certificate.json"
jq -e 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' \
  "$evidence_dir/management.certificate.json" >/dev/null
curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
  "https://$management_host/api/v1/health" \
  > "$evidence_dir/management.shadow-health.json"
jq -e '.status == "ok"' "$evidence_dir/management.shadow-health.json" >/dev/null
curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
  "$reviewed_server/loom-frontend-config.json" \
  > "$evidence_dir/management.frontend-config.json"
jq -e --arg origin "$reviewed_server" '
  .environment == "development" and .routePath == "" and
  .apiBase == "" and .apiRouteBase == ($origin + "/api")
' "$evidence_dir/management.frontend-config.json" >/dev/null
curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
  "$reviewed_server/auth/reset" > "$evidence_dir/management.reset-spa.html"
grep -Fq '<div id="root">' "$evidence_dir/management.reset-spa.html"
curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
  "$reviewed_server/auth/setup" > "$evidence_dir/management.setup-spa.html"
grep -Fq '<div id="root">' "$evidence_dir/management.setup-spa.html"
assert_web_api_route_contract

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
    svc-db-url | LC_ALL=C sort)"
  test "$(secret_keys loom-personal-dev-management)" = "$expected_management"
  test "$(secret_keys loom-personal-dev-activation-public)" = public-key
  test "$(secret_keys loom-personal-dev-activation-agent)" = private-key
}

assert_storage_and_migration() {
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace loom-dev get persistentvolumeclaims \
    -l app.kubernetes.io/managed-by=loom-personal-dev-control-plane -o json \
    | jq -e '.items | length >= 3 and all(.[]; .status.phase == "Bound")' >/dev/null
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s \
    --namespace loom-dev get jobs -l app=loom-personal-dev-migration -o json \
    | jq -e '
        .items | length >= 1 and
        any(.[]; any(.status.conditions[]?;
          .type == "Complete" and .status == "True")) and
        all(.[]; all(.status.conditions[]?;
          .type != "Failed" or .status != "True"))
      ' >/dev/null
}

assert_runtime_scanner_release_bindings() {
  local field
  local runtime_class
  local runtime_handler
  local runtime_profile_sha256
  test "$(jq -r .schema_version "$trusted_release")" = 3
  test "$(jq -cS .images "$trusted_release")" = \
    "$(jq -cS .release.images "$operational_plan")"
  for field in \
    binary_sha256 \
    cache_identity_sha256 \
    database_sha256 \
    database_metadata_sha256 \
    java_database_sha256 \
    java_database_metadata_sha256; do
    test "$(jq -r ".scanner.$field" "$trusted_release")" = \
      "$(jq -r ".builder.scanner_$field" "$operational_plan")"
  done
  runtime_class="$(jq -r .builder.runtime_class_name "$operational_plan")"
  runtime_handler="$(jq -r .builder.runtime_handler "$operational_plan")"
  runtime_profile_sha256="$(jq -r .builder.runtime_profile_sha256 "$operational_plan")"
  kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get \
    "runtimeclass.node.k8s.io/$runtime_class" -o json \
    | jq -e \
      --arg name "$runtime_class" \
      --arg handler "$runtime_handler" \
      --arg profile "$runtime_profile_sha256" '
        .apiVersion == "node.k8s.io/v1" and .kind == "RuntimeClass" and
        .metadata.name == $name and .handler == $handler and
        .metadata.annotations["loom.dev/runtime-profile-sha256"] == $profile and
        .scheduling.nodeSelector == {
          "kubernetes.io/arch":"amd64",
          "kubernetes.io/os":"linux",
          "loom.dev/personal-dev-runtime-profile-a":$profile[0:32],
          "loom.dev/personal-dev-runtime-profile-b":$profile[32:64]
        } and
        (.scheduling.tolerations // []) == [] and
        (has("overhead") | not)
      ' >/dev/null
}

assert_no_dynamic_namespaces_or_workers() {
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
}

assert_dns_tls_ingress() {
  local certificate
  local dns_addresses
  local ingress
  local network_policy
  local solver_policy
  local health
  dns_addresses="$(mktemp "$evidence_dir/management-dns-recheck.XXXXXX.txt")"
  solver_policy="$(mktemp "$evidence_dir/acme-policy-recheck.XXXXXX.json")"
  network_policy="$(mktemp "$evidence_dir/management-policy-recheck.XXXXXX.json")"
  ingress="$(mktemp "$evidence_dir/management-ingress-recheck.XXXXXX.json")"
  certificate="$(mktemp "$evidence_dir/management-certificate-recheck.XXXXXX.json")"
  health="$(mktemp "$evidence_dir/management-health-recheck.XXXXXX.json")"
  getent ahosts "$management_host" | awk '{print $1}' | LC_ALL=C sort -u \
    > "$dns_addresses"
  test -s "$dns_addresses"
  test "$(cat "$dns_addresses")" = \
    "$(awk '{print $1}' "$evidence_dir/management.dns.txt" | LC_ALL=C sort -u)"
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    networkpolicy/loom-personal-dev-acme-http01-ingress -o json > "$solver_policy"
  jq -e --argjson port "$solver_port" '
    .spec.podSelector.matchLabels == {"acme.cert-manager.io/http01-solver":"true"} and
    .spec.policyTypes == ["Ingress"] and (.spec | has("egress") | not) and
    (.spec.ingress[0] | has("from") | not) and
    .spec.ingress[0].ports == [{port:$port,protocol:"TCP"}]
  ' "$solver_policy" >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    networkpolicy/loom-personal-dev-management-ingress -o json > "$network_policy"
  jq -e '
    .spec.podSelector.matchLabels == {"app":"loom-personal-dev-management"} and
    .spec.policyTypes == ["Ingress"] and (.spec | has("egress") | not) and
    (.spec.ingress[0] | has("from") | not) and
    .spec.ingress == [{ports:[{port:8090,protocol:"TCP"}]}]
  ' "$network_policy" >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    ingress/loom-personal-dev-management -o json > "$ingress"
  jq -e --arg host "$management_host" '
    .spec.rules == [{host:$host,http:.spec.rules[0].http}] and
    .spec.tls[0].hosts == [$host]
  ' "$ingress" >/dev/null
  kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \
    certificate/loom-personal-dev-management-tls -o json > "$certificate"
  jq -e --arg host "$management_host" '
    (.spec.dnsNames | index($host)) != null and
    any(.status.conditions[]?; .type == "Ready" and .status == "True")
  ' "$certificate" >/dev/null
  curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 \
    "https://$management_host/api/v1/health" > "$health"
  rm -f -- "$dns_addresses" "$solver_policy" "$network_policy" \
    "$ingress" "$certificate" "$health"
}

assert_current_artifacts() {
  local path
  assert_exact_source
  for path in "${external_artifacts[@]}" "$kubeconfig" \
    "$operational_render" "$operational_render_evidence" \
    "$shadow_render" "$shadow_render_evidence"; do
    assert_file_binding "$path"
  done
  test "${reviewed_file_sha256[$trusted_release]}" = "$trusted_release_sha256"
  test "${reviewed_file_sha256[$operational_plan]}" = "$operational_plan_sha256"
  test "${reviewed_file_sha256[$acceptance_plan]}" = "$acceptance_plan_sha256"
  test "${reviewed_file_sha256[$acceptance_result]}" = "$acceptance_result_sha256"
  test "${reviewed_file_sha256[$acceptance_rollback_manifest]}" = \
    "$acceptance_rollback_manifest_sha256"
  test "${reviewed_file_sha256[$rollback_evidence]}" = \
    "$rollback_shadow_status_sha256"
}

assert_ready_shadow_status() {
  local output
  output="$(mktemp "$evidence_dir/pre-apply-shadow.XXXXXX.json")"
  "$loom_cli" admin personal-dev-control-plane status \
    --namespace loom-dev \
    --kubeconfig "$kubeconfig" \
    --file "$profile" \
    --trusted-release-file "$trusted_release" \
    --trusted-release-sha256 "$trusted_release_sha256" > "$output"
  chmod 0600 "$output"
  jq -e '
    .schema == "loom-personal-dev-control-plane-status-v1" and
    .mode == "shadow" and .ready == true and .blockers == [] and
    .manager_ceiling == 0 and all(.components[]; .ready == true)
  ' "$output" >/dev/null
  rm -f -- "$output"
}

assert_operational_manager_boundary() {
  local output
  local status_rc=0
  output="$(mktemp "$evidence_dir/pre-apply-operational-boundary.XXXXXX.json")"
  "$loom_cli" admin personal-dev-control-plane status-operational \
    --namespace loom-dev \
    --kubeconfig "$kubeconfig" \
    --file "$profile" \
    --trusted-release-file "$trusted_release" \
    --trusted-release-sha256 "$trusted_release_sha256" \
    --operational-plan-file "$operational_plan" \
    --operational-plan-sha256 "$operational_plan_sha256" \
    "${operational_evidence_args[@]}" > "$output" || status_rc=$?
  test "$status_rc" -eq 0 || test "$status_rc" -eq 1
  chmod 0600 "$output"
  jq -e --arg plan "$operational_plan_sha256" '
    .schema == "loom-personal-dev-control-plane-status-v1" and
    .mode == "operational" and .operational_plan_sha256 == $plan and
    .capacity_publication_ready == true and .manager_ceiling == 0 and
    .worker_available == false and
    any(.components[]; .name == "manager" and .ready == true) and
    any(.components[]; .name == "runtime-class" and .ready == true) and
    any(.components[];
      .name == "personal-workers" and .observed == 0 and .ready == true)
  ' "$output" >/dev/null
  rm -f -- "$output"
}

assert_operational_interlocks() {
  assert_current_artifacts
  assert_reviewed_kubeconfig
  assert_secret_key_inventory
  assert_storage_and_migration
  assert_runtime_scanner_release_bindings
  assert_no_dynamic_namespaces_or_workers
  assert_dns_tls_ingress
  assert_web_api_route_contract
  assert_ready_shadow_status
  assert_operational_manager_boundary
}

assert_rollback_interlocks() {
  local output
  local status_rc=0
  assert_current_artifacts
  assert_reviewed_kubeconfig
  assert_secret_key_inventory
  assert_storage_and_migration
  assert_runtime_scanner_release_bindings
  assert_no_dynamic_namespaces_or_workers
  output="$(mktemp "$evidence_dir/rollback-operational-boundary.XXXXXX.json")"
  "$loom_cli" admin personal-dev-control-plane status-operational \
    --namespace loom-dev \
    --kubeconfig "$kubeconfig" \
    --file "$profile" \
    --trusted-release-file "$trusted_release" \
    --trusted-release-sha256 "$trusted_release_sha256" \
    --operational-plan-file "$operational_plan" \
    --operational-plan-sha256 "$operational_plan_sha256" \
    "${operational_evidence_args[@]}" > "$output" || status_rc=$?
  test "$status_rc" -eq 0 || test "$status_rc" -eq 1
  chmod 0600 "$output"
  jq -e --arg plan "$operational_plan_sha256" '
    .schema == "loom-personal-dev-control-plane-status-v1" and
    .mode == "operational" and .operational_plan_sha256 == $plan and
    .capacity_publication_ready == true and .worker_available == false and
    .manager_ceiling == 0 and
    (.blockers | type == "array") and
    (.blockers as $blockers |
      ($blockers | unique | length) == ($blockers | length)) and
    ([.blockers[] | select(. != "web_not_ready")] | length == 0) and
    (.components | type == "array" and length == 7 and
      (map(.name) | sort) == [
        "cluster-resources", "manager", "namespaced-resources",
        "namespaces", "personal-workers", "runtime-class", "web"
      ]) and
    any(.components[];
      .name == "personal-workers" and .observed == 0 and .ready == true) and
    (if (.blockers | index("web_not_ready")) != null then
      all(.components[];
        if .name == "web" or .name == "namespaced-resources" then
          .ready == false
        else
          .ready == true
        end)
     else
      all(.components[]; .ready == true)
     end)
  ' "$output" >/dev/null
  rm -f -- "$output"
}
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
  .yaml_sha256 == $yaml and .resource_count == 38
' "$operational_render_evidence" >/dev/null
jq -e --arg yaml "$shadow_render_sha256" '
  .schema == "loom-personal-dev-control-plane-render-v1" and
  .mode == "shadow" and .yaml_sha256 == $yaml and .resource_count == 38
' "$shadow_render_evidence" >/dev/null

capture_file_binding "$operational_render" 16777216
capture_file_binding "$operational_render_evidence" 4194304
capture_file_binding "$shadow_render" 16777216
capture_file_binding "$shadow_render_evidence" 4194304

assert_operational_render_binding() {
  assert_file_binding "$operational_render"
  assert_file_binding "$operational_render_evidence"
  test "${reviewed_file_sha256[$operational_render]}" = \
    "$operational_render_sha256"
  jq -e \
    --arg plan "$operational_plan_sha256" \
    --arg yaml "$operational_render_sha256" '
      .schema == "loom-personal-dev-control-plane-render-v1" and
      .mode == "operational" and .operational_plan_sha256 == $plan and
      .yaml_sha256 == $yaml and .resource_count == 38
    ' "$operational_render_evidence" >/dev/null
}

assert_shadow_render_binding() {
  assert_file_binding "$shadow_render"
  assert_file_binding "$shadow_render_evidence"
  test "${reviewed_file_sha256[$shadow_render]}" = "$shadow_render_sha256"
  test "$shadow_render_sha256" = \
    "$(jq -r .release.shadow_manifest_sha256 "$operational_plan")"
  jq -e --arg yaml "$shadow_render_sha256" '
    .schema == "loom-personal-dev-control-plane-render-v1" and
    .mode == "shadow" and .yaml_sha256 == $yaml and .resource_count == 38
  ' "$shadow_render_evidence" >/dev/null
}
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

assert_server_side_diff_unchanged "$operational_render" "$evidence_dir/operational.server-side-diff.txt" operational
assert_operational_interlocks
assert_operational_render_binding
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$operational_render" \
  > "$evidence_dir/operational.server-side-apply.txt"
chmod 0600 "$evidence_dir/operational.server-side-apply.txt"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-web --timeout=300s
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-activation-agent --timeout=300s
assert_web_api_route_contract

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
for the completed two-owner acceptance. Do not submit a TaskSet, Batch, Trial,
Slurm job, or model call.

```bash
launch_xdg_config_root="<absolute-mode-0700-launch-xdg-config-root>"
owner_source="<absolute-small-arbitrary-source-root>"
owner_name="<reviewed-launch-smoke-name>"
assert_owner_only_file "$launch_xdg_config_root/loom/config.toml"

launch_owner_cli() {
  XDG_CONFIG_HOME="$launch_xdg_config_root" "$loom_cli" "$@"
}

launch_owner_whoami="$evidence_dir/launch-owner.whoami.json"
launch_owner_cli auth whoami --format json | jq -cS . > "$launch_owner_whoami"
chmod 0600 "$launch_owner_whoami"

assert_launch_owner_server() {
  local launch_owner_server
  launch_owner_server="$(jq -er '.server | select(type == "string" and length > 0)' "$launch_owner_whoami")"
  test "$launch_owner_server" = "$reviewed_server"
}
assert_launch_owner_server

launch_owner_cli service up \
  --environment "dev-$owner_name" \
  --source-root "$owner_source" \
  --min-slots 0 \
  --max-slots 2 \
  > "$evidence_dir/launch-owner.deploy.txt" 2>&1
launch_owner_cli dev status "$owner_name" --format json \
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
  curl --disable --fail --silent --show-error --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 30 "$url" \
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
launch_owner_cli dev destroy "$owner_name" --format json \
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
chmod 0600 "$shadow_recheck" "$shadow_recheck_evidence"
cmp -s "$shadow_recheck" "$shadow_render"
cmp -s "$shadow_recheck_evidence" "$shadow_render_evidence"
rm -f -- "$shadow_recheck" "$shadow_recheck_evidence"

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
chmod 0600 "$evidence_dir/rollback.server-side-diff.txt"

assert_server_side_diff_unchanged "$shadow_render" "$evidence_dir/rollback.server-side-diff.txt" rollback
assert_rollback_interlocks
assert_shadow_render_binding
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$shadow_render" \
  > "$evidence_dir/rollback.server-side-apply.txt"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-web --timeout=300s
assert_web_api_route_contract
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
