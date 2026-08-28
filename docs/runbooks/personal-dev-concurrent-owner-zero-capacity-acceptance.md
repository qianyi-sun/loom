# Personal-development concurrent-owner zero-capacity acceptance

This runbook is the separately reviewed concurrent-owner certification and rollback
procedure for the personal-development management plane. It enables lifecycle, restricted
source building, and stable-route activation in the shared infrastructure
namespace loom-dev, then exercises two concurrent environments owned by two
distinct acceptance owners in loom-dev-<name> namespaces. Physical capacity remains unchanged: the exact global manager stays
at executable-new-capacity ceiling zero, no personal worker exists, and no task
is submitted.

The certification proves that each owner can deploy arbitrary committed, modified, and untracked source
through the candidate-aware command. Architecture-specific and architecture-neutral tasks are out of scope
until the separately reviewed issue #906 work is complete.

Repository merge, operational approval, and a successful render are each
necessary but not sufficient authority for the live apply. Run the mutable
sections only inside the separately reviewed concurrent-owner certification window
and only after every interlock below passes.

## Stop conditions and authority boundary

Stop without improvising cleanup for any of these conditions:

- credential or kubeconfig drift;
- a closed, expired, or unapproved acceptance window;
- RuntimeClass or scanner drift;
- Secret key inventory drift;
- migration or storage drift;
- manager identity, configuration-epoch regression, execution epoch/state, or
  ceiling drift;
- namespace ownership drift; or
- any personal worker Deployment.

Also stop for changed source/release/plan bytes, a mutable image, missing
backup/restore evidence, a build namespace that survives its bounded operation,
or a server-side diff outside the reviewed resource set. Secret values arrive
only through the approved Secret channel. The operator observes the exact key inventory
but never places values in files, arguments, output, or acceptance
evidence.

Never delete PVCs. Never delete a namespace directly. Personal environments are
retired only through the authenticated manager-first destroy operation. Do not
change the global-manager ceiling, execution state, or controller configuration;
do not start, stop, or reconfigure either physical pool; and do not add a
temporary worker. Preserve all evidence and retain the current zero-capacity
state when an interlock fails.

## 1. Bind immutable evidence and reviewed credentials

Use one Bash session for the procedure. The protected workflow artifact is
named personal-dev-trusted-release-run-<run>-attempt-<attempt> and contains
exactly the aggregate release, its evidence, and its digest.

    set -euo pipefail
    umask 077

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

    repo="$(validated_repository_root "$(pwd -P)")"
    evidence_dir="<absolute-owner-only-outside-worktree-concurrent-owner-evidence-directory>"
    trusted_release_artifact="<absolute-downloaded-personal-dev-trusted-release-run-<run>-attempt-<attempt>>"
    trusted_release="$trusted_release_artifact/trusted-release.json"
    trusted_release_evidence="$trusted_release_artifact/trusted-release-evidence.json"
    acceptance_plan="<absolute-owner-only-acceptance-plan.json>"
    profile="$repo/deploy/dev-fleet/personal-dev-control-plane.toml"
    scanner_cache_lock="$repo/deploy/dev-fleet/personal-dev-scanner-cache-lock.json"
    loom_cli="$repo/.venv/bin/loom"
    python_cli="$repo/.venv/bin/python"
    kubeconfig="<absolute-reviewed-self-contained-mode-0600-kubeconfig>"
    expected_kube_context="<exact-reviewed-kubernetes-context>"

    acceptance_render="$evidence_dir/reviewed-acceptance.yaml"
    acceptance_render_evidence="$evidence_dir/reviewed-acceptance.render.json"
    shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"
    shadow_render_evidence="$evidence_dir/reviewed-rollback-shadow.render.json"
    pre_acceptance_status="$evidence_dir/pre-acceptance.status.json"
    post_acceptance_status="$evidence_dir/post-acceptance.status.json"
    scanner_cache_init_status="$evidence_dir/scanner-cache-init.status.json"
    pre_deploy_acceptance_status="$evidence_dir/pre-deploy.status.json"
    initial_acceptance_status="$evidence_dir/after-initial.status.json"
    updated_acceptance_status="$evidence_dir/after-updates.status.json"
    post_denials_acceptance_status="$evidence_dir/after-denials.status.json"
    post_destroy_acceptance_status="$evidence_dir/after-destroy.status.json"
    post_redeploy_acceptance_status="$evidence_dir/after-redeploy.status.json"
    pre_rollback_acceptance_status="$evidence_dir/pre-rollback.status.json"
    rollback_pre_status="$evidence_dir/rollback-pre.status.json"
    rollback_scanner_cache_init_status="$evidence_dir/rollback-scanner-cache-init.status.json"
    rollback_status="$evidence_dir/rollback-shadow.status.json"

    backup_restore_evidence="<absolute-owner-only-backup-restore-evidence>"
    runtime_profile="<absolute-reviewed-runtime-profile>"
    trusted_launcher_profile="$evidence_dir/trusted-launcher-profile.json"
    scanner_finding_policy="$evidence_dir/scanner-finding-policy.json"

    owner_0_xdg="<absolute-mode-0700-owner-0-xdg-config-root>"
    owner_1_xdg="<absolute-mode-0700-owner-1-xdg-config-root>"
    owner_0_name="<owner-0-personal-name>"
    owner_1_name="<owner-1-personal-name>"
    owner_0_source_v1="<absolute-owner-0-source-v1>"
    owner_0_source_v2="<absolute-owner-0-source-v2>"
    owner_1_source_v1="<absolute-owner-1-source-v1>"
    owner_1_source_v2="<absolute-owner-1-source-v2>"
    acceptance_result="$evidence_dir/acceptance-result-v2.json"
    acceptance_verification="$evidence_dir/acceptance-result-verification.json"
    denials_jsonl="$evidence_dir/cross-owner-denials.jsonl"

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
    test -x "$loom_cli"
    test -x "$python_cli"
    export PYTHONPATH=src:.
    loom() {
      "$loom_cli" "$@"
    }

    launcher_tmp="$(mktemp "$evidence_dir/trusted-launcher.XXXXXX.json")"
    launcher_render_evidence="$evidence_dir/trusted-launcher-profile.render.json"
    "$loom_cli" admin personal-dev-control-plane render-trusted-launcher-profile \
      --file "$profile" \
      --trusted-release-file "$trusted_release" \
      --trusted-release-sha256 "$(tr -d '\n' < "$trusted_release_artifact/trusted-release.sha256")" \
      --source-root "$repo" \
      > "$launcher_tmp" 2> "$launcher_render_evidence"
    chmod 0600 "$launcher_tmp" "$launcher_render_evidence"
    mv "$launcher_tmp" "$trusted_launcher_profile"

    scanner_tmp="$(mktemp "$evidence_dir/scanner-policy.XXXXXX.json")"
    scanner_render_evidence="$evidence_dir/scanner-finding-policy.render.json"
    "$loom_cli" admin personal-dev-control-plane render-scanner-finding-policy \
      --file "$profile" \
      --trusted-release-file "$trusted_release" \
      --trusted-release-sha256 "$(tr -d '\n' < "$trusted_release_artifact/trusted-release.sha256")" \
      --source-root "$repo" \
      > "$scanner_tmp" 2> "$scanner_render_evidence"
    chmod 0600 "$scanner_tmp" "$scanner_render_evidence"
    mv "$scanner_tmp" "$scanner_finding_policy"

    acceptance_evidence_args=(
      --source-root "$repo"
      --trusted-launcher-profile-file "$trusted_launcher_profile"
      --scanner-finding-policy-file "$scanner_finding_policy"
      --backup-restore-evidence-file "$backup_restore_evidence"
    )

The two policy files above are generated from the exact checkout, profile, and
trusted release; they are not manually authored. Review their canonical render
records, bind their SHA-256 values into the acceptance plan, and generate the
backup/restore record with
[`personal-dev-backup-restore-evidence.md`](personal-dev-backup-restore-evidence.md)
before continuing. The renderer and status observer rederive both policies and
semantically validate the completed restore record on every invocation.

The two XDG roots must already contain distinct, non-rotating user-owned API
bearer credentials provisioned through the approved authentication channel.
Browser sessions, legacy team tokens, service/admin credentials, or incomplete
whoami identity fields cannot enter this bounded proof. Ordinary personal
deployment remains compatible with browser sessions outside this runbook. Do
not copy either token into another root or acceptance artifact.

The following helpers reject duplicate/noncanonical JSON and bind every
owner-only input to stable bytes.

    canonical_json_sha256() {
      "$python_cli" - "$1" <<'PY'
    import hashlib
    import json
    import math
    import sys
    from pathlib import Path

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise SystemExit("duplicate JSON field")
            result[key] = value
        return result

    def reject_constant(value):
        raise SystemExit("non-finite JSON value")

    path = Path(sys.argv[1])
    payload = path.read_bytes()
    value = json.loads(
        payload.decode("ascii"),
        object_pairs_hook=unique,
        parse_constant=reject_constant,
        parse_float=lambda raw: float(raw) if math.isfinite(float(raw)) else reject_constant(raw),
    )
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if payload != canonical:
        raise SystemExit("JSON is not canonical")
    print(hashlib.sha256(payload).hexdigest())
    PY
    }

    assert_owner_only_file() {
      local path="$1"
      local maximum_bytes="${2:-4194304}"
      test -f "$path" && test ! -L "$path"
      test "$(realpath -e "$path")" = "$path"
      test "$(stat -c %u "$path")" = "$(id -u)"
      test "$(stat -c %a "$path")" = 600
      test "$(stat -c %h "$path")" = 1
      test "$(stat -c %s "$path")" -gt 0
      test "$(stat -c %s "$path")" -le "$maximum_bytes"
    }

    assert_canonical_json_line() {
      local path="$1"
      local checked
      checked="$(mktemp "$evidence_dir/canonical-line.XXXXXX.json")"
      jq -cS . "$path" > "$checked"
      cmp -s "$checked" "$path"
      rm -f "$checked"
    }

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
    management_host="$("$python_cli" -c 'import sys; from urllib.parse import urlsplit; print(urlsplit(sys.argv[1]).hostname or "")' "$reviewed_server")"
    test -n "$management_host"

    test -d "$trusted_release_artifact" && test ! -L "$trusted_release_artifact"
    test "$(realpath -e "$trusted_release_artifact")" = "$trusted_release_artifact"
    test "$(stat -c %u "$trusted_release_artifact")" = "$(id -u)"
    test "$(stat -c %a "$trusted_release_artifact")" = 700
    test "$(find "$trusted_release_artifact" -mindepth 1 -maxdepth 1 -printf x | wc -c)" -eq 3
    test -z "$(find "$trusted_release_artifact" -mindepth 1 -maxdepth 1 ! -name trusted-release.json ! -name trusted-release-evidence.json ! -name trusted-release.sha256 -print -quit)"
    assert_owner_only_file "$trusted_release"
    assert_owner_only_file "$trusted_release_evidence"
    assert_owner_only_file "$trusted_release_artifact/trusted-release.sha256"

    trusted_release_sha256="$(tr -d '\n' < "$trusted_release_artifact/trusted-release.sha256")"
    test "$trusted_release_sha256" = "$(canonical_json_sha256 "$trusted_release")"
    test "$(canonical_json_sha256 "$trusted_release_evidence")" = "$(jq -r .release_evidence_sha256 "$trusted_release")"

    assert_owner_only_file "$acceptance_plan"
    test "$(stat -c %a "$acceptance_plan")" = 600
    test "$(stat -c %h "$acceptance_plan")" = 1
    acceptance_plan_sha256="$(sha256sum "$acceptance_plan" | awk '{print $1}')"
    test "$acceptance_plan_sha256" = "$(canonical_json_sha256 "$acceptance_plan")"
    test "$(jq -r .release.trusted_release_sha256 "$acceptance_plan")" = "$trusted_release_sha256"
    test "$(jq -r .release.release_evidence_sha256 "$acceptance_plan")" = "$(jq -r .release_evidence_sha256 "$trusted_release")"
    test "$(jq -r .manager.executable_new_capacity_ceiling "$acceptance_plan")" = 0

    test "$(git rev-parse --show-toplevel)" = "$(pwd -P)"
    test -z "$(git status --porcelain=v1 --untracked-files=all)"
    test "$(git rev-parse HEAD)" = "$(jq -r .source_sha "$trusted_release")"
    test "$(git rev-parse HEAD^{tree})" = "$(jq -r .source_tree "$trusted_release")"
    test -f "$profile" && test ! -L "$profile"
    profile_sha256="$(sha256sum "$profile" | awk '{print $1}')"
    test -f "$scanner_cache_lock" && test ! -L "$scanner_cache_lock"
    test "$(realpath -e "$scanner_cache_lock")" = "$scanner_cache_lock"
    scanner_cache_lock_sha256="$(sha256sum "$scanner_cache_lock" | awk '{print $1}')"

    assert_owner_only_file "$kubeconfig"
    kubeconfig_sha256="$(sha256sum "$kubeconfig" | awk '{print $1}')"
    kubeconfig_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$kubeconfig")"

    backup_restore_evidence_sha256="$(jq -r .storage.backup_restore_evidence_sha256 "$acceptance_plan")"
    runtime_profile_sha256="$(jq -r .builder.runtime_profile_sha256 "$acceptance_plan")"
    trusted_launcher_profile_sha256="$(jq -r .builder.trusted_launcher_profile_sha256 "$acceptance_plan")"
    scanner_finding_policy_sha256="$(jq -r .builder.scanner_finding_policy_sha256 "$acceptance_plan")"

    for path in "$backup_restore_evidence" "$runtime_profile" "$trusted_launcher_profile" "$scanner_finding_policy"; do
      assert_owner_only_file "$path"
    done
    test "$(sha256sum "$backup_restore_evidence" | awk '{print $1}')" = "$backup_restore_evidence_sha256"
    test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = "$runtime_profile_sha256"
    test "$(sha256sum "$trusted_launcher_profile" | awk '{print $1}')" = "$trusted_launcher_profile_sha256"
    test "$(sha256sum "$scanner_finding_policy" | awk '{print $1}')" = "$scanner_finding_policy_sha256"

    assert_scanner_release_binding() {
      test "$(jq -r .schema_version "$trusted_release")" = 3
      test -f "$scanner_cache_lock" && test ! -L "$scanner_cache_lock"
      test "$(realpath -e "$scanner_cache_lock")" = "$scanner_cache_lock"
      test "$(sha256sum "$scanner_cache_lock" | awk '{print $1}')" = "$scanner_cache_lock_sha256"
      test "$scanner_cache_lock_sha256" = "$(jq -r .scanner.lock_sha256 "$trusted_release")"
      test "$(jq -r .images.personal_dev_scanner_cache "$trusted_release")" = "$(jq -r .release.images.personal_dev_scanner_cache "$acceptance_plan")"
      test "$(jq -r .scanner.binary_sha256 "$trusted_release")" = "$(jq -r .builder.scanner_binary_sha256 "$acceptance_plan")"
      test "$(jq -r .scanner.cache_identity_sha256 "$trusted_release")" = "$(jq -r .builder.scanner_cache_identity_sha256 "$acceptance_plan")"
      test "$(jq -r .scanner.database_sha256 "$trusted_release")" = "$(jq -r .builder.scanner_database_sha256 "$acceptance_plan")"
      test "$(jq -r .scanner.database_metadata_sha256 "$trusted_release")" = "$(jq -r .builder.scanner_database_metadata_sha256 "$acceptance_plan")"
      test "$(jq -r .scanner.java_database_sha256 "$trusted_release")" = "$(jq -r .builder.scanner_java_database_sha256 "$acceptance_plan")"
      test "$(jq -r .scanner.java_database_metadata_sha256 "$trusted_release")" = "$(jq -r .builder.scanner_java_database_metadata_sha256 "$acceptance_plan")"
    }
    assert_scanner_release_binding

## 2. Define the repeated read-only interlocks

These functions are observations only. Acceptance status compares the complete
expected render, the plan digest, the exact RuntimeClass handler/profile,
scanner bindings, dynamic namespace ownership, all Deployments in all
namespaces, and the authenticated global-manager identity, monotonic
configuration-epoch floor, exact execution epoch/state, and ceiling. Each
personal projection advances the global configuration epoch; advancement is
expected, while regression is a stop condition.

    assert_reviewed_kubeconfig() {
      test -f "$kubeconfig" && test ! -L "$kubeconfig"
      test "$(realpath -e "$kubeconfig")" = "$kubeconfig"
      test "$(stat -c %u "$kubeconfig")" = "$(id -u)"
      test "$(stat -c %a "$kubeconfig")" = 600
      test "$(stat -c %h "$kubeconfig")" = 1
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$kubeconfig")" = "$kubeconfig_identity"
      test "$(sha256sum "$kubeconfig" | awk '{print $1}')" = "$kubeconfig_sha256"
      test "$(kubectl --kubeconfig "$kubeconfig" config current-context)" = "$expected_kube_context"
    }

    assert_acceptance_window() {
      local now started expires rollback_expires
      now="$(date -u +%s)"
      started="$(date -u -d "$(jq -r .window.started_at "$acceptance_plan")" +%s)"
      expires="$(date -u -d "$(jq -r .window.expires_at "$acceptance_plan")" +%s)"
      rollback_expires="$(date -u -d "$(jq -r .window.rollback_expires_at "$acceptance_plan")" +%s)"
      test "$started" -le "$now"
      test "$now" -lt "$expires"
      test "$expires" -le "$rollback_expires"
    }

    assert_rollback_window() {
      local now rollback_expires
      now="$(date -u +%s)"
      rollback_expires="$(date -u -d "$(jq -r .window.rollback_expires_at "$acceptance_plan")" +%s)"
      test "$now" -lt "$rollback_expires"
    }

    secret_keys() {
      kubectl --kubeconfig "$kubeconfig" --request-timeout=10s --namespace loom-dev get secret "$1" -o 'go-template={{range $key, $value := .data}}{{$key}}{{"\n"}}{{end}}' | LC_ALL=C sort
    }

    assert_secret_key_inventory() {
      local expected_management expected_public expected_private
      expected_management="$(printf '%s\n' admin-secrets.toml capacity-lifecycle-ca.pem capacity-lifecycle-certificate.pem capacity-lifecycle-private-key.pem capacity-lifecycle-token capacity-reporter-ca.pem capacity-reporter-certificate.pem capacity-reporter-private-key.pem config.json dev-instance-database-admin-url minio-access-key minio-secret-key postgres-database postgres-password postgres-user secret-store-master-key svc-db-url | LC_ALL=C sort)"
      expected_public="public-key"
      expected_private="private-key"
      test "$(secret_keys loom-personal-dev-management)" = "$expected_management"
      test "$(secret_keys loom-personal-dev-activation-public)" = "$expected_public"
      test "$(secret_keys loom-personal-dev-activation-agent)" = "$expected_private"
    }

    assert_no_dynamic_namespaces() {
      kubectl --kubeconfig "$kubeconfig" --request-timeout=10s get namespaces -o json | jq -e '[.items[].metadata.name | select(startswith("loom-dev-") or startswith("loom-build-"))] | length == 0' >/dev/null
    }

    assert_storage_and_migration() {
      kubectl --kubeconfig "$kubeconfig" --request-timeout=10s --namespace loom-dev get persistentvolumeclaims -l app.kubernetes.io/managed-by=loom-personal-dev-control-plane -o json | jq -e '.items | length >= 3 and all(.[]; .status.phase == "Bound")' >/dev/null
      kubectl --kubeconfig "$kubeconfig" --request-timeout=10s --namespace loom-dev get jobs -l app=loom-personal-dev-migration -o json | jq -e '.items | length >= 1 and any(.[]; any(.status.conditions[]?; .type == "Complete" and .status == "True")) and all(.[]; all(.status.conditions[]?; .type != "Failed" or .status != "True"))' >/dev/null
    }

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
        (.metadata.annotations | keys | sort) as $keys |
        ($keys == ["cert-manager.io/cluster-issuer","loom.dev/render-input-sha256","loom.dev/trusted-release-sha256","nginx.ingress.kubernetes.io/proxy-body-size","nginx.ingress.kubernetes.io/proxy-read-timeout"] or $keys == ["cert-manager.io/cluster-issuer","loom.dev/acceptance-plan-sha256","loom.dev/render-input-sha256","loom.dev/trusted-release-sha256","nginx.ingress.kubernetes.io/proxy-body-size","nginx.ingress.kubernetes.io/proxy-read-timeout"]) and
        .metadata.annotations["cert-manager.io/cluster-issuer"] == "letsencrypt-prod" and .metadata.annotations["nginx.ingress.kubernetes.io/proxy-body-size"] == "512m" and .metadata.annotations["nginx.ingress.kubernetes.io/proxy-read-timeout"] == "300" and
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

    capture_scanner_cache_init_status() {
      local output="$1"
      kubectl --kubeconfig "$kubeconfig" --request-timeout=10s --namespace loom-dev get pods -l app=loom-personal-dev-management -o json | jq -cS '[.items[] | {init: [.status.initContainerStatuses[]? | select(.name == "personal-dev-scanner-cache-init") | {exit_code: .state.terminated.exitCode, image_id: .imageID, name: .name, reason: .state.terminated.reason}], pod: .metadata.name}]' > "$output"
      chmod 0600 "$output"
      assert_canonical_json_line "$output"
      jq -e 'length == 1 and (.[0].init | length == 1) and .[0].init[0].name == "personal-dev-scanner-cache-init" and .[0].init[0].exit_code == 0 and .[0].init[0].reason == "Completed" and (.[0].init[0].image_id | type == "string" and test("@sha256:[0-9a-f]{64}$"))' "$output" >/dev/null
    }

    assert_current_artifacts() {
      assert_owner_only_file "$trusted_release"
      assert_owner_only_file "$trusted_release_evidence"
      assert_owner_only_file "$acceptance_plan"
      for path in "$backup_restore_evidence" "$runtime_profile" "$trusted_launcher_profile" "$scanner_finding_policy" "$acceptance_render" "$acceptance_render_evidence" "$shadow_render" "$shadow_render_evidence"; do
        assert_owner_only_file "$path"
      done
      test "$(git rev-parse HEAD)" = "$(jq -r .source_sha "$trusted_release")"
      test "$(git rev-parse HEAD^{tree})" = "$(jq -r .source_tree "$trusted_release")"
      test -z "$(git status --porcelain=v1 --untracked-files=all)"
      test "$(sha256sum "$profile" | awk '{print $1}')" = "$profile_sha256"
      test "$(canonical_json_sha256 "$trusted_release")" = "$trusted_release_sha256"
      test "$(canonical_json_sha256 "$trusted_release_evidence")" = "$(jq -r .release_evidence_sha256 "$trusted_release")"
      test "$(canonical_json_sha256 "$acceptance_plan")" = "$acceptance_plan_sha256"
      assert_scanner_release_binding
      test "$(sha256sum "$backup_restore_evidence" | awk '{print $1}')" = "$backup_restore_evidence_sha256"
      test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = "$runtime_profile_sha256"
      test "$(sha256sum "$trusted_launcher_profile" | awk '{print $1}')" = "$trusted_launcher_profile_sha256"
      test "$(sha256sum "$scanner_finding_policy" | awk '{print $1}')" = "$scanner_finding_policy_sha256"
      test "$(sha256sum "$acceptance_render" | awk '{print $1}')" = "$acceptance_render_sha256"
      test "$(sha256sum "$shadow_render" | awk '{print $1}')" = "$shadow_render_sha256"
      test "$(sha256sum "$acceptance_render_evidence" | awk '{print $1}')" = "$acceptance_render_evidence_sha256"
      test "$(sha256sum "$shadow_render_evidence" | awk '{print $1}')" = "$shadow_render_evidence_sha256"
      test "$shadow_render_sha256" = "$(jq -r .release.shadow_manifest_sha256 "$acceptance_plan")"
    }

    capture_pre_acceptance_status() {
      local status_rc=0
      "$loom_cli" admin personal-dev-control-plane status-acceptance --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" "${acceptance_evidence_args[@]}" > "$pre_acceptance_status" || status_rc=$?
      test "$status_rc" -eq 1
      chmod 0600 "$pre_acceptance_status"
      assert_canonical_json_line "$pre_acceptance_status"
      jq -e --arg plan "$acceptance_plan_sha256" '.schema == "loom-personal-dev-control-plane-status-v1" and .mode == "acceptance" and .acceptance_plan_sha256 == $plan and .capacity_publication_ready == true and .worker_available == false and .manager_ceiling == 0 and any(.components[]; .name == "personal-workers" and .observed == 0 and .ready == true)' "$pre_acceptance_status" >/dev/null
    }

    assert_live_acceptance() {
      local output="$1"
      assert_current_artifacts
      assert_acceptance_window
      assert_reviewed_kubeconfig
      assert_secret_key_inventory
      assert_storage_and_migration
      assert_web_api_route_contract
      "$loom_cli" admin personal-dev-control-plane status-acceptance --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" "${acceptance_evidence_args[@]}" > "$output"
      chmod 0600 "$output"
      assert_canonical_json_line "$output"
      jq -e --arg plan "$acceptance_plan_sha256" '.schema == "loom-personal-dev-control-plane-status-v1" and .mode == "acceptance" and .ready == true and .blockers == [] and .acceptance_plan_sha256 == $plan and .application_ready == true and .capacity_publication_ready == true and .worker_available == false and .manager_ceiling == 0 and all(.components[]; .ready == true)' "$output" >/dev/null
    }

    assert_pre_apply_interlocks() {
      assert_current_artifacts
      assert_acceptance_window
      assert_reviewed_kubeconfig
      assert_secret_key_inventory
      assert_storage_and_migration
      assert_no_dynamic_namespaces
      assert_web_api_route_contract
      capture_pre_acceptance_status
    }

    assert_rollback_interlocks() {
      local status_rc=0
      assert_current_artifacts
      assert_rollback_window
      assert_reviewed_kubeconfig
      assert_secret_key_inventory
      assert_storage_and_migration
      assert_no_dynamic_namespaces
      "$loom_cli" admin personal-dev-control-plane status-acceptance --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" "${acceptance_evidence_args[@]}" > "$rollback_pre_status" || status_rc=$?
      test "$status_rc" -eq 0 || test "$status_rc" -eq 1
      chmod 0600 "$rollback_pre_status"
      assert_canonical_json_line "$rollback_pre_status"
      jq -e --arg plan "$acceptance_plan_sha256" '
        .schema == "loom-personal-dev-control-plane-status-v1" and
        .mode == "acceptance" and .acceptance_plan_sha256 == $plan and
        .capacity_publication_ready == true and .worker_available == false and
        .manager_ceiling == 0 and
        (.blockers | type == "array") and
        (.blockers as $blockers |
          ($blockers | unique | length) == ($blockers | length)) and
        ([.blockers[] |
          select(. != "acceptance_window_expired" and . != "web_not_ready")]
          | length == 0) and
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
      ' "$rollback_pre_status" >/dev/null
    }

The successful live acceptance record contains these exact facets:

    {"application_ready":true,"capacity_publication_ready":true,"manager_ceiling":0,"worker_available":false}

## 3. Render and byte-review forward and rollback manifests

Render both modes before mutation. The shadow digest must equal the acceptance
plan's exact rollback binding. Review both YAML files and both canonical render
records; the acceptance diff may enable only personal lifecycle/build flags,
one activation replica, and the plan-bound readiness/interlock settings.
Both manifests must contain the exact `personal-dev-scanner-cache-init`
container and release-bound generation mount. Deployment rollout readiness
proves that init completed before management started; the procedure records
that init's zero exit status and immutable image ID after each apply.

    acceptance_tmp="$(mktemp "$evidence_dir/acceptance.XXXXXX.yaml")"
    acceptance_evidence_tmp="$(mktemp "$evidence_dir/acceptance.XXXXXX.json")"
    if ! "$loom_cli" admin personal-dev-control-plane render-acceptance --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" "${acceptance_evidence_args[@]}" > "$acceptance_tmp" 2> "$acceptance_evidence_tmp"; then
      rm -f "$acceptance_tmp" "$acceptance_evidence_tmp"
      exit 1
    fi
    chmod 0600 "$acceptance_tmp" "$acceptance_evidence_tmp"
    mv "$acceptance_tmp" "$acceptance_render"
    mv "$acceptance_evidence_tmp" "$acceptance_render_evidence"

    shadow_tmp="$(mktemp "$evidence_dir/shadow.XXXXXX.yaml")"
    shadow_evidence_tmp="$(mktemp "$evidence_dir/shadow.XXXXXX.json")"
    if ! "$loom_cli" admin personal-dev-control-plane render --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" > "$shadow_tmp" 2> "$shadow_evidence_tmp"; then
      rm -f "$shadow_tmp" "$shadow_evidence_tmp"
      exit 1
    fi
    chmod 0600 "$shadow_tmp" "$shadow_evidence_tmp"
    mv "$shadow_tmp" "$shadow_render"
    mv "$shadow_evidence_tmp" "$shadow_render_evidence"

    acceptance_render_sha256="$(sha256sum "$acceptance_render" | awk '{print $1}')"
    shadow_render_sha256="$(sha256sum "$shadow_render" | awk '{print $1}')"
    acceptance_render_evidence_sha256="$(sha256sum "$acceptance_render_evidence" | awk '{print $1}')"
    shadow_render_evidence_sha256="$(sha256sum "$shadow_render_evidence" | awk '{print $1}')"
    test "$shadow_render_sha256" = "$(jq -r .release.shadow_manifest_sha256 "$acceptance_plan")"
    jq -e --arg plan "$acceptance_plan_sha256" --arg yaml "$acceptance_render_sha256" '.schema == "loom-personal-dev-control-plane-render-v1" and .mode == "acceptance" and .acceptance_plan_sha256 == $plan and .yaml_sha256 == $yaml and .resource_count == 38' "$acceptance_render_evidence" >/dev/null
    jq -e --arg yaml "$shadow_render_sha256" '.schema == "loom-personal-dev-control-plane-render-v1" and .mode == "shadow" and .yaml_sha256 == $yaml and .resource_count == 38' "$shadow_render_evidence" >/dev/null

## 4. Diff and apply acceptance

Review the complete server-side diff. A resource removal, identity transition,
storage replacement, unpinned image, worker, capacity actuator, broader RBAC,
or unrelated namespace is a stop condition.

    acceptance_diff_status=0
    kubectl --kubeconfig "$kubeconfig" diff --server-side --field-manager=loom-personal-dev-control-plane -f "$acceptance_render" > "$evidence_dir/acceptance.server-side-diff.txt" 2>&1 || acceptance_diff_status=$?
    test "$acceptance_diff_status" -eq 0 || test "$acceptance_diff_status" -eq 1
    chmod 0600 "$evidence_dir/acceptance.server-side-diff.txt"

    assert_pre_apply_interlocks
    kubectl --kubeconfig "$kubeconfig" apply --server-side --field-manager=loom-personal-dev-control-plane -f "$acceptance_render" > "$evidence_dir/acceptance.server-side-apply.txt"
    chmod 0600 "$evidence_dir/acceptance.server-side-apply.txt"

    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status deployment/loom-personal-dev-management --timeout=300s
    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status deployment/loom-personal-dev-web --timeout=300s
    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status deployment/loom-personal-dev-activation-agent --timeout=300s
    capture_scanner_cache_init_status "$scanner_cache_init_status"
    assert_live_acceptance "$post_acceptance_status"

## 5. Verify two pinned user-owned bearer credentials

Both XDG roots and both configuration files must be absolute, owner-only,
non-symlink paths with distinct real paths and distinct device/inode
identities. Capture canonical secret-free identity JSON and compare owner 0 and
owner 1 with the correspondingly ordered v2 plan entries. The single-owner v1
record remains historical compatibility; it is not authority for this proof.
Keep all four arbitrary source roots separate from the clean control-plane
checkout and from each other.

    test "$(jq -r .schema_version "$acceptance_plan")" = 2
    test "$(jq '.acceptance_owners | length' "$acceptance_plan")" = 2
    test "$owner_0_name" != "$owner_1_name"
    test "$(realpath -e "$owner_0_xdg")" != "$(realpath -e "$owner_1_xdg")"

    for owner_root in "$owner_0_xdg" "$owner_1_xdg"; do
      test -d "$owner_root" && test ! -L "$owner_root"
      test "$(realpath -e "$owner_root")" = "$owner_root"
      test "$(stat -c %u "$owner_root")" = "$(id -u)"
      test "$(stat -c %a "$owner_root")" = 700
      test -f "$owner_root/loom/config.toml" && test ! -L "$owner_root/loom/config.toml"
      test "$(realpath -e "$owner_root/loom/config.toml")" = "$owner_root/loom/config.toml"
      test "$(stat -c %u "$owner_root/loom/config.toml")" = "$(id -u)"
      test "$(stat -c %a "$owner_root/loom/config.toml")" = 600
      test "$(stat -c %h "$owner_root/loom/config.toml")" = 1
    done

    owner_0_config="$owner_0_xdg/loom/config.toml"
    owner_1_config="$owner_1_xdg/loom/config.toml"
    owner_0_xdg_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_0_xdg")"
    owner_1_xdg_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_1_xdg")"
    owner_0_xdg_device_inode="$(stat -c '%d:%i' "$owner_0_xdg")"
    owner_1_xdg_device_inode="$(stat -c '%d:%i' "$owner_1_xdg")"
    test "$owner_0_xdg_device_inode" != "$owner_1_xdg_device_inode"
    test "$(realpath -e "$owner_0_config")" != "$(realpath -e "$owner_1_config")"
    owner_0_config_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_0_config")"
    owner_1_config_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_1_config")"
    owner_0_config_device_inode="$(stat -c '%d:%i' "$owner_0_config")"
    owner_1_config_device_inode="$(stat -c '%d:%i' "$owner_1_config")"
    test "$owner_0_config_identity" != "$owner_1_config_identity"
    test "$owner_0_config_device_inode" != "$owner_1_config_device_inode"
    owner_0_config_sha256="$(sha256sum "$owner_0_config" | awk '{print $1}')"
    owner_1_config_sha256="$(sha256sum "$owner_1_config" | awk '{print $1}')"

    owner_0_whoami="$evidence_dir/owner-0.whoami.json"
    owner_1_whoami="$evidence_dir/owner-1.whoami.json"
    XDG_CONFIG_HOME="$owner_0_xdg" loom auth whoami --format json > "$owner_0_whoami"
    XDG_CONFIG_HOME="$owner_1_xdg" loom auth whoami --format json > "$owner_1_whoami"
    chmod 0600 "$owner_0_whoami" "$owner_1_whoami"
    assert_canonical_json_line "$owner_0_whoami"
    assert_canonical_json_line "$owner_1_whoami"
    jq -e --arg user "$(jq -r '.acceptance_owners[0].user_id' "$acceptance_plan")" --arg team "$(jq -r '.acceptance_owners[0].team_id' "$acceptance_plan")" '.auth_kind == "bearer" and .credential_type == "user_owned_api_token" and .principal_type == "team" and .role == null and .user_id == $user and .team_id == $team and .scopes == ["read:own","submit"] and (has("token") | not) and (has("session_cookie") | not) and (has("csrf") | not)' "$owner_0_whoami" >/dev/null
    jq -e --arg user "$(jq -r '.acceptance_owners[1].user_id' "$acceptance_plan")" --arg team "$(jq -r '.acceptance_owners[1].team_id' "$acceptance_plan")" '.auth_kind == "bearer" and .credential_type == "user_owned_api_token" and .principal_type == "team" and .role == null and .user_id == $user and .team_id == $team and .scopes == ["read:own","submit"] and (has("token") | not) and (has("session_cookie") | not) and (has("csrf") | not)' "$owner_1_whoami" >/dev/null
    owner_0_principal="$(jq -cS '{user_id,team_id}' "$owner_0_whoami")"
    owner_1_principal="$(jq -cS '{user_id,team_id}' "$owner_1_whoami")"
    test "$owner_0_principal" != "$owner_1_principal"

    assert_reviewed_owner_servers() {
      local owner_0_server owner_1_server
      owner_0_server="$(jq -er '.server | select(type == "string" and length > 0)' "$owner_0_whoami")"
      owner_1_server="$(jq -er '.server | select(type == "string" and length > 0)' "$owner_1_whoami")"
      test "$owner_0_server" = "$owner_1_server"
      test "$owner_0_server" = "$reviewed_server"
      test "$owner_1_server" = "$reviewed_server"
    }
    assert_reviewed_owner_servers

    assert_owner_sessions() {
      local owner_0_check owner_1_check
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_0_xdg")" = "$owner_0_xdg_identity"
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_1_xdg")" = "$owner_1_xdg_identity"
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_0_config")" = "$owner_0_config_identity"
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_1_config")" = "$owner_1_config_identity"
      test "$(sha256sum "$owner_0_config" | awk '{print $1}')" = "$owner_0_config_sha256"
      test "$(sha256sum "$owner_1_config" | awk '{print $1}')" = "$owner_1_config_sha256"
      owner_0_check="$(mktemp "$evidence_dir/owner-0-check.XXXXXX.json")"
      owner_1_check="$(mktemp "$evidence_dir/owner-1-check.XXXXXX.json")"
      XDG_CONFIG_HOME="$owner_0_xdg" loom auth whoami --format json > "$owner_0_check"
      XDG_CONFIG_HOME="$owner_1_xdg" loom auth whoami --format json > "$owner_1_check"
      cmp -s "$owner_0_check" "$owner_0_whoami"
      cmp -s "$owner_1_check" "$owner_1_whoami"
      rm -f "$owner_0_check" "$owner_1_check"
    }

    declare -A source_real_paths=()
    for source_root in "$owner_0_source_v1" "$owner_0_source_v2" "$owner_1_source_v1" "$owner_1_source_v2"; do
      test -d "$source_root"
      test ! -L "$source_root"
      source_real_path="$(realpath -e "$source_root")"
      test "$source_real_path" = "$source_root"
      test "$source_real_path" != "$repo"
      source_real_paths["$source_real_path"]=1
    done
    test "${#source_real_paths[@]}" -eq 4

## 6. Run concurrent initial deploys and updates

Start both owner-specific candidate-aware commands before either wait. Minimum
demand is explicitly zero. Initial maxima are 2 and 2; owner 0 then updates to
3 while owner 1 updates to 4, using independent arbitrary source roots.

    owner_0_initial="$evidence_dir/owner-0.initial.json"
    owner_1_initial="$evidence_dir/owner-1.initial.json"
    owner_0_updated="$evidence_dir/owner-0.updated.json"
    owner_1_updated="$evidence_dir/owner-1.updated.json"

    assert_owner_sessions
    assert_live_acceptance "$pre_deploy_acceptance_status"
    ( XDG_CONFIG_HOME="$owner_0_xdg" loom service up --environment "dev-$owner_0_name" --source-root "$owner_0_source_v1" --min-slots 0 --max-slots 2 ) > "$evidence_dir/owner-0.deploy-v1.txt" 2>&1 &
    owner_0_deploy_pid=$!
    ( XDG_CONFIG_HOME="$owner_1_xdg" loom service up --environment "dev-$owner_1_name" --source-root "$owner_1_source_v1" --min-slots 0 --max-slots 2 ) > "$evidence_dir/owner-1.deploy-v1.txt" 2>&1 &
    owner_1_deploy_pid=$!

    owner_0_deploy_status=0
    owner_1_deploy_status=0
    wait "$owner_0_deploy_pid" || owner_0_deploy_status=$?
    wait "$owner_1_deploy_pid" || owner_1_deploy_status=$?
    test "$owner_0_deploy_status" -eq 0
    test "$owner_1_deploy_status" -eq 0
    chmod 0600 "$evidence_dir/owner-0.deploy-v1.txt" "$evidence_dir/owner-1.deploy-v1.txt"

    XDG_CONFIG_HOME="$owner_0_xdg" loom dev status "$owner_0_name" --format json > "$owner_0_initial"
    XDG_CONFIG_HOME="$owner_1_xdg" loom dev status "$owner_1_name" --format json > "$owner_1_initial"
    chmod 0600 "$owner_0_initial" "$owner_1_initial"
    jq -e --arg user "$(jq -r '.acceptance_owners[0].user_id' "$acceptance_plan")" --arg team "$(jq -r '.acceptance_owners[0].team_id' "$acceptance_plan")" '.status == "ready" and .application_status == "ready" and .capacity_prepared == true and .worker_available == false and .min_slots == 0 and .max_slots == 2 and .owner_user_id == $user and .owner_team_id == $team' "$owner_0_initial" >/dev/null
    jq -e --arg user "$(jq -r '.acceptance_owners[1].user_id' "$acceptance_plan")" --arg team "$(jq -r '.acceptance_owners[1].team_id' "$acceptance_plan")" '.status == "ready" and .application_status == "ready" and .capacity_prepared == true and .worker_available == false and .min_slots == 0 and .max_slots == 2 and .owner_user_id == $user and .owner_team_id == $team' "$owner_1_initial" >/dev/null
    assert_live_acceptance "$initial_acceptance_status"

    assert_owner_sessions
    ( XDG_CONFIG_HOME="$owner_0_xdg" loom service up --environment "dev-$owner_0_name" --source-root "$owner_0_source_v2" --min-slots 0 --max-slots 3 ) > "$evidence_dir/owner-0.deploy-v2.txt" 2>&1 &
    owner_0_update_pid=$!
    ( XDG_CONFIG_HOME="$owner_1_xdg" loom service up --environment "dev-$owner_1_name" --source-root "$owner_1_source_v2" --min-slots 0 --max-slots 4 ) > "$evidence_dir/owner-1.deploy-v2.txt" 2>&1 &
    owner_1_update_pid=$!

    owner_0_update_status=0
    owner_1_update_status=0
    wait "$owner_0_update_pid" || owner_0_update_status=$?
    wait "$owner_1_update_pid" || owner_1_update_status=$?
    test "$owner_0_update_status" -eq 0
    test "$owner_1_update_status" -eq 0
    chmod 0600 "$evidence_dir/owner-0.deploy-v2.txt" "$evidence_dir/owner-1.deploy-v2.txt"

    XDG_CONFIG_HOME="$owner_0_xdg" loom dev status "$owner_0_name" --format json > "$owner_0_updated"
    XDG_CONFIG_HOME="$owner_1_xdg" loom dev status "$owner_1_name" --format json > "$owner_1_updated"
    chmod 0600 "$owner_0_updated" "$owner_1_updated"
    jq -e '.status == "ready" and .worker_available == false and .min_slots == 0 and .max_slots == 3' "$owner_0_updated" >/dev/null
    jq -e '.status == "ready" and .worker_available == false and .min_slots == 0 and .max_slots == 4' "$owner_1_updated" >/dev/null
    test "$(jq -r .candidate_sha "$owner_0_initial")" != "$(jq -r .candidate_sha "$owner_0_updated")"
    test "$(jq -r .candidate_sha "$owner_1_initial")" != "$(jq -r .candidate_sha "$owner_1_updated")"
    test "$(jq -r .candidate_sha "$owner_0_updated")" != "$(jq -r .candidate_sha "$owner_1_updated")"
    owner_0_candidate="$(jq -r .candidate_sha "$owner_0_updated")"
    owner_1_candidate="$(jq -r .candidate_sha "$owner_1_updated")"
    assert_live_acceptance "$updated_acceptance_status"

## 7. Prove bidirectional cross-owner isolation

The updated environments must have disjoint subject, namespace, database,
bucket, route, host, and worker-pool identities. Then run read, update, and
destroy denial probes in exact owner-0-to-owner-1 then owner-1-to-owner-0 order.
Each target owner captures canonical status before and after the probe. Exact
exit status 1, empty stdout, the operation's exact canonical hidden-resource
receipt, byte-equal target status, and a fresh live acceptance interlock are
mandatory after every attempt. Candidate, preflight, credential, server, or
successful target responses cannot certify a denial.

    for field in subject_id subject_incarnation identity.environment identity.namespace identity.database identity.task_bucket identity.trajectories_bucket identity.artifacts_bucket identity.route_host identity.worker_control_plane_host identity.worker_gateway_host identity.route_path identity.worker_pool; do
      test "$(jq -r ".$field" "$owner_0_updated")" != "$(jq -r ".$field" "$owner_1_updated")"
    done
    : > "$denials_jsonl"
    chmod 0600 "$denials_jsonl"

    probe_cross_owner_denial() {
      local actor_xdg="$1"
      local actor_candidate="$2"
      local target_xdg="$3"
      local target_name="$4"
      local actor_index="$5"
      local target_index="$6"
      local operation="$7"
      local prefix="$evidence_dir/denial-$actor_index-$target_index-$operation"
      local before="$prefix.before.json"
      local after="$prefix.after.json"
      local stdout="$prefix.stdout"
      local stderr="$prefix.stderr"
      local expected_stderr="$prefix.expected.stderr"
      local interlock_status="$prefix.interlock.json"
      local expected_receipt
      local target_epoch
      local rc=0

      XDG_CONFIG_HOME="$target_xdg" loom dev status "$target_name" --format json | jq -cS . > "$before"
      target_epoch="$(jq -er ".operation_epoch | select(type == \"number\" and . > 0)" "$before")"
      case "$operation" in
        read)
          expected_receipt='{"error_code":"resource_hidden","http_method":"GET","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_read"}'
          XDG_CONFIG_HOME="$actor_xdg" loom dev status "$target_name" --format json --expected-hidden-denial > "$stdout" 2> "$stderr" || rc=$?
          ;;
        update)
          expected_receipt='{"error_code":"resource_hidden","http_method":"PUT","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_update"}'
          XDG_CONFIG_HOME="$actor_xdg" loom service up --environment "dev-$target_name" --candidate "$actor_candidate" --expected-operation-epoch 1 --min-slots 0 --quiet --expected-hidden-denial > "$stdout" 2> "$stderr" || rc=$?
          ;;
        destroy)
          expected_receipt='{"error_code":"resource_hidden","http_method":"DELETE","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_destroy"}'
          XDG_CONFIG_HOME="$actor_xdg" loom dev destroy "$target_name" --format json --expected-operation-epoch "$target_epoch" --expected-hidden-denial > "$stdout" 2> "$stderr" || rc=$?
          ;;
        *)
          return 2
          ;;
      esac
      test "$rc" -eq 1
      test ! -s "$stdout"
      printf '%s\n' "$expected_receipt" > "$expected_stderr"
      cmp -s "$stderr" "$expected_stderr"
      rm -f "$expected_stderr"
      XDG_CONFIG_HOME="$target_xdg" loom dev status "$target_name" --format json | jq -cS . > "$after"
      chmod 0600 "$before" "$after" "$stdout" "$stderr"
      assert_canonical_json_line "$before"
      assert_canonical_json_line "$after"
      cmp -s "$before" "$after"
      assert_live_acceptance "$interlock_status"

      jq -cS -n \
        --arg actor_team_id "$(jq -r ".acceptance_owners[$actor_index].team_id" "$acceptance_plan")" \
        --arg actor_user_id "$(jq -r ".acceptance_owners[$actor_index].user_id" "$acceptance_plan")" \
        --arg operation "$operation" \
        --arg stderr_sha256 "$(sha256sum "$stderr" | awk '{print $1}')" \
        --arg stdout_sha256 "$(sha256sum "$stdout" | awk '{print $1}')" \
        --arg target_after_sha256 "$(sha256sum "$after" | awk '{print $1}')" \
        --arg target_before_sha256 "$(sha256sum "$before" | awk '{print $1}')" \
        --arg target_environment "$target_name" \
        --arg target_team_id "$(jq -r ".acceptance_owners[$target_index].team_id" "$acceptance_plan")" \
        --arg target_user_id "$(jq -r ".acceptance_owners[$target_index].user_id" "$acceptance_plan")" \
        '{actor_team_id:$actor_team_id,actor_user_id:$actor_user_id,exit_code:1,operation:$operation,stderr_sha256:$stderr_sha256,stdout_sha256:$stdout_sha256,target_after_sha256:$target_after_sha256,target_before_sha256:$target_before_sha256,target_environment:$target_environment,target_team_id:$target_team_id,target_user_id:$target_user_id}' \
        >> "$denials_jsonl"
    }

    assert_owner_sessions
    probe_cross_owner_denial "$owner_0_xdg" "$owner_0_candidate" "$owner_1_xdg" "$owner_1_name" 0 1 read
    probe_cross_owner_denial "$owner_0_xdg" "$owner_0_candidate" "$owner_1_xdg" "$owner_1_name" 0 1 update
    probe_cross_owner_denial "$owner_0_xdg" "$owner_0_candidate" "$owner_1_xdg" "$owner_1_name" 0 1 destroy
    probe_cross_owner_denial "$owner_1_xdg" "$owner_1_candidate" "$owner_0_xdg" "$owner_0_name" 1 0 read
    probe_cross_owner_denial "$owner_1_xdg" "$owner_1_candidate" "$owner_0_xdg" "$owner_0_name" 1 0 update
    probe_cross_owner_denial "$owner_1_xdg" "$owner_1_candidate" "$owner_0_xdg" "$owner_0_name" 1 0 destroy
    test "$(wc -l < "$denials_jsonl")" = 6
    assert_live_acceptance "$post_denials_acceptance_status"

## 8. Destroy normally, retain, redeploy, and finish cleanup

Owner 0 uses default data removal. Owner 1 uses `--keep-data`, then redeploys
the retained name. The retained subject ID must remain stable while its
incarnation rotates. Finally destroy owner 1 normally and prove all dynamic
namespaces are absent before rollback.

    owner_0_destroyed="$evidence_dir/owner-0.destroyed.json"
    owner_1_destroyed="$evidence_dir/owner-1.destroyed.json"
    owner_1_redeployed="$evidence_dir/owner-1.redeployed.json"
    owner_1_final_destroyed="$evidence_dir/owner-1.final-destroyed.json"
    retained_subject_id="$(jq -r .subject_id "$owner_1_updated")"
    retained_incarnation="$(jq -r .subject_incarnation "$owner_1_updated")"

    assert_owner_sessions
    XDG_CONFIG_HOME="$owner_0_xdg" loom dev destroy "$owner_0_name" --format json > "$owner_0_destroyed"
    XDG_CONFIG_HOME="$owner_1_xdg" loom dev destroy "$owner_1_name" --keep-data --format json > "$owner_1_destroyed"
    chmod 0600 "$owner_0_destroyed" "$owner_1_destroyed"
    jq -e '.status == "deleted" and .keep_data == false' "$owner_0_destroyed" >/dev/null
    jq -e '.status == "deleted" and .keep_data == true' "$owner_1_destroyed" >/dev/null
    owner_1_redeploy_epoch="$(jq -er ".operation_epoch | select(type == \"number\" and . > 0)" "$owner_1_destroyed")"
    assert_live_acceptance "$post_destroy_acceptance_status"

    assert_owner_sessions
    XDG_CONFIG_HOME="$owner_1_xdg" loom service up --environment "dev-$owner_1_name" --source-root "$owner_1_source_v2" --expected-operation-epoch "$owner_1_redeploy_epoch" --min-slots 0 --max-slots 2 > "$evidence_dir/owner-1.redeploy.txt" 2>&1
    XDG_CONFIG_HOME="$owner_1_xdg" loom dev status "$owner_1_name" --format json > "$owner_1_redeployed"
    chmod 0600 "$evidence_dir/owner-1.redeploy.txt" "$owner_1_redeployed"
    jq -e --arg subject "$retained_subject_id" --arg incarnation "$retained_incarnation" '.status == "ready" and .subject_id == $subject and .subject_incarnation != $incarnation and .deployment_generation == 1 and .worker_available == false and .min_slots == 0 and .max_slots == 2' "$owner_1_redeployed" >/dev/null
    assert_live_acceptance "$post_redeploy_acceptance_status"

    assert_owner_sessions
    XDG_CONFIG_HOME="$owner_1_xdg" loom dev destroy "$owner_1_name" --format json > "$owner_1_final_destroyed"
    chmod 0600 "$owner_1_final_destroyed"
    jq -e '.status == "deleted" and .keep_data == false' "$owner_1_final_destroyed" >/dev/null
    assert_no_dynamic_namespaces
    assert_live_acceptance "$pre_rollback_acceptance_status"

## 9. Reapply the byte-reviewed inert shadow

Re-render the rollback shadow from the same profile and trusted release, then
require byte equality with the artifact reviewed before forward apply. Do not
synthesize rollback YAML from live state. Server-side apply does not prune
objects, so any resource-identity difference is a stop condition requiring a
separate reviewed plan.

    shadow_render_recheck="$(mktemp "$evidence_dir/shadow-recheck.XXXXXX.yaml")"
    shadow_evidence_recheck="$(mktemp "$evidence_dir/shadow-recheck.XXXXXX.json")"
    if ! "$loom_cli" admin personal-dev-control-plane render --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" > "$shadow_render_recheck" 2> "$shadow_evidence_recheck"; then
      rm -f "$shadow_render_recheck" "$shadow_evidence_recheck"
      exit 1
    fi
    chmod 0600 "$shadow_render_recheck" "$shadow_evidence_recheck"
    cmp -s "$shadow_render_recheck" "$shadow_render"
    rm -f "$shadow_render_recheck" "$shadow_evidence_recheck"

    rollback_diff_status=0
    kubectl --kubeconfig "$kubeconfig" diff --server-side --field-manager=loom-personal-dev-control-plane -f "$shadow_render" > "$evidence_dir/rollback.server-side-diff.txt" 2>&1 || rollback_diff_status=$?
    test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1
    chmod 0600 "$evidence_dir/rollback.server-side-diff.txt"

    assert_rollback_interlocks
    kubectl --kubeconfig "$kubeconfig" apply --server-side --field-manager=loom-personal-dev-control-plane -f "$shadow_render" > "$evidence_dir/rollback.server-side-apply.txt"
    chmod 0600 "$evidence_dir/rollback.server-side-apply.txt"

    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status deployment/loom-personal-dev-management --timeout=300s
    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status deployment/loom-personal-dev-web --timeout=300s
    capture_scanner_cache_init_status "$rollback_scanner_cache_init_status"
    assert_web_api_route_contract
    test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get deployment/loom-personal-dev-activation-agent -o jsonpath='{.spec.replicas}')" = 0
    "$loom_cli" admin personal-dev-control-plane status --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" > "$rollback_status"
    chmod 0600 "$rollback_status"
    assert_canonical_json_line "$rollback_status"
    jq -e '.schema == "loom-personal-dev-control-plane-status-v1" and .mode == "shadow" and .ready == true and .blockers == [] and .manager_ceiling == 0 and all(.components[]; .ready == true)' "$rollback_status" >/dev/null
    rollback_shadow_status_sha256="$(sha256sum "$rollback_status" | awk '{print $1}')"
    assert_no_dynamic_namespaces

## 10. Assemble and verify canonical v2 evidence while inert

Only after normal final cleanup, byte-exact shadow reapply, and successful
shadow status may the canonical result exist. Project every lifecycle snapshot
to the strict non-secret field set, bind all six already byte-validated
canonical denial-receipt digests and eight status digests, and verify the final
digest through the read-only strict v2 loader. The loader independently
requires the exact operation-specific receipt SHA-256 values. It hashes the
exact owner-only rollback manifest and requires every top-level rendered
resource's render-input and trusted-release annotations to match the observed
rollback status and accepted result. A verification failure leaves the system
inert and blocks durable launch.

    project_acceptance_snapshot() {
      local source="$1"
      local selected="$2"
      jq -cS '{application_status,candidate_sha,capacity_prepared,capacity_status,deployment_generation,identity:{environment:.identity.environment,namespace:.identity.namespace,database:.identity.database,task_bucket:.identity.task_bucket,trajectories_bucket:.identity.trajectories_bucket,artifacts_bucket:.identity.artifacts_bucket,route_host:.identity.route_host,worker_control_plane_host:.identity.worker_control_plane_host,worker_gateway_host:.identity.worker_gateway_host,route_path:.identity.route_path,worker_pool:.identity.worker_pool},keep_data,max_slots,min_slots,name,operation_epoch,owner_team_id,owner_user_id,status,subject_id,subject_incarnation,worker_available}' "$source" > "$selected"
      chmod 0600 "$selected"
      assert_canonical_json_line "$selected"
    }

    owner_0_initial_selected="$evidence_dir/owner-0.initial.selected.json"
    owner_0_updated_selected="$evidence_dir/owner-0.updated.selected.json"
    owner_0_destroyed_selected="$evidence_dir/owner-0.destroyed.selected.json"
    owner_1_initial_selected="$evidence_dir/owner-1.initial.selected.json"
    owner_1_updated_selected="$evidence_dir/owner-1.updated.selected.json"
    owner_1_destroyed_selected="$evidence_dir/owner-1.destroyed.selected.json"
    owner_1_redeployed_selected="$evidence_dir/owner-1.redeployed.selected.json"
    owner_1_final_destroyed_selected="$evidence_dir/owner-1.final-destroyed.selected.json"
    project_acceptance_snapshot "$owner_0_initial" "$owner_0_initial_selected"
    project_acceptance_snapshot "$owner_0_updated" "$owner_0_updated_selected"
    project_acceptance_snapshot "$owner_0_destroyed" "$owner_0_destroyed_selected"
    project_acceptance_snapshot "$owner_1_initial" "$owner_1_initial_selected"
    project_acceptance_snapshot "$owner_1_updated" "$owner_1_updated_selected"
    project_acceptance_snapshot "$owner_1_destroyed" "$owner_1_destroyed_selected"
    project_acceptance_snapshot "$owner_1_redeployed" "$owner_1_redeployed_selected"
    project_acceptance_snapshot "$owner_1_final_destroyed" "$owner_1_final_destroyed_selected"

    jq -cS -j -n \
      --arg acceptance_manifest_sha256 "$acceptance_render_sha256" \
      --arg acceptance_plan_sha256 "$acceptance_plan_sha256" \
      --arg after_denials "$(sha256sum "$post_denials_acceptance_status" | awk '{print $1}')" \
      --arg after_destroy "$(sha256sum "$post_destroy_acceptance_status" | awk '{print $1}')" \
      --arg after_initial "$(sha256sum "$initial_acceptance_status" | awk '{print $1}')" \
      --arg after_redeploy "$(sha256sum "$post_redeploy_acceptance_status" | awk '{print $1}')" \
      --arg after_updates "$(sha256sum "$updated_acceptance_status" | awk '{print $1}')" \
      --arg pre_deploy "$(sha256sum "$pre_deploy_acceptance_status" | awk '{print $1}')" \
      --arg pre_rollback "$(sha256sum "$pre_rollback_acceptance_status" | awk '{print $1}')" \
      --arg release_sha256 "$trusted_release_sha256" \
      --arg rollback_shadow "$rollback_shadow_status_sha256" \
      --arg shadow_manifest_sha256 "$shadow_render_sha256" \
      --slurpfile cross_owner_denials "$denials_jsonl" \
      --slurpfile owner_0_destroyed "$owner_0_destroyed_selected" \
      --slurpfile owner_0_initial "$owner_0_initial_selected" \
      --slurpfile owner_0_updated "$owner_0_updated_selected" \
      --slurpfile owner_1_destroyed "$owner_1_destroyed_selected" \
      --slurpfile owner_1_final_destroyed "$owner_1_final_destroyed_selected" \
      --slurpfile owner_1_initial "$owner_1_initial_selected" \
      --slurpfile owner_1_redeployed "$owner_1_redeployed_selected" \
      --slurpfile owner_1_updated "$owner_1_updated_selected" \
      '{acceptance_manifest_sha256:$acceptance_manifest_sha256,acceptance_plan_sha256:$acceptance_plan_sha256,cross_owner_denials:$cross_owner_denials,owners:[{destroyed:$owner_0_destroyed[0],final_destroyed:null,initial:$owner_0_initial[0],redeployed:null,updated:$owner_0_updated[0]},{destroyed:$owner_1_destroyed[0],final_destroyed:$owner_1_final_destroyed[0],initial:$owner_1_initial[0],redeployed:$owner_1_redeployed[0],updated:$owner_1_updated[0]}],release_sha256:$release_sha256,schema:"loom-personal-dev-zero-capacity-acceptance-result-v2",shadow_manifest_sha256:$shadow_manifest_sha256,status_sha256s:{after_denials:$after_denials,after_destroy:$after_destroy,after_initial:$after_initial,after_redeploy:$after_redeploy,after_updates:$after_updates,pre_deploy:$pre_deploy,pre_rollback:$pre_rollback,rollback_shadow:$rollback_shadow}}' \
      > "$acceptance_result"
    chmod 0600 "$acceptance_result"
    acceptance_result_sha256="$(canonical_json_sha256 "$acceptance_result")"
    test "$acceptance_result_sha256" = "$(sha256sum "$acceptance_result" | awk '{print $1}')"
    printf '%s\n' "$acceptance_result_sha256" > "$evidence_dir/acceptance-result-v2.sha256"
    chmod 0600 "$evidence_dir/acceptance-result-v2.sha256"

    "$loom_cli" admin personal-dev-control-plane verify-acceptance-result \
      --acceptance-plan-file "$acceptance_plan" \
      --acceptance-plan-sha256 "$acceptance_plan_sha256" \
      --acceptance-result-file "$acceptance_result" \
      --acceptance-result-sha256 "$acceptance_result_sha256" \
      --acceptance-manifest-sha256 "$acceptance_render_sha256" \
      --rollback-shadow-manifest-file "$shadow_render" \
      --rollback-shadow-status-file "$rollback_status" \
      > "$acceptance_verification"
    chmod 0600 "$acceptance_verification"
    assert_canonical_json_line "$acceptance_verification"
    jq -e \
      --arg result "$acceptance_result_sha256" \
      --arg rollback_shadow_status_sha256 "$rollback_shadow_status_sha256" \
      '.schema == "loom-personal-dev-zero-capacity-acceptance-verification-v1" and
       .verified == true and .owner_count == 2 and
       .cross_owner_denial_count == 6 and
       .acceptance_result_sha256 == $result and
       .rollback_shadow_status_sha256 == $rollback_shadow_status_sha256' \
      "$acceptance_verification" >/dev/null

A verified result from this procedure is the required acceptance input to
[`personal-dev-multi-owner-durable-launch.md`](personal-dev-multi-owner-durable-launch.md).

Retain the trusted-release artifact, acceptance plan, backup/restore record,
both reviewed manifests, scanner-init completion records, diffs, applies,
status records, owner operation records, canonical result, and their hashes.
The final state is the inert shared management shadow: the release-bound cache
generation remains prepared, lifecycle and builder are disabled, activation
replicas are zero, no dynamic namespace or personal worker exists, and global
executable capacity is still exactly zero.
