# Personal-development zero-capacity acceptance

This runbook is the controlled issue #1280 acceptance and rollback procedure
for the personal-development management plane. It enables lifecycle, restricted
source building, and stable-route activation in the shared infrastructure
namespace loom-dev, then exercises two concurrent owners in loom-dev-<owner>
namespaces. Physical capacity remains unchanged: the exact global manager stays
at executable-new-capacity ceiling zero, no personal worker exists, and no task
is submitted.

The acceptance proves that each owner can deploy arbitrary committed, modified, and untracked source
through the candidate-aware command. Architecture-specific and architecture-neutral tasks are out of scope
until the separately reviewed issue #906 work is complete.

Repository merge, operational approval, and a successful render are each
necessary but not sufficient authority for the live apply. Run the mutable
sections only inside the explicit issue #1280 acceptance window and only after
every interlock below passes.

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

    evidence_dir="<absolute-owner-only-outside-worktree-issue-1280-evidence-directory>"
    trusted_release_artifact="<absolute-downloaded-personal-dev-trusted-release-run-<run>-attempt-<attempt>>"
    trusted_release="$trusted_release_artifact/trusted-release.json"
    trusted_release_evidence="$trusted_release_artifact/trusted-release-evidence.json"
    acceptance_plan="<absolute-owner-only-acceptance-plan.json>"
    profile="$(pwd -P)/deploy/dev-fleet/personal-dev-control-plane.toml"
    kubeconfig="<absolute-reviewed-self-contained-mode-0600-kubeconfig>"
    expected_kube_context="<exact-reviewed-kubernetes-context>"

    acceptance_render="$evidence_dir/reviewed-acceptance.yaml"
    acceptance_render_evidence="$evidence_dir/reviewed-acceptance.render.json"
    shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"
    shadow_render_evidence="$evidence_dir/reviewed-rollback-shadow.render.json"
    pre_acceptance_status="$evidence_dir/pre-acceptance.status.json"
    post_acceptance_status="$evidence_dir/post-acceptance.status.json"
    initial_acceptance_status="$evidence_dir/initial-two-owner.status.json"
    updated_acceptance_status="$evidence_dir/updated-two-owner.status.json"
    post_rejection_acceptance_status="$evidence_dir/post-rejection.status.json"
    post_destroy_acceptance_status="$evidence_dir/post-destroy.status.json"
    final_acceptance_status="$evidence_dir/final-acceptance.status.json"
    rollback_pre_status="$evidence_dir/rollback-pre.status.json"
    rollback_status="$evidence_dir/rollback-shadow.status.json"

    backup_restore_evidence="<absolute-owner-only-backup-restore-evidence>"
    runtime_profile="<absolute-reviewed-runtime-profile>"
    trusted_launcher_profile="<absolute-reviewed-trusted-launcher-profile>"
    scanner_binary="<absolute-reviewed-scanner-binary>"
    scanner_database="<absolute-reviewed-scanner-database-archive>"
    scanner_java_database="<absolute-reviewed-scanner-java-database-archive>"
    scanner_finding_policy="<absolute-reviewed-scanner-finding-policy>"

    owner_a_xdg="<absolute-mode-0700-owner-a-xdg-config-root>"
    owner_b_xdg="<absolute-mode-0700-owner-b-xdg-config-root>"
    owner_a_name="<owner-a-personal-name>"
    owner_b_name="<owner-b-personal-name>"
    owner_a_source_v1="<absolute-owner-a-source-v1>"
    owner_a_source_v2="<absolute-owner-a-source-v2>"
    owner_b_source_v1="<absolute-owner-b-source-v1>"
    owner_b_source_v2="<absolute-owner-b-source-v2>"

    install -d -m 0700 "$evidence_dir"
    test -z "$(find "$evidence_dir" -mindepth 1 -maxdepth 1 -print -quit)"
    export PYTHONPATH=src:.
    loom() {
      /home/hongjian/loom/.venv/bin/loom "$@"
    }

The two XDG roots are two distinct authenticated owner sessions, not aliases
for one credential. Prepare their credentials through the approved
authentication channel before continuing. Do not copy a token between roots.

The following helpers reject duplicate/noncanonical JSON and bind every
owner-only input to stable bytes.

    canonical_json_sha256() {
      /home/hongjian/loom/.venv/bin/python - "$1" <<'PY'
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

    assert_owner_only_file "$kubeconfig"
    kubeconfig_sha256="$(sha256sum "$kubeconfig" | awk '{print $1}')"
    kubeconfig_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$kubeconfig")"

    backup_restore_evidence_sha256="$(jq -r .storage.backup_restore_evidence_sha256 "$acceptance_plan")"
    runtime_profile_sha256="$(jq -r .builder.runtime_profile_sha256 "$acceptance_plan")"
    trusted_launcher_profile_sha256="$(jq -r .builder.trusted_launcher_profile_sha256 "$acceptance_plan")"
    scanner_binary_sha256="$(jq -r .builder.scanner_binary_sha256 "$acceptance_plan")"
    scanner_database_sha256="$(jq -r .builder.scanner_database_sha256 "$acceptance_plan")"
    scanner_java_database_sha256="$(jq -r .builder.scanner_java_database_sha256 "$acceptance_plan")"
    scanner_finding_policy_sha256="$(jq -r .builder.scanner_finding_policy_sha256 "$acceptance_plan")"

    for path in "$backup_restore_evidence" "$runtime_profile" "$trusted_launcher_profile" "$scanner_finding_policy"; do
      assert_owner_only_file "$path"
    done
    assert_owner_only_file "$scanner_binary" 536870912
    assert_owner_only_file "$scanner_database" 4294967296
    assert_owner_only_file "$scanner_java_database" 4294967296
    test "$(sha256sum "$backup_restore_evidence" | awk '{print $1}')" = "$backup_restore_evidence_sha256"
    test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = "$runtime_profile_sha256"
    test "$(sha256sum "$trusted_launcher_profile" | awk '{print $1}')" = "$trusted_launcher_profile_sha256"
    test "$(sha256sum "$scanner_binary" | awk '{print $1}')" = "$scanner_binary_sha256"
    test "$(sha256sum "$scanner_database" | awk '{print $1}')" = "$scanner_database_sha256"
    test "$(sha256sum "$scanner_java_database" | awk '{print $1}')" = "$scanner_java_database_sha256"
    test "$(sha256sum "$scanner_finding_policy" | awk '{print $1}')" = "$scanner_finding_policy_sha256"

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

    assert_current_artifacts() {
      assert_owner_only_file "$trusted_release"
      assert_owner_only_file "$trusted_release_evidence"
      assert_owner_only_file "$acceptance_plan"
      for path in "$backup_restore_evidence" "$runtime_profile" "$trusted_launcher_profile" "$scanner_finding_policy" "$acceptance_render" "$acceptance_render_evidence" "$shadow_render" "$shadow_render_evidence"; do
        assert_owner_only_file "$path"
      done
      assert_owner_only_file "$scanner_binary" 536870912
      assert_owner_only_file "$scanner_database" 4294967296
      assert_owner_only_file "$scanner_java_database" 4294967296
      test "$(git rev-parse HEAD)" = "$(jq -r .source_sha "$trusted_release")"
      test "$(git rev-parse HEAD^{tree})" = "$(jq -r .source_tree "$trusted_release")"
      test -z "$(git status --porcelain=v1 --untracked-files=all)"
      test "$(sha256sum "$profile" | awk '{print $1}')" = "$profile_sha256"
      test "$(canonical_json_sha256 "$trusted_release")" = "$trusted_release_sha256"
      test "$(canonical_json_sha256 "$trusted_release_evidence")" = "$(jq -r .release_evidence_sha256 "$trusted_release")"
      test "$(canonical_json_sha256 "$acceptance_plan")" = "$acceptance_plan_sha256"
      test "$(sha256sum "$backup_restore_evidence" | awk '{print $1}')" = "$backup_restore_evidence_sha256"
      test "$(sha256sum "$runtime_profile" | awk '{print $1}')" = "$runtime_profile_sha256"
      test "$(sha256sum "$trusted_launcher_profile" | awk '{print $1}')" = "$trusted_launcher_profile_sha256"
      test "$(sha256sum "$scanner_binary" | awk '{print $1}')" = "$scanner_binary_sha256"
      test "$(sha256sum "$scanner_database" | awk '{print $1}')" = "$scanner_database_sha256"
      test "$(sha256sum "$scanner_java_database" | awk '{print $1}')" = "$scanner_java_database_sha256"
      test "$(sha256sum "$scanner_finding_policy" | awk '{print $1}')" = "$scanner_finding_policy_sha256"
      test "$(sha256sum "$acceptance_render" | awk '{print $1}')" = "$acceptance_render_sha256"
      test "$(sha256sum "$shadow_render" | awk '{print $1}')" = "$shadow_render_sha256"
      test "$(sha256sum "$acceptance_render_evidence" | awk '{print $1}')" = "$acceptance_render_evidence_sha256"
      test "$(sha256sum "$shadow_render_evidence" | awk '{print $1}')" = "$shadow_render_evidence_sha256"
      test "$shadow_render_sha256" = "$(jq -r .release.shadow_manifest_sha256 "$acceptance_plan")"
    }

    capture_pre_acceptance_status() {
      local status_rc=0
      /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane status-acceptance --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" > "$pre_acceptance_status" || status_rc=$?
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
      /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane status-acceptance --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" > "$output"
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
      /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane status-acceptance --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" > "$rollback_pre_status" || status_rc=$?
      test "$status_rc" -eq 0 || test "$status_rc" -eq 1
      chmod 0600 "$rollback_pre_status"
      assert_canonical_json_line "$rollback_pre_status"
      jq -e '.application_ready == true and .capacity_publication_ready == true and .worker_available == false and .manager_ceiling == 0 and ([.blockers[] | select(. != "acceptance_window_expired")] | length == 0) and all(.components[]; .ready == true) and any(.components[]; .name == "personal-workers" and .observed == 0 and .ready == true)' "$rollback_pre_status" >/dev/null
    }

The successful live acceptance record contains these exact facets:

    {"application_ready":true,"capacity_publication_ready":true,"manager_ceiling":0,"worker_available":false}

## 3. Render and byte-review forward and rollback manifests

Render both modes before mutation. The shadow digest must equal the acceptance
plan's exact rollback binding. Review both YAML files and both canonical render
records; the acceptance diff may enable only personal lifecycle/build flags,
one activation replica, and the plan-bound readiness/interlock settings.

    acceptance_tmp="$(mktemp "$evidence_dir/acceptance.XXXXXX.yaml")"
    acceptance_evidence_tmp="$(mktemp "$evidence_dir/acceptance.XXXXXX.json")"
    if ! /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane render-acceptance --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" --acceptance-plan-file "$acceptance_plan" --acceptance-plan-sha256 "$acceptance_plan_sha256" > "$acceptance_tmp" 2> "$acceptance_evidence_tmp"; then
      rm -f "$acceptance_tmp" "$acceptance_evidence_tmp"
      exit 1
    fi
    chmod 0600 "$acceptance_tmp" "$acceptance_evidence_tmp"
    mv "$acceptance_tmp" "$acceptance_render"
    mv "$acceptance_evidence_tmp" "$acceptance_render_evidence"

    shadow_tmp="$(mktemp "$evidence_dir/shadow.XXXXXX.yaml")"
    shadow_evidence_tmp="$(mktemp "$evidence_dir/shadow.XXXXXX.json")"
    if ! /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane render --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" > "$shadow_tmp" 2> "$shadow_evidence_tmp"; then
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
    jq -e --arg plan "$acceptance_plan_sha256" --arg yaml "$acceptance_render_sha256" '.schema == "loom-personal-dev-control-plane-render-v1" and .mode == "acceptance" and .acceptance_plan_sha256 == $plan and .yaml_sha256 == $yaml and .resource_count == 33' "$acceptance_render_evidence" >/dev/null
    jq -e --arg yaml "$shadow_render_sha256" '.schema == "loom-personal-dev-control-plane-render-v1" and .mode == "shadow" and .yaml_sha256 == $yaml and .resource_count == 33' "$shadow_render_evidence" >/dev/null

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
    kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status deployment/loom-personal-dev-activation-agent --timeout=300s
    assert_live_acceptance "$post_acceptance_status"

## 5. Verify the two owner sessions

Each root must be absolute, current-user-owned, mode 0700, and contain its own
mode-0600 single-link Loom configuration. The two identity records are
non-secret, owner-only evidence; review them against acceptance_owners before
starting a build. Keep these four arbitrary source roots separate from the
clean control-plane checkout used to render and observe the release.

    test "$owner_a_xdg" != "$owner_b_xdg"
    test "$owner_a_name" != "$owner_b_name"
    for owner_xdg in "$owner_a_xdg" "$owner_b_xdg"; do
      test -d "$owner_xdg" && test ! -L "$owner_xdg"
      test "$(realpath -e "$owner_xdg")" = "$owner_xdg"
      test "$(stat -c %u "$owner_xdg")" = "$(id -u)"
      test "$(stat -c %a "$owner_xdg")" = 700
      test -f "$owner_xdg/loom/config.toml" && test ! -L "$owner_xdg/loom/config.toml"
      test "$(stat -c %u "$owner_xdg/loom/config.toml")" = "$(id -u)"
      test "$(stat -c %a "$owner_xdg/loom/config.toml")" = 600
      test "$(stat -c %h "$owner_xdg/loom/config.toml")" = 1
    done

    XDG_CONFIG_HOME="$owner_a_xdg" loom auth whoami > "$evidence_dir/owner-a.whoami.txt"
    XDG_CONFIG_HOME="$owner_b_xdg" loom auth whoami > "$evidence_dir/owner-b.whoami.txt"
    chmod 0600 "$evidence_dir/owner-a.whoami.txt" "$evidence_dir/owner-b.whoami.txt"
    owner_a_config="$owner_a_xdg/loom/config.toml"
    owner_b_config="$owner_b_xdg/loom/config.toml"
    owner_a_config_sha256="$(sha256sum "$owner_a_config" | awk '{print $1}')"
    owner_b_config_sha256="$(sha256sum "$owner_b_config" | awk '{print $1}')"
    owner_a_config_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_a_config")"
    owner_b_config_identity="$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_b_config")"

    assert_owner_sessions() {
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_a_config")" = "$owner_a_config_identity"
      test "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%y:%z' "$owner_b_config")" = "$owner_b_config_identity"
      test "$(sha256sum "$owner_a_config" | awk '{print $1}')" = "$owner_a_config_sha256"
      test "$(sha256sum "$owner_b_config" | awk '{print $1}')" = "$owner_b_config_sha256"
    }

    for source_root in "$owner_a_source_v1" "$owner_a_source_v2" "$owner_b_source_v1" "$owner_b_source_v2"; do
      test -d "$source_root" && test ! -L "$source_root"
      test "$(realpath -e "$source_root")" = "$source_root"
    done

## 6. Concurrent initial deploys and updates

Start both candidate-aware commands before waiting so build and lifecycle
isolation are exercised concurrently. Minimum demand is explicitly zero;
different finite maxima exercise independent policy updates without requesting
executable capacity.

    assert_owner_sessions
    assert_live_acceptance "$evidence_dir/pre-owner-deploy.status.json"
    ( XDG_CONFIG_HOME="$owner_a_xdg" loom service up --environment "dev-$owner_a_name" --source-root "$owner_a_source_v1" --min-slots 0 --max-slots 2 ) > "$evidence_dir/owner-a.deploy-v1.txt" 2>&1 &
    owner_a_deploy_pid=$!
    ( XDG_CONFIG_HOME="$owner_b_xdg" loom service up --environment "dev-$owner_b_name" --source-root "$owner_b_source_v1" --min-slots 0 --max-slots 2 ) > "$evidence_dir/owner-b.deploy-v1.txt" 2>&1 &
    owner_b_deploy_pid=$!

    owner_a_deploy_status=0
    owner_b_deploy_status=0
    wait "$owner_a_deploy_pid" || owner_a_deploy_status=$?
    wait "$owner_b_deploy_pid" || owner_b_deploy_status=$?
    test "$owner_a_deploy_status" -eq 0
    test "$owner_b_deploy_status" -eq 0
    chmod 0600 "$evidence_dir/owner-a.deploy-v1.txt" "$evidence_dir/owner-b.deploy-v1.txt"

    XDG_CONFIG_HOME="$owner_a_xdg" loom dev status "$owner_a_name" --format json > "$evidence_dir/owner-a.v1.json"
    XDG_CONFIG_HOME="$owner_b_xdg" loom dev status "$owner_b_name" --format json > "$evidence_dir/owner-b.v1.json"
    chmod 0600 "$evidence_dir/owner-a.v1.json" "$evidence_dir/owner-b.v1.json"
    jq -e --arg user "$(jq -r .acceptance_owners[0].user_id "$acceptance_plan")" --arg team "$(jq -r .acceptance_owners[0].team_id "$acceptance_plan")" '.status == "ready" and .application_status == "ready" and .capacity_prepared == true and .worker_available == false and .min_slots == 0 and .owner_user_id == $user and .owner_team_id == $team' "$evidence_dir/owner-a.v1.json" >/dev/null
    jq -e --arg user "$(jq -r .acceptance_owners[1].user_id "$acceptance_plan")" --arg team "$(jq -r .acceptance_owners[1].team_id "$acceptance_plan")" '.status == "ready" and .application_status == "ready" and .capacity_prepared == true and .worker_available == false and .min_slots == 0 and .owner_user_id == $user and .owner_team_id == $team' "$evidence_dir/owner-b.v1.json" >/dev/null
    assert_live_acceptance "$initial_acceptance_status"

    assert_owner_sessions
    ( XDG_CONFIG_HOME="$owner_a_xdg" loom service up --environment "dev-$owner_a_name" --source-root "$owner_a_source_v2" --min-slots 0 --max-slots 3 ) > "$evidence_dir/owner-a.deploy-v2.txt" 2>&1 &
    owner_a_update_pid=$!
    ( XDG_CONFIG_HOME="$owner_b_xdg" loom service up --environment "dev-$owner_b_name" --source-root "$owner_b_source_v2" --min-slots 0 --max-slots 4 ) > "$evidence_dir/owner-b.deploy-v2.txt" 2>&1 &
    owner_b_update_pid=$!

    owner_a_update_status=0
    owner_b_update_status=0
    wait "$owner_a_update_pid" || owner_a_update_status=$?
    wait "$owner_b_update_pid" || owner_b_update_status=$?
    test "$owner_a_update_status" -eq 0
    test "$owner_b_update_status" -eq 0
    chmod 0600 "$evidence_dir/owner-a.deploy-v2.txt" "$evidence_dir/owner-b.deploy-v2.txt"

    XDG_CONFIG_HOME="$owner_a_xdg" loom dev status "$owner_a_name" --format json > "$evidence_dir/owner-a.v2.json"
    XDG_CONFIG_HOME="$owner_b_xdg" loom dev status "$owner_b_name" --format json > "$evidence_dir/owner-b.v2.json"
    chmod 0600 "$evidence_dir/owner-a.v2.json" "$evidence_dir/owner-b.v2.json"
    jq -e '.status == "ready" and .worker_available == false and .min_slots == 0 and .max_slots == 3' "$evidence_dir/owner-a.v2.json" >/dev/null
    jq -e '.status == "ready" and .worker_available == false and .min_slots == 0 and .max_slots == 4' "$evidence_dir/owner-b.v2.json" >/dev/null
    test "$(jq -r .candidate_sha "$evidence_dir/owner-a.v1.json")" != "$(jq -r .candidate_sha "$evidence_dir/owner-a.v2.json")"
    test "$(jq -r .candidate_sha "$evidence_dir/owner-b.v1.json")" != "$(jq -r .candidate_sha "$evidence_dir/owner-b.v2.json")"
    test "$(jq -r .candidate_sha "$evidence_dir/owner-a.v2.json")" != "$(jq -r .candidate_sha "$evidence_dir/owner-b.v2.json")"
    test "$(jq -r .deployment_generation "$evidence_dir/owner-a.v2.json")" -gt "$(jq -r .deployment_generation "$evidence_dir/owner-a.v1.json")"
    test "$(jq -r .deployment_generation "$evidence_dir/owner-b.v2.json")" -gt "$(jq -r .deployment_generation "$evidence_dir/owner-b.v1.json")"
    assert_live_acceptance "$updated_acceptance_status"

## 7. Prove cross-owner rejection

An unexpected success is an authorization incident and an immediate stop.
The mutation probes use the normal API but must fail at the owner visibility
boundary before any lifecycle operation is created.

    expect_owner_rejection() {
      local label="$1"
      shift
      local output="$evidence_dir/$label.rejection.txt"
      local rejection_rc=0
      "$@" > "$output" 2>&1 || rejection_rc=$?
      chmod 0600 "$output"
      test "$rejection_rc" -eq 1
      grep -Eq 'HTTP 404' "$output"
    }

    assert_owner_sessions
    XDG_CONFIG_HOME="$owner_a_xdg" expect_owner_rejection owner-a-read-b loom dev status "$owner_b_name" --format json
    XDG_CONFIG_HOME="$owner_b_xdg" expect_owner_rejection owner-b-read-a loom dev status "$owner_a_name" --format json
    XDG_CONFIG_HOME="$owner_a_xdg" expect_owner_rejection owner-a-mutate-b loom dev destroy "$owner_b_name" --no-wait --format json
    XDG_CONFIG_HOME="$owner_b_xdg" expect_owner_rejection owner-b-mutate-a loom dev destroy "$owner_a_name" --no-wait --format json

    XDG_CONFIG_HOME="$owner_a_xdg" loom dev status "$owner_a_name" --format json > "$evidence_dir/owner-a.after-rejection.json"
    XDG_CONFIG_HOME="$owner_b_xdg" loom dev status "$owner_b_name" --format json > "$evidence_dir/owner-b.after-rejection.json"
    jq -e '.status == "ready" and .worker_available == false' "$evidence_dir/owner-a.after-rejection.json" >/dev/null
    jq -e '.status == "ready" and .worker_available == false' "$evidence_dir/owner-b.after-rejection.json" >/dev/null
    assert_live_acceptance "$post_rejection_acceptance_status"

## 8. Destroy, retain, and prove subject rotation

Capture owner B's subject before teardown. Owner A uses default data removal;
owner B uses --keep-data. Both commands wait for manager-first authority
retirement and durable cleanup checkpoints.

    assert_owner_sessions
    retained_subject="$(jq -r .subject_id "$evidence_dir/owner-b.after-rejection.json")"
    XDG_CONFIG_HOME="$owner_a_xdg" loom dev destroy "$owner_a_name" --format json > "$evidence_dir/owner-a.destroy.json"
    XDG_CONFIG_HOME="$owner_b_xdg" loom dev destroy "$owner_b_name" --keep-data --format json > "$evidence_dir/owner-b.destroy-keep-data.json"
    chmod 0600 "$evidence_dir/owner-a.destroy.json" "$evidence_dir/owner-b.destroy-keep-data.json"
    jq -e '.status == "deleted" and .keep_data == false' "$evidence_dir/owner-a.destroy.json" >/dev/null
    jq -e '.status == "deleted" and .keep_data == true' "$evidence_dir/owner-b.destroy-keep-data.json" >/dev/null
    assert_live_acceptance "$post_destroy_acceptance_status"

Redeploying the retained name must create a new authority, not revive the
retired subject or reporter credentials.

    assert_owner_sessions
    XDG_CONFIG_HOME="$owner_b_xdg" loom service up --environment "dev-$owner_b_name" --source-root "$owner_b_source_v2" --min-slots 0 --max-slots 4 > "$evidence_dir/owner-b.redeploy.txt" 2>&1
    XDG_CONFIG_HOME="$owner_b_xdg" loom dev status "$owner_b_name" --format json > "$evidence_dir/owner-b.redeployed.json"
    chmod 0600 "$evidence_dir/owner-b.redeploy.txt" "$evidence_dir/owner-b.redeployed.json"
    jq -e --arg previous "$retained_subject" '.status == "ready" and .subject_id != $previous and .worker_available == false and .min_slots == 0' "$evidence_dir/owner-b.redeployed.json" >/dev/null
    assert_live_acceptance "$final_acceptance_status"

Create one canonical, owner-only result before final retirement. It contains
only non-secret state and hashes; keep the detailed owner logs mode 0600.

    jq -cS -n --arg release "$trusted_release_sha256" --arg plan "$acceptance_plan_sha256" --arg acceptance "$acceptance_render_sha256" --arg shadow "$shadow_render_sha256" --slurpfile owner_a "$evidence_dir/owner-a.v2.json" --slurpfile owner_b "$evidence_dir/owner-b.v2.json" --slurpfile owner_a_destroy "$evidence_dir/owner-a.destroy.json" --slurpfile owner_b_destroy "$evidence_dir/owner-b.destroy-keep-data.json" --slurpfile owner_b_redeploy "$evidence_dir/owner-b.redeployed.json" '{acceptance_manifest_sha256:$acceptance,acceptance_plan_sha256:$plan,owner_a:$owner_a[0],owner_a_destroy:$owner_a_destroy[0],owner_b:$owner_b[0],owner_b_destroy:$owner_b_destroy[0],owner_b_redeploy:$owner_b_redeploy[0],release_sha256:$release,schema:"loom-personal-dev-zero-capacity-acceptance-result-v1",shadow_manifest_sha256:$shadow}' > "$evidence_dir/acceptance-result.json"
    chmod 0600 "$evidence_dir/acceptance-result.json"
    assert_canonical_json_line "$evidence_dir/acceptance-result.json"
    sha256sum "$evidence_dir/acceptance-result.json" > "$evidence_dir/acceptance-result.sha256"
    chmod 0600 "$evidence_dir/acceptance-result.sha256"

Retire the redeployed owner B environment through the same normal path. Do not
proceed to shared-plane rollback until both personal namespaces and all build
sandboxes are gone.

    assert_owner_sessions
    XDG_CONFIG_HOME="$owner_b_xdg" loom dev destroy "$owner_b_name" --format json > "$evidence_dir/owner-b.final-destroy.json"
    chmod 0600 "$evidence_dir/owner-b.final-destroy.json"
    jq -e '.status == "deleted"' "$evidence_dir/owner-b.final-destroy.json" >/dev/null
    assert_no_dynamic_namespaces

## 9. Reapply the byte-reviewed inert shadow

Re-render the rollback shadow from the same profile and trusted release, then
require byte equality with the artifact reviewed before forward apply. Do not
synthesize rollback YAML from live state. Server-side apply does not prune
objects, so any resource-identity difference is a stop condition requiring a
separate reviewed plan.

    shadow_render_recheck="$(mktemp "$evidence_dir/shadow-recheck.XXXXXX.yaml")"
    shadow_evidence_recheck="$(mktemp "$evidence_dir/shadow-recheck.XXXXXX.json")"
    if ! /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane render --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" > "$shadow_render_recheck" 2> "$shadow_evidence_recheck"; then
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
    test "$(kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get deployment/loom-personal-dev-activation-agent -o jsonpath='{.spec.replicas}')" = 0
    /home/hongjian/loom/.venv/bin/loom admin personal-dev-control-plane status --namespace loom-dev --kubeconfig "$kubeconfig" --file "$profile" --trusted-release-file "$trusted_release" --trusted-release-sha256 "$trusted_release_sha256" > "$rollback_status"
    chmod 0600 "$rollback_status"
    assert_canonical_json_line "$rollback_status"
    jq -e '.schema == "loom-personal-dev-control-plane-status-v1" and .mode == "shadow" and .ready == true and .blockers == [] and .manager_ceiling == 0 and all(.components[]; .ready == true)' "$rollback_status" >/dev/null

Retain the trusted-release artifact, acceptance plan, backup/restore record,
both reviewed manifests, diffs, applies, status records, owner operation
records, canonical result, and their hashes. The final state is the inert
shared management shadow: lifecycle and builder disabled, activation replicas
zero, no dynamic namespace, no personal worker, and global executable capacity
still exactly zero.
