# Personal-development native builder two-owner acceptance

This runbook activates the release-bound native builder in the shared
`loom-dev` management plane and proves two simultaneous personal deployments:
native OLDLAB `linux/amd64` Kubernetes Jobs and native GB10 `linux/arm64`
grants. Each owner may deploy a different arbitrary local source tree,
including committed, modified, and untracked files accepted by the source
snapshot policy.

This is application-candidate acceptance, not task execution. It contains no
task submission and no Slurm mutation. No personal worker becomes available,
and the executable-new-capacity ceiling remains exactly `0`. There is no QEMU
or runc fallback.

Every accepted personal control-plane status contains the exact canonical
fragments `"manager_ceiling":0` and `"worker_available":false`.

Complete the runtime runbook through agent activation first. Management apply
must occur only after the agent service is observed active. Signed durable
zero-grant readiness must occur after that apply and before either owner
request. Cleanup uses only each owner's authenticated Loom API. Never delete a
personal or build namespace directly.

Secret values are never printed, stored in evidence, passed as command
arguments, or copied into the repository. The two owner sessions remain in
separate existing mode-`0700` XDG roots outside the evidence directory.

## 1. Bind the protected release, native plan, and evidence

The profile is a separately reviewed owner-only schema-3 copy of the checked-in
shadow profile with only `[native_builder]` prepared identity, public key,
current public-store origin/CIDRs, and exact release bindings populated. The
operational plan is canonical schema 2 and binds the same values. Both are
immutable inputs to this window.

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
operational_plan='<absolute-owner-only-native-operational-plan.json>'
operational_plan_sha256='<native-operational-plan-64-lowercase-hex>'
baseline_operational_manifest='<absolute-byte-reviewed-native-operational-manifest>'
baseline_operational_manifest_sha256='<native-operational-manifest-64-lowercase-hex>'
previous_operational_manifest='<absolute-byte-reviewed-previous-operational-manifest>'
previous_operational_manifest_sha256='<previous-operational-manifest-64-lowercase-hex>'
rollback_shadow_manifest='<absolute-byte-reviewed-schema-4-shadow-manifest>'
rollback_shadow_sha256='<rollback-shadow-64-lowercase-hex>'
runtime_evidence='<absolute-active-native-runtime-evidence-directory>'
runtime_profile_sha256='c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d'
archive_sha512='dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7'

reviewed_kubeconfig='<absolute-owner-only-mode-0600-kubeconfig>'
evidence_root='<absolute-existing-owner-only-evidence-root-outside-repository>'
gb10_target='<ssh-user>@gx10-01c7'
slurm_observer='<read-only-slurm-observer-ssh-target>'
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
loom_cli="$repository_root/.venv/bin/loom"

trusted_launcher_profile='<absolute-owner-only-trusted-launcher-profile.json>'
scanner_finding_policy='<absolute-owner-only-scanner-finding-policy.json>'
backup_restore_evidence='<absolute-owner-only-backup-restore-evidence.json>'
operational_evidence_args=(
  --source-root "$repository_root"
  --trusted-launcher-profile-file "$trusted_launcher_profile"
  --scanner-finding-policy-file "$scanner_finding_policy"
  --backup-restore-evidence-file "$backup_restore_evidence"
)

owner_0_xdg='<absolute-mode-0700-owner-0-xdg-config-root>'
owner_1_xdg='<absolute-mode-0700-owner-1-xdg-config-root>'
owner_0_source='<absolute-owner-0-source-root>'
owner_1_source='<absolute-owner-1-source-root>'
owner_0_name='<owner-0-personal-name>'
owner_1_name='<owner-1-personal-name>'

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir="$evidence_root/${timestamp}-native-two-owner-$merged_source_sha"
test "$(git rev-parse --show-toplevel)" = "$repository_root"
test "$(git rev-parse HEAD)" = "$merged_source_sha"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test -x "$loom_cli"

for path in "$trusted_release" "$profile" "$operational_plan" \
  "$baseline_operational_manifest" "$previous_operational_manifest" \
  "$rollback_shadow_manifest" "$reviewed_kubeconfig" \
  "$trusted_launcher_profile" "$scanner_finding_policy" \
  "$backup_restore_evidence"; do
  test -f "$path"
  test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 600
  test "$(stat -c %h "$path")" = 1
done

test "$(sha256sum "$trusted_release" | awk '{print $1}')" = \
  "$trusted_release_sha256"
test "$(sha256sum "$profile" | awk '{print $1}')" = "$profile_sha256"
test "$(sha256sum "$operational_plan" | awk '{print $1}')" = \
  "$operational_plan_sha256"
test "$(sha256sum "$baseline_operational_manifest" | awk '{print $1}')" = \
  "$baseline_operational_manifest_sha256"
test "$(sha256sum "$previous_operational_manifest" | awk '{print $1}')" = \
  "$previous_operational_manifest_sha256"
test "$(sha256sum "$rollback_shadow_manifest" | awk '{print $1}')" = \
  "$rollback_shadow_sha256"
test "$(jq -er .schema_version "$trusted_release")" = 4
test "$(jq -er .schema_version "$operational_plan")" = 2
test "$(jq -er .source.commit "$operational_plan")" = "$merged_source_sha"
test "$(jq -er .release.trusted_release_sha256 "$operational_plan")" = \
  "$trusted_release_sha256"
test "$(jq -er .native_builder.runtime_profile_sha256 "$operational_plan")" = \
  "$runtime_profile_sha256"
test "$(jq -er .manager.executable_new_capacity_ceiling "$operational_plan")" = 0

test -d "$runtime_evidence"
test ! -L "$runtime_evidence"
test "$(realpath -e "$runtime_evidence")" = "$runtime_evidence"
test "$(stat -c %u "$runtime_evidence")" = "$(id -u)"
test "$(stat -c %a "$runtime_evidence")" = 700
test -f "$runtime_evidence/immutable-inputs.json"
test ! -L "$runtime_evidence/immutable-inputs.json"
test "$(stat -c %u "$runtime_evidence/immutable-inputs.json")" = "$(id -u)"
test "$(stat -c %a "$runtime_evidence/immutable-inputs.json")" = 600
test "$(stat -c %h "$runtime_evidence/immutable-inputs.json")" = 1
jq -e --arg source "$merged_source_sha" --arg release "$trusted_release_sha256" \
  --arg profile "$runtime_profile_sha256" --arg prepared "$profile_sha256" \
  --arg archive "$archive_sha512" '
    .source_sha == $source and .trusted_release_sha256 == $release and
    .profile_sha256 == $profile and .prepared_profile_sha256 == $prepared and
    .archive_sha512 == $archive
  ' "$runtime_evidence/immutable-inputs.json" >/dev/null

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
kubeconfig="$evidence_dir/kubeconfig"
install -m 0600 "$reviewed_kubeconfig" "$kubeconfig"

for path in "$owner_0_xdg" "$owner_1_xdg"; do
  test -d "$path"
  test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(stat -c %u "$path")" = "$(id -u)"
  test "$(stat -c %a "$path")" = 700
done
test "$(realpath -e "$owner_0_xdg")" != "$(realpath -e "$owner_1_xdg")"
for config in "$owner_0_xdg/loom/config.toml" "$owner_1_xdg/loom/config.toml"; do
  test -f "$config"
  test ! -L "$config"
  test "$(realpath -e "$config")" = "$config"
  test "$(stat -c %u "$config")" = "$(id -u)"
  test "$(stat -c %a "$config")" = 600
  test "$(stat -c %h "$config")" = 1
done
test "$(stat -c %d:%i "$owner_0_xdg/loom/config.toml")" != \
  "$(stat -c %d:%i "$owner_1_xdg/loom/config.toml")"
for path in "$owner_0_source" "$owner_1_source"; do
  test -d "$path"
  test ! -L "$path"
  test "$(realpath -e "$path")" = "$path"
  test "$(realpath -e "$path")" != "$repository_root"
done
test "$(realpath -e "$owner_0_source")" != "$(realpath -e "$owner_1_source")"
```

Use sources whose trusted Docker build paths take long enough to observe the
overlap. The source trees must be different and must produce two different
source archives/candidate hashes. Do not add artificial external dependencies
or any task invocation.

## 2. Define read-only snapshots and record the initial boundary

```bash
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

capture_counts() {
  local output="$1"
  local grants=null grant_table tasks workers
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
      select(startswith("loom-dev-") or startswith("loom-build-"))] | sort' \
    > "$1"
  chmod 0600 "$1"
}

capture_slurm() {
  local output="$1" queue="$1.queue"
  ssh "${ssh_options[@]}" "$slurm_observer" -- scontrol show nodes --json \
    | jq -cS . > "$output"
  ssh "${ssh_options[@]}" "$slurm_observer" -- squeue --json \
    | jq -cS . > "$queue"
  jq -cnS --slurpfile nodes "$output" --slurpfile jobs "$queue" \
    '{nodes:$nodes[0],queue:$jobs[0]}' > "$output.merged"
  mv "$output.merged" "$output"
  rm -f "$queue"
  chmod 0600 "$output"
}

assert_no_loom_slurm_jobs() {
  jq -e '[.queue.jobs[]? |
    select(((.name // "") | ascii_downcase | startswith("loom")))] |
    length == 0' "$1" >/dev/null
}

capture_counts "$evidence_dir/before-database-counts.json"
capture_namespaces "$evidence_dir/before-namespaces.json"
capture_slurm "$evidence_dir/before-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/before-slurm.json"
"$loom_cli" admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  > "$evidence_dir/before-capacity.status.json"
chmod 0600 "$evidence_dir/before-capacity.status.json"
jq -e '.active_native_grants == null or .active_native_grants == 0' \
  "$evidence_dir/before-database-counts.json" >/dev/null
jq -e '. == []' "$evidence_dir/before-namespaces.json" >/dev/null
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/before-capacity.status.json" >/dev/null
```

## 3. Prove agent-before-management ordering, render, and apply

Capture the active host unit before any management mutation. An agent that is
retrying against disabled native management is acceptable at this point; a
missing, exited, wrong-image, or wrong-unit agent is not.

```bash
ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc '
  jq -cnS \
    --arg activestate "$(systemctl show loom-personal-dev-native-builder-agent.service --property=ActiveState --value)" \
    --arg fragmentpath "$(systemctl show loom-personal-dev-native-builder-agent.service --property=FragmentPath --value)" \
    --arg substate "$(systemctl show loom-personal-dev-native-builder-agent.service --property=SubState --value)" \
    "[{activestate:\$activestate,fragmentpath:\$fragmentpath,substate:\$substate}]"
' | jq -cS . > "$evidence_dir/agent-active-pre-management.json"
chmod 0600 "$evidence_dir/agent-active-pre-management.json"
jq -e 'length == 1 and .[0].activestate == "active" and
  .[0].substate == "running" and
  .[0].fragmentpath == "/etc/systemd/system/loom-personal-dev-native-builder-agent.service"' \
  "$evidence_dir/agent-active-pre-management.json" >/dev/null

rendered_operational="$evidence_dir/native-operational.rendered.yaml"
rendered_operational_evidence="$evidence_dir/native-operational.render.json"
"$loom_cli" admin personal-dev-control-plane render-operational \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${operational_evidence_args[@]}" \
  > "$rendered_operational" 2> "$rendered_operational_evidence"
chmod 0600 "$rendered_operational" "$rendered_operational_evidence"
cmp -s "$rendered_operational" "$baseline_operational_manifest"

diff_status=0
kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$baseline_operational_manifest" \
  > "$evidence_dir/native-operational.server-side-diff.txt" 2>&1 || diff_status=$?
test "$diff_status" -eq 0 || test "$diff_status" -eq 1
chmod 0600 "$evidence_dir/native-operational.server-side-diff.txt"

capture_counts "$evidence_dir/pre-apply-database-counts.json"
jq -e '.active_native_grants == null or .active_native_grants == 0' \
  "$evidence_dir/pre-apply-database-counts.json" >/dev/null
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$baseline_operational_manifest" \
  > "$evidence_dir/native-operational.server-side-apply.txt"
chmod 0600 "$evidence_dir/native-operational.server-side-apply.txt"

kubectl --kubeconfig "$kubeconfig" --namespace loom-dev rollout status \
  deployment/loom-personal-dev-management --timeout=300s
```

Now require the agent's fresh signed durable row, exact identity/inventory,
authenticated public-store probe, zero active grants, OLDLAB runtime,
unavailable personal workers, and ceiling zero. This is the
`signed-zero-grant-readiness` gate.

```bash
signed_readiness="$evidence_dir/signed-zero-grant-readiness.status.json"
readiness_deadline=$((SECONDS + 180))
while true; do
  readiness_rc=0
  "$loom_cli" admin personal-dev-control-plane status-operational \
    --namespace loom-dev --kubeconfig "$kubeconfig" \
    --file "$profile" \
    --trusted-release-file "$trusted_release" \
    --trusted-release-sha256 "$trusted_release_sha256" \
    --operational-plan-file "$operational_plan" \
    --operational-plan-sha256 "$operational_plan_sha256" \
    "${operational_evidence_args[@]}" \
    > "$signed_readiness" || readiness_rc=$?
  if test "$readiness_rc" -eq 0 && jq -e '
      .ready == true and .blockers == [] and
      .manager_ceiling == 0 and .worker_available == false and
      any(.components[]; .name == "native-builder" and
        .observed == 1 and .ready == true)
    ' "$signed_readiness" >/dev/null; then
    break
  fi
  test "$SECONDS" -lt "$readiness_deadline"
  sleep 2
done
chmod 0600 "$signed_readiness"
```

Pause here and run section 7 of the runtime runbook. That seals the host
activation's after-state while grants and dynamic namespaces are still zero.
Return to this section only after its evidence index is complete; do not start
either owner early.

## 4. Verify two distinct owner credentials

```bash
owner_0_whoami="$evidence_dir/owner-0.whoami.json"
owner_1_whoami="$evidence_dir/owner-1.whoami.json"
XDG_CONFIG_HOME="$owner_0_xdg" "$loom_cli" auth whoami --format json \
  | jq -cS . > "$owner_0_whoami"
XDG_CONFIG_HOME="$owner_1_xdg" "$loom_cli" auth whoami --format json \
  | jq -cS . > "$owner_1_whoami"
chmod 0600 "$owner_0_whoami" "$owner_1_whoami"

for whoami in "$owner_0_whoami" "$owner_1_whoami"; do
  jq -e '.auth_kind == "bearer" and
    .credential_type == "user_owned_api_token" and
    .principal_type == "team" and .role == null' "$whoami" >/dev/null
  test "$(jq -er .server "$whoami")" = \
    "$(jq -er .management_origin "$runtime_evidence/prepared-profile-binding.json")"
done
owner_0_principal="$(jq -cS '{user_id,team_id}' "$owner_0_whoami")"
owner_1_principal="$(jq -cS '{user_id,team_id}' "$owner_1_whoami")"
test "$owner_0_principal" != "$owner_1_principal"
```

## 5. Launch both source-fresh deployments and capture overlap

Start both owner commands before either wait. Both request `min_slots=0`; their
application candidate builds must not create a Task or request a worker.

```bash
owner_0_log="$evidence_dir/owner-0.deploy.txt"
owner_1_log="$evidence_dir/owner-1.deploy.txt"
( XDG_CONFIG_HOME="$owner_0_xdg" "$loom_cli" service up \
    --environment "dev-$owner_0_name" --source-root "$owner_0_source" \
    --min-slots 0 --max-slots 2 ) > "$owner_0_log" 2>&1 &
owner_0_deploy_pid=$!
( XDG_CONFIG_HOME="$owner_1_xdg" "$loom_cli" service up \
    --environment "dev-$owner_1_name" --source-root "$owner_1_source" \
    --min-slots 0 --max-slots 2 ) > "$owner_1_log" 2>&1 &
owner_1_deploy_pid=$!

simultaneous_jobs="$evidence_dir/simultaneous-amd64-jobs.json"
simultaneous_grants="$evidence_dir/simultaneous-arm64-grants.json"
simultaneous_containers="$evidence_dir/simultaneous-arm64-containers.json"
overlap_deadline=$((SECONDS + 600))
while true; do
  jobs_before="$evidence_dir/jobs-before.tmp"
  jobs_after="$evidence_dir/jobs-after.tmp"
  kubectl --kubeconfig "$kubeconfig" get jobs --all-namespaces \
    -l loom.dev/platform=amd64 -o json \
    | jq -cS '[.items[] | select(.status.active == 1) |
        {candidate:.metadata.labels["loom.dev/candidate"],
         name:.metadata.name,namespace:.metadata.namespace,
         runtime_class:.spec.template.spec.runtimeClassName,
         uid:.metadata.uid}] | sort_by(.uid)' > "$jobs_before"
  if jq -e 'length == 2 and all(.[]; .runtime_class == "loom-personal-dev-builder")' \
      "$jobs_before" >/dev/null; then
    read_count "SELECT coalesce(jsonb_agg(jsonb_build_object(
      'candidate',left(c.candidate_sha,12),'grant_id',g.id,
      'platform',g.platform,'provider',g.provider,'state',g.state)
      ORDER BY g.id)::text,'[]')
      FROM personal_dev_native_build_grants g
      JOIN personal_dev_candidates c ON c.id=g.candidate_id
      WHERE g.state='running'" | jq -cS . > "$simultaneous_grants"
    kubectl --kubeconfig "$kubeconfig" get jobs --all-namespaces \
      -l loom.dev/platform=amd64 -o json \
      | jq -cS '[.items[] | select(.status.active == 1) |
          {candidate:.metadata.labels["loom.dev/candidate"],
           name:.metadata.name,namespace:.metadata.namespace,
           runtime_class:.spec.template.spec.runtimeClassName,
           uid:.metadata.uid}] | sort_by(.uid)' > "$jobs_after"
    if jq -e 'length == 2 and all(.[];
        .platform == "linux/arm64" and
        .provider == "gb10-gvisor-docker-v1" and .state == "running")' \
        "$simultaneous_grants" >/dev/null && cmp -s "$jobs_before" "$jobs_after"; then
      mv "$jobs_after" "$simultaneous_jobs"
      rm -f "$jobs_before"
      break
    fi
  fi
  rm -f "$jobs_before" "$jobs_after" "$simultaneous_grants"
  test "$SECONDS" -lt "$overlap_deadline"
  sleep 1
done

ssh "${ssh_options[@]}" "$gb10_target" -- sudo /bin/sh -euc '
  endpoint=unix:///run/loom-personal-dev-builder/docker.sock
  ids="$(docker -H "$endpoint" ps -q --filter label=loom.personal-dev-native-builder.managed=true)"
  test "$(printf "%s\n" "$ids" | sed "/^$/d" | wc -l)" = 4
  docker -H "$endpoint" inspect $ids
' | jq -cS '[.[] | {
    id:.Id,image:.Image,runtime:.HostConfig.Runtime,
    role:.Config.Labels["loom.personal-dev-native-builder.role"],
    grant:.Config.Labels["loom.personal-dev-native-builder.grant-id"],
    platform:.Config.Labels["loom.personal-dev-native-builder.platform"]}] |
  sort_by(.grant,.role)' > "$simultaneous_containers"
jq -e 'length == 4 and
  ([.[] | select(.role == "buildkit")] | length == 2) and
  ([.[] | select(.role == "client")] | length == 2) and
  all(.[]; .runtime == "runsc-personal-dev-native" and
    .platform == "linux/arm64") and
  ([.[].grant] | unique | length == 2)' "$simultaneous_containers" >/dev/null
chmod 0600 "$simultaneous_jobs" "$simultaneous_grants" \
  "$simultaneous_containers"

owner_0_rc=0
owner_1_rc=0
wait "$owner_0_deploy_pid" || owner_0_rc=$?
wait "$owner_1_deploy_pid" || owner_1_rc=$?
test "$owner_0_rc" -eq 0
test "$owner_1_rc" -eq 0
chmod 0600 "$owner_0_log" "$owner_1_log"
```

The overlap evidence must show two simultaneous amd64 Jobs and two
simultaneous arm64 grants, plus four distinct GB10 containers split into two
BuildKit/client pairs.

## 6. Prove native completion and immutable multi-platform publication

```bash
owner_0_status="$evidence_dir/owner-0.status.json"
owner_1_status="$evidence_dir/owner-1.status.json"
XDG_CONFIG_HOME="$owner_0_xdg" "$loom_cli" dev status "$owner_0_name" \
  --format json | jq -cS . > "$owner_0_status"
XDG_CONFIG_HOME="$owner_1_xdg" "$loom_cli" dev status "$owner_1_name" \
  --format json | jq -cS . > "$owner_1_status"
chmod 0600 "$owner_0_status" "$owner_1_status"
for status in "$owner_0_status" "$owner_1_status"; do
  jq -e '.status == "ready" and .application_status == "ready" and
    .min_slots == 0 and .worker_available == false' "$status" >/dev/null
done
owner_0_candidate="$(jq -er .candidate_sha "$owner_0_status")"
owner_1_candidate="$(jq -er .candidate_sha "$owner_1_status")"
[[ "$owner_0_candidate" =~ ^[0-9a-f]{64}$ ]]
[[ "$owner_1_candidate" =~ ^[0-9a-f]{64}$ ]]
test "$owner_0_candidate" != "$owner_1_candidate"

publication_query="SELECT jsonb_build_object(
  'archive_sha256',archive_sha256,'candidate_sha',candidate_sha,
  'image_manifest_digest',image_manifest_digest,
  'publication',publication_json)::text
  FROM personal_dev_candidates
  WHERE candidate_sha IN ('$owner_0_candidate','$owner_1_candidate')
  ORDER BY candidate_sha"
read_count "$publication_query" | jq -cS . \
  > "$evidence_dir/candidate-publications.jsonl"
test "$(wc -l < "$evidence_dir/candidate-publications.jsonl")" = 2
test "$(jq -r .archive_sha256 "$evidence_dir/candidate-publications.jsonl" | sort -u | wc -l)" = 2

runtime_query="SELECT jsonb_build_object(
  'candidate_sha',c.candidate_sha,'evidence',g.runtime_evidence_json)::text
  FROM personal_dev_native_build_grants g
  JOIN personal_dev_candidates c ON c.id=g.candidate_id
  WHERE c.candidate_sha IN ('$owner_0_candidate','$owner_1_candidate')
    AND g.state='succeeded'
  ORDER BY c.candidate_sha"
read_count "$runtime_query" | jq -cS . \
  > "$evidence_dir/native-runtime-evidence.jsonl"
test "$(wc -l < "$evidence_dir/native-runtime-evidence.jsonl")" = 2
jq -e '.evidence.platform == "linux/arm64" and
  .evidence.provider == "gb10-gvisor-docker-v1" and
  .evidence.runtime_name == "runsc-personal-dev-native" and
  .evidence.client_container_id != .evidence.buildkit_container_id and
  .evidence.client_exit_code == 0 and
  .evidence.client_oom_killed == false and
  .evidence.buildkit_running == true' \
  "$evidence_dir/native-runtime-evidence.jsonl" >/dev/null

index_list="$evidence_dir/candidate-indexes.txt"
jq -r '.publication.images[] |
  .index as $index | .platforms as $platforms |
  select(($platforms|keys|sort) == ["linux/amd64","linux/arm64"]) | $index' \
  "$evidence_dir/candidate-publications.jsonl" | sort -u > "$index_list"
test "$(wc -l < "$index_list")" = 4
while IFS= read -r index; do
  [[ "$index" =~ @sha256:[0-9a-f]{64}$ ]]
  inspect="$evidence_dir/index-$(printf '%s' "$index" | sha256sum | awk '{print $1}').json"
  docker buildx imagetools inspect --raw "$index" | jq -cS . > "$inspect"
  jq -e '.mediaType == "application/vnd.oci.image.index.v1+json" and
    ([.manifests[].platform | (.os + "/" + .architecture)] | sort) ==
      ["linux/amd64","linux/arm64"]' "$inspect" >/dev/null
  chmod 0600 "$inspect"
done < "$index_list"
chmod 0600 "$evidence_dir/candidate-publications.jsonl" \
  "$evidence_dir/native-runtime-evidence.jsonl" "$index_list"
```

The OLDLAB evidence is the two captured Job specs with RuntimeClass
`loom-personal-dev-builder`, whose checked runtime handler is
`runsc-personal-dev`; the GB10 evidence is the two signed grant completions with
runtime `runsc-personal-dev-native`. Together with the four immutable indexes,
this proves native multi-platform publication.

## 7. Prove owner isolation and route behavior

```bash
for field in identity.namespace identity.database identity.task_bucket \
  identity.trajectories_bucket identity.artifacts_bucket identity.route_host \
  subject_id subject_incarnation; do
  test "$(jq -er ".$field" "$owner_0_status")" != \
    "$(jq -er ".$field" "$owner_1_status")"
done

probe_cross_owner_denial() {
  local actor_xdg="$1" target_name="$2" output="$3"
  local rc=0
  XDG_CONFIG_HOME="$actor_xdg" "$loom_cli" dev status "$target_name" \
    --expected-hidden-denial > "$output.stdout" 2> "$output.stderr" || rc=$?
  test "$rc" -eq 1
  test ! -s "$output.stdout"
  chmod 0600 "$output.stdout" "$output.stderr"
}
probe_cross_owner_denial "$owner_0_xdg" "$owner_1_name" \
  "$evidence_dir/owner-0-to-owner-1"
probe_cross_owner_denial "$owner_1_xdg" "$owner_0_name" \
  "$evidence_dir/owner-1-to-owner-0"

for status in "$owner_0_status" "$owner_1_status"; do
  route_host="$(jq -er .identity.route_host "$status")"
  [[ "$route_host" =~ ^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$ ]]
  route_output="$evidence_dir/route-$route_host.json"
  curl --fail --silent --show-error --proto '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 30 "https://$route_host/api/v1/health" \
    | jq -cS . > "$route_output"
  jq -e '.status == "ok"' "$route_output" >/dev/null
  chmod 0600 "$route_output"
done
```

## 8. Authenticated owner cleanup and final zero-residue proof

Only the owning authenticated API sessions retire their resources.

```bash
XDG_CONFIG_HOME="$owner_0_xdg" "$loom_cli" dev destroy "$owner_0_name" \
  --format json | jq -cS . > "$evidence_dir/owner-0.destroyed.json"
XDG_CONFIG_HOME="$owner_1_xdg" "$loom_cli" dev destroy "$owner_1_name" \
  --format json | jq -cS . > "$evidence_dir/owner-1.destroyed.json"
chmod 0600 "$evidence_dir"/owner-*.destroyed.json
jq -e '.status == "deleted"' "$evidence_dir/owner-0.destroyed.json" >/dev/null
jq -e '.status == "deleted"' "$evidence_dir/owner-1.destroyed.json" >/dev/null

cleanup_deadline=$((SECONDS + 300))
while true; do
  capture_counts "$evidence_dir/final-zero-grants.json"
  capture_namespaces "$evidence_dir/final-zero-namespaces.json"
  if jq -e '.active_native_grants == 0' \
      "$evidence_dir/final-zero-grants.json" >/dev/null &&
    jq -e '. == []' "$evidence_dir/final-zero-namespaces.json" >/dev/null; then
    break
  fi
  test "$SECONDS" -lt "$cleanup_deadline"
  sleep 2
done

jq -cS '{tasks}' "$evidence_dir/final-zero-grants.json" \
  > "$evidence_dir/final-zero-tasks.json"
jq -cS '{workers}' "$evidence_dir/final-zero-grants.json" \
  > "$evidence_dir/final-zero-workers.json"
jq -e --slurpfile before "$evidence_dir/before-database-counts.json" '
  .tasks == $before[0].tasks and .workers == $before[0].workers and
  .active_native_grants == 0' "$evidence_dir/final-zero-grants.json" >/dev/null

capture_slurm "$evidence_dir/after-slurm.json"
assert_no_loom_slurm_jobs "$evidence_dir/after-slurm.json"
"$loom_cli" admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  > "$evidence_dir/final-capacity.status.json"
jq -e '. == {executable_new_capacity_ceiling:0,status:"ready"}' \
  "$evidence_dir/final-capacity.status.json" >/dev/null
chmod 0600 "$evidence_dir/final-zero-"*.json \
  "$evidence_dir/final-capacity.status.json"
```

## 9. Restore the exact operational state and seal evidence

Successful acceptance keeps the reviewed native operational management plane,
but with no owner/build namespace and no active grant. Re-render it from the
same exact inputs, require byte identity, require an empty server-side diff,
and run status again. This restores the exact operational state rather than
leaving an acceptance-specific manifest.

```bash
restored_operational_manifest="$evidence_dir/restored-operational.yaml"
restored_operational_evidence="$evidence_dir/restored-operational.render.json"
"$loom_cli" admin personal-dev-control-plane render-operational \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${operational_evidence_args[@]}" \
  > "$restored_operational_manifest" 2> "$restored_operational_evidence"
chmod 0600 "$restored_operational_manifest" "$restored_operational_evidence"
cmp -s "$restored_operational_manifest" "$baseline_operational_manifest"

kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-personal-dev-control-plane \
  -f "$baseline_operational_manifest" \
  > "$evidence_dir/final-operational.diff.txt" 2>&1 || final_diff_rc=$?
test "${final_diff_rc:-0}" -eq 0
test ! -s "$evidence_dir/final-operational.diff.txt"

"$loom_cli" admin personal-dev-control-plane status-operational \
  --namespace loom-dev --kubeconfig "$kubeconfig" \
  --file "$profile" \
  --trusted-release-file "$trusted_release" \
  --trusted-release-sha256 "$trusted_release_sha256" \
  --operational-plan-file "$operational_plan" \
  --operational-plan-sha256 "$operational_plan_sha256" \
  "${operational_evidence_args[@]}" \
  > "$evidence_dir/final-operational.status.json"
jq -e '.ready == true and .blockers == [] and
  .manager_ceiling == 0 and .worker_available == false and
  any(.components[]; .name == "native-builder" and
    .observed == 1 and .ready == true)' \
  "$evidence_dir/final-operational.status.json" >/dev/null

jq -cnS \
  --arg owner_0_candidate "$owner_0_candidate" \
  --arg owner_1_candidate "$owner_1_candidate" \
  --arg operational_plan "$operational_plan_sha256" \
  --arg profile "$profile_sha256" \
  --arg release "$trusted_release_sha256" \
  '{manager_ceiling:0,operational_plan_sha256:$operational_plan,
    owner_candidates:[$owner_0_candidate,$owner_1_candidate],
    profile_sha256:$profile,status:"passed",
    trusted_release_sha256:$release,worker_available:false}' \
  > "$evidence_dir/acceptance-result.json"
chmod 0600 "$evidence_dir/acceptance-result.json"

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

Secret values are never part of the evidence index. Completion requires two
distinct candidate/source-archive hashes, two native amd64 Jobs overlapping two
native arm64 grants, two complete immutable OCI indexes per candidate, isolated
routes, authenticated cleanup, zero active grants, zero personal/build
namespaces, unchanged Task/Worker and Slurm snapshots, workers unavailable,
and the exact operational render at ceiling zero.

## Failure rollback

Before any owner request, a failed management activation may reapply
`$previous_operational_manifest` only after its exact SHA-256 is rechecked. If
schema compatibility is uncertain, use `$rollback_shadow_manifest` instead.
After an owner request, first complete authenticated owner cleanup and prove
zero active grants/namespaces; only then reapply the previous operational or
shadow manifest. Follow the runtime runbook to stop the agent before the
dedicated daemon. Never improvise a namespace, grant, container, image, Slurm,
Task, Worker, or capacity mutation.
