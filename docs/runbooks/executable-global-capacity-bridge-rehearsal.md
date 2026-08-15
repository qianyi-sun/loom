# Executable global capacity bridge rehearsal

This runbook prepares and observes one global execution epoch at an effective
new-capacity ceiling and rate of exactly zero. It covers the Package 5C1
prepared-only manager and the distinct GB10 and OLDLAB controller-local
inventory services. It contains no activation command.

The repository is render-only. A merge does not authorize Kubernetes apply,
database migration, legacy-writer freeze, systemd installation/start/enable,
Slurm mutation, activation, or a ceiling change. The shadow deployment,
preparation, and abort below are live changes and may run only in the explicit
operator window tracked by issue #906.

Personal applications use `loom-dev-<owner>` namespaces. `loom-dev` contains
shared infrastructure only. Never create or accept `loom-dev-shared`.

## Current live boundary and stop conditions

The read-only audit recorded on 2026-08-15 remains the starting state until a
new #906 audit supersedes it:

- there is no `loom-dev` or `loom-dev-<owner>` namespace and no deployed global
  manager, personal-development controller/builder, or capacity executor;
- the environment-local OLDLAB and GB10 autoscalers remain the authoritative
  allocation/submission writers; and
- both Slurm pools contain unrelated workloads that must remain untouched.

Before the window, only render and inspect artifacts. Do not freeze a writer,
apply the manager manifest, install a unit, prepare or abort an epoch, or query
Slurm through the prepared service.

During the window, stop immediately and retain the current zero-capacity state
for any unapproved image or source commit, incomplete subject acknowledgement,
missing or changed pool binding, missing freeze/restore evidence, stale lease
or inventory, journal mismatch, foreign/unknown/unsigned/quarantined physical
record, failed database restore, or possible writer overlap. `ready=true` means
only that the zero-ceiling prepared rehearsal is complete; it is not worker,
application, or activation readiness.

## 1. Prepare owner-only render evidence

Use an exact CI-approved commit and immutable manager/executor images. The
reviewed execution policy and preparation request are #906 inputs; this package
does not synthesize freeze or rollback evidence. The policy must contain the
complete current subject acknowledgement set, exact OLDLAB and GB10 executor
bindings, controller-authority digests, rollback digest, and every legacy
allocation, submission, claim, pressure, cancellation, and release writer at
its frozen or retired high-water mark.

The policy is canonical JSON with no trailing newline. It is not a credential,
but it controls an authority transition and is therefore a current-user-owned,
single-link, non-symlink regular file with mode `0600`. The exact raw-file
SHA-256 is independently reviewed.

```bash
umask 077
evidence_dir=artifacts/capacity/zero-ceiling
execution_policy="$evidence_dir/execution-policy.json"
preparation_request="$evidence_dir/execution-preparation.json"
manager_render="$evidence_dir/control-plane.yaml"
install -d -m 0700 "$evidence_dir"

test -f "$execution_policy" && test ! -L "$execution_policy"
test "$(stat -c %u "$execution_policy")" = "$(id -u)"
test "$(stat -c %a "$execution_policy")" = 600
execution_policy_sha256="$(sha256sum "$execution_policy" | awk '{print $1}')"
test "${#execution_policy_sha256}" = 64

test -f "$preparation_request" && test ! -L "$preparation_request"
test "$(stat -c %u "$preparation_request")" = "$(id -u)"
test "$(stat -c %a "$preparation_request")" = 600
sha256sum "$execution_policy" "$preparation_request" \
  > "$evidence_dir/operator-inputs.sha256"
chmod 0600 "$evidence_dir/operator-inputs.sha256"
```

Render one immutable policy ConfigMap and the shadow manager. Substitute only
the approved digest reference and non-nil authority UUID:

```bash
manager_image='ghcr.io/qianyi-sun/loom-capacity-manager@sha256:<64-lowercase-hex>'
authority_incarnation='<reviewed-non-nil-uuid>'

uv run --no-sync loom admin capacity-control-plane render \
  --file deploy/dev-fleet/capacity-control-plane.toml \
  --manager-image "$manager_image" \
  --authority-incarnation "$authority_incarnation" \
  --execution-policy-file "$execution_policy" \
  --execution-policy-sha256 "$execution_policy_sha256" \
  > "$manager_render"
chmod 0600 "$manager_render"
sha256sum "$manager_render" > "$manager_render.sha256"
chmod 0600 "$manager_render.sha256"

grep -F "name: loom-capacity-execution-policy-${execution_policy_sha256:0:32}" \
  "$manager_render"
grep -F "value: $execution_policy_sha256" "$manager_render"
! grep -E 'executable_new_capacity_ceiling:[[:space:]]*[1-9]' "$manager_render"
```

Supplying only one policy argument, a digest that differs from the canonical
policy payload, an unsafe file, or a changed file fails before YAML is written.
The rendered ConfigMap contains only canonical policy JSON. The pod init copies
that projection into a memory-backed, manager-UID-owned mode-`0600` file; the
manager mounts only the copied directory read-only.

## 2. Shadow-deploy only inside the #906 window

First repeat and archive the live read-only audit. Confirm that local writers
are still authoritative and the global manager is absent. Open the approved
window before the first command that changes state.

The renderer includes the `loom-dev` Namespace, but not the referenced Secret.
Create the namespace, provision the exact reviewed Secret through the approved
secret channel, then apply the byte-reviewed render. Do not put Secret values
in shell arguments, logs, the render, or this evidence directory.

```bash
kubeconfig=/absolute/path/to/reviewed-kubeconfig

kubectl --kubeconfig "$kubeconfig" create namespace loom-dev \
  --dry-run=client -o yaml \
  | kubectl --kubeconfig "$kubeconfig" apply --server-side \
      --field-manager=loom-capacity-control-plane -f -

# Provision the pre-reviewed loom-capacity-manager Secret now, using the
# approved secret channel. Stop unless all README-listed keys are present.

kubectl --kubeconfig "$kubeconfig" diff --server-side \
  --field-manager=loom-capacity-control-plane -f "$manager_render"
kubectl --kubeconfig "$kubeconfig" apply --server-side \
  --field-manager=loom-capacity-control-plane -f "$manager_render"
kubectl --kubeconfig "$kubeconfig" -n loom-dev wait \
  --for=condition=complete --timeout=900s \
  job -l app.kubernetes.io/managed-by=loom-capacity-control-plane
kubectl --kubeconfig "$kubeconfig" -n loom-dev rollout status \
  deployment/loom-capacity-manager --timeout=300s

uv run --no-sync loom admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig"
```

The last command must emit exactly:

```json
{"executable_new_capacity_ceiling":0,"status":"ready"}
```

Verify the runtime policy without printing its content:

```bash
kubectl --kubeconfig "$kubeconfig" -n loom-dev exec \
  deployment/loom-capacity-manager -c manager -- \
  python -c 'import os,stat; p="/etc/loom-capacity-manager/execution-policy/execution-policy.json"; s=os.stat(p); assert stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.geteuid() and s.st_nlink==1'
```

This is still shadow state. The health result does not authorize preparation
or prove that any worker capacity exists.

## 3. Freeze legacy writers and prepare the exact epoch

Preparation changes the management database even though its effective ceiling
and rate remain zero. It is forbidden outside the #906 window. Freeze every
legacy writer named by the policy, capture its exact high-water and evidence
digest, prove its timer/process cannot issue new work, complete the database
restore/rollback check, and byte-compare those facts with both the policy and
preparation request before proceeding.

Use a mode-`0600` curl configuration owned by the operator. It contains the
approved mTLS CA/certificate/key paths and the bearer token for exactly one
scope; keeping it out of the command line prevents credential disclosure in
process listings. The prepare identity has unbound
`capacity:execution:prepare`; the read identity has `capacity:read`; the abort
identity is a different unbound `capacity:execution:abort` principal.

Run the HTTP commands from the separately approved client path that can reach
the cluster-internal manager Service. Replace `manager_origin` only with that
reviewed endpoint.

```bash
manager_origin='https://loom-capacity-manager.loom-dev.svc.cluster.local:8443'
prepare_curl_config=/absolute/owner-only/path/prepare.curl
read_curl_config=/absolute/owner-only/path/read.curl
abort_curl_config=/absolute/owner-only/path/abort.curl
prepared_response="$evidence_dir/prepared-response.json"
prepare_idempotency_key="$(uuidgen)"

for config in "$prepare_curl_config" "$read_curl_config" "$abort_curl_config"; do
  test -f "$config" && test ! -L "$config"
  test "$(stat -c %u "$config")" = "$(id -u)"
  test "$(stat -c %a "$config")" = 600
done

curl --silent --show-error --fail-with-body \
  --config "$prepare_curl_config" \
  --header 'Content-Type: application/json' \
  --header "Idempotency-Key: $prepare_idempotency_key" \
  --data-binary "@$preparation_request" \
  "$manager_origin/v2/execution-preparations" \
  > "$prepared_response"
chmod 0600 "$prepared_response"

jq -e '
  .schema_version == 2 and
  .execution_state == "prepared" and
  .executable_new_capacity_ceiling == 0 and
  .executable_new_capacity_rate_per_minute == 0
' "$prepared_response"
sha256sum "$prepared_response" > "$prepared_response.sha256"
chmod 0600 "$prepared_response.sha256"
```

Retry only with the same request bytes and idempotency UUID. Never reuse that
UUID for changed bytes. The returned authority, writer, configuration and
execution epochs plus manifest digest are the only values permitted in the
live controller profile.

## 4. Render and stage each prepared-only controller

Create an owner-only live copy of
`deploy/dev-fleet/capacity-pool-executor.toml.example`. Replace every
shape-valid example with reviewed controller-local facts and the exact
prepared response. Keep `namespace = "loom-dev"` and
`executable_new_capacity_ceiling = 0`. GB10 and OLDLAB must retain distinct
controller, executor, incarnation, credential, key, state, journal, policy,
partition, and node identities.

Render all three files for each pool. Rendering invokes no controller and does
not install or start the service.

```bash
executor_profile="$evidence_dir/capacity-pool-executor.live.toml"
test -f "$executor_profile" && test ! -L "$executor_profile"
test "$(stat -c %u "$executor_profile")" = "$(id -u)"
test "$(stat -c %a "$executor_profile")" = 600

for pool in gb10 oldlab; do
  uv run --no-sync loom admin capacity-control-plane render-executor \
    --file "$executor_profile" --pool "$pool" --output config \
    > "$evidence_dir/$pool-executor.json"
  uv run --no-sync loom admin capacity-control-plane render-executor \
    --file "$executor_profile" --pool "$pool" --output inventory-policy \
    > "$evidence_dir/$pool-inventory-policy.json"
  uv run --no-sync loom admin capacity-control-plane render-executor \
    --file "$executor_profile" --pool "$pool" --output service-environment \
    > "$evidence_dir/$pool-service.env"
  chmod 0600 "$evidence_dir/$pool-executor.json" \
    "$evidence_dir/$pool-inventory-policy.json" \
    "$evidence_dir/$pool-service.env"
  grep -Fx 'LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING=0' \
    "$evidence_dir/$pool-service.env"
  sha256sum "$evidence_dir/$pool-executor.json" \
    "$evidence_dir/$pool-inventory-policy.json" \
    "$evidence_dir/$pool-service.env" \
    > "$evidence_dir/$pool-rendered.sha256"
  chmod 0600 "$evidence_dir/$pool-rendered.sha256"
done
```

On each controller, the #906 installer places only that controller's files at
the paths named by its rendered environment. The executor configuration,
inventory policy, environment file, bearer token, TLS key/certificate, and
ownership key are regular non-symlink files owned by
`loom_capacity_executor:loom_capacity_executor` with mode `0600`. State and
journal directories are owned by that account and mode `0700`. The systemd
unit files are root-owned mode `0644`.

After byte comparison, install the checked-in prepared service and timer and
the rendered controller-local inputs. This is a live #906 action. On each
controller, verify syntax and then enable only the timer:

```bash
sudo systemd-analyze verify \
  /etc/systemd/system/loom-capacity-pool-executor-prepared.service \
  /etc/systemd/system/loom-capacity-pool-executor-prepared.timer
sudo systemctl daemon-reload
sudo systemctl enable --now loom-capacity-pool-executor-prepared.timer
sudo systemctl start loom-capacity-pool-executor-prepared.service
sudo systemctl show loom-capacity-pool-executor-prepared.service \
  -p Result -p ExecMainStatus -p ActiveState -p SubState
sudo journalctl -u loom-capacity-pool-executor-prepared.service \
  --since '15 minutes ago' --no-pager
```

One successful oneshot validates the exact local config, digest, EUID,
controller, partition, fixed `scontrol show nodes --json` and `squeue --json`
binaries, then registers, heartbeats, captures, journals, publishes, confirms,
heartbeats, and exits. Registration is deterministic and automatic; there is
no operator-created registration body. The timer repeats no sooner than 30
seconds after the previous oneshot becomes inactive. Prepared-only mode cannot
construct the Slurm submission/cancellation backend and fails if the manager is
shadow, active, or drain-only.

## 5. Prove prepared readiness

Capture raw and sorted JSON without exposing the read token:

```bash
readiness_response="$evidence_dir/prepared-readiness.json"
curl --silent --show-error --fail-with-body \
  --config "$read_curl_config" \
  "$manager_origin/v2/status/execution-preparation" \
  > "$readiness_response"
chmod 0600 "$readiness_response"
jq -cS . "$readiness_response" \
  > "$evidence_dir/prepared-readiness.canonical.json"
chmod 0600 "$evidence_dir/prepared-readiness.canonical.json"

jq -e '
  .ready == true and
  .policy_mode == "pinned" and
  .policy_sha256 == $digest and
  .executable == false and
  .blockers == [] and
  .execution.execution_state == "prepared" and
  .execution.executable_new_capacity_ceiling == 0 and
  .execution.executable_new_capacity_rate_per_minute == 0 and
  .expected_subject_count == .acknowledged_subject_count and
  ([.executors[].pool_id] == ["gb10", "oldlab"]) and
  all(.executors[];
    .registered and .current and .lease_fresh and .inventory_fresh and
    .post_inventory_heartbeat and .blockers == [] and
    .foreign_record_count == 0 and .unknown_record_count == 0 and
    .ownership_missing_record_count == 0 and .quarantined_record_count == 0)
' --arg digest "$execution_policy_sha256" "$readiness_response"

sha256sum "$readiness_response" \
  "$evidence_dir/prepared-readiness.canonical.json" \
  > "$evidence_dir/prepared-readiness.sha256"
chmod 0600 "$evidence_dir/prepared-readiness.sha256"
```

The canonical response has this exact top-level shape; values and timestamps
come from the locked manager state and database clock:

```json
{"acknowledged_subject_count":"<N>","blockers":[],"executable":false,"execution":{"authority_incarnation":"<uuid>","configuration_epoch":"<positive integer>","executable_new_capacity_ceiling":0,"executable_new_capacity_rate_per_minute":0,"execution_epoch":"<positive integer>","execution_manifest_sha256":"<sha256>","execution_state":"prepared","schema_version":2,"trusted_fleet_release_sha256":"<sha256>","writer_epoch":"<positive integer>"},"executors":[{"pool_id":"gb10","...":"bounded readiness fields"},{"pool_id":"oldlab","...":"bounded readiness fields"}],"expected_subject_count":"<N>","policy_mode":"pinned","policy_sha256":"<sha256>","ready":true,"schema_version":2}
```

JSON integers are numbers on the wire; the quoted metavariables above are
documentation placeholders, not literal response values. Any blocker or pool
order other than `gb10`, `oldlab` fails the rehearsal. Do not infer activation
permission from an empty blocker list.

## 6. Stop refresh, abort safely, and retain evidence

Abort is a second management-database mutation and requires the same #906
window. Stop both prepared timers first so no refresh races the transition.
Preserve journals and installed files; do not replace a journal with an empty
one.

Run on both controllers:

```bash
sudo systemctl disable --now loom-capacity-pool-executor-prepared.timer
sudo systemctl stop loom-capacity-pool-executor-prepared.service
sudo systemctl show loom-capacity-pool-executor-prepared.timer \
  -p ActiveState -p SubState -p UnitFileState
sudo systemctl show loom-capacity-pool-executor-prepared.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
```

Construct one exact mode-`0600` abort document from the prepared response. It
contains only the authority UUID, writer epoch, execution epoch, manifest
digest, `schema_version: 2`, and `executable: true`. Byte-review it before the
request.

```bash
abort_request="$evidence_dir/execution-abort.json"
abort_response="$evidence_dir/execution-abort-response.json"
abort_idempotency_key="$(uuidgen)"

jq -c '{
  schema_version: 2,
  authority_incarnation,
  expected_writer_epoch: .writer_epoch,
  execution_epoch,
  execution_manifest_sha256,
  executable: true
}' "$prepared_response" > "$abort_request"
chmod 0600 "$abort_request"

execution_epoch="$(jq -r .execution_epoch "$prepared_response")"
curl --silent --show-error --fail-with-body \
  --config "$abort_curl_config" \
  --header 'Content-Type: application/json' \
  --header "Idempotency-Key: $abort_idempotency_key" \
  --data-binary "@$abort_request" \
  "$manager_origin/v2/execution-preparations/$execution_epoch/abort" \
  > "$abort_response"
chmod 0600 "$abort_response"

jq -e --argjson epoch "$execution_epoch" '
  .execution_epoch == $epoch and .replayed == false
' "$abort_response"
sha256sum "$abort_request" "$abort_response" \
  > "$evidence_dir/execution-abort.sha256"
chmod 0600 "$evidence_dir/execution-abort.sha256"
```

A safe abort is possible only for the exact current prepared epoch, while no
executable intent exists. It append-only retires the preparation and restores
`shadow`, ceiling/rate zero, with the increase freeze retained. Retry only with
the same abort bytes and idempotency UUID; a replay returns `replayed: true`.

Finally capture status again. It must be not ready with `manager-shadow`; the
general manager health probe must still report ceiling zero:

```bash
curl --silent --show-error --fail-with-body \
  --config "$read_curl_config" \
  "$manager_origin/v2/status/execution-preparation" \
  > "$evidence_dir/post-abort-readiness.json"
chmod 0600 "$evidence_dir/post-abort-readiness.json"
jq -e '
  .ready == false and .executable == false and
  (.blockers | index("manager-shadow") != null)
' "$evidence_dir/post-abort-readiness.json"

uv run --no-sync loom admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig"
sha256sum "$evidence_dir"/*.json "$evidence_dir"/*.yaml \
  > "$evidence_dir/final-evidence.sha256"
chmod 0600 "$evidence_dir/final-evidence.sha256"
```

The #906 operator decision, not this runbook, determines whether frozen legacy
writers remain stopped or are restored after abort. There is deliberately no
`prepared -> active` endpoint, CLI operation, systemd mode, or command in this
package.
