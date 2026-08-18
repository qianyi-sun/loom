# Executable global capacity bridge rehearsal

This runbook prepares and observes one global execution epoch at an effective
new-capacity ceiling and rate of exactly zero, then performs one explicitly
bounded protected activation followed by drain and retirement. It covers the
prepared-only and active GB10 and OLDLAB controller-local services. Activation
is an HTTP authority transition; no CLI command applies infrastructure or
changes the ceiling.

The repository renderers are non-installing. A merge does not authorize
Kubernetes apply, database migration, legacy-writer freeze, systemd
installation/start/enable, Slurm mutation, activation, or a ceiling change.
Every live action below may run only in the explicit operator window tracked
by issue #906.

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
record, failed database restore, or possible writer overlap. `ready=true` plus
the exact `readiness_sha256` is only a short-lived activation precondition. It
is not worker or application readiness, and the manager revalidates it under
the activation transaction locks.

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
  (.readiness_sha256 | test("^[0-9a-f]{64}$")) and
  .readiness_sha256 != ("0" * 64) and
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
{"acknowledged_subject_count":"<N>","blockers":[],"executable":false,"execution":{"authority_incarnation":"<uuid>","configuration_epoch":"<positive integer>","executable_new_capacity_ceiling":0,"executable_new_capacity_rate_per_minute":0,"execution_epoch":"<positive integer>","execution_manifest_sha256":"<sha256>","execution_state":"prepared","schema_version":2,"trusted_fleet_release_sha256":"<sha256>","writer_epoch":"<positive integer>"},"executors":[{"pool_id":"gb10","...":"bounded readiness fields"},{"pool_id":"oldlab","...":"bounded readiness fields"}],"expected_subject_count":"<N>","policy_mode":"pinned","policy_sha256":"<sha256>","readiness_sha256":"<sha256>","ready":true,"schema_version":2}
```

JSON integers are numbers on the wire; the quoted metavariables above are
documentation placeholders, not literal response values. Any blocker or pool
order other than `gb10`, `oldlab` fails the rehearsal. Do not infer activation
permission from an empty blocker list. `readiness_sha256` is the canonical
digest of the complete typed readiness object excluding that top-level digest
field; it is not the raw response-file digest recorded above.

## 6. Stage the active package while disabled

Do all positive-runtime rendering and installation before stopping prepared
refresh. For each pool, prepare one reviewed owner-only mode-`0600`
`ApprovedLaunchProfileSetV2` and one mode-`0600`
`ActivationRuntimeArtifactV2`. The artifact's execution context predicts the
exact active response: the prepared authority, writer, configuration and
execution fences; `execution_state: "active"`; and exactly the positive
ceiling and rate in the reviewed preparation request. It also binds the exact
pool executor, approved profile-set digest, active immutable-manifest digest,
controller/local/signing authorities, owner-only admission and handoff
directories, journal and state paths, Slurm authority, and complete profiles.

The artifact loader validates its embedded controller-local directories and
journal against the current UID. Therefore run each pool iteration on that
pool's controller as `loom_capacity_executor`, after its owner-only mode-`0700`
runtime directories exist; the loop below is a two-controller template, not a
command to run both pools on one host. Render the manifest digest first,
construct and byte-review the artifact with that digest at its final
controller-local path, and only then render the artifact-bound config and
environment:

```bash
pool='<this-controller-gb10-or-oldlab>'
approved_profiles="$evidence_dir/$pool-approved-profiles.json"
active_artifact="/etc/loom-capacity-executor/$pool-activation-runtime.json"

for input in "$approved_profiles" "$active_artifact"; do
  test -f "$input" && test ! -L "$input"
  test "$(stat -c %u "$input")" = "$(id -u)"
  test "$(stat -c %a "$input")" = 600
done

uv run --no-sync loom admin capacity-control-plane render-executor \
  --file "$executor_profile" --pool "$pool" \
  --output active-manifest-sha256 \
  --approved-profiles-file "$approved_profiles" \
  > "$evidence_dir/$pool-active-manifest.sha256"
uv run --no-sync loom admin capacity-control-plane render-executor \
  --file "$executor_profile" --pool "$pool" --output active-config \
  --activation-runtime-artifact "$active_artifact" \
  > "$evidence_dir/$pool-active.json"
uv run --no-sync loom admin capacity-control-plane render-executor \
  --file "$executor_profile" --pool "$pool" \
  --output active-service-environment \
  --activation-runtime-artifact "$active_artifact" \
  > "$evidence_dir/$pool-active-service.env"
jq -cS .execution "$active_artifact" \
  > "$evidence_dir/$pool-expected-active-context.json"
chmod 0600 "$evidence_dir/$pool-active-manifest.sha256" \
  "$evidence_dir/$pool-active.json" \
  "$evidence_dir/$pool-active-service.env" \
  "$evidence_dir/$pool-expected-active-context.json"

# After transferring both canonical context files to central operator evidence.
cp "$evidence_dir/gb10-expected-active-context.json" \
  "$evidence_dir/expected-active-context.json"
cmp -s "$evidence_dir/expected-active-context.json" \
  "$evidence_dir/oldlab-expected-active-context.json"
chmod 0600 "$evidence_dir/expected-active-context.json"
```

Transfer only the resulting hashes and canonical `.execution` evidence to the
central operator evidence directory, then compare the two predicted contexts
as shown above. Do not copy or reuse a pool's runtime artifact on the other
controller.

On each controller, the #906 installer places only that pool's active config,
runtime artifact and environment at the exact rendered paths, plus the
checked-in active service and timer. The active config and artifact are
`/etc/loom-capacity-executor/<pool>-active.json` and
`/etc/loom-capacity-executor/<pool>-activation-runtime.json`; the environment
is `/etc/loom-capacity-executor/active-service.env`. Apply the same ownership,
mode and non-symlink rules as the prepared files. Install the units root-owned
mode `0644`, but keep both active timers disabled and both active services
stopped:

```bash
sudo systemctl disable --now loom-capacity-pool-executor-active.timer
sudo systemctl stop loom-capacity-pool-executor-active.service
sudo systemctl daemon-reload
sudo systemd-analyze verify \
  /etc/systemd/system/loom-capacity-pool-executor-active.service \
  /etc/systemd/system/loom-capacity-pool-executor-active.timer
sudo systemctl is-enabled loom-capacity-pool-executor-active.timer \
  | grep -Fx disabled
test "$(systemctl is-active loom-capacity-pool-executor-active.timer)" = inactive
test "$(systemctl is-active loom-capacity-pool-executor-active.service)" = inactive
```

The active service has no `[Install]` section and cannot be enabled directly.
Do not start it yet.

## 7. Lock final readiness and activate the exact epoch

Use separate unbound principals with only `capacity:execution:activate`,
`capacity:execution:drain`, and `capacity:execution:retire`. Validate their
owner-only mode-`0600` curl configurations exactly as in section 3:

```bash
activate_curl_config=/absolute/owner-only/path/activate.curl
drain_curl_config=/absolute/owner-only/path/drain.curl
retire_curl_config=/absolute/owner-only/path/retire.curl

for config in "$activate_curl_config" "$drain_curl_config" \
  "$retire_curl_config"; do
  test -f "$config" && test ! -L "$config"
  test "$(stat -c %u "$config")" = "$(id -u)"
  test "$(stat -c %a "$config")" = 600
done
```

Obtain readiness again after all staging work. Replace the earlier response;
do not activate against an older digest. Repeat every section 5 assertion and
record the exact nonzero `readiness_sha256`. Then stop both prepared timers and
services so no refresh can race the transition. Preserve journals and all
installed files.

```bash
curl --silent --show-error --fail-with-body \
  --config "$read_curl_config" \
  "$manager_origin/v2/status/execution-preparation" \
  > "$readiness_response"
chmod 0600 "$readiness_response"
jq -e '
  .ready == true and .blockers == [] and
  (.readiness_sha256 | test("^[0-9a-f]{64}$")) and
  .readiness_sha256 != ("0" * 64)
' "$readiness_response"

# Run on both controllers.
sudo systemctl disable --now loom-capacity-pool-executor-prepared.timer
sudo systemctl stop loom-capacity-pool-executor-prepared.service
sudo systemctl show loom-capacity-pool-executor-prepared.timer \
  -p ActiveState -p SubState -p UnitFileState
sudo systemctl show loom-capacity-pool-executor-prepared.service \
  -p ActiveState -p SubState -p Result -p ExecMainStatus
```

Construct the strict activation request from the final response and reviewed
preparation request. Also construct the drain request in advance from the
common expected active context so emergency drain does not depend on editing
JSON after activation. Byte-review both mode-`0600` files.

```bash
activation_request="$evidence_dir/execution-activation.json"
activation_response="$evidence_dir/execution-activation-response.json"
drain_request="$evidence_dir/execution-drain.json"
drain_response="$evidence_dir/execution-drain-response.json"
activation_idempotency_key="$(uuidgen)"
execution_epoch="$(jq -r .execution_epoch "$prepared_response")"

jq -cn --slurpfile prepared "$prepared_response" \
  --slurpfile preparation "$preparation_request" \
  --slurpfile readiness "$readiness_response" '{
    schema_version: 2,
    authority_incarnation: $prepared[0].authority_incarnation,
    expected_writer_epoch: $prepared[0].writer_epoch,
    execution_epoch: $prepared[0].execution_epoch,
    execution_manifest_sha256: $prepared[0].execution_manifest_sha256,
    prepared_readiness_sha256: $readiness[0].readiness_sha256,
    executable_new_capacity_ceiling: $preparation[0].requested_ceiling,
    executable_new_capacity_rate_per_minute:
      $preparation[0].requested_rate_per_minute,
    executable: true
  }' > "$activation_request"

jq -c '{
  schema_version: 2,
  authority_incarnation,
  expected_writer_epoch: .writer_epoch,
  execution_epoch,
  execution_manifest_sha256,
  expected_executable_new_capacity_ceiling:
    .executable_new_capacity_ceiling,
  expected_executable_new_capacity_rate_per_minute:
    .executable_new_capacity_rate_per_minute,
  executable: true
}' "$evidence_dir/expected-active-context.json" > "$drain_request"
chmod 0600 "$activation_request" "$drain_request"

curl --silent --show-error --fail-with-body \
  --config "$activate_curl_config" \
  --header 'Content-Type: application/json' \
  --header "Idempotency-Key: $activation_idempotency_key" \
  --data-binary "@$activation_request" \
  "$manager_origin/v2/execution-preparations/$execution_epoch/activate" \
  > "$activation_response"
chmod 0600 "$activation_response"

jq -cS . "$activation_response" \
  > "$evidence_dir/activation-response.canonical.json"
cmp -s "$evidence_dir/expected-active-context.json" \
  "$evidence_dir/activation-response.canonical.json"
```

The manager recomputes readiness while holding the transition locks and
rejects any changed digest, fence, policy, acknowledgement, lease, inventory,
or heartbeat with `409`. Retry only the same bytes with the same idempotency
UUID. Never replace the request after a timeout; first determine whether the
exact request was accepted.

After the exact response comparison succeeds, enable and start the active
timer on both controllers, then force one service tick. Verify the two manager
executor sequences advanced beyond the final prepared response and archive
both controller journals:

```bash
# Run on both controllers.
sudo systemctl enable --now loom-capacity-pool-executor-active.timer
sudo systemctl start loom-capacity-pool-executor-active.service
sudo systemctl show loom-capacity-pool-executor-active.service \
  -p Result -p ExecMainStatus -p ActiveState -p SubState
sudo journalctl -u loom-capacity-pool-executor-active.service \
  --since '15 minutes ago' --no-pager

curl --silent --show-error --fail-with-body \
  --config "$read_curl_config" "$manager_origin/v2/status/executors" \
  > "$evidence_dir/active-executors.json"
chmod 0600 "$evidence_dir/active-executors.json"
jq -e --slurpfile before "$readiness_response" '
  .execution_state == "active" and .blockers == [] and
  ([.items[].pool_id] == ["gb10", "oldlab"]) and
  all(.items[] as $after;
    any($before[0].executors[];
      .pool_id == $after.pool_id and
      $after.heartbeat_sequence > .heartbeat_sequence and
      $after.inventory_sequence > .inventory_sequence))
' "$evidence_dir/active-executors.json"
```

If any check fails after activation was accepted, post the pre-reviewed drain
before any other remediation. A successful rehearsal also drains immediately
after the single verified tick; this is not an open-ended production window.
Use a fresh idempotency UUID and require the exact `drain-only`, zero-ceiling,
zero-rate response:

```bash
drain_idempotency_key="$(uuidgen)"
curl --silent --show-error --fail-with-body \
  --config "$drain_curl_config" \
  --header 'Content-Type: application/json' \
  --header "Idempotency-Key: $drain_idempotency_key" \
  --data-binary "@$drain_request" \
  "$manager_origin/v2/execution-epochs/$execution_epoch/drain" \
  > "$drain_response"
chmod 0600 "$drain_response"
jq -e '
  .execution_state == "drain-only" and
  .executable_new_capacity_ceiling == 0 and
  .executable_new_capacity_rate_per_minute == 0
' "$drain_response"
```

If the activation request itself was rejected and status proves the epoch is
still prepared, do not send drain; use the prepared-only abort branch below.

## 8. Continue drain-only cleanup and retire

Keep both active timers enabled during drain-only. The retained activation
artifact authorizes the exact executors to consume zero-capacity commands,
cancel or release their own work, and publish final evidence; it cannot admit
new capacity. Do not disable the timers merely because drain succeeded.

Wait until current manager status contains exactly both pools, no executor
blocker, empty inventory counts, and `retirement_safe: true`. The heartbeat
must be newer than the final inventory. Query every subject acknowledged by
the preparation request and require every observed intent state to be
`released`, with no active or quarantined capacity. The retirement transaction
independently repeats the all-intents-released check under database locks.

```bash
executor_status="$evidence_dir/drain-only-executors.json"
curl --silent --show-error --fail-with-body \
  --config "$read_curl_config" "$manager_origin/v2/status/executors" \
  > "$executor_status"
chmod 0600 "$executor_status"
jq -e '
  .execution_state == "drain-only" and
  .executable_new_capacity_ceiling == 0 and .blockers == [] and
  ([.items[].pool_id] == ["gb10", "oldlab"]) and
  all(.items[];
    .blockers == [] and .retirement_safe == true and
    .inventory_record_counts == {} and
    .inventory_digest != null and
    .inventory_observed_at < .last_heartbeat_at)
' "$executor_status"

subject_statuses="$evidence_dir/drain-only-subjects.jsonl"
: > "$subject_statuses"
chmod 0600 "$subject_statuses"
while read -r subject_id; do
  curl --silent --show-error --fail-with-body \
    --config "$read_curl_config" \
    "$manager_origin/v2/status/subjects/$subject_id" \
    >> "$subject_statuses"
  printf '\n' >> "$subject_statuses"
done < <(jq -r '.subject_acknowledgements[].subject_id' "$preparation_request")
jq -se '
  all(.[].intent_state_counts;
    all(to_entries[]; .key == "released")) and
  all(.[];
    .active_capacity_intent_count == 0 and
    .active_capacity_slots == 0 and
    .quarantined_intent_count == 0)
' "$subject_statuses"
```

Build the two canonical `ExecutionRetirementExecutorCheckpointV2` values only
from that fresh executor response. Byte-review the request, post it with a new
idempotency UUID, and require a non-replayed retirement response:

```bash
retirement_request="$evidence_dir/execution-retirement.json"
retirement_response="$evidence_dir/execution-retirement-response.json"
retirement_idempotency_key="$(uuidgen)"

jq -cn --slurpfile drained "$drain_response" \
  --slurpfile status "$executor_status" '{
    schema_version: 2,
    authority_incarnation: $drained[0].authority_incarnation,
    expected_writer_epoch: $drained[0].writer_epoch,
    execution_epoch: $drained[0].execution_epoch,
    execution_manifest_sha256: $drained[0].execution_manifest_sha256,
    executor_checkpoints: [$status[0].items[] | {
      schema_version: 2,
      executor_id,
      executor_incarnation,
      pool_id,
      pool_generation,
      heartbeat_sequence,
      command_sequence,
      journal_sequence,
      journal_digest,
      inventory_sequence,
      inventory_digest
    }],
    executable: true
  }' > "$retirement_request"
chmod 0600 "$retirement_request"

curl --silent --show-error --fail-with-body \
  --config "$retire_curl_config" \
  --header 'Content-Type: application/json' \
  --header "Idempotency-Key: $retirement_idempotency_key" \
  --data-binary "@$retirement_request" \
  "$manager_origin/v2/execution-epochs/$execution_epoch/retire" \
  > "$retirement_response"
chmod 0600 "$retirement_response"
jq -e --argjson epoch "$execution_epoch" '
  .execution_epoch == $epoch and .replayed == false
' "$retirement_response"
```

Only after retirement succeeds, disable the active timers and stop any
remaining active services on both controllers. The manager must be shadow at
ceiling zero:

```bash
# Run on both controllers.
sudo systemctl disable --now loom-capacity-pool-executor-active.timer
sudo systemctl stop loom-capacity-pool-executor-active.service

curl --silent --show-error --fail-with-body \
  --config "$read_curl_config" \
  "$manager_origin/v2/status/execution-preparation" \
  > "$evidence_dir/post-retirement-readiness.json"
chmod 0600 "$evidence_dir/post-retirement-readiness.json"
jq -e '
  .ready == false and .executable == false and
  (.blockers | index("manager-shadow") != null)
' "$evidence_dir/post-retirement-readiness.json"
uv run --no-sync loom admin capacity-control-plane status \
  --namespace loom-dev --kubeconfig "$kubeconfig"
```

### Prepared-only abort branch

If a failure occurs before activation is accepted and current status proves
the exact epoch remains prepared, keep both active timers disabled. With both
prepared timers already stopped, construct and byte-review this mode-`0600`
request and use the separate abort principal:

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
```

Abort is valid only for the exact current prepared epoch while no executable
intent exists. It append-only retires preparation and restores shadow at zero,
with the increase freeze retained. Retry only the same bytes and idempotency
UUID. The #906 operator decision determines whether frozen legacy writers
remain stopped or are restored after abort or retirement.

Finally hash all retained evidence without changing any journal:

```bash
mapfile -d '' evidence_files < <(
  find "$evidence_dir" -maxdepth 1 -type f \
    \( -name '*.json' -o -name '*.jsonl' -o -name '*.yaml' \) \
    -print0 | sort -z
)
test "${#evidence_files[@]}" -gt 0
sha256sum "${evidence_files[@]}" > "$evidence_dir/final-evidence.sha256"
chmod 0600 "$evidence_dir/final-evidence.sha256"
```
