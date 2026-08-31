# Task-image builder Phase 1 site convergence

This runbook converges the active Phase 1 native task-image builders. They are
exclusive Slurm allocations on `trt-gb10-2` (arm64) and
`trt-eai-oldlab-6` (x86_64), with the exact
`loom-task-image-builder` QoS and reservation. It does not activate the
allocation-scoped rootless provider. The rootless bundle, QoS, identity, and
node material below are retained only as disabled Phase 2 prerequisites; they
are not a gate or substitute for this active native-builder procedure.

The authoritative design is [task-image builder reliability](../architecture/task-image-builder-reliability.md).

## Phase 1 stop conditions and protected inputs

Run this procedure only from one reviewed immutable candidate. A failure at any
step leaves scale-up closed: do not enable the next supervisor, submit a canary
task, or fall back to the transitional exec transport. Existing ready registry
digests continue to serve trials independently of builder availability.

Use the rollout-only credential only for deployment actions; no supervisor unit
may reference it. The supervisor credential has a distinct, fixed path on both
controllers.

```bash
CANDIDATE_ROOT=/opt/loom-staging-runner/candidates/REVIEWED_GIT_SHA/repo
ROLLOUT_KUBECONFIG=/var/lib/loom-staging-rollout/kubeconfig
STATE_ROOT=/var/lib/loom-staging-rollout
SUPERVISOR_USER=loom-rollout
SUPERVISOR_UID="$(id -u "$SUPERVISOR_USER")"
SUPERVISOR_GID="$(id -g "$SUPERVISOR_USER")"
SUPERVISOR_GROUP="$(id -gn "$SUPERVISOR_USER")"
test "$SUPERVISOR_GROUP" = loom-rollout
SUPERVISOR_XDG_RUNTIME_DIR="/run/user/$SUPERVISOR_UID"
SUPERVISOR_KUBECONFIG="$STATE_ROOT/external-supervisor.kubeconfig"
EVIDENCE_ROOT="$STATE_ROOT/evidence/task-image-builder-phase1"
CAPACITY_PROFILE="$CANDIDATE_ROOT/deploy/dev-fleet/capacity-control-plane.toml"
MANAGER_IMAGE=REGISTRY/loom-capacity-manager@sha256:IMMUTABLE_DIGEST
AUTHORITY_INCARNATION=REVIEWED_NON_NIL_UUID

as_supervisor() {
  sudo -u "$SUPERVISOR_USER" env -i \
    HOME="$STATE_ROOT" USER="$SUPERVISOR_USER" LOGNAME="$SUPERVISOR_USER" \
    PATH=/usr/local/bin:/usr/bin:/bin \
    XDG_RUNTIME_DIR="$SUPERVISOR_XDG_RUNTIME_DIR" \
    DBUS_SESSION_BUS_ADDRESS="unix:path=$SUPERVISOR_XDG_RUNTIME_DIR/bus" \
    "$@"
}
```

On each controller, resolve the account identity at runtime. The installed
authority owns the protected state root itself: it must already be exactly a
non-symlink directory owned by `loom-rollout:loom-rollout` with mode `0700`.
Do not run `install`, `chown`, or `chmod` on that state root. Only create the
dedicated evidence child directory after the protected-root assertion passes.

```bash
getent passwd "$SUPERVISOR_USER"
test -n "$SUPERVISOR_UID"
test -n "$SUPERVISOR_GID"
test "$SUPERVISOR_GROUP" = loom-rollout
test ! -L "$STATE_ROOT"
test "$(stat -c '%F:%U:%G:%a' "$STATE_ROOT")" = \
  "directory:$SUPERVISOR_USER:$SUPERVISOR_GROUP:700"
sudo install -d -o "$SUPERVISOR_USER" -g "$SUPERVISOR_GROUP" -m 0700 \
  "$EVIDENCE_ROOT"
test "$(stat -c '%F:%U:%G:%a' "$EVIDENCE_ROOT")" = \
  "directory:$SUPERVISOR_USER:$SUPERVISOR_GROUP:700"
```

The only storage floor for an active builder is
`LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB`; the checked-in worker value is 20 GiB.
The builder itself probes Docker's root after bounded owned cleanup, and exits
before creating a control-plane client or claim when the probe is unavailable
or the final free bytes are below that floor. It repeats this admission after
each processed claim. Do not describe an allocation as materializing, or use a
canary to test it, before this storage admission passes.

Phase 1 launches the builder through `docker-compose.remote-worker.yml`, so the
container reads free space through
`LOOM_WORKER_TASK_IMAGE_STORAGE_PROBE_PATH=/run/loom/docker-storage-probe`.
That path is an empty read-only Docker-managed volume on the daemon data
filesystem, explicitly named `loom-task-image-builder-storage-probe` and
labelled `loom.task-image-storage-probe=true`. It is shared across allocation
project names, contains no task data, is never pruned by Loom, and avoids
mounting the daemon's Docker-root directory into the container.

## Ordered Phase 1 convergence

Perform the following sequence in order. Each numbered acceptance command is a
hard boundary; retain its output in the change record. Reapplying an earlier
declarative object is safe, but never skip forward after a failure.

### 1. Publish the stable witness transport

Render the reviewed capacity control plane through `loom admin` and apply it
with the rollout credential. Choose exactly one command below. The no-policy
form must not receive policy or CIDR arguments. The policy form binds the
reviewed policy bytes, its exact SHA-256, and every reviewed external-manager
controller CIDR.

```bash
loom admin capacity-control-plane render \
  --file "$CAPACITY_PROFILE" \
  --manager-image "$MANAGER_IMAGE" \
  --authority-incarnation "$AUTHORITY_INCARNATION" \
  | kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev apply -f -
```

```bash
CAPACITY_EXECUTION_POLICY=/secure/reviewed/execution-policy.json
CAPACITY_EXECUTION_POLICY_SHA256=REVIEWED_64_HEX_SHA256
CAPACITY_EXTERNAL_MANAGER_CIDR=REVIEWED_CONTROLLER_CIDR

loom admin capacity-control-plane render \
  --file "$CAPACITY_PROFILE" \
  --manager-image "$MANAGER_IMAGE" \
  --authority-incarnation "$AUTHORITY_INCARNATION" \
  --execution-policy-file "$CAPACITY_EXECUTION_POLICY" \
  --execution-policy-sha256 "$CAPACITY_EXECUTION_POLICY_SHA256" \
  --external-manager-client-cidr "$CAPACITY_EXTERNAL_MANAGER_CIDR" \
  | kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev apply -f -
```

Confirm that the stable ConfigMap—not a pod name—is present and that both keys
change while the publisher is running. Sample twice more than 30 seconds apart
so the evidence spans more than two 30-second witness TTLs. The readers still
perform signature, fingerprint, canonical digest, authority, pool, epoch,
execution-state, ceiling, and expiry validation; a successful readback is not
trust in the ConfigMap alone.

```bash
for SAMPLE in 1 2 3; do
  SAMPLE_PATH="$EVIDENCE_ROOT/witness-sample-$SAMPLE.json"
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
    get configmap loom-global-execution-witness-v1 -o json >"$SAMPLE_PATH"
  jq -er '(.data["gb10.json"] | length > 0) and (.data["oldlab.json"] | length > 0)' \
    "$SAMPLE_PATH"
  jq -er '.metadata.resourceVersion' "$SAMPLE_PATH"
  jq -er '.data["gb10.json"]' "$SAMPLE_PATH" | sha256sum
  jq -er '.data["oldlab.json"]' "$SAMPLE_PATH" | sha256sum
  if [ "$SAMPLE" -lt 3 ]; then sleep 31; fi
done
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get deploy loom-capacity-manager -o jsonpath='{.spec.template.spec.containers[?(@.name=="witness-publisher")].name}{"\\n"}'
```

Retain all three JSON objects, resourceVersions, and two key digests per
sample. The first-to-third interval is 62 seconds, spanning more than two
30-second witness TTLs. Stop if either key is empty, either signed value does
not refresh, or a reader rejects it. The publisher is the only workload token
holder: the manager container retains `automountServiceAccountToken: false`;
the publisher may only `get` and `patch` this ConfigMap.

### 2. Publish and prove the dedicated supervisor credential

On each controller, publish the narrow credential at the fixed path using the
rollout kubeconfig held only for this action. The publisher installs the scoped
database Secret, database-port-forward authority, and ConfigMap `get`; it must
not grant `pods/exec`.

```bash
sudo env KUBECONFIG="$ROLLOUT_KUBECONFIG" \
  "$CANDIDATE_ROOT/deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh" \
  "$SUPERVISOR_KUBECONFIG"
sudo chown "$SUPERVISOR_USER:$SUPERVISOR_GROUP" "$SUPERVISOR_KUBECONFIG"
sudo chmod 0600 "$SUPERVISOR_KUBECONFIG"
test ! -L "$SUPERVISOR_KUBECONFIG"
test "$(stat -c '%F:%U:%G:%a' "$SUPERVISOR_KUBECONFIG")" = \
  "regular file:$SUPERVISOR_USER:$SUPERVISOR_GROUP:600"
as_supervisor kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  get configmap loom-global-execution-witness-v1 -o name
as_supervisor kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  auth can-i create pods/exec
```

Accept only `directory:loom-rollout:loom-rollout:700` for the protected parent,
`regular file:loom-rollout:loom-rollout:600` for the final file, the exact
ConfigMap read, and `no` for `pods/exec`. `as_supervisor` starts from an empty
environment, so these runtime readbacks have no ambient `KUBECONFIG` and never
open `$ROLLOUT_KUBECONFIG`. Repeat the same commands on `gx10-01c7` and
`TRT-EAI-OLDLAB-1`.

### 3. Apply the active Phase 1 profile and prove empty-queue reconciliation

`loom-staging-rollout --env staging start` is the only staging mutation path.
It creates the broker-owned request envelope and applies the reviewed profile
to both controllers; do not invoke `loom admin environment-state apply` for
staging. All four active supervisors must name the stable ConfigMap source and
the dedicated supervisor credential: GB10 and OLDLAB trial supervisors, plus
GB10 and OLDLAB task-image-builder supervisors. No active argument may contain
`--global-execution-manager-export` or the rollout kubeconfig path.

```bash
loom-staging-rollout --env staging preflight
loom-staging-rollout --env staging start --dry-run
START_RESPONSE="$(loom-staging-rollout --env staging start)"
printf '%s\n' "$START_RESPONSE" | tee "$EVIDENCE_ROOT/staging-rollout-start.json"
ROLL_OUT_REQUEST_ID="$(printf '%s\n' "$START_RESPONSE" | jq -er '.request_id')"
loom-staging-rollout --env staging status "$ROLL_OUT_REQUEST_ID" \
  | tee "$EVIDENCE_ROOT/staging-rollout-status.json"
loom-staging-rollout --env staging logs "$ROLL_OUT_REQUEST_ID" \
  | tee "$EVIDENCE_ROOT/staging-rollout-logs.txt"
```

After `status` reports the protected attempt complete, run the next block
locally on the named controller. It starts each existing oneshot exactly once,
then retains only that invocation's journal JSON. Capture a journal cursor
immediately before every start and query only after that cursor: this prevents
a lifecycle line, or an earlier invocation, from being mistaken for the
supervisor result. `systemctl show` must report `Result=success` and
`ExecMainStatus=0`; each timer must be `enabled` and `active`. Each unit's
installed argument vector must name the reachable dedicated kubeconfig both for
its database client and ConfigMap witness reader. The trial result is a JSON
list with no queued slots or `scale_up`; the builder result is a JSON object
with a zero queue and no submitted or cancelled builder IDs. A
ConfigMap/signature failure is an acceptance failure, not a reason to retry
through exec.

The supervisor identity is deliberately not a member of `systemd-journal`.
The authorized operator therefore reads the system journal with `sudo`, bounded
by the pre-start cursor and the conjunction of the exact supervisor UID and
exact user-unit field. The captured journal and parsed result are then returned
to `loom-rollout:loom-rollout` with mode `0600`; do not broaden journal group
membership to make this evidence step work.

```bash
journal_cursor() {
  sudo journalctl --lines=0 --show-cursor --no-pager \
    | sed -n 's/^-- cursor: //p'
}

capture_one_json_result() {
  UNIT="$1"
  START_CURSOR="$2"
  RESULT_KIND="$3"
  JOURNAL_PATH="$EVIDENCE_ROOT/$UNIT.journal.jsonl"
  RESULT_PATH="$EVIDENCE_ROOT/$UNIT.json"

  test -n "$START_CURSOR"
  sudo journalctl _UID="$SUPERVISOR_UID" _SYSTEMD_USER_UNIT="$UNIT" \
    "--after-cursor=$START_CURSOR" --output=json --no-pager \
    | sudo tee "$JOURNAL_PATH" >/dev/null
  JOURNAL_PIPE_STATUS=("${PIPESTATUS[@]}")
  test "${JOURNAL_PIPE_STATUS[0]}" -eq 0
  test "${JOURNAL_PIPE_STATUS[1]}" -eq 0
  sudo chown "$SUPERVISOR_USER:$SUPERVISOR_GROUP" "$JOURNAL_PATH"
  sudo chmod 0600 "$JOURNAL_PATH"
  sudo jq -s -e --arg kind "$RESULT_KIND" '
    [ .[]
      | .MESSAGE?
      | select(type == "string")
      | (try fromjson catch empty)
      | select(
          if $kind == "trial" then
            type == "array" and all(.[]; type == "object"
              and (.queued_slots | type == "number")
              and (.action | type == "string"))
          elif $kind == "builder" then
            type == "object"
              and (.queued_materializations | type == "number")
              and (.submitted_job_ids | type == "array")
              and (.cancelled_job_ids | type == "array")
          else error("unknown supervisor result kind")
          end
        )
    ] as $results
    | if ($results | length) == 1 then $results[0]
      else error("expected exactly one acceptable JSON result after start cursor")
      end
  ' "$JOURNAL_PATH" | sudo tee "$RESULT_PATH" >/dev/null
  RESULT_PIPE_STATUS=("${PIPESTATUS[@]}")
  test "${RESULT_PIPE_STATUS[0]}" -eq 0
  test "${RESULT_PIPE_STATUS[1]}" -eq 0
  sudo chown "$SUPERVISOR_USER:$SUPERVISOR_GROUP" "$RESULT_PATH"
  sudo chmod 0600 "$RESULT_PATH"
}
```

On `gx10-01c7`:

```bash
for UNIT in loom-autoscaler-gb10-staging.service loom-task-image-builder-gb10-staging.service; do
  case "$UNIT" in
    loom-autoscaler-*) RESULT_KIND=trial ;;
    loom-task-image-builder-*) RESULT_KIND=builder ;;
    *) exit 2 ;;
  esac
  START_CURSOR="$(journal_cursor)"
  test -n "$START_CURSOR"
  as_supervisor systemctl --user start "$UNIT"
  as_supervisor systemctl --user show "$UNIT" \
    --property=Result --property=ExecMainStatus
  as_supervisor sh -c 'systemctl --user cat "$1" > "$2"' sh \
    "$UNIT" "$EVIDENCE_ROOT/$UNIT.unit"
  rg -F -- '--kubeconfig /var/lib/loom-staging-rollout/external-supervisor.kubeconfig' \
    "$EVIDENCE_ROOT/$UNIT.unit"
  rg -F -- '--global-execution-witness-config-map loom-global-execution-witness-v1' \
    "$EVIDENCE_ROOT/$UNIT.unit"
  rg -F -- '--global-execution-witness-kubeconfig /var/lib/loom-staging-rollout/external-supervisor.kubeconfig' \
    "$EVIDENCE_ROOT/$UNIT.unit"
  ! rg -F -- '--global-execution-manager-export' "$EVIDENCE_ROOT/$UNIT.unit"
  ! rg -F -- '/var/lib/loom-staging-rollout/kubeconfig' "$EVIDENCE_ROOT/$UNIT.unit"
  capture_one_json_result "$UNIT" "$START_CURSOR" "$RESULT_KIND"
done
as_supervisor systemctl --user show loom-autoscaler-gb10-staging.timer \
  loom-task-image-builder-gb10-staging.timer \
  --property=UnitFileState --property=ActiveState
as_supervisor jq -e 'type == "array" and all(.[]; .queued_slots == 0 and .action != "scale_up")' \
  "$EVIDENCE_ROOT/loom-autoscaler-gb10-staging.service.json"
as_supervisor jq -e '.queued_materializations == 0 and .submitted_job_ids == [] and .cancelled_job_ids == []' \
  "$EVIDENCE_ROOT/loom-task-image-builder-gb10-staging.service.json"
```

On `TRT-EAI-OLDLAB-1`:

```bash
for UNIT in loom-autoscaler-oldlab-staging.service loom-task-image-builder-oldlab-staging.service; do
  case "$UNIT" in
    loom-autoscaler-*) RESULT_KIND=trial ;;
    loom-task-image-builder-*) RESULT_KIND=builder ;;
    *) exit 2 ;;
  esac
  START_CURSOR="$(journal_cursor)"
  test -n "$START_CURSOR"
  as_supervisor systemctl --user start "$UNIT"
  as_supervisor systemctl --user show "$UNIT" \
    --property=Result --property=ExecMainStatus
  as_supervisor sh -c 'systemctl --user cat "$1" > "$2"' sh \
    "$UNIT" "$EVIDENCE_ROOT/$UNIT.unit"
  rg -F -- '--kubeconfig /var/lib/loom-staging-rollout/external-supervisor.kubeconfig' \
    "$EVIDENCE_ROOT/$UNIT.unit"
  rg -F -- '--global-execution-witness-config-map loom-global-execution-witness-v1' \
    "$EVIDENCE_ROOT/$UNIT.unit"
  rg -F -- '--global-execution-witness-kubeconfig /var/lib/loom-staging-rollout/external-supervisor.kubeconfig' \
    "$EVIDENCE_ROOT/$UNIT.unit"
  ! rg -F -- '--global-execution-manager-export' "$EVIDENCE_ROOT/$UNIT.unit"
  ! rg -F -- '/var/lib/loom-staging-rollout/kubeconfig' "$EVIDENCE_ROOT/$UNIT.unit"
  capture_one_json_result "$UNIT" "$START_CURSOR" "$RESULT_KIND"
done
as_supervisor systemctl --user show loom-autoscaler-oldlab-staging.timer \
  loom-task-image-builder-oldlab-staging.timer \
  --property=UnitFileState --property=ActiveState
as_supervisor jq -e 'type == "array" and all(.[]; .queued_slots == 0 and .action != "scale_up")' \
  "$EVIDENCE_ROOT/loom-autoscaler-oldlab-staging.service.json"
as_supervisor jq -e '.queued_materializations == 0 and .submitted_job_ids == [] and .cancelled_job_ids == []' \
  "$EVIDENCE_ROOT/loom-task-image-builder-oldlab-staging.service.json"
```

Accept only these four successful signed ConfigMap reconciliations, the exact
local Slurm authority, and zero active builder jobs with an empty
materialization queue. Verify the fixed Phase 1 allocation shape without
submitting a task: GB10 is `trt-gb10-2`, 19 CPUs, 110000 MiB, `gb10`,
`04:00:00`; OLDLAB is `trt-eai-oldlab-6`, 12 CPUs, 49152 MiB, `all`,
`04:00:00`; both are exclusive, concurrency one, account `loom-staging`, QoS
and reservation `loom-task-image-builder`.

```bash
squeue --name=loom-task-image-builder --format='%i %T %P %N %a %q' --noheader
scontrol show reservation loom-task-image-builder -o
sacctmgr --noheader --parsable2 show qos where name=loom-task-image-builder \
  format=Name,Flags,MaxJobsPU,MaxSubmitJobsPU,MaxWall
```

### 4. Clear GB10 storage only within Loom ownership

First inventory stopped containers. A container is eligible only when it is in
`created`, `dead`, or `exited` and either its own labels or its referenced
image labels show exactly one of `loom.task-image=true`,
`loom.task-sidecar=true`, or `loom.trial-cache=true`. Running containers,
unlabelled containers and images, and all container volumes are outside this
procedure. Repository names and tags do not establish ownership.

```bash
sudo docker ps --all --no-trunc --format '{{.ID}} {{.Status}} {{.Image}}'
sudo docker inspect "$CANDIDATE_CONTAINER_ID" \
  --format '{{.State.Status}} {{json .Config.Labels}} {{.Image}}'
sudo docker image inspect "$CANDIDATE_IMAGE_ID" --format '{{json .Config.Labels}}'
```

After an operator records the stopped state and qualifying label evidence for
that one exact container ID, remove only that ID; this command deliberately
does not pass `--volumes`. Repeat only for separately verified IDs, then let a
builder's own cleanup perform its managed-image TTL/pressure eviction and
fresh probe. Never run `docker system prune`, `docker container prune`, image
prune without the managed label filter, or a wildcard removal.

```bash
sudo docker container rm "$CANDIDATE_CONTAINER_ID"
```

Accept GB10 only after the builder records a reachable Docker root and final
free bytes at or above its required bytes; do not submit a canary just to cause
the check. If an allocation fails, queued demand remains durable but that exact
`(environment, pool)` waits five minutes after its latest failed allocation
before a retry. Do not manually shorten or bypass this cooldown.

### 5. Remove transitional exec authority only after proof

After both controllers have successfully read and validated the stable
ConfigMap, remove #1679's temporary exact-pod manager-export authority using
the rollout credential. The valid starting states are the complete four-object
transition set or complete absence; a partial set is a hard failure. Discovery
also rejects any `pods/exec` Role other than the exact reviewed transition
identity. Revoke the namespaced RoleBinding and Role before removing the
now-inert admission binding and policy.

```bash
TRANSITION_WITNESS_EXEC_NAME="loom-external-slurm-autoscaler-manager-export"
TRANSITION_EXEC_ROLES="$({
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev get role -o json \
    | jq -cer --arg name "$TRANSITION_WITNESS_EXEC_NAME" '
      [.items[] | select(any(.rules[]?; (.resources // []) | index("pods/exec")))
        | .metadata.name]
      | if (. == [] or . == [$name]) then .
        else error("unexpected pods/exec Role is present")
        end'
} )"
TRANSITION_PRESENT_COUNT=0
if kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get role "$TRANSITION_WITNESS_EXEC_NAME" -o json; then
  TRANSITION_PRESENT_COUNT=$((TRANSITION_PRESENT_COUNT + 1))
fi
if kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get rolebinding "$TRANSITION_WITNESS_EXEC_NAME" -o json; then
  TRANSITION_PRESENT_COUNT=$((TRANSITION_PRESENT_COUNT + 1))
fi
if kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" \
  get validatingadmissionpolicy "$TRANSITION_WITNESS_EXEC_NAME" -o json; then
  TRANSITION_PRESENT_COUNT=$((TRANSITION_PRESENT_COUNT + 1))
fi
if kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" \
  get validatingadmissionpolicybinding "$TRANSITION_WITNESS_EXEC_NAME" -o json; then
  TRANSITION_PRESENT_COUNT=$((TRANSITION_PRESENT_COUNT + 1))
fi
test "$TRANSITION_PRESENT_COUNT" -eq 0 || test "$TRANSITION_PRESENT_COUNT" -eq 4

if [ "$TRANSITION_PRESENT_COUNT" -eq 4 ]; then
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
    get rolebinding "$TRANSITION_WITNESS_EXEC_NAME" -o json \
    | jq -e --arg name "$TRANSITION_WITNESS_EXEC_NAME" '
      .roleRef == {
        apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: $name
      }
      and .subjects == [{
        kind: "ServiceAccount",
        name: "loom-external-slurm-autoscaler",
        namespace: "loom-staging"
      }]'
  TRANSITION_DELETE_IDENTITY="role/$TRANSITION_WITNESS_EXEC_NAME/rolebinding/$TRANSITION_WITNESS_EXEC_NAME/validatingadmissionpolicybinding/$TRANSITION_WITNESS_EXEC_NAME/validatingadmissionpolicy/$TRANSITION_WITNESS_EXEC_NAME"
  printf 'Delete exact transition authority? Type %s: ' "$TRANSITION_DELETE_IDENTITY"
  read -r TRANSITION_DELETE_CONFIRMATION
  test "$TRANSITION_DELETE_CONFIRMATION" = "$TRANSITION_DELETE_IDENTITY"
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
    delete rolebinding "$TRANSITION_WITNESS_EXEC_NAME"
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
    delete role "$TRANSITION_WITNESS_EXEC_NAME"
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" \
    delete validatingadmissionpolicybinding "$TRANSITION_WITNESS_EXEC_NAME"
  kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" \
    delete validatingadmissionpolicy "$TRANSITION_WITNESS_EXEC_NAME"
fi

! kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get role "$TRANSITION_WITNESS_EXEC_NAME" -o name
! kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  get rolebinding "$TRANSITION_WITNESS_EXEC_NAME" -o name
! kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" \
  get validatingadmissionpolicybinding "$TRANSITION_WITNESS_EXEC_NAME" -o name
! kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" \
  get validatingadmissionpolicy "$TRANSITION_WITNESS_EXEC_NAME" -o name
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev get role -o json \
  | jq -e '[.items[] | select(any(.rules[]?; (.resources // []) | index("pods/exec")))]
    | if length == 0 then true else error("unexpected pods/exec Role remains") end'
as_supervisor kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  auth can-i create pods/exec
```

Acceptance is `no`, with all four supervisors still reconciling from
`loom-global-execution-witness-v1`. Do not restore exec as a response to a
read failure: an unavailable witness must leave scale-up closed and drain
legacy-supervisor-owned capacity.

Re-run the two controller-local reconciliation blocks from step 3 after the
deletion. Retain the four new JSON files and require the same empty-queue/no-
submission assertions before proceeding.

## Rollback order

Capture the prior reviewed immutable capacity manifest, protected rollout
request/status evidence, supervisor profile/unit artifact, dedicated
kubeconfig, and any transition authority JSON before each mutation. The
transition objects are removal evidence only and must never be restored. A
rollback may restore only the other recorded prior artifacts; it must never
construct an ad hoc profile, credential, RBAC rule, or exec fallback.

1. Close scale-up first on both controllers. Stop all four timers and active
   oneshots as `loom-rollout`; do not start another reconciliation while rollback
   is in progress.
2. If the staging profile or units must be restored, first merge a protected
   rollback PR that restores the recorded prior branch-controlled bytes to the
   installed authority's pinned `refs/heads/dev` branch. The PR must pass CI
   and review. Record its exact merged release SHA, verify that preflight and
   dry-run bind to it, then use a new `loom-staging-rollout --env staging start`
   request. `start` accepts no ref or SHA argument; do not use `loom admin
   environment-state apply` or write user-systemd units directly.
3. Restore only the captured immutable capacity manifest and dedicated
   supervisor-kubeconfig artifact. Never restore transition exec RBAC or its
   admission objects. A witness failure remains fail-closed; restoring a
   ConfigMap transport never restores `pods/exec`.
4. Read back file ownership, ConfigMap access, exec denial, protected rollout
   status, and all four controller-local units before reopening scale-up.

On `gx10-01c7`, close the GB10 pair:

```bash
for UNIT in loom-autoscaler-gb10-staging.timer \
  loom-task-image-builder-gb10-staging.timer \
  loom-autoscaler-gb10-staging.service \
  loom-task-image-builder-gb10-staging.service; do
  as_supervisor systemctl --user stop "$UNIT"
done
```

On `TRT-EAI-OLDLAB-1`, close the OLDLAB pair:

```bash
for UNIT in loom-autoscaler-oldlab-staging.timer \
  loom-task-image-builder-oldlab-staging.timer \
  loom-autoscaler-oldlab-staging.service \
  loom-task-image-builder-oldlab-staging.service; do
  as_supervisor systemctl --user stop "$UNIT"
done
```

From the protected staging rollout host, merge the protected rollback PR before
this block. It restores the recorded prior branch-controlled bytes, has passed
CI and review, and is merged to the installed authority's pinned `dev` branch.
Set `ROLLBACK_RELEASE_SHA` to that exact merge SHA; a raw captured profile is
evidence only and cannot be selected directly by the rollout CLI. Capture and
bind each broker response before the real start. Only after the real response
is bound to that same SHA may the separately captured non-branch artifacts be
restored.

```bash
ROLLBACK_RELEASE_SHA=REVIEWED_MERGED_DEV_SHA
PREVIOUS_REVIEWED_CAPACITY_MANIFEST=/secure/rollback/REVIEWED_CAPACITY_MANIFEST.yaml
PREVIOUS_REVIEWED_SUPERVISOR_KUBECONFIG=/secure/rollback/REVIEWED_EXTERNAL_SUPERVISOR.kubeconfig

assert_response_sha() {
  jq -e --arg sha "$ROLLBACK_RELEASE_SHA" '
    if has("candidate_sha") then .candidate_sha == $sha
    elif has("resolved_sha") then .resolved_sha == $sha
    else error("response has no candidate identity")
    end
  ' "$1"
}

PREFLIGHT_RESPONSE="$(loom-staging-rollout --env staging preflight)"
printf '%s\n' "$PREFLIGHT_RESPONSE" \
  | tee "$EVIDENCE_ROOT/staging-rollback-preflight.json"
assert_response_sha "$EVIDENCE_ROOT/staging-rollback-preflight.json"
ROLLBACK_DRY_RUN_RESPONSE="$(loom-staging-rollout --env staging start --dry-run)"
printf '%s\n' "$ROLLBACK_DRY_RUN_RESPONSE" \
  | tee "$EVIDENCE_ROOT/staging-rollback-dry-run.json"
assert_response_sha "$EVIDENCE_ROOT/staging-rollback-dry-run.json"
ROLLBACK_START_RESPONSE="$(loom-staging-rollout --env staging start)"
printf '%s\n' "$ROLLBACK_START_RESPONSE" \
  | tee "$EVIDENCE_ROOT/staging-rollback-start.json"
assert_response_sha "$EVIDENCE_ROOT/staging-rollback-start.json"
ROLLBACK_REQUEST_ID="$(printf '%s\n' "$ROLLBACK_START_RESPONSE" | jq -er '.request_id')"
ROLLBACK_STATUS_RESPONSE="$(loom-staging-rollout --env staging status "$ROLLBACK_REQUEST_ID")"
printf '%s\n' "$ROLLBACK_STATUS_RESPONSE" \
  | tee "$EVIDENCE_ROOT/staging-rollback-status.json"
assert_response_sha "$EVIDENCE_ROOT/staging-rollback-status.json"

# Apply this captured manifest only to the controller that owns its recorded
# cluster artifact, after that controller's brokered rollback attempt completes.
kubectl --kubeconfig "$ROLLOUT_KUBECONFIG" --namespace loom-dev \
  apply -f "$PREVIOUS_REVIEWED_CAPACITY_MANIFEST"
# On each applicable external-supervisor controller, restore the separately
# captured credential and prove the exact reachable dedicated authority.
sudo install -o "$SUPERVISOR_USER" -g "$SUPERVISOR_GROUP" -m 0600 \
  "$PREVIOUS_REVIEWED_SUPERVISOR_KUBECONFIG" "$SUPERVISOR_KUBECONFIG"
test ! -L "$SUPERVISOR_KUBECONFIG"
test "$(stat -c '%F:%U:%G:%a' "$SUPERVISOR_KUBECONFIG")" = \
  "regular file:$SUPERVISOR_USER:$SUPERVISOR_GROUP:600"
as_supervisor kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  get configmap loom-global-execution-witness-v1 -o name
as_supervisor kubectl --kubeconfig "$SUPERVISOR_KUBECONFIG" --namespace loom-dev \
  auth can-i create pods/exec
```

Define `PREVIOUS_REVIEWED_CAPACITY_MANIFEST` and
`PREVIOUS_REVIEWED_SUPERVISOR_KUBECONFIG` only from the captured immutable,
review-approved rollback record. Perform the credential restore/readback on
each applicable controller; it must remain
`regular file:loom-rollout:loom-rollout:600`, with ConfigMap `get` success and
`pods/exec` `no`. Then run the step 3 controller-local status/unit/journal JSON
readbacks on both controllers. They prove all four units use the dedicated
credential and only their successful empty-queue results permit timers to be
re-enabled by the protected staging rollout.

## Disabled Phase 2 prerequisites: inputs, staging, and read-only preflight

This section is intentionally separate from the active Phase 1 rollout above.
Its rootless runtime bundle and evidence are prerequisites for a future Phase 2
decision only. They must not install, enable, or construct a rootless provider,
and their absence must not alter the native Phase 1 builder procedure.

Use one reviewed candidate checkout, one signed offline bundle per
architecture, and owner-controlled receipt/evidence locations. The candidate,
receipt, evidence, and bundle-parent paths below must already exist; each
bundle output path itself must be absent before assembly. Do not create a
storage mount as part of this workflow.

```bash
CANDIDATE_ROOT=/srv/loom/candidates/PHASE1_CANDIDATE
OLDLAB_BUNDLE=/srv/loom/offline/task-image-builder/oldlab
GB10_BUNDLE=/srv/loom/offline/task-image-builder/gb10
RECEIPT_ROOT=/srv/loom/receipts/task-image-builder-phase1
EVIDENCE_ROOT=/srv/loom/evidence/task-image-builder-phase1
```

Before a maintenance window, assemble the complete signed bundle into an
*absent* output path. Assembly is networked because it fetches the pinned
Ubuntu Snapshot Service closure, but it is inert: it does not install,
activate, or contact a cluster. It verifies the newly assembled bundle before
publishing it. Later bundle verification is offline. Assemble both
architectures before any host change:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_bundle.py" assemble \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --keyring /usr/share/keyrings/ubuntu-archive-keyring.gpg \
  --architecture x86_64 --output "$OLDLAB_BUNDLE"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_bundle.py" assemble \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --keyring /usr/share/keyrings/ubuntu-archive-keyring.gpg \
  --architecture aarch64 --output "$GB10_BUNDLE"
```

Stage the verified bundles locally on the target host/controller according to
local transfer policy. They include the pinned Ubuntu metadata, packages,
keyring, and runtime artifacts; staging is not installation or activation.
Verify each bundle offline before any host change:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_release.py" verify \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --bundle "$OLDLAB_BUNDLE" \
  --architecture x86_64

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_release.py" verify \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --runtime-manifest "$CANDIDATE_ROOT/deploy/task-image-builder/rootless-runtime-v1.json" \
  --bundle "$GB10_BUNDLE" \
  --architecture aarch64
```

On each controller, inspect the immutable policy plan before proceeding.  It
is an evidence-shape plan, not a convergence action:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_conformance.py" plan \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml"
```

Retain the candidate revision, policy digest, release digest, bundle-verifier
output, and planned receipt locations with the change record.  A verification
failure, a digest mismatch, unexpected writable artifact, identity conflict,
or missing storage prerequisite is a stop condition.

## Disabled Phase 2 controller preparation and additive Slurm readback

Converge OLDLAB completely before considering GB10.  For a future authorized
window, first run these non-mutating controller assertions on the exact
controller for the selected cluster:

```bash
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  check oldlab

sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  check --cluster-id oldlab \
  --receipt-dir "$RECEIPT_ROOT/oldlab/controller"
```

Always invoke Slurm plan/check/apply through the Python receipt producer above.
The Bash converger is its internal delegate and is not an operator entry point.

The identity contract is only `loom-builder` (UID 993) with primary group
`loom-task-builder` (GID 980), `/nonexistent`, and `/usr/sbin/nologin`; it has
no supplementary groups or Slurm administrative authority.  The Slurm check
must preserve and fingerprint the legacy QoS, association, reservation, fixed
nodes, backend, and supervisor.  Rootless objects are additive-only and use
the OLDLAB QoS `loom-task-image-builder-rootless-oldlab`; a present-but-different
rootless object is a hard failure, never a modification.

These commands are convergence assertions, not a separate success-only
preflight interface.  From a valid absent state, the identity check reports
`controller builder identity is incomplete` and the Slurm check reports
`task-image builder Slurm prerequisites are not converged`; each is an expected
nonzero assertion only after its policy/controller/legacy readback is otherwise
conflict-free.  Treat any identity conflict, legacy fingerprint mismatch,
present-but-different rootless object, unsafe/drift error, or other unexpected
error as fatal; do not apply.

When the absent-state assertions are the only failures, all approvals exist,
and the external storage prerequisite has been proven, the future authorized
sequence is identity apply, successful identity check, additive Slurm apply,
then successful Slurm check:

```bash
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  apply oldlab
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  check oldlab
sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  apply --cluster-id oldlab \
  --receipt-dir "$RECEIPT_ROOT/oldlab/controller"
sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  check --cluster-id oldlab \
  --receipt-dir "$RECEIPT_ROOT/oldlab/controller"
```

Inspect the apply and successful-check output immediately after each boundary.
The successful Slurm check/readback must show exact rootless objects and an
unchanged legacy fingerprint; retain its state receipt with the change record.
Do not continue to any node if an expected success assertion is nonzero or any
receipt/readback is absent or ambiguous.  No builder job is submitted at this
stage.

Repeat the same absent-state assertion, authorized apply, successful check,
and receipt/readback order for `gb10` only after OLDLAB evidence is complete,
all 15 GB10 aliases (including `trt-gb10-7`) are reachable, and
command-scoped noninteractive administrative authority has been granted.  Its
initial non-mutating assertions are:

```bash
sudo "$CANDIDATE_ROOT/deploy/slurm/install-loom-task-image-builder-controller-identity.sh" \
  check gb10

sudo python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_slurm_converge.py" \
  check --cluster-id gb10 \
  --receipt-dir "$RECEIPT_ROOT/gb10/controller"
```

GB10 uses the distinct additive QoS
`loom-task-image-builder-rootless-gb10`.  Neither cluster may use the legacy
`loom-task-image-builder` name for the rootless policy.

## Disabled Phase 2 one-node maintenance boundary

Only after the preceding checks and the externally provisioned mount have
passed, an authorized operator may schedule maintenance for one policy node.
Never perform two node applies concurrently, never cancel a job to make the
node idle, and never take over another operator's drain.  OLDLAB order is
`trt-eai-oldlab-3`, then `trt-eai-oldlab-4`, then `trt-eai-oldlab-5`; complete
the evidence for each node before selecting the next.

For the selected node, run host plan then host check, followed by controller
maintenance plan then maintenance check.  These examples use the first
OLDLAB node and create no mutable host or Slurm state:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_converge.py" plan \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --bundle "$OLDLAB_BUNDLE" \
  --receipt-dir "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/host"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_host_converge.py" check \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --bundle "$OLDLAB_BUNDLE" \
  --receipt-dir "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/host"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_node_maintenance.py" plan \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --candidate-root "$CANDIDATE_ROOT" --bundle "$OLDLAB_BUNDLE" \
  --receipt-root "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/maintenance"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_node_maintenance.py" check \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --candidate-root "$CANDIDATE_ROOT" --bundle "$OLDLAB_BUNDLE" \
  --receipt-root "$RECEIPT_ROOT/oldlab/trt-eai-oldlab-3/maintenance"
```

`check` is expected to remain negative while the required dedicated mount is
missing; record that result as a blocker and do not escalate it into an apply.
When all documented prerequisites and approvals exist, the authorized
maintenance procedure invokes exactly one node's `apply` action, records its
initial Slurm state/reason, drains only with the versioned Loom reason, waits
for natural idleness, applies the receipt-backed host change, performs the
bounded contained smoke/readback, and resumes only after every readback
passes and Loom still owns the drain.  The maintenance tool, not this runbook,
owns those mutation details and produces the terminal maintenance receipt.

Inspect the host receipt and maintenance receipt after every terminal result.
Accept `prepared` only with a matching candidate/policy/release digest,
verified rollback metadata, storage/quota and cgroup readback, Slurm alias
binding, smoke cleanup facts, and a maintained legacy fingerprint.  A
`blocked`, `rolled_back`, or `drained_rollback_failed` result stops fleet
progress.

After a rollback, an operator must inspect the receipt, the node's actual
Slurm state/reason, restored configuration and mount/quota readback, and the
recorded failure before choosing a separately authorized remediation.  Keep
the node drained when restoration is incomplete or unverified.  Automation
must not resume it, retry it, or advance to another node.

For GB10, use the same one-node protocol only after its authority and complete
reachability gates pass.  Its policy inventory is exactly `trt-gb10-1` through
`trt-gb10-15`; do not omit or substitute an alias.

## Disabled Phase 2 evidence assembly and verification

Evidence is collected only after a controller/node reaches the required
readback state.  Collection reads facts and receipts into caller-selected
outputs; assembly requires the complete inventory for both clusters.  The
following command shapes are inert templates, not a claim that any receipt or
evidence file exists today:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_evidence.py" \
  collect-controller --candidate-root "$CANDIDATE_ROOT" \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --cluster-id oldlab --slurm-receipt OLDLAB_SLURM_RECEIPT \
  --output "$EVIDENCE_ROOT/oldlab-controller.json"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_evidence.py" \
  collect-node --candidate-root "$CANDIDATE_ROOT" \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --cluster-id oldlab --slurm-node trt-eai-oldlab-3 \
  --host-receipt OLDLAB_NODE_HOST_RECEIPT \
  --maintenance-receipt OLDLAB_NODE_MAINTENANCE_RECEIPT \
  --output "$EVIDENCE_ROOT/oldlab-trt-eai-oldlab-3.json"
```

Collect one controller fragment and one node fragment for every exact policy
node in OLDLAB and GB10.  Then assemble both complete inventories, verify the
envelope, and write a canonical copy to the selected evidence location:

```bash
python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_evidence.py" \
  assemble --candidate-root "$CANDIDATE_ROOT" \
  --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --release "$CANDIDATE_ROOT/deploy/task-image-builder/host-release-v2.json" \
  --controller-evidence OLDLAB_CONTROLLER_EVIDENCE \
  --controller-evidence GB10_CONTROLLER_EVIDENCE \
  --node-evidence OLDLAB_NODE_EVIDENCE --node-evidence GB10_NODE_EVIDENCE \
  --output "$EVIDENCE_ROOT/phase1-assembled.json"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_conformance.py" \
  verify --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --evidence "$EVIDENCE_ROOT/phase1-assembled.json"

python3 "$CANDIDATE_ROOT/scripts/ops/task_image_builder_prerequisite_conformance.py" \
  canonicalize --policy "$CANDIDATE_ROOT/deploy/task-image-builder/prerequisites-v1.toml" \
  --evidence "$EVIDENCE_ROOT/phase1-assembled.json" \
  --output "$EVIDENCE_ROOT/phase1-canonical.json"
```

The two placeholder node-evidence arguments above stand for repeated
`--node-evidence` arguments: one for every policy node, with no duplicates or
omissions.  Do not assemble partial OLDLAB or GB10 evidence. Successful
verification remains a disabled-Phase-2 record: it reports zero certified
nodes, production certification false, and
`phase2_guard_provider_release_missing`.

## Closed Phase 2 boundary

This Phase 2 evidence is a prerequisite record, not authorization to build or
operate a rootless builder. Phase 2 must separately deliver and accept the
node guard, allocation-contained build-environment provider, credential
projection, network policy, publication/retention path, and contained BuildKit
execution. Until then, do not activate a rootless provider, policy, or supervisor,
advertise a rootless node feature, or certify rootless production readiness.
This boundary does not block the active native Phase 1 builder, including the
acceptance rerun task `4139e767`; that rerun must still wait for every active
Phase 1 convergence gate above.
