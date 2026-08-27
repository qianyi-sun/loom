# Concurrent-owner personal development zero-capacity acceptance

## Status and decision

The personal-development management plane, lifecycle authority, source sealer,
builder, per-environment storage, and global-capacity projection are already
implemented. The current bounded acceptance contract proves two concurrent
environments owned by one authenticated user. That was sufficient for the
single-owner #1280 launch decision, but it does not prove the product goal that
two people can deploy concurrently without reading or mutating each other's
state.

The multi-person gate will use a version-2 acceptance plan and result while
retaining version-1 parsing for already reviewed single-owner evidence. The v2
plan authorizes exactly two distinct users on two distinct teams. The same
digest-pinned acceptance manifest, manager identity, expiry, backup/restore
evidence, and byte-reviewed inert rollback remain in force. No operational
manifest, worker, task, Slurm command, or nonzero executable capacity is added.

The existing `personal-dev-zero-capacity-acceptance.md` and
`personal-dev-durable-launch.md` paths retain their #1280 sole-owner meaning.
This design is implemented only by
`personal-dev-concurrent-owner-zero-capacity-acceptance.md` and
`personal-dev-multi-owner-durable-launch.md`; a schema-v1 result cannot be used
to onboard a second person.

This is preferable to either replacing the v1 schema or running a procedural
second session outside the plan:

1. Replacing v1 would invalidate the accepted single-owner record and its
   durable-plan hash chain.
2. An unbound second session would exercise real authority that the reviewed
   plan did not name.
3. A separate v2 contract makes the stronger proof explicit without changing
   the application-plane render beyond its plan digest.

## Acceptance-plan contract

`PersonalDevAcceptancePlan` remains the public type. Its canonical schema has
two accepted forms:

- schema version 1 contains one `acceptance_owner` and remains byte-compatible;
- schema version 2 contains `acceptance_owners`, an array of exactly two owner
  objects.

Each owner object contains only canonical nonzero `team_id` and `user_id`
UUIDs. A v2 plan is valid only when:

- the owners are sorted by `(team_id, user_id)` canonical text;
- both user IDs are distinct;
- both team IDs are distinct;
- the global live-instance limit is at least two;
- the global builder concurrency is at least two;
- each per-owner live-instance and builder limit is at least one; and
- every existing release, storage, scanner, RuntimeClass, manager, principal,
  quota, window, and zero-ceiling check passes unchanged.

The dataclass stores a tuple of owners. Its legacy `acceptance_owner` property
is available only for a v1 plan, so existing callers keep their exact behavior
and new code cannot silently select one owner from a two-owner plan. Rendering,
runtime interlock, and status use the plan digest and shared safety fields; they
do not grant authority based on list position.

## Exact non-rotating acceptance credentials

The acceptance runbook uses two separate absolute XDG configuration roots.
Each root and configuration file is owner-only, non-symlink, single-link, and
byte/inode pinned for the complete window. The two paths and file identities
must be different. Before any source sealing or lifecycle mutation, each root
must already contain a distinct, non-rotating, user-owned API bearer token.
Browser sessions, legacy team tokens, service credentials, administrator
credentials, and missing identity fields fail this bounded acceptance gate.
Ordinary personal deployments continue to support both browser sessions and
user-owned API tokens.

`loom auth whoami` gains `--format json`. JSON output is one sorted,
newline-terminated, secret-free record containing the server, principal type,
credential type, user ID, team ID, role, scopes, token prefix, and expiry as
reported by the server. It never emits the bearer token, session cookie, or
CSRF value. The runbook requires exact `auth_kind="bearer"`,
`credential_type="user_owned_api_token"`, the existing bearer projection
`principal_type="team"` and `role=null`, each token's non-null exact user/team
pair to match one v2 plan entry, and exact `read:own` plus `submit` scopes
before any source is sealed. The principal projection deliberately remains
compatible with the shared whoami API; the plan-bound `user_id` distinguishes
the user-owned token from a legacy team token.

## Concurrent lifecycle and isolation proof

Owner 0 and owner 1 each use one distinct personal environment name and two
distinct arbitrary source roots. Initial `loom service up` commands start
before either is awaited, with `min_slots=0` and `max_slots=2`. Both updates
also start before either is awaited; owner 0 updates to maximum 3 and owner 1
to maximum 4. The two candidate digests must change independently, while all
subject, namespace, database, bucket, route, and worker-pool identities remain
disjoint between owners.

While both updated applications are ready, the runbook exercises the complete
cross-owner matrix in both directions:

- read: `loom dev status` for the other owner's name;
- update: `loom service up` for the other owner's name using the actor's
  already-ready candidate and the fixed positive noncreating epoch sentinel
  `1`; and
- destroy: `loom dev destroy` for the other owner's name, fenced by the exact
  positive operation epoch in the target owner's pinned before-status record.

Every attempt must exit exactly 1 and write no stdout. Before and after each
attempt, the target owner records canonical JSON status. Those byte streams
must be identical. The actor therefore learns no target document, and the
target's candidate, policy, generation, operation epoch, subject, and identity
cannot have changed. The update probe reuses an existing actor-owned candidate,
so it does not create an unrelated build merely to test authorization. The
personal `loom service up` command supplies `--quiet` for this probe; quiet mode
suppresses its actor-side progress and success summary but never suppresses
errors, changes HTTP behavior, or applies to local/staging/production targets.
All three probes use an explicit expected-hidden-denial mode. Only HTTP 404
from the intended target GET, PUT, or DELETE emits one canonical secret-free
receipt and exits 1. Candidate lookup, validation or preflight failure,
credential rejection, server failure, unexpected success, and response detail
never produce that receipt. The fixed receipt contains only schema,
`resource_hidden`, target method/phase, and status 404; the target response body
is discarded. The route supplies the same phase marker for a missing target and
an existing foreign target, so the marker proves which application handler
answered without becoming a resource-existence oracle. The target owner's
pinned before/after status records, not a distinguishable denial response,
prove that the named target existed throughout the probe.

Owner 0 then destroys normally. Owner 1 destroys with retained data, redeploys
the same name using the exact epoch captured in its destroyed status, and
destroys it finally. Redeploy must preserve `subject_id`
while rotating `subject_incarnation`. This schema-v2 retained-name contract is
owned by the separate concurrent-owner procedure; it does not reinterpret or
rewrite the protected schema-v1 sole-owner runbook.
The incarnation is the generation fence that prevents retired credentials and
acknowledgements from controlling the new deployment.

## Canonical result evidence

`personal_dev_acceptance_evidence.py` owns a strict v2 result validator. The
result is an owner-only, mode-0600, single-link canonical JSON file and binds:

- exact release, acceptance-plan, acceptance-manifest, and rollback-manifest
  SHA-256 values;
- both plan owners in canonical order;
- selected initial, updated, destroyed, retained-name redeployed, and final
  destroyed lifecycle snapshots;
- all six cross-owner denials, including actor, target, operation, exact exit
  code, empty-stdout digest, stderr digest, and equal before/after target-status
  digests; and
- digest-pinned acceptance-status observations plus final inert-shadow status.

Lifecycle snapshots contain the complete non-secret identity and fencing
fields needed for semantic validation, but omit volatile timestamps and error
text. The validator proves owner binding, readiness, zero minimums, finite
maxima, candidate and generation transitions, disjoint identities, destroy
semantics, retained-name incarnation rotation, denial-matrix completeness, and
zero worker availability. Unknown, missing, duplicate, reordered, noncanonical,
oversized, or digest-mismatched evidence fails closed.

The exact snapshot fields are `application_status`, `candidate_sha`,
`capacity_prepared`, `capacity_status`, `deployment_generation`, `identity`,
`keep_data`, `max_slots`, `min_slots`, `name`, `operation_epoch`,
`owner_team_id`, `owner_user_id`, `status`, `subject_id`,
`subject_incarnation`, and `worker_available`. `identity` contains the existing
canonical environment, namespace, database, three bucket, three host, route
path, and worker-pool fields. One owner-result entry contains `initial`,
`updated`, `destroyed`, `redeployed`, and `final_destroyed` snapshots. Owner 0
has null redeploy/final-destroy fields; owner 1 has both retained-name fields.

Each denial entry contains `actor_team_id`, `actor_user_id`, `operation`,
`target_environment`, `target_team_id`, `target_user_id`, `exit_code`,
`stdout_sha256`, `stderr_sha256`, `target_before_sha256`, and
`target_after_sha256`. The six-entry order is owner 0 against owner 1, then
owner 1 against owner 0, with `read`, `update`, and `destroy` in that order.
`exit_code` is exactly 1, `stdout_sha256` is the SHA-256 of empty bytes,
`stderr_sha256` is the exact operation-specific canonical expected-hidden-
denial receipt digest, and the target hashes are equal. Read binds target GET,
update binds target PUT, and destroy binds target DELETE. The strict validator
therefore rejects candidate-stage, wrong-phase, credential, server, successful,
or detail-bearing stderr evidence even when it is nonempty.

The result's status binding is an object with exact SHA-256 values for
`pre_deploy`, `after_initial`, `after_updates`, `after_denials`,
`after_destroy`, `after_redeploy`, `pre_rollback`, and `rollback_shadow`.
Every acceptance observation except `rollback_shadow` is produced by the same
plan-bound `status-acceptance` command; `rollback_shadow` is produced by the
ordinary inert-shadow status command after cleanup and rollback.

The CLI adds a read-only `verify-acceptance-result` command. It loads the exact
v2 plan and result plus the result-bound rollback-shadow status, validates
their relationship, and emits one canonical secret-free verification record.
The rollback status must contain the canonical ordered shadow components
(`cluster-resources`, `manager`, `namespaced-resources`, `namespaces`,
`personal-workers`, and `runtime-class`), with positive resource inventories,
one manager, one shared namespace, one runtime class, and zero personal
workers. The durable operational plan may bind only the verified v2 result for
the final multi-person launch evidence, and
`personal-dev-multi-owner-durable-launch.md` verifies it before render.

## Capacity and rollback invariants

The global executable-new-capacity ceiling remains exactly zero for the whole
procedure. Every mutable lifecycle phase is bracketed by the existing
acceptance-status observer, which proves the exact manager incarnation,
execution epoch/state, nonregressing configuration epoch, zero ceiling, and
zero personal workers. The runbook contains no task submission, worker
activation, `sbatch`, `scancel`, or pool-executor mutation command. With the
global manager as the only capacity authority, these checks prevent either
architecture-neutral or architecture-specific personal demand from becoming
executable.

After both personal environments and every build namespace are retired, the
runbook reapplies the byte-exact inert shadow rendered before forward apply.
Final status must again report shadow mode, activation replicas zero, no
dynamic namespace, no personal worker, and manager ceiling zero. The v2 result
is assembled and verified only after this rollback succeeds.

## Testing and rollout

Unit tests cover v1 byte compatibility, every v2 owner/list/quota rejection,
canonical result validation, lifecycle transitions, denial ordering, exact
operation-specific receipt digests, equal state hashes, and secret-free JSON
identity output. Request-sequence CLI tests prove each accepted receipt comes
from its intended target request and that explicit fencing bypasses only the
destroy/redeploy preflight it replaces. CLI tests also prove invalid
plans and results fail before runner construction. Package-boundary tests parse
the `personal-dev-concurrent-owner-zero-capacity-acceptance.md` shell and
require two XDG roots, concurrent commands, all six denials, exact state
comparisons, incarnation rotation, repeated zero-capacity checks, normal
teardown, and byte-exact rollback. They separately preserve both #1280 runbooks
byte-for-byte.

The change ships through a normal protected PR after the evidence-hardening PR
is merged. The live run uses a new trusted release from the final protected
commit and remains inside a separately reviewed concurrent-owner certification
window opened before a second person is onboarded. Missing or expired
credentials, either owner, any plan/result mismatch, any cross-owner success,
any target-state change, any worker, any nonzero ceiling, or incomplete cleanup
stops the procedure and preserves the inert safety boundary.

## Non-goals

- enabling OLDLAB or GB10 execution;
- submitting architecture-specific or architecture-neutral tasks;
- changing pool weights, autoscaler allocation, or `min_slots` defaults;
- admitting more than two owners in this bounded proof;
- changing namespace, database, bucket, DNS, or route derivation; or
- treating successful source builds as evidence of model/task capacity.
