# Global capacity zero-ceiling preparation design

Status: approved under the delegated autonomous-development mandate
Package: 5C1
Date: 2026-08-15
Live activation: prohibited by this package

## Decision

Loom will add the missing production path for preparing, observing, and safely
aborting one global execution epoch while its executable-new-capacity ceiling
is exactly zero. The manager will load one immutable, digest-pinned owner
policy, accept one exactly fenced preparation request, register the two
controller-local executors, and report deterministic readiness blockers. Each
executor will publish a complete read-only Slurm inventory from its own
controller while the epoch is prepared.

This package deliberately exposes no execution-activation endpoint and ships
no nonzero ceiling. It makes the zero-capacity topology deployable and
rehearsable; it does not freeze a live writer, install or start a unit, apply a
Kubernetes manifest, submit or cancel a Slurm job, or authorize the #906
cutover.

The package continues the approved
[`Executable global capacity bridge`](executable-global-capacity-bridge-design.md).
Its purpose is to replace the current test-only preparation path and synthetic
empty prepared inventory with an operator-usable, fail-closed boundary before
any active execution control is added.

## Why this package is next

The executable bridge merged in #1417 contains the database state machine and
pool execution protocol, but the production process intentionally remains
inert:

- `CapacityManagementStore` can prepare, activate, drain, and retire an epoch,
  but production cannot load an `ExecutionPreparationPolicyV2`;
- the HTTP service exposes executable work and status routes but no execution
  preparation or executor-registration routes;
- a prepared executor currently publishes a synthetic empty inventory rather
  than observing its physical controller; and
- the checked-in systemd unit is validation-only and cannot maintain the fresh
  prepared inventory required by #906.

Adding a direct activation command at this point would bypass the missing
physical-evidence gate. A documentation-only checklist would be replayable by
humans but could not prevent an unsafe state transition. The selected package
therefore creates the verifiable zero-ceiling state first.

## Alternatives considered

1. **Digest-pinned policy, protected preparation API, and controller-local
   read-only inventory.** This is selected. It gives the manager authoritative,
   replayable evidence while retaining a hard zero ceiling and exact pool
   credential boundaries.
2. **A live-probing shell preflight.** Rejected. Hard-coded SSH, `kubectl`, and
   `systemctl` orchestration would be environment-specific, difficult to replay,
   and too easy to extend accidentally into scheduler mutation.
3. **An offline JSON checklist only.** Rejected. A signed or hashed checklist
   can preserve what an operator asserted, but it cannot prove that both
   registered executors observed their exact controllers or that their
   evidence is current in the manager database.

## Authority and state boundaries

The manager starts in one of two policy modes:

- **execution policy disabled:** the default when no policy path and digest are
  configured. Preparation returns a generic disabled conflict and all
  executable ceilings remain zero.
- **execution policy pinned:** a bounded JSON policy is loaded once at process
  startup from a regular non-symlink file and must match an independently
  configured SHA-256 digest. The parsed strict contract is immutable for the
  process lifetime. Any missing pair, unsafe file, digest mismatch, malformed
  field, or unsupported contract prevents manager readiness.

The policy is not a credential. The Kubernetes renderer stores it in an
immutable, digest-addressed ConfigMap and binds the expected digest separately
in the Deployment. Bearer tokens, TLS keys, ownership keys, and database
credentials remain outside the ConfigMap.

Preparation is a manager-database mutation but not a capacity mutation. It may
only perform this transition:

```text
shadow, ceiling=0
        |
        | exact operator request + pinned owner policy
        v
prepared, ceiling=0
```

A separately fenced abort restores `shadow, ceiling=0`. Abort is allowed only
for the exact current prepared epoch and manifest, after revalidating the
pinned policy, while no executable intent exists. It retires the prepared
epoch append-only and preserves all audit evidence. It cannot drain or retire
an active epoch.

No route in this package performs `prepared -> active`. Restarting or replacing
the manager writer retains the existing safe behavior: an exact prepared epoch
is retired, the successor writer remains shadow, and the increase freeze stays
set.

## HTTP surface

The manager adds these versioned routes:

- `POST /v2/execution-preparations` accepts
  `ExecutionPreparationV2`, requires an unbound
  `capacity:execution:prepare` principal and `Idempotency-Key`, and returns only
  the resulting zero-ceiling `ExecutionContextV2`.
- `PUT /v2/executors/{pool_id}/registration` accepts
  `ExecutableExecutorRegistrationV2`, requires the exact bound executor
  principal and `Idempotency-Key`, and returns the unchanged prepared context.
- `POST /v2/execution-preparations/{execution_epoch}/abort` accepts an exact
  `ExecutionPreparationAbortV2`, requires an unbound
  `capacity:execution:abort` principal and `Idempotency-Key`, and returns a
  typed retired-preparation receipt.
- `GET /v2/status/execution-preparation` requires `capacity:read` and returns a
  bounded, canonical readiness report.

Preparation and abort credentials are separate scopes so granting deployment
automation the ability to create a zero-ceiling rehearsal does not grant it
future activation authority. Pool executors retain their single-purpose
`capacity:execute:pool` credentials and must match the pool, executor,
incarnation, and pool generation in both the authenticated principal and body.

Request validation retains the global body-size bound. Conflicts do not reveal
which protected identity or policy field differed. Idempotent replay is valid
only while the exact resulting state remains authoritative.

## Prepared physical inventory

Each controller receives two separate artifacts:

1. the existing exact executor configuration, which contains manager,
   executor, pool, controller, profile, journal, credential, and prepared-epoch
   bindings; and
2. a read-only Slurm inventory policy, which contains the complete canonical
   node set, relevant partitions, per-slot resource facts, query identity,
   supported Slurm/parser versions, complete-visibility evidence digest, and
   root-owned executable/configuration digests.

Both artifacts are regular current-service-UID-owned mode-0600 files and are
independently digest-pinned in the systemd environment. The inventory policy
contains no credential and no command line. It can select only the existing
typed `scontrol show nodes --json` and `squeue --json` runner.

While the manager context is `prepared`, the executor:

1. validates its immutable local configuration and policy digests;
2. registers its exact executor binding idempotently;
3. renews its manager lease from the durable journal head;
4. captures a race-checked, complete read-only controller snapshot;
5. classifies every in-scope physical record under the existing ownership
   rules;
6. journals the exact inventory before publication, publishes it, records the
   confirmation, and heartbeats the new journal head; and
7. exits successfully without constructing `sbatch`, `scancel`, or any
   executable scheduler backend.

The prepared path has no activation runtime artifact and refuses an active or
drain-only manager context. This makes an accidental later activation stop the
prepared-only service rather than inherit mutation authority.

An installable prepared-inventory oneshot/timer may keep leases and evidence
fresh, but it is safe only because its executable supports prepared context
exclusively. The existing validation-only unit remains available for offline
artifact checks. Repository presence does not authorize installing, starting,
or enabling either unit.

## Readiness report

The canonical readiness report is derived from locked manager state and
database time. It includes only bounded operator-safe identities and counts:

- policy mode and policy digest;
- authority incarnation, writer/configuration/execution epochs, manifest
  digest, state, ceiling, and increase-freeze state;
- exact expected and registered executor identities per pool;
- lease, heartbeat, journal, inventory sequence, inventory freshness,
  complete-record counts, ownership classifications, and quarantine counts;
- configured and acknowledged subject counts; and
- stable blocker codes.

`ready=true` means only **prepared zero-ceiling rehearsal ready**. It requires:

- a pinned policy and the exact current prepared epoch;
- ceiling and rate both zero and increase freeze set;
- the complete current subject acknowledgement set still matching the active
  configuration and candidate identities;
- exactly one registered/current executor for OLDLAB and one for GB10;
- fresh leases, post-inventory heartbeats, canonical journal checkpoints, and
  fresh complete inventories for both pools; and
- no invalid, unknown, ambiguous, or quarantined in-scope physical record.

Foreign records are never mutated. They are reported explicitly and keep this
package's #906 readiness false, matching the current issue stop condition.
Later acceptance may change that only through a separately reviewed resource-
accounting design; this package does not silently reinterpret foreign work as
available capacity.

The report never calls a scheduler, follows a link, reads a bearer token, or
changes state. Application readiness, prepared-capacity readiness, and worker
availability remain separate.

## Rendering and deployment artifacts

`loom admin capacity-control-plane render` gains an optional exact pair:

- `--execution-policy-file`
- `--execution-policy-sha256`

Supplying neither preserves the current shadow-only manifest byte-for-byte
apart from deliberate renderer-version changes. Supplying only one fails.
Supplying both validates the file and renders:

- one immutable ConfigMap whose name includes the policy digest;
- a read-only volume mount at a fixed manager path; and
- the policy path and expected digest environment variables.

`render-executor` gains `inventory-policy` and prepared-service environment
outputs. The strict TOML profile carries both pool policies and rejects missing
or duplicate canonical nodes, shared state/credential paths, mismatched pool
generations, mutable identities, unsupported Slurm versions, zero visibility
evidence, or any command outside the fixed read-only runner.

All checked-in examples keep `executable_new_capacity_ceiling = 0`. No renderer
accepts a ceiling override. No command applies a manifest, installs a unit,
starts a timer, freezes a writer, or invokes Slurm mutation.

## Failure behavior and rollback

- **Policy absent:** manager remains healthy for shadow allocation; preparation
  is disabled and readiness reports `execution-policy-disabled`.
- **Policy unsafe, malformed, or digest-mismatched:** manager startup fails
  closed; readiness never becomes healthy.
- **Preparation conflict or partial executor registration:** ceiling stays zero;
  readiness identifies the missing or changed binding.
- **Inventory query failure, warning, partial visibility, race, oversize output,
  stale lease, or journal mismatch:** no replacement inventory is published;
  readiness becomes false and prior commitments remain charged.
- **Foreign, ambiguous, unknown, or unsigned Loom-scoped record:** report it,
  keep readiness false, and do not submit, cancel, adopt, or release it.
- **Prepared-only service observes active/drain-only:** stop with failure before
  constructing a scheduler backend.
- **Abort:** require the exact prepared fence and zero intents; append the
  retirement audit and return to shadow at zero. Any mismatch leaves the epoch
  prepared at zero.

Database restore rehearsal and legacy writer freeze execution remain #906
inputs. Their immutable digests are already bound in the owner policy and
preparation manifest; this package does not claim to generate those facts.

## Verification

The implementation must prove:

1. production settings reject half-configured, unsafe, oversized, changed, or
   digest-mismatched policy files and create no execution policy by default;
2. authorization keeps prepare and abort unbound and separate, while executor
   registration requires the exact single-purpose executor principal;
3. prepare, registration, readiness, replay, abort, writer replacement, and
   hostile request cases preserve database fences and a zero ceiling;
4. prepared inventory runs real typed read-only snapshots, journals before
   publication, recovers from every interruption point, and has no reachable
   scheduler-mutation backend;
5. readiness fails for every missing pool, stale lease, stale inventory,
   missing post-inventory heartbeat, invalid journal, incomplete subject set,
   quarantine, ambiguity, unknown record, and foreign record;
6. deterministic render tests cover the ConfigMap, digest binding, exact
   controller-local artifacts, systemd sandbox, and permanent zero ceiling;
7. the focused capacity and migration suites, repository security/hygiene
   gates, strict typing, and full authoritative CI pass; and
8. a source scan confirms this package adds no activation route, apply command,
   nonzero checked-in ceiling, `loom-dev-shared`, secret in rendered output, or
   mutable Slurm command.

## Follow-on packages and live sequence

Package 5C1 is followed, in order, by:

1. authenticated legacy freeze/adoption evidence collection and restore/
   rollback rehearsal completion;
2. an activation interlock plus separately scoped activate/drain/retire
   operator routes that consume the passing prepared-readiness checkpoint;
3. an explicit #906 operator window that freezes legacy writers, deploys and
   rehearses this zero-ceiling topology, then activates one exact slot for the
   x86, ARM, and architecture-neutral sequence; and
4. bounded expansion and live concurrent two-owner personal-development
   acceptance, including arbitrary source, isolation, safe redeploy, scale to
   zero, destroy, and artifact cleanup.

Until those steps pass, the global manager is not the live capacity authority
and the multi-person development environment is not operationally complete.
