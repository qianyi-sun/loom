# Global Fleet Capacity Manager

The global capacity-manager service computes deterministic, fleet-wide
allocations from versioned fleet configuration, subject configuration, demand
reports, pool observations, and current commitments. Shadow allocation and the
v1 executor protocol remain non-executable evidence surfaces. A separate v2
ledger can persist sealed executable allocation epochs and issue exact,
short-lived launch permits after a fenced execution epoch is activated.

The manager still has no worker-claim mutation or scheduler client. A v2 permit
can be consumed only by the exact registered pool executor. The separately
packaged controller-local executor and protected admission path can turn it
into physical capacity only under the exact active execution context; their
live installation, transport, and containment evidence remain activation
blockers. The checked-in Package 5A deployment remains inert at an executable
ceiling of zero.

## Authority boundary

The service uses an independent management database and its own migration
tree. It must not share an application environment's database. One authority
incarnation and monotonically fenced writer epoch protect shadow
reconciliation. A commit succeeds only while the exact input digest and writer
fence still match; changed inputs cause a fresh calculation instead of a
partial result.

The HTTP server requires mutual TLS and separately verifies hashed bearer
principals with bounded scopes. Owner-only `0600` files hold its database URL,
principal data, and TLS keys. Request bodies and list limits are bounded, and
metrics avoid dynamic environment or subject labels.

The service accepts configuration proposals and activation records, reporter
input, dynamic personal-subject projections, dry-run grant and executor
records, fenced execution preparation/activation evidence, executable-v2 pool
work, reconciliation requests, and read-only status/audit queries. Its database
enforces:

- executable work has one exact prepared execution epoch, manifest, fleet
  release, pool generation, and executor incarnation;
- executable intents descend only from sealed executable allocation epochs;
- executor and intent high-waters and states advance monotonically;
- command and protected-release receipts are append-only; and
- reporter sequence, writer fences, idempotency, and replay fail closed.

## Shadow allocation

The allocator applies tier priority, configured global/tier/account/subject
ceilings, stable current assignments, constrained-before-flexible placement,
and deterministic fairness across accounts and subjects. Missing, stale,
invalid, or equivocal reports do not free physical commitments.

The output is a hypothetical allocation plus diagnostics and audit evidence.
Existing environment-local scheduling and autoscaler paths remain the only
executable authorities.

1. strict tier priority (`production`, `staging`, `development`);
2. fleet, tier, account, subject, pending, and rollout ceilings;
3. progressive round-robin fairness between accounts and then subjects;
4. local task priority and stable current assignments;
5. constrained-before-flexible placement with exact per-node packing.

`min_slots` is configurable per subject and defaults to `0`. It is a bounded
minimum request, not a guarantee when compatible capacity is unavailable or a
higher-priority limit applies.

An architecture-specific task lists only compatible pools/domains, so it waits
or runs there. An architecture-neutral task may use either `gb10` or `oldlab`;
the allocator preserves existing placement where possible and otherwise uses
deterministic constrained-first topology placement. Users do not steer neutral
work by assigning pool weights. An explicit task pool requirement remains a
hard eligibility constraint.

Missing, stale, invalid, or equivocal reports never free capacity. Existing
claims and physical commitments remain charged, uncertain scope is frozen, and
old/new rollout overlap is limited by the subject and account surge ceilings.

## Fenced dry-run executor protocol

The same mTLS service exposes an implemented, non-executable grant protocol. A
manager principal can propose an exact reservation and issue a launch-ordering
permit. A pool-bound executor principal can register, renew its lease, fetch
its checkpoint, publish complete inventory, accept a reservation, register
bootstrap evidence, account for permit consumption, fence an unused intent,
and submit partial-release evidence. A subject-bound demand reporter publishes
the corresponding protected-environment release acknowledgement.

Protocol state and transition validation bind the authority and writer epochs,
pool and pool generation, executor identity and incarnation, and the relevant
reservation, intent, permit, subject, candidate, and deployment identities.
Every top-level contract and receipt has `executable: false`, while health and
status continue to report an executable-new-capacity ceiling of zero. The
executor library has no scheduler client, process-execution entry point, or
Slurm mutation surface, so a reservation acceptance or consumed permit is an
ordering/accounting record, not permission to run `sbatch`, cancel a job,
signal a worker, or release live capacity.

Registration binds one controller-local Ed25519 ownership key to an exact pool
executor. Only the public verification key reaches the manager. Complete
inventories carry signed immutable ownership metadata; missing, conflicting,
foreign, or unverifiable observations remain charged or quarantined rather
than being treated as free capacity. Fresh pool evidence is required at each
capacity-increase transition.

Executor commands are journal-first. The controller-local append-only journal
is fsynced before a transition, reconciled with the manager checkpoint, and
confirmed only after the bounded response validates. An ambiguous transport
failure is replayed with the same contract; a changed or regressed journal
fences the incarnation. Release evidence is accepted only after intent-close
state and the exact append-only environment-agent release fence agree, which
prevents delayed bootstrap or worker registration from reclaiming the shape.
See the
[pool-executor dry-run runbook](../runbooks/global-fleet-pool-executor-dry-run.md)
for controller binding, recovery, and rehearsal checks.

## Fenced executable-v2 work queue

Executable-v2 state is physically separate from the dry-run-v1 ledger. The
manager serves a pool only while the authority, execution epoch, manifest,
executor registration, executor incarnation, pool generation, and latest
sealed allocation epoch all match. Proposal, acceptance, bootstrap, permit,
consumption, close, and partial release commands share one authority-first lock
order and append exact command receipts.

Permit issue and consumption recheck global, tier, account, subject, pool,
pending-job, pending-slot, rate, topology, selected-node, executor-lease, and
inventory-freshness bounds. The final database-time fence includes the
earliest deadline of every pool observation used for global accounting. A
newer allocation epoch supersedes unused older work, and expired allocation
inputs cannot create or consume a permit.

Consumption moves an intent to `submitting-unknown`, which remains charged
until signed inventory observes physical work or the executor publishes an
exact post-consumption recovery command. Recovery requires a fresh complete
inventory plus authenticated controller evidence that both the submit process
and scheduler submission are absent; it moves the intent only into the normal
protected close path. It never frees capacity from an empty observation alone.

Protected release acknowledgements are authority-validated before replay and
stored as append-only, strictly increasing receipts. An old exact replay stays
valid after a successor is recorded, while physical release uses only the
highest retained protected registration epoch and matching terminal evidence.
Database triggers use `search_path=pg_catalog`, qualify queue roots through
`public`, reject illegal direct-SQL state/high-water changes, prohibit executor
unfencing, and make command and protected-release receipts immutable.

Accepted multi-shape reservations use one batched manager admission plan for
the complete subject/pool tranche. After the first covered intent reaches
`bootstrap-acknowledged`, the manager delivers the plan's exact shapes and
allowances to the subject reporter. The capacity agent enriches those manager
identities only with guard-local execution generations, sealed requirements
digests, and lifecycle sequences. It then commits the prepared plan, worker
shapes, placement allowances, and every protected assignment transition in one
serializable transaction. Any changed or incomplete local fact rolls back the
whole convergence. Only that committed transaction yields the exact
acknowledgement returned to the manager; the manager rejects a partial,
rebound, or replay-equivocated assignment set.

Admission acknowledgement does not bypass bootstrap protection. An intent
becomes `launch-ready` only when its exact protected bootstrap and the complete
batched admission plan are both acknowledged; `bootstrap-acknowledged` remains
the launch barrier until the plan acknowledgement is stored. PR #1425 completed
the protected manager-plan, capacity-agent admission, exact-assignment, claim
lifecycle, and release-acknowledgement mutation path. Public task-claim routes
remain disconnected, and the shipped executable ceiling remains zero until an
operator performs the protected activation sequence.

This queue is executable authority, not scheduler actuation. The manager has
no scheduler client and never calls Slurm from its HTTP routes. A separate
controller-local active executor can consume those exact commands only under a
matching activation artifact and manager execution context. The control-plane
CLI still exposes no apply, start, or ceiling-changing command; activation,
drain, and retirement are least-scope protected HTTP transitions.

The installed prerequisite builder prepares the initial #906 cutover with a
one-slot global execution ceiling. It retains the immutable physical pool
ceilings and configured finite launch-rate envelope; physical capacity is not
permission to activate every slot. The manager requires preparation to match
the owner policy and activation to match that exact prepared ceiling. Later
expansion therefore requires a separately reviewed execution epoch, not a
larger activation request against the initial preparation. Preparation remains
non-launching at an effective ceiling of zero until all activation gates pass.

After preparation, the installed execution plan issues the existing staging
executor's database login through a separate retained component. The original
bootstrap still requires a sealed executor. Issuance records that bootstrap's
terminal, database and role identity, guard, inputs, and one generated credential
before atomically enabling LOGIN and granting only owner-issued CONNECT. Recovery
uses the same credential and independently pinned sealed or issued schema profile;
PUBLIC and implicit routine grants are included in privilege drift checks. A
completed component can be observed under a valid successor guard; an interrupted
issuance must retain its original acknowledged guard. The credential is private
rollout state at this point; controller delivery and activation remain separate.

The installed rollout manager client exposes typed `activate_execution`,
`drain_execution`, and `retire_execution` transports for those existing APIs.
Each uses its own execution credential, requires a nonzero idempotency key, and
rejects response epochs or manifests that differ from the request. Activation
must return the exact requested ceiling and rate; drain must return zero for
both. Transport errors are not automatically retried: the caller must reconcile
the exact operation and retain its idempotency key. These methods do not run a
cutover, create freeze evidence, or install/start executors. The protected
activation composition still owns foundation-generation binding, the complete
writer and workload inventory, its operation journal, and #906's acceptance gates.

The installed owner-authority publication uses Linux atomic create-only rename
to retain a single-link, owner-only evidence file without overwriting an existing
publication. A process exit after publication leaves readable evidence; exact
replay syncs its directory before acknowledging completion. Unsupported atomic
publication, different existing evidence, and unsafe metadata refuse without a
hard-link fallback or automatic adoption. This storage guarantee does not create
freeze evidence or perform the live cutover.

## Controller-local Slurm inventory and active execution

OLDLAB's installed controller channel passes canonical request bytes to the
digest-pinned executor installer through Docker's attached stdin
(`--interactive`, without a TTY). The fixed command validator requires that
attachment for discovery, prerequisites, credentials, and prepared-executor
operations. Passing input to the Docker client alone does not deliver it to
the container. A disposable nonprivileged Docker regression covers all ten
operations; it proves byte delivery, not host admission or fleet activation.
The installer itself remains network-isolated. For `/host` operations, fixed
host commands use the validated host PID 1's network and UTS namespaces through
host `nsenter` after `chroot`; the controller hostname is read through that same
channel. Sharing a PID namespace or filesystem root alone does not supply host
networking or hostname identity. The executor image integration test verifies
this boundary inside a disposable container with its own root and namespaces,
without mounting the real host or invoking Slurm.

OLDLAB's supported partition provisioner converges the scheduler configuration
to the same root-owned, non-group-writable authority required by executor
discovery. It accepts the precise legacy ownership only as a migration input,
preserves configuration bytes in a private snapshot, and atomically publishes
an independent protected inode so retained legacy writable descriptors cannot
alter the new authority. This metadata transition alone never reloads Slurm;
it requires coordinated exclusion of conflicting administrator writes and does
not change foreign jobs or activate the executor. Disposable-container coverage
exercises actual ownership, retained descriptors and the unchanged executor
authority-file validator.

The separate `loom_capacity_pool_executor` namespace in the Loom wheel can
capture one controller-local Slurm 23.11 snapshot with only `scontrol show
nodes --json`, `squeue --json`, and `scontrol show config`. It brackets the
node read with two queue reads, then brackets that sequence with configuration
reads requiring the exact cluster and one unambiguous `PrivateData=none`.
Controller discovery and prerequisite admission require the same visibility
policy and bind it into their evidence digest. Partition or account membership
alone cannot prove that foreign jobs are visible. The fixed queue argv runs with protected `SQUEUE_ALL=1`; the runner
requires its effective UID to equal the dedicated non-root query UID. The
protected policy binds that UID, its query-principal identity, and a nonzero
evidence digest proving that the principal has complete job visibility under
the controller's `PrivateData` policy. All of those query semantics enter the
controller evidence digest. The adapter accepts only allocation-identical
queue documents whose finite positive `last_update`, exact cluster, Slurm
patch release, and data-parser identity match the protected policy and node
document. Every protected node must retain its exact CPU, memory, GPU, and
partition envelope. The adapter maps OLDLAB's uppercase controller names back
to canonical fleet IDs. The protected policy, rather than a compiled range,
selects GB10 nodes; the current canonical safe set is 1 through 15. Physical
node 16 remains outside Loom authority.

One accepted snapshot produces both `PoolObservationV1` and
`ExecutableExecutorInventoryV2`. Healthy busy nodes remain visible; current
jobs, node-less nonterminal jobs, pending arrays, GPU/TRES use, and unavailable
canonical nodes stay charged. A node-less job's canonical comma-separated
partition set is charged whenever any eligible partition reaches the protected
nodes; malformed, empty, or duplicate partition entries fail closed. Per-node
allocation counters are reconciled against visible jobs, and any hidden
residual or ambiguity becomes a quarantined node or full-pool charge. The
subprocess runner owns fixed `/usr/bin` binaries, a digest-bound root-owned
`/etc/loom/capacity/slurm.conf`, a minimal environment, bounded output and
timeout, and cancellation-safe child reaping. Every foreign or ambiguous
physical record is quarantined and therefore cannot authorize a capacity
increase.

The checked-in prepared systemd package uses that inventory path only at an
effective ceiling of zero and cannot construct the scheduler backend. The
separate active oneshot/timer requires an owner-only exact
`ActivationRuntimeArtifactV2`, a positive approved profile-set digest, and the
manager's exact active or drain-only context before constructing its fixed
Slurm submit/cancel backend. Drain atomically zeros ceiling and rate while the
active timers continue release cleanup and final inventory publication;
retirement requires fresh retirement-safe checkpoints from both pools and
every executable intent released.

The operator renders controller activation files from a portable
`ActivationRuntimeDocumentV2` without inspecting remote paths on its own host.
The document validates canonical paths and execution/profile bindings; it cannot
construct an executable runtime. Target-side `--validate-activation-only` checks
real private artifact/admission files and the authenticated manager context
without creating a journal or scheduler backend. Ordinary execution repeats
those checks, then binds the manager client to the verified active registration,
retaining that exact registration for drain replay.

## Dynamic personal subject projection

The fixed controller command channels expose `observe-active`,
`converge-active-files`, and `enable-active-timer`. An activation request binds a
stable operation UUID to the exact prepared profile, prerequisite release, and
portable V2 document. The installer derives the active config, runtime artifact,
and service environment; callers cannot supply an arbitrary file map. All
prepared, prerequisite, release, credential, and active mutations share one host
lock. Active file convergence requires stopped executor units and retains its
operation under a root-owned private authority directory before publishing any
service-owned input. Retained activation intent fences subsequent prepared
mutations. Changed operation identities or existing file bytes are refused.

Timer enablement validates the artifact using the installed service-user Python
and authenticated current manager context, then rechecks the local inputs. It
separately enables and starts the fixed active timer, allowing exact retries
after an interrupted enable and when already active. While files remain staged,
`refresh-active-preparation` runs only the exact prepared inventory oneshot,
which authenticates the still-prepared epoch without enabling either timer. This
refresh permits recovery when inventory expires during an interrupted cutover.
Each daemon invocation
independently authenticates execution authority. These transports do not create
admission bindings, freeze legacy writers, publish execution authority, or grant
manager activation; the installed composition must supply those prerequisites.

After stable-route activation, the lifecycle service registers the personal
deployment through `PUT /v1/development-projections/{subject_id}` before the
environment can be marked ready. The manager derives the
`dev-<name>` subject, its immutable owner account, and both physical-pool
profiles from the active operator-owned fleet template in one serializable
configuration epoch. The request binds the candidate publication, local
activation acknowledgement, deployment/configuration generations, reporter
incarnation, protected-admission evidence, trusted capacity-agent installation,
supported architectures, and required protocols. The lifecycle
cannot supply a priority tier, pool weight, worker shape, account ceiling, or
an executable override.

Projection is unavailable until an active fleet generation explicitly
contains a development-subject template and an owner-account template. Exact
operation and idempotency replays converge; identity reuse, stale epochs,
quota violations, or incomplete pool/architecture bindings fail closed.
Derived reporter credentials are hash-only and bound to the exact subject,
incarnation, configuration generation, deployment generation, and reporter
incarnation. Retrying a deployment rotates the reporter incarnation and
fences the predecessor. The projection response and audit log never expose
the token or its hash. All resulting allocations remain shadow-only while the
global executable ceiling is zero.

The independently installed capacity agent then captures protected lifecycle
demand and publishes an exact, sequence-fenced report. Its readiness probe
remains unavailable until a report succeeds. Personal lifecycle readiness is
therefore gated on both the subject projection and the agent's initial demand
publication, but neither event grants or launches physical capacity.

Personal teardown uses the same projection route with `operation_kind` set to
`destroy`. The manager first records the subject as `disabled` with zero
minimum and maximum demand. Only after that acknowledgement does the lifecycle
seal the personal database authorities and begin namespace, database, bucket,
tenant, and credential cleanup. Epoch contention is retried against a newer
configuration; incomplete retirement evidence fails before local deletion.

## Service surface

The HTTP service exposes configuration proposal/activation, dynamic personal
subject projection, report ingestion, dry-run-v1 executor records,
executable-v2 checkpoint/work/inventory/command routes, protected-release
acknowledgements, reconciliation, status, audit, health, and metrics. Mutual
TLS authenticates the transport, hashed bearer principals bind the exact
operator, reporter, manager, or pool-executor authority, and metric labels
never contain subject IDs or dynamic environment names.

The manager service has no scheduler client, Slurm mutation, or direct
physical-release client. Protected claim admission and lifecycle convergence
are mediated through exact manager plans, the capacity agent, and the personal
guard mutation surface merged in PR #1425; they do not expose an ordinary
public claim route. The packaged deployment's mTLS startup/readiness probe
observes any exact ready nonnegative ceiling so the Service remains routable
during activation and drain. The separate operator `status` command continues
to require the exact zero-ceiling boundary.

Run the checked-in offline proof without a live database or controller:

```bash
uv run --frozen python scripts/ops/global_fleet_capacity_shadow_once.py \
  --fleet tests/fixtures/capacity/fleet-v1.toml \
  --subjects tests/fixtures/capacity/subjects-v1.toml \
  --snapshot tests/fixtures/capacity/snapshot-v1.json \
  --output shadow-evidence.json
```

The output is canonical JSON, written atomically with mode `0600`, and records
`mode: shadow`, `executable: false`, and a zero executable ceiling.

## Environment-side guard data

Application databases can contain the separately owned
`loom_capacity_guard` schema. It stores sealed trial requirements, protected
attempt identities, prepared bindings, lifecycle observations, protected
release fences, legacy-writer inventory, and audit records under append-only
and serializable constraints.

Guard migration 0030 supplies schema `USAGE` to the verified definers of the
existing public terminal-closure and requeue triggers. Those definers already
have `EXECUTE` on their two specific guarded callees; PostgreSQL also requires
schema resolution permission, including for ordinary trials when the guard is
installed but inactive. This grants neither protected-table access nor general
claim/admission authority. The migration rejects missing or changed trigger
security bindings. Downgrade retains schema resolution because it may predate
the migration and is shared by the earlier trigger contracts; their specific
function permissions remain controlled by their owning migrations.

Guard 0031 adds interception for the `public.trials` mutation domain. Ordinary
writers hold compatible shared locks on a persistent sentinel and append one
immutable ledger entry per actual row mutation after explicit initialization.
Rollback removes that transaction's entries. Initialization and freeze require
READ COMMITTED, lock the exact registered authority without waiting, and reject
changed identities. Freeze takes exclusive ownership before counting committed
ledger entries; sequence gaps are not treated as committed mutations. Callers
must roll back a refused transaction before retrying.

The trusted public-table owner provisions a fixed, zero-argument trigger-cleanup
helper, executable only by that owner and the guard owner. Guard downgrade calls
it and checks the private uninitialized sentinel in the same transaction, so
refusal restores both public triggers. Before public DDL, downgrade locks the
complete guard-owned table set without waiting, using `ONLY` to avoid locking
foreign inheritance descendants; unexpected owners are rejected. This also
prevents older migrations in the same transaction from deadlocking with terminal
processing. The helper is not a standalone retirement
API. Protected staging authority maintenance also uses nonblocking lock-set
acquisition and requires an uninitialized sentinel and empty mutation ledger.
Personal provisioning may connect as a shared-fixture administrator while the
application tables belong to the instance role. It assumes that expected owner
only for helper provisioning, restores its prior role afterward, and rejects an
unexpected table owner without transferring ownership or retaining helper grants.

Application Alembic supports explicit `LOOM_DB_OWNER_ROLE` online execution.
This mode requires a distinct non-login, non-inheriting database owner and a
least-privileged, non-inheriting migration login expiring within one hour. The
only membership touching either role is the login's direct, non-admin,
non-inheriting `SET` grant to the owner. Alembic assumes the owner locally inside
its migration transaction, so created objects belong to the non-login owner;
offline execution is rejected. Omitting the setting retains historical
migration behavior and is not protected owner-separation evidence.

This migration capability does not itself transfer existing objects or change
runtime credentials. PostgreSQL `NOLOGIN`, password removal, and membership
revocation do not strip an already connected session's current owner authority.
Protected convergence must transfer exact application ownership, including
privileged helper functions, and reconcile old sessions before accepting writer
retirement. Updating only login attributes cannot establish that boundary.

The protected ownership transaction has a fixed three-definer handoff substep
for terminal closure, retry transformation, and trigger retirement. It requires
database-administrator authority, prior committed legacy login/membership
sealing, no non-administrator sessions in that database, an uninitialized writer
fence, and the table already transferred in the same transaction. Other databases'
sessions are outside this check. Function-body hashes are pinned to the trusted
application migrations; signature, security settings, owner, ACL, and exact
trigger attachments must match. Unexpected attachments on foreign tables cause
refusal, not cleanup. Bounded function-catalog locks protect verification and
ownership changes. The two private bridge permissions move to the new definer
owner and are revoked from the previous owner; exact replay is checked.
This substep is not a complete ownership-transfer API: the protected caller must
still compose database/schema/all-object transfer, runtime credentials and
grants, session reconciliation, and recovery before reopening runtime access.
The caller must serialize administrator DDL during this phase; the catalog-lock
operation can otherwise overwrite a concurrent cost-only metadata edit.

Guard reprovisioning can receive an explicit operator-owned
`ApplicationOwnerBinding` for the exact database, retained runtime role, and
new application owner. It checks the binding before provisioning and again in
the public-grant transaction; it never discovers and adopts a new owner from
the catalog. The database and trial table must have that non-login,
non-inheriting, nonprivileged owner, with no cross-database role dependencies.
Administrator provisioning requires no owner memberships; transient provisioning
requires only its exact pre-armed migrator membership. Helper DDL assumes that
owner and restores the provisioner without returning helper authority to the
runtime login. Cleanup uses an administrator-mode provisioner after transient
owner memberships are sealed; the limited migrator is not cleanup authority.
Cleanup validates the same binding and refuses foreign owner
dependencies even when the target database is already gone. The protected caller
must serialize administrator DDL through provisioning and cleanup.

This binding is not an all-object ownership or credential-sealing certificate.
No production lifecycle or staging installer selects it yet: exact application
inventory/transfer, migration credentials and manifest consumers, durable
recovery, and protected installer admission must be composed before enabling it.

The application schema inventory reader produces canonical PostgreSQL 16/17 catalog
observations for the database/public-schema owners and ACLs, relations, columns,
defaults, indexes, sequences, constraints, triggers, routines, types, rules,
policies, inheritance edges, and bound-role default privileges. It normalizes
only explicit role identity fields, not SQL text. Physical OIDs, task rows,
sequence cursors, statistics, and fast-default row storage are excluded. Internal
foreign-key trigger identities use constraints and functions rather than
OID-derived trigger names.

The reader queries only catalogs, never application relations or migration-version
rows. Qualified bootstrap calls and restored canonical session settings prevent
search-path substitution and formatting-dependent comparisons. A same-statement
gate rejects unhandled namespaced objects (including operators, operator classes/
families, collations and text-search objects), extension-managed application
objects, and referenced non-native type-output/type-modifier-output and index/
table access-method callbacks before deparsing. The roots explicitly include
every public relation (including standalone indexes) and the public namespace;
temporary schemas cannot shadow catalog reads. Unrelated foreign objects are not
adopted or modified. Object and byte limits bound accepted observations, not the
query's resource consumption.

The reader requires READ COMMITTED and the caller must externally serialize
relevant DDL throughout observation. PostgreSQL deparsers use catalog caches
that can be newer than an MVCC snapshot; a single SQL statement does not prevent
concurrent DDL from invalidating safety checks. Old-snapshot isolation modes are
rejected. This reader is not a safe privileged observer of a concurrently writable
schema; isolated trusted reference builds and a fully quiesced protected transfer
can satisfy its preconditions.

These observations are not trusted reference artifacts or ownership-transfer
receipts. The runtime package now bundles separate PostgreSQL-major-bound
legacy-owner and sealed-owner references for the reviewed pairs
`0134` / `guard_0030`, `0142` / `guard_0035`, `0147` / `guard_0035` and
`0147` / `guard_0036`, `0148` / `guard_0035`, and current `0148` / `guard_0036`.
