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
nodes --json` and `squeue --json`. It brackets the node read with two queue
reads. The fixed queue argv runs with protected `SQUEUE_ALL=1`; the runner
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
`0134` / `guard_0030`, `0142` / `guard_0033`, `0146` / `guard_0033` and
`0146` / `guard_0035`.
The 0142 pair includes the publication/keyset, task-source, personal
membership, incarnation-storage and build-platform objects introduced by
application migrations `0135`–`0142`. The current 0146 pair additionally includes
the reviewed build-grant constraint, reward backfill, zero-quota constraint and
native build observation changes. Older reference pins remain available for
their explicit historical revisions. The trial-writer migration follows the
native-reader fence (`guard_0031`) and typed-terminal importer (`guard_0032`);
it does not replace either upstream security boundary. Both PostgreSQL
majors have independently generated pins; PostgreSQL 17 uses a pinned vanilla
17.4 image, not an observation of the live CNPG database.
The `guard_0035` protected writer migration adds narrow public-column permissions
for the non-login guard owner. Its six public reference profiles have separate
PostgreSQL 16/17 pins. The retained `0142` and `0146` / `guard_0033` profiles use the
original role-convergence recipe in `scripts/application_schema_trial_writer_baseline.py`;
current grants cannot silently change a rollback reference.

After the legacy trial writer is frozen, only the credential-validated protected
claim, retry, state/output, pending-cancellation and legacy-adoption functions can issue private,
one-use mutation permissions. They bind the
transaction/backend, frozen writer/epoch/operation, registration/authority,
trial/attempt/generation and exact worker/claim. Pending cancellation instead
requires an unclaimed protected attempt and records absent worker/claim identities
explicitly, alongside its actual cancellation lifecycle transition ID; only that
operation and legacy adoption permit those identity columns to be null. Its private issuer runs after
the cancel lifecycle event is inserted and verifies that exact current head, team,
attempt and absence of an executable claim before permitting the public update. The existing statement trigger
requires that permission for UPDATE; the existing AFTER-row accounting trigger
checks the full new row against the old row plus the permitted changes and
consumes the permission. An optional node-setup attempt refund has its own
permission in the same ledger. A claim permission is issued only after assignment,
readiness, admission policy/reservation and the actual new execution lease have
been validated or created. It permits only the claimed state, authenticated
worker, claim timestamp, cleared pre-start/failure fields and one attempt-count
increment. Due retry timestamps remain unchanged; reservation identity still
uses the protected attempt sequence even when public attempt counts are refunded.
Suppressed, extra-row or extra-column changes fail the transaction; quota,
reservation, policy, family, lease, lifecycle and successor changes roll back
with it. Validation relies on
the attested trigger chain's transactional effects, not arbitrary externally
side-effecting triggers. Claim and retry take the runtime lock before authentication and
uses nonblocking conflicting locks, so a competing operation is refused and
must retry its whole transaction rather than deadlock during a lock upgrade.

These consumed permissions remain private evidence; they do not advance the
frozen legacy mutation ledger. Authority reassignment locks and inventories the
permission table, and downgrade refuses retained evidence. There is no broad
role exemption, caller-set session flag or new public trigger authority. This
covers protected claim/retry, state reports, output publication, unclaimed
cancellation and explicit adoption of untouched legacy trials. It does not
authorize ordinary writers, frozen new submissions or recovery, and does not
by itself make the fleet ready for activation.

An authenticated `POST /api/v1/trials/{trial_id}/adopt-protected` accepts an
`operation_id` UUID and requires an administrator or a same-team bearer token
with `submit` scope. The control plane reads the existing projection and passes
its exact configuration, timestamps, ownership, provider and batch fields to the
runtime definer. SQL rechecks these inputs in the mutation transaction. Only an
untouched queued trial with an active original seven-day lifecycle authority can
be adopted. The original trial and batch IDs, submission time and expiration
remain unchanged. Ordinary submission still rejects an unproven ID collision.

The append-only adoption record binds the operation, original trial and lifecycle
identities, canonical inputs and protected attempt. An exact replay returns the
original receipt; a conflicting operation or projection fails. The frozen update
permission names this real adoption operation and permits only the state change
to `protected-pending`. Failed updates roll back the adoption record and every
protected projection. The guard owner gains only the three additional inspection
columns needed to reject prior claim/heartbeat/failure history. Runtime credentials
receive only the named adoption function. Existing task-image preparation and
guard-owned readiness validation run before the endpoint reports readiness;
adoption itself does not bypass prerequisite validation or activate capacity.

Protected demand capture reads the public projection without temporarily changing
trial state. The existing checks still validate runtime readiness, attempt and
retry bindings before reporting demand. The private capture normalizes an atomic
origin's `protected-pending` state in its query result only; the public row and
frozen legacy mutation counter remain unchanged. A pending public row cannot
resolve a contradictory terminal lifecycle blocker. Reporter sequence and
protected lifecycle writes retain their existing authority and transaction checks.

State reports validate the executor credential and exact live claim inside the
same serializable transaction as the public transition. Terminal-result and
family-plugin decisions use preliminary Python reads whose exact persisted inputs
are compared again under SQL locks. Trial state, exact claim closure and the
family transition commit together; pending sibling cancellation uses the existing
durable follow-up. Materializing-to-terminal reports close their exact claim even
though the historical public terminal trigger only handles claimed/running rows.

The worker's `PATCH /trials/{id}/trajectory_index` path also authenticates in its
write transaction. It commits result/index changes, typed artifacts, lifecycle
authorities, versioned object identities and lineage edges atomically. Descriptor
policy remains shared with the ordinary path, while SQL verifies its trial/batch
inputs and scopes every artifact to the authenticated trial. Suppressed or altered
writes and conflicting object versions roll back the whole publication. The
runtime login receives only the named protected function, with no general public
DML privileges.


Catalog SQL selects `daticulocale` on 16 and `datlocale` on 17 before
parsing; the canonical field remains `icu_locale` and hashes include the major.
The existing protected
source-tree/image admission binds this code and its reference; callers cannot
supply an alternative expected digest. `scripts/build_application_schema_reference.py`
rebuilds the reference from two independently created databases using actual
application migrations and the guard role/grant provisioner and migrations.
The older pair uses the preserved provisioning recipe from protected release
`7f8687e4a4a38ccf54e8c6b74821021da60879b7` in
`scripts/application_schema_baseline.py`; newer helper/column grants must not
leak into an older reference. It uses only disposable PostgreSQL containers at
the bundled immutable reference image digests, binds their ports to loopback, and
accepts no database address or output path. Inherited application database/owner
settings cannot select the migration target. Both fresh inventories must agree;
CI compares their metadata with the bundled reference and verifies the actual
migration heads and release image pin. Regeneration alone does not authorize
updating the trusted reference to match live drift.

Each ownership shape has three fixed environment profiles: `application-only` represents
personal-development grants; `staging-readonly` additionally provisions
the installer's exact `loom_rollout_readonly` CONNECT/public USAGE and 21-table
SELECT grants. `cnpg-staging` adds the same grants in a fresh UTF8 database
with explicit `C` collation and character classification, matching the admitted
CNPG initdb contract. Locale values remain part of the exact inventory.
The generator executes the same readonly bootstrap SQL in each
fresh database; it does not copy grants or hashes from a live installation. The
readonly role name remains exact in the inventory, including grantors and grant
options. Extra writes, missing reads, schema CREATE, or grant options change the
pin and are rejected. Ownership transfer and runtime-login restoration take the
same fixed ACL-profile selection from trusted operation requirements. A caller
must not discover/select a profile from live grants or change it on replay.
These pins do not replace separate role-attribute, membership, private-schema,
process and uninterrupted coordination-guard admission.

The generator emits all six ownership/environment profiles for each supported
revision pair and PostgreSQL major. Preparation, transfer, completion and login
restoration select the same pair from the original protected checkpoint; an
unknown pair is refused. The admitted catalog and the application/guard version
rows are both checked before transfer and login restoration. The older handoff
moves the two existing application bridges and requires absence of the later
writer fence and retirement helper. The current handoff also admits the helper
and requires its uninitialized writer fence. Schema compatibility
alone does not admit PostgreSQL 17 protected startup: LOGIN event triggers can
execute before the first administrator query. A catalog check after connecting
is too late to prevent that execution. The catalog reader separately refuses
every database-wide event trigger, including disabled triggers and callback
functions outside `public`; it never adopts or modifies an existing policy.
Callers must externally serialize event-trigger DDL throughout observation/use
and admit their privileged connection's startup before invoking the reader.

The destination reference is generated independently from an empty database:
application migrations run through an expiring, non-inheriting login with only
CONNECT and exclusive SET membership in the non-login owner. That owner owns
both the database and public schema before any application objects are created.
The temporary login and its CONNECT/membership are retired before the actual
bound guard provisioner runs. It is not generated by transferring the legacy
database and declaring that output correct.

Before the application admission snapshot, the internal owner preparation creates
`loom_app_staging_owner` as a non-login, non-inheriting role without credentials,
memberships or object authority. The original retained guard and fixed staging
administrator/database identity are checked against PostgreSQL. An immutable
attempt precedes CREATE ROLE; its actual OID is flushed to the component journal
inside that SQL transaction, before commit. A lost acknowledgement can recover
only that OID. If creation rolled back, a bounded subsequent attempt requires the
old peer to be absent (or the same current idle connection), followed by a fresh
role-absence check. An unrecorded or changed role is refused without adoption or
deletion. Guard loss observed before commit rolls creation back. This preparation
does not transfer objects or provide the complete installed handoff entrypoint.

The protected workload runtime keeps a journaled baseline of seven staging
writer Deployments and the lifecycle CronJob, and appends late owned CronJob
children before pausing them. Every patch tests the saved UID, current resource
version and complete spec. It checks the retained original database guard before
and after each patch, rejects other scale controllers and unowned Pods using the
application credentials, and waits for owned Pods to drain. Recovery repeats
a durable forward-restoration intent before actual database completion. A lost
completion reply resumes that forward path without re-entering login sealing or
manager replacement. Successful database completion is still required before any
workload restart. Recovery restores only the saved controllers and checks their
serving generation. The CronJob
remains suspended under the original rollout guard until that guard is released.
These operations are parts of the application handoff, not component terminal or
CNPG input-fence release authority.

The internal application handoff component composes original guard retention,
credential/configuration and primary-runtime binding, CNPG input fencing, SQL
profile admission for `loom`, `postgres` and `template1`, database preparation,
workload drain, one-shot manager replacement, forward restoration and retained-name
fence retirement in one original component journal. Every phase rechecks the
original guard and inputs; only freshly observed restoration and retired fences
permit an exact terminal. External operator/process/storage writer admission and
supervised guard/epoch readers are required enclosing capabilities. Their binding
is recorded before SQL mutation; a recorded digest does not establish authority.
The installed handoff factory now supplies supervised original-guard checks,
actual operator/storage observations and exact epoch readers without observing or
mutating while the component chain is constructed. It uses normal exact epoch
reads before retention acknowledgement, then the original guard mailbox while
admission can be closed; both paths recheck guard identity. The enclosing executor
and early worker recovery still need the component wired into the full chain.

A completed handoff uses a separate replay path. The journal first validates the
original restoration, fence retirement, terminal and retention acknowledgement.
Fresh observations then bracket the current protected guard and epoch, original
credential and cluster identities, current operator/storage and SQL admission,
retired fence names and any independently admitted successor role. The SQL reader
checks enduring ownership, the original runtime password and absence of runtime
DDL authority. It permits later owner migrations to advance schema markers and
add objects, while admitting only the fixed reviewed public SECURITY DEFINER
bodies. An unknown or modified trigger definer is rejected even when runtime
EXECUTE privileges are absent, because trigger invocation bypasses that ACL.

The optional successor identity constrains one exact application migrator with
non-inheriting SET membership in the application owner. A separately recorded
capacity guard owner selects the distinct capacity migrator envelope: exactly
two non-admin inheriting SET memberships, in that guard owner and the application
owner. The observer verifies the guard owner's saved OID, sealed attributes,
schema ownership and database scope, and rejects additional memberships and role
settings in either envelope. It does not authorize a
role discovered in the live catalog or replace successor credential, Job and
retirement evidence. Completed replay preserves the historical terminal only
after these fresh checks; later components certify their own schema and workload
effects. Pending handoffs retain strict original-guard and original-schema
recovery. The real successor lifecycle and installed executor wiring remain
required before this completed replay path can be used in a rollout.

The SQL migrator provisioner creates only a new sealed role, publishes its OID
through the protected journal callback before committing creation, and rechecks
the original coordination guard before commit. An interrupted creation cannot
adopt an ambient same-name role. Arming uses one saved credential and a fixed
expiry within one hour, with only non-inheriting SET membership in the application
owner and CONNECT on the target database. Lost acknowledgement verifies the
same credential and expiry without extending it. Sealing disables login and
removes the password; the separate closed-admission retirement step must still
terminate surviving owner sessions and remove the role. These SQL phases do not
deliver a Secret, run a migration Job or publish lifecycle completion.

Post-handoff migrator cleanup has separate admission phases on the maintenance
database. Closure requires the exact sealed migrator and saved owner/guard.
Reopening requires the migrator role and its sessions to be absent, owner
memberships retired, no pending database startup and the unchanged original
runtime credential. Both phases reconcile lost commit acknowledgements without
rotating credentials. The lifecycle must journal its maintenance peer and intents,
establish that an earlier peer cannot still commit, and stop its owned Job before
using these phases.

Ownership handoff and application migration use separate append-only guard
retention requests and acknowledgements. The guard validates both records before
acknowledging the sole pending operation, and remembers every component it has
observed. Deleting a migration's retention files cannot be masked by a completed
handoff. Each component requires its own acknowledgement before sensitive work;
only its bound terminal releases that retention. A migration may retain the
original guard or the exact successor at the already-claimed epoch. A completed
handoff's historical reader checks its own retention independently while the
migration keeps the current guard alive.

The journal's early application recovery entrypoint selects that sole pending
handoff or migration from its retained intent. It requires the saved plan,
acknowledged live guard, advanced epoch terminal and original complete component
ordering, then executes only the selected component at its original ordinal.
Ordinary epoch, workload and other component callbacks do not run while database
admission may be closed. The original handoff-only entrypoint remains restricted
to handoff recovery. Installed worker dispatch must invoke the general entrypoint
before its normal database admission reads.

The migration artifact builder can bind the fixed separated staging owner into
its digest and deterministic Job name. That profile references a dedicated
per-Job Secret for the temporary database URL and CA, passes the explicit owner
to Alembic, disables service-account token mounting and uses a non-root process
with a read-only filesystem. It omits automatic Job TTL deletion so the protected
lifecycle can observe and retire the exact Job and its Pods. Artifact inspection
rejects a runtime credential substitution, changed owner, extra environment
authority or automatic deletion. The existing default renderer remains available
for legacy environments; installed selection requires the complete owner lifecycle.

The Kubernetes migration resource adapter requires recorded creation dispatch and
the generation-bound desired objects under the same guard. A lost creation reply
may reconcile only that exact object; a recorded UID that disappears is never
silently recreated. New credential delivery refuses existing Jobs or Pods waiting
on the Secret. Job creation checks the recorded Secret UID before and after use.
Cleanup sends UID/resourceVersion-preconditioned foreground deletion and refuses
to remove credentials while a Job or Pod still consumes them. These effects are
subordinate to the migration journal and SQL retirement phases, not standalone
rollout or credential authority.

The private migration journal records a bounded, hash-chained sequence beneath
its original component intent. It binds the completed handoff, original guard,
credential and input digests, each fixed credential generation and creation peer,
the role OID before SQL commit, and Secret/Job dispatch before delivery. Recovery
retires an interrupted generation before issuing another credential. Replacement
maintenance peers are recorded before continuing cleanup, after fresh catalog
checks prove that their predecessors cannot still commit. An unrecorded or
replaced role is never adopted.

The migration lifecycle composes these SQL and Kubernetes phases. It seals the
login, stops the generation's Job and Pods, closes database admission, retires
role sessions and memberships, removes the role and Secret, then reopens admission
with the original runtime password. Read-only resource reconciliation handles
lost creation acknowledgements without recreating credentials. If an interrupted
Job already committed its migration, the fully retired lifecycle can observe the
target revision without rerunning it. A completed component checks enduring
ownership, target revision and the absence of every retired generation's resources
and roles under the current guard; a pending component retains its original guard
and avoids ordinary application database reads.

Generation manifests derive only names and credential references from the
attested protected Job. The URL is fixed to the staging PostgreSQL read-write
service and `loom` database with `verify-full` and the dedicated CA mount. CA
admission checks the fixed CNPG Secret, owning cluster, unchanged Secret UID and
resource version, certificate authority constraint and remaining validity. It
retains only the public certificate. The installed migration factory shares the
handoff's journal and input observers, and uses the retained epoch connection
while admission can be closed. Apply and convergence select the same installed
handoff/migration pair after the epoch claim, preserving their original ordinals.
Before ordinary final admission reads, the worker resolves the original pending
operation and invokes its installed recovery path under the same acknowledged
guard. It requires a matching terminal and completed retention before proceeding.
Peer-loss recovery retires its maintenance connection before completion opens
its own, preserving the original guard and replacement handoff peer while
enforcing cluster-wide client retirement.
Normal prefix and convergence observations require later owner grants to have
retired; they do not adopt active memberships as successor authority.

Later requests select the original completed ownership transfer from a bounded
inventory of private request journals. They revalidate its original plan, intent,
phase chain and terminal, rejecting contradictory origins. A current guarded
observation checks the enduring SQL effect and retired fences, then records a new
observational terminal at ordinal two. Migration and capacity require that current
terminal before consuming the original handoff evidence. Historical observations
cannot authorize another ownership transfer or replace current operation guards.

PostgreSQL can launch autovacuum while database admission is closed. A quiescence
refusal rolls back the ownership transaction; completion may wait up to thirty
seconds when the only extra backend is autovacuum or it has already retired
before the retry probe. The probe admits exactly the original peer and guard,
after verifying cluster-wide client retirement. Each retry repeats closure,
complete drainage and schema checks.
Other refusals and surviving client writers remain errors.
The trigger's quiescence refusal uses application SQLSTATE `55L01`. The protected
peer transport preserves that bounded code while discarding server diagnostics,
so recovery does not depend on private error text or retry unrelated `55000`
authority refusals.

Capacity bootstrap uses a permanent migrator with exactly the application owner
and guard owner memberships. Its SQL helpers retain both owner identities, keep
the permanent roles sealed after session retirement and verify the original
runtime password before reopening admission. The private ordered journal binds
the six original capacity role OIDs, seed and preceding migration digests, and
each fixed credential lease before delivery. A resumed generation must finish
retirement before another can be armed. Installed apply, convergence and early
recovery select this lifecycle at its original ordinal, passing the executing
journal instance through the capacity component. Reopening follows exact resource
retirement and removal of both owner memberships and the guard owner's temporary
schema-creation grant. Ordinary credentials become durable only after full
configuration observation; a retry preserves credentials already durable.

An unused legacy authority can migrate only from its independently certified
configuration and complete audit history. The component journals that initial
state and the certified SQL digest before a credential generation, then executes
the same bounded transaction through its recorded creation connection. Recovery
observes a committed target before retrying. Unknown configuration or surviving
legacy bootstrap resources cannot become fresh bootstrap authority. Completion
also rechecks the original ordinary role OIDs and passwords. These checks establish
the capacity database component's result; fleet activation retains its separate
evidence requirements.

The read-only CNPG operator observer admits the fixed 1.25.1 command, environment,
security context, volume projections and absent configuration overrides. It brackets
a host-process observation with unchanged Kubernetes inputs. The host reader binds
CRI labels/container ID, PID/start time and namespaces, requires a single operator
process and an actual read-only root mount, and compares the running and stored
executable inodes and independently pinned bytes. Nanosecond timestamps detect
same-size changes during hashing. It accepts a terminal Pod record only when all
reported containers have terminated. The command runner uses a dedicated SSH observation transport with fixed key,
root-owned endpoint/host-key files and a forced-command remote observer. Only
literal host/port mappings are admitted; inherited agents, forwarding and
connection sharing are disabled. A fresh challenge binds each result to the
original identity and exact installed observer source, and trust-file identity,
ACL and content are checked again after the call. The bounded host protocol
refuses duplicate fields and arbitrary commands. Trusted installation of that
endpoint and the enclosing administrator/storage writer exclusion remain
required; this observer does not provision access, create a privileged inspection
workload or establish that exclusion.

The root-only node installer `scripts/ops/staging_cnpg_observer_install.py`
accepts the exact protected-release observer source/digest and the dedicated
controller public key. It refuses foreign accounts/files, journals its selected
UID before account creation, and preserves pending installation identity across
lost account/publication acknowledgements. The dedicated account has a root-owned
home and a single restricted forced-command key; its sudo rule permits only the
fixed observer wrapper without arguments. That wrapper runs an immutable,
content-addressed source file with isolated Python. Every successful replay
rechecks installed bytes and permissions. Source candidates and transition
records remain available for rollback. This installer does not provision the
controller key/host trust or grant authority to deploy unmerged code.

The primary-volume observer reads the actual claim, all persistent volumes,
CSI attachments and Pods twice. It requires the original Cluster-owned Longhorn
RWO/ext4 claim, its exact PV claim reference and a single attachment on the
original node; it rejects alias PVs, inline Longhorn consumers and additional
Pod consumers. The original primary Pod/container/spec must still match. It
separately binds the observed privileged host-workload inventory. That inventory
does not establish administrator or storage-writer exclusion. The external-input
composer brackets these observations with the same admitted operator process
and binds their digests to the original final plan; the issue-scoped coordinated
maintenance window remains a separate enclosing operational requirement.

The read-only restoration observer combines the exact replacement runtime,
original credential/configuration bindings, PostgreSQL's restored login/schema
state and the complete saved workload inventory at its original ready endpoints.
It brackets workload readiness with fresh process, credential and SQL observations.
Its stable digest contains no passwords. Partial phase records or database-only
completion cannot supply this evidence. The enclosing handoff must still preserve
the original supervised guard and exclude other privileged writers; this observer
does not publish an outcome or retire a policy.

Inside the original active handoff, the journal invokes that observer and flushes
an immutable restoration record only after checking the original guard's retained
acknowledgement before and after observation. Resume repeats live observation even
when the record exists; a lost fsync acknowledgement never supplies durability.
Read-only classification validates the record against all original phase bindings
without writing or flushing it. This historical record alone is not release
authority; the enclosing component still needs the live checks and fence phase.

The internal `retire_application_migrator` cleanup requires the saved transient
role name/OID, original database/postmaster and coordination guard, committed
NOLOGIN/PASSWORD NULL, and closed database admission. Revoking SET membership
does not revoke sessions that already assumed the owner. Cleanup observes
startup locks across databases before session statistics, since a role-wide
login may have authenticated in another database without publishing its backend
row yet. It refuses prepared work and foreign sessions, then retires only the
saved login's exact backends. It removes that
login's database grant and role without deleting owned objects or signalling
other workloads. Same-name replacement, unexpected ownership/grants or owner
membership drift are refusals. Lost cleanup acknowledgements can be reconciled
against the original OID's absence under the same closed-admission boundary.
The protected migration Job/Secret lifecycle must supply that saved identity,
stop its Job, serialize privileged writers, and recover admission and workloads;
this SQL substep alone is not a migration terminal or a cutover entrypoint.

The internal administrator-only runtime-grant helper requires READ COMMITTED,
sealed owner/runtime roles with no memberships, and exact public ownership.
It grants runtime CONNECT/public USAGE, ordinary SELECT/INSERT/UPDATE/DELETE,
and sequence SELECT/USAGE; scoped owner default privileges preserve ordinary
access through later trusted migrations. Migration bookkeeping (`alembic_version`)
is SELECT-only for runtime: it cannot falsify the next trusted migration's starting
revision. The helper rejects residual table or column writes to that marker,
CREATE/TRIGGER/TRUNCATE and table or column REFERENCES authority, and grants no
private helper execution. Catalog
lookups use a protected, restored search path. It supplies neither quiescence
nor object locks: a live caller must already admit and lock its exact shape.

These are independently provisioned profiles, not evidence that every existing
deployment matches them. The comparison helper does not authenticate its own
installation, discover safe role bindings, validate foreign function definitions,
seal credentials, or lock objects. The protected release must admit its installed
code before use.

The internal `transfer_application_ownership` transaction composes these fixed
profiles with exact public-object ownership and the three-definer handoff. It
requires an active READ COMMITTED administrator transaction, sealed application
roles without memberships, no unreconciled ordinary sessions or prepared
transactions, and no foreign role/object authority. Source catalog admission
precedes exact public-table NOWAIT locks and a second comparison; only then may
it read `alembic_version` and require the expected single revision row. Database,
public schema, relation, routine and type ownership move together with ordinary
runtime grants. The result must match the independent sealed reference. Replay
requires that same destination and rechecks the private execution bridges.
Refusal rolls back the complete substep and its locks to a savepoint, preserving
the caller's transaction and settings. Success retains locks until caller commit.

This internal transaction has no deployment caller. The surrounding protected
workflow must first admit the installed release and private guard definitions,
ownership and ACLs; commit credential/membership sealing; reconcile sessions;
and externally serialize administrator DDL. It must also compose durable
operation recovery and migration/runtime credential consumers before live use.
The public inventory cannot substitute for those prerequisites, and successful
isolated transfer tests are not live activation evidence.

The protected runner exposes a fixed PostgreSQL 17 staging peer-psql connection using its
existing kubeconfig and clean environment. It accepts no target, password, or
candidate command. Trusted host-side inventory and transfer code share a narrow
transaction/JSON-row interface with psycopg; the peer transport retains one
backend across BEGIN, savepoints and COMMIT. Public requests accept one SQL
statement and reserve transaction control for the context manager. Qualified
base64 JSON framing preserves positional columns and nested JSON, while raw
database diagnostics are discarded. Input is bounded to 1 MiB per exchange,
combined output to 16 MiB, queries to 30 seconds by default, and the session to
a 300-second deadline. PostgreSQL also has statement and idle-transaction
timeouts. Protocol corruption, EOF or timeout makes the channel unavailable;
PostgreSQL 17 transaction-timeout termination leaves the outcome UNKNOWN, not
an inferred rollback.

Both fixed staging commands (application and maintenance databases) set libpq
startup `PGOPTIONS='-c event_triggers=off'` before executing psql. The peer
checks that this setting came from the client, refuses all existing event
triggers, then restores and verifies normal event-trigger handling before
returning a usable connection. Existing DDL policies are not silently bypassed:
their presence causes refusal, and no policy is deleted or disabled in catalogs.
The generic transport also supports PostgreSQL 16, which has no LOGIN event
triggers or `event_triggers` setting; the fixed staging entry points require 17.
This ordering depends on the admitted fixed command and serialized administrator
DDL. A caller-supplied process that changed its setting after connecting cannot
obtain retrospective startup proof from the handshake.

The connection records the PostgreSQL system identifier, server start, backend
PID/start, database OID/name and session identity. Killing or reaping the local
process is not remote-retirement evidence, and a lost COMMIT acknowledgement
leaves an unknown outcome. The durable protected workflow must reconcile that
exact backend and database state before restoring credentials. This runner
entry point is not wired into deployment ownership transfer yet and does not
replace any of its release, guard, sealing, serialization or recovery prerequisites.

The internal `seal_application_login` phase requires a fresh idle PostgreSQL 16/17
administrator connection and owns its READ COMMITTED transaction. It validates
the exact database owner, ordinary role attributes, absent memberships and
application-only role dependencies. Visible uses of that role in another
database or its prepared transactions cause refusal. A membership-free legacy
owner may initially have INHERIT; the seal explicitly commits NOLOGIN,
NOINHERIT and PASSWORD NULL together and verifies that exact target state.
It supports pre-transfer replay, restores session settings and rolls back all
three attributes together on failure. It neither alters memberships nor signals
backends, creates roles or transfers objects. Its caller must first journal
recoverable credential/Secret intent and serialize credential and administrator
DDL writers. No deployment caller invokes this phase yet.

Committed login sealing is not a session-drain certificate. Real PostgreSQL
coverage proves an existing owner connection retains DDL authority, and a login
paused during startup can become visible after sealing and a fresh empty
`pg_stat_activity` check. PostgreSQL 16 and 17 check LOGIN before publishing normal
backend statistics. Its PID-based signalling also has a documented PID-reuse
race; matching a prior backend start timestamp does not make termination exact.
A protected startup-admission barrier and owned-workload shutdown must therefore
precede quiescence claims; unknown or foreign sessions must not be killed as a
shortcut. Empty session observations and local peer-process cleanup cannot
replace that evidence.

The internal `application_database_admission` helper supplies a scoped normal-client
admission barrier, not a complete maintenance workflow. A separately fixed staging
maintenance peer connects to `postgres`, while the existing handoff peer remains
connected to `loom`. Capture requires an initially open application database,
sealed ordinary source/successor roles, and the exact administrator handoff backend.
It binds PostgreSQL system identity, database/role OIDs, and the handoff's server
start and backend PID/start; timestamps compare as instants, independent of session
timezone. The peer transport fixes ISO date formatting before recording identity.
The caller must persist that captured target in its protected operation intent
before committing `ALLOW_CONNECTIONS false`. Recovery cannot adopt an already
closed database or replace the saved target with a new observation. Reopening
accepts only the recorded source or successor owner and restores the captured open
state through the maintenance peer, including after lost close acknowledgement.

Under externally serialized admission/role/database DDL and protected owned-client
shutdown, the drain helper checks target database startup `RowExclusiveLock`s
before clearing the statistics snapshot and requiring only the exact handoff
backend, no prepared transactions, and still-closed admission. This ordering avoids
missing a startup between its lock release and normal statistics publication.
It never signals clients. All admission writers, including legacy database owners,
must remain contained throughout; closed state at two observations cannot exclude
an intervening reopen/close. PostgreSQL background workers, including autovacuum,
can override normal admission, so this check does not exclude later privileged or
background activity. Those maintenance concerns remain caller prerequisites.
Real PostgreSQL tests cover late startup, surviving clients, identity drift,
transaction rollback, lost acknowledgement recovery, and complete ownership
handoff/replay while closed. These tests are not fleet activation evidence.

Connection-loss recovery has a distinct non-signalling drain check,
`require_application_database_recovery_drained`. It uses the saved target and
handoff identity but requires the old handoff to be absent, together with every
other client except an explicitly captured rollout guard. It retains the same
startup-before-statistics ordering, sealed roles, closed admission and prepared
transaction checks. A server restart or missing/replaced guard refuses recovery;
this does not reacquire lost coordination. The check neither opens the database
nor creates or journals a replacement handoff connection. Those operations still
require an independently admitted, durable recovery sequence. Normal transfer
drain continues to require its exact live handoff backend.

For an independently authorized recovery edge,
`reopen_application_database_for_handoff_recovery` performs that loss-drain and
reopens admission in one maintenance transaction. It checks the saved guard again
after the change; detected loss rolls the change back. Both application roles
remain `NOLOGIN`, so reopening does not restore application execution. An
already-open database refuses rather than being adopted as a successful retry.
The durable caller must reconcile an uncertain commit, admit and record the new
peer, then close and drain again before transferring ownership. This helper does
not supply that durable replacement sequence or relax the surrounding workload,
DDL, process and continuous-guard admission requirements.

After reading its saved recovery intent, the caller can reconcile an uncertain
reopen using `reclose_application_database_for_handoff_recovery`. It checks the
same postmaster, sealed roles and saved guard before changing admission and
rechecks the guard before commit, including on an already-closed retry. It
neither adopts nor terminates surviving peers. A subsequent loss-drain must
exclude all unknown peers before reopening; a recorded replacement instead
requires the ordinary exact-peer drain before transfer.
Reclosure always performs the catalog update, even when committed admission is
already false: an interrupted maintenance backend may still have an uncommitted
reopen. The update must serialize behind that transaction or fail with the
bounded lock timeout; a stale closed snapshot alone cannot certify cleanup.

The database completion sequence now composes the guarded transfer, reopening
and original-password login restoration. `complete_application_handoff_database`
requires the saved target, exact current handoff peer, original guard, original
credential and fixed schema ACL profile. A sealed retry first serializes closure
against an uncertain reopen and verifies the drain. Before validating or performing
ownership transfer, it also checks startup locks across databases, prepared work,
and surviving client sessions outside the application database. Only the exact
handoff peer, original guard and current maintenance peer are exempt; an idle
foreign client is not evidence that its queued work has retired. Refusal never
signals those clients. Native background/replication processes still require the
enclosing admitted SQL profile and manager/external-writer exclusion.
Reopening then requires committed successor ownership and
checks the original guard before and after the admission change. Login restoration
also checks that guard inside its transaction. An already-restored retry only
observes the exact ordinary login and trusted schema; it never reseals the account
or overwrites an unexpected password to force recovery. Lost commit acknowledgements
at transfer, reopen and login are reconciled from actual PostgreSQL state.
The returned database outcome is not a component terminal or proof of CNPG or
workload recovery. The installed caller must retain its journaled authority,
process/DDL exclusion and workload containment, and open any replacement peer
through the existing bounded recovery chain before invoking this sequence.

The protected staging command runner binds this sequence to the active original
component journal. It requires acknowledged guard retention, the saved database
and coordination identity, an observed manager executable replacement, and no
pending peer publication. The selected peer is exactly the original or latest
journaled replacement. Credentials are recovered from the original backup and
matching live Secrets; callers cannot supply passwords or change staging's role
bindings or schema ACL profile. This database-only step publishes no component
terminal and cannot release the retained guard. The enclosing component still
must prove harmful manager SQL retirement, external writer exclusion and workload
recovery; the executable replacement receipt supplies none of those proofs.

Lost-peer database completion has a distinct fixed runner operation. It publishes
or resumes the next bounded peer-recovery intent before connecting. Maintenance
observes the saved server and guard, rejects surviving or unknown privileged
peers, and routes sealed recovery through reclosure, loss-drain and temporary
reopening. An already-restored login permits only a replacement connection for
full schema/credential verification. The observed replacement is journaled before
completion SQL. A successful verified database outcome keeps admission open;
uncertain failure still attempts guarded reclosure, which refuses an already
restored login instead of silently resealing it. The original sealed-peer context
continues to reclose unconditionally and must not enclose runtime restoration.
Neither operation publishes the whole handoff's component terminal.

The installed staging mutation guard is a necessary surviving connection, not an
application worker to stop: it holds the database-local advisory lock that excludes
lifecycle GC. Moving that lock to the maintenance database would lose coordination.
`capture_application_coordination_guard` can bind this one already-admitted guard
before closure: PID/start, PostgreSQL system/postmaster/database identity, fixed
`loom_rollout_readonly` role OID/name, and the operation-derived application name.
Capture and subsequent checks require its exact granted database-local advisory
lock, including the single-key `objsubid=1` identity. Names, readonly defaults and
snapshots do not authenticate the program or prove uninterrupted lock ownership;
the enclosing operation must admit effective role privileges, the installed guard
process and its supervision. Its existing post-readiness loop never reacquires a
lost lock and uses catalog-only health queries compatible with handoff table locks.

The application handoff journal also supports an explicit guard-retention
handshake. Before any login seal, the active `application-ownership-handoff`
component publishes its intent and exact original guard identity under the
existing request. The supervised guard durably acknowledges that request before
the component may proceed. Publication alone cannot win a race with guard
shutdown. Once acknowledged, rollout-owner exit or an ordinary stop request does
not release the lock while the handoff remains pending. Only the same component's
validated terminal at the expected mutation epoch restores ordinary release
semantics. The terminal must represent the complete database, credential,
workload and input-fence outcome; retention itself supplies none of that evidence.

The initial database phase composes original backup credential recovery, journaled
owner creation, guarded login sealing, immutable admission capture and guarded
closure through the fixed runner's retained peer transport. A bounded read-only
comparison with the trusted legacy `staging-readonly` schema profile precedes any
owner creation or login change. Login sealing compares the current SCRAM credential with the recovered
original before altering the role. Both sealing and closure check the exact
original peer and coordination guard inside their SQL transactions, including
after mutation; detected authority loss rolls that transaction back. Closure
replay checks the guard even when the database is already closed. A committed
owner or seal with a lost acknowledgement can recover before admission capture.
After the target is recorded, preparation never recaptures a closed database or
adopts a different peer. It refuses re-entry after manager replacement, peer
recovery or workload restoration has begun. This phase still relies on enclosing
process/input/writer admission and supplies no client-drain, ownership-transfer,
workload-recovery or terminal fence-release evidence.

Broker and worker resume can select that same original guard only when the
acknowledged pending component matches the exact advanced-epoch recovery plan,
attempt, candidate, tree and original starting epoch. They compare the full live
supervised guard evidence and separately require the database epoch at starting
plus one. A fresh nonce challenge through a private runtime mailbox asks only the
original guard's existing connection for that epoch, with lock checks before and
after its fixed SELECT. It opens no new database connection while admission is
closed. Stale replies, changed identities, unknown request fields and lock loss
refuse; a bounded SELECT timeout preserves a still-healthy guard for retry.
A failed resumed launch cannot release the retained guard. A worker still records
its failed or cancelled attempt when guard release is refused for pending handoff,
so another resume can use the same journal. It cannot record success while that
retention remains pending; the refusal does not prove the guard is still healthy.
Ordinary resumes still acquire their own guard; completed retention restores those normal
semantics. The installed composition still needs admission recovery before any
fresh database connection when the application database is closed; guard selection
alone does not provide that recovery or complete the ownership handoff.

The journal has a separate pending-handoff recovery entrypoint that accepts the
original complete chain and executes only its existing handoff ordinal. It
requires the acknowledged original guard, stored plan, advanced epoch terminal,
unchanged component intents and existing execution lock. It never creates a
missing operation or calls surrounding component classifiers while admission is
closed. Failure, an unexpected observed epoch or incomplete convergence preserves
retention without a terminal. Installed preflight still needs the complete
handoff component and live authority checks before using this entrypoint; the
journal method itself cannot restore database access or authorize a cutover.

The manager and systemd stop transport both refuse pending retention, including
cleanup of a failed launch. Orphan reconciliation retains the lifecycle CronJob
freeze even if the guard has died. Lost locks, deadline expiry and missing or
changed acknowledged records fail closed without restoring the CronJob or
reacquiring a guard. This preserves containment, not continued recovery authority
after the original guard is lost. No deployed handoff component selects the
handshake yet; complete handoff composition and admission remain required.

With this immutable optional binding, drain requires exactly the handoff backend
and that guard; it still refuses extra readonly/admin sessions, startup activity,
prepared work, missing/replaced guards and lost locks. Ownership transfer accepts
the same guard only together with the exact still-closed admission target. The
Python transaction boundaries and the fixed definer-handoff SQL both recheck the
guard; no generic session allowlist is accepted. Without the optional binding,
the existing no-guard behavior is unchanged. Real PostgreSQL 16/17 tests exercise
guard health while application locks remain held, ownership transfer/replay and
loss refusal. The isolated ownership fixture establishes its guard session and
then restores the original ACL profile; it does not certify the installed readonly
role's additional ACLs against that profile. Those live grants still require exact
schema admission. This does not wire the deployment handoff or admit live CNPG writers.

The protected component journal can now persist the full non-secret admission
target and handoff identity under its existing component intent. The immutable
`application-admission.json` record is bound to that intent, with no separate
release authority or caller-selected path. Reads and writes require the active
apply callback's process/thread; scope ends before failure diagnostics run.
Retry validates exact fields, private file ownership/mode, and intent binding,
then flushes both record files and their component/journal directory entries
before returning. Visible-but-not-durable publication is not accepted as recovery
evidence. The enclosing attempt directory remains admitted by the outer plan store.
Real PostgreSQL coverage composes journal publication, lost close acknowledgement,
new journal/maintenance connection, saved-target reopening and final-state replay;
publication failure leaves normal connection admission open.

Lost-peer recovery now has a bounded append-only journal sequence under that same
active component. `application-handoff-NN-intent.json` binds its explicit ordinal
to the digest of the original record or preceding peer receipt, and must be
durable before reopening. `application-handoff-NN-peer.json` binds the replacement
identity to that intent. The original admission/guard record is never overwritten.
Reads validate and flush the complete chain, rejecting gaps, orphan receipts,
unknown entries, changed bindings and successors after an unresolved intent.
At most sixteen recovery attempts are recorded; exhaustion refuses further
reopening rather than discarding history. Retrying an ordinal does not allocate
another attempt. A peer receipt cannot reuse an earlier backend, change the
server/database identity, or substitute the guard. These are identity records,
not proof that a peer remains alive, admission is closed, or transfer is safe.
The caller still admits the actual privileged process and recloses/exact-peer
drains before transfer; it must not adopt a connection discovered during recovery.

The fixed runner's `recover_staging_peer_database` composes these records and
database primitives inside the matching active component/plan. It requires the
original staging target and guard, accepts only an unresolved or new successor
ordinal, and durably records intent before reopening. It first recloses uncertain
admission, proves the lost-peer drain, opens the fixed privileged peer, records
its actual identity, then recloses and exact-peer drains before yielding it.
An unknown surviving connection blocks recovery without being adopted or signalled.
Cleanup opens a fresh fixed maintenance channel because a lost COMMIT response
may poison the original channel; it uses the same guarded, serializing reclosure.
Cleanup failure remains a failure, not a safe terminal outcome. PostgreSQL tests
exercise intent/receipt interruptions, dropped COMMIT response after observed
commit, pending maintenance transactions, unknown peers and successor retries.
The yielded scope permits only sealed, closed-database handoff work, not LOGIN
restoration or release. Independent workload/DDL/process exclusion and continuous
supervision of the original guard remain prerequisites.

This does not yet compose credential recovery, owned-workload shutdown, CNPG
process retirement or the full safe-outcome lifecycle. No deployed component
invokes this recovery sequence. A future handoff
component must classify recoverable intermediate state as READY and reconcile
inside apply; transient closure must not become a separately certified terminal
that contradicts the eventual reopened state. Exact completion requires the full
ownership/credential/workload outcome, never merely open admission. Passwords do
not belong in this non-secret component record.

Records with the optional coordination guard use schema 2 and durably retain its
complete identity with the admission target. Existing schema-1 no-guard records
retain their exact encoding and cannot be upgraded during replay to admit a new
session. A guard-bearing record cannot drop or replace its guard either. Recovery
uses the saved binding; it never recaptures a replacement after admission closes.

The internal `restore_application_runtime_login` phase enables the former owner
as an ordinary runtime login only after the independent sealed-owner application
profile and application-only role scope match. It checks the saved system identity,
database/role OIDs, recorded successor ownership, reopened admission, sealed owner
attributes and absent memberships. It owns a fresh administrator READ COMMITTED
transaction; changed identity, schema/grants, or an unexpected existing credential
refuses without enabling login. It generates SCRAM-SHA-256 locally with a fresh
salt and sends the sensitive verifier, never plaintext, in the role update.
Literal passwords resembling stored verifiers are still treated as passwords.
Replay verifies the same credential against the stored SCRAM keys and does not
rotate the verifier, including after a lost commit acknowledgement. This phase
currently accepts nonempty printable non-space ASCII credentials up to 1024
characters; unsupported credentials refuse rather than being normalized.

`observe_application_runtime_login` uses the same identity, authority, schema and
credential checks in an explicitly read-only transaction. It distinguishes a
still-sealed runtime from an already-restored original login without executing
role DDL. This lets recovery reconcile a lost restoration acknowledgement without
rotating credentials or treating open database admission as completion. Either
state requires the exact separated ownership and trusted ACL profile; drift
refuses. This is only the database portion of handoff evidence: workload recovery,
original-guard continuity and safe fence release still require the enclosing
protected component.

Admission and ownership transfer retain strict password-absence defaults. An
explicit `runtime_password`, recovered from the protected original credential,
allows only a matching bounded SCRAM verifier on the still-`NOLOGIN` former
owner/runtime role. It does not permit LOGIN, membership, excess privileges, or
a password on the successor owner. The credential is checked at admission
capture/close/drain/reopen and before/after ownership transfer, including replay.
Runtime grant SQL permits this mode only for a NOLOGIN runtime; its protected
caller supplies credential verification, while SQL retains all owner/ACL checks.
Restoration may enable a NOLOGIN role already holding the same original password,
but refuses an unknown password without replacing it or enabling login. Plaintext
credentials are neither SQL arguments nor admission-journal fields.

This narrow mode accommodates an independently admitted password-only refresher:
CNPG 1.25.1's application-password reconciler can run after an instance restart
or Secret resource-version change even with no managed roles. That path changes
PASSWORD only, not LOGIN or ownership. Real isolated tests exercise its statement
shape between phases and from a second backend during the handoff transaction;
they do not establish live CNPG writer admission. The protected operation must
still bind CNPG configuration and the unchanged original Secret, exclude all
other credential/role/DDL writers, and admit any managed-role/database controllers.
Passing a password to these primitives is not that authority. In-memory CNPG
Secret caching or pausing the main operator alone is not writer exclusion.

CNPG-specific original-credential recovery refuses MD5-shaped literals and the
reserved `SCRAM-SHA-256$` syntax, including leading `$` characters. PostgreSQL's
stored-secret parser accepts leading delimiters and stores recognized verifiers
verbatim even with `password_encryption=scram-sha-256`; CNPG's refresh would
therefore change authentication away from the original literal password.
Refusal occurs before live Secret reads, durable credential binding, or handoff.
It does not normalize or rotate the credential. The generic restore primitive
still supports verifier-shaped literal passwords in contexts without that raw
password writer. Real PostgreSQL 16/17 tests demonstrate successful literal
authentication before CNPG-shaped refresh and failure afterward. This check does
not establish CNPG image/configuration admission or its effective server-side
SCRAM encryption settings; those remain required before live handoff.

Protected backup already exports the application `loom-secrets` alongside the
capacity credentials. New protected Secret inventories use schema 2 and also
record explicit presence or absence of the optional staging
`loom-postgres-cnpg-credentials` Secret. Present objects must agree across two
reads, including UID, resource version, and canonical payload. The backup retains
the private canonical payload and its digest, not live UID/resource-version
evidence for a later handoff. Schema 1 retains its original six-identity contract:
it remains readable, but says nothing about CNPG presence. A CNPG handoff must
require schema 2 with that credential present, plus fresh live identity admission;
generic backup validation alone does not establish either condition.

Rehearsal accepts CNPG credentials only as a `kubernetes.io/basic-auth` Secret
with exactly `username` and `password`, and replaces both with isolated rehearsal
values. Unknown fields, missing credentials, or another Secret type refuse.
This clone is not an original-credential recovery source. Neither inventory
capture nor rehearsal cloning changes a live Secret or the CNPG operator's role
reconciliation. CNPG-managed role memberships and credential writers still need
explicit admission before an ownership handoff.

The internal `recover_application_runtime_credential` phase binds the original
application/CNPG backup credentials and current live Secret identities to an
active protected component intent. It checks the exact final plan, original
manifest digest and complete checkpoint component map, and requires observed
CNPG presence. Full backup validation, including the PostgreSQL payload, runs
between trusted reads of the manifest, inventory and selected Secret files;
changed content, file identity or ACL refuses. Recovery does not reinterpret an
aged-out original lease as permission for a new operation: initial admission
still belongs to the existing restore-verified lease and final-gate workflow.

Both credential sources must identify the fixed staging `loom` application
role with the same supported password. All direct URLs and every present pool
URL must match those credentials, database `loom`, and their separate fixed
staging PostgreSQL/PgBouncer endpoints. Percent decoding occurs exactly once,
preserving literal plus signs; unescaped password `@` delimiters refuse because
SQLAlchemy interprets them differently. The current query contract admits
`sslmode`, bounded `connect_timeout`, and a bounded `application_name`; routing,
role, password and unsupported query overrides refuse before live reads. This
does not claim compatibility with arbitrary operator-provided connection options.

Before reading live credentials, recovery also calls
`capture_cnpg_writer_configuration`. Within the same active component intent it
checks the fixed staging Cluster, complete Database/Pooler/Publication/Subscription API inventories and
monitoring ConfigMap twice. The supported declared-SQL-writer profile requires
the fixed application bootstrap/credential references, disabled superuser password
access, no managed roles or pooler integration, no target Database/Pooler/Publication/Subscription CRs,
and only reviewed PostgreSQL parameters. Custom bootstrap SQL, preload libraries,
extension settings, plugins, unknown Cluster fields and monitoring Secret sources
refuse. Monitoring SQL must match the independently pinned upstream CNPG 1.25.1
`config/manager/default-monitoring.yaml` queries, not an adopted live digest.

Declared-writer reads use fixed raw Kubernetes API endpoints to retain list
resource versions and pagination metadata; incomplete lists refuse. Foreign
clusters' resources are neither changed nor adopted. The journal persists the
Cluster UID/generation, monitoring UID/resource version and configuration digest
in `application-cnpg-configuration.json`, subordinate to its component intent.
Publication and retry must be durable before proceeding. Changes to these inputs
refuse across recovery; unrelated Cluster health/status resource-version changes
do not. Other known Cluster fields are fingerprinted, not semantically certified.

This declared-input check is not complete controller admission or a Kubernetes
write fence. It does not prove running operator/instance image and process
identity, trusted volumes, effective SQL settings, extension absence, or retirement
of already queued role/database/extension actions. In particular, CNPG can drop
an installed managed extension even when its configuration no longer requests it.
The outer handoff must still serialize configuration/Secret/admin writers and
admit or retire controller work before changing database authority. Desired image
tags and repeated observations cannot replace those checks.

`render_cnpg_input_fence` renders five intent-named, fail-closed Kubernetes
ValidatingAdmissionPolicy/Binding pairs for a temporary staging input fence.
They deny application/CNPG Secret and monitoring ConfigMap writes, and mutations
of Database/Pooler/Publication/Subscription objects that refer to `loom-postgres` before or after the
request. The target Cluster cannot be created/deleted or change spec, labels,
finalizers, or role/pooler writer status. Ordinary health/primary status updates
and unrelated clusters/namespaces remain allowed. All annotation changes,
including a CNPG restart request by the installed administrator, are denied.
Restarting PostgreSQL beneath the handoff would destroy its surviving database
guard and peer identity. Retirement that replaces the postmaster must therefore
finish before acquiring the surviving guard, under separately established
preparation authority; this fence does not provide that preparation lifecycle.
Kubernetes returns an explicit Forbidden denial even to an otherwise
RBAC-authorized administrator.

A separate policy denies the target Cluster's `/scale` endpoint and the `/scale`
endpoints of explicitly inventoried target Poolers. Scale requests contain a
`Scale` object, not the parent's cluster reference; the caller must bind a complete
target Pooler name inventory and retire pre-fence API writes. The declared-writer
handoff profile rejects all target Poolers, so its renderer call must explicitly
pass an empty tuple. Foreign Pooler scaling remains allowed. CNPG 1.25.1 also
registers Publication and Subscription reconcilers inside each instance; both
their objects and status are covered, not just Database and Pooler controllers.
The expanded declared-writer profile is version 2 so a saved narrower binding
cannot be silently reused.

The renderer is not installed automatically and does not authorize live use.
The outer protected operation still must durably bind exact policy/binding UIDs,
verify enforcement, exclude their removal and other holders of the same API
principal, and reconcile installation/removal interruptions. It must also retire
pre-fence API requests and cached controller actions, admit actual processes and
images, and preserve the fence until a safe database/workload outcome is known.
The policy does not block direct privileged SQL, pod or operator replacement,
or protect itself against an administrator changing admission authority. The
Docker integration lane tests real enforcement in a disposable Kubernetes API,
not live CNPG process retirement or full fleet activation.

The fixed staging primary admission reads Pod ownership, node, container and
bootstrap image digests before executing its read-only process probe. It refuses
extra containers, environment entries, executable hooks, unreviewed probes and
volume sources, then checks both running/stored manager and PostgreSQL bytes.
The PostgreSQL executable hash comes from the never-started pinned image. Pod
specification, process start ticks and executable device/inode observations allow
the enclosing handoff to detect changes without adopting a successor primary.
Replicas are not execution targets for this probe.
The component journal binds the original Cluster UID, Pod specification, manager
and postmaster before application mutation. Recovery re-reads the same primary
and accepts only its already-issued manager executable transition. A changed
postmaster or input is refused before a receipt is published. An unchanged
manager after dispatch remains pending. Read-only classification can inspect
this original binding without publishing or flushing records.
Credential and declared-writer observations are also available without journal
mutation. They read the same verified backup, compare live Secrets twice, and
validate CNPG configuration twice. Their results expose only non-secret bindings
in the recovery view; the recovered password remains excluded from diagnostic
representations. Active recovery separately publishes both immutable bindings
before any use of the credential for SQL mutation.

Effective SQL admission separately checks each supported connectable database
(`loom`, `postgres`, `template1`) against the original PostgreSQL 17.4 server.
It checks active and pending executable settings, role settings, extensions,
publications/subscriptions, privileged roles and native C definitions. Native
function references come from the pinned image's initialization files; the
PL/pgSQL and Snowball scripts contribute their exact signatures while their
assigned OIDs are normalized. The maintenance databases must have no user
relations or functions. CNPG's inert configuration checksum and its reviewed
archive/restore commands are supported. These read-only observations do not
establish administrator exclusion, retire cached controller or server requests,
or supply full handoff completion. The enclosing component must retain those
boundaries and the original guard through actual SQL and workload recovery.

Pinned CNPG 1.25.1 also supports an in-place instance-manager executable replacement
that adopts the existing postmaster. The disposable Docker integration test uses
the exact upstream release manifest checksum and immutable operator/PostgreSQL
images. Before replacement it compares both `/proc/1/exe` and the stored
`/controller/manager` bytes against the regular-file executable extracted from a
never-started container of the pinned operator image. It does not derive trusted
bytes from mutable Cluster status or the target volume; either executable mismatch
refuses before the replacement transport. Its control proves that removing a managed role from the desired Cluster
does not retire an already queued role change. Its replacement case observes a
different `/proc/1/exe` inode with identical executable bytes, disappearance of the
old queued role backend in the `postgres` database, preservation of NOLOGIN, and
the same original application backend, postmaster start time, and advisory lock.
An HTTP EOF or replacement of `/controller/manager` alone is not success evidence.

A separate disposable startup case seals the application role with `NOLOGIN` and
clears its password, then removes login/replication rights from the streaming
role. After exact manager replacement, CNPG restores the application password
from its unchanged Secret and restores streaming login/replication rights. The
application role remains `NOLOGIN`, and the original backend, postmaster and
advisory lock survive. No password or hash is emitted by the test. Thus replacement
is not SQL-writer silence even with no managed roles: startup loses the manager's
in-memory Secret-version cache and replays credential reconciliation. These
effects must be included in startup admission and credential recovery; clearing
a password alone is not a durable admission seal.

The startup case also exercises closed application-database admission: it commits
`ALLOW_CONNECTIONS false` through the separate `postgres` maintenance database
and verifies closure before replacement. The target database cannot close itself;
PostgreSQL rejects that command with SQLSTATE `22023`. Test peers use
`ON_ERROR_STOP` so a later success marker cannot hide failed setup or mutation SQL.
After replacement, admission stays closed, a new privileged application peer is
rejected, and the original backend, postmaster and advisory lock still survive.
This verifies the closed-database replacement mechanism, not complete handoff or
retirement of all other privileged writers.

The protected runner now has a narrowly scoped `issue_staging_manager_replacement`
transport for the pinned amd64 manager on OLDLAB3–5. It requires an active
component's immutable original-admission intent. A private subordinate dispatch
record is flushed before the single upload attempt; an already-issued operation
never uploads again, including after timeout, EOF, or process interruption.
The saved receipt admits only a changed executable inode on the same device,
Pod UID, container, node, restart count and process start time. It does not prove
harmful SQL retirement. Read-only classification can inspect these process
recovery records through an exact plan/component/ordinal/implementation/input
binding, without entering active apply or acquiring publication authority.

Binary capture streams into a private temporary file with the independently
pinned size and SHA-256; the generic command runner's smaller bounds are unchanged.
The upload uses one exact Pod's loopback tunnel, pins the server's public TLS
certificate read over authenticated Kubernetes, and never follows redirects,
ambient proxies, or automatic retries. Process and closed-database/original-guard
checks, plus durable dispatch, precede the final TLS connection: CNPG permits
only three seconds for headers and twenty seconds for the request. The connection
stays open for receipt/EOF rather than discarding potentially buffered TLS data.
Real local TLS deadline tests cover slow validation and journal writes, and a
disposable pinned-CNPG test exercises the actual 61 MB stream while preserving
the original backend, postmaster and advisory lock. These are transport tests;
the full installed handoff component is still required before live invocation.

This mechanism does not supply a live replacement caller or complete retirement
authority. The upstream replacement waits explicitly for log pipelines, not every
SQL/API operation. Other pending SQL, controller/API writers, actual executable
and volume authority, replacement startup effects, and ambiguous-failure recovery
must be admitted separately. In particular, the live Cluster status binary hash
is not an independent executable trust anchor, and fresh instance startup can
reconcile replication permissions and extensions. Preserve ordinary guard-loss
fencing; never reinterpret an uncertain replacement as permission to reacquire it.
This evidence supports investigating a continuous-guard handoff before introducing
a disruptive database restart and a separate preparation-to-new-guard lifecycle.

The protected runner's `probe_cnpg_input_fence` uses only fixed server-side
dry-run requests for the Cluster, dependent Database, scale endpoints, both
Secrets and monitoring ConfigMap. Every request must be rejected by its exact
intent-named policy and binding with the expected Forbidden reason. An accepted
request reports not enforced; unrelated RBAC, validation, transport and timeout
failures raise sanitized errors instead of becoming positive evidence. The
runner disables kubectl user preferences so local CLI settings cannot rewrite
protected commands. Disposable API tests exercise actual kubectl output and
verify that an unfenced dry-run probe does not change the target Cluster.

The existing active component journal can durably prepare the fence request
(target Pooler names and digest of the complete rendered
object set), then record caller-verified per-object UIDs and rendered-object
digests. Both fresh publication and replay flush the files and directories.
Request, renderer, intent, ordinal or UID changes refuse; retries recover the
original request rather than selecting new policy inputs. Request schema 2 has no
restart-authority fields. Legacy schema-1 restart-enabled requests are refused,
not reinterpreted or upgraded in place; existing policies and receipts cannot
silently become evidence for the narrower guarded contract. These subordinate
records are not an installation/removal lifecycle, ownership-adoption proof or
process-retirement certificate. Protected acquisition/recovery must still verify
the live objects, resolve ambiguous create outcomes, exclude policy writers and
permit removal only after a durably recorded safe handoff outcome.

`acquire_cnpg_input_fence` composes the existing journal and protected runner
into create-only acquisition. An authoritative absent read precedes a durable
per-object creation nonce and exact creation-document digest. The nonce is added
to the policy or binding before CREATE. A lost reply can recover only from that
pending record and exact live nonce, spec, metadata and field-management readback;
the resulting server UID is then bound immutably. An unknown existing object is
not adopted, and a known missing, replaced, or changed object is never recreated
or overwritten. Fresh creation and retries verify all identities again after
the exact denial probes and require current, warning-free policy type-checking.
Normal controller status-only server-side apply is permitted, not input changes.
Structured CNPG writer status uses explicitly dynamic CEL values only for
absent-to-empty normalization; real structural-schema API tests retain whole-value
equality and deny nonempty role/pooler changes.

This acquisition helper requires **prior** exclusion of policy/authority writers
and retirement of their outstanding requests. Nonces, managedFields and generation
1 are consistency checks, not authenticated creator identity or evidence that
competing writers are gone. Its CREATE requests must also be retired before later
removal; a timeout or local process exit does not prove server-request retirement.
Acquisition is not wired into live handoff yet and does not authorize policy
removal or database ownership changes. The disposable Kubernetes test drops a
real CREATE reply, resumes from the journal, and verifies preservation of the
original UID and idempotent repeated acquisition without duplicates.

The internal retirement path retains all ten intent-specific object names while
CREATE requests might still be outstanding.
`prepare_cnpg_fence_retirement_patch` prepares an exact UID/resourceVersion/spec
tested JSON Patch that changes only a policy's matchConditions to literal false.
Kubernetes then skips the policy; bindings and every original UID remain retained.
Late CREATEs conflict with occupied names instead of restoring an active fence.
Exact retired generation-2 readback is idempotent, and active acquisition refuses
that retired state. The patch-preparation function alone is not evidence that
database/workload recovery is safe.

The disposable Kubernetes test exercises retirement through the protected runner's
kubectl transport, retains all ten identities, rejects stale patches and delayed
CREATEs, and checks admission propagation across all five policy scopes. That
transport matters: the apiserver JSON Patch library compares encoded scalar bytes,
so a Python client can fail the full-spec test on differently escaped CEL strings
even when decoded specs agree. Kubectl normalizes the encoding; UID, version and
whole-spec preconditions remain intact.

The internal `retire_cnpg_input_fence` executor repeats combined restoration
observation and durable publication, verifies all ten retained identities, and
flushes an irreversible retirement decision before its first patch. The decision
binds the restoration digest and complete original request, CREATE nonces and UID
inventory. Acquisition refuses that decision even before the first policy changes.
Each patch is bracketed by the original retained guard check. Ambiguous replies
propagate; resume accepts exact active or retired endpoints and never reactivates
them. Completion requires complete readback and successful fixed server dry runs
in every policy scope, including saved Pooler scale endpoints. Partial acquisitions
do not qualify. The disposable Kubernetes test runs this executor through actual
kubectl with lost patch replies; its database/process/workload observations are
controlled fixtures, so it is not full installed handoff evidence.
Read-only classification uses the same exact retired-object and propagation
observer against the original journal ordinal. It validates every stored binding
without publishing or flushing records and returns a stable retirement digest.
The component must independently combine this with fresh restoration and original
guard/writer checks before deriving its terminal.
Retained objects must not be pruned by GitOps, later operations or
cleanup; eventual garbage collection still needs outstanding-request retirement
and separate authority. The approach eliminates this operation's delayed-CREATE
reactivation hazard, not the preceding external policy-writer exclusion or CNPG
process-retirement requirements. No live release caller is wired yet.

Two protected reads of each fixed live Secret must match the saved canonical
payload and the same UID/resource version. The journal publishes only source
hashes and live identities in immutable `application-credentials.json`, bound
to its existing component intent. Fresh publication and matching retry flush the
files and directory entries before returning the sensitive original credential;
changed backup/live identity refuses instead of rotating or adopting credentials.
Parser and subprocess failures are sanitized, and passwords are excluded from
the returned object's representation and journal. Secret content remains private.

These phases preserve the application password and URLs through ownership
separation. They do not serialize external credential writers, admit CNPG role
reconciliation or private guard definitions, coordinate all migration consumers,
or stop/restart owned workloads. The full handoff component and its intermediate
recovery branches remain unfinished; no deployed caller invokes these phases.
Real tests
prove restored authentication while schema creation, trigger disabling, table
truncation, migration-marker writes and owner-role assumption remain denied;
these are credential/permission tests, not real-model or fleet-activation proof.

This interception is not complete cutover authority. There is no live freeze
caller: public table-owner credential sealing, private protected-transition
permits and terminal/retry continuity, bound-writer configuration rollover,
durable operation reconciliation, and the other mutation domains must be
completed before activation. A frozen trial domain currently rejects ordinary
trial mutations; installing its schema is not evidence of global autoscaling.

The base guard remains disabled at allocation epoch zero. Its ordinary
prepared bindings are non-executable, and normal submission and claim routes
do not use it to authorize work.

A separate least-privileged executor role can call serializable protected
procedures that prepare an executable intent, bind its exact Slurm job,
register or drain its worker incarnation, admit an exact protected attempt,
project an admitted claim's terminal evidence, and acknowledge terminal worker
release. Claim admission requires the attempt's current protected assignment to
name the same intent, allocation epoch, pool, and shape as the exact registered
worker; an unassigned or cross-intent attempt fails before a lease can persist.
Guard 0020 activation is fail-closed: it requires zero pre-exact-assignment
`executable_claim_leases` rows because guard 0013 stored no immutable
claim-to-assignment transition reference, so a later lifecycle head cannot
prove the claim's exact assigned intent at admission time.
Preparation requires the exact append-only protected bootstrap
registration produced by the trusted demand agent, including its subject,
intent, full execution binding, command sequence, epochs, bootstrap hash, and
receipt digest. These transitions are append-only, bind the subject, candidate
publication, execution fence, worker, requirements digest, and monotonic
high-water marks, and serialize claim admission against terminal lifecycle
projection. A candidate or deployment reconfiguration immediately denies new
claims to workers registered under the prior binding while preserving their
credential-authenticated drain and release cleanup path. The candidate role,
the trusted demand-agent role, and `PUBLIC` have no access to this surface.
Personal-development provisioning creates the executor role sealed as
`NOLOGIN`; no checked-in candidate route, worker route, or deployed daemon
invokes the procedures. Consequently this protected surface cannot consume
manager work or change physical capacity while the global executable ceiling
and deployment entry points remain zero.

Implementation lives under `src/loom_capacity_manager/`,
`src/loom_capacity_executor/`, `src/loom_capacity_pool_executor/`, and
`src/loom_capacity_agent/`. The service, executor, and protected-store
integration suites prove v1/v2 ledger isolation,
exact executable bindings, fail-closed ambiguity, and protected release before
capacity is uncharged.

## Package 5A render-only control-plane foundation

Package 5A packages the single management authority without activating it. A
strict profile at
[`deploy/dev-fleet/capacity-control-plane.toml`](../../deploy/dev-fleet/capacity-control-plane.toml)
renders one independent capacity PostgreSQL instance, one migration/authority
bootstrap Job, one manager Deployment and ClusterIP Service, and
component-scoped least-access NetworkPolicies in `loom-dev`. The manager
release image is published as `loom-capacity-manager` for native AMD64 and
ARM64 and runs as UID/GID 65532.

Release publication preserves source-accurate, immutable evidence. Each native
archive is rebuilt from the protected release commit inside the hosted
`publish` job and is checked by Trivy v0.74.0 using image scanning, OS and
library vulnerabilities, a `20m0s` timeout, `CRITICAL` severity, exit code 1,
unfixed findings included, the vulnerability scanner only, and no cache. A
repository helper writes the fixed config and reviewed ignore file outside the
checkout before each scan. Fixable findings must be removed by updating the
image or dependency. The expiring exceptions cover the three unfixed CRITICAL
Perl CVEs (CVE-2026-13221, CVE-2026-42496, and CVE-2026-8376) only on
the Debian Perl packages required by Debian base runtimes, the agent toolchain,
and the staging-compatible PostgreSQL 17.4 rehearsal image, CVE-2026-43185
only on the agent compiler's
`linux-libc-dev`, and CVE-2025-7458, CVE-2026-6653, and CVE-2023-45853 only on
required PostgreSQL 17.4 rehearsal dependencies. Every entry includes exact
Debian PURL scopes, its review statement, and 2026-09-12 UTC expiration in the
signed predicate. Policy
generation fails closed at that boundary. A repository-owned installer accepts
only the architecture-specific v0.74.0 release archive whose repository-pinned
SHA-256 matches; no policy-forbidden third-party action is required. The signed
predicate binds the scanner identity, release URL and architecture archive
digest, complete policy-file identities, explicit exception metadata, and scan
report. Its only publication mode is `trusted-rebuild`, bound to the protected
release head, tree, ref, and current
run. PR build archives remain local to their untrusted scan jobs and are not
uploaded; the publisher never downloads, loads, scans as release, attests, or
publishes those bytes. Each architecture push contributes its emitted digest
directly to a canonical post-verification record, rather than allowing a later
mutable-tag lookup.

The manifest job accepts exactly the current image's AMD64 and ARM64 records,
validates their release and mode identities, verifies the registry
attestations at the recorded immutable subjects, and joins only those digests.
It records the temporary manifest's creation digest once and performs registry
validation, attestation, and attestation verification through that immutable
digest. The official release SHA and branch tags move only after final
verification succeeds. These publication controls produce the inert Package
5A image; by themselves they do not apply infrastructure, activate authority,
or execute capacity.

The Kubernetes namespace `loom-dev` is the shared infrastructure home, not the
logical shared-development demand subject. The one authority accounts for all
four demand classes: production; staging; shared development (the logical
`development` subject under the `shared-development` account); and personal
development (each `dev-<name>` subject backed by a `loom-dev-<name>` application
namespace). All four share the operator-defined physical OLDLAB/GB10 capacity
according to their tiers and limits.

The renderer requires a digest-pinned manager image and reviewed non-nil
authority UUID. It references, but never creates or prints, the existing
`loom-capacity-manager` Secret. That Secret supplies PostgreSQL identity,
`database-url`, bearer-principal and executor-public-key registries, manager
server/client trust, and the dedicated health client certificate and key. The
exact key contract and evidence commands are documented in the
[`deploy/dev-fleet` operator notes](../../deploy/dev-fleet/README.md).
Only credential-preparation init containers mount that projected Secret. They
copy the bounded, exact key set to mode-0600 UID-owned files on a memory-backed
volume; the migration and manager application containers mount only that
prepared runtime directory, read-only. A held projected-generation descriptor
and pre-install rebinding check prevent a Kubernetes `..data` rotation from
mixing credential generations.

The staging and personal-candidate control planes prepare their separate
protected worker-runtime credential with the same non-root restrictions. The
init container mounts the memory-backed volume at
`/run/loom/protected-worker-runtime-volume` and creates an owner-owned `private`
child, mode `0700` with inherited setgid removed. It does not chmod the
Kubernetes-owned, fsGroup-writable volume root. The control plane mounts only
that child through read-only `subPath: private` at
`/run/loom/protected-worker-runtime`, preserving the `files/database-url` and
staging `files/ca.crt` consumer paths. Secret-copy ownership and exact-mode
validation remain mandatory; no root init or extra capability is required.

When an execution policy is rendered, the same immutable manager image also
runs a byte-transparent TCP router pinned to OLDLAB1. It binds
`192.168.50.103:31443` for the still-ClusterIP-only manager Service and
`192.168.50.103:31432` for the staging CNPG primary. Separate fixed-purpose
containers forward each route without terminating TLS. PostgreSQL ingress
admits only this router namespace and pod selector on port 5432; the installed
manager-policy component owns and creates that rule before starting the router. The one host-port exception lives in a
dedicated namespace because the manager namespace retains Restricted Pod
Security. That router namespace is default-deny, and the pod remains non-root,
read-only, capability-free, and without a service-account token. Both its
NetworkPolicy and the proxy process admit only the 1–8 explicit sorted private
client host routes supplied with the execution policy. The manager admits the
router pod on port 8443, not those source IPs directly. Broader, public,
duplicate, special, missing, or policy-free CIDRs fail before YAML is emitted.
Transport admission does not replace manager-terminated mTLS and independently
bound bearer principal scopes.

Controller SQL admission uses the existing executor role after its retained
issuance component has completed. The installed source derives the URL from
that component's saved credential and binds the current CNPG CA, staging subject
incarnation, configuration, deployment, and candidate generations. The URL
retains the certificate DNS hostname and `sslmode=verify-full`, with a single
`hostaddr` selecting the private relay. Root-owned operation evidence precedes
publication of service-owned private credential and admission files. The public
CA resides under the root-owned release trust directory; both service startup
and pre-enable validation receive its fixed `PGSSLROOTCERT` path. Partial
publication recovers only the exact operation-derived temporary file, preserving
the credential across process death. Exact replay creates no temporary file.
These files remain inert until the separately bound activation operation enables
the active timer.

The activation coordinator binds both complete controller requests to the
prerequisite profile, candidate, credential metadata, controller transports, and
canonical staging subject, including its candidate generation. A private
immutable journal retains those inputs and the exact manager request before
effects. A shared operation lock excludes concurrent activation attempts. Both
prepared timers stop before active files are staged; fresh inventory from both
controllers precedes the one-slot manager transition and active timer enables.
A lost manager reply reuses its original readiness digest and idempotency key.
After a partial enable, compensation validates the saved authority and current
manager epoch independently of failed forward dependencies. Its retained drain
intent forbids reactivation, including after a lost drain reply. These coordinator
checks retire a positively uncommitted prepared epoch if its retained readiness
expires or changes, preserving the abort request across lost replies. A delayed
activation that wins the manager's transition lock is drained instead. A retired
preparation requires a successor preparation; its old activation intent is never
rewritten.

The installed final-gate executor exposes a separate activation operation. Its
source reads the completed preparation journal and published profile, derives
both admission bundles from retained issuance, and builds fixed controller
transports. Recovery reconstructs requests from the private inputs before
opening issuance or desired-configuration sources, so failed forward dependencies
do not prevent an already selected drain. Admission compares the seed's reporter
incarnation after applying the same generation derivation as manager configuration;
the bootstrap seed and retained credential remain unchanged.

Forward observation checks the exact authenticated manager context and existing
resource manifests for the current phase. Prepared execution requires the increase
freeze; active execution requires its release. Signed witnesses must match current
epoch, state and ceiling, while their historical shadow/key digest remains the
immutable prerequisite binding. Only an authenticated immediate predecessor may
wait up to thirty seconds for the asynchronous publisher, with the full manager
context pinned throughout. Changed keys, epochs, expired witnesses and a missed
deadline refuse forward progress.

The installed `loom-staging-rollout-final-gate activate-prepared` command takes
`--plan` and `--plan-sha256`. Initial activation additionally takes `--documents`
and `--documents-sha256`; the private file must be named
`execution-activation-documents.json` beside the exact attempt's plan and contain
both typed pool documents. Recovery omits both document arguments and uses the
retained inputs. The command verifies immutable plan, envelope and artifacts;
the installed forward guard checks checkpoint freshness before every forward
step. Expired backup admission therefore refuses new effects without blocking
saved-authority drain. Output contains the execution context and plan digest;
exit 0 denotes active, exit 1 denotes drain-only, and exit 2 denotes refusal.
It is separate from prepared convergence and does not re-run the rollout's
prepared-only verification after activation. Trusted worker launch material and
complete legacy writer closure remain required before live execution.

The schema migration writes a canonical seed event beside its generated
bootstrap authority UUID. A reviewed replacement requires that one pristine
seed and writes an append-only binding event in the same locked transaction.
Legacy markerless state allows only same-UUID backfill. Duplicate, malformed,
contradictory, or different later reserved evidence fails closed even before a
writer registers. Percent-encoded database
URLs retain their SQLAlchemy meaning: percent escaping occurs only at the
Alembic ConfigParser boundary.
Migration connections have fixed connect, lock, and statement timeouts, the Job
has an active deadline, and PostgreSQL startup is protected by a bounded startup
probe before liveness begins.
The DNS-label-safe, length-bounded migration Job name incorporates the
migration head and manager image digest plus a digest of the canonical complete
Job spec and exact head. Any immutable spec change therefore renders a new Job
instead of colliding with an old template.

The control-plane CLI commands are deterministic `render` and read-only
`status`; executor rendering now includes separately artifact-bound active
config and environment outputs. The status path performs a real in-Pod mTLS
probe. The probe first verifies that the mounted server certificate contains
the `127.0.0.1` IP SAN and the
`loom-capacity-manager.loom-dev.svc.cluster.local` DNS SAN. A policy-enabled
release additionally requires the router endpoint `192.168.50.103` as an IP
SAN. The probe then succeeds only for the exact canonical response
`{"executable_new_capacity_ceiling":0,"status":"ready"}`. The CLI has no
apply, install, start, public-exposure, HTTP-transition, or ceiling-changing
operation. Protected activation/drain/retire live on the manager API, and the
separate controller-local active executor package owns Slurm actuation. Merging
repository support does not authorize a live deployment; apply and activation
remain reserved for #906's explicit operator change window.

## Current activation blockers

There is intentionally no live global fleet manifest. Repository support can
render the manager and prepared/active executor artifacts and exposes protected
activation, drain, and retirement transitions, but it does not apply or start
them. The checked-in
[fleet-state example](../../deploy/fleet-state/README.md) is synthetic. The
diagnostic inventory of the current development, staging, and production
environment copies reports these conflicts:

- `gb10`: allowed nodes, slot/job/concurrency ceilings, per-slot CPU and
  memory, requested/reserved resources, and resource-aware settings;
- `oldlab`: controller and cluster identity, partition, allowed nodes,
  architecture/exclusivity/container settings, slot/job/concurrency ceilings,
  per-slot CPU and memory, requested/reserved resources, and resource-aware
  settings.

Those facts must be measured and reconciled into one reviewed immutable fleet
generation. The manager must not choose an environment copy or merge node
lists implicitly.

The sealed allocation/work queue, protected claim/admission/lifecycle routing,
atomic activation/drain/retire transitions, and active executor package are
implemented. Live use remains blocked on reviewed real-fleet evidence, exact
fenced OLDLAB and GB10 executor installation, live personal-lifecycle
convergence, mixed-workload containment tracked by issue #896, GB10
health/capacity convergence, and the evidence and explicit operator window
tracked by issue #906. Until those activation-boundary gates pass, the global
manager remains undeployed and inert at zero; existing environment-local
OLDLAB and GB10 autoscalers remain the live writers.

## Verification

The capacity gate runs contract, state, topology, allocator, store, API, mTLS,
property, migration, and offline-driver tests; Ruff; strict Mypy; compilation;
whitespace checks; and a scheduler/process/path source audit. Integration tests
prove v1 isolation, exact current-allocation fencing, crash recovery,
authority-first release replay, hostile-search-path safety, direct-SQL guards,
and upgrade/downgrade/re-upgrade parity. Deployment tests separately prove the
checked-in Package 5A remains at a zero executable ceiling.

## Protected executable bridge package

The executable-v2 package has one global manager spanning production, staging,
shared development, and personal-development subjects, and exactly one
controller-local executor for OLDLAB and GB10. Users do not configure pool
weights, QoS, shapes, profiles, or priorities: `min_slots` defaults to zero,
architecture-specific demand constrains eligibility, and neutral placement is
manager-owned. `loom-dev` is shared infrastructure; personal namespaces are
`loom-dev-<owner>`, never `loom-dev-shared`.

The checked-in executor profile has immutable images and an exact zero
executable ceiling. A positive runtime is a separate owner-reviewed artifact
bound to the exact active execution context and approved launch profiles.
Rendering and systemd validation are non-installing; no merge authorizes
activation or live infrastructure mutation. The manager's v2 status may report
exact active physical Slurm-job intent, but only a matching fresh protected
personal guard registration, with no later release/drain, can make a worker
available. Scheduler evidence and pod readiness alone are insufficient.
