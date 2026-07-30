# Developer-sandbox Slurm policy

The current scheduler membership is registry-owned and dynamic. Every active
developer environment receives its own `loom-dev-<runtime-id>` account and
non-login Slurm user from its registry record; there is no fixed developer
allowlist or upper environment count. Live cohort acceptance requires at least
two active committed environments, while normal create/update/check/destroy
continues to operate on the exact environment and deployment IDs.

The installed isolated-Python capacity authority is the supported persistent
surface for per-deployment reconciliation and readback:

```bash
sudo /usr/local/libexec/loom-developer-environment-capacity-authority \
  reconcile --deployment-id <DEPLOYMENT_ID>
sudo /usr/local/libexec/loom-developer-environment-capacity-authority \
  check --deployment-id <DEPLOYMENT_ID>
```

It reads the root-owned registry snapshot, derives the environment's exact
account/user/QoS/resource generation, and rejects caller-selected profiles,
paths, accounts, and capacity overrides.

Reconciliation is incremental. Adding a fourth, fifth, or later environment
may create only that environment's derived account, association, guard
binding, adapters, and units; it must preserve every existing environment's
candidate, account, association, handoff bytes, lease epoch, unit state, and
running job IDs. Removing an environment waits only for that exact
environment's jobs and owned resources to become terminal. Peer environments
do not have to drain, and the authority never cancels, preempts, stops, or
kills foreign or peer jobs.

The canonical scheduler and host-containment inputs for the two shared
capacity domains live in:

- `deploy/slurm/developer-sandboxes/oldlab.toml`
- `deploy/slurm/developer-sandboxes/gb10.toml`

They describe two independent Slurm clusters. Never submit a GB10 request
through the OLDLAB controller or add GB10 nodes to the OLDLAB inventory.

Both domains cover the complete 20-node infrastructure fleet: five OLDLAB
nodes and 15 GB10 nodes, including `trt-gb10-7`, with
`excluded_nodes=[]`.

## Legacy-v1-only static profile and historical acceptance workflow

Everything from this heading to the end of this runbook that names
`qianyi`, `hongjian`, or `devansh`, assumes exactly three child accounts, or
uses `--sandbox all --candidate-sha <QIANYI_SHA>` is retained only to recover
or interpret an existing `legacy-v1` fleet transaction. It is not the current
membership contract and must not be used to exclude a newly registered
developer environment. Six lanes and 33-checkpoint references in the linked
historical evidence have the same legacy-only scope.

## Admission boundary

Non-exclusive Loom jobs remain disabled until all of these conditions pass for
the exact merged candidate:

1. the cluster uses `proctrack/cgroup`, `task/cgroup`, cgroup job accounting,
   enforced associations/QoS, and multifactor fair-share;
2. CPU, RAM, swap, and device controllers are constrained and delegated;
   opted-in Loom jobs also carry the reviewed finite PID ceiling in the exact
   `loom-cgroup-v1:pids=<N>` Slurm comment;
3. the Docker daemon selected by the Loom worker uses the `cgroupfs` driver so
   an absolute Slurm job-cgroup path can be passed as `CgroupParent`;
4. the three child accounts have equal fair-share under the aggregate
   `loom-dev` budget, with no small per-developer resource ceiling;
5. every worker, trial, verifier, and sidecar is a strict child of the finite
   Slurm job cgroup and the aggregate caps stay within its allocation; and
6. the candidate-bound acceptance artifact passes
   `scripts/ops/nonexclusive_slurm_acceptance.py verify`.

The host Docker cgroup driver cannot be changed while jobs, containers, or GPU
compute processes are active. The supported host converger takes a
candidate-owned Slurm drain for one node, waits for all three activity classes
to clear, applies the checked-in profile, restarts Docker and `slurmd`, and
reads back the effective settings before moving to the next node. A foreign
DRAIN/DOWN is preserved and blocks the transaction. The controller is changed
last. Do not edit `/etc/slurm`, `/etc/docker/daemon.json`, or SlurmDB
associations by hand.

Both target clusters currently run Slurm 23.11.4. In that release, the root
Prolog runs outside cgroups and runs before `PrologFlags=Contain` creates the
extern step, so a Prolog cannot safely set the eventual job cgroup. The
converger therefore installs and enables the persistent
`loom-slurm-job-cgroup-guard.service`, not a Prolog. The root guard discovers
only exact `job_<id>` directories beneath a Slurm scope and looks up that exact
job through the local Slurm controller. Its closed schema-v2 configuration
contains exactly the three `loom-dev-<sandbox>` accounts, each bound to its
fixed `loom-sandbox-<sandbox>` service user and that sandbox's own SHA/tree.
The three SHAs must be pairwise distinct and the complete map is protected by
one candidate-set digest. The guard accepts the fixed
`loom-cgroup-v1:pids=<N>` grammar only when account, `UserId`, job-name SHA,
and candidate-set binding all agree. It then enables the `cpu`, `memory`, and
`pids` subtree controllers and lowers `pids.max` to the cluster profile's
exact value. The batch entry waits for that exact readback before starting
Docker.

A malformed Loom comment, wrong account, wrong ceiling, ambiguous job
identity, mismatched cgroup, missing controller, or drifted readback therefore
keeps the batch entry fail-closed. Jobs outside the Loom comment namespace are
unchanged. The Loom submitter, not an operator command line, emits the comment
from the checked-in pool configuration. `PrologFlags=Contain` remains required
to create the extern/job cgroup; do not add the incompatible newer
`RunInJob` flag to these Slurm 23.11 clusters.

The GB10 profile requires an administrator of that Slurm domain. Membership in
the host `docker` group is not scheduler-administration authority.

## Convergence

Render the secret-free atomic-set plan from the installed oldlab-2 authority:

```bash
sudo /usr/local/libexec/loom-developer-sandbox-host slurm-converge \
  --domain oldlab --sandbox all --candidate-sha <QIANYI_SHA>
```

Mutation remains disabled unless `--execute` is present. The authority records
the exact candidate-set/profile/operation binding in a root-private drain
journal before taking each node drain. If the node remains busy,
the root-installed `loom-developer-sandbox-slurm-recovery.timer` retries the
same closed operation every 30 seconds and after boot; once the node is
quiescent it completes the transaction and resumes only its own drain. It
never calls `scancel`, stops a container, kills a GPU process, resumes a
foreign drain, or accepts a caller-selected recovery path. It validates the
desired Docker daemon configuration before the atomic writes. Every pass holds
the persistent per-cluster administration lock
at `/var/lib/loom-developer-sandbox-slurm-policy/locks/<cluster>.lock` across
recovery, preflight, every mutation, and final readback. The lock and its
non-symlink parent chain are root-owned; the lock has exact mode `0600`, is
validated by descriptor/path device and inode, link count, owner, group, and
mode, and is acquired with `flock`.

The same pass records a root-only, fsynced transaction journal below
`/var/lib/loom-developer-sandbox-slurm-policy/transactions/`; its file and
accounting snapshots remain below the adjacent `snapshots/` directory. The
state directories are root-owned mode `0700`, and journals/manifests/accounting
records are single-link regular files with exact mode `0600`. Recovery accepts
only the canonical timestamped snapshot directory and the exact
`manifest.json` and `accounting-cas.json` filenames recorded by this tool;
symlink, hardlink, owner, mode, inode, or path drift fails closed before a
restore. The snapshot manifest is a closed, ordered six-file inventory. Every
row records presence, original mode/UID/GID/link count/size, and the exact
content SHA-256. Archived file bodies are separate root-private mode-`0600`
single-link files read with `O_NOFOLLOW`; their metadata and digest are checked
before restore, and the restored live file is checked again against the same
row. Missing, extra, replaced, hardlinked, foreign-owned, or content-tampered
snapshot entries cannot satisfy rollback CAS.

The accounting snapshot is a compare-and-swap record scoped only to the Loom QoS,
parent/child accounts, and three exact user associations; the converger never
dumps or loads the whole cluster database. Unrelated accounts and concurrent
unrelated changes are untouched. Each `sacctmgr` mutation has exact scoped
state checks immediately before and after it, and external references are
checked again immediately before deleting a newly created account or QoS.
Owned-field or intermediate-state drift fails closed. Each phase is durable
before the next mutation. A failed daemon reload, restart,
`scontrol reconfigure`, accounting mutation, or live readback restores both the
files and the exact owned accounting fields automatically. A later invocation
recovers an orphaned non-terminal journal before starting new work. Policy
journals are canonical closed objects bound to the exact operation, cluster,
physical host, Slurm node, candidate, restart/accounting flags, canonical
snapshot/accounting paths, and finite phase set. Recovery validates that
complete identity, requires the accounting flag and canonical accounting path
to agree in both directions, and prevalidates the closed snapshot plus any
cluster-bound accounting CAS before any file restore or service restart. An
open-shape, cross-host, cross-cluster, foreign, or malformed journal is a hard
stop.

The administration lock is cooperative across supported operations. Every
administrator must use this converger for Loom-owned accounting changes and
must not invoke direct `sacctmgr` writes. A process that bypasses the tool
cannot be physically blocked by a filesystem `flock`; the per-mutation
readbacks detect observable bypass drift and stop, but cannot prove that a
same-field write was overwritten inside the unavoidable database-command
race. Direct `sacctmgr` mutation is therefore unsupported and invalidates the
transaction evidence.

The live command binds the installed guard configuration and status to one
atomic candidate-set transition. Direct live policy CLI use without the
canonical three-account binding is rejected. Use the fleet maintenance
authority below; it derives the binding from all three root-owned installed
desired states and refuses a dirty, partial, legacy, hybrid, duplicated-SHA,
or cross-account map.

The OLDLAB and GB10 profiles cap opted-in jobs at 32768 and 65536 PIDs,
respectively; `plan` reports the bound before any mutation. A restart pass
also daemon-reloads, enables, and starts the cgroup guard. Unit activity alone
is not health. On every scan the guard atomically publishes a root-only status
record containing its timestamp, candidate-set/config digests,
scanned/verified/failed counts, bounded failure reasons, and the last real job
resource probe independently for each account. The final live check requires
that status to be fresh and failure-free and requires the selected sandbox's
account-bound job probe with finite CPU, memory, and PID cgroup readback; the
GB10 profile additionally requires a positive allocated GPU TRES readback.

After the collector publishes a fresh combined receipt, materialize a complete
proof bundle on each profile's exact submit host. Fetches use only the
installed node-authority transport and its forced `check` verb. There is no
separate root SSH key, known-hosts file, caller-selected remote path, or
fallback in the Slurm policy.

Each request carries one canonical artifact ID binding the exact sandbox,
candidate SHA/tree, and one of the eight approved proof source names. The node
authority independently binds that ID to its fixed logical node, canonical
hostname, and domain before reading a root-owned single-link artifact with a
1 MiB source bound. The response remains bounded after JSON/base64 framing,
and bundle metadata records only the logical node, hostname, and transport
artifact ID. It never records a credential or remote filesystem path.

The materializer copies the combined receipt, fleet, both domain
manifests/signatures, and both publisher public keys into a root-only local
bundle. It verifies both Ed25519 signatures, candidate/tree identity,
monotonic publisher generations, and the complete 20-node fleet before an
atomic no-replace publish. Run the command once on `oldlab2` with the OLDLAB
profile and once on `gb10-1` with the GB10 profile:

Recovery runs under a persistent `<cluster>/<sandbox>` proof lock before any
fresh remote fetch. This permits `qianyi`, `hongjian`, and `devansh` to
materialize different candidate SHAs concurrently without sharing a
transaction, stage, final bundle, or current/high-water pointer. A
closed, journal-owned stage is rolled forward and re-read; a journal-owned
partial stage is removed only after every present entry passes the private-file
checks. A published final is adopted only when its complete manifest and local
file digests match the journal. Unknown journal fields, paths, stages, finals,
or current/high-water state fail closed. Publication and complete final
readback happen before the monotonic high-water pointer changes, so a rename
failure leaves the previous accepted bundle selected. After recovery commits,
a rotated fresh receipt starts a new transaction instead of being blocked by
the prior receipt's journal.

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py materialize-runtime-proof \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --sandbox "$SANDBOX_USER" \
  --candidate-sha "$EXACT_CANDIDATE_SHA" \
  --execute
```

Then create and check the allocation-side artifact from that same exact submit
host. OLDLAB and GB10 are independent controllers; never reuse one domain's
command or evidence for the other:

```bash
sudo python scripts/ops/developer_sandbox_slurm_policy.py allocation-probe \
  --profile deploy/slurm/developer-sandboxes/oldlab.toml \
  --sandbox "$SANDBOX_USER" \
  --candidate-sha "$EXACT_CANDIDATE_SHA" \
  --candidate-root "$EXACT_CANDIDATE_ROOT" \
  --worker-env "$PRIVATE_WORKER_ENV" \
  --batch-uid "$EXPECTED_BATCH_UID" \
  --batch-gid "$EXPECTED_BATCH_GID" \
  --expected-pool oldlab \
  --expected-concurrency 4 \
  --execute

```

`--expected-pool` and `--expected-concurrency` are assertions, not overrides.
Both the submit-side probe and compute-side checker re-read the exact
candidate's checked-in shared-capacity policy and runtime-domain contract.
OLDLAB is bound to `oldlab`/`4`; GB10 is bound to `gb10`/`8`. Any CLI value,
published env, profile inventory, or cross-domain source that disagrees fails
before the allocation matrix can be accepted.

`--sandbox` is mandatory for proof materialization, allocation probing,
compute-side checking, and strict live readback. It remains the logical label
`qianyi`, `hongjian`, or `devansh`; it is never interpreted as a personal
login user. The policy maps it exactly to the non-login service identity
`loom-sandbox-<sandbox>` in `profile.accounting.users` and the aligned
`loom-dev-<sandbox>` child account. The submitted `--uid`, pwd and numeric
UID/GID readback, squeue/sacct user, Slurm account, journal, and compute result
must all agree with that mapping. Cross-sandbox or personal-user reuse fails
closed.

The probe is an all-node matrix by default. While holding one persistent
`<cluster>/<sandbox>/<candidate>` lock, it walks the profile's `allowed_nodes`
exactly once in declared order. Every bounded, non-exclusive `sbatch` carries
`--oversubscribe` and the exact
`--nodelist=<allowed_nodes node>`; its `srun` repeats that Slurm `NodeName`.
Inside the allocation, the observed compute OS hostname must independently
equal `host_aliases[node]`. The deterministic job name contains the declared
sandbox, node, complete 64-hex immutable runtime-proof bundle ID, and durable
attempt number and is bounded to Slurm's safe 128-character limit. The bundle
ID binds the candidate and every receipt, signed domain, fleet, and manifest
byte, so a receipt-only or truncated generation can never share the current
job namespace. Random scheduler placement or a single successful node is never
acceptance.

Submission uses immediate parsable output, not `sbatch --wait`. A root-owned,
single-link mode-`0600` matrix journal and per-node inflight record live below
a non-symlink root-owned mode-`0700` directory, and every transition is
atomically written and fsynced. A rerun first recovers every exact inflight or
attempt-name orphan, including a job that completed between `sbatch` and
inflight-journal creation. Before any `scancel`, queue/accounting identity must
match the exact job ID/name, user, account, and cluster. A pending `squeue` row
may legitimately show `(Resources)` or `(Priority)` instead of an allocated
NodeName, so exact node identity is required after allocation and in terminal
accounting. Decorated terminal states such as `CANCELLED by <uid>` are
normalized before cleanup is finalized. Effective QoS is read from `sacct` and
compared with the profile; final evidence never substitutes the requested QoS
for that readback. The probe then submits only nodes without completed
evidence; completed nodes are not repeated. Cancellation, timeout, ambiguous
jobs, or terminal readback failure stays durable and fail-closed. One failed
node invalidates the final artifact; it cannot be hidden by passes elsewhere
in the matrix.

The live probe and check do not accept a receipt or bundle path. They read the
root-owned `<cluster>/<sandbox>` high-water index, derive the
immutable local bundle path, require its exact closed-world file set, and
re-read and verify every digest and both signatures. There is no fallback to
the collector's or publishers' global paths. The allocation matrix binds the
full local bundle ID as well as the receipt digest.

The matrix is a proof-bundle-generation transaction. Its generation ID is the
complete bundle ID, and its expiry is the earliest expiry among the receipt,
both signed domains, and fleet proof. Before starting and before each node
submission, that window must still cover the configured allocation timeout
plus a 30-second safety margin. Re-running a completed current generation
returns the existing final artifact; it does not mint a new top-level
timestamp from old node evidence.
Final readback validates every node's `completed_at` as well as the matrix
completion time. When the receipt rotates or the generation/evidence becomes
stale, the converger first recovers every exact inflight job, atomically
archives both the old matrix and final artifact below the root-only
`<cluster>/<sandbox>` state directory, and starts a new all-node matrix.
Operators must not delete a matrix to renew evidence. A tightly bound legacy
12-hex receipt generation is accepted only for exact recovery and archival
when its journal, runtime attestation, candidate, and sandbox all agree;
missing, unrelated, cross-sandbox, or otherwise foreign short generations
fail closed and are never accepted as evidence.

If a job reached `COMPLETED` but its compute result cannot be safely opened or
validated, recovery preserves the exact result long enough for one replay. An
unavailable or invalid replay is durably recorded on the node row, the result
is discarded, and a new generation-scoped attempt is submitted. Recovery
cannot loop forever on a terminal job whose result was already removed.

The final root-only artifact is closed-world: its node list and host-alias map
must equal the profile, and every node appears once and only once with its
exact job/account/QoS, explicit route, TRES/GPU result, completed batch and
`srun`, and exact cgroup-guard job/node binding. It also binds the candidate
SHA/tree, submit cluster/controller/host, numeric batch UID/GID, pool,
concurrency, and candidate/env metadata.

The required combined runtime receipt is root-owned mode `0600`, canonical,
fresh, candidate-bound, and collected from the 20-node domain
runtime/fleet authority. The matrix validates its digest, selected domain
generation and manifest signature digest, exact profile host coverage, and
the fleet's exact 5 OLDLAB plus 15 GB10 infrastructure set. Receipt, manifest,
signature, or fleet path/digest/generation/host drift fails before submission.
The allocation matrix uses the complete 15-node GB10 infrastructure and
production-eligible set. The 2026-07-29 owner correction supersedes #822's
static node 7 exclusion, and acceptance requires `excluded_nodes=[]`.
The installed five-minute persistent renewal timer regenerates that proof from
the exact candidate rather than extending its 15-minute expiry. Every
generation is also archived as an immutable root-owned receipt linked to its
predecessor, so a fairness run longer than one receipt TTL can prove continuous
liveness without accepting caller-provided timestamps or digests.
The receipt is prerequisite evidence, not a substitute for the allocation:
inside every `srun`, the candidate's own policy program rechecks raw Git tree
bytes, the exact env device/inode/content digest and effective
pool/concurrency/candidate values, Docker access/cgroup driver, and secret-safe
rendering of the worker, sandbox-link, and cgroup-parent Compose layers. The
candidate cgroup helper must identify the actual `SLURM_JOB_ID` parent, and
both rendered services must use that exact parent. The compute node writes a
mode-`0600`, numeric-identity-owned result that the submit side opens without
following symlinks and binds into the final evidence. The old `id`/`sleep`
probe is no longer accepted.

The full invalidate, recovery, submission, polling, evidence, and finalize
transaction first holds the persistent per-cluster administration lock and
then the root-owned mode-`0600` `flock` keyed by cluster and candidate. Final
`check` uses the same domain-then-candidate order across file planning,
effective Slurm/Docker/systemd/accounting readback, inflight checks, and
matrix/final artifact validation. Apply, rollback, allocation, and readback
therefore cannot interleave different policy epochs or deadlock through an
opposite lock order. Internal unlocked readers exist only for callers that
already hold the domain lock and are not operator surfaces.

Before submission, on every allocation, and again during the final check, the candidate verifier
walks the safe non-symlink, non-group/world-writable parent chain; opens the
private env as the same inode with exact mode `0600`; rejects duplicate,
malformed, or empty env assignments; and compares every repository file's raw
blob bytes and executable mode directly with the candidate commit tree. It
also rejects non-zero index stages, skip-worktree/assume-unchanged flags,
extra or missing files, and filter/working-tree-encoding attributes. `git
status` is not the trust decision.

The `file_plan` and `live_readback` sections are deliberately separate. The
live section verifies effective `scontrol show config` task/proctrack/
accounting/fair-share settings, `cgroup.conf`, Docker's cgroup driver, guard
enablement/activity/fresh health, and the exact QoS, account hierarchy,
per-user associations, fair-share, and group TRES returned by `sacctmgr`.

The fleet authority runs the controller last. Its controller invocation also
converges the parent account, equal-share child accounts, user associations,
aggregate TRES budget, and QoS.

Repeat with `gb10.toml` only from the independently authorized GB10
administrator path. A failed or partial pass is rerun idempotently; do not
repair SlurmDB rows or daemon JSON by hand, and do not bypass the shared
administration lock with direct `sacctmgr`.

The fleet rollback command below restores the file snapshot and exact
Loom-owned accounting fields from the last committed candidate-set
transaction. It reloads/reconfigures services, performs live readback, and
automatically restores the pre-rollback state if any rollback phase fails. Do
not delete or edit the journal or snapshot tree by hand.

## Fleet maintenance authority

Do not run the policy CLI directly across the fleet. The persistent installer
exposes a maintenance-window command whose only caller inputs are the closed
domain, the literal `all` sandbox set, and qianyi's exact authority SHA:

```bash
sudo /usr/local/libexec/loom-developer-sandbox-host slurm-converge \
  --domain oldlab --sandbox all --candidate-sha <QIANYI_SHA> --execute
sudo /usr/local/libexec/loom-developer-sandbox-host slurm-check \
  --domain oldlab --sandbox all --candidate-sha <QIANYI_SHA> --execute
```

The host captures all three exact desired SHA/tree bindings once, assigns a
durable generation and convergence ID, and sends the same canonical payload
to every node. The node authority independently inspects all three shared
candidate roots and requires their policy/profile/guard/unit surfaces to be
byte-compatible before invoking the qianyi authority copy. The node authority
then derives the domain profile, live root, restart, and controller accounting
behavior. No caller-provided profile, root, path, accounting, restart, or
arbitrary policy flag reaches the node. The policy independently revalidates
the complete account map, physical hostname, Slurm node,
cluster/controller identity, and candidate-owned zero-job/zero-container drain
precondition. GB10 node 7 is in
the same infrastructure and production allocation set as its peers. If an
external job keeps the host busy, convergence waits; it must never cancel or
preempt that job.

Candidate-set capture, converge, check, and rollback hold the same global
installer lock used by desired-state activation for their entire pass, then
take the per-domain maintenance lock. An install therefore cannot replace one
of the three desired SHA/tree bindings after capture while fleet work is still
using it.

The single per-domain candidate-set journal advances compute nodes first and
writes each exact authority receipt before continuing. A busy or foreign
job/container stops the whole set transition without cancellation. Retrying a
blocked generation reuses its captured convergence ID and receipts. Starting
a new converge after completion increments the generation and mints a new
convergence ID, so old success receipts cannot mask later host drift. The
controller runs only after every compute receipt and check readback exists. A
final check covers every node. The journal remains on failure so the pass is
resumable.

Rollback is receipt-owned and candidate-bound:

```bash
sudo /usr/local/libexec/loom-developer-sandbox-host slurm-rollback \
  --domain oldlab --sandbox all --candidate-sha <QIANYI_SHA> --execute
```

Each rollback envelope names only the prior authority request ID. The node
authority rejects a different node/domain/sandbox/candidate/tree or an
advanced policy journal/snapshot digest. It never accepts a snapshot or
journal path from the host command. A new rollback is admitted only from a
completed candidate-set pass with a converge receipt for every node; it never
skips a node whose persistent recovery timer may still finish later. An
already-started rollback remains resumable from its exact journal receipts and,
like converge, restores compute nodes before the controller.

## Fair-share contract

`loom-dev` is the aggregate capacity boundary. Its three children are:

- `loom-dev-qianyi`
- `loom-dev-hongjian`
- `loom-dev-devansh`

Each child has equal fair-share and uses the `loom-dev` QoS. The aggregate
account may use the reviewed pool budget when idle, while the children have no
fixed one- or two-worker ceiling. The shared-capacity broker still clamps
active plus pending slots before submission. Slurm fair-share and current
cluster availability determine actual capacity; the profiles do not create a
reservation or a guaranteed full-pool entitlement.

The corresponding Slurm association users are
`loom-sandbox-qianyi`, `loom-sandbox-hongjian`, and
`loom-sandbox-devansh`. Personal login UIDs are deliberately absent because
their numeric identities are not consistent across OLDLAB and GB10.
