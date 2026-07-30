# Developer sandbox live acceptance

This runbook closes the live, candidate-bound acceptance scope for the
registry-owned active developer-environment cohort. Pre-merge acceptance
deliberately runs a different candidate in every selected environment at the
same time. The cohort is projected from the root-owned registry snapshot,
contains at least two active environments with committed deployments, and has
no fixed upper cardinality or developer-name allowlist. It covers a directly
observed overlap window, a
large multi-node batch, simultaneous fair-share pressure, non-Loom Slurm
peers, container cgroup containment, storage/I/O bounds, and recovery from
cancel, TTL expiry, submit-host restart, and worker crash.

The durable harness is
`scripts/ops/developer_sandbox_live_acceptance.py`. Its default is a read-only
plan. It does **not** submit Slurm jobs, request or cancel capacity, restart a
service, or kill a worker. Those actions remain separately authorized and must
use the installed exact-candidate control surface. The harness validates
offline evidence, imports sanitized facts from fixed root-owned producer
paths, and persists a root-owned, crash-safe phase and receipt journal. Every
journal mutation requires explicit `--execute`.

Do not use repository tests, a Draft PR, a green CI result, or an earlier
candidate's live artifact as a substitute for this acceptance.

## Registry-owned scope

The verifier rejects changes to this topology:

- submit host: `trt-eai-oldlab-2`;
- environments: the complete ordered active-and-committed cohort projected
  from the supplied root-owned registry snapshot, with at least two entries;
- pools: `oldlab` (20 slots, 10 pending) and `gb10` (120 slots, 24 pending);
- OLDLAB nodes 1 through 5;
- GB10 infrastructure nodes 1 through 15;
- production workload-eligible GB10 nodes 1 through 15;
- excluded nodes: none (`excluded_nodes=[]`).

The 2026-07-29 owner correction supersedes #822's temporary static exclusion
of `trt-gb10-7`. Node 7 now has the same infrastructure and capacity
eligibility as every other GB10 node. When it is busy, the candidate-owned
drain/quiescence gate defers disruptive convergence until acceptance-owned
work is quiescent; the acceptance procedure must never cancel or preempt an
external job. The complete acceptance boundary remains all 15 GB10 nodes.

The pre-merge `candidates` map is closed-world relative to that registry
projection: it contains exactly one entry for every projected runtime ID, each
with a full SHA, full tree, and that environment's runtime-receipt chain. All
SHAs must be pairwise distinct; every tree
must still be a full hash and bind the corresponding SHA's installed content.
Every sandbox phase, capacity sample, burst, runtime envelope, fairness
participant, fault row, and overlap observation binds the corresponding map
entry. A receipt stored under one sandbox but naming another sandbox's
candidate fails.

The Slurm cgroup guard uses the same closed map as one atomic cluster
generation. For each sandbox, acceptance must read back a fresh probe under
that sandbox's exact `loom-dev-<sandbox>` account and fixed
`loom-sandbox-<sandbox>` Slurm `UserId`; the probe's SHA/tree and candidate-set
digest must match the map. One global SHA, an allowed-SHA list, a legacy
schema-v1 config, a partial/hybrid map, or a probe borrowed from another
account fails acceptance.

Each short-lived receipt covers at most 15 minutes; successive generations
must advance both domain generations, link the previous immutable receipt
digest, overlap without a liveness gap, and collectively cover the complete
acceptance window. Evidence is closed-schema, canonical, secret-free,
timezone-qualified, and collected no later than five minutes after the final
readback.

For a cohort of `N` environments, the artifact contains exactly
`3 * N * (N - 1)` cross-environment denials: every directed
source-to-foreign-target pair multiplied by `worker_identity`, `object_store`,
and `result_path`. Every row binds both the source environment's
SHA/tree and the target sandbox's different SHA/tree, lands in the source
sandbox's declared preflight or baseline phase, and reports `denied=true`.

The eventual exact squash-merged staging candidate is not another entry in the
pre-merge map. It is recorded only in the independent top-level
`promotion_candidate` object and its `promotion_staging_regression` phase.
That SHA must differ from every pre-merge SHA, and the phase's canonical
digest must match its content.

An overlap observation is not accepted from `job_active=true` or
`service_active=true` alone. It carries a closed, sanitized `scontrol`
readback for a RUNNING job and a closed systemd readback for an active/running
unit. Each object has a verifier-recomputed canonical SHA-256. The Slurm
account is `loom-dev-<sandbox>`, the submit user is the sandbox name, and the
job name is exactly
`loom-sandbox-<sandbox>-<FIRST_12_CANDIDATE_SHA>-<node>` (with the controller's
safe node normalization and 128-character truncation). The service readback
binds its unit and active SHA/tree. The observation also binds the exact
`multi_candidate_overlap` capacity sample by request ID, lease epoch,
observation sequence, and canonical sample digest.

## Admission and authority

Before creating a session, record all of the following outside the artifact:

1. the separately authorized live test window;
2. the exact, distinct SHA/tree pair for every environment in the registry
   cohort;
3. the persistent renewal timer is active and a fresh combined runtime receipt
   generation exists for the complete cohort and both NFS domains;
4. installed capacity broker, two adapters per environment, supervisor, Slurm
   policy, cgroup guard, and remote-link readbacks for every exact candidate;
5. a healthy non-Loom peer workload in each Slurm domain;
6. reviewed disk, cache, read-I/O, and write-I/O bounds;
7. an operator who can drain only the acceptance-owned requests, jobs, and
   containers.

Stop if a required mutating dependency does not expose an explicit execution
gate. Do not replace it with a raw SQLite edit, direct Control Plane policy
write, manual `/etc/slurm` edit, unscoped `scancel`, Docker prune, or
host-wide process kill. The acceptance harness's `--execute` acknowledges only
its own journal write; it does not expand live authority.

Render the plan from any checkout:

```bash
python scripts/ops/developer_sandbox_live_acceptance.py
```

The output must report `mode=plan_read_only`,
`live_mutations_supported=false`, the exact topology above, all eleven phases,
and all four fault scenarios.

## Start the durable session

Run this only as root on `trt-eai-oldlab-2` after the admission checks pass:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py session-start \
  --registry-snapshot <ROOT_OWNED_REGISTRY_SNAPSHOT_JSON> \
  --candidates <EXACT_REGISTRY_COHORT_CANDIDATE_MAP_JSON> \
  --execute
```

The candidate-map JSON is keyed by the projected runtime IDs and contains only
`sha` and `tree` for each entry. The command rejects fewer than two active
committed registry environments, a missing or extra map entry, registry
candidate drift, or duplicate candidate SHAs.

The command requires every directory from the state root through `sessions`,
the session, `checkpoints`, and `trusted-receipts` to be non-symlink
`root:root` mode `0700`.
The session lock, state, checkpoints, and sealed evidence are non-symlink
`root:root` mode `0600`. Reads recheck owner, mode, device, and inode around
the open file descriptor. Writes use fsync and create-exclusive or atomic
replacement while a per-session flock is held. Save the returned session ID.
A host or process restart does not lose the next required phase:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py session-status \
  --session-id <SESSION_ID>
```

Never edit `state.json`, remove a checkpoint, or advance around a failed phase.
Rerun the exact observation and submit the next checkpoint only after its stop
conditions are clean.

## Trusted live producer receipts

Self-reported hashes in the final JSON are not authority. During the overlap,
record one immutable trusted receipt for each sandbox/pool pair. The command
reads only these fixed root-owned producer paths:

- `/var/lib/loom-shared-capacity/observations/<sandbox>-<pool>.json`;
- the registry environment's exact `state_root/sandbox-state.json`;
- `/var/lib/loom-developer-sandbox-live-authority/overlap/<pool>/<sandbox>/<candidate_sha>/<job_id>.json`.

The first path is the existing shared-capacity-adapter output. It remains a
canonical JSON list containing exactly one object with only `sandbox`,
`pool_name`, `candidate_sha`, `request_id`, `lease_epoch`,
`capacity_lease_state`, `observed_at`, `observation_sequence`,
`pending_slots`, `active_slots`, `draining_slots`, `terminal_slots`, and
`payload_sha256`. The payload digest is over that object without
`payload_sha256`, using sorted compact JSON and no trailing LF. Do not add job,
allocation, host, phase, tree, requested, or granted fields to this existing
producer.

The second path is the existing installer state and remains closed to
`schema_version`, `sandbox`, `compose_project`, `candidate_sha`,
`candidate_tree`, `source_repo`, and `updated_at`. It proves installed
candidate identity, not systemd or Slurm state. Do not add `kind`,
`source_host`, or service status to it.

The third path is produced by the persistent fixed authority documented in
[`developer-sandbox-live-authority.md`](developer-sandbox-live-authority.md).
It must be a
canonical `root:root` mode `0600`, single-link regular file beneath
root-owned, non-group/world-writable directories. Its closed top-level fields
are:

```json
{
  "schema_version": 1,
  "kind": "loom.developer-sandbox.live-overlap-observation",
  "source_host": "trt-eai-oldlab-2",
  "observed_at": "<RFC3339>",
  "sandbox": "<registry-runtime-id>",
  "pool": "oldlab",
  "candidate_sha": "<40 lowercase hex>",
  "candidate_tree": "<40 lowercase hex>",
  "capacity_observation_sha256": "<sha256 of canonical adapter [object] file>",
  "sandbox_state_sha256": "<sha256 of canonical sandbox-state object>",
  "capacity_sample": "<closed final-evidence capacity sample object>",
  "job_readback": "<closed sanitized RUNNING Slurm readback object>",
  "service_readback": "<closed sanitized systemctl readback object>"
}
```

For `oldlab`, `source_host` is `trt-eai-oldlab-2`. For `gb10`, the producer
runs through `gx10-01c7` and records its canonical host identity
`trt-gb10-1`. The producer obtains `job_readback` from the fixed CP active-job
registry and fixed job ID through the installed node authority/transport, and
`service_readback` from the fixed unit using only non-secret `systemctl show`
properties. It sanitizes to schema fields before writing;
raw command output, environment, command line, labels, and URLs are forbidden.
The capacity sample carries the full job ID, account, user,
controller-derived job name, node, and allocation, and its request ID, lease
epoch, sequence, counters, and timestamp must match the existing adapter row.
The Slurm user is the fixed non-login `loom-sandbox-<sandbox>` service identity,
not a personal login identity. The adapter, Slurm, systemd, and collection
timestamps remain their real observation times: adapter age is bounded to 120
seconds, the live queries and publication to 30 seconds, and ordering/freshness
is rechecked by the importer. They must never be rewritten to one synthesized
timestamp. A missing independent producer is a hard stop.

After that producer has atomically published a pair's file, import it:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py \
  session-record-overlap \
  --session-id <SESSION_ID> \
  --sandbox <registry-runtime-id> \
  --pool <oldlab|gb10> \
  --job-id <SLURM_JOB_ID> \
  --execute
```

The importer walks each fixed root using directory file descriptors and
`O_NOFOLLOW`, requires root ownership, safe directory modes, a single-link
mode-`0600` leaf, canonical secret-free JSON, and stable inode/size/mtime
through the read. It stores the three source snapshots, their fixed paths and
digests, and a strictly increasing sequence in an immutable session receipt.
It rejects a repeated sandbox/pool pair, sequence regression, path
substitution, hardlink, symlink, source swap, candidate drift, or mismatched
capacity/job/service identity.

The exact squash-merged staging regression has a separate root authority
receipt at `/var/lib/loom-staging-rollout/acceptance/promotion.json`. Its
closed fields are `schema_version=1`,
`kind=loom.staging-rollout.acceptance`, `source_host=trt-eai-oldlab-1`,
`rollout_id`, `candidate_sha`, `candidate_tree`, `result=pass`, and
`observed_at`. Import it after the regression:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py \
  session-record-promotion \
  --session-id <SESSION_ID> \
  --execute
```

The same direct-root node-authority bootstrap/upgrade transaction also installs
the platform-health collector at
`/usr/local/libexec/loom-developer-sandbox-platform-health-authority`, its
systemd unit at
`/etc/systemd/system/loom-developer-sandbox-platform-health-authority.service`,
and its sudoers policy at
`/etc/sudoers.d/loom-developer-sandbox-platform-health-authority`. After the
fixed collector completes all checkpoints, import its immutable
`/var/lib/loom-developer-sandbox-platform-health-authority/sessions/<SESSION_ID>/evidence.json`:

The collector is the supported Gate-6 materializer; do not hand-author the old
non-exclusive JSON shape. Collect the seven ordered checkpoints `baseline`,
`mixed_non_loom`, `cancel_cleanup`, `ttl_cleanup`, `submit_host_restart`,
`worker_crash`, and `final_drain`. Between `mixed_non_loom` and
`cancel_cleanup`, invoke the fixed `sample` command at least 120 times across
at least four hours:

The phase evidence supplied to every environment's `mixed_non_loom`
checkpoint must include a closed `trial_batches` object with the exact
`oldlab` and `gb10` soak batch UUIDs returned by that sandbox's workload
submission. These are `2 * N` distinct soak batches for cohort size `N`; they
are not the earlier
`large_batch_burst` batches. The root checkpoint journal persists this
manifest with the sandbox candidate SHA/tree, and later samples accept no
replacement batch or caller-selected database.

```bash
sudo /usr/local/libexec/loom-developer-sandbox-platform-health-authority \
  sample --session-id <SESSION_ID> --execute
```

Each invocation rereads the exact `2 * N` environment/pool jobs through the
fixed
root-owned node transport, plus Slurm/Docker/cgroup, Kubernetes, MinIO,
Longhorn, non-Loom peer, CPU/memory/PID, and GPU-device authorities. It appends
one canonical mode-`0600` sample to a digest-chained journal. A crash or
disconnect does not erase prior samples: rerun the same command, and the
collector verifies and resumes the exact chain. The four-hour interval may
span bounded Slurm job rotation because the checked-in QoS has a two-hour wall
limit, but every sample must still contain exactly one candidate-bound job for
each environment/pool pair. Any duplicate, foreign candidate, excluded node,
missing node 7, device-isolation failure, peer failure, or envelope breach is
a hard stop.

Trial success is read from durable Loom trial rows, not inferred from Slurm
job exit status. On `oldlab-2`, the collector verifies each candidate's
root-owned combined activation receipt, exact desired state, and current
`sandbox-state.json`. It requires the desired state, lifecycle state, and
activation receipt for each environment to name the same candidate
SHA/tree and Compose project, then resolves exactly one healthy `postgres`
container whose Compose config path/hash and creation/start generation precede
that lifecycle record. The resulting container, desired-state, lifecycle, and
receipt digests are retained in every sample.

The fixed read-only query selects **all** trials from the two manifest batch
UUIDs; it has no `finished_at` lower bound and uses left joins for worker and
Slurm attribution. Thus an early-finished, unclaimed, pending, or otherwise
unattributed trial cannot disappear from the census or denominator. Batch
creation must fall within the root checkpoint's `mixed_non_loom` phase window.
Every terminal trial must bind its batch, sandbox, candidate, pool, and a
Slurm job observed in the sample chain. Trial UUIDs and terminal snapshots are
monotonic. The final sample requires every trial in every fixed batch to be
terminal with a finish time; any queued, claimed, or running row blocks Gate
6. Missing attribution, a foreign batch, duplicate identity, or drift is a
hard stop. The final success ratio is `succeeded / (succeeded + failed +
cancelled)` globally and for every sandbox/pool pair, with a non-zero
denominator and a minimum of `0.95`; `attempt_count - 1` is retained as retry
attribution.

The final authority evidence includes the exact seven checkpoints and a closed
`gate6_observations` object. It requires allocated-GPU usability and
unallocated-device denial on GB10, explicit zero-device exposure and denial on
OLDLAB, and root accounting for cancellation, TTL expiry, submit-host restart,
and worker crash with zero remaining jobs or containers. Node `trt-gb10-7` is
part of the 15-node GB10 closed world; a busy node defers collection until
existing work finishes and is never statically excluded or preempted.

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py \
  session-record-platform-health \
  --session-id <SESSION_ID> \
  --execute
```

### Capacity recommendation and repository promotion

The platform-health receipt is evidence, not permission to activate OLDLAB.
Its capacity recommendation must be derived deterministically from the
complete fixed OLDLAB node set and must retain the exact candidate, topology,
headroom, and source-receipt binding. A missing node, stale receipt, failed
checkpoint, candidate mismatch, or non-deterministic/manual capacity value is a
hard stop.

Use the repo-only capacity-promotion tool from the exact candidate checkout.
It has no path, value, `--execute`, or activation override. Its only inputs are:

- checked-in `deploy/environment-state/staging.toml`;
- checked-in
  `deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml`;
- root-owned
  `/var/lib/loom-developer-sandbox-platform-health-authority/current.json`;
- the exact
  `/var/lib/loom-developer-sandbox-platform-health-authority/sessions/<SESSION_ID>/evidence.json`
  named by that closed current pointer.

The stable operation order is:

```bash
python scripts/ops/developer_sandbox_capacity_promotion.py check
python scripts/ops/developer_sandbox_capacity_promotion.py render \
  > /tmp/developer-sandbox-oldlab-capacity.patch
# Review and apply the patch through the normal repository workflow.
python scripts/ops/developer_sandbox_capacity_promotion.py check
```

`check` is deliberately offline and accepts exactly two repository states. The
first is the committed fail-closed profile: empty evidence bindings,
provisional disabled OLDLAB values, and a disabled/inactive supervisor. The
second is an enabled profile whose historical provenance has a closed
session-to-fixed-evidence-path binding, payload digest, registry-candidate-set
digest, recorded expiry, policy-source digest, and recommendation digest, and
whose capacity values, external allocation prerequisite, and supervisor state
exact-match the checked-in policy. Any hybrid, malformed, or config-drifted
state fails. A recorded historical expiry does not invalidate repository
provenance; `check` never reads `/var/lib`.

`render`, unlike `check`, reads the fixed root path and requires the current
evidence to be fresh. Its input repository must still be the exact disabled
profile: empty promotion bindings, provisional OLDLAB values, GB10-only
external prerequisites, and a disabled/inactive OLDLAB supervisor. One
deterministic diff then binds provenance, writes the final measured capacity
values, adds OLDLAB to the external-authority prerequisite set, moves its env
file under `/srv/loom/staging-shared/generated`, broadens the fixed template
glob to the two named pools, and enables the desired policy and supervisor
together. It emits that diff only on stdout; it does not write the staging
profile, install files, apply environment state, enable a live pool, start a
timer, or mutate authority state. Apply and commit the reviewed diff through
the normal PR path.

After merge, rerun offline `check` on the exact merged candidate, then generate
new fresh merged-candidate live acceptance for the runtime-host and activation
gates. Those live gates re-read current root evidence and enforce freshness;
they do not rewrite the checked-in historical provenance. The `/tmp` patch
above is process-only and should be deleted after the repository change is
applied.

The staging pressure producer is separately installed at
`/usr/local/libexec/loom-staging-pressure-reclaim-authority`, with fixed config
`/etc/loom/staging-pressure-reclaim-authority.toml`, systemd unit
`/etc/systemd/system/loom-staging-pressure-reclaim-authority.service`, and
sudoers policy `/etc/sudoers.d/loom-staging-pressure-reclaim-authority`.
After its staging-only transaction publishes the signed immutable wrapper
under
`/srv/loom/staging-shared/results/pressure-reclaim/<SESSION_ID>/<AUTHORITY_SESSION_ID>.json`,
import it using identities rather than a caller-provided path:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py \
  session-record-staging-pressure \
  --session-id <SESSION_ID> \
  --authority-session-id <AUTHORITY_SESSION_ID> \
  --execute
```

The importer requires a root-owned mode-`0600` Ed25519 public key at
`/etc/loom/staging-pressure-reclaim/authority-public.pem`, verifies the
signature and closed receipt, and exact-binds the evidence to the acceptance
session, promotion SHA/tree, source host, sequence, and staging regression
window. This is staging pressure evidence only; it is not production evidence.

Finalization requires all `2 * N` ordered overlap receipts, promotion receipt,
complete platform-health receipt, and signed staging-pressure receipt. It
reloads them from the protected journal and exact-matches their capacity
sample, job/service readbacks, candidates, source bindings, signatures, and
timestamps to the final evidence. It never re-trusts a caller-recomputed
self-hash.

## Bounded state machine

Phases must be completed in this exact order. Each phase is checkpointed once
for each environment in the registry projection's stable order, for
`11 * N` total checkpoints. A final artifact with a missing, duplicated, or
reordered environment/phase pair fails.

| Phase | Required action and readback | Hard stop |
| --- | --- | --- |
| `preflight` | Read back each environment's exact candidate/tree, both domain receipts, all `2 * N` adapter identities, broker/supervisor state, Slurm policy, cgroup guard, topology, and empty acceptance-owned capacity. Begin the directed cross-environment denial matrix. | Any stale/cross-candidate receipt, reused SHA, missing node, active prior test lease, or nonzero invariant. |
| `baseline` | Complete all `3 * N * (N - 1)` cross-environment denials. Record healthy non-Loom peer jobs, disk free space, cache footprint, and domain I/O counters before Loom pressure. | Missing/duplicate denial, a same-environment row, any allowed foreign access, peer already failing, disk below the reviewed floor, or an unknown owner on a planned test resource. |
| `multi_candidate_overlap` | In each pool, hold one active service and active Slurm job for every sandbox. Record a non-empty common interval, exact candidate, unique job ID/name, Slurm account/user, eligible node, canonical job and service readbacks/digests, and the exact active capacity-sample binding. | No common interval, reused job identity, inactive service/job, candidate/account/user/name/readback mismatch, wrong-pool node, digest drift, missing active capacity binding, or a synthesized session-wide timestamp. |
| `large_batch_burst` | Submit at least 100 trials for each sandbox/pool pair through acceptance-owned batches and let every burst use at least two eligible nodes. Record requested/granted/peak slots and per-node trial counts. | `--exclusive`, one-node placement, duplicate trial ID, pool/pending overshoot, or excluded-node use. |
| `fairness_contention` | Keep equal requests from every cohort environment eligible in each pool for at least 30 minutes. Retain the raw series and record the phase-closing requested/granted/pending/active/draining/terminal sample for each pair. | An environment receives no grant cycle, waits more than 600 seconds for its first grant, total grant skew exceeds 20%, indefinite starvation, replayed observation, or temporary overshoot. |
| `mixed_non_loom` | Run the acceptance workload with the pre-existing non-Loom Slurm peer. Capture baseline/during/after throughput, latency, running/completed/failed counts, and storage/I/O. | New peer failure, disruption, throughput regression over 20%, ENOSPC/I/O error, or a reviewed cache/I/O limit breach. |
| `cancel_cleanup` | Explicitly cancel one acceptance-owned request and its batch through the supported candidate-bound surface. Observe drain and retry attribution. | Recovery over 600 seconds, orphan, lost trial, duplicate retry, or unknown attribution. |
| `ttl_cleanup` | Let one bounded acceptance request expire without extending or rewriting its lease. Observe automatic retire/drain. | Capacity remains committed after the deadline, an observation refreshes an expired lease, or any orphan remains. |
| `submit_host_restart` | Restart only the installed submit-host acceptance dependencies during an acceptance-owned workload, then resume the same durable session. | New request identity, regressed observation sequence, lost broker/adaptor state, duplicate grant, or recovery over 600 seconds. |
| `worker_crash` | Terminate only one acceptance-owned test worker through the supported worker control surface. Observe retirement, trial retry, and replacement attribution. | Foreign worker impact, orphan container/job/lease/trial, duplicate retry, or recovery over 600 seconds. |
| `final_drain` | Cancel or expire every acceptance-owned request, wait for zero pending/active/draining capacity, and capture final peer/storage/runtime readbacks. | Any live acceptance job/container/lease, peer delta, secret-like field, or evidence older than five minutes. |

For Slurm/runtime evidence, record one representative job for every
sandbox/pool pair. Each row must prove:

- the child account is `loom-dev-<sandbox>`;
- the allocation is non-exclusive;
- `AllocTRES` includes CPU and memory, plus GPU TRES for GPU allocations;
- the finite job cgroup has CPU, memory, and PID controllers;
- worker, trial, verifier, and sidecar containers are strict descendants of
  that job cgroup;
- configured and live-read CPU, memory, and PID limits match;
- the four-container aggregate does not exceed either Slurm or cgroup maxima.

Do not store `scontrol show job` output wholesale. Extract only the
closed-schema fields. Environment values, command lines, URLs, and arbitrary
labels can contain credentials or private endpoints and do not belong in this
artifact.

## Phase checkpoints

After a phase passes, write a sanitized phase-evidence JSON object with exactly
these keys:

```json
{
  "sandbox": "<registry-runtime-id>",
  "candidate_sha": "<40 lowercase hex>",
  "candidate_tree": "<40 lowercase hex>",
  "phase": "preflight",
  "started_at": "2026-07-28T19:55:00Z",
  "finished_at": "2026-07-28T20:00:00Z",
  "deadline_seconds": 600,
  "status": "pass"
}
```

Advance the journal:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py \
  session-checkpoint \
  --session-id <SESSION_ID> \
  --phase <EXACT_NEXT_PHASE> \
  --sandbox <EXACT_NEXT_SANDBOX> \
  --phase-evidence <SANITIZED_PHASE_EVIDENCE_JSON> \
  --execute
```

The command canonicalizes the actual phase payload, computes its SHA-256
itself, and persists the generated checkpoint. It rejects a wrong candidate,
tree, phase, timestamp, deadline, failure status, extra field, or secret-like
field/value. A crash after the checkpoint write but before the state advance is
recoverable: rerun the same command and exact phase payload. The harness
verifies the existing bytes and digest, then advances state. Concurrent calls
are serialized by the session flock.

Checkpoint support files remain under the operator's protected evidence root.
The final artifact's phase row adds the generated digest as
`checkpoint_sha256`. Finalization removes that digest field, recomputes the
canonical phase hash, and requires it to match the root-owned checkpoint.
Changing phase content while retaining a caller-supplied digest therefore
cannot pass.

## Build and verify the final artifact

Build one JSON document following
`docs/evidence/developer-sandbox-live-acceptance.schema.json`.

Schema version 2 is intentionally incompatible with the legacy single
`candidate` object. Do not translate a v1 artifact by copying one SHA across
the registry cohort; collect new candidate-distinct evidence.

Important closed sets are:

- exactly `3 * N * (N - 1)` unique directed
  cross-environment/resource denials;
- exactly two multi-candidate overlap windows, one per pool, each covering all
  cohort candidates for the complete common interval;
- all `2 * N` environment/pool capacity streams and runtime envelopes,
  including the `2 * N` overlap-phase capacity samples (`18 * N`
  phase-closing samples total across the nine capacity phases);
- exactly one large-batch per environment/pool pair and one fairness row per
  pool;
- exactly one non-Loom peer and one storage/I/O row per pool;
- exactly one recovery row for `cancel`, `ttl_expiry`,
  `submit_host_restart`, and `worker_crash`;
- zero global overshoot, duplicate, starvation, exclusive-job, cgroup-escape,
  peer-disruption, storage-error, orphan, and retry-attribution counters.

Every sandbox-scoped row binds the SHA/tree from its exact pre-merge map entry.
Capacity samples must land inside that sandbox's named phase. Runtime
envelopes bind a timestamp inside `mixed_non_loom`. Bursts bind intervals
inside their sandbox's `large_batch_burst`; fairness participants bind their
own candidates while the common window fits every environment's phase window.
Peer baseline/during/after and storage baseline/minimum/after observations land
in `baseline`, `mixed_non_loom`, and `final_drain`. Each fault interval lands
inside its corresponding cancel, TTL, submit-host restart, or worker-crash
phase. A session-wide timestamp is not enough.

The phase verifier first constructs the exact `11 * N`-key
`(phase, sandbox)` map. A duplicate or missing key is a controlled failure and
stops all validation that depends on phase windows; it must never surface a
`KeyError` or continue using a different sandbox's window.

Verify offline before copying or publishing:

```bash
python scripts/ops/developer_sandbox_live_acceptance.py verify \
  --evidence <FINAL_JSON>
```

The verifier checks schema and cross-record semantics. It does not contact a
host. It reports controlled field locations, never evidence values.

Create a canonical copy without overwriting an existing artifact:

```bash
python scripts/ops/developer_sandbox_live_acceptance.py collect \
  --input <FINAL_JSON> \
  --output <NEW_CANONICAL_JSON>
```

`collect` uses create-exclusive semantics. A partial or invalid input creates
no output. Keep the canonical artifact in the protected, candidate-specific
evidence root; publish only the secret-free artifact and its digest.

After pre-merge final drain, wait for the PR's exact squash merge. Run the
staging regression against that full merged SHA/tree, record it only under
`promotion_candidate.staging_regression`, and compute its checkpoint digest
from the canonical phase object without `checkpoint_sha256` (sorted keys,
compact separators, UTF-8, and one trailing LF). Do not rewrite the pre-merge
map to the promotion SHA.

After all `11 * N` pre-merge checkpoints, the independent promotion
regression, and
an offline pass, seal the artifact into the session:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py session-finalize \
  --session-id <SESSION_ID> \
  --evidence <FINAL_JSON> \
  --execute
```

Finalization re-verifies the artifact, requires the exact session/pre-merge
candidate-map identity, separately verifies the promotion candidate and phase,
writes or exact-verifies the root-owned sealed copy, records its
canonical digest, and marks the session complete. If a crash leaves
`evidence.json` present before the complete state is committed, rerun the same
command; exact bytes are verified before the journal advances. A complete
session rejects a different artifact but safely accepts an identical
finalization retry. For every phase, finalization recomputes the canonical
phase digest, rebuilds the complete expected checkpoint through the same
session/sandbox/candidate/tree/status metadata constructor used at collection time,
and requires exact object equality with the root-owned journal. Editing only a
checkpoint's session, phase, candidate, tree, status, or digest fails.

After finalization, seal Gate 6 from fixed authorities:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py \
  session-seal-gate6 \
  --session-id <SESSION_ID> \
  --execute
```

This command accepts no caller-selected evidence paths. It rereads the sealed
complete `11 * N` live artifact, its trusted receipts, the root-owned
platform-health authority, and the `2 * N` exact allocation matrices under
`/var/lib/loom-developer-sandbox-slurm-policy/allocation-probes`. Every matrix
must bind its own registry candidate, reviewed pool concurrency (`oldlab=4`,
`gb10=8`), and the complete 5/15-node set with `excluded_nodes=[]`. The
GB10 pair artifacts pass the unchanged non-exclusive v1 schema and semantic
verifier. OLDLAB cannot pass that schema's mandatory non-empty GPU list, so the
same Gate-6 verifier instead requires a native empty allocation plus positive
zero-device exposure and denial proof; it never invents a GPU ID.

The command writes `2 * N` canonical pair artifacts and
`gate6/acceptance.json` under the protected session directory, records
`gate6_sha256` in the closed session state, and is idempotent only for exact
bytes. An incomplete `11 * N` phase set, candidate drift, node 7 omission, a
non-empty
exclusion list, partial matrix, missing worker/trial/verifier/sidecar, short
soak, failed negative isolation, incomplete cleanup, or receipt mismatch fails
closed.

## Failure and cleanup

On any stop condition:

1. stop adding pressure;
2. lower only acceptance-owned capacity through the supported exact-bound
   surface;
3. wait for pending, active, and draining counts to reach zero;
4. verify zero acceptance-owned jobs, containers, leases, and nonterminal
   trials;
5. record the sanitized failure outside the passing artifact;
6. fix the durable implementation or contract before starting a new session.

Never turn a failed checkpoint into `status=pass`, reuse a candidate-mismatched
session, raise a budget to make an overshoot disappear, suppress a peer
failure, or delete broker/adapter state to manufacture a clean final drain.

## Legacy-v1-only historical cardinalities

The names `qianyi`, `hongjian`, and `devansh`, the six lanes, 18 isolation
denials, 33 phase checkpoints, 54 capacity samples, and six allocation
matrices describe only the retired `legacy-v1` three-environment acceptance
cohort. They may be used to recover or interpret an existing legacy journal,
but they must never be used as the current admission allowlist or as a reason
to truncate a registry-owned cohort.
