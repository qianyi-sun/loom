# Developer sandbox live acceptance

This runbook closes the live, candidate-bound acceptance scope for the three
shared developer sandboxes. It covers a large multi-node batch, simultaneous
fair-share pressure, non-Loom Slurm peers, container cgroup containment,
storage/I/O bounds, and recovery from cancel, TTL expiry, submit-host restart,
and worker crash.

The durable harness is
`scripts/ops/developer_sandbox_live_acceptance.py`. Its default is a read-only
plan. It does **not** submit Slurm jobs, request or cancel capacity, restart a
service, or kill a worker. Those actions remain separately authorized and must
use the installed exact-candidate control surface. The harness only validates
offline evidence and persists a root-owned, crash-safe phase journal. Every
journal mutation requires explicit `--execute`.

Do not use repository tests, a Draft PR, a green CI result, or an earlier
candidate's live artifact as a substitute for this acceptance.

## Fixed scope

The verifier rejects changes to this topology:

- submit host: `trt-eai-oldlab-2`;
- sandboxes: `qianyi`, `hongjian`, and `devansh`;
- pools: `oldlab` (20 slots, 10 pending) and `gb10` (140 slots, 30 pending);
- OLDLAB nodes 1 through 5;
- GB10 nodes 1 through 6 and 8 through 15;
- excluded node: `trt-gb10-7`.

All phase, capacity, burst, runtime-envelope, and fault records bind the same
full candidate SHA. The session also binds the full Git tree and a continuous
root-owned renewal chain of combined oldlab+GB10 runtime receipts for each
sandbox. Each short-lived receipt covers at most 15 minutes; successive
generations must advance both domain generations, link the previous immutable
receipt digest, overlap without a liveness gap, and collectively cover the
complete acceptance window. Evidence is closed-schema, secret-free,
timezone-qualified, and collected no later than five minutes after the final
readback.

The artifact also contains exactly 18 cross-sandbox denials: all six directed
source-to-foreign-target sandbox pairs multiplied by `worker_identity`,
`object_store`, and `result_path`. Every row binds the same SHA/tree, lands in
the declared preflight or baseline phase, and reports `denied=true`.

## Admission and authority

Before creating a session, record all of the following outside the artifact:

1. the separately authorized live test window;
2. the exact merged candidate SHA and tree;
3. the persistent renewal timer is active and a fresh combined runtime receipt
   generation exists for all three sandboxes and both NFS domains;
4. installed capacity broker, six adapters, supervisor, Slurm policy, cgroup
   guard, and remote-link readbacks for that exact candidate;
5. a healthy non-Loom peer workload in each Slurm domain;
6. reviewed disk, cache, read-I/O, and write-I/O bounds;
7. an operator who can drain only the acceptance-owned requests, jobs, and
   containers.

Stop if a required mutating dependency does not expose an explicit execution
gate. Do not replace it with a raw SQLite edit, direct Control Plane policy
write, manual `/etc/slurm` edit, unscoped `scancel`, Docker prune, or
host-wide process kill. The acceptance harness's `--execute` acknowledges only
its own journal write; it does not expand live authority.

Render the fixed plan from any checkout:

```bash
python scripts/ops/developer_sandbox_live_acceptance.py
```

The output must report `mode=plan_read_only`,
`live_mutations_supported=false`, the exact topology above, all ten phases,
and all four fault scenarios.

## Start the durable session

Run this only as root on `trt-eai-oldlab-2` after the admission checks pass:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py session-start \
  --candidate-sha <FULL_CANDIDATE_SHA> \
  --candidate-tree <FULL_CANDIDATE_TREE> \
  --execute
```

The command requires every directory from the state root through `sessions`,
the session, and `checkpoints` to be non-symlink `root:root` mode `0700`.
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

## Bounded state machine

Phases must be completed in this exact order. A final artifact with a missing,
duplicated, or reordered phase fails.

| Phase | Required action and readback | Hard stop |
| --- | --- | --- |
| `preflight` | Read back exact candidate/tree, both domain receipts, six adapter identities, broker/supervisor state, Slurm policy, cgroup guard, topology, and empty acceptance-owned capacity. Begin the directed cross-sandbox denial matrix. | Any stale receipt, wrong candidate, missing node, active prior test lease, or nonzero invariant. |
| `baseline` | Complete all 18 cross-sandbox denials. Record healthy non-Loom peer jobs, disk free space, cache footprint, and domain I/O counters before Loom pressure. | Missing/duplicate denial, a same-sandbox row, any allowed foreign access, peer already failing, disk below the reviewed floor, or an unknown owner on a planned test resource. |
| `large_batch_burst` | Submit at least 100 trials to each pool through an acceptance-owned batch and let each burst use at least two eligible nodes. Record requested/granted/peak slots and per-node trial counts. | `--exclusive`, one-node placement, duplicate trial ID, pool/pending overshoot, or excluded-node use. |
| `fairness_contention` | Keep equal requests from all three sandboxes eligible in each pool for at least 30 minutes. Retain the raw series and record the phase-closing requested/granted/pending/active/draining/terminal sample for each pair. | A sandbox receives no grant cycle, waits more than 600 seconds for its first grant, total grant skew exceeds 20%, indefinite starvation, replayed observation, or temporary overshoot. |
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
`docs/evidence/developer-sandbox-live-acceptance.schema.json`. Important closed
sets are:

- exactly 18 unique directed cross-sandbox/resource denials;
- all six sandbox/pool capacity streams and runtime envelopes;
- exactly one large-batch and one fairness row per pool;
- exactly one non-Loom peer and one storage/I/O row per pool;
- exactly one recovery row for `cancel`, `ttl_expiry`,
  `submit_host_restart`, and `worker_crash`;
- zero global overshoot, duplicate, starvation, exclusive-job, cgroup-escape,
  peer-disruption, storage-error, orphan, and retry-attribution counters.

Every capacity sample binds candidate SHA/tree and must land inside the phase
named by that sample. Runtime envelopes bind SHA/tree and a timestamp inside
`mixed_non_loom`. Bursts and fairness windows bind SHA/tree and intervals
inside `large_batch_burst` and `fairness_contention`, respectively. Peer
baseline/during/after and storage baseline/minimum/after observations land in
`baseline`, `mixed_non_loom`, and `final_drain`. Each fault interval lands
inside its corresponding cancel, TTL, submit-host restart, or worker-crash
phase. A session-wide timestamp is not enough.

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

After all ten checkpoints and an offline pass, seal it into the session:

```bash
sudo python scripts/ops/developer_sandbox_live_acceptance.py session-finalize \
  --session-id <SESSION_ID> \
  --evidence <FINAL_JSON> \
  --execute
```

Finalization re-verifies the artifact, requires the exact session/candidate
identity, writes or exact-verifies the root-owned sealed copy, records its
canonical digest, and marks the session complete. If a crash leaves
`evidence.json` present before the complete state is committed, rerun the same
command; exact bytes are verified before the journal advances. A complete
session rejects a different artifact but safely accepts an identical
finalization retry. For every phase, finalization recomputes the canonical
phase digest, rebuilds the complete expected checkpoint through the same
session/candidate/tree/status metadata constructor used at collection time,
and requires exact object equality with the root-owned journal. Editing only a
checkpoint's session, phase, candidate, tree, status, or digest fails.

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
