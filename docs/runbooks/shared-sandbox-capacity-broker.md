# Shared Sandbox Capacity Broker

The shared capacity broker is the single slot authority for the disposable
`sandbox-qianyi`, `sandbox-hongjian`, and `sandbox-devansh` Control Planes. It
does not run inside any sandbox and does not read a sandbox database. One
submit-host service owns its SQLite state and publishes candidate-bound,
secret-free grant handoffs for sandbox-specific adapters.

This repository slice is a broker and handoff contract. It does not authorize
Slurm policy changes, non-exclusive worker activation, shared-host mutation, or
production/staging reclaim. Those remain gated by their owning issues.

## Safety contract

- The sandbox allowlist is fixed to `qianyi`, `hongjian`, and `devansh`.
- Every request binds a full lowercase 40-character candidate SHA, one pool,
  `min_slots`, `target_slots`, bounded TTL, purpose, and preemptibility.
- All request, cancel, observation, fair-share, grant, and audit changes occur
  under one SQLite `BEGIN IMMEDIATE` transaction.
- The broker reserves a new grant as `pending_slots` before emitting it.
  Capacity is therefore counted before a sandbox adapter can submit work.
- A lower grant does not immediately free capacity. The previous
  `pending + active + draining` observation remains committed until the
  sandbox reports the new lease epoch drained.
- Global, per-pool, global-pending, and per-pool-pending budgets are checked
  before every one-slot grant. A final partial grant cannot overshoot.
- Grant handoffs contain no worker token, admin token, object-store credential,
  provider secret, private endpoint, or environment-file body.
- An observation with a stale timestamp, old lease epoch, wrong binding,
  regressing sequence or terminal count, rebound digest, or more nonterminal
  slots than the broker committed is rejected atomically. Exact replays do not
  refresh broker liveness.

The state database should be installed at:

```text
/var/lib/loom-shared-capacity/broker.sqlite3
```

The directory and database must be writable only by the dedicated broker
service identity. Sandboxes receive handoff JSON through a separately
authenticated transport; they must never receive write access to the database.
Back up the database and its WAL/SHM files as one SQLite unit.

Before that service identity exists, the safe bootstrap is a `root:root`
mode-`0700` directory with no database file. That state permits only explicit
root-invoked validation; do not relax the mode or transfer ownership to any
sandbox account as a shortcut.

## Request capacity

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  request \
  --sandbox qianyi \
  --candidate-sha 0123456789abcdef0123456789abcdef01234567 \
  --pool gb10 \
  --min-slots 20 \
  --target-slots 140 \
  --ttl-minutes 120 \
  --purpose large-batch-runtime-validation \
  --idempotency-key qianyi-gb10-runtime-validation-001 \
  --preemptible
```

The idempotency key may be replayed only with an identical request. It cannot
be rebound to another candidate or capacity shape.

## Reconcile grants

The submit-host supervisor supplies reviewed physical budgets on every pass:

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  reconcile \
  --global-budget 160 \
  --pool-budget gb10=140 \
  --pool-budget oldlab=20 \
  --global-pending-budget 40 \
  --pool-pending-budget gb10=30 \
  --pool-pending-budget oldlab=10
```

Allocation has two fair rounds:

1. rotate one slot at a time across eligible sandboxes until their requested
   minima or the available budget is reached;
2. continue the same aging-aware rotation toward target slots.

An idle pool can therefore burst to one sandbox's target. When several
sandboxes remain active, persisted `last_granted_seq` aging moves a scarce
slot to the least recently granted sandbox. The old holder first receives a
lower handoff and drains. The next holder is granted only after release is
observed, so fairness never creates temporary overcommit.

## Sandbox autoscaler handoff

Each `handoffs` item is an `AutoscalerGrantHandoff`:

```json
{
  "schema_version": 1,
  "request_id": "uuid",
  "lease_epoch": 3,
  "sandbox": "qianyi",
  "environment": "sandbox-qianyi",
  "candidate_sha": "0123456789abcdef0123456789abcdef01234567",
  "pool_name": "gb10",
  "enabled": true,
  "min_slots": 0,
  "max_slots": 47,
  "expires_at": "2026-07-27T22:00:00Z",
  "preemptible": true
}
```

The sandbox adapter must:

1. reject a handoff unless its sandbox, pool, and exact deployed candidate SHA
   all match;
2. reject a lease epoch older than its last applied epoch;
3. set the local autoscaler ceiling to `max_slots`; the broker remains the only
   authority that may raise that ceiling;
4. treat `enabled=false` or `max_slots=0` as drain-to-zero, not immediate proof
   that capacity was released;
5. return an observation for the same request and epoch.

This interface intentionally does not call the existing Control Plane
autoscaler API. The environment-specific adapter below owns authentication and
delivery, while the broker remains transport- and token-agnostic.

## Persistent sandbox handoff adapter

The environment-specific adapter is now
`scripts/ops/shared_capacity_adapter.py`. It is deliberately a separate
process from the broker:

- the broker remains the only process allowed to increase a grant;
- the adapter consumes one exact broker-produced `handoffs` item and cannot
  construct or expand a grant;
- the adapter validates sandbox, environment, pool, full candidate SHA,
  request ID, and monotonically increasing lease epoch before calling the
  sandbox Control Plane;
- each adapter config has a reviewed `max_slots_bound`; the checked-in GB10
  bound is `140` and the OLDLAB bound is `20`, and a larger handoff is rejected
  before any Control Plane call;
- the adapter reads the sandbox's `sandbox-state.json` and requires the local
  autoscaler policy's `actuator_config.candidate_sha` to match that same SHA
  and tree;
- every policy update carries the exact durable
  `shared_capacity_binding` (`request_id`, `lease_epoch`, `candidate_sha`, and
  `preemptible`), and the returned `capacity_lease_state` must confirm the
  requested active or retiring transition before state is committed;
- the admin credential is read from the existing mode-`0600` admin secret
  TOML. Neither the config, argv, result JSON, observation, nor durable adapter
  state contains the token;
- an expired or disabled handoff can only lower the local ceiling to zero;
- after at least one accepted handoff, a missing handoff reuses only its
  durable request/epoch/preemptibility binding and drains the local ceiling to
  zero; if adapter state is lost, that binding is recovered from the Control
  Plane lease state and can only be used to drain;
- enabling requires a fresh combined cross-domain runtime attestation for the
  exact sandbox, candidate SHA, and candidate tree; an invalid receipt blocks
  a first create and first disables an existing active policy with its exact
  binding before the adapter exits nonzero;
- activation holds a shared lock on
  `/var/lib/loom-shared-capacity/runtime-attestations/.collector.lock` from
  receipt validation through the policy PUT and independent GET readback. The
  collector takes the same lock exclusively before invalidating or replacing
  a receipt;
- immediately before mutation, the receipt must retain more lifetime than
  three configured HTTP timeout windows (GET, PUT, and readback). The adapter
  uses the live clock again after readback and reopens the receipt; expiry,
  deletion, or digest replacement causes an exact-bound disable/max-zero
  transition to `retiring` and a nonzero exit;
- a malformed, rewritten-same-epoch, regressing-epoch, or
  candidate-mismatched handoff fails without an autoscaler mutation;
- restart state is durable. Unknown Control Plane counters retain the last
  committed observation instead of releasing uncertain capacity.

Checked-in adapter configs cover both pools for all three sandboxes:

```text
deploy/developer-sandboxes/shared-capacity-adapters/
  qianyi-gb10.toml       qianyi-oldlab.toml
  hongjian-gb10.toml     hongjian-oldlab.toml
  devansh-gb10.toml      devansh-oldlab.toml
```

Each config contains paths and a secret-file reference, never a secret. The
installed contract is:

```text
/etc/loom/shared-capacity-adapters/<sandbox>-<pool>.toml
/srv/loom/developer-sandboxes/<sandbox>/secrets/admin.toml
/srv/loom/developer-sandboxes/<sandbox>/sandbox-state.json
/var/lib/loom-shared-capacity/handoffs/current/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/observations/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/adapters/<sandbox>-<pool>.json
/var/lib/loom-shared-capacity/runtime-attestations/<sandbox>/<SHA>/combined.json
```

The supervisor publishes an immutable generation directory containing all six
instance outcomes. A published outcome is either the **unaltered** matching
broker handoff or an explicit `status=absent` manifest entry. It fsyncs every
file and the generation directory, then commits the complete set with one
atomic `current` symlink replacement. Adapters only read through `current`; an
interrupted cycle therefore exposes the old complete generation or the new
complete generation, never a mixed six-file set. The current and immediately
previous generations are retained. Files are mode `0600`, directories are mode
`0700`, and a sandbox account has no write access.

The adapter writes a one-element JSON observation array compatible with
`--observations-json`. Its closed schema binds `sandbox`, `pool_name`,
`candidate_sha`, `request_id`, `lease_epoch`, the exact Control Plane
`capacity_lease_state`, `observed_at`, a durable monotonically increasing
`observation_sequence`, the four slot counters, and `payload_sha256`. The
digest is SHA-256 of canonical JSON with `payload_sha256` omitted. The sequence
is recovered from both adapter state and the last valid observation, so a crash
between the two atomic writes cannot rebind a sequence. Observation files are
inputs, never grants: the broker rejects old epochs, wrong bindings, stale
timestamps, sequence regression or rebinding, and nonterminal counts above its
commitment. An exact sequence-and-digest replay is ignored and does not refresh
`last_observed_at`.

### Persistent broker supervisor

`scripts/ops/shared_capacity_supervisor.py` owns the persistent broker cycle.
Its checked-in closed configuration is:

```text
deploy/developer-sandboxes/shared-capacity-supervisor/config.toml
```

The installed paths are:

```text
/etc/loom/shared-capacity-supervisor.toml
/var/lib/loom-shared-capacity/broker.sqlite3
/var/lib/loom-shared-capacity/supervisor-state.json
/var/lib/loom-shared-capacity/supervisor-audit.jsonl
/var/lib/loom-shared-capacity/evidence/supervisor-latest.json
```

One invocation performs this ordered cycle:

1. read broker status and reject more than one nonterminal request for any
   sandbox/pool instance;
2. read at most one observation from each of the six configured files;
3. accept an observation only when its sandbox, pool, candidate, request,
   epoch, policy lease state, timestamp, sequence, and digest exactly match the
   current broker binding. Stale or wrong-binding observations fail the cycle;
   an exact duplicate is recorded as `duplicate_ignored` and is not sent to
   the broker, so a dead adapter cannot renew liveness by replaying one file.
   The only old-request exception is an exact known terminal tombstone with
   zero committed and zero nonterminal slots; it is ignored without refreshing
   liveness so it cannot deadlock publication of the next request;
4. call `SharedCapacityBroker.reconcile` exactly once, so all accepted
   observations, TTL/cancel changes, fair-share decisions, budgets, leases,
   and broker audit events commit in one `BEGIN IMMEDIATE` transaction;
5. independently recompute the report aggregate and per-pool committed and
   pending slots. Publication is rejected unless the persisted budgets exactly
   equal the configured budgets and all of these bounds hold:

   ```text
   global slots <= 160       global pending <= 40
   gb10 slots <= 140         gb10 pending <= 30
   oldlab slots <= 20        oldlab pending <= 10
   ```

6. validate every handoff against its request ID, epoch, sandbox, environment,
   pool, candidate, grant, preemptibility, and enabled state, materialize all
   six published-or-absent outcomes in an immutable generation, fsync it, then
   publish the complete set with one atomic `current` symlink replacement;
7. append one secret-free, monotonically sequenced audit event and atomically
   update supervisor state and latest evidence.

A crash after the broker transaction but before the `current` replacement
cannot overshoot: capacity is already reserved by the broker and adapters still
see the prior complete generation. A crash after the replacement exposes the
new complete generation. The next idempotent cycle validates and reuses an
unchanged generation. Missing observations preserve the broker's prior
commitment and reduce utilization rather than freeing uncertain capacity.

### Initial sandbox autoscaler policies

A fresh sandbox Control Plane has no GB10 or OLDLAB autoscaler row. `bootstrap`
is the sole explicit exception to grant binding: it only creates a disabled,
max-zero, unbound policy, or validates an existing policy without mutating a
bound lease. It never consumes an enabled handoff. `run` owns the locked
grant-reconciliation path. The checked-in policy templates are:

```text
deploy/developer-sandboxes/shared-capacity-policies/gb10.toml
deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml
```

For a missing row, `bootstrap` creates the unbound disabled baseline. A valid
enabled first `run` can also create the policy directly as active with the
grant's exact binding.
A disabled or expired first handoff creates a bound scale-to-zero row in
`retiring`, which cannot activate until retirement completes and the broker
issues a newer epoch. The policy binds the exact sandbox candidate,
non-exclusive Slurm mode, the reviewed node allowlist, `loom-dev-<sandbox>` account,
`loom-dev` QoS, external-runner authority, and positive container CPU, memory,
and PID containment. `container_pids` is the per-container cap, while the exact
actuator `job_pids_max` is the Slurm-job aggregate; bootstrap rejects a policy
unless `job_pids_max >= container_pids * requested_concurrency`. Existing rows
are never silently overwritten:
any candidate, account/QoS, node, containment, or authority drift fails closed.
The admin token is read from the mode-`0600` TOML and never appears in argv,
policy files, output, state, or audit.

Manual read-only validation of the same idempotent path is:

```bash
python scripts/ops/shared_capacity_adapter.py \
  --config /etc/loom/shared-capacity-adapters/qianyi-gb10.toml \
  bootstrap
```

This never activates capacity. It creates or validates only the disabled
baseline and leaves an existing bound policy unchanged. The candidate checkout
remains immutable under the candidate namespace.
The secret-bearing worker env uses the separate canonical runtime path:

```text
/shared_work/loom/runtime/sandboxes/<sandbox>/<candidate-SHA>/worker-<pool>.env
```

OLDLAB and GB10 are independent NFS domains: the identical logical path does
not mean they share one backing file. A later persistent publisher must
materialize the exact candidate-bound mode-`0600` env independently in each
Slurm domain before authorized activation (`worker-oldlab.env` in OLDLAB and
`worker-gb10.env` in GB10). Missing, wrong-SHA, wrong-domain, or unsafe-mode
runtime env materialization is a fail-closed activation blocker.

The adapter accepts only the root-owned mode-`0600`, canonical combined receipt
at
`/var/lib/loom-shared-capacity/runtime-attestations/<sandbox>/<SHA>/combined.json`.
It must carry the exact candidate tree, a fresh collector window, the exact
OLDLAB and GB10 manifest/signature references and digests, and a fresh fleet
attestation reference. A Control-Plane-only receipt is insufficient. Formal
`external_runner` compatibility evidence remains an independent activation
prerequisite owned outside this adapter; the receipt does not replace it.

`loom-developer-sandbox-attestation-renewal.timer` runs every five minutes and
re-executes the exact candidate's fleet check, both domain publishers, and the
combined collector. It does not extend or rewrite an old receipt. Each verified
result is copied to an immutable root-owned mode-`0600` history record under
`.../<sandbox>/<SHA>/renewals/`, with a monotonic generation and digest link to
the previous record. The adapter continues to consume only the current
`combined.json`; the immutable series is the liveness evidence used by the
long-running acceptance gate. A reboot or manual sandbox-service start also
rebuilds an expired proof before lifecycle convergence.

Run one supervisor cycle manually:

```bash
python scripts/ops/shared_capacity_supervisor.py \
  --config /etc/loom/shared-capacity-supervisor.toml \
  run
```

The command emits only counts, digests, binding identifiers, and publication
status. Failure emits only:

```json
{"error":"shared-capacity-supervisor-failed-safely"}
```

Run one adapter manually for validation:

```bash
python scripts/ops/shared_capacity_adapter.py \
  --config /etc/loom/shared-capacity-adapters/qianyi-gb10.toml \
  run
```

The secret-free result reports the request, epoch, applied ceiling, expiry
state, and observation. Exit status `1` emits only:

```json
{"error":"shared-capacity-adapter-failed-safely"}
```

### Exact-candidate systemd contract

The checked-in adapter template and timer are:

```text
deploy/developer-sandboxes/loom-shared-capacity-adapter@.service
deploy/developer-sandboxes/loom-shared-capacity-adapter@.timer
```

Render the service against the full candidate SHA:

```bash
python scripts/ops/render_shared_capacity_adapter_service.py \
  --git-sha <full-lowercase-40-character-SHA> \
  > /tmp/loom-shared-capacity-adapter@.service
```

The renderer rejects a mutable path, an unresolved placeholder, or a partial
SHA. The installed unit runs from:

```text
/opt/loom-shared-capacity/candidates/<SHA>/repo
/opt/loom-shared-capacity/candidates/<SHA>/venv
```

The timer is a persistent 15-second oneshot schedule. Install and enablement
mutate shared-host systemd state and therefore require explicit live authority;
repository merge alone does not authorize them. Before activation, verify the
rendered service and timer with `systemd-analyze verify`, copy the reviewed
instance configs to `/etc/loom/shared-capacity-adapters`, and prove the admin
secret and sandbox-state files have their required ownership/modes.

The bootstrap unit runs as root because the three sandbox roots are private
mode `0700` under different owners and the broker root is root-only. Do not
weaken those modes. Replacing root requires a dedicated service identity plus
reviewed ACLs that grant only the exact state, handoff, observation, and secret
files needed by each instance.

The broker supervisor uses its own network-isolated exact-candidate unit:

```text
deploy/developer-sandboxes/loom-shared-capacity-supervisor.service
deploy/developer-sandboxes/loom-shared-capacity-supervisor.timer
```

Render it with:

```bash
python scripts/ops/render_shared_capacity_supervisor_service.py \
  --git-sha <full-lowercase-40-character-SHA> \
  > /tmp/loom-shared-capacity-supervisor.service
```

The supervisor timer also runs every 15 seconds and needs no network. The
persistent installer publishes all four unit templates in one transaction but
leaves the supervisor and every adapter disabled and inactive. A separate
activation transaction runs the supervisor once and reads back the complete
six-instance generation before it enables the supervisor timer or starts an
adapter. Both services render from the same immutable candidate SHA.

Do not assemble the candidate, virtual environment, configs, or units with
manual `cp`, mutable symlinks, or ad-hoc `systemctl` calls. The persistent
oldlab2 root converger owns that transaction:

```bash
sudo python3 scripts/ops/shared_capacity_runtime_host.py plan \
  --source-repo /root/loom-merged-dev \
  --candidate-sha <FULL_LOWERCASE_40_CHAR_SHA>

sudo python3 scripts/ops/shared_capacity_runtime_host.py install \
  --source-repo /root/loom-merged-dev \
  --candidate-sha <FULL_LOWERCASE_40_CHAR_SHA> \
  --execute

sudo /usr/local/libexec/loom-shared-capacity-runtime-host check \
  --candidate-sha <FULL_LOWERCASE_40_CHAR_SHA> \
  --mode installed \
  --execute

sudo /usr/local/libexec/loom-shared-capacity-runtime-host activate \
  --candidate-sha <FULL_LOWERCASE_40_CHAR_SHA> \
  --execute

sudo /usr/local/libexec/loom-shared-capacity-runtime-host check \
  --candidate-sha <FULL_LOWERCASE_40_CHAR_SHA> \
  --mode activated \
  --execute
```

The source must be a clean repository at the exact SHA/tree with its checked-in
`uv.lock`. Git verification ignores system and user configuration, rejects
replacement objects, alternates, grafts, shallow history, unsafe index modes
and flags, and any raw index/tree mismatch. Plan digests and installed files
are read from exact commit blobs, never from the mutable source worktree after
verification. The converger clones that immutable commit under
`/opt/loom-shared-capacity/candidates/<SHA>/repo`, creates
`/opt/loom-shared-capacity/candidates/<SHA>/venv` with `uv sync --frozen
--no-dev`, removes every write bit from the root-owned candidate, and renders
every `ExecStart` directly to that SHA. `/opt/loom-shared-capacity/current` is
an atomic audit pointer only; no service executes through it.

Before any candidate staging or systemd mutation, the converger durably records
the transaction-owned staging path, whether the final candidate pre-existed,
all files, and every unit's enabled/active state below the root-only
`/var/lib/loom-shared-capacity/runtime-host-installer/transactions`. Recovery
removes only journal-owned staging/final paths and rejects an unjournaled
`.install-*` orphan. `install` atomically publishes the six adapter configs,
supervisor config, exact units and timers, then explicitly leaves all fourteen
concrete services/timers disabled and inactive. It cannot consume a
pre-existing nonzero handoff.

`activate` is the only path that may start them. It first verifies the installed
SHA/tree and inactive state, the broker database and current handoff bindings,
all six exact configs, each referenced mode-`0600` admin secret file, each
sandbox-state candidate binding, and all six fresh combined cross-domain
runtime receipts. First activation additionally requires six explicit
`enabled=false`, `min_slots=0`, `max_slots=0` broker handoffs, zero broker
pending/active/draining/committed slots, and six Control Plane policies already
disabled at max zero with zero last pending/actual/draining/occupied/queued
slots. Unknown counters fail closed. This zero-capacity gate means a later
adapter-start failure cannot strand positive capacity written by an earlier
adapter. Tokens are read only inside the isolated candidate process and are
never emitted.

The same activation transaction first writes a token-bound admission fence into
the broker's SQLite authority under `BEGIN IMMEDIATE`. New requests are rejected
while that fence exists; an exact idempotent replay of an already recorded
request remains read-only. The zero gate requires the six selected requests to
be terminal, so a request racing the fence either commits first and makes
activation fail closed, or is rejected before insertion. The converger keeps
the fence through supervisor/adapter startup, activated-state readback, and the
durable `committed` journal update. It releases only its exact transaction token
after that commit. Crash recovery idempotently releases a committed fence or
restores the inactive snapshot before releasing a rolled-back fence; another
transaction's fence is never removed.

Activation then runs the supervisor once, validates its complete atomic
generation against fresh broker status, enables the supervisor timer, and
starts/enables each adapter timer in the closed order. Any failure restores the
pre-activation all-disabled unit snapshot while the zero-capacity CP policies
remain zero. Only `check --mode activated` accepts enabled timers and successful
service results; it also re-reads each adapter state and Control Plane policy
and requires either zero state or the exact current broker handoff binding.

The installed host profile is closed-world. It names exactly six instances and
records two independent scheduler routes: OLDLAB submits on
`trt-EAI-OLDLAB-2` to controller `TRT-EAI-OLDLAB-1`; GB10 submits to and is
controlled by `trt-gb10-1`. Both check modes require all six configs, query
both `list-units` and `list-unit-files`, and require every concrete unit's exact
fragment path. They reject an extra config, duplicate systemd readback, or any
loaded, installed, or enabled `loom-shared-capacity-adapter@...` instance
outside that allowlist, and never combine the two domains through one
controller. A seventh adapter or a cross-domain submit/controller fallback is
a #827 fail-closed blocker.

An interrupted transaction is restored before the next install. A completed
candidate can be rolled back only through its exact retained transaction:

```bash
sudo /usr/local/libexec/loom-shared-capacity-runtime-host rollback \
  --candidate-sha <CURRENT_FULL_SHA> \
  --execute
```

Rollback of an installed-but-inactive candidate stops the current timers,
restores the prior candidate pointer, configs, exact units, and each prior
enabled/active state, and removes the current candidate only when the journal
proves it did not pre-exist. An activated candidate uses an additional durable
retirement sequence. The retained install transaction first becomes the active
rollback journal and closes broker admission with that exact transaction ID.
It cancels only the current candidate's request in each of the exact three
sandboxes by two pools; a nonterminal request for another candidate fails
closed and is never cancelled.

While the fence remains closed, rollback repeatedly runs the supported
supervisor and six adapter service cycles. It does not restore local files until
all six broker requests and leases are terminal at zero, the current atomic
handoffs are disabled at zero, all six Control Plane policies and counters read
back disabled at zero, and the matching Control Plane worker/Slurm status has
no pending or running jobs. The journal records closing, draining, drained,
restoring, restored-with-fence, and admission-open phases. Recovery resumes the
recorded phase; a timeout, adapter failure, readback failure, or host restart
keeps the recoverable journal and admission fence in place.

Before closing admission, rollback copies the exact candidate's installer to
the fixed root-owned mode-`0700` recovery entrypoint
`/var/lib/loom-shared-capacity/runtime-host-installer/runtime-host-recovery`,
records its path and digest in the journal, and validates that binding on every
resume. This entrypoint is deliberately outside the install snapshot and
remains available after the public `/usr/local/libexec` program has been
restored to an older version or removed. After an interruption, resume from the
disk entrypoint rather than an old public wrapper:

```bash
sudo /var/lib/loom-shared-capacity/runtime-host-installer/runtime-host-recovery \
  recover \
  --execute
```

The recovery command takes the same installer lock and resumes only the exact
active journal; it does not infer a transaction from operator input. The
root-owned recovery entrypoint remains as the durable bootstrap for later
inspection and is refreshed by the next activated rollback.

Only after external capacity is proven retired does rollback restore the local
snapshot. It then reopens only its exact broker fence, records that durable
phase, and removes a journal-owned candidate last. `install` refuses to replace
an `activated` runtime, so this retirement cannot be bypassed by publishing a
new candidate. Never delete the installer state, active pointer, journal,
broker database, adapter state, observations, or audit files to force a
rollback.

### Shared-capacity retirement mapping

Control-plane
[`compute_autoscaler_decision`](../../src/loom_control_plane/worker_pool_autoscaler.py)
materializes disabled broker handoffs as a durable lease retirement, rather
than treating `enabled=false` as immediate proof that capacity disappeared.
Adapters that call
`PUT /admin/worker-pool-autoscaler-policies/{environment}/{pool_name}` must
preserve the exact request, epoch, candidate, and preemptibility binding:

| Handoff | WPAP lease transition |
|---------|-----------------------|
| `enabled=true`, `max_slots=N>0` | `enabled=true`, `min_slots=0`, `max_slots=N`, exact `shared_capacity_binding`; durable state `active` |
| `enabled=false` or `max_slots=0` | `enabled=false`, `min_slots=0`, `max_slots=0` with the same binding; `active → retiring`, then the retirement actuator drains to terminal Slurm readback before `retired` |

Never emulate retirement by keeping a disabled grant locally enabled. While
`retiring`, every retry keeps the exact request, epoch, candidate SHA, and
preemptible binding; it cannot switch requests. A missing row plus a valid
positive first grant is created directly as bound `active`; a zero-capacity
first handoff is bound `retiring` and cannot activate without a newer epoch.

Admin auth uses secret **files** only. Handoffs, observations, and evidence must
never contain tokens or object-store credentials.

### Adapter / supervisor ownership

The durable per-sandbox handoff adapter and observation/reconcile supervisor
are part of the single #1023 integration candidate:
`scripts/ops/shared_capacity_adapter.py`,
`scripts/ops/shared_capacity_supervisor.py`. This broker remains
transport- and token-agnostic; adapters own WPAP mutation against each
sandbox's loopback Control Plane (`sandbox-qianyi|hongjian|devansh`, not
`development`).

## Report observed capacity

Prepare a secret-free JSON array:

```json
[
  {
    "sandbox": "qianyi",
    "pool_name": "gb10",
    "candidate_sha": "0123456789abcdef0123456789abcdef01234567",
    "request_id": "11111111-1111-4111-8111-111111111111",
    "lease_epoch": 3,
    "capacity_lease_state": "active",
    "observed_at": "2026-07-28T18:00:00Z",
    "observation_sequence": 42,
    "pending_slots": 7,
    "active_slots": 40,
    "draining_slots": 0,
    "terminal_slots": 0,
    "payload_sha256": "4248f9a47934b09cad70cd2ec603f5c7f9ee5103d36dfb8f8294b2a318a0a76a"
  }
]
```

Then reconcile it with the same reviewed budgets:

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  reconcile \
  --global-budget 160 \
  --pool-budget gb10=140 \
  --pool-budget oldlab=20 \
  --global-pending-budget 40 \
  --pool-pending-budget gb10=30 \
  --pool-pending-budget oldlab=10 \
  --observations-json /var/lib/loom-shared-capacity/observations.json
```

The status output follows
[`shared-sandbox-capacity-evidence.schema.json`](../evidence/shared-sandbox-capacity-evidence.schema.json).
Its aggregate and each lease expose:

- requested slots;
- broker-granted slots;
- observed/reserved pending slots;
- active slots;
- draining slots;
- cumulative terminal slots;
- the last accepted observation timestamp, sequence, digest, and policy lease
  state;
- committed slots, defined as
  `max(granted, pending + active + draining)`.

## Cancel, TTL, and recovery

Cancel is drain-first:

```bash
python scripts/ops/shared_capacity_broker.py \
  --state-db /var/lib/loom-shared-capacity/broker.sqlite3 \
  cancel \
  --request-id REQUEST_UUID \
  --reason operator_cancelled
```

The handoff becomes `enabled=false`, `max_slots=0`, and its epoch increments.
The request stays `draining` while any pending, active, or draining slots are
observed. It becomes `terminal` only after a same-epoch zero-nonterminal
observation. TTL expiry uses the same path with terminal reason `ttl_expired`.

After a broker restart, run `status`, query the three sandbox adapters and
Slurm for the current epoch observations, then run `reconcile`. Do not edit the
SQLite tables or reduce observed slot counts by hand. A missing or uncertain
observation keeps capacity committed and fails toward lower utilization.

After an adapter restart, its durable state fences replay. Reapplying an
identical handoff is idempotent. Do not delete adapter state to bypass an epoch
error: cancel the broker request, publish the zero-ceiling handoff, and wait
for a same-epoch zero-nonterminal observation first.

After a supervisor restart, the last sequence is recovered from both its
state and append-only audit tail. A config digest change is rejected while any
capacity remains committed. Drain all leases before changing budgets, paths,
or instance membership; never delete supervisor state or audit to bypass that
gate.

## Rollback

Rollback is drain-first and preserves authority records:

1. cancel each nonterminal broker request;
2. reconcile and atomically publish the resulting `enabled=false`,
   `max_slots=0` handoffs;
3. leave the adapter timers running until every same-epoch observation reports
   zero pending, active, and draining slots and broker status is terminal;
4. stop and disable the six adapter timers, then stop and disable the
   supervisor timer;
5. retain `broker.sqlite3`, WAL/SHM files, adapter states, observations, and
   audit/evidence JSON as one incident/recovery record.

Never roll back by deleting the SQLite database, editing a lease epoch,
lowering an observation by hand, removing the handoff before drain completes,
or restoring a larger local autoscaler `max_slots`. Named sandbox volumes are
outside this adapter rollback and remain governed by the developer-sandbox
destroy contract.

## Production-pressure drain and retry acceptance

The broker grant ceiling and the Control Plane production-pressure intent are
separate, composable authorities. A production-pressure signal must first
fence new claims through
`POST /admin/worker-pools/<sandbox-environment>/<pool>/prod-pressure`. The
installed submit-host external autoscaler is then the only process allowed to
run `scancel`.

For Slurm pools the durable drain contract is:

- pending jobs and running jobs with zero in-flight trials are cancelled on the
  next external reconcile, without waiting for the preemption grace period;
- busy non-preemptible jobs remain `draining` until their trials finish, then
  the next reconcile cancels the now-idle allocation;
- busy preemptible jobs are not cancelled until the recorded grace period has
  elapsed;
- a successful `scancel` does not release broker or Control Plane capacity.
  The Slurm job must read back terminal first;
- a successful cancel awaiting terminal read-back is persisted on the
  `SlurmWorkerJob`. A submit-host restart observes it instead of issuing a
  duplicate `scancel`;
- a failed `scancel` leaves the job and worker draining. It is not marked
  released and its active trials are not marked interrupted;
- after terminal read-back, an authorized preemption records
  `failure_reason=prod_capacity_pressure`; the crash detector preserves that
  attribution when it returns the interrupted trial to `queued` with retry
  backoff;
- pressure recovery reactivates only controller-owned draining workers that
  still have live Slurm jobs. A cancellation already in flight is settled
  before normal scaling resumes.

`LIVE SANDBOX + SLURM AUTHORITY REQUIRED`

Exercise this only through the exact installed bridge and external autoscaler
services. Do not run an ambient checkout or call `scancel` manually. Capture
the secret-free outputs of:

```bash
systemctl status loom-prod-pressure-worker-control.timer
systemctl --user status 'loom-autoscaler-*.timer'
squeue -h -o '%i|%T|%j|%a|%q|%R'
```

For each `sandbox-{qianyi,hongjian,devansh}` and each reviewed pool, the
acceptance sequence is:

1. record the current autoscaler status, active Slurm jobs, worker drain states,
   and in-flight trial counts;
2. apply a sanitized nonzero pressure snapshot through the installed bridge;
3. prove claims are fenced before the next actuator tick;
4. prove pending/idle jobs disappear only after terminal Slurm read-back;
5. keep one busy non-preemptible job and prove it is not cancelled, finish its
   trial, then prove the next cycle releases it;
6. keep one busy preemptible job through the grace boundary, prove terminal
   read-back and the persistent retry attribution, then prove crash reclaim
   returns the trial to `queued`;
7. interrupt/restart the external service between `scancel` and terminal
   read-back and prove no duplicate cancellation or lost retry attribution;
8. clear pressure and prove only still-live held workers return to `active`.

The autoscaler decision reason is the operator status surface. It reports
secret-free counts for `cancelled`, `awaiting_terminal`, `cancel_failed`,
`held_busy`, and `retryable_trials`. Any nonzero `cancel_failed` count is a
stop condition; repair the installed Slurm authority and let the timer retry.
## Evidence retention

Persist the complete JSON output from every accepted request, reconcile,
cancel, and final terminal status. The append-only audit stream records only
bounded identifiers, counts, candidate SHA, and reasons. It intentionally
contains no token-shaped field or raw sandbox configuration.
