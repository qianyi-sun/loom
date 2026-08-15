# Global fleet pool-executor dry run

Loom provides one pool-bound dry-run executor protocol for each physical Slurm
controller. It is usable only while the management authority has
`executable_new_capacity_ceiling = 0`. The code can reserve, order, inventory,
quarantine, and release dry-run records; it has no scheduler client, subprocess
entry point, or Slurm mutation surface.

Do not install a second global manager or one executor per development
environment. The single manager runs in the shared `loom-dev` infrastructure
namespace. That Kubernetes namespace is not the logical shared-development
demand subject. The manager accounts for all four demand classes together:
production; staging; shared development (the logical `development` subject);
and personal development (each `dev-<name>` subject backed by a
`loom-dev-<name>` application namespace). OLDLAB and GB10 each have exactly one
controller-local executor incarnation and journal.

## Render and status evidence

The checked-in `deploy/dev-fleet/capacity-control-plane.toml` packages only the
global management authority. `deploy/dev-fleet/shared-fixture.yaml` and the
`loom-global-dev-fleet-autoscaler.*` SQLite timer remain legacy-inert; never
install either beside the global writer.

Render a reviewed candidate to an owner-only evidence file. Replace the
shape-valid example digest and UUID with the reviewed release values:

```bash
install -d -m 0700 artifacts/capacity
render_path="$(mktemp artifacts/capacity/control-plane.XXXXXX.yaml)"
uv run --no-sync loom admin capacity-control-plane render \
  --file deploy/dev-fleet/capacity-control-plane.toml \
  --manager-image ghcr.io/qianyi-sun/loom-capacity-manager@sha256:1111111111111111111111111111111111111111111111111111111111111111 \
  --authority-incarnation 11111111-1111-4111-8111-111111111111 \
  > "$render_path"
printf 'rendered evidence: %s\n' "$render_path" >&2
```

Do not apply that file during this repository slice. A Kubernetes apply is a
live #906 operator-change-window action and requires all freeze, adoption,
executor, and rollback evidence first.

Only after such an authorized deployment, capture the read-only mTLS status:

```bash
set -o pipefail
install -d -m 0700 artifacts/capacity
status_path="$(mktemp artifacts/capacity/status.XXXXXX.json)"
uv run --no-sync loom admin capacity-control-plane status \
  --namespace loom-dev \
  --kubeconfig /absolute/path/to/kubeconfig \
  | tee "$status_path"
```

The sole successful response is
`{"executable_new_capacity_ceiling":0,"status":"ready"}` followed by a newline.
A nonzero command exit or any other response fails the rehearsal gate.

The existing `loom-capacity-manager` Kubernetes Secret must provide these exact
keys; neither command accepts their plaintext values:

- `postgres-user`, `postgres-password`, `postgres-database`, `database-url`;
- `principals.json`, `ownership-public-keys.json`;
- `server-ca.pem`, `server-certificate.pem`, `server-private-key.pem`,
  `client-ca.pem`; and
- `health-certificate.pem`, `health-private-key.pem`.

Only the credential-preparation init containers mount the projected Secret.
The manager and migration application containers mount only the prepared
memory-backed credential directory, read-only; every prepared file is a
UID-owned mode-0600 regular nonsymlink file. Percent-encoded database URLs are
supported without changing their SQLAlchemy meaning because percent escaping
is confined to Alembic's ConfigParser boundary.

The first reviewed bootstrap-authority replacement atomically appends an audit
marker. Exact replay backfills a marker missing from an earlier bootstrap
implementation and is otherwise idempotent. Duplicate, contradictory, or later
different binding evidence fails closed. The DNS-label-safe, length-bounded
migration Job name combines the migration head and manager-image digest with a
digest of the canonical complete Job spec and exact head, so any immutable spec
change produces a new Job.

## Manager trust roots

The manager service requires its existing database, principal registry, and
mTLS files plus an ownership verification-key registry:

```text
LOOM_CAPACITY_PRINCIPALS_FILE=/var/run/loom-capacity-manager/runtime/credentials/principals.json
LOOM_CAPACITY_DB_URL_FILE=/var/run/loom-capacity-manager/runtime/credentials/database-url
LOOM_CAPACITY_EXPECTED_AUTHORITY_INCARNATION=11111111-1111-4111-8111-111111111111
LOOM_CAPACITY_TLS_CERT_FILE=/var/run/loom-capacity-manager/runtime/credentials/server-certificate.pem
LOOM_CAPACITY_TLS_KEY_FILE=/var/run/loom-capacity-manager/runtime/credentials/server-private-key.pem
LOOM_CAPACITY_TLS_CLIENT_CA_FILE=/var/run/loom-capacity-manager/runtime/credentials/client-ca.pem
LOOM_CAPACITY_OWNERSHIP_PUBLIC_KEYS_FILE=/var/run/loom-capacity-manager/runtime/credentials/ownership-public-keys.json
```

Every referenced secret file must be a regular nonsymlink file owned by the
service UID with mode `0600`. The ownership registry is bounded, strict JSON on
one line. Each raw Ed25519 public key appears under exactly one key ID:

```json
{"schema_version":1,"keys":[{"signing_key_id":"oldlab-key-1","public_key_base64":"<canonical-base64-32-bytes>"},{"signing_key_id":"gb10-key-1","public_key_base64":"<canonical-base64-32-bytes>"}]}
```

Executor registration fails unless its key ID and SHA-256 fingerprint match
that registry exactly. Rotation retains old verification keys until all jobs
and reservations signed by them are terminal.

The manager server certificate must contain both the `127.0.0.1` IP SAN used by
the in-Pod health request and the
`loom-capacity-manager.loom-dev.svc.cluster.local` DNS SAN used by trusted
capacity agents. The health probe parses the mounted server certificate and
fails unless both exact identities are present before attempting HTTPS.

## Controller binding

Each controller gets a separate mTLS certificate, bearer principal with only
`capacity:execute:pool`, owner-only bearer/TLS files, owner-only Ed25519 private
key, and an owner-only `0700` state directory. The principal is bound to the
exact pool ID, executor ID, and executor incarnation. In the schema-version-1
principal registry, `executor_pool_generation` remains optional so legacy v1
executors can keep using every `/v1/executors/...` route. Every `/v2/executors/...`
route additionally requires the same principal to carry a positive
`executor_pool_generation` exactly matching the prepared v2 pool binding. The
private signing key never reaches the manager or any environment namespace.

Construct `DryRunExecutorBinding`, `ExecutorConnection`, and
`CapacityExecutorClient.from_files(...)`, then open `ExecutorJournal` and wrap
the client in `RemoteDryRunPoolExecutor`. The remote executor:

1. fetches its exact central checkpoint over mTLS;
2. proves the local journal still contains that checkpoint;
3. fsyncs a request record before every transition;
4. sends one canonical, pool-bound `executable=false` contract;
5. validates the bounded exact receipt; and
6. fsyncs confirmation only after that validation.

After a timeout, restart with the same journal and redeliver the exact command.
A verified manager 4xx response is a definitive rejection and receives an
fsynced `*-rejected` journal record; transport loss and 5xx responses remain
pending because the central outcome may be ambiguous.
A missing, corrupt, regressing, permission-broadened, or different journal/key
must fence the incarnation. Never create an empty replacement journal to make
old work appear absent. An expired incumbent may renew only with its exact
retained checkpoint; fenced or replaced incarnations cannot recover this way.

Capacity increases require the latest pool observation to be valid, eligible,
and fresh by management-database receipt time at proposal, acceptance, permit
issue, and permit consumption. An unavailable pool may still heartbeat and
publish complete inventory so it can recover without losing accounting state,
but it cannot advance any capacity-increase transition.

Rollout-surge reservations remain fail-closed: permit consumption is rejected
without the exact durable old-worker drain acknowledgement from the protected
lifecycle authority. Do not treat the recorded surge pairing by itself as
proof that the old worker is already draining and nonclaimable.

## Protected environment release fence

An accepted shape remains charged until both its pool evidence and its exact
environment-agent fence exist. For an unsubmitted or terminal shape, use this
ordering:

1. the pool executor commits the central intent-close transition and fsyncs it
   in its controller-local journal;
2. an environment-agent replica builds `PreparedProtectedReleaseV1` from the
   exact prepared plan, tranche, intent, manager epochs, and pool generation;
3. `CapacityPreparedAdmissionStore.acknowledge_protected_release(...)`
   serializes on the protected runtime authority, verifies the local bootstrap
   high-water, rejects any prepared worker, and appends the release fence;
4. only after that local transaction commits, the same replica calls
   `DemandReporterClient.publish_protected_release(...)` with a durable UUID
   idempotency key; and
5. the pool executor may submit terminal or unused evidence only after the
   manager has accepted that exact subject-agent acknowledgement.

The protected release row is append-only and permanently blocks a delayed
bootstrap or worker insert for the stable shape identity. Concurrent agent
replicas converge on the same canonical acknowledgement. A different release
identity, payload, bootstrap high-water, reporter incarnation, or manager
fence fails closed. If publication times out, retain the local row and replay
the same payload and idempotency key; never manufacture a new local fence to
make the manager accept a changed acknowledgement.

## Rehearsal gate

For both controllers, verify registration, checkpoint, heartbeat, complete
inventory, acceptance, bootstrap, permit ordering, ambiguous retry,
quarantine, partial release, restart, and local-lock exclusion. During the
entire rehearsal:

- the manager health response must continue to report ceiling zero;
- no service or test may call `sbatch`, `scancel`, signal a job, or release a
  live worker;
- ambiguous or missing inventory stays charged or quarantined; and
- evidence is retained separately for OLDLAB and GB10.

Do not enable or install this dry-run surface as a scheduler-facing daemon. It
contains no scheduler mutation client or process-execution entry point, and the
manager refuses a non-zero executable ceiling. Existing executable capacity
controllers are separate authorities and cannot treat these receipts as
permission to launch, cancel, signal, or release physical capacity.

The controller-local read-only observer is the separate
`loom_capacity_pool_executor` namespace in the Loom wheel. It may use only
`scontrol show nodes --json` and `squeue --json`; the latter brackets the node
read. The exact queue argv must run with protected `SQUEUE_ALL=1` under the
dedicated non-root query UID. Before rehearsal, retain reviewed evidence that
the named query principal has complete job visibility under the controller's
`PrivateData` policy, place that evidence digest in the protected inventory
policy, and verify the running effective UID matches. It requires an
allocation-stable queue, exact protected Slurm 23.11 controller/parser
identity, exact per-node resources and partitions, and digest-bound fixed
binaries plus a root-owned protected Slurm config. Node-less jobs use a
validated comma-separated partition set and are charged when any listed
partition can reach protected nodes. Visible jobs are reconciled with node
allocation counters; hidden residuals, malformed partition lists, compact
arrays, and other ambiguity fail closed or quarantine the full affected
capacity. The paired shadow/executable inventory carries the manager's exact
journal checkpoint and binds the full-visibility identity/evidence. This
observer does not change the rehearsal rule above: it is not a daemon, all
foreign or ambiguous records stay quarantined, and its presence does not
permit installing or activating a pool executor.
