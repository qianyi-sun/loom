# Global fleet pool-executor dry run

Package 3 provides one pool-bound executor protocol for each physical Slurm
controller. It is deliberately usable only while the management authority has
`executable_new_capacity_ceiling = 0`. The code can reserve, order, inventory,
quarantine, and release dry-run records; it has no scheduler client, subprocess
entry point, or Slurm mutation surface.

Do not install a second global manager or one executor per development
environment. The single manager accounts for production, staging, `loom-dev`,
and every `loom-dev-<name>` subject together. OLDLAB and GB10 each have exactly
one controller-local executor incarnation and journal.

## Manager trust roots

The manager service requires its existing database, principal registry, and
mTLS files plus an ownership verification-key registry:

```text
LOOM_CAPACITY_PRINCIPALS_FILE=/run/loom-capacity/principals.json
LOOM_CAPACITY_DB_URL_FILE=/run/loom-capacity/database-url
LOOM_CAPACITY_EXPECTED_AUTHORITY_INCARNATION=<uuid>
LOOM_CAPACITY_TLS_CERT_FILE=/run/loom-capacity/tls.crt
LOOM_CAPACITY_TLS_KEY_FILE=/run/loom-capacity/tls.key
LOOM_CAPACITY_TLS_CLIENT_CA_FILE=/run/loom-capacity/client-ca.crt
LOOM_CAPACITY_OWNERSHIP_PUBLIC_KEYS_FILE=/run/loom-capacity/ownership-keys.json
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

## Controller binding

Each controller gets a separate mTLS certificate, bearer principal with only
`capacity:execute:pool`, owner-only bearer/TLS files, owner-only Ed25519 private
key, and an owner-only `0700` state directory. The principal is bound to the
exact pool ID, executor ID, and executor incarnation. The private signing key
never reaches the manager or any environment namespace.

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

Rollout-surge reservations remain fail-closed in Package 3: permit consumption
is rejected until the protected lifecycle package supplies the exact durable
old-worker drain acknowledgement. Do not treat the recorded surge pairing by
itself as proof that the old worker is already draining and nonclaimable.

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

Do not enable or install a scheduler-facing daemon from this package. Package 5
owns controller service installation, legacy freeze/adoption, exact Slurm
mutation proofs, rollback rehearsal, and the one transaction that can raise
the executable ceiling after #906 and #896 are satisfied.
