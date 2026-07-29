# Staging pressure-reclaim authority

This root-owned authority produces the live scheduling receipt required by the
developer-sandbox acceptance contract. It is deliberately narrower than a
general pressure tool:

- the environment is always `staging`;
- one receipt covers only the reviewed `gb10` pool;
- every active Loom Slurm job in that pool must belong to the exact registered
  acceptance session;
- any foreign Loom job fails the run before pressure is posted;
- every non-Loom job visible in the `gb10` partition is recorded before,
  during, and after reclaim and must be unchanged;
- the interrupted and claim-probe trials are acceptance-owned and bound by
  exact IDs; and
- no result from this authority is production evidence or evidence for closing
  #489.

The authority uses the existing actuator-neutral endpoint:

`POST /admin/worker-pools/staging/gb10/prod-pressure`

The Control Plane records the claim fence, and the existing Slurm autoscaler
actor owns draining, `scancel`, terminal readback, and retryable attribution.
This authority does not run `scancel` itself.

`prod_capacity_pressure` in a receipt is the Control Plane's existing
retryable failure-reason enum. It does not mean that this staging-only
authority queried, authenticated to, or mutated a production Control Plane.

## Persistent state and trust

The installed state root is
`/var/lib/loom-staging-pressure-reclaim`, root-owned mode `0700`. It contains:

- `sessions/<session-id>.json`: immutable root-reviewed input;
- `transactions/<session-id>.json`: crash-recovery journal;
- `receipts/<session-id>.json`: immutable canonical receipt;
- `receipts/<session-id>.sig`: canonical signature envelope;
- `high-water/gb10.json`: monotonic pool sequence; and
- `current.json`: current committed receipt pointer.

Files are root-owned, single-link regular files, mode `0600`. Receipts are
Ed25519-signed with the fixed key pair under
`/etc/loom/staging-pressure-reclaim`; both the private key and the locally
pinned public key are root-owned mode `0600`. Bootstrap publishes each key with
an fsynced atomic no-replace operation. A crash after private-key publication
rolls forward by deriving the missing public key; bootstrap never deletes,
replaces, or repairs an unsafe or foreign key leaf. Existing pairs are
cryptographically read back with a real Ed25519 sign/verify challenge and are
not rewritten. A session retry rolls forward from its durable phase. The
producer never derives a path from a receipt or accepts a caller-provided
receipt path.

The installed CLI also has no config-path option. Live commands always read the
fixed `/etc/loom/staging-pressure-reclaim-authority.toml`; tests may inject a
temporary path by calling the internal config loader directly.
The loopback Control Plane URL, both token paths, node transport, published
receipt root, and both key paths are exact compiled bindings; changing any one
in TOML fails config loading.

## Install prerequisites

The direct-root node-authority `bootstrap` and `upgrade` transactions install
and read back only these candidate-owned files:

- `scripts/ops/staging_pressure_reclaim_authority.py` as
  `/usr/local/libexec/loom-staging-pressure-reclaim-authority`;
- the TOML profile as
  `/etc/loom/staging-pressure-reclaim-authority.toml`;
- the systemd unit and sudoers fragment.

These are system mappings, not merely copies under the node authority's
`SOURCE_ROOT`. Upgrade first snapshots each exact target, including whether it
was absent. It disables the affected sudoers entries, replaces the scripts,
config, service, and sudoers bytes, validates both sudoers fragments with
`visudo`, validates both service units with `systemd-analyze`, reloads systemd,
and exact-matches mode and SHA-256 readback. Any failure restores prior bytes
and removes targets that were absent before the upgrade.

The two token files are external, direct-root secret prerequisites. They are
not repository assets and the node authority does not create, copy, install,
rotate, hash, or report them. Before bootstrap, an administrator must provision:

- `/etc/loom/staging-pressure-reclaim/admin-token`, with
  `admin:worker_pools` and `admin:slurm_workers` and access to read the two
  acceptance trials; and
- `/etc/loom/staging-pressure-reclaim/acceptance-worker-token`, with
  `worker:claim` and `worker:report`.

Each must be a root-owned, root-group, single-link regular file with exact mode
`0600` beneath a root-owned, non-group/world-writable, non-symlink parent chain.
`bootstrap` and `check` validate these prerequisites but never emit token
content, a token digest, or an Authorization header.

The node authority must expose the fixed check action
`staging-pressure-reclaim-observe` only on `trt-gb10-1`, dispatching the
candidate-installed program's `observe-slurm` command. Until that action is
installed and candidate-bound, producer runs fail closed.

Bootstrap the durable state and Ed25519 key pair once as direct root:

```console
sudo /usr/local/libexec/loom-staging-pressure-reclaim-authority bootstrap --execute
```

On a clean install both key leaves must be absent. A valid private-only state is
the one supported crash-recovery case. A public-only state, RSA key, mismatched
pair, symlink, hardlink, wrong owner/group, or wrong mode fails closed and is
preserved for investigation. Verify the complete installed boundary without
mutating persistent authority state:

```console
sudo /usr/local/libexec/loom-staging-pressure-reclaim-authority check
```

Do not grant `bootstrap` or `register-session` through sudoers.

## Register one reviewed session

The input must be canonical JSON with a trailing newline and have this closed
shape:

```json
{"acceptance_session_id":"11111111111111111111111111111111","candidate_sha":"0000000000000000000000000000000000000000","candidate_tree":"1111111111111111111111111111111111111111","claim_probe":{"caps":[{"cpu_arch":"x86_64","gpu_vendor":"nvidia","mounted_fs":true,"network_policies":["public"],"os":"linux","resource_modes":["auto"]}],"task_id":"pressure-claim-probe","team_id":"00000000-0000-0000-0000-000000000002","trial_id":"00000000-0000-0000-0000-000000000003","worker_id":"00000000-0000-0000-0000-000000000004"},"created_at":"2026-07-29T12:00:00Z","environment":"staging","expires_at":"2026-07-29T14:00:00Z","interrupted_trial":{"task_id":"pressure-interrupted","team_id":"00000000-0000-0000-0000-000000000002","trial_id":"00000000-0000-0000-0000-000000000005","worker_id":"00000000-0000-0000-0000-000000000007"},"kind":"loom.staging-pressure-reclaim.session","loom_jobs":[{"compose_project":"loom-pressure-acceptance","job_id":"12345","job_name":"loom-pressure","registry_id":"00000000-0000-0000-0000-000000000006","sandbox_identity":"qianyi","slurm_account":"loom-staging","slurm_qos":"loom-staging","slurm_user":"loom-staging-worker","worker_id":"00000000-0000-0000-0000-000000000007"}],"pool":"gb10","schema_version":1,"session_id":"00000000-0000-0000-0000-000000000001"}
```

Each registered active job must carry these non-secret environment markers in
the Control Plane Slurm registry:

```text
LOOM_STAGING_PRESSURE_SESSION_ID=<exact session UUID>
LOOM_STAGING_PRESSURE_OWNERSHIP=acceptance-owned
```

The interrupted trial must already be `claimed` or `running` on the
acceptance-owned allocation. The claim-probe trial must be queued, capability
compatible, and the only trial eligible for its distinct probe worker.

Registration is a direct-root review boundary:

```console
sudo /usr/local/libexec/loom-staging-pressure-reclaim-authority \
  register-session --source /root/reviewed-pressure-session.json
```

## Run and verify

After registration, the rollout identity may run the exact session:

```console
sudo -u loom-rollout sudo \
  /usr/local/libexec/loom-staging-pressure-reclaim-authority \
  run --session-id 00000000-0000-0000-0000-000000000001
```

The transaction proves, in order:

1. closed-world Loom ownership and baseline Slurm peers;
2. active pressure and a `204` claim fence;
3. terminal owned jobs and `prod_capacity_pressure` retry attribution;
4. cleared pressure and an exact acceptance-owned recovered claim;
5. immediate safe requeue of that probe claim; and
6. unchanged non-Loom peers.

On commit the producer also writes one immutable signed wrapper to
`/srv/loom/staging-shared/results/pressure-reclaim/<acceptance-session-id>/<authority-session-id>.json`.
The live-acceptance importer derives that path from the two canonical session
identities, verifies its Ed25519 signature against the local pinned public key,
and binds it to the exact promotion SHA/tree and staging regression window.

Verify a receipt or the monotonic current pointer:

```console
sudo /usr/local/libexec/loom-staging-pressure-reclaim-authority \
  verify --session-id 00000000-0000-0000-0000-000000000001
sudo systemctl start loom-staging-pressure-reclaim-authority.service
```

If a run stops after pressure was posted, rerun the same session ID. Never
register a replacement session merely to bypass a failed phase. A foreign Loom
job, changed peer snapshot, mismatched trial, missing terminal readback, or
signature/high-water mismatch requires investigation; the authority must not
be forced past those gates.
