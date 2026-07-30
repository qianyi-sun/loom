# Developer sandbox cross-domain runtime publication

This runbook publishes one exact registry-owned developer-environment
candidate and its private worker environment into the independent OLDLAB and
GB10 `/shared_work` NFS domains. Identical path strings do not imply shared
storage: the publisher converges and verifies each domain separately.

This is a root/admin installation path for issue #1023. It does not grant
any developer general sudo on GB10, activate a worker, submit a
Slurm job, or authorize staging/production. Every peer first requires the
external-root exact-tree bootstrap documented in
`docs/runbooks/developer-sandboxes.md`. After bootstrap, oldlab2 may invoke
only the fixed node-authority `transact` and `check` commands; it never receives
raw `install`, `tar`, `rm`, `chown`, `chmod`, `python3`, or candidate-path
authority. If that fixed GB10 authority is unavailable, publication remains
fail-closed. Docker group membership alone is not a runtime-authority
fallback. Changing the pinned source tree uses the same runbook's journaled
`authority-upgrade` privileged-Docker transaction; manual deletion, an
operator-invoked internal Python entrypoint, or in-place source replacement is
unsupported.

The root-owned registry snapshot is the only environment-membership source.
It supplies the runtime ID, service user/group and UID/GID, candidate and
runtime roots, ports, Slurm identity, resource generation, and current
candidate. There is no developer-name allowlist or fixed cohort size.
Registering a fourth, fifth, or later authorized developer therefore requires
no profile copy, port selection, authority rebuild, or peer-environment
handoff.

## Stable identity and path contract

For a normal `dynamic-v1` environment, both domains use the exact
registry-assigned environment roots:

```text
/shared_work/loom/candidates/environments/<env_id>/<40-hex-SHA>
/shared_work/loom/runtime/environments/<env_id>/<SHA>/worker-oldlab.env
/shared_work/loom/runtime/environments/<env_id>/<SHA>/worker-gb10.env
```

The candidate directory is a standalone clean Git checkout at that exact
commit and tree. It is atomically published with Linux `renameat2(...,
RENAME_NOREPLACE)`, owned by `root:sharedwork`, and read-only to the
`sharedwork` group. A final-name collision is verified, never replaced. Files
and directories are recursively checked for exact ownership and absence of
group/world write bits.

The fixed three `legacy-v1` migration records retain their pre-registry paths
only so migration does not rename existing resources:

```text
/shared_work/loom/candidates/sandboxes/<sandbox>/<40-hex-SHA>
/shared_work/loom/runtime/sandboxes/<sandbox>/<SHA>/worker-oldlab.env
/shared_work/loom/runtime/sandboxes/<sandbox>/<SHA>/worker-gb10.env
```

New environments must never be placed under `sandboxes/`. The publisher takes
the candidate and runtime roots from the verified registry binding, creates
those exact roots independently in each NFS domain, and never derives a path
from a developer name.

Each private env is owned by the registry-assigned non-login batch service identity at
mode `0600`; its sandbox and SHA parents are mode `2750` with that identity's
dedicated primary group. `sharedwork`, the human developer account, and other
environment service identities never get secret-env access.

The following checked-in identities are only `legacy-v1` migration seeds and
examples. They are not the current membership list:

| Sandbox | Non-login batch identity | UID:GID |
| --- | --- | ---: |
| qianyi | `loom-sandbox-qianyi` | `31021:31021` |
| hongjian | `loom-sandbox-hongjian` | `31022:31022` |
| devansh | `loom-sandbox-devansh` | `31023:31023` |

For every registry environment, the node authority creates or verifies only
its exact assigned identity with `/nonexistent` and `/usr/sbin/nologin`, after
proving the name and numbers are free or already exact on all 20 nodes. Every
peer compares its local passwd/group identity, the NFS inode, and the env owner
UID/GID before the publisher signs an attestation. Human UID drift therefore
cannot become an NFS authorization input. Any UID/GID collision, login-capable
metadata, explicit dedicated-group member, cross-peer identity drift, or
`sharedwork` GID drift fails before publication.

The existing `loom-rollout` identity is not part of this contract. Its absence
on an OLDLAB client cannot silently become a numeric-UID authorization.

## Host-local remote-link prerequisite

The worker env must exact-bind:

```text
LOOM_WORKER_CONTROL_PLANE_URL=http://sandbox-link:8080
LOOM_WORKER_GATEWAY_URL=http://sandbox-link:9100
LOOM_WORKER_MINIO_ENDPOINT=http://sandbox-link:9000
LOOM_WORKER_SANDBOX_IDENTITY=<sandbox>
LOOM_WORKER_CANDIDATE_SHA=<SHA>
LOOM_WORKER_POOL_NAME=<oldlab|gb10>
LOOM_WORKER_MAX_CONCURRENT=<4 for oldlab|8 for gb10>
LOOM_WORKER_TOKEN_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/worker-token
LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/minio-access-key
LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/minio-secret-key
LOOM_WORKER_CP_TLS_CA_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/ca.pem
LOOM_WORKER_CP_TLS_CERT_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/client.pem
LOOM_WORKER_CP_TLS_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/client-key.pem
```

Pool and concurrency are not publication-time inputs. The host installer reads
them from the exact candidate's checked-in
`deploy/developer-sandboxes/shared-capacity-policies/<domain>.toml`, where
`policy.actuator_config.requested_concurrency` is `4` for OLDLAB and `8` for
GB10. The runtime-domain closed schema binds the same source, pool, and value;
an OLDLAB seed cannot be published into GB10 (or vice versa), and an operator
cannot lower or raise the effective worker concurrency with a CLI override.

The worker addresses only the host-local `sandbox-link` relay. Listener and
loopback target ports are read from the exact registry environment. The
following values document only the three `legacy-v1` migration seeds:

| Sandbox | Control-plane listener → target | Gateway listener → target | MinIO listener → target |
| --- | --- | --- | --- |
| qianyi | 26080 → 20080 | 26100 → 20100 | 26900 → 20900 |
| hongjian | 27080 → 21080 | 27100 → 21100 | 27900 → 21900 |
| devansh | 28080 → 22080 | 28100 → 22100 | 28900 → 22900 |

All oldlab2 targets are `127.0.0.1`; worker-host routes terminate at
`192.168.50.14`. The publisher validates every literal URL, sandbox/SHA
identity, and host path. It rejects raw tokens, MinIO credentials, provider
keys, passwords, private keys, and inline PEM material. The env is a
secret-free reference manifest, not a secret store.

The referenced token, MinIO credentials, CA, certificate, and private key are
installed separately on every worker host by the root-owned developer-sandbox
connectivity installer. Secret content must never be placed on NFS. This
publisher deliberately does not open or copy the referenced host files;
missing host-local bundle convergence remains a hard worker-start
prerequisite, proved by the fleet attestation below.

Env contents remain redacted from plans, receipts, peer readbacks, and errors.
The rollback copy is stored only below the root-owned mode-`0700`
`/var/lib/loom-developer-domain-runtime`.

## Install identities and the fixed helper on every peer

Use the exact candidate checkout containing this helper and config. Run the
plan on every declared peer:

```bash
sudo python3 scripts/ops/developer_sandbox_domain_runtime.py host-converge \
  --config deploy/developer-sandboxes/runtime-domains.toml \
  --domain oldlab
```

The plan reports group and `sharedwork` membership changes without mutation.
Apply only through the domain's root/admin path:

```bash
sudo python3 scripts/ops/developer_sandbox_domain_runtime.py host-converge \
  --config deploy/developer-sandboxes/runtime-domains.toml \
  --domain oldlab \
  --execute
```

Apply the analogous `--domain gb10` command independently on every GB10
infrastructure peer, including `trt-gb10-7`. This converges identities, shared
paths, candidate readback, and remote-link prerequisites. The 2026-07-29 owner
correction supersedes #822's static exclusion: all 15 GB10 peers, including
node 7, are infrastructure- and capacity-eligible. If a peer is busy, the
candidate-owned drain/quiescence gate defers disruptive convergence without
cancelling or preempting external work. The applied command installs:

```text
/usr/local/libexec/loom-developer-domain-runtime
/etc/loom/developer-runtime-domains.toml
```

No non-root fallback exists. A failed `sudo -n` from the publisher is evidence
that host authority has not been installed; do not work around it with an
operator-owned copy or a shared secret file.

## Plan and publish each domain

The source repository must contain the full exact commit. The worker env seed
must be a regular non-symlink file with no group/world permissions.

OLDLAB plan:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime publish \
  --config /etc/loom/developer-runtime-domains.toml \
  --domain oldlab \
  --sandbox <registry-runtime-id> \
  --candidate-sha <SHA> \
  --source-repo /root/loom-candidate-source \
  --worker-env-seed /run/loom-private/<registry-runtime-id>-oldlab.env
```

The JSON contains only candidate identity, target paths, actions, stable group,
and peer names. It reports env values as `redacted`.

Execute the unchanged request on the declared publisher
`trt-eai-oldlab-1`:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime publish \
  --config /etc/loom/developer-runtime-domains.toml \
  --domain oldlab \
  --sandbox <registry-runtime-id> \
  --candidate-sha <SHA> \
  --source-repo /root/loom-candidate-source \
  --worker-env-seed /run/loom-private/<registry-runtime-id>-oldlab.env \
  --execute
```

Repeat with `--domain gb10` and its domain-local source/env seed on
`gx10-01c7`. Do not copy the OLDLAB candidate directory or env through the Mac:
resolve and publish the exact SHA independently in each NFS domain.

The publisher performs mandatory readback on every configured peer through the
installed fixed node-authority transport and its read-only `check` action. It
verifies:

- peer hostname matches inventory;
- candidate HEAD/tree, owner, group, mode, and write protection;
- env owner, dedicated stable GID, and mode without reading its values;
- candidate inode and env inode agree across clients. Device IDs are reported
  but deliberately not compared because NFS clients may assign different local
  device numbers.

Any peer failure rolls the transaction back automatically and returns a
secret-safe error. There is no flag to skip peer readback.

## Remote-link fleet attestation

Before either domain can publish a signed runtime attestation, the oldlab2
remote-link controller must atomically persist:

```text
/var/lib/loom-developer-sandbox-links/attestations/<sandbox>/<SHA>/fleet.json
```

It is a `root:root` regular file at mode `0600`. Its exact top-level schema is
`schema_version`, `sandbox`, `candidate_sha`, `generated_at`, `expires_at`,
`eligible_nodes`, `bundle_generation`, `server`, `nodes`, and
`payload_sha256`. Times use UTC second precision (`YYYY-MM-DDTHH:MM:SSZ`);
expiry is exactly 900 seconds after generation. A consumer rejects a
generation more than 30 seconds in the future, more than 60 seconds old, or
already expired.

`eligible_nodes` is ordered and complete for remote-link infrastructure:
`oldlab-1` through `oldlab-5`, followed by `trt-gb10-1` through
`trt-gb10-15`, including `trt-gb10-7`. The GB10 subset is the same complete
capacity-eligible set; no static node exclusion is permitted. Each of the 20
node rows binds:

- the exact candidate SHA and client URI SAN
  `spiffe://loom/developer-sandbox/<sandbox>/candidate/<SHA>/worker`;
- route status to `192.168.50.14`, TLS 1.3, the shared CA fingerprint, and a
  client-certificate fingerprint;
- root ownership and mode `0600` for `worker-token`, `minio-access-key`,
  `minio-secret-key`, and `client-key.pem`;
- healthy control-plane, gateway, and MinIO relays on the sandbox-specific
  listener ports.

The server row binds oldlab2, address `192.168.50.14`, active unit
`loom-developer-sandbox-link@<sandbox>.service`, exact active SHA, CA/server
certificate fingerprints, client URI SAN, all three listener-to-loopback
targets, TLS 1.3, and health paths `/healthz`, `/healthz`, and
`/minio/health/live`.

Fleet fingerprints are `sha256:` followed by 64 lowercase hex characters.
Its digest is computed over canonical JSON without `payload_sha256`, using
sorted keys, separators `(",", ":")`, `ensure_ascii=true`, and UTF-8, then
stored as `sha256:<64-lowercase-hex>`. Every consumer recomputes it from the
readback bytes. Domain and combined attestations bind the fleet path, digest,
generation time, and expiry; a different or incomplete fleet cannot be
substituted.

## Signed domain and combined activation attestations

Each publisher emits a secret-free signed pair:

```text
/var/lib/loom-developer-domain-attestations/<sandbox>/<SHA>/<domain>.json
/var/lib/loom-developer-domain-attestations/<sandbox>/<SHA>/<domain>.sig
```

The JSON and base64 detached signature are `root:root` mode `0644` under
mode-`0755` roots. The Ed25519 keypair stays publisher-local at
`/etc/loom/developer-domain-runtime/attestation-keys/<domain>.{key,pub}`;
the private key and its directory are root-only. The closed manifest binds the
domain, sandbox, candidate SHA/tree/path/metadata, private env metadata/schema
(including the capacity-policy pool and concurrency binding),
literal host-local TLS references, publisher generation/key/freshness, and
complete peer readback. The closed manifest also binds the fleet attestation
reference, all three local URLs, all three oldlab2 upstream listeners, and all
six host-local references. It expires exactly 15 minutes after issue.

Copy each reviewed public key to oldlab2, plan the pin, then apply it:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime pin-key \
  --config /etc/loom/developer-runtime-domains.toml \
  --domain oldlab \
  --public-key /root/reviewed-keys/oldlab.pub

sudo /usr/local/libexec/loom-developer-domain-runtime pin-key \
  --config /etc/loom/developer-runtime-domains.toml \
  --domain oldlab \
  --public-key /root/reviewed-keys/oldlab.pub \
  --execute
```

Repeat for GB10. Keys are pinned as
`/etc/loom/developer-domain-runtime/trusted-attestation-keys/<domain>.pub`.
The collector never trusts a key fetched beside an attestation.

On oldlab2, collect both fresh domains in plan mode, then apply:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime collect \
  --config /etc/loom/developer-runtime-domains.toml \
  --sandbox <registry-runtime-id> \
  --candidate-sha <SHA>

sudo /usr/local/libexec/loom-developer-domain-runtime collect \
  --config /etc/loom/developer-runtime-domains.toml \
  --sandbox <registry-runtime-id> \
  --candidate-sha <SHA> \
  --execute
```

The collector runs as root only on oldlab2. Execute mode takes the global
advisory lock:

```text
/var/lib/loom-shared-capacity/runtime-attestations/.collector.lock
```

While holding that lock, it first unlinks any prior combined receipt for the
same sandbox/SHA and fsyncs the parent directory. It then reads and verifies
the fleet proof, fetches both domain inputs through the node authority's
bounded `export-domain-attestation` action, verifies pinned key IDs, Ed25519
signatures, canonical digests, freshness, complete peer sets, non-regressing
generations, and the same sandbox, SHA, tree, and fleet digest. Every check
remains under the lock. Only then does it atomically write and fsync the
root-only mode-`0600` receipt and its parent:

```text
/var/lib/loom-shared-capacity/runtime-attestations/<sandbox>/<SHA>/combined.json
```

Its closed top-level schema is `schema_version`, `kind`, `sandbox`,
`candidate_sha`, `candidate_tree`, `collector`, `fleet_attestation`, `domains`,
and `payload_sha256`. Each domain row contains only source paths,
payload/signature SHA-256, pinned key ID, generation, and publication/expiry
times. It expires no later than either input or 15 minutes after collection.
Any fleet, transport, root, signature, freshness, identity, peer, or
non-regression failure leaves no activatable combined receipt: the previous
receipt was durably revoked before validation began and no replacement is
written.

Digest construction is exact: serialize the object *without*
`payload_sha256` as UTF-8 JSON using sorted keys, `ensure_ascii=true`, and
separators `(",", ":")`; SHA-256 those bytes and insert the lowercase hex as
`payload_sha256` for domain and combined objects (fleet uses the `sha256:`
prefix specified above). Serialize the final object with the same settings and
append one LF for the file. Domain Ed25519 signatures cover those final bytes,
including the LF. The combined receipt is not separately signed because it is
root-only on the same host as the capacity adapter; its input domain rows carry
the verified detached-signature SHA-256 and pinned key IDs.

## Check and rollback

The peer helper's bounded local check is:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime inspect-local \
  --config /etc/loom/developer-runtime-domains.toml \
  --domain oldlab \
  --sandbox <registry-runtime-id> \
  --candidate-sha <SHA> \
  --candidate-tree <TREE>
```

A successful publication returns a root-only receipt path. To inspect the
rollback plan:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime rollback \
  --config /etc/loom/developer-runtime-domains.toml \
  --receipt /var/lib/loom-developer-domain-runtime/<domain>/<sandbox>/<SHA>/transaction-<id>/receipt.json
```

To restore the prior env and remove only a candidate created by that exact
receipt:

```bash
sudo /usr/local/libexec/loom-developer-domain-runtime rollback \
  --config /etc/loom/developer-runtime-domains.toml \
  --receipt /var/lib/loom-developer-domain-runtime/<domain>/<sandbox>/<SHA>/transaction-<id>/receipt.json \
  --execute
```

Rollback rejects receipts outside the fixed state root and re-verifies the
candidate SHA/tree before targeted removal. It is idempotent. Keep committed
receipts and their root-only previous-env snapshots until the corresponding
#1023 acceptance and recovery drill is complete; then remove them only through
a separately reviewed retention procedure.

## #1063 sandbox-installer integration contract

The `codex/1023-sandbox-installer` implementation predates this publisher and
must not retain its local `loom-rollout` dependency. Before that branch becomes
Ready, update `scripts/ops/developer_sandbox_host.py` to:

- remove `PUBLISH_USER = "loom-rollout"` and every
  `_identity(PUBLISH_USER, SHARED_GROUP)` lookup;
- remove the `publish_candidate()` mutation path from `install()`; candidate
  materialization belongs exclusively to this root publisher;
- make `verify_candidate_root()` and `verify_candidate()` require
  `root:sharedwork`, mode `2750`, exact HEAD/tree, clean status, and no
  group/world-writable entry;
- make install/activate/check/rollback consume only
  `/shared_work/loom/candidates/sandboxes/<sandbox>/<SHA>`;
- consume the OLDLAB private env only from
  `/shared_work/loom/runtime/sandboxes/<sandbox>/<SHA>/worker-oldlab.env`,
  require `loom-sandbox-<sandbox>:loom-sandbox-<sandbox>` mode `0600`, and never copy TLS PEM
  content from NFS;
- retain the local owner-private
  `/srv/loom/developer-sandboxes/<sandbox>` Compose state separately.

At authoring time, the stale references were `PUBLISH_USER` near line 42;
`verify_candidate_root()` near line 518; `publish_candidate()` near line 541;
`_identity(PUBLISH_USER, SHARED_GROUP)` near line 946 and the install path
around lines 1196-1203; plus the `loom-rollout:sharedwork` runbook statements
near lines 16-21 and 67-74. Re-run
`rg -n "PUBLISH_USER|loom-rollout|publish_candidate"` after rebasing because
line numbers can move. Any remaining publication-authority match is a merge
blocker.
