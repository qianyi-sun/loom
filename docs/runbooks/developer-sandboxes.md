# Self-service developer environments

This runbook covers the persistent developer-environment service on
`oldlab-2` and its candidate-bound OLDLAB and GB10 execution plane. Any
authorized member of `loom-developers` can create and operate one isolated
environment. The service does not contain a developer allowlist or require a
checked-in per-user profile. Nothing here authorizes staging or production.

The root-owned registry derives the principal from the Unix socket peer and
allocates the immutable environment ID, runtime ID, UID/GID, service identity,
Compose project, database, volumes, buckets, namespaces, roots, cgroup, and
Slurm account/QoS. Its port binding is generation-fenced as described below. A
caller cannot select or override any of those resources. `qianyi`, `hongjian`,
and `devansh` are only the initial `legacy-v1` migration seed; their checked-in
profiles preserve existing identities during migration and are never an
admission list or the runtime cohort.

## Developer workflow

Create a single-head Git bundle and one buildx `type=docker` archive for each
worker architecture. The archive tag is a transport placeholder, not identity;
the authority binds the config ID and every referenced OCI blob/diff ID.

```bash
candidate_sha="$(git rev-parse HEAD)"
candidate_tree="$(git rev-parse HEAD^{tree})"
short_sha="$(printf '%s' "${candidate_sha}" | cut -c1-12)"
git bundle create ./loom-candidate.bundle HEAD

docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --build-arg "LOOM_BUILD_SHA=${candidate_sha}" \
  --tag "loom-worker:${short_sha}-amd64" \
  --output "type=docker,dest=./loom-worker-linux-amd64.tar" \
  --file deploy/Dockerfile.worker .
docker buildx build \
  --platform linux/arm64 \
  --provenance=false \
  --build-arg "LOOM_BUILD_SHA=${candidate_sha}" \
  --tag "loom-worker:${short_sha}-arm64" \
  --output "type=docker,dest=./loom-worker-linux-arm64.tar" \
  --file deploy/Dockerfile.worker .

amd64_image_id="sha256:$(tar -xOf ./loom-worker-linux-amd64.tar manifest.json |
  jq -r '.[0].Config | split(\"/\")[-1]')"
arm64_image_id="sha256:$(tar -xOf ./loom-worker-linux-arm64.tar manifest.json |
  jq -r '.[0].Config | split(\"/\")[-1]')"

loom-developer-environment create \
  --idempotency-key <stable-create-request-id> \
  --display-name <human-readable-name> \
  --bundle ./loom-candidate.bundle \
  --candidate-sha "${candidate_sha}" \
  --candidate-tree "${candidate_tree}" \
  --amd64-image-digest "${amd64_image_id}" \
  --arm64-image-digest "${arm64_image_id}" \
  --amd64-image-archive ./loom-worker-linux-amd64.tar \
  --arm64-image-archive ./loom-worker-linux-arm64.tar
```

`image_digests` is retained as the external request key for compatibility, but
its two values have one narrow meaning: the archive's Docker config digest for
`linux/amd64` and `linux/arm64`. They are not universal runtime `.Id` values,
registry index/platform-manifest digests, archive checksums, aggregate Compose
digests, or node-bootstrap identities. The two config IDs must be different
because a Docker image config records its architecture.

Do not preload images manually. The root authority verifies both archives
offline, persists them under the immutable candidate namespace, and streams
the architecture-matched archive through the fixed `qianyi` transport to all
20 runtime nodes. Each node must prove one supported backend: classic
`overlay2`, where runtime `.Id` equals the config digest, or Docker's
containerd snapshotter (`overlayfs` with
`driver-type=io.containerd.snapshotter.v1`), where runtime `.Id` and
`.Descriptor` equal the archive's top-level load descriptor (an OCI manifest
or OCI index). The verifier independently binds that load descriptor to one
target-platform manifest, config, and layer DAG; a provenance index may contain
only explicitly annotated attestations that reference that platform manifest.
Each node preserves any peer placeholder-tag binding, adds an
authority-derived runtime-ID tag, and requires `Os` to be
`linux`, `Architecture` to match the selected key, and
`org.opencontainers.image.revision` equals `candidate_sha`. The image's default
command must be exactly `python -m loom_worker` and its entrypoint must be
empty, so an otherwise correct-looking config cannot redirect execution. The
deployer persists all 20 node receipts before its first host mutation, rejects
unknown backends and any runtime-ID/backend difference within a domain, then
starts each domain's worker by that domain's immutable runtime ID with
`--no-build`; it may still build
the other local services from the exact candidate checkout. Post-start
readback requires the worker container's actual Docker `.Image` to equal the
same persisted domain runtime ID. A correct-looking tag or operator-written label never
substitutes for those two image-engine checks.

The authority authenticates the socket peer, verifies and persists the bundle,
allocates only from the current trusted fleet inventory, and journals
registration, host services, dual-domain capacity, verification, and commit.
The same idempotency key is an exact replay only; changed input requires a new
key. Inventory absence, staleness, UID/GID collision, partial domain
convergence, or a busy node fails closed without publishing the environment as
active.

Every fleet-identity refresh still collects all 20 nodes, including
`trt-gb10-7`. Direct OLDLAB and `trt-gb10-1` checks remain independently
scheduled, while at most two checks may simultaneously traverse the shared
forced proxy on `trt-gb10-1` toward `trt-gb10-2` through `trt-gb10-15`. This
topology-bound admission is not a retry or direct-SSH fallback. One failed or
timed-out node aborts the complete refresh before inventory publication,
registration, candidate import, or deployment mutation.

On the canonical `oldlab-2` authority host, initial port allocation and every
allowed pre-deployment refresh merge the root-visible TCP/TCP6 listener
inventory with Docker's published-port reservations. A pristine dynamic
environment may be rebound atomically before its first immutable deployment
publication; the authority appends the old and new binding to its port journal
and advances both the resource and registry generations. A raw
`begin-deployment` caller must refresh and retry against that new generation;
the integrated deployer reloads it automatically. Once candidate
materialization, deployment history, activation, retirement, or revival has
made the binding externally observable, the ports are immutable. Missing,
malformed, incomplete, or stale host inventory fails closed without changing
the record.

Normal lifecycle operations never take an environment name or resource path:

```bash
loom-developer-environment check

loom-developer-environment update \
  --idempotency-key <stable-update-request-id> \
  --bundle ./loom-candidate.bundle \
  --candidate-sha <40-hex-commit> \
  --candidate-tree <40-hex-tree> \
  --amd64-image-digest sha256:<64-hex> \
  --arm64-image-digest sha256:<64-hex> \
  --amd64-image-archive ./loom-worker-linux-amd64.tar \
  --arm64-image-archive ./loom-worker-linux-arm64.tar

loom-developer-environment rollback \
  --idempotency-key <stable-rollback-request-id>

loom-developer-environment destroy \
  --idempotency-key <stable-destroy-request-id>
```

`destroy` retires only exact registry-owned resources after all owned jobs are
terminal. It never cancels, preempts, stops, or kills foreign or nonterminal
jobs. Reboot recovery uses the persisted registry snapshot, deployment journal,
and `loom-developer-environment@<registry-runtime-id>.service`; adding a fourth
or later developer requires no repository edit or authority rebuild.

Adding an environment is a pure additive transition for every peer: existing
candidates, published port bindings, identities, Compose projects, adapter and
handoff bytes, lease epochs, unit states, and active job IDs do not change.
Retiring an environment waits only for that environment's owned work and
resources to become terminal; peers continue running and never have to hand
off or drain. A later `create` by the same principal revives the same stable
environment identity with a new resource generation, candidate, credentials,
and request key. A retired identity is never reassigned to another principal,
and an old create key or secret cannot revive it.

## Host path contract

Provision the shared candidate namespace on the `/shared_work` NFS source, not
independently on each client. For `dynamic-v1`, the root publisher converges
the registry-assigned
`/shared_work/loom/candidates/environments/<env_id>` and
`/shared_work/loom/runtime/environments/<env_id>` roots as
`root:sharedwork` mode `2750`; the candidate SHA and private worker env are
created beneath those exact roots. The `sandboxes/<runtime_id>` roots are
reserved for the three `legacy-v1` migration records and are never used for a
new environment. Publish the exact SHA/tree independently in the OLDLAB and
GB10 NFS domains through
`developer_sandbox_domain_runtime.py`; identical logical paths do not imply one
backing filesystem. Developers have read/traverse access but cannot mutate a
published candidate. The host installer never fetches a branch or remote URL.
It requires its own checkout to be clean at the requested exact HEAD, creates
one root-private single-HEAD Git bundle, streams that bundle only to the two
declared domain publishers, and removes every local and remote staging copy
after materialization. A failed remote cleanup is a hard failure with a
root-owned recovery record, not a warning.

Private cross-domain worker envs are not owned by human developer accounts,
whose numeric UIDs are not stable across the fleet. The registry allocates one
non-login service identity per environment from the current complete 20-node
identity inventory. The node authority proves the name and numeric identity are
either unused everywhere or already exact before it idempotently creates them.
Each candidate env is owned by that service identity at mode `0600`; collisions
or login-capable metadata stop convergence before publication.

On `oldlab-2`, `/srv/loom` and `/srv/loom/developer-sandboxes` are
`root:sharedwork` mode `2750`. Each developer root and its `cache`, `evidence`,
and `runtime` children are owned by that developer and mode `0700`. Clear an
inherited setgid bit explicitly after creating those private children beneath
the setgid parent. Resolve owners by account name on the target host; do not
copy numeric UIDs from another node.

The capacity-broker state root is separate:
`/var/lib/loom-shared-capacity` stays `root:root` mode `0700` until a dedicated
broker service identity is installed. In that bootstrap state, only a
root-invoked broker may initialize the database. Never grant a sandbox account
write access to the broker authority.

## Persistent node-authority bootstrap

Before the first candidate can be installed, persist the fixed node authority
on every declared OLDLAB and GB10 peer. These hosts do not expose a direct-root
operator login; the supported bootstrap and upgrade channel is the
repository's one-shot privileged Docker transaction invoked by the
authenticated host user. Docker daemon access supplies the initial host-root
authority: the container chroots into a writable bind of the host root,
installs the fixed authority on the host, performs installed-state readback,
and exits. Docker is not a runtime dependency after that transaction.

The Python bootstrap/upgrade implementation is an internal image entrypoint,
not an operator command. It requires a persistent host-root view of PID
1/systemd, the local canonical node identity, exact clean Git SHA/tree, and
regular single-link request/bundle/trust inputs with fixed safe modes, exact
digests, and independent read-only bind mounts. The authenticated Docker
operator may stage those external inputs; the entrypoint detects metadata
drift while reading them and copies them into a root-owned, mode-`0700`
host-stage before use. It installs the fixed source, policy,
lock/journal/receipt roots, wrapper, systemd assets, and finally the validated
sudoers file. It rolls back only files and directories created by a failed
attempt.

The node authority is the single installer and lifecycle owner of
`/usr/local/libexec/scripts/ops/developer_sandbox_capacity_contract.py`.
The environment authority on `oldlab-2` consumes that file as an exact-candidate
prerequisite: bootstrap, upgrade, and readback require a root-owned, mode-`0644`,
single-link regular file with safe ancestry and bytes identical to the bound
candidate. Neither the environment-authority transaction nor the shared-capacity
runtime-host transaction backs up, replaces, chmods, removes, or rolls back this
node-owned contract. Both validate the node policy's exact SHA/tree and persist
the prerequisite digest separately from their own installed-asset digests.
Therefore install or upgrade node authority before either consumer, and treat
prerequisite drift as a node-authority repair rather than a consumer rollback
target.

The one-shot bootstrap invokes every host Python transaction with isolated
imports and bytecode writes disabled. During an environment-authority upgrade,
the installer may retire only closed `__pycache__` directories beneath its
fixed import roots: every directory and `.pyc` must remain root-owned,
non-writable by group/other, single-link, bounded, and match the closed
bytecode-name grammar. The retirement and file digests are appended to the
persistent installer journal before normal installed-inventory verification.
An unknown file, symlink, unsafe owner/mode, or changing inventory fails closed
and is never deleted. Readback does not clean drift; it continues to report
any cache as an invalid installed inventory.

Use
`deploy/developer-sandboxes/Containerfile.node-bootstrap` and the fixed
`developer_sandbox_node_docker_bootstrap.py` entrypoint. Build the image from
the exact candidate for the node architecture and record its immutable image
content ID. Run it once with `--rm`, `--privileged`, host PID/UTS/cgroup
namespaces, `--network=none`, a writable `/` bind at `/host`, and read-only
binds for the canonical request, exact Git bundle, and closed trust-input
directory. Render schema-version-2 requests with the checked-in
`developer_sandbox_node_docker_request.py`; it hashes trust inputs without
emitting their contents and binds the candidate SHA/tree, bundle digest,
expected node, action, transport expectation, operation ID, and every input
digest. Reuse an operation ID only for an exact retry; generate a new ID for
each later readback or lifecycle phase. Never delete or rewrite an earlier
receipt to accommodate changed installed state. The entrypoint accepts no argv or shell,
accepts the closed infrastructure inventory `oldlab-1` through `oldlab-5` and
`trt-gb10-1` through `trt-gb10-15`, creates a root-owned exact checkout under
host `/run`, and writes its non-secret receipt below
`/var/lib/loom-developer-sandbox-node-bootstrap/receipts/`. Each receipt embeds
the complete canonical child result plus its digest. Under the host lock, an
exact request replay validates and returns that persisted result before staging
or invoking any host action, so a lost container response cannot change
idempotency evidence or repeat the mutation.

Bootstrap `trt-gb10-7` as a normal member of the complete 15-node
infrastructure and capacity-eligible set. The 2026-07-29 owner correction
supersedes #822's static exclusion. If node 7 is busy, the candidate-owned
drain/quiescence gate defers disruptive convergence and must never cancel or
preempt an external job.

The runtime command shape is fixed; substitute only absolute paths to the
already-rendered inputs and the immutable local image content ID:

```bash
docker run --rm --privileged \
  --pid=host --uts=host --cgroupns=host \
  --network=none --read-only \
  --mount type=bind,src=/,dst=/host,bind-propagation=rslave \
  --mount type=bind,src="${REQUEST_JSON}",dst=/run/loom-node-bootstrap/request.json,readonly \
  --mount type=bind,src="${CANDIDATE_BUNDLE}",dst=/run/loom-node-bootstrap/candidate.bundle,readonly \
  --mount type=bind,src="${TRUST_INPUT_DIR}",dst=/run/loom-node-bootstrap/input,readonly \
  "${BOOTSTRAP_IMAGE_ID}"
```

Do not append an argv, shell, Docker socket, restart policy, network, or
additional mount. `authority-bootstrap` and `authority-upgrade` require an
empty trust directory. Server/client transport actions accept only the role
filenames derived from the checked-in inventory. A successful container
report is followed by host-state `validate-install`, `check-server`, and where
applicable `check-client`; success is not inferred from container exit alone.
The final `readback` also requires the installed transport program and routes
to equal the staged exact-candidate bytes and binds their SHA-256 digests beside
the candidate SHA/tree in the durable receipt. Authority and transport remain
separate persistent transactions and rollback domains; an authority receipt
alone is not complete node-stack evidence.

The oldlab-2 Unix socket admits authenticated members of `loom-developers`.
The root authority then invokes the fixed transport as its single operator,
`qianyi`. Remote nodes grant only that transport operator these three commands:

```text
qianyi ALL=(root) NOPASSWD:NOSETENV: /usr/local/libexec/loom-developer-sandbox-node-authority transact
qianyi ALL=(root) NOPASSWD:NOSETENV: /usr/local/libexec/loom-developer-sandbox-node-authority check
qianyi ALL=(root) NOPASSWD:NOSETENV: /usr/local/libexec/loom-developer-sandbox-node-authority load-image
```

Future developers receive no direct remote sudo or SSH authority. Their only
entry is the group-owned oldlab-2 socket; its root service uses
`runuser -u qianyi` and the fixed transport inventory. Remote nodes therefore
need neither the developer group nor per-developer membership changes. There
is no wildcard and no permission to run `install`, `tar`, `rm`, `chown`,
`chmod`, `python3`, a candidate path, or an operator-selected path. All three
verbs authenticate the exact fixed sudo caller/command and revalidate
the root-owned policy and every installed asset. `transact` accepts only a
bounded canonical stdin envelope whose closed schema binds the fixed node,
domain, sandbox, exact candidate SHA/tree, action, payload digest, and optional
prior receipt. The authority uses its own root-private stage and fixed
installed programs, persists an idempotent root-owned receipt plus fsynced
journal record, and retains the inner domain-runtime receipt needed by the
existing targeted rollback. `check` accepts only the read-only exact-candidate
inspection action. `load-image` accepts only the closed, bounded metadata line
followed by the exact-size verified worker archive stream; it does not accept a
path, argv, shell command, or generic Docker operation.

The policy pins both installed source SHA and tree. A squash-merged commit with
the same tree but a different SHA is transactionally rebound and must not be
treated as an unchanged installation. Upgrade through a new closed
`authority-upgrade` Docker request and immutable bootstrap image for the exact
candidate; never invoke the internal Python entrypoint directly, or delete or
overwrite the installed authority by hand.

Upgrade has no runtime sudoers entry. It first verifies the old policy,
wrapper, sudoers, fixed source,
host binding, lock, journal, receipt inventory, and the clean root-owned new
checkout. Before creating an upgrade snapshot or disabling admission, it also
requires every historical dynamic receipt to contain the exact
`worker_image_id` binding introduced with the config-ID contract; an older
dynamic receipt stops the upgrade for an explicit evidence migration, while
unrelated historical receipts remain valid. Under the existing exclusive
runtime lock it recovers any prior
interrupted upgrade, snapshots every replaceable authority asset, records the
old/new SHA and tree plus the primary journal and receipt digests, and writes a
root-owned active transaction and append-only upgrade journal. It then removes
sudoers admission, atomically replaces the fixed source, wrapper, and policy,
validates the new source sudoers, installs sudoers last, and performs a full
policy/asset/state readback. The runtime reads policy and request only after
taking the same lock, so a request queued before upgrade either completes on
the old tree or is revalidated against the new tree.

Any ordinary failure restores the exact snapshot with old sudoers last,
revalidates the old identity, proves the primary journal and all receipts are
byte-identical, and records `rolled-back`. A process crash leaves the active
transaction and snapshot for the next persistent-root `upgrade` invocation to
recover. If both upgrade and snapshot restoration fail, sudoers remains absent
and the active evidence is retained for external-root repair. Successful and
rolled-back snapshots remain under
`/var/lib/loom-developer-sandbox-node-authority/upgrades/`; receipts, the
primary runtime journal, and all domain-runtime rollback evidence are never
reinitialized.

Stop before host installation if any node lacks this exact-tree authority or
any of the three fixed commands fails its readback.

## Legacy-v1 seed migration reference

The profile- and `--sandbox`-based commands below exist only to migrate the
three initial `legacy-v1` records without changing their established resource
identities. They require the explicit `--legacy-v1-seed-migrate` gate and must
not be used for a new developer. New and updated environments use the
peer-authenticated workflow above.

Before installing the dynamic authority, its installer opens any existing
registry database strictly read-only and checks every committed deployment for
an exact `deployment_finalizations` record. If it reports
`legacy committed finalization migration required`, stop. Do not delete the
database, clear committed rows, manufacture receipt digests, or hand-edit the
schema. Confirm the real root-owned finalization journals first and implement a
separate evidence-driven migration for that observed database. The normal
first install is allowed only for a new/empty registry or one with no committed
pre-finalization rows; this preflight performs no DDL.

The worker image contract also has an explicit database schema generation.
Upgrading the prior binding generation is automatic only when every
environment is `ready` or `retired` with no current candidate and all
candidate/deployment/finalization history is empty. One immediate transaction
then installs the `docker-archive-identities/v2` candidate binding, all-node
runtime-binding column, and database schema v3 marker together. Any populated
v2 history stops with `worker image binding v2-to-v3 requires explicit
migration`; never reinterpret, delete, or hand-edit those rows to make the
upgrade pass. Legacy usernames are consulted only during the first seed
migration; later authority upgrades do not depend on those Unix accounts
continuing to exist.

### Legacy-v1 fixed host installer — do not use for new environments

`scripts/ops/developer_sandbox_host.py` is the root-side converger for the
three sandbox stacks. It is plan-only by default and has a fixed repository,
host, account, group, NFS namespace, state namespace, and systemd unit. It does
not accept a remote URL, ref, path, user, port, or secret-value override.

Render the complete three-sandbox plan from an exact checkout:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py plan \
  --candidate-sha <full-lowercase-40-character-commit-SHA>
```

The JSON plan contains the exact candidate path, Compose project, private
state and secret paths, all ten reserved ports, expected owner/mode, unit name,
and read-only NFS readback commands for `oldlab-1` through `oldlab-5`. It never
contains raw credential values.

After separate live-host authorization and exact-tree node-authority readback,
run the same command on `trt-eai-oldlab-2` as root with `install --execute`:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py install \
  --candidate-sha <SHA> \
  --execute
```

The installer performs these bounded steps:

1. require root and the canonical host name `trt-eai-oldlab-2`;
2. require `/shared_work` to be the OLDLAB NFS mount, resolve `sharedwork` and
   the three developer identities, acquire the global installer lock, and
   create a root-owned mode-`0600` single-HEAD bundle from the clean exact
   checkout;
3. send closed exact-candidate envelopes only to each node's fixed authority;
   the authority runs its root-owned installed domain helper to converge stable
   identities, publisher parents, and signing keys, then accepts the bounded
   bundle only on `oldlab-1` and `trt-gb10-1`, where `materialize` atomically
   creates the domain-local candidate and proves SHA/tree, raw tracked bytes,
   owner/mode, cleanliness, and one shared inode across every peer without
   reading fleet proof or writing env/attestation;
4. converge each local state/cache/evidence/runtime/secrets directory to its
   developer owner and mode `0700`;
5. create the per-developer `secrets/sandbox.env` and `secrets/admin.toml`
   once, atomically, as owner-only mode `0600` files; existing valid files are
   never rotated or overwritten;
6. require the shared-capacity adapters to be stopped and their policies
   absent or terminal, journal a root-owned bounded prepare transaction, and
   temporarily converge only the exact candidate's loopback stack without
   writing desired or lifecycle state;
7. install the exact relay locally and send all 20 host-local client bundles
   over encrypted transport to the fixed node authority, never to a raw remote
   root command; then require every route, TLS identity, and Control
   Plane/Gateway/MinIO health check before persisting a fresh fleet proof;
8. invoke `attest` on each publisher only after the fresh 20-node fleet proof
   exists; it republishes the secret-free reference env from a root-private
   seed, rechecks all domain peers, signs the domain receipt, and the collector
   closed-schema verifies both domain inputs into one fresh combined receipt;
9. only then atomically write desired state, restart the steady systemd unit,
   verify exact candidate/tree/runtime/receipt binding, and remove the
   transaction journal. Any failure stops the uncommitted prepare, invalidates
   its proof, restores the prior relay and sandbox candidate when present, and
   leaves capacity disabled.

The durable locations for one sandbox are therefore:

| Purpose | Path |
| --- | --- |
| immutable candidate | `/shared_work/loom/candidates/sandboxes/<developer>/<SHA>` |
| lifecycle binding | `/srv/loom/developer-sandboxes/<developer>/sandbox-state.json` |
| secrets | `/srv/loom/developer-sandboxes/<developer>/secrets/sandbox.env` |
| admin singleton | `/srv/loom/developer-sandboxes/<developer>/secrets/admin.toml` |
| cache | `/srv/loom/developer-sandboxes/<developer>/cache` |
| evidence | `/srv/loom/developer-sandboxes/<developer>/evidence` |
| runtime | `/srv/loom/developer-sandboxes/<developer>/runtime` |
| desired candidate | `/etc/loom/developer-sandboxes/desired/<developer>.json` |
| OLDLAB worker env | `/shared_work/loom/runtime/sandboxes/<developer>/<SHA>/worker-oldlab.env` |
| combined receipt | `/var/lib/loom-shared-capacity/runtime-attestations/<developer>/<SHA>/combined.json` |
| activation journal | `/var/lib/loom-developer-sandbox-installer/transactions/<developer>.json` |
| bounded source stage | `/var/lib/loom-developer-sandbox-installer/source/<SHA>/` (removed on exit) |
| installed fixed profile | `/etc/loom/developer-sandboxes/profiles/<developer>.toml` |
| per-node authority receipt | `/var/lib/loom-developer-sandbox-node-authority/receipts/<REQUEST-SHA256>.json` |
| per-node authority journal | `/var/lib/loom-developer-sandbox-node-authority/journal.jsonl` |
| authority upgrade snapshots | `/var/lib/loom-developer-sandbox-node-authority/upgrades/<UPGRADE-ID>/` |
| authority upgrade journal | `/var/lib/loom-developer-sandbox-node-authority/upgrade-journal.jsonl` |

Do not copy or reveal the private files during readback. Compare only
owner/mode, required key names, and secret fingerprints when an authorized
isolation procedure requires it.

### Legacy-v1 persistent create, update, and check

The enabled `loom-developer-sandbox@.service` is a replayable oneshot. It
selects `create`, `update`, or `check` from the persisted lifecycle binding,
then finishes with the full Compose health check and exact loopback-port
readback. It is safe to invoke repeatedly:

```bash
sudo systemctl start loom-developer-sandbox@qianyi.service
sudo systemctl start loom-developer-sandbox@hongjian.service
sudo systemctl start loom-developer-sandbox@devansh.service
```

An installer or unit rerun with the same SHA performs an idempotent forced
Compose convergence followed by a check; this repairs a partially created or
mixed stack instead of trusting state alone. A rerun with a different exact
SHA records the old SHA as the sole rollback target and converges an update.
Named Compose volumes remain attached to the fixed per-developer Compose
project.

On a fresh database, the initial worker token is deliberately only bootstrap
material. After the sandbox Control Plane is healthy, the host entry checks the
token without registering a worker. If it is rejected, the entry uses that
sandbox's loopback Admin API and private admin file to mint worker and
batch-runner tokens, atomically replaces only those env-file values, and
force-converges the stack so worker and Service receive them. Raw tokens never
enter argv, JSON output, systemd `Environment=`, or logs.

Run a read-only installed-state check with:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py check \
  --candidate-sha <SHA> \
  --sandbox all \
  --execute
```

This validates the NFS mount, candidate owner and immutability, private
owner/mode, secret-file shape, exact lifecycle SHA, Compose health, and all
reserved loopback listeners. Run the plan's five fixed node-transport
`check` invocations separately and require identical inode, UID, GID, and mode
for each candidate root and exact candidate. Device numbers may differ between
NFS clients.

### Safe rollback

Rollback is limited to the exact `previous_sha` stored by the last successful
desired-state change:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_host.py rollback \
  --sandbox qianyi \
  --candidate-sha <RECORDED-PREVIOUS-SHA> \
  --execute
```

The target must already be a clean immutable published candidate. Both forward
update and rollback require the current and target candidates to have the same
Git tree for `migrations/`; the rollback then atomically swaps desired/current
history and runs the normal unit. If an update unit fails, the installer
restores desired state and attempts the previous same-migration candidate once.
For a first-create failure, it retains the new desired record so a repeat can
repair the partial stack. This rollback preserves named volumes.

If the migration trees differ, code-only rollback is intentionally refused.
Use a separately reviewed database/object-store backup and restore procedure;
do not run Alembic downgrade ad hoc. Likewise, never add
`--delete-volumes` to recovery unless the developer explicitly authorizes
irreversible sandbox-data deletion.

### Legacy-v1 manual workflow

The remaining profile-, fixed-port-, and `--sandbox`-based procedures in this
section apply only to the three initial `legacy-v1` seed records. They are
migration and recovery references for preserving those existing identities;
they are not the create, update, destroy, admission, or capacity workflow for a
new developer. The current self-service workflow is the registry-generated
workflow at the top of this runbook, and its cohort may contain any number of
authorized developers within the configured resource ranges.

#### Preconditions

Materialize a clean candidate checkout at the exact profile path:

```text
<candidate_root>/<full-lowercase-40-character-commit-SHA>
```

The checkout's `HEAD` must equal the requested SHA, its tree must resolve, and
`git status --porcelain=v1 --untracked-files=all` must be empty.

Create a mode `0600` Compose env file containing these keys:

```text
LOOM_DEV_POSTGRES_USER
LOOM_DEV_POSTGRES_PASSWORD
LOOM_DEV_MINIO_ROOT_USER
LOOM_DEV_MINIO_ROOT_PASSWORD
LOOM_CP_STEP_JWT_SIGNING_KEY
LOOM_SECRET_STORE_MASTER_KEY
LOOM_WORKER_TOKEN
```

Create a separate mode `0600`, non-symlink admin secret TOML with the normal
`[admin].token` format. The planner validates both files but never reads secret
values into the child-process environment or prints them in its JSON output.

#### Plan first

From the candidate checkout, render a mutation-free plan:

```bash
uv run --no-sync python scripts/ops/developer_sandbox.py plan \
  --operation create \
  --profile deploy/developer-sandboxes/qianyi.toml \
  --source-repo /shared_work/loom/candidates/sandboxes/qianyi/<SHA> \
  --candidate-sha <SHA> \
  --secrets-env /secure/path/qianyi.env \
  --admin-secret-file /secure/path/qianyi-admin.toml
```

`create`, `update`, `check`, and `destroy` also default to plan-only mode. Add
`--execute` only while logged into the `oldlab-2` SSH target. The connection
alias is separate from host identity: execution lowercases the canonical local
hostname, removes only a trailing DNS dot, and then requires the exact profile
identity `trt-eai-oldlab-2`. Any other hostname fails closed.

Create and update validate Compose, start Postgres and MinIO, apply migrations,
then build and start the exact candidate. Successful mutations write a
mode-`0600` candidate/state binding and an evidence record. `check` requires the
requested SHA to match state and requires every expected service to be running
and not unhealthy.

Destroy is project-scoped and preserves named volumes by default:

```bash
uv run --no-sync python scripts/ops/developer_sandbox.py destroy ... --execute
```

Use `--delete-volumes` only when the developer explicitly intends to delete
that sandbox project's persistent data. No command in this workflow is rollout
or staging evidence.

#### Candidate-bound remote data-plane link

The three sandboxes keep their Control Plane, Gateway, and MinIO host ports
bound to `127.0.0.1`. Do not change those Compose bindings to `0.0.0.0` or a
LAN address. One root-owned multi-service relay per sandbox listens on the
exact oldlab2 private address `192.168.50.14`, requires TLS 1.3 plus a client
certificate, checks the client URI SAN against both the sandbox identity and
the one active 40-character candidate SHA, and only then forwards to the exact
loopback service.

| Sandbox | Control Plane | Gateway | MinIO |
| --- | --- | --- | --- |
| `qianyi` | `26080 → 20080` | `26100 → 20100` | `26900 → 20900` |
| `hongjian` | `27080 → 21080` | `27100 → 21100` | `27900 → 21900` |
| `devansh` | `28080 → 22080` | `28100 → 22100` | `28900 → 22900` |

Every mapping is `https://192.168.50.14:<listener>` to
`127.0.0.1:<target>`.

The server certificate has only the `192.168.50.14` IP SAN. Each candidate
uses a new CA and client certificate. The client URI SAN is exactly:

```text
spiffe://loom/developer-sandbox/<sandbox>/candidate/<SHA>/worker
```

There is no previous-SHA grace set. Activating or rolling back atomically
switches the `current` symlink to one installed SHA and restarts the relay.
Every other candidate is rejected before the loopback connection is opened.

##### Host-local secret contract

OLDLAB `/shared_work` and GB10 `/shared_work` are independent NFS domains.
Candidate source and the secret-free worker env can use the same logical paths
in each domain, but TLS keys and worker bearer tokens must be independently
installed by root on every eligible worker node:

```text
/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/
  ca.pem
  client.pem
  client-key.pem
  worker-token
  minio-access-key
  minio-secret-key
  metadata.json
```

The private key and all three secret files are root-owned mode `0600`. They
must never be placed below `/shared_work`, embedded in an image, passed as
command arguments, or rendered by `docker compose config`. Installing the GB10
copy requires the externally bootstrapped exact-tree node authority and the
separate Slurm-admin policy path; Docker group membership is not installation
authority.

Each NFS domain materializes a mode `0600` worker env containing the normal
non-secret worker settings plus these exact candidate-bound references:

```text
LOOM_WORKER_CONTROL_PLANE_URL=http://sandbox-link:8080
LOOM_WORKER_GATEWAY_URL=http://sandbox-link:9100
LOOM_WORKER_MINIO_ENDPOINT=http://sandbox-link:9000
LOOM_WORKER_SANDBOX_IDENTITY=<sandbox>
LOOM_WORKER_CANDIDATE_SHA=<SHA>
LOOM_WORKER_POOL_NAME=<oldlab|gb10>
LOOM_WORKER_MAX_CONCURRENT=<4 for oldlab|8 for gb10>
LOOM_SANDBOX_LINK_CP_UPSTREAM=https://192.168.50.14:<cp-port>
LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM=https://192.168.50.14:<gateway-port>
LOOM_SANDBOX_LINK_MINIO_UPSTREAM=https://192.168.50.14:<minio-port>
LOOM_WORKER_TOKEN_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/worker-token
LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/minio-access-key
LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/minio-secret-key
LOOM_WORKER_CP_TLS_CA_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/ca.pem
LOOM_WORKER_CP_TLS_CERT_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/client.pem
LOOM_WORKER_CP_TLS_KEY_FILE_HOST=/etc/loom/developer-sandbox-links/clients/<sandbox>/<SHA>/client-key.pem
```

The pool/concurrency pair comes only from the exact candidate's checked-in
shared-capacity policy (`requested_concurrency=4` for OLDLAB and `8` for GB10).
The domain publisher validates that contract and rejects a seed for the other
NFS/Slurm domain; these values are not operator-tunable publication inputs.

It must not contain raw worker, MinIO, provider, API, password, or key
credentials. Validate both the OLDLAB and GB10 copies:

```bash
python scripts/ops/developer_sandbox_remote_link_host.py validate-env \
  --sandbox qianyi \
  --candidate-sha <SHA> \
  --env-file /shared_work/loom/runtime/sandboxes/qianyi/<SHA>/worker-oldlab.env
```

The non-exclusive sandbox Slurm job must append
`-f deploy/docker-compose.remote-worker.sandbox-link.yml` after the base remote
worker Compose file. A Compose-private `sandbox-link` sidecar owns the three
mTLS credentials and exposes only ports 8080, 9100, and 9000 to the worker
network; it publishes no host ports. The worker sees only its bearer token and
MinIO key files. The sidecar is read-only, cannot restart outside the Slurm
allocation, has positive CPU/memory/PID limits, inherits the validated
driver-compatible allocation parent (the Slurm job cgroup for `cgroupfs` or
its receipt-bound allocation systemd slice for `systemd`),
and carries the same sandbox/candidate/job/Compose cleanup labels. Missing,
writable, malformed, or cross-candidate material fails before registration.

##### Plan, install, activate

All mutation commands are plan-only without `--execute`.

1. While the six sandbox capacity policies remain disabled, prepare one
   root-private issuance tree on oldlab2:

   ```bash
   sudo python scripts/ops/developer_sandbox_remote_link_host.py \
     prepare-rotation --sandbox qianyi --candidate-sha <SHA> --execute
   ```

2. Install, but do not activate, the server candidate:

   ```bash
   sudo python scripts/ops/developer_sandbox_remote_link_host.py \
     install-server --sandbox qianyi --candidate-sha <SHA> \
     --credential-source \
       /var/lib/loom/developer-sandbox-links/issuance/qianyi/<SHA>/server \
     --execute
   ```

3. Transfer each node's directory over the administrative encrypted transport
   to a root-private local inbox on that node. Never stage it on either NFS
   domain. With a host-local file containing only the already-minted sandbox
   worker token, the node administrator installs the exact node certificate:

   ```bash
   sudo python /secure/source/developer_sandbox_remote_link_host.py \
     install-client --sandbox qianyi --candidate-sha <SHA> \
     --node trt-gb10-2 \
     --credential-source /secure/inbox/qianyi/<SHA>/trt-gb10-2 \
     --worker-token-file /secure/inbox/qianyi/<SHA>/worker-token \
     --minio-access-key-file /secure/inbox/qianyi/<SHA>/minio-access-key \
     --minio-secret-key-file /secure/inbox/qianyi/<SHA>/minio-secret-key \
     --execute
   ```

   Repeat for all five OLDLAB nodes and all 15 GB10 infrastructure nodes.
   GB10-7 is a normal infrastructure and capacity-eligible member. If it is
   busy, candidate-owned drain/quiescence defers disruptive convergence
   without cancelling or preempting the external job.

4. Confirm the sandbox itself is healthy, both env copies validate, and all
   clients are installed. Activate exactly one server SHA:

   ```bash
   sudo /usr/local/libexec/loom-developer-sandbox-remote-link-host \
     activate-server --sandbox qianyi --candidate-sha <SHA> --execute
   ```

5. The root-installed sandbox host installer now performs the fleet gate
   internally; there is no standalone fleet or SSH command. It sends the
   closed `inspect-link-client` check envelope through the installed
   node-authority transport to all 20 infrastructure link nodes, then sends
   `inspect-link-server` only to `oldlab-2`. The host validates the complete
   response schemas, exact node/domain/candidate identity, route, TLS version,
   certificate fingerprints, secret-file metadata, and all three service
   health results before constructing the fleet document. It then sends one
   canonical bounded `fleet-attestation-json` payload to `oldlab-2` through
   the authority's fixed `persist-fleet-attestation` transaction.

   One inaccessible node, authority failure, route failure, certificate
   mismatch, TLS downgrade, secret-file drift, server mismatch, CA-generation
   mismatch, or unhealthy Control Plane/Gateway/MinIO response fails the whole
   gate. A green transaction atomically persists a root-owned mode-`0600`
   receipt at
   `/var/lib/loom-developer-sandbox-links/attestations/<sandbox>/<SHA>/fleet.json`.
   Its canonical digest binds the exact five OLDLAB and 15 GB10 infrastructure
   nodes, oldlab2 relay state, all three listeners, bundle generation, and
   15-minute expiry. It proves the complete all-15 GB10 capacity-eligible set
   with `excluded_nodes=[]`. Do not enable capacity from a missing, stale,
   incomplete, or
   digest-mismatched receipt.

##### Rollback and readback

Rollback is an explicit atomic switch to a previously installed exact SHA; it
does not accept old and new certificates simultaneously:

```bash
sudo /usr/local/libexec/loom-developer-sandbox-remote-link-host \
  rollback-server --sandbox qianyi --candidate-sha <PRIOR_SHA> --execute
```

After rollback, restore the prior candidate-bound env references and run the
supported sandbox-host rollback/renewal transaction. Installation and renewal
share the same fixed authority fleet collector, so the prior SHA receives a
fresh fleet receipt before capacity is re-enabled. Readback output is
secret-free: it includes sandbox, node, candidate SHA, route/health status, and
certificate fingerprints, never certificate bodies, private keys, tokens, or
environment dumps.

## Cross-sandbox negative probes (A3)

Automated negatives live in-repo and do not require `oldlab-2`:

1. Static profile isolation:

```bash
uv run --no-sync python scripts/validate_developer_sandbox_isolation.py \
  --profiles-dir deploy/developer-sandboxes --json
```

2. CI dual-stack crossover tests:
   `tests/integration/test_developer_sandbox_crossover.py` (foreign worker/admin
   tokens → 401; foreign MinIO creds / bucket names rejected).

3. Secret-safe probe helper (default dry-run):

```bash
uv run --no-sync python scripts/ops/developer_sandbox_crossover_probe.py \
  --write-evidence /tmp/a3-crossover-dry-run.json
```

Dry-run / CI dual-stack negatives are **not** live A3 host evidence and are
**not** `#896` soak evidence. Evidence JSON records fingerprints and status
codes only — never raw `loom_w_*` / admin / MinIO secrets.

Live `--execute` is fail-closed. It verifies the fixed root-owned
`/var/lib/loom-developer-environment-registry/current-snapshot.json`, selects
every active environment whose current candidate has a latest committed
deployment, and requires at least two environments. Loopback endpoints,
private state/secret paths, buckets, service identities, and each environment's
candidate SHA/tree come only from that snapshot. Candidate SHAs may differ.
The probe runs every ordered foreign pair plus same-environment positive
controls, verifies exact clean candidate checkouts and fresh OLDLAB+GB10
combined activation receipts, then re-reads both authorities to reject a
concurrent generation change. Run it as root on `oldlab-2`:

```bash
uv run --no-sync python scripts/ops/developer_sandbox_crossover_probe.py \
  --execute \
  --write-evidence /tmp/a3-crossover-execute.json
```

There are no per-developer endpoint, path, bucket, secret, or candidate CLI
flags. Credentials are sent only to each registry-selected environment's local
loopback CP/MinIO services. Evidence embeds the verified registry projection,
source generation/digest, exact environment-candidate bindings, sanitized
runtime receipt identities, ordered pair results, and no secret values.

### Profile identity notes

- These notes apply only to the `legacy-v1` migration profiles.
- Dynamic environments derive provider and bucket identities from `env_id`;
  runtime, Compose, broker, and acceptance consume the registry projection
  rather than profile files.
- Storage credentials, namespace separation, network separation, and the live
  ordered-pair crossover are all required boundaries; a bucket name by itself
  is not an authorization boundary.

## Shared capacity brokerage

Capacity brokerage and the broker→WPAP handoff adapter are documented in
[`shared-sandbox-capacity-broker.md`](shared-sandbox-capacity-broker.md). This
sandbox runbook does not configure Slurm packing or enable shared-worker pools.

Staging GB10 steady-state autoscaling is explicitly bound to
`external_broker = "staging-gb10-v1"` and `cluster = "trt-gb10"`. The
autoscaler may query, submit, or cancel only through
`sudo -n /usr/local/libexec/loom-staging-external-slurm-authority broker-*`;
that root-owned client sends the fixed node transport to controller and submit
host `trt-gb10-1`. It has no direct `squeue`, `sacct`, `sinfo`, `sbatch`, or
`scancel` path and no local or OLDLAB fallback. Queries use the read-only
`check` verb. Job observations and cancellations must resolve through the
root-owned submission ledger for the exact candidate, so a foreign job is
never queried, cancelled, or preempted. The fixed eligible set remains all 15
GB10 nodes, including `trt-gb10-7`; a busy node is skipped without widening
authority.

Each submit request is serialized by its 64-hex request ID and writes a
root-owned mode-`0600` WAL below
`/var/lib/loom-staging-external-slurm-authority/submissions` before `sbatch`.
The Slurm job name and comment both bind that request ID. On replay, the host
returns a completed WAL result or recovers the one exact matching job from
Slurm before it may submit; duplicate matches and identity drift fail closed.
The controller ledger persists the full validated result, so a crash before
the outer receipt is published replays without contacting the host or Slurm.

Slurm node convergence is a durable maintenance-window operation driven by the
root-published registry cohort. The capacity authority first proves identity
availability on all 20 nodes, idempotently converges exact non-login service
identities, then converges each controller before its complete compute set. It
persists every receipt, stops safely on busy or foreign work without cancelling
it, and resumes only by replaying the exact candidate and registry binding.
The node authority fixes topology, candidate/runtime paths,
restart/accounting semantics, and the registry-derived identity; developers
and operators cannot inject those values through the envelope or CLI. See
[`developer-sandbox-slurm-policy.md`](developer-sandbox-slurm-policy.md) for
the full drain, readback, and exact-owned rollback contract.

The host-side Slurm maintenance journal root and its parent are exact
root-owned mode-`0700` directories. Each journal is a root-owned mode-`0600`
single-link regular file. Loads use `O_NOFOLLOW`, compare descriptor and path
identity, and read the same inode twice before accepting canonical JSON.
Foreign-owned, hardlinked, symlinked, replaced, raced, or open-shape state is
never adopted for resume or rollback.
