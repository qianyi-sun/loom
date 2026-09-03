# Task-image builder Phase 2B2 node guard implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Every
> production behavior follows a red/green/refactor cycle and every merge claim
> requires fresh verification.

**Goal:** Deliver the production-inert, root-owned task-image builder node guard,
its precompiled default-deny BPF policy, deterministic release, staging-only
installer, and conformance surface.

**Architecture:** A standard-library-only Python zipapp accepts a bounded local
`SOCK_SEQPACKET` protocol, derives peer/Slurm/cgroup facts locally, and is the
only mTLS client of the Phase 2B1 authority service. It creates a three-cgroup
containment tree, loads a precompiled little-endian eBPF object through a
release-pinned `bpftool`, creates and pins cgroup `bpf_link` objects through the
`bpf(2)` syscall, persists an atomic root-owned ledger, and transfers bootstrap
and session material only in fully sealed memfds over `SCM_RIGHTS`. The release
remains unusable in production: no node config, bearer, TLS key, activation
marker, current-release link, feature advertisement, or running service is
created by this increment.

**Tech stack:** Python 3.11 standard library, Linux cgroup v2 and `bpf(2)`, a
precompiled `bpfel` ELF object built by digest-pinned Clang 18, `bpftool` staged
as a digest-bound release member, systemd, pytest, Ruff, and strict mypy.

**Spec:** `docs/architecture/2026-09-02-task-image-builder-phase2-production.md`

## Global constraints

- Base every change on merge `be9c9c93406eba71eeb1327508f93ae9870e0871`
  in the dedicated Phase 2B2 worktree.
- The installed guard executes only as
  `/usr/bin/python3 -I -B <content-addressed>/loom-task-image-builder-guard.pyz`.
- Guard zipapp imports are restricted to the Python standard library and its own
  captured package; it never imports from a writable path or loads plugins.
- The only supervisor-supplied initial authority is one nonzero canonical grant
  UUID. Job, node, identity, executable, cgroup, and Slurm facts are derived
  locally.
- Both IPv4 and IPv6 are default-deny. Connect, UDP send, socket create/release,
  ingress, and egress programs are attached and pinned before the supervisor is
  moved or any credential descriptor is released.
- Every secret-bearing local payload is a bounded, fully sealed memfd passed by
  `SCM_RIGHTS`; secrets never enter paths, arguments, environment, ledger, or
  logs.
- Ambiguous reconciliation leaves deny policy pinned and removes only the
  `loom_rootless_buildkit` active feature. It never drains a node, changes node
  state, cancels a foreign job, or writes outside the exact Loom subtree.
- The staging installer never enables, starts, restarts, or daemon-reloads a
  service and never creates a live config, activation marker, or current-release
  link.
- Preserve `production_certification_allowed=false`, an empty certified-node
  set, `phase2_guard_provider_release_missing`, both disabled rootless provider
  policies, and the active Phase 1 path.
- Do not modify `docs/superpowers/**` or `.superpowers/**`.

---

### Task 1: Strict guard models, safe files, config, and local descriptor protocol

**Files:**

- Create: `src/loom_task_image_builder_guard/__init__.py`
- Create: `src/loom_task_image_builder_guard/errors.py`
- Create: `src/loom_task_image_builder_guard/models.py`
- Create: `src/loom_task_image_builder_guard/safeio.py`
- Create: `src/loom_task_image_builder_guard/config.py`
- Create: `src/loom_task_image_builder_guard/protocol.py`
- Test: `tests/unit/test_task_image_builder_guard_config.py`
- Test: `tests/unit/test_task_image_builder_guard_protocol.py`

**Interfaces:**

- `GuardError(code: str)` exposes only a bounded snake-case `code` and never
  incorporates request text or secrets in `str(error)`.
- `canonical_json(value: object) -> bytes` emits bounded ASCII JSON with sorted
  keys, compact separators, no NaN, and a trailing newline only when requested
  by a file writer.
- `read_stable_file(path: Path, *, uid: int, gid: int, mode: int,
  maximum: int) -> bytes` uses `lstat`, `O_NOFOLLOW|O_CLOEXEC`, `fstat`, a
  bounded read, and final identity comparison.
- `GuardConfig.from_file(path: Path) -> GuardConfig` accepts exactly schema
  `loom.task-image-builder-node-guard-config/v1`, the native
  `oldlab/x86_64` or `gb10/arm64` pair, exact node/identity/command hashes,
  absolute safe paths, positive protocol limits, fixed authority HTTPS origin,
  and exact containment/resource policy digests.
- `LocalRequest.parse(payload: bytes) -> LocalRequest` accepts exactly one of
  `project`, `exchange`, or `ack`. `project` contains only `grant_id`;
  `exchange` contains `grant_id`, `exchange_id`, and `proof_sha256` and requires
  one sealed input memfd; `ack` contains the response UUID and no descriptor.
- `create_sealed_memfd(name: str, payload: bytes, *, maximum: int) -> int`,
  `read_sealed_memfd(fd: int, *, maximum: int) -> bytes`, and
  `send_packet`/`receive_packet` enforce `MFD_CLOEXEC|MFD_ALLOW_SEALING`, all
  four required seals, regular anonymous-file metadata, one descriptor at most,
  packet truncation rejection, and close-on-error ownership.

- [ ] **Step 1: Write failing configuration and safe-file tests**

  Name the breaks: a symlink, wrong owner/mode, changing inode, oversized or
  duplicate JSON key, unknown field, non-native cluster/architecture, unsafe
  path, zero digest, HTTP origin, or nonpositive limit must become accepted for
  the test to fail. Use literal expected error codes and controlled filesystem
  fixtures.

- [ ] **Step 2: Run the config tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_config.py`

  Expected: collection fails because `loom_task_image_builder_guard.config`
  does not exist.

- [ ] **Step 3: Implement the minimal models, safe I/O, and config loader**

  Use frozen `dataclass(slots=True)` values and explicit type/key/range checks;
  do not depend on Pydantic, PyYAML, or repository imports.

- [ ] **Step 4: Run the config tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_config.py`

- [ ] **Step 5: Write failing packet and sealed-memfd tests**

  Use a real `socket.socketpair(AF_UNIX, SOCK_SEQPACKET)` and real memfds. Name
  the breaks: accepting stream framing, multiple FDs, a writable/growable
  descriptor, a truncated packet, secret text in JSON, a second semantic field,
  or an ack for a different response UUID.

- [ ] **Step 6: Run the protocol tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_protocol.py`

  Expected: import or missing-symbol failure for the unimplemented protocol.

- [ ] **Step 7: Implement the bounded packet and descriptor protocol**

  Ensure ancillary buffers use `CMSG_SPACE(sizeof(int))`, reject
  `MSG_TRUNC|MSG_CTRUNC`, set received FDs close-on-exec, and close every
  unreturned descriptor in `finally` paths.

- [ ] **Step 8: Run both test files and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_config.py tests/unit/test_task_image_builder_guard_protocol.py`

- [ ] **Step 9: Commit Task 1**

  Commit message: `feat(builder): add strict node guard protocol`

---

### Task 2: Peer, executable, Slurm, and exact batch-cgroup identity

**Files:**

- Create: `src/loom_task_image_builder_guard/identity.py`
- Create: `src/loom_task_image_builder_guard/slurm.py`
- Test: `tests/unit/test_task_image_builder_guard_identity.py`
- Test: `tests/unit/test_task_image_builder_guard_slurm.py`

**Interfaces:**

- `PeerInspector.capture(connection: socket.socket) -> PeerHandle` reads
  `SO_PEERCRED`, opens a pidfd before any slow operation, opens
  `/proc/<pid>/exe` through `O_NOFOLLOW` semantics, records executable device,
  inode and SHA-256, parses one unified cgroup-v2 entry, and validates real,
  effective, saved, and filesystem UID/GID plus canonical supplementary groups.
- `PeerHandle.assert_unchanged() -> None` polls the pidfd and rechecks executable
  identity/digest and exact cgroup membership at every authority, attachment,
  move, and descriptor-transfer boundary.
- `PinnedCommandRunner.run(command: CommandIdentity, argv: tuple[str, ...]) ->
  CommandResult` opens and hashes the root-owned command, executes its stable
  `/proc/self/fd/<fd>` handle with a fixed locale/PATH, bounds output/time, and
  compares final metadata.
- `SlurmInspector.observe(job_id: str, peer: PeerHandle) -> SlurmFacts` parses
  one `scontrol show job --oneliner` record and one allocation-level
  `sacct --parsable2 --noheader` record. It requires RUNNING, the exact local
  batch host/node, `loom-builder(993)`, `loom-task-builder(980)`, dedicated
  account/partition/native QoS, exact grant comment, feature, CPU, memory,
  zero-swap policy, time limit, and matching controller/accounting facts.
- `derive_batch_cgroup(peer: PeerHandle, facts: SlurmFacts, root: Path) ->
  BatchCgroup` rejects ambiguous Slurm markers, another job, symlink traversal,
  non-domain cgroups, a path other than `step_batch/user/task_0`, multiple
  resident processes, or an inode/path change. The peer PID must be the sole
  process in the batch-task cgroup.
- `projection_request(...) -> dict[str, object]` emits the exact Phase 2B1
  `TaskImageProjectionRequestV1` wire shape with a persisted request UUID and
  locally observed facts.

- [ ] **Step 1: Write failing peer/pidfd/executable tests**

  Use a synthetic procfs plus injectable `pidfd_open`/poll functions. Name the
  breaks: PID reuse, death, re-exec, executable replacement, elevated real or
  saved identity, a forbidden supplementary group, malformed proc status, and
  cgroup drift.

- [ ] **Step 2: Run identity tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_identity.py`

- [ ] **Step 3: Implement peer capture and boundary revalidation**

  Keep all opened descriptors owned by `PeerHandle.close()` and make close
  idempotent. Hash from the already-open executable descriptor.

- [ ] **Step 4: Run identity tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_identity.py`

- [ ] **Step 5: Write failing pinned-command, Slurm, and cgroup tests**

  Fixture output must include complete controller and accounting rows. Name the
  breaks: accepting partial command output, shell execution, stale or terminal
  state, a second record, cluster/node/account/QoS/comment/resource mismatch,
  an array/heterogeneous job, a non-batch step, internal processes, or a symlink
  escape.

- [ ] **Step 6: Run Slurm tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_slurm.py`

- [ ] **Step 7: Implement exact command and Slurm/cgroup derivation**

  Pass an argv tuple directly to `subprocess.run`; set stdin to `DEVNULL`; never
  invoke a shell; never trust `SLURM_*` environment variables.

- [ ] **Step 8: Run Task 2 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_identity.py tests/unit/test_task_image_builder_guard_slurm.py`

- [ ] **Step 9: Commit Task 2**

  Commit message: `feat(builder): derive node guard allocation identity`

---

### Task 3: Precompiled default-deny BPF policy and containment attachment

**Files:**

- Create: `deploy/task-image-builder/guard-network-v1.bpf.c`
- Create: `deploy/task-image-builder/guard-network-v1.bpf.o`
- Create: `deploy/task-image-builder/guard-network-v1.bpf.build.json`
- Create: `deploy/task-image-builder/guard-network-map-schema-v1.json`
- Create: `scripts/ops/task_image_builder_guard_bpf_build.py`
- Create: `src/loom_task_image_builder_guard/bpf.py`
- Create: `src/loom_task_image_builder_guard/containment.py`
- Test: `tests/ops/test_task_image_builder_guard_bpf_build.py`
- Test: `tests/unit/test_task_image_builder_guard_bpf.py`
- Test: `tests/unit/test_task_image_builder_guard_containment.py`

**Interfaces:**

- The build script uses exactly
  `docker.io/silkeh/clang:18-bookworm@sha256:3914c93a02e866795aafc80737488e515b96390eff3d2787cf8c5095997baea9`,
  forces `linux/amd64`, invokes Clang 18 with `-target bpfel -O2 -g`, and writes a
  provenance document binding image, compiler, source, object, and ELF-section
  digests. The object contains no host-architecture machine code.
- The BPF object exports exactly eight cgroup programs: IPv4/IPv6 connect,
  UDP4/UDP6 sendmsg, inet socket create/release, ingress, and egress. Map schemas
  bind subject-by-cgroup identity, IPv4/IPv6 endpoint allowlists, traffic
  limits/state, flow cookies, counters, and drop reasons.
- Every program rejects a missing subject, policy, limiter state, malformed or
  fragmented packet, raw socket, unauthorized IP/protocol/port, exhausted
  byte/packet/new-flow/DNS budget, or flow ceiling. Both address families are
  denied unless an exact reviewed endpoint exists.
- `BpfSyscall` implements `BPF_OBJ_GET`, `BPF_MAP_UPDATE_ELEM`,
  `BPF_OBJ_GET_INFO_BY_FD`, `BPF_LINK_CREATE`, and `BPF_OBJ_PIN` for x86_64 and
  aarch64 using zero-initialized `bpf_attr` buffers and checked descriptor
  ownership.
- `BpfLoader.attach(tree: ContainmentTree, policy: NetworkPolicy) ->
  BpfAttachment` runs the pinned `bpftool prog loadall` three times so the root,
  trusted-service, and build-egress scopes own independent maps/state; programs
  maps before link creation; pins 24 links under a new per-grant staging
  directory; reads back sorted nonzero link/program/map IDs; and atomically
  publishes the pin directory.
- `ContainmentManager.prepare(batch: BatchCgroup, peer: PeerHandle,
  policy: GuardPolicy) -> ContainmentAttachment` creates only
  `loom-builder/{trusted-service,build-egress}`, verifies inherited finite CPU,
  memory, zero swap and device authority, attaches policy while empty, moves
  only the pidfd-pinned peer into `trusted-service`, then enables delegated
  `io pids`, writes/reads exact positive `pids.max` and every `io.max` ceiling,
  and returns canonical attachment/probe evidence.

- [ ] **Step 1: Write failing BPF build and object-contract tests**

  Invoke the builder through an injected command runner and parse the generated
  ELF with a small test utility. Name the breaks: a mutable image, missing
  `bpfel`, host-endian output, missing hook/map, changed source digest, unknown
  section, or a provenance/object mismatch.

- [ ] **Step 2: Run the build tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_guard_bpf_build.py`

- [ ] **Step 3: Add BPF source/build tooling and generate the checked object**

  Build through the digest-pinned container, inspect the object as `EM_BPF`
  little-endian ELF, and retain the provenance JSON as a reviewed source
  artifact.

- [ ] **Step 4: Run build tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_guard_bpf_build.py`

- [ ] **Step 5: Write failing syscall/loader tests**

  Use a fake `libc.syscall` and pinned-command runner while exercising real
  binary struct layouts. Name the breaks: attaching before map population,
  wrong attach type/target, duplicate or zero IDs, unpinned links, non-atomic
  publish, missing program, policy digest mismatch, or cleanup of a published
  deny link on an error path.

- [ ] **Step 6: Run BPF loader tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_bpf.py`

- [ ] **Step 7: Implement the raw syscall wrapper and fail-closed loader**

  Keep link FDs open until every link is pinned and read back. On any ambiguity,
  preserve the staging pins and return a quarantinable error code.

- [ ] **Step 8: Run BPF loader tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_bpf.py`

- [ ] **Step 9: Write failing containment-order/readback tests**

  Record every filesystem/BPF/peer operation in a fake kernel adapter. Name the
  breaks: a link attached after the move, writing an ancestor/sibling, moving
  `slurmstepd`, enabling controllers before the parent is empty, an unbounded
  inherited value, incorrect I/O device, or a scalar not read back exactly.

- [ ] **Step 10: Run containment tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_containment.py`

- [ ] **Step 11: Implement ordered containment creation and proof**

  Restrict every mutation through a `ContainmentTree` that retains opened
  directory FDs and validates device/inode identities before each write.

- [ ] **Step 12: Run all Task 3 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_guard_bpf_build.py tests/unit/test_task_image_builder_guard_bpf.py tests/unit/test_task_image_builder_guard_containment.py`

- [ ] **Step 13: Commit Task 3**

  Commit message: `feat(builder): pin allocation network containment`

---

### Task 4: mTLS authority client, crash-persistent ledger, reconciliation, and service loop

**Files:**

- Create: `src/loom_task_image_builder_guard/authority.py`
- Create: `src/loom_task_image_builder_guard/ledger.py`
- Create: `src/loom_task_image_builder_guard/service.py`
- Create: `src/loom_task_image_builder_guard/__main__.py`
- Test: `tests/unit/test_task_image_builder_guard_authority.py`
- Test: `tests/unit/test_task_image_builder_guard_ledger.py`
- Test: `tests/unit/test_task_image_builder_guard_service.py`
- Test: `tests/integration/test_task_image_builder_guard_local_flow.py`

**Interfaces:**

- `AuthorityClient` builds one TLS 1.3 client context from root-owned CA,
  certificate, and key files, disables redirects, reads a root-owned bearer,
  sets a fixed `Authorization: Bearer` header, uses bounded HTTPS responses and
  timeouts, and exposes `challenge`, `attach`, `exchange`, `attest`, and
  `revoke`. Route paths are derived internally from the grant UUID.
- Response validators accept exactly the Phase 2B1 challenge, projection
  receipt, session, and attestation wire shapes; every UUID, digest, time,
  generation, proof binding, and token prefix/length is checked before use.
- `GuardLedger.create_intent(...)`, `record_challenge(...)`,
  `record_attachment(...)`, `record_exchange(...)`, `quarantine(...)`, and
  `remove_terminal(...)` write root-owned `0600`, single-link JSON through
  create/fsync/rename/fsync. Entries contain only nonsecret IDs, digests,
  cgroup/pin identities, state, generations, and deadlines.
- `GuardService.start()` reconciles every ledger entry and orphan pin directory
  before binding its root-owned `0660 root:loom-task-builder` socket. Live exact
  entries retain pinned policy; terminal+empty exact entries are removed;
  ambiguity is durably quarantined and invokes only
  `scontrol update NodeName=<exact> ActiveFeatures-=loom_rootless_buildkit`.
- Projection order is peer capture -> ledger intent -> exact Slurm/cgroup
  observation -> authority challenge -> empty-tree attachment -> peer move ->
  authority proof -> sealed projection memfd -> ack. Exchange accepts a sealed
  bootstrap descriptor, calls the authority as the node identity, returns a
  sealed session descriptor, and records only its public binding.
- A monotonic attestation loop revalidates peer/cgroup/pins/Slurm before each
  generation and stops renewal on any drift. Errors revoke when an exact grant
  is known, quarantine only builder capability on ambiguity, and never include
  secret/request bodies in logs.

- [ ] **Step 1: Write failing authority-client tests**

  Use an in-process TLS server with a test CA/client certificate and real HTTP
  framing. Name the breaks: no client cert, redirect following, wrong bearer,
  oversized/chunked response, non-JSON/duplicate keys, route injection, binding
  mismatch, expired response, or a token appearing in an exception/repr.

- [ ] **Step 2: Run authority tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_authority.py`

- [ ] **Step 3: Implement the fixed mTLS client and strict response models**

  Open credential files with `read_stable_file`; never allow a request to select
  a URL, header, certificate, or repository scope.

- [ ] **Step 4: Run authority tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_authority.py`

- [ ] **Step 5: Write failing ledger/restart tests**

  Simulate crashes before and after each fsync/rename boundary. Name the breaks:
  accepting a symlink/hardlink, losing an intent ID required for exact replay,
  persisting a token, deleting deny pins on ambiguity, cleaning a live/nonempty
  job, accepting changed IDs/digests, or binding the socket before reconciliation.

- [ ] **Step 6: Run ledger tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_ledger.py`

- [ ] **Step 7: Implement atomic ledger state and reconciliation decisions**

  Bound entry count and bytes, sort traversal by grant UUID, reject all unknown
  files, and make exact replay produce byte-identical authority requests.

- [ ] **Step 8: Run ledger tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_ledger.py`

- [ ] **Step 9: Write failing service and real local-flow tests**

  Drive a real seqpacket socket and real sealed descriptors with fake Slurm,
  BPF, cgroup, and HTTPS boundaries. Name the breaks: credential release before
  attach/readback, another UID/job peer, missing ack, semantic exchange replay,
  stale attestation, service restart with live pins, broad Slurm mutation, or a
  log/ledger containing `loom_tibp_` or `loom_tibs_`.

- [ ] **Step 10: Run service tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_service.py tests/integration/test_task_image_builder_guard_local_flow.py`

- [ ] **Step 11: Implement service orchestration and CLI**

  The CLI supports only `--config <absolute-path>` and `--self-check`; it rejects
  inherited unsafe environment variables and uses a fixed error-code log format.

- [ ] **Step 12: Run all Task 4 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_authority.py tests/unit/test_task_image_builder_guard_ledger.py tests/unit/test_task_image_builder_guard_service.py tests/integration/test_task_image_builder_guard_local_flow.py`

- [ ] **Step 13: Commit Task 4**

  Commit message: `feat(builder): mediate guarded projection sessions`

---

### Task 5: Deterministic content-addressed guard release

**Files:**

- Create: `deploy/task-image-builder/guard-release-v1.json`
- Create: `deploy/task-image-builder/guard-config-oldlab-v1.example.json`
- Create: `deploy/task-image-builder/guard-config-gb10-v1.example.json`
- Create: `scripts/ops/task_image_builder_guard_release.py`
- Test: `tests/ops/test_task_image_builder_guard_release.py`
- Test: `tests/ops/test_task_image_builder_guard_package_boundary.py`
- Modify: `pyproject.toml`

**Interfaces:**

- `build_release(source_root: Path, bpftool: Path, output: Path,
  architecture: Literal['x86_64','aarch64']) -> GuardRelease` captures the exact
  guard source set, emits a sorted `ZIP_STORED` zipapp with 1980-01-01 timestamps
  and canonical modes, includes no `.pyc`/metadata/writable imports, validates
  the precompiled BPF/provenance/schema/unit artifacts, copies the opened
  bpftool binary, and writes a canonical manifest of every size/mode/SHA-256.
- Release identity is SHA-256 of the canonical manifest excluding only its
  `release_sha256` field; output is a new directory named by that digest and is
  published with no-replace semantics after fsync of every member and directory.
- Package-boundary tests parse the guard AST and zipapp: imports must resolve to
  the standard library or `loom_task_image_builder_guard`; no repository source
  path may remain; invoking the zipapp through `/usr/bin/python3 -I -B` must
  reach the deterministic self-check.
- Example configs contain only public/example values, bind each native cluster,
  and remain unusable without separately provisioned root-owned TLS/bearer files
  and the absent activation marker.

- [ ] **Step 1: Write failing release and package-boundary tests**

  Name the breaks: source-order or mtime affecting bytes, a changed mode/digest,
  unsafe archive name, unbounded member, symlink/hardlink, wrong ELF architecture,
  mutable bpftool path, third-party import, ambient import success, or overwrite
  of an existing release.

- [ ] **Step 2: Run release tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_guard_release.py tests/ops/test_task_image_builder_guard_package_boundary.py`

- [ ] **Step 3: Implement deterministic zipapp and release assembly**

  Read every source/artifact from an opened stable FD, calculate expectations
  independently in tests, and include the final manifest inside and beside the
  release directory.

- [ ] **Step 4: Run release tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_guard_release.py tests/ops/test_task_image_builder_guard_package_boundary.py`

- [ ] **Step 5: Add the package to strict mypy and its console self-check path**

  Extend only the mypy file list; the production systemd invocation remains the
  content-addressed zipapp, not an editable-install console script.

- [ ] **Step 6: Run focused lint/type/package verification**

  Run: `.venv/bin/ruff check src/loom_task_image_builder_guard scripts/ops/task_image_builder_guard_release.py tests/ops/test_task_image_builder_guard_release.py tests/ops/test_task_image_builder_guard_package_boundary.py`

  Run: `.venv/bin/mypy --strict src/loom_task_image_builder_guard`

- [ ] **Step 7: Commit Task 5**

  Commit message: `build(builder): assemble deterministic guard release`

---

### Task 6: Hardened inert unit, staging-only installer, and conformance evidence

**Files:**

- Create: `deploy/task-image-builder/loom-task-image-builder-node-guard.service`
- Create: `scripts/ops/install_task_image_builder_guard.py`
- Create: `scripts/ops/task_image_builder_guard_conformance.py`
- Create: `docs/evidence/task-image-builder-guard-conformance-v1.schema.json`
- Create: `docs/runbooks/task-image-builder-phase2b2-node-guard.md`
- Test: `tests/ops/test_install_task_image_builder_guard.py`
- Test: `tests/ops/test_task_image_builder_guard_conformance.py`
- Test: `tests/ops/test_task_image_builder_guard_deployment.py`

**Interfaces:**

- `stage_guard_release(bundle: Path, context: InstallContext) -> StageReceipt`
  requires root in live mode, validates the manifest and every opened member,
  installs only to
  `/opt/loom-task-image-builder-guard/releases/<release_sha256>/`, and writes a
  durable receipt under `/var/lib/loom-task-image-builder-guard/staged/`.
- Staging does not write `/etc/loom`, `/etc/systemd/system`, `/run`, or bpffs;
  does not create `current`, `active`, or the Unix socket; and never invokes
  `systemctl`, `scontrol`, or the authority service.
- The shipped unit requires an exact config and
  `/etc/loom/task-image-builder-guard/activation-v1.json`, runs root with a
  closed capability/system-call/address-family/filesystem boundary sufficient
  only for cgroup/BPF/procfs inspection and fixed HTTPS, has finite
  `MemoryMax`, `TasksMax`, startup timeout, watchdog, restart limit, and explicit
  writable Loom runtime/state/bpffs paths. It has no default activation symlink.
- `conform(staged_release, *, live=False) -> ConformanceReport` verifies release
  digest/modes/owners, isolated zipapp self-check, `EM_BPF` object/provenance,
  map schema, unit hardening/inert conditions, no live config/marker/current
  link/socket/process/pins, and unchanged Phase 1/rootless provider blockers.
  Live mode additionally probes kernel/cgroup/bpffs/pidfd/memfd and staged
  bpftool features without attaching to a foreign cgroup.
- The report matches schema
  `loom.task-image-builder-guard-conformance/v1`, includes only public digests
  and typed checks, sets `production_ready=false`, and retains
  `phase2_guard_provider_release_missing`.

- [ ] **Step 1: Write failing installer safety/idempotency tests**

  Name the breaks: non-root live staging, changed source inode, digest mismatch,
  path escape, preexisting different release, partial publish, un-fsynced
  receipt, a `systemctl`/Slurm call, or creation of any activation/live path.

- [ ] **Step 2: Run installer tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_install_task_image_builder_guard.py`

- [ ] **Step 3: Implement staging-only install and exact idempotency**

  A repeated byte-identical release returns the same receipt; a same-name
  mismatch fails closed and preserves both the existing release and the staged
  candidate for inspection.

- [ ] **Step 4: Run installer tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/ops/test_install_task_image_builder_guard.py`

- [ ] **Step 5: Write failing unit/deployment/conformance tests**

  Execute the conformance script against a controlled root. Name the breaks: a
  weakened unit directive, missing activation condition, broad writable path,
  mutable ExecStart, active service/socket/pin, absent object hook/map, invalid
  receipt, enabled provider, certified node, removed blocker, or conformance
  claiming production readiness.

- [ ] **Step 6: Run deployment/conformance tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_guard_deployment.py tests/ops/test_task_image_builder_guard_conformance.py`

- [ ] **Step 7: Implement unit, conformance report, schema, and operator runbook**

  Document staging and read-only inspection commands, the deliberate absence of
  activation, exact quarantine semantics, and the Phase 2C inputs required
  before promotion.

- [ ] **Step 8: Run all Task 6 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/ops/test_install_task_image_builder_guard.py tests/ops/test_task_image_builder_guard_deployment.py tests/ops/test_task_image_builder_guard_conformance.py`

- [ ] **Step 9: Commit Task 6**

  Commit message: `ops(builder): stage inert node guard release`

---

### Task 7: Cross-boundary regression, self-review, PR, and protected merge

**Files:**

- Modify only files listed in Tasks 1-6 when fixing verified defects.

- [ ] **Step 1: Run the complete Phase 2B2 and affected Phase 2 suite**

  Run all new guard tests plus the Phase 2B1 authority, Phase 2A projection,
  provider-policy, prerequisite-profile, host convergence/evidence/conformance,
  and deployment contract tests.

- [ ] **Step 2: Run repository-quality gates**

  Run Ruff on every changed Python file, strict mypy on the guard package and
  changed scripts, all three Alembic head checks, deterministic release rebuild
  comparison, `git diff --check`, forbidden-path scan, secret-prefix scan, and
  `git status --short`.

- [ ] **Step 3: Perform an adversarial self-review against every invariant**

  Review the full base-to-head diff for descriptor leaks, TOCTOU windows,
  pidfd/re-exec gaps, filesystem escape, secret serialization, BPF open-window
  ordering, unsafe cleanup, broad Slurm mutation, non-determinism, third-party
  imports, service activation, Phase 1 drift, and configuration ambiguity. For
  each real defect, first add a failing regression test, then fix and rerun the
  focused and complete gates. Repeat until the review finds no unresolved issue.

- [ ] **Step 4: Commit verified review fixes**

  Use narrow commit messages describing each proven defect; do not rewrite or
  force-push published history.

- [ ] **Step 5: Push the feature branch and open a PR to `dev`**

  Include the approved design, threat-boundary summary, TDD evidence, exact
  verification commands/results, inertness proof, generated-object provenance,
  and Phase 2C handoff in the PR body. Enable ordinary squash auto-merge only
  after current-head checks exist.

- [ ] **Step 6: Resolve review and protected CI without bypasses**

  Reproduce every valid finding with a failing test. If `dev` advances, use the
  forge update-branch operation; never force-push, admin-merge, or bypass a
  required check.

- [ ] **Step 7: Verify the squash merge and post-merge invariants**

  Fetch `origin/dev`; prove the tested head tree equals the squash tree; require
  one head in each migration family; confirm authority replicas zero and
  default-deny, both rootless providers disabled, production certification
  false, no certified nodes, the provider-release blocker present, no
  activation marker/current link/feature advertisement, and Phase 1 unchanged.

- [ ] **Step 8: Record the Phase 2C handoff**

  Report exact merged commit/tree and the remaining work: allocation supervisor,
  sealed bootstrap/session consumer, RootlessKit/BuildKit executor, quota-backed
  storage, claim/heartbeat/cleanup, and composite provider release. Do not call
  Phase 2 production-active.

