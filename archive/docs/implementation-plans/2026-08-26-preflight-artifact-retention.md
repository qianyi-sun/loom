# Preflight Artifact Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Safely retire old, unreferenced preflight artifact bundles without
allowing publication, lookup, restart, or maintenance races.

**Architecture:** A service-owned lock outside `preflight-artifacts/` gives
readers and inventory shared access and gives publication and apply exclusive
access. An evidence collector classifies exact four-file snapshots into
protected or grace-expired candidates; an operator approves a bounded plan
digest, and apply revalidates references and metadata before plan-bound
quarantine. An empty quarantine remains until its receipt is durable, closing
the deleted-but-unreceipted crash window.

**Tech Stack:** Python 3.11, `fcntl.flock`, descriptor-relative filesystem
operations, immutable JSON evidence, argparse, pytest, Ruff, strict mypy.

**Spec:** `docs/architecture/preflight-artifact-lifecycle.md`

## Global Constraints

- The lifecycle lock is a service-owned, single-link `0600` regular file
  outside `preflight-artifacts/`.
- Complete reads and inventory take a shared lock; publication and apply take
  an exclusive lock.
- Retention never runs automatically during publication or lookup.
- Reference collection is evidence-first and fails closed on unreadable,
  malformed, unknown, duplicated, or changing authority.
- Protect active, nonterminal, advancing, resumable, current-release,
  maintenance-pinned, and younger-than-seven-day references.
- One plan contains at most 32 candidates; more require another approval.
- Plans bind directory and file device, inode, owner, group, mode, link count,
  size, modification/change timestamps, and file SHA-256.
- A candidate contains only `artifact.json`, `rendered.yaml`, `migration.yaml`,
  and `production-defaults.json` as private, regular, single-link files.
- Symlinks, hard links, unexpected files, unknown entries, authority drift,
  changing metadata, and unreceipted disappearance block deletion.
- Restart may complete only the quarantine named by the durable exact claim.
- Use locked `uv==0.11.26`; make no dependency changes.
- Do not create or push `docs/superpowers/**`.
- Integrate only by PR to `dev`, protected CI, and squash merge.

---

## File Map

- `src/loom_cli/rollout/preflight_artifact_store.py`: lifecycle locking.
- `src/loom_cli/rollout/preflight_artifact_retention.py`: immutable retention
  records, plans, and policy.
- `src/loom_cli/rollout/operator/preflight_artifact_references.py`: installed
  evidence-reference discovery.
- `src/loom_cli/rollout/operator/installed_preflight_artifact_retention.py`:
  inventory, approval, claims, quarantine, receipts, restart, and apply.
- `src/loom_cli/rollout/operator/store.py`: strict request enumeration and the
  durable artifact-retention claim/receipt authority.
- `src/loom_cli/rollout/operator/lifecycle.py` and
  `checkpoint_coordinator.py`: durable admission blocks.
- `src/loom_cli/rollout/operator/broker.py`: coordinator-only command.
- Focused tests under `tests/loom_cli/rollout/` and its `operator/` subtree.

### Task 1: Add one artifact-store lifecycle lock

**Files:**

- Modify: `src/loom_cli/rollout/preflight_artifact_store.py:1-483`
- Modify: `tests/loom_cli/rollout/test_preflight_artifact_store.py`

**Interfaces:**

- Produces `shared_lifecycle_lock() -> AbstractContextManager[None]`.
- Produces `exclusive_lifecycle_lock() -> AbstractContextManager[None]`.
- `publish()` holds exclusive access through final readback; `read()` and
  `load()` hold shared access through full reconstruction.

- [ ] **Step 1: Write failing lock tests**

Name these breaks: a reader releasing between files, publication beside
retirement, and an unsafe alias being trusted. With threads and events, hold
exclusive access and prove `read()` waits; hold shared access and prove
`publish()` waits. Assert `state/preflight-artifacts.lock` is a service-owned,
single-link `0600` regular file. Replace it with a symlink and hard link and
require `PreflightArtifactStoreError`.

- [ ] **Step 2: Run and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/test_preflight_artifact_store.py -k 'lifecycle_lock or lock_alias or serialization'
```

Expected: FAIL because the lock API is missing.

- [ ] **Step 3: Implement the safe re-entrant context**

Add `fcntl` and a per-store `ContextVar`. Create
`state_root/preflight-artifacts.lock` with `O_CREAT|O_EXCL|O_NOFOLLOW`, fsync
it and `state_root`, and validate existing type, service UID, exact mode, and
link count without repairing drift. Acquire `LOCK_SH` or `LOCK_EX`; nested
shared under shared and any nested operation under exclusive reuse the held
descriptor, while shared-to-exclusive promotion fails.

Refactor the existing bodies into private locked helpers so public entry
points take one lock for the entire operation and `publish()` performs final
readback without reacquiring.

- [ ] **Step 4: Run full store tests and verify green**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/test_preflight_artifact_store.py
```

- [ ] **Step 5: Commit**

```bash
git add src/loom_cli/rollout/preflight_artifact_store.py tests/loom_cli/rollout/test_preflight_artifact_store.py
git commit -m "feat(rollout): lock preflight artifact lifecycle"
```

### Task 2: Define exact bounded retention plans

**Files:**

- Create: `src/loom_cli/rollout/preflight_artifact_retention.py`
- Create: `tests/loom_cli/rollout/test_preflight_artifact_retention.py`

**Interfaces:**

- Produces frozen slotted `ArtifactFileIdentity`,
  `PreflightArtifactInventoryRecord`, `OpaqueArtifactEvidence`,
  `PreflightArtifactProtection`, and `PreflightArtifactRetentionPlan`.
- `plan_digest` is SHA-256 of canonical compact sorted JSON.
- Fixed policy: `RETENTION_GRACE = timedelta(days=7)`,
  `MAX_RETIREMENTS_PER_PLAN = 32`, and the exact four-file tuple.

- [ ] **Step 1: Write failing contract and policy tests**

Use hand-derived literal metadata for round trips. Independently mutate a
duplicate digest, ordering, file set, kind/mode, link count, timestamp, hash,
protection reason, overlap, 33 candidates, and non-UTC inventory time. Build
40 old records and assert only the oldest 32 are selected with digest tie
breaks. Assert exactly seven days is eligible and one nanosecond younger is
protected.

- [ ] **Step 2: Run and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/test_preflight_artifact_retention.py
```

Expected: import failure because the module is absent.

- [ ] **Step 3: Implement strict immutable contracts**

Use exact-key `to_dict()`/`from_dict()` validation. Record directory and file
`st_dev`, `st_ino`, `st_uid`, `st_gid`, mode, `st_nlink`, `st_size`,
`st_mtime_ns`, and `st_ctime_ns`; files also record SHA-256. A bundle validates
its digest name, private directory authority, exact file names, and descriptor
digest. The plan records root identity, inventory time/cutoff, references with
sorted reasons, candidates, protected records, opaque evidence, environment,
namespace, and schema.

- [ ] **Step 4: Implement classification and cap**

Referenced records are protected. Add `grace-period` to records newer than the
fixed cutoff. Sort the remainder oldest-first, digest for ties, and take 32.
Opaque evidence forces an empty candidate tuple so it cannot coexist with
broad deletion authority.

- [ ] **Step 5: Run tests and commit**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/test_preflight_artifact_retention.py
git add src/loom_cli/rollout/preflight_artifact_retention.py tests/loom_cli/rollout/test_preflight_artifact_retention.py
git commit -m "feat(rollout): model artifact retention plans"
```

### Task 3: Collect every installed evidence reference

**Files:**

- Create: `src/loom_cli/rollout/operator/preflight_artifact_references.py`
- Create: `tests/loom_cli/rollout/operator/test_preflight_artifact_references.py`
- Modify: `src/loom_cli/rollout/operator/store.py:302-1414`
- Modify: `tests/loom_cli/rollout/operator/test_store.py`

**Interfaces:**

- Produces `RequestStore.request_ids() -> tuple[str, ...]` and
  `attempt_numbers(request_id: str) -> tuple[int, ...]`; neither skips unknown
  or unsafe entries.
- Produces
  `InstalledPreflightArtifactReferenceInventory.collect(now: datetime) -> tuple[PreflightArtifactProtection, ...]`.
- Every request reference comes from
  `PreflightArtifactReference.from_assessment()`.

- [ ] **Step 1: Write failing strict-enumeration tests**

Create real request-store records and assert deterministic request/attempt
enumeration. Add a symlink, unknown file, unsafe mode, numeric gap, and an
attempt without `envelope.json`; each must raise `RequestStoreError`.

- [ ] **Step 2: Run enumeration tests and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_store.py -k 'request_ids or attempt_numbers'
```

- [ ] **Step 3: Implement strict enumeration**

Use descriptor-relative `os.scandir()` under validated private roots, validate
every entry rather than filtering it, compare root metadata before/after, and
call typed readers for every discovered request/envelope. Require consecutive
positive attempt numbers and return sorted tuples.

- [ ] **Step 4: Write failing reference tests**

Use real typed evidence and literal expected digest/reason pairs. Cover active
pointer; backup pending/running/cancel-requested/verified/launch-pending;
failed backup before `backup_cleanup_done`; backup rotation active/candidate;
fresh failed/cancelled/launch-failed resume; post-protected-apply resume;
newest `attempt_done` release; backup retention/recovery claims; in-flight
manifest ownership; and in-flight lifecycle capacity. Prove preview requests,
cleaned terminal preflight failures, and expired/superseded records are
excluded. Corrupt or duplicate each authority and require the entire
collection to fail.

- [ ] **Step 5: Run reference tests and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_preflight_artifact_references.py
```

- [ ] **Step 6: Implement evidence-first collection**

Merge allowlisted reasons into sorted sets. Resume eligibility requires exact
request/envelope config binding and readable fresh attestation/lease, except
that a durable successful `final.protected-apply` chain remains protected for
post-apply resume. The current installed release is the newest valid
`attempt_done` UTC event with request ID as tie break. Maintenance readers
validate exact private schemas and treat unknown/changing journals as errors.

- [ ] **Step 7: Run tests and commit**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_store.py tests/loom_cli/rollout/operator/test_preflight_artifact_references.py
git add src/loom_cli/rollout/operator/store.py src/loom_cli/rollout/operator/preflight_artifact_references.py tests/loom_cli/rollout/operator/test_store.py tests/loom_cli/rollout/operator/test_preflight_artifact_references.py
git commit -m "feat(rollout): inventory artifact references"
```

### Task 4: Add durable claims and receipts

**Files:**

- Modify: `src/loom_cli/rollout/operator/store.py`
- Modify: `src/loom_cli/rollout/operator/lifecycle.py`
- Modify: `src/loom_cli/rollout/operator/checkpoint_coordinator.py`
- Modify: `tests/loom_cli/rollout/operator/test_store.py`
- Modify: `tests/loom_cli/rollout/operator/test_lifecycle.py`
- Modify: `tests/loom_cli/rollout/operator/test_checkpoint_coordinator.py`

**Interfaces:**

- Produces
  `read_preflight_artifact_retention_claim() -> tuple[str, tuple[str, ...]] | None`.
- Produces `claim_preflight_artifact_retention(plan_sha256, bundle_digests)`
  and `clear_preflight_artifact_retention_claim(plan_sha256)`.
- Produces exact publish/read methods for retirement receipts bound to bundle,
  plan, and inventory-record digest.

- [ ] **Step 1: Write failing claim/receipt tests**

Prove exact idempotent claim, competing-plan rejection, active rejection,
wrong clear rejection, claim blocking `set_active`, sorted unique maximum-32
digests, receipt no-replace idempotence, collision rejection, and rejection of
a self-consistent receipt from another plan/record.

- [ ] **Step 2: Write failing admission tests**

Assert a durable artifact claim blocks lifecycle admission with reason
`preflight_artifact_retention_busy` without a maintenance marker and blocks
checkpoint work before all creator/activator/retirer side effects.

- [ ] **Step 3: Run and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_store.py tests/loom_cli/rollout/operator/test_lifecycle.py tests/loom_cli/rollout/operator/test_checkpoint_coordinator.py -k 'artifact_retention'
```

- [ ] **Step 4: Implement exact durable authority**

Store `preflight-artifact-retention-claim.json` under the active lock and
immutable receipts under
`preflight-artifact-retirements/{bundle_digest}.json`.
Claims contain exact sorted bundle digests, schema, and approved plan digest.
Receipts contain bundle, plan, record digest, and schema. Update active,
lifecycle, and checkpoint admission to check both backup and artifact claims.

- [ ] **Step 5: Run tests and commit**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_store.py tests/loom_cli/rollout/operator/test_lifecycle.py tests/loom_cli/rollout/operator/test_checkpoint_coordinator.py -k 'retention'
git add src/loom_cli/rollout/operator/store.py src/loom_cli/rollout/operator/lifecycle.py src/loom_cli/rollout/operator/checkpoint_coordinator.py tests/loom_cli/rollout/operator/test_store.py tests/loom_cli/rollout/operator/test_lifecycle.py tests/loom_cli/rollout/operator/test_checkpoint_coordinator.py
git commit -m "feat(rollout): claim artifact retirement authority"
```

### Task 5: Inventory and retire with restart convergence

**Files:**

- Create:
  `src/loom_cli/rollout/operator/installed_preflight_artifact_retention.py`
- Create:
  `tests/loom_cli/rollout/operator/test_installed_preflight_artifact_retention.py`

**Interfaces:**

- Produces `inventory() -> PreflightArtifactRetentionPlan`.
- Produces `load_claim(approved_plan_digest)`, `claim(plan)`, and `apply(plan)`.
- Evidence root: `state_root/preflight-artifact-retention`.
- Quarantine root: `state_root/preflight-artifact-quarantine`.

- [ ] **Step 1: Write failing exact-inventory tests**

Publish real bundles, age selected entries with `os.utime()`, inject references,
and assert candidates/reasons, seven-day boundary, exact metadata/hashes,
ordering, and cap. Add symlink, hard link, fifth file, unsafe directory,
descriptor drift, changing-entry hook, and unknown root entry; each yields no
deletion authority or a normalized installed-retention error.

- [ ] **Step 2: Run inventory tests and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_installed_preflight_artifact_retention.py -k 'inventory or grace or unsafe or unknown'
```

- [ ] **Step 3: Implement shared-lock inventory**

Open the artifact root with `O_DIRECTORY|O_NOFOLLOW`, record root identity, and
scan every child. Open exact digest directories relative to that descriptor,
require only the four names, open files `O_RDONLY|O_NOFOLLOW`, validate before
and after a bounded read, and hash bytes. Call typed store `read()` under the
re-entrant shared lock. Changing observations raise; unknown safe evidence is
opaque and prevents candidates; unsafe paths raise.

- [ ] **Step 4: Write failing approval/apply/restart tests**

Cover wrong approval, competing claim, candidate/reference/protected drift,
concurrent publication, active rollout, and opaque drift before rename. Assert
the exact ordering: plan-bound rename, both parent fsyncs, no-follow recheck,
four unlinks, empty quarantine retained, receipt publication, quarantine
removal, applied evidence, then claim clear.

Inject crashes after rename, one unlink, four unlinks, receipt, and quarantine
removal. Restart with the same claim must converge. Source and quarantine both
absent without receipt, mismatched quarantine, recreated source after receipt,
or receipt from another plan must fail closed and keep the claim.

- [ ] **Step 5: Run apply tests and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_installed_preflight_artifact_retention.py -k 'apply or claim or quarantine or restart or receipt or concurrent'
```

- [ ] **Step 6: Implement approval, exact claim, and convergence**

Inventory publishes `{plan_digest}.plan.json` no-replace. `load_claim()` validates
digest, private metadata, canonical round trip, and computed digest. `claim()`
requires no active rollout. `apply()` takes a private nonblocking `.apply.lock`,
requires the exact durable claim, then takes the artifact exclusive lock and
recollects references/store metadata.

Before progress require exact root timestamps. On restart permit only timestamp
changes explained by exact claimed quarantines/receipts, while root identity
and every remaining entry still match. A candidate newly protected aborts.

For each candidate accept exactly source, exact quarantine, or exact receipt.
Rename descriptor-relative and fsync both parents. Revalidate/delete the exact
files from the quarantine descriptor. Publish the receipt while the empty
directory still exists, then remove/fsync it. A receipt with a recreated source
is drift. Publish applied evidence, validate all receipts, then clear the claim
while the execution guard remains held.

- [ ] **Step 7: Run tests and commit**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_installed_preflight_artifact_retention.py
git add src/loom_cli/rollout/operator/installed_preflight_artifact_retention.py tests/loom_cli/rollout/operator/test_installed_preflight_artifact_retention.py
git commit -m "feat(rollout): retire exact preflight artifacts"
```

Before finishing, publish 257 complete old siblings with one protected active
reference and assert repeated bounded plans retire only unprotected bundles
without changing that protected publication or its readability.

### Task 6: Wire coordinator-only broker authority

**Files:**

- Modify: `src/loom_cli/rollout/operator/broker.py`
- Modify: `tests/loom_cli/rollout/operator/test_broker.py`
- Modify allowed operator docs only if the installed command catalog is already
  maintained outside `docs/superpowers/**`.

**Interfaces:**

- Adds `preflight-artifact-retention inventory`.
- Adds `preflight-artifact-retention apply --approved-plan-sha256 DIGEST`.
- Adds `BrokerDependencies.preflight_artifact_retention`.

- [ ] **Step 1: Write failing parser/authority tests**

Reject non-coordinators, absent maintenance, active rollout, unconfigured
service, malformed digest, and non-sealed authority. Assert inventory, claim,
and bounded local apply all occur while the exclusive launch guard is held,
and stdout is secret-free canonical JSON.

- [ ] **Step 2: Run broker tests and verify RED**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_broker.py -k 'preflight_artifact_retention'
```

- [ ] **Step 3: Implement command and installed wiring**

Require coordinator, sealed-cumulative staging, configured service, and
maintenance-active/idle under `launch_guard()`. Keep the guard through the
bounded maximum-32 local-file apply so lifecycle authority cannot change
between revalidation and retirement. Construct the installed service from
config, `RequestStore`,
`PreflightArtifactStore`, service UID, clock, and the installed reference
collector.

- [ ] **Step 4: Run focused suites and commit**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_broker.py tests/loom_cli/rollout/test_preflight_artifact_store.py tests/loom_cli/rollout/test_preflight_artifact_retention.py tests/loom_cli/rollout/operator/test_preflight_artifact_references.py tests/loom_cli/rollout/operator/test_installed_preflight_artifact_retention.py
git add src/loom_cli/rollout/operator/broker.py tests/loom_cli/rollout/operator/test_broker.py
git commit -m "feat(rollout): expose artifact retention command"
```

### Task 7: Verify, self-review, and integrate

**Files:** Review every changed file. Do not add `docs/superpowers/**`.

**Interfaces:** None; this is the release gate.

- [ ] **Step 1: Run formatting and static analysis**

```bash
uv run --no-sync ruff format --check src/loom_cli/rollout tests/loom_cli/rollout
uv run --no-sync ruff check src tests packages migrations capacity_guard_migrations capacity_migrations
uv run --no-sync mypy
```

Expected: zero errors.

- [ ] **Step 2: Run the rollout partition**

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout tests/ops/test_staging_rollout_host.py
```

Expected: zero failures/errors.

- [ ] **Step 3: Self-review every invariant**

Read the design and plan from the top, then inspect `git diff --check`, diff
stat, and complete diff. For every invariant identify the production branch
and test mutation. Trace crashes at every rename/delete/receipt boundary,
concurrent lookup/publication, a reference added after inventory, unknown state,
hard link, recreated receipted source, and claim clearing. Fix every finding
with a failing test first and repeat Steps 1-2 until clean.

- [ ] **Step 4: Verify scope**

```bash
git status --short
git diff --check
git diff --name-only origin/dev...HEAD
```

Expected: only planned source/test/architecture/archived-plan paths; no
`docs/superpowers/**`, secrets, runtime evidence, or unrelated files.

- [ ] **Step 5: Push and open PR**

```bash
git push -u origin feat/preflight-artifact-retention
gh pr create --base dev --head feat/preflight-artifact-retention --title "feat(rollout): retain preflight artifacts safely" --body "Adds evidence-first, digest-approved preflight artifact retention with shared/exclusive lifecycle locking, bounded exact plans, crash-safe quarantine, and durable receipts. Verification: rollout tests, Ruff, and mypy."
gh pr checks --watch
```

Do not bypass protection. Address review/CI findings with TDD and rerun gates.

- [ ] **Step 6: Squash merge after all required checks pass**

```bash
gh pr merge --squash --delete-branch
```

Record PR URL, squash SHA, check counts, and merge state.

### Task 8: Install, inventory, converge, and validate builders

**Files:** No repository edits unless evidence exposes a separate bug; that
requires another isolated worktree/PR/CI cycle.

**Interfaces:** Root-owned installed rollout authority and builder validation
commands delivered by prior activation phases.

- [ ] **Step 1: Install merged `dev` through root authority**

```bash
cd /opt/loom-staging-runner/source
sudo -n /usr/bin/python3 -B scripts/ops/staging_rollout_host.py install --source-mode merged-dev --smoke-on-behalf-team-id bbce1c49-8d6b-429c-a338-de37a6b533b7
```

Require ready install record, admission enabled, maintenance disabled, clean
host check, and passing protected preflight.

- [ ] **Step 2: Enter maintenance and run inventory only**

Use the root-owned maintenance transition, then:

```bash
/usr/local/bin/loom-staging-rollout --env staging preflight-artifact-retention inventory
```

Review every candidate digest, age, record digest, protected reason, opaque
record, count, and plan digest. Do not apply unexplained items.

- [ ] **Step 3: Apply only the reviewed digest and restore admission**

```bash
read -r LOOM_ARTIFACT_PLAN_SHA256
/usr/local/bin/loom-staging-rollout --env staging preflight-artifact-retention apply --approved-plan-sha256 "$LOOM_ARTIFACT_PLAN_SHA256"
```

Require one receipt per candidate, no quarantine residue, cleared claim,
exact applied evidence, and clean repeat inventory. Restore admission only
through the root-owned maintenance transition.

- [ ] **Step 4: Validate native builders**

Run existing Phase 1 health/preflight/one-shot materialization evidence on GB10
ARM64 and OLDLAB x86_64. Require builder-token admission, registry push/exact
pull, Slurm QoS/reservation/exclusive native capacity, no trial starvation,
and retention evidence. Do not expose credentials.

- [ ] **Step 5: Rerun task `4139e767`**

Require terminal evidence for native task-image materialization, exact digest
pull, nonzero LLM calls, valid lifecycle/ATIF evidence, and passing verifier
evidence. If it fails, preserve receipts and diagnose in a new worktree rather
than mutating staging manually.
