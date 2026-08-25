# Preflight Artifact Exact Lookup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every cardinality-dependent preflight artifact lookup and make
all detached and maintenance consumers load the exact immutable bundle digest
bound by assessment evidence or operator input.

**Architecture:** Add a strict `PreflightArtifactReference` projection of the
passing `artifacts.publish` execution, replace `load_exact()` enumeration with
one digest-addressed loader, and thread that reference through the detached
runtime factory. Maintenance inventory/apply commands carry the exact digest
explicitly, and broker preflight/start responses expose the same secret-free
digest for operators.

**Tech Stack:** Python 3.11, immutable JSON evidence, pathlib/openat-style file
validation, argparse, pytest, Ruff, strict mypy.

**Spec:** `docs/architecture/preflight-artifact-lifecycle.md`

## Global Constraints

- Implement only the exact-lookup correction from design phase 1; do not
  delete, quarantine, rename, or garbage-collect an existing artifact.
- Do not introduce a mutable `latest` pointer, database catalog, directory
  index, raised scan limit, or store-wide runtime enumeration.
- Preserve existing immutable descriptor and request/assessment schemas.
- Every selected digest must still revalidate candidate SHA, candidate tree,
  mutation epoch, images, manifests, migration inputs, and production defaults.
- Historical duplicate publications remain individually addressable by digest.
- Shared staging mutation remains owned by `loom-staging-rollout`.
- Do not create or push any path under `docs/superpowers/**`.
- Use the locked `uv==0.11.26` environment and make no dependency changes.
- Integrate only through a PR to `dev`, passing required CI before squash merge.

---

## File Map

- `src/loom_cli/rollout/preflight_artifact_reference.py` owns strict extraction
  and validation of the one Tier 1 publication reference.
- `src/loom_cli/rollout/preflight_artifact_store.py` owns direct digest loading
  and complete reconstruction/identity verification.
- `src/loom_cli/rollout/preflight_orchestrator.py` derives the reference from
  the persisted assessment before detached runtime construction.
- `src/loom_cli/rollout/operator/deep_preflight_authority.py` and
  `src/loom_cli/rollout/operator/installed_deep_preflight.py` thread the
  optional reference through the admission/detached composition boundary.
- `src/loom_cli/rollout/operator/broker.py`,
  `src/loom_cli/rollout/operator/installed_manifest_ownership.py`, and
  `src/loom_cli/rollout/operator/installed_lifecycle_capacity.py` bind explicit
  maintenance digests and publish safe operator output.
- Focused tests under `tests/loom_cli/rollout/` and
  `tests/loom_cli/rollout/operator/` prove the reference, loader, orchestration,
  and CLI contracts.
- `docs/architecture/staging-rollout.md` and
  `docs/architecture/staging-rollout-preflight.md` record the command and
  persistence contract.

### Task 1: Add strict publication references and direct digest loading

**Files:**

- Create: `src/loom_cli/rollout/preflight_artifact_reference.py`
- Create: `tests/loom_cli/rollout/test_preflight_artifact_reference.py`
- Modify: `src/loom_cli/rollout/preflight_artifact_store.py:348-490`
- Modify: `tests/loom_cli/rollout/test_preflight_artifact_store.py:303-438`

**Interfaces:**

- Produces:
  `PreflightArtifactReference.from_assessment(assessment: PreflightAssessment) -> PreflightArtifactReference`.
- Produces:
  `PreflightArtifactReference.require_publication(publication: PreflightArtifactPublication) -> None`.
- Produces:
  `PreflightArtifactStore.load(*, bundle_digest: str, candidate_sha: str, candidate_tree: str, mutation_epoch: int, image_tag: str, namespace: str, image_run: DockerRunner, container_registry_push: str = "") -> LoadedPreflightArtifacts`.
- Removes: `PreflightArtifactStore.load_exact(...)` and all store enumeration.

- [ ] **Step 1: Write failing reference and large-store tests**

Create tests that build a real passing assessment, select its
`artifacts.publish` execution, and assert the exact seven evidence fields map
to a frozen reference. Add malformed cases by replacing that execution with a
failed execution and by removing or duplicating it; each must raise
`PreflightArtifactReferenceError`.

Update the store test to call the wished-for digest API:

```python
loaded = store.load(
    bundle_digest=publication.bundle_digest,
    candidate_sha="a" * 40,
    candidate_tree="f" * 40,
    mutation_epoch=8,
    image_tag=image_tag,
    namespace="loom-staging",
    image_run=_docker,
)
```

Add 257 unrelated digest-shaped directories after publishing the target and
assert the target still loads. Add one unrelated malformed sibling and assert
it is ignored. Add a wrong expected mutation epoch and assert loading fails
with an identity-drift error before image reconstruction.

- [ ] **Step 2: Run the focused tests and observe the expected failures**

Run:

```bash
uv run --no-sync pytest -q \
  tests/loom_cli/rollout/test_preflight_artifact_reference.py \
  tests/loom_cli/rollout/test_preflight_artifact_store.py
```

Expected: FAIL because the reference module and `load()` do not exist and the
current `load_exact()` rejects a store larger than 256 entries.

- [ ] **Step 3: Implement the strict reference projection**

Add a frozen, slotted dataclass with these fields:

```python
@dataclass(frozen=True, slots=True)
class PreflightArtifactReference:
    bundle_digest: str
    image_artifact_sha256: str
    manifest_artifact_sha256: str
    rendered_manifest_sha256: str
    migration_manifest_sha256: str
    migration_artifact_sha256: str
    production_defaults_sha256: str
```

`from_assessment()` must require exactly one `artifacts.publish` execution,
require it to have passed, require the exact evidence-key set, validate every
value as lowercase 64-hex, and construct the dataclass. `require_publication()`
must compare every field against the corresponding publication property and
raise a normalized reference error on drift. Export both the type and error.

- [ ] **Step 4: Replace enumeration with one digest-addressed read**

Rename `load_exact()` to `load()`, add required `bundle_digest`, validate all
expected identity inputs, and replace the `os.scandir()`/matches block with:

```python
publication = self.read(bundle_digest)
if (
    publication.candidate_sha != candidate_sha
    or publication.candidate_tree != candidate_tree
    or publication.mutation_epoch != mutation_epoch
):
    raise PreflightArtifactStoreError("preflight artifact lookup identity drifted")
```

Keep the existing trusted-file reads and complete image/manifest/migration/
defaults reconstruction checks unchanged. Remove the ambiguity and 256-entry
selection behavior rather than retaining a compatibility scan.

- [ ] **Step 5: Run the focused tests and verify green**

Run the Step 2 command. Expected: all tests pass.

- [ ] **Step 6: Commit the exact reference and store loader**

```bash
git add \
  src/loom_cli/rollout/preflight_artifact_reference.py \
  src/loom_cli/rollout/preflight_artifact_store.py \
  tests/loom_cli/rollout/test_preflight_artifact_reference.py \
  tests/loom_cli/rollout/test_preflight_artifact_store.py
git commit -m "fix(rollout): load exact preflight artifact digest"
```

### Task 2: Thread assessment evidence through detached rehearsal

**Files:**

- Modify: `src/loom_cli/rollout/preflight_orchestrator.py`
- Modify: `src/loom_cli/rollout/operator/deep_preflight_authority.py`
- Modify: `src/loom_cli/rollout/operator/installed_deep_preflight.py`
- Modify: `tests/loom_cli/rollout/test_preflight_orchestrator.py`
- Modify: `tests/loom_cli/rollout/operator/test_installed_deep_preflight.py`
- Modify: affected fake runtime factories found by
  `rg -n "runtime_factory=|sources_factory=" tests/loom_cli/rollout`.

**Interfaces:**

- Changes `RuntimeFactory` to
  `Callable[[CandidateBinding, int, PreflightArtifactReference | None], CandidatePreflightRuntime]`.
- Changes `RuntimeSourcesFactory` and
  `InstalledDeepPreflightComposition.sources()` to accept the same optional
  reference after `RuntimePurpose`.
- Admission always receives `None`; detached rehearsal receives the exact
  reference derived from its persisted assessment.

- [ ] **Step 1: Write failing orchestration tests**

Change test factories to record `(epoch, reference)` pairs. Assert assessment
construction calls the factory with `None`, while
`build_rehearsal_attestor()` calls it with the exact reference from the
assessment. In the installed composition fake, expose `load()` and assert its
arguments include `bundle_digest=reference.bundle_digest`. Return a loaded
publication whose component digests match the reference.

Add a detached test where the loaded publication disagrees with one component
digest and assert `sources()` fails before building rehearsal actions.

- [ ] **Step 2: Run the orchestration tests and observe the expected failures**

Run:

```bash
uv run --no-sync pytest -q \
  tests/loom_cli/rollout/test_preflight_orchestrator.py \
  tests/loom_cli/rollout/operator/test_installed_deep_preflight.py
```

Expected: FAIL because factories currently accept two arguments and the
installed composition still calls `load_exact()` without evidence.

- [ ] **Step 3: Pass the optional reference through the orchestrator**

In `CandidatePreflightOrchestrator`, make `_runtime()` accept an optional
reference. `assess()` passes `None`. `build_rehearsal_attestor()` derives
`PreflightArtifactReference.from_assessment(assessment)` and passes it to
`_runtime()`. Keep candidate and mutation-epoch checks after factory return.

In `DeepPreflightAuthority._orchestrator()`, require `reference is None` for
admission and non-`None` for detached rehearsal before invoking the sources
factory. This prevents an evidence-free detached runtime or evidence-bearing
admission runtime.

- [ ] **Step 4: Load and verify the referenced publication in installed composition**

Add `artifact_reference: PreflightArtifactReference | None = None` to
`InstalledDeepPreflightComposition.sources()`. For detached rehearsal require
it, call `artifact_store.load(bundle_digest=artifact_reference.bundle_digest,
...)`, then call `artifact_reference.require_publication(loaded.publication)`.
For admission reject a non-`None` reference and retain `loaded_artifacts=None`.

- [ ] **Step 5: Update all typed fakes and run the focused tests**

Use `rg` to update every two-argument runtime/sources fake deliberately; do not
add `*args` solely to hide a missed interface. Run the Step 2 command and
expect all tests to pass.

- [ ] **Step 6: Commit detached evidence threading**

```bash
git add src/loom_cli/rollout/preflight_orchestrator.py \
  src/loom_cli/rollout/operator/deep_preflight_authority.py \
  src/loom_cli/rollout/operator/installed_deep_preflight.py \
  tests/loom_cli/rollout
git commit -m "fix(rollout): bind detached artifacts to assessment evidence"
```

### Task 3: Make maintenance artifact selection explicit

**Files:**

- Modify: `src/loom_cli/rollout/operator/broker.py`
- Modify: `src/loom_cli/rollout/operator/installed_manifest_ownership.py`
- Modify: `src/loom_cli/rollout/operator/installed_lifecycle_capacity.py`
- Modify: `src/loom_cli/rollout/manifest_ownership_operator.py`
- Modify: `tests/loom_cli/rollout/operator/test_broker.py`
- Modify: `tests/loom_cli/rollout/operator/test_installed_manifest_ownership.py`
- Modify: `tests/loom_cli/rollout/operator/test_installed_lifecycle_capacity.py`

**Interfaces:**

- `manifest-ownership inventory` and `apply` require
  `--artifact-bundle-sha256`.
- `lifecycle-capacity inventory` and `apply` require
  `--artifact-bundle-sha256`.
- `InstalledManifestOwnershipService.inventory/apply` and
  `_operator()` accept the digest and use `PreflightArtifactStore.load()`.
- `ArtifactLoader` becomes
  `Callable[[CandidateBinding, int, str], LoadedPreflightArtifacts]`.
- `InstalledLifecycleCapacityService.inventory()` and `prepare_apply()` accept
  the digest; `execute_claimed()` reloads `plan.artifact_bundle_sha256`.

- [ ] **Step 1: Write failing parser and service tests**

Add parser tests proving both maintenance actions reject a missing digest and
accept exactly 64 lowercase hex. Add handler tests asserting the digest reaches
the service for inventory and apply.

Change manifest-ownership tests to call:

```python
service.inventory(candidate, artifact_bundle_sha256="a" * 64)
service.apply(
    candidate,
    artifact_bundle_sha256="a" * 64,
    request_id="req-manifest-ownership-abcd1234",
    approved_inventory_sha256=inventory_digest,
)
```

Change lifecycle-capacity fakes to record the third loader argument and assert
inventory, prepare, and execute all use the exact plan bundle digest. Add a
mismatched digest test that fails before Kubernetes mutation.

- [ ] **Step 2: Run maintenance tests and observe the expected failures**

Run:

```bash
uv run --no-sync pytest -q \
  tests/loom_cli/rollout/operator/test_broker.py \
  tests/loom_cli/rollout/operator/test_installed_manifest_ownership.py \
  tests/loom_cli/rollout/operator/test_installed_lifecycle_capacity.py
```

Expected: FAIL because the CLI does not accept the digest, ownership still
enumerates the store, and the capacity loader accepts only candidate and epoch.

- [ ] **Step 3: Add strict CLI digest arguments and handler plumbing**

Use an argparse type that requires exactly 64 lowercase hex. Add the option as
required on all four subcommands. Extend `_manifest_ownership()` and
`_lifecycle_capacity()` parameters and pass the digest for both inventory and
apply. Keep lifecycle launch/maintenance guards unchanged.

- [ ] **Step 4: Change ownership to load the explicit digest**

Add `artifact_bundle_sha256` to `inventory()`, `apply()`, and `_operator()`.
Call `PreflightArtifactStore.load(bundle_digest=artifact_bundle_sha256, ...)`.
Add `artifact_bundle_sha256` to `OwnershipInventory`, its hash input, and
`to_document()` so the approved inventory changes if the digest changes.
Update `ManifestOwnershipOperator` construction with the digest.

- [ ] **Step 5: Change capacity to pin and replay the plan digest**

Make `inventory(artifact_bundle_sha256=...)` call the three-argument loader.
Make `prepare_apply()` accept the CLI digest and rebuild the approved plan with
it. In `execute_claimed()`, compare repeated inventory using
`plan.artifact_bundle_sha256`; no later step may accept an ambient digest.
Update the default broker loader to call `PreflightArtifactStore.load()`.

- [ ] **Step 6: Run focused tests and commit**

Run the Step 2 command. Expected: all tests pass.

```bash
git add src/loom_cli/rollout/operator \
  src/loom_cli/rollout/manifest_ownership_operator.py \
  tests/loom_cli/rollout/operator
git commit -m "fix(rollout): require maintenance artifact digest"
```

### Task 4: Expose safe digest evidence and close the regression surface

**Files:**

- Modify: `src/loom_cli/rollout/operator/broker.py`
- Modify: `tests/loom_cli/rollout/operator/test_broker.py`
- Modify: `docs/architecture/staging-rollout.md`
- Modify: `docs/architecture/staging-rollout-preflight.md`
- Modify: any tests asserting exact broker response dictionaries.

**Interfaces:**

- Produces:
  `_preflight_artifact_reference(assessment: PreflightAssessment) -> PreflightArtifactReference`.
- Preflight, preview, and backup-pending JSON add
  `preflight_artifact_bundle_sha256`.
- Maintenance examples consume that exact safe response field.

- [ ] **Step 1: Write failing broker response tests**

For `preflight`, `start --dry-run`, and real staged start, assert the response
contains the bundle digest from the passing `artifacts.publish` execution. Add
a malformed assessment fake and assert the broker fails before publishing a
request or backup job.

- [ ] **Step 2: Run the focused broker tests and observe failure**

Run:

```bash
uv run --no-sync pytest -q tests/loom_cli/rollout/operator/test_broker.py
```

Expected: FAIL because current responses contain only the assessment digest.

- [ ] **Step 3: Derive once and publish the safe digest**

Immediately after a passing assessment, construct the strict reference. Reuse
it for response output; do not rescan the store or parse descriptor paths.
Include only the lowercase digest, never descriptor content or paths.

- [ ] **Step 4: Update architecture command documentation**

Document the required maintenance option:

```text
loom-staging-rollout --env staging manifest-ownership inventory \
  --artifact-bundle-sha256 DIGEST
loom-staging-rollout --env staging lifecycle-capacity inventory \
  --artifact-bundle-sha256 DIGEST
```

State that `DIGEST` comes from preflight/start output and is revalidated
against candidate/tree/epoch and approved inventory at apply.

- [ ] **Step 5: Run the complete focused regression suite**

Run:

```bash
uv run --no-sync pytest -q \
  tests/loom_cli/rollout/test_preflight_artifact_reference.py \
  tests/loom_cli/rollout/test_preflight_artifact_store.py \
  tests/loom_cli/rollout/test_preflight_orchestrator.py \
  tests/loom_cli/rollout/operator/test_installed_deep_preflight.py \
  tests/loom_cli/rollout/operator/test_installed_manifest_ownership.py \
  tests/loom_cli/rollout/operator/test_installed_lifecycle_capacity.py \
  tests/loom_cli/rollout/operator/test_broker.py
uv run --no-sync ruff check \
  src/loom_cli/rollout tests/loom_cli/rollout
uv run --no-sync mypy \
  src/loom_cli/rollout/preflight_artifact_reference.py \
  src/loom_cli/rollout/preflight_artifact_store.py \
  src/loom_cli/rollout/preflight_orchestrator.py \
  src/loom_cli/rollout/operator/deep_preflight_authority.py \
  src/loom_cli/rollout/operator/installed_deep_preflight.py \
  src/loom_cli/rollout/operator/installed_manifest_ownership.py \
  src/loom_cli/rollout/operator/installed_lifecycle_capacity.py
```

Expected: all pytest tests pass, Ruff reports no findings, and mypy reports no
issues.

- [ ] **Step 6: Prove no runtime enumeration remains**

Run:

```bash
rg -n "load_exact|scandir\(self\.root\)|len\(entries\) > 256" \
  src/loom_cli/rollout tests/loom_cli/rollout
```

Expected: no production occurrence; any historical wording in an explicit
regression test must not call an enumeration API.

- [ ] **Step 7: Commit documentation and response evidence**

```bash
git add src/loom_cli/rollout/operator/broker.py \
  tests/loom_cli/rollout/operator/test_broker.py \
  docs/architecture/staging-rollout.md \
  docs/architecture/staging-rollout-preflight.md
git commit -m "docs(rollout): document exact artifact authority"
```

### Task 5: Review, PR, CI, and protected rollout

**Files:**

- Review all branch changes against
  `docs/architecture/preflight-artifact-lifecycle.md`.
- Do not add or modify `docs/superpowers/**`.

**Interfaces:**

- Produces one reviewable PR to `dev` containing the design and exact-lookup
  correction.
- Produces one merged immutable candidate SHA for installed rollout authority.

- [ ] **Step 1: Run verification-before-completion checks**

Run the Task 4 full regression suite, `git diff --check origin/dev...HEAD`, and
`git status --short`. Record exact outputs before claiming readiness.

- [ ] **Step 2: Self-review every changed file**

Check reference exactness, error normalization, old-request compatibility,
argument propagation, candidate/tree/epoch validation order, secret-safe
output, and absence of broad enumeration or deletion. Correct findings through
new failing tests before code changes.

- [ ] **Step 3: Push the branch and open a PR to `dev`**

Use title:

```text
fix(rollout): load preflight artifacts by evidence digest
```

The PR body must include the 257-entry reproduction, root cause, direct lookup
invariant, maintenance interface change, focused verification, and explicit
statement that the PR performs no artifact deletion.

- [ ] **Step 4: Address review through the review workflow**

Read every review comment, verify it against code and tests, and implement only
technically valid changes through TDD. Reply with evidence and resolve threads
only after the update is pushed.

- [ ] **Step 5: Wait for all required CI and squash merge**

Do not merge with pending, skipped-required, cancelled, or failing checks.
After all required checks pass, squash merge through GitHub and verify the
merged SHA is an ancestor of fresh `origin/dev`.

- [ ] **Step 6: Converge and validate installed authority**

Install the exact merged SHA through the root-owned staging rollout authority.
Verify host authority and protected preflight, run a dry run, clean failed
request `req-7cc91e8b83b54d62` through its request-bound cleanup command after
reviewing the cleanup plan, then submit one fresh rollout. Require migration
`0109`, final smoke, and rollout summary evidence to pass before continuing to
the separate retention PR.
