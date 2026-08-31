# Task-Image Builder Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore durable GB10 and OLDLAB builder supervision without pod-bound exec authority and prevent builders from claiming work below their storage floor.

**Architecture:** A capacity-manager sidecar publishes short-lived signed exports to one stable ConfigMap; external supervisors read the object with a dedicated least-privilege kubeconfig and retain all existing cryptographic checks. The protected rollout publishes and proves that credential locally on both controllers through fixed operations before it can activate supervisor units; no credential bytes cross SSH. Builder startup removes only provably managed stopped containers and images, then enforces storage admission before claiming work, while the autoscaler cools down after failed allocations.

**Tech Stack:** Python 3.12, asyncio, urllib/SSL, Kubernetes YAML/RBAC, systemd supervisor arguments, SQLAlchemy/PostgreSQL, Docker SDK, Slurm, pytest.

**Spec:** `docs/architecture/task-image-builder-reliability.md`

## Global Constraints

- Do not create or modify `docs/superpowers` content.
- Keep Phase 2 rootless provider disabled and inert.
- Preserve fail-closed behavior for every unavailable or invalid witness source.
- Never grant `pods/exec` to an external supervisor.
- Never expose `/var/lib/loom-staging-rollout/kubeconfig` to a runtime supervisor.
- Never delete unlabelled Docker resources automatically.
- Never claim a materialization before storage admission succeeds.

---

### Task 1: Stable witness publisher and Kubernetes ownership

**Files:**
- Create: `src/loom_capacity_manager/global_execution_witness_publisher.py`
- Modify: `src/loom_cli/capacity_control_plane.py`
- Test: `tests/unit/test_capacity_manager_global_execution_witness_publisher.py`
- Test: `tests/loom_cli/test_capacity_control_plane.py`

**Interfaces:**
- Consumes: `build_current_global_execution_witness_export(...)` and the existing protected database/signing-key inputs.
- Produces: `publish_global_execution_witnesses_once(settings) -> None`, a ten-second publisher loop, ConfigMap `loom-global-execution-witness-v1`, and publisher-only Kubernetes RBAC.

- [ ] **Step 1: Write failing publisher tests**

Assert that one publication builds `gb10.json` and `oldlab.json`, sends a bounded merge patch only to the exact namespace/name, never logs the token or payload after failure, and rejects unsafe API coordinates.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider -q tests/unit/test_capacity_manager_global_execution_witness_publisher.py`

Expected: collection/import failure because the publisher module does not exist.

- [ ] **Step 3: Implement the minimal publisher**

Use a frozen settings model, bounded projected-token/CA reads, `urllib.request` with a five-second timeout, an ASCII JSON merge patch, and an async loop that publishes every ten seconds. Generate both exports before issuing the single patch.

- [ ] **Step 4: Write failing renderer tests**

Require the stable ConfigMap, publisher ServiceAccount, exact-name Role and RoleBinding, explicit token projection mounted only into the publisher sidecar, `automountServiceAccountToken: false`, and publisher environment pointing at protected DB/key files.

- [ ] **Step 5: Run renderer tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider -q tests/loom_cli/test_capacity_control_plane.py`

Expected: missing ConfigMap, sidecar, and RBAC documents.

- [ ] **Step 6: Render the publisher resources and pass focused tests**

Modify the capacity-control-plane renderer without changing manager API behavior. Run both Task 1 test files until green.

### Task 2: Stable read transport and least-privilege controller credentials

**Files:**
- Modify: `scripts/ops/worker_pool_autoscaler_external_once.py`
- Modify: `deploy/k8s/external-slurm-autoscaler-authority.yaml`
- Modify: `deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh`
- Modify: `deploy/environment-state/staging.toml`
- Modify: `src/loom_cli/rollout/external_supervisor_readiness.py`
- Test: `tests/ops/test_worker_pool_autoscaler_external_once.py`
- Test: `tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py`
- Test: `tests/ops/test_task_image_builder_deployment_contract.py`
- Test: `tests/loom_cli/test_environment_state.py`
- Test: `tests/loom_cli/rollout/test_external_supervisor_readiness.py`

**Interfaces:**
- Consumes: ConfigMap keys `<pool_id>.json` and `parse_global_execution_witness_export(...)`.
- Produces: `--global-execution-witness-config-map`, `--global-execution-witness-namespace`, and `--global-execution-witness-kubeconfig` supervisor arguments.

- [ ] **Step 1: Replace exec expectations with failing ConfigMap-reader tests**

Assert the exact shell-free command:

```text
kubectl --kubeconfig <dedicated> --request-timeout=10s -n loom-dev get configmap loom-global-execution-witness-v1 -o json
```

Require bounded output, exact data-key selection, malformed/missing-key rejection, signature validation, and rejection of mixed file/ConfigMap sources.

- [ ] **Step 2: Run reader tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider -q tests/ops/test_worker_pool_autoscaler_external_once.py`

Expected: parser and command assertions fail because only deployment exec exists.

- [ ] **Step 3: Implement the ConfigMap reader**

Keep the legacy exec parser source for one transition release, but make sources mutually exclusive and configure no active profile to use exec. Parse bounded ConfigMap JSON and feed only the selected ASCII export to the existing verifier.

- [ ] **Step 4: Write failing RBAC and profile tests**

Require exact ConfigMap `get` authority in `loom-dev`, no `pods/exec`, and the dedicated kubeconfig path for both DB and witness arguments on all four staging supervisors.

- [ ] **Step 5: Run RBAC/profile/readiness tests and verify RED**

Run the four Task 2 contract suites and confirm failures identify the missing authority and old paths.

- [ ] **Step 6: Update manifests, publisher, profiles, and readiness checks**

Apply the cross-namespace Role/RoleBinding, install the dedicated kubeconfig with owner-only mode, switch staging arguments, and make protected readiness reject active exec sources or rollout-kubeconfig reuse.

- [ ] **Step 7: Run all Task 2 tests until green**

Run the five listed test files with `-p no:cacheprovider` and no bytecode writes.

### Task 3: Hard storage admission and owned stopped-container cleanup

**Files:**
- Modify: `src/loom_worker/trial_cache.py`
- Modify: `src/loom_worker/task_image_builder.py`
- Test: `tests/unit/test_trial_cache.py`
- Test: `tests/unit/test_task_image_builder.py`

**Interfaces:**
- Produces: immutable `ManagedImageCleanupResult` with Docker root, actual probe path, final/required free bytes, probe status, and cleanup error count.
- Consumes: `LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB` as a hard builder admission floor and an optional explicit storage-probe path for containerized builders.

- [ ] **Step 1: Write failing cleanup tests**

Require removal of stopped containers whose container or referenced image has a managed Loom label. Require retention of running containers, unlabelled containers, and all volumes. Assert the structured final probe result.

- [ ] **Step 2: Run cleanup tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider -q tests/unit/test_trial_cache.py`

Expected: missing result type and stopped-container cleanup behavior.

- [ ] **Step 3: Implement minimal owned cleanup and structured evidence**

Remove eligible stopped containers without volumes, retain warning-only cleanup errors, perform existing managed-image eviction, and return the final disk probe.

- [ ] **Step 4: Write failing builder admission tests**

Assert that unavailable/below-floor results raise a safe fatal storage error before `HttpControlPlaneClient` construction or claim, while an admitted result preserves the existing claim loop.

- [ ] **Step 5: Run builder tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider -q tests/unit/test_task_image_builder.py`

Expected: the builder continues past an unsafe result.

- [ ] **Step 6: Enforce admission and pass Task 3 tests**

Add a safe fatal error and non-secret top-level failure message. Run both Task 3 suites until green.

### Task 4: Failed-allocation cooldown

**Files:**
- Modify: `src/loom_control_plane/task_image_builder_autoscaler.py`
- Modify: `src/loom_cli/environment_state.py`
- Modify: `scripts/ops/task_image_builder_autoscaler_external_once.py`
- Modify: `deploy/environment-state/staging.toml`
- Test: `tests/unit/test_task_image_builder_autoscaler.py`
- Test: `tests/integration/test_task_image_builder_autoscaler.py`
- Test: `tests/ops/test_task_image_builder_autoscaler_external_once.py`
- Test: `tests/loom_cli/test_environment_state.py`

**Interfaces:**
- Adds: `failure_backoff_seconds: int` to `TaskImageBuilderPoolConfig`, defaulting to 300 in environment-state profiles.
- Adds: `failure_backoff_active: bool` to reconciliation output.

- [ ] **Step 1: Write a failing integration test for recent failure suppression**

Insert queued demand and a failed builder job finished inside the cooldown. Assert zero submission and `failure_backoff_active is True`; move the failure outside the cooldown and assert submission resumes.

- [ ] **Step 2: Run autoscaler tests and verify RED**

Run the Task 4 unit and integration suites. Expected: missing config/result fields and immediate resubmission.

- [ ] **Step 3: Implement cooldown and configuration normalization**

Query only the latest failed job for the exact environment/pool, suppress target jobs until its bounded cooldown expires, and preserve drain behavior when the witness forbids capacity.

- [ ] **Step 4: Add policy/loader tests and pass Task 4 suites**

Require a positive bounded cooldown in TOML normalization and runtime configuration, then run all Task 4 suites until green.

### Task 5: Brokered controller-local credential convergence

**Files:**
- Modify: `deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh`
- Create: `src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py`
- Create: `src/loom_cli/rollout/operator/protected_external_supervisor_credential_component.py`
- Modify: `src/loom_cli/rollout/operator/protected_gb10_external_supervisor_transport.py`
- Modify: `scripts/ops/gb10_external_supervisor_broker.py`
- Modify: `src/loom_cli/rollout/operator/protected_apply_executor.py`
- Modify: `src/loom_cli/rollout/operator/installed_final_gate_executor.py`
- Test: `tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py`
- Create: `tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py`
- Modify: `tests/loom_cli/rollout/operator/test_protected_gb10_external_supervisor_transport.py`
- Modify: `tests/loom_cli/rollout/operator/test_protected_apply_executor.py`
- Modify: `tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py`

**Interfaces:**
- Produces: immutable `ExternalSupervisorCredentialEvidence` containing only the controller identity, fixed-path SHA-256, UID, GID, mode, size, database-Secret readability, witness-ConfigMap readability, and `pods/exec` denial.
- Produces: `ProtectedExternalSupervisorCredentialTransport.observe() -> ExternalSupervisorCredentialEvidence | None` and `publish() -> ExternalSupervisorCredentialEvidence`.
- Adds: fixed GB10 operations `observe_credential` and `publish_credential`; their canonical requests contain only schema version, candidate SHA/tree, and operation.
- Produces: `ProtectedExternalSupervisorCredentialComponent`, with component IDs `external-supervisor-credential-gb10` and `external-supervisor-credential-oldlab` ordered before both existing supervisor-unit components.

- [ ] **Step 1: Write failing publisher check-mode and local-transport tests**

Require `--check /var/lib/loom-staging-rollout/external-supervisor.kubeconfig` to perform no mutation, reject symlinks/non-regular files/wrong owner or mode, prove the dedicated database Secret and witness ConfigMap are readable, and require `auth can-i create pods/exec` to return exactly `no`. Require observation to return no token, CA, kubeconfig content, or command output.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py`

Expected: failure because check mode, the evidence type, and the fixed local transport do not exist.

- [ ] **Step 3: Implement fixed local publication and observation**

Add a non-mutating check mode to the existing publisher. Implement the local transport with fixed candidate script, source kubeconfig, output path, service UID/GID, bounded subprocess output, clean environment, and exact post-publication observation. Treat an absent path as repairable; reject every present unsafe or ineffective credential as drift rather than overwriting it implicitly.

- [ ] **Step 4: Write failing GB10 wire and broker tests**

Require canonical `observe_credential` and `publish_credential` requests to contain only `candidate_sha`, `candidate_tree`, `operation`, and `schema_version`. Require the forced broker to dispatch only the candidate helper as UID/GID `995:2007`, and assert that token-, certificate-, kubeconfig-, path-, and arbitrary-command fields are rejected.

- [ ] **Step 5: Run the GB10 transport tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q tests/loom_cli/rollout/operator/test_protected_gb10_external_supervisor_transport.py tests/ops/test_gb10_external_supervisor_broker.py`

Expected: request-operation and response decoding failures because the credential operations are not allowlisted.

- [ ] **Step 6: Implement controller-local GB10 operations**

Extend the existing typed request parser and candidate helper only. The remote helper constructs the fixed local credential transport from its verified candidate prefix, reads the source kubeconfig locally, and returns canonical evidence. Never place credential content in the request, response, exception, journal, or process arguments.

- [ ] **Step 7: Write failing protected-order tests**

Require both credential components to classify exact or repairable before apply, and require the protected journal order to be GB10 credential, OLDLAB credential, GB10 supervisor units, then OLDLAB supervisor units. Prove a second credential failure leaves all supervisor `apply` calls at zero and that a successfully published first narrow credential is retained without rollback to the broad source credential.

- [ ] **Step 8: Run the protected executor/final-gate tests and verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q tests/loom_cli/rollout/operator/test_protected_apply_executor.py tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py`

Expected: missing component IDs, transports, and ordering assertions.

- [ ] **Step 9: Implement fail-closed credential-before-unit composition**

Add the credential components to protected apply and convergence, inject fixed local/GB10 transports from the installed final gate, include their non-secret evidence digests in terminal/convergence evidence, and keep supervisor activation closed unless both components are exact. Do not compensate a narrow credential by restoring or exposing the rollout kubeconfig.

- [ ] **Step 10: Pass all Task 5 tests and audit the wire**

Run every Task 5 test, `bash -n deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh`, Ruff, mypy, and a source scan proving the GB10 credential request/response fields cannot contain secret material.

### Task 6: Documentation, verification, commit, and protected delivery

**Files:**
- Modify: `docs/architecture/task-image-materialization.md`
- Modify: `docs/runbooks/task-image-builder-phase1-site-convergence.md`
- Modify: any generated config/package snapshots required by tests

**Interfaces:**
- Produces: exact operator rollout/removal/readback commands and live acceptance criteria.

- [ ] **Step 1: Update architecture and convergence documentation**

Document the stable signed-object transport, dedicated credential path, hard pre-claim storage floor, owned cleanup boundary, cooldown, transitional exec removal, and ordered rollback-safe convergence.

- [ ] **Step 2: Run focused verification**

Run every test named in Tasks 1-5, plus formatting, Ruff, and mypy targets selected by `scripts/plan_ci_validations.py` for the changed paths.

- [ ] **Step 3: Run broader affected suites**

Run capacity-control-plane rendering, environment-state/rollout, task-image builder, and config snapshot suites. Regenerate only repository-owned generated files using `loom config-codegen` when the schema changes.

- [ ] **Step 4: Self-review the complete diff**

Check for secret exposure, broad RBAC, pod-name coupling, unsafe Docker ownership inference, claim-before-admission races, missing cooldown scoping, stale docs, and any `docs/superpowers` changes. Correct issues and rerun affected tests.

- [ ] **Step 5: Commit reviewable changes**

Create focused commits for witness publication/transport and storage admission/cooldown. Do not push directly to `dev`.

- [ ] **Step 6: Open a PR and pass protected CI/review**

Push the feature branch, open a PR against `dev`, respond to review with evidence, and merge only after required checks pass.

- [ ] **Step 7: Converge staging in the specified order**

Publish capacity-manager manifests, prove fresh ConfigMap exports, install dedicated kubeconfigs, apply staging supervisors, perform owned GB10 cleanup, remove temporary exec RBAC, and verify empty-queue reconciliations on both architectures.
