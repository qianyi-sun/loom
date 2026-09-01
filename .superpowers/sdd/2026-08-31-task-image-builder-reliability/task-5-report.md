# Task 5 report: brokered controller-local credential convergence

## Implementation

- Added a non-mutating `--check` mode to the external Slurm autoscaler
  credential publisher. It rejects symlinks, non-regular files, unexpected
  ownership/mode/link count/size, checks the dedicated database Secret and
  witness ConfigMap, and requires `pods/exec` authority to be exactly `no`.
- Added fixed local credential publication and observation. Its immutable
  evidence contains only controller identity, fixed-path SHA-256, ownership,
  mode, size, two required read checks, and `pods/exec` denial.
- Added canonical GB10 `observe_credential` and `publish_credential`
  operations. Their request contains only candidate SHA/tree, operation, and
  schema version; their response contains canonical non-secret evidence.
- Added journal-ready credential components before external supervisor units,
  including convergence observations and evidence digests. Controller order is
  GB10 credential, OLDLAB credential, GB10 units, OLDLAB units.
- Bound the installed final gate to the GB10 credential adapter and OLDLAB
  fixed local credential transport, with fixed identities. It now checks both
  effective UID and GID.
- Updated only the Phase 1 credential runbook to use protected publication and
  post-apply non-mutating readback. Phase 2 was not changed.

## RED/GREEN evidence

Initial focused protected-executor/final-gate RED run:

```console
PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q \
  tests/loom_cli/rollout/operator/test_protected_apply_executor.py \
  tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py
# 4 failed, 33 passed
```

The failures were the expected missing credential transport constructor
arguments and absent installed-gate credential map. The same command was green
after composition: `37 passed`.

Additional RED/GREEN checks verified that effective GID drift is rejected and
that the GB10 credential adapter wraps the GB10 controller transport rather
than reusing the unit transport. The GB10 focused suite also caught and
validated recovery from an intermediate adapter placement regression.

## Final verification

```console
PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_component.py \
  tests/loom_cli/rollout/operator/test_protected_gb10_external_supervisor_transport.py \
  tests/ops/test_gb10_external_supervisor_broker.py \
  tests/loom_cli/rollout/operator/test_protected_apply_executor.py \
  tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py
# 126 passed in 17.11s

bash -n deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh
# passed

uv run --frozen --no-sync ruff check \
  scripts/ops/gb10_external_supervisor_broker.py \
  src/loom_cli/rollout/operator/installed_final_gate_executor.py \
  src/loom_cli/rollout/operator/protected_apply_executor.py \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_component.py \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py \
  src/loom_cli/rollout/operator/protected_gb10_external_supervisor_transport.py \
  tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py \
  tests/loom_cli/rollout/operator/test_protected_apply_executor.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_component.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py \
  tests/loom_cli/rollout/operator/test_protected_gb10_external_supervisor_transport.py \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/ops/test_gb10_external_supervisor_broker.py
# All checks passed

uv run --frozen --no-sync mypy \
  scripts/ops/gb10_external_supervisor_broker.py \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_component.py \
  src/loom_cli/rollout/operator/protected_gb10_external_supervisor_transport.py \
  src/loom_cli/rollout/operator/protected_apply_executor.py \
  src/loom_cli/rollout/operator/installed_final_gate_executor.py
# Success: no issues found in 6 source files

git diff --check
# passed
```

The source audit confirmed the broker allowlists both credential operations as
the common canonical request shape, the request encoder rejects every other
field, and credential responses use only
`ExternalSupervisorCredentialEvidence.to_dict()`. The focused tests reject
token, certificate, kubeconfig, path, and arbitrary-command wire fields.

## Files changed

- `deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh`
- `docs/runbooks/task-image-builder-phase1-site-convergence.md`
- `scripts/ops/gb10_external_supervisor_broker.py`
- `src/loom_cli/rollout/operator/installed_final_gate_executor.py`
- `src/loom_cli/rollout/operator/protected_apply_executor.py`
- `src/loom_cli/rollout/operator/protected_external_supervisor_credential_component.py`
- `src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py`
- `src/loom_cli/rollout/operator/protected_gb10_external_supervisor_transport.py`
- Task 5 focused tests under `tests/loom_cli/rollout/operator/` and `tests/ops/`

## Self-review and concern

No functional blockers were found. The local transport accepts injectable paths
only for focused tests; production construction binds
`/var/lib/loom-staging-rollout/kubeconfig` to
`/var/lib/loom-staging-rollout/external-supervisor.kubeconfig`. GB10 continues
to use its pre-existing verified root-owned candidate flow and drops helper
execution to UID/GID `995:2007`; OLDLAB now explicitly binds both effective UID
and GID.

## Review fix round 1/5

### Findings addressed

1. The publisher now requires `pods/exec` denial in both `loom-staging` and
   `loom-dev`, then verifies the same denial with
   `kubectl auth can-i --all-namespaces create pods/exec`. The transport emits
   `pods_exec_denied=True` only after this expanded, fail-closed publisher
   check succeeds.
2. The production credential command runner no longer uses
   `capture_output=True`. Both streams are sent to `subprocess.DEVNULL` as a
   bounded sink, and it returns empty byte streams to preserve the transport
   interface without retaining child output in memory.
3. The Phase 1 rollback no longer accepts or installs a captured kubeconfig.
   It relies on the reviewed rollback candidate's protected
   credential-before-unit convergence, then performs a non-mutating `--check`
   plus file metadata, database Secret, witness ConfigMap, and all-namespace
   `pods/exec` readback.

### RED/GREEN evidence

RED, after adding the three focused regression tests and before the fixes:

```console
PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py \
  tests/ops/test_task_image_builder_deployment_contract.py
# 3 failed, 21 passed in 0.69s
```

The failures were respectively the missing staging/all-namespace authority
proof, missing bounded `stdout`/`stderr` sinks, and manual rollback credential
installation.

GREEN for the same command after the fixes:

```console
# 24 passed in 0.66s
```

Final focused Task 5 and runbook verification:

```console
PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_component.py \
  tests/loom_cli/rollout/operator/test_protected_gb10_external_supervisor_transport.py \
  tests/ops/test_gb10_external_supervisor_broker.py \
  tests/loom_cli/rollout/operator/test_protected_apply_executor.py \
  tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py \
  tests/ops/test_task_image_builder_deployment_contract.py
# 139 passed in 19.77s

bash -n deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh
# passed

uv run --frozen --no-sync ruff check \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/ops/test_task_image_builder_deployment_contract.py
# All checks passed

uv run --frozen --no-sync mypy \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py
# Success: no issues found in 1 source file

git diff --check
# passed
```

### Self-review

The namespace and all-namespace checks are both exact `no` comparisons; every
non-`no`, command failure, or malformed result stops publication. DEVNULL
prevents unbounded child-output allocation and leaves failures secret-free.
The rollback runbook contains no manual credential install, ownership change,
or permission repair and requires the same narrow authority readback as the
forward path. No unresolved concerns were found.

## Review fix round 2/5

### Findings addressed

1. Replaced the incomplete empty-namespace `--all-namespaces` access review
   with a complete current-cluster proof. The credential can now only `list`
   namespace metadata through a dedicated ClusterRole/ClusterRoleBinding; the
   publisher rejects an unavailable or empty listing, validates each returned
   `namespace/<name>` identity, and requires exact `no` for `pods/exec` in
   every enumerated namespace. A third namespace that answers `yes` now fails
   non-mutating credential validation.
2. Rewrote rollback credential readback as two explicit controller-local
   blocks—one for `gx10-01c7`, one for `TRT-EAI-OLDLAB-1`. Each performs only
   fixed-path non-mutating validation, metadata, database-Secret, witness
   ConfigMap, and complete namespace-by-namespace `pods/exec` checks. No
   manual credential mutation was reintroduced. The prior DEVNULL runner fix
   remains unchanged.

### RED/GREEN evidence

RED, after adding the namespace-listing, third-namespace, manifest, and
controller-local runbook regressions and before the fixes:

```console
PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/ops/test_task_image_builder_deployment_contract.py
# 5 failed, 17 passed in 0.24s
```

The failures proved that the ClusterRole audit authority and complete namespace
enumeration were absent, the third namespace was not queried, and rollback
still lacked explicit local GB10/OLDLAB readback blocks.

GREEN for the same command after the fixes:

```console
# 22 passed in 0.21s
```

Final focused Task 5, credential transport, authority, and runbook verification:

```console
PYTHONDONTWRITEBYTECODE=1 uv run --frozen --no-sync pytest -p no:cacheprovider -q \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_transport.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_credential_component.py \
  tests/loom_cli/rollout/operator/test_protected_gb10_external_supervisor_transport.py \
  tests/ops/test_gb10_external_supervisor_broker.py \
  tests/loom_cli/rollout/operator/test_protected_apply_executor.py \
  tests/loom_cli/rollout/operator/test_installed_final_gate_executor.py \
  tests/ops/test_task_image_builder_deployment_contract.py
# 141 passed in 20.25s

bash -n deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh
# passed

uv run --frozen --no-sync ruff check \
  tests/ops/test_external_slurm_autoscaler_kubernetes_authority.py \
  tests/ops/test_task_image_builder_deployment_contract.py
# All checks passed

uv run --frozen --no-sync mypy \
  src/loom_cli/rollout/operator/protected_external_supervisor_credential_transport.py
# Success: no issues found in 1 source file

git diff --check
# passed
```

### Self-review

The complete proof no longer relies on `--all-namespaces`; it asks the API for
the current namespace set and checks every individual namespace. The new RBAC
grant exposes only namespace names, not namespace contents, Secrets, or any
subresource. The rollback blocks are explicitly local to both controllers and
retain only non-mutating credential operations. No unresolved concerns were
found.
