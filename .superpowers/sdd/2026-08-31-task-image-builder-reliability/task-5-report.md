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
