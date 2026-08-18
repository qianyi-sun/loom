# Task-image builder Phase 0 rollout validation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the task-image builder's protected-rollout bootstrap cycle by
making rehearsal validation side-effect-free, keeping materialization and
runtime checks ahead of live reconciliation, and removing hosted GitHub staging
deployment authority.

**Architecture:** The exact candidate profile remains the single policy source.
`--validate-only` will stop after policy, local Slurm-authority, and pure batch
request rendering checks, before any database, credential, repository, or
release-specific environment access. The protected live oneshot remains the
host-local boundary and executes materialization, post-materialization file and
credential checks, a non-submitting Slurm `sbatch --test-only` request, and only
then reconciliation; the existing protected supervisor transport enables its
timer only after that oneshot succeeds. Generic GitHub deployment retains
development and production but explicitly rejects staging, whose sole mutation
entrypoint remains the installed host-local rollout authority.

**Tech Stack:** Python 3.11, asyncio, SQLAlchemy, Slurm CLI, systemd user units,
Bash, GitHub Actions YAML, pytest, Ruff, strict mypy.

## Global constraints

- Implement only design Phase 0, "rollout validation correction".
- Do not enable a task-image builder, change a policy blocker, submit a builder
  allocation, or mutate a Slurm QoS, reservation, account, association, or node.
- Do not add rootless BuildKit, node-guard, publication, scheduler-fence, shadow,
  or legacy-retirement behavior from Phases 1–5.
- Preserve the existing exclusive builder backend as inert/current behavior; a
  future phase replaces it after native acceptance.
- Rehearsal validation must not require the release-specific builder env,
  worker repository, builder token, registry Docker config, or live production
  database contents.
- Materialization, post-materialization validation, and reconciliation must
  retain the exact order: write the derived env; validate files and dedicated
  credentials; execute only a non-submitting Slurm request; then reconcile.
- Shared staging mutation remains owned by the root-installed host-local
  `loom-staging-rollout` authority and its protected lock.
- Hosted CI may validate committed profiles and release artifacts but must not
  expose a generic staging mutation job or script path.
- Do not create or push any path under `docs/superpowers/**`.
- Do not report task/run `4139e767` unblocked; incident acceptance belongs to
  Phase 4 after both native rootless paths pass acceptance.
- Use the locked `uv==0.11.26` environment and make no dependency changes.
- Integrate only through a PR to `dev`, passing required CI before squash merge.

---

## File map

- `scripts/ops/task_image_builder_autoscaler_external_once.py` owns the builder
  supervisor's four ordered concerns: candidate-policy validation, protected
  env materialization, post-materialization validation, and live reconcile.
- `src/loom_control_plane/task_image_builder_autoscaler.py` owns both the real
  `sbatch` request and a derived `--test-only` request that cannot submit a job.
- `tests/ops/test_task_image_builder_autoscaler_external_once.py` proves the
  rehearsal/live boundary and the exact materialize/validate/activate order.
- `tests/unit/test_task_image_builder_autoscaler.py` proves the Slurm test
  request derives from the live request without `--parsable` submission mode.
- `.github/workflows/deploy-environment.yml` exposes only generic development
  and production deployments.
- `scripts/validate_environment_isolation.py` continues validating all three
  environment profiles while requiring the hosted workflow to omit staging.
- `scripts/ops/deploy_environment.sh` rejects staging even when invoked outside
  GitHub Actions, preventing a hidden second mutation entrypoint.
- `tests/ops/test_environment_isolation.py` and
  `tests/ops/test_deploy_environment_release_manifest.py` execute the hosted
  authority contract.
- `docs/architecture/staging-rollout-preflight.md` records the corrected four
  phases and their evidence boundary.
- `docs/runbooks/operator-runbook.md` and `docs/runbooks/staging-launch.md`
  identify the installed staging authority and hosted workflow limits.
- `tests/ops/test_release_runbook.py` protects the documented operator path.

### Task 1: Make builder rehearsal validation pure

**Files:**

- Modify: `tests/ops/test_task_image_builder_autoscaler_external_once.py`
- Modify: `scripts/ops/task_image_builder_autoscaler_external_once.py:421-504`

**Interfaces:**

- Consumes:
  `build_task_image_builder_sbatch_request(config: TaskImageBuilderPoolConfig, *, node: str) -> SbatchRequest`.
- Produces:
  `_rehearsal_validation_evidence(config: TaskImageBuilderPoolConfig) -> dict[str, object]`.
- Preserves: inherited `--validate-only` CLI spelling used by
  `ExternalSupervisorIdentity.validation_argv()`.

- [ ] **Step 1: Add the failing missing-runtime rehearsal test**

Add imports for `asyncio` and `json`, then add a real `_main_async` test. The
configured env, repository, token, and Docker config paths are deliberately not
created; any access is the production regression this test catches.

```python
def _enabled_config(module: Any, tmp_path: Path):
    return module.TaskImageBuilderPoolConfig(
        environment="staging",
        pool_name="task-image-builder-gb10",
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        allowed_nodes=("trt-gb10-1",),
        env_file=str(tmp_path / "future-builder.env"),
        env_template_file=str(tmp_path / "future-worker.env"),
        builder_token_file=str(tmp_path / "future-builder-token"),
        repo_dir=str(tmp_path / "future-repo"),
        registry_docker_config_dir=str(tmp_path / "future-docker-config"),
        partition="gb10",
        time_limit="04:00:00",
        requested_cpus=20,
        requested_memory_mib=115000,
        requested_concurrency=1,
        max_jobs=1,
        pending_job_cap=1,
        idle_exit_after_seconds=120,
        sbatch_path="/usr/bin/sbatch",
        squeue_path="/usr/bin/squeue",
        sacct_path="/usr/bin/sacct",
        scancel_path="/usr/bin/scancel",
        command_timeout_seconds=20.0,
        exclusive=True,
        slurm_account="loom-staging",
        slurm_qos="loom-task-image-builder",
        slurm_reservation="loom-task-image-builder",
        job_output_dir=str(tmp_path / "future-output"),
    )


def test_validate_only_succeeds_before_runtime_materialization(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _enabled_config(module, tmp_path)
    monkeypatch.setattr(module, "_load_enabled_builder_config", lambda _args: config)
    monkeypatch.setattr(
        module.transport,
        "_validate_local_slurm_authority",
        lambda _args: SimpleNamespace(cluster_name="trt-gb10"),
    )

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("rehearsal touched protected runtime state")

    monkeypatch.setattr(module, "_materialize_builder_env", forbidden)
    monkeypatch.setattr(module, "_validate_builder_runtime_files", forbidden)
    monkeypatch.setattr(module, "_validate_builder_credentials", forbidden)
    monkeypatch.setattr(module.transport, "_load_cp_db_url", forbidden)

    asyncio.run(module._main_async(_args(module, tmp_path, "--validate-only")))

    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "rehearsal-validate-only"
    assert payload["pool_name"] == "task-image-builder-gb10"
    assert payload["request_nodes"] == ["trt-gb10-1"]
    assert len(payload["request_set_sha256"]) == 64
    assert not Path(config.env_file).exists()
```

- [ ] **Step 2: Run the test and observe the expected failure**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_autoscaler_external_once.py::test_validate_only_succeeds_before_runtime_materialization
```

Expected: FAIL because current `_main_async()` reaches
`_reconcile_with_credentials()`, which calls `_materialize_builder_env()`.

- [ ] **Step 3: Add pure rendered-request evidence**

Import `build_task_image_builder_sbatch_request` beside the existing builder
types. Add the following helper above `_reconcile_with_credentials()`:

```python
def _rehearsal_validation_evidence(
    config: TaskImageBuilderPoolConfig,
) -> dict[str, object]:
    requests = {
        node: build_task_image_builder_sbatch_request(config, node=node)
        for node in config.allowed_nodes
    }
    request_digests = {
        node: hashlib.sha256(
            json.dumps(
                {"args": list(request.args), "stdin": request.stdin},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        for node, request in requests.items()
    }
    request_set_sha256 = hashlib.sha256(
        json.dumps(
            request_digests,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "request_nodes": sorted(request_digests),
        "request_set_sha256": request_set_sha256,
    }
```

This validates the intended submission shape from candidate inputs only and
publishes hashes rather than paths, scripts, credentials, or environment
contents.

- [ ] **Step 4: Return from rehearsal before live inputs**

In `_main_async()`, immediately after checking the local Slurm cluster against
the candidate policy, add:

```python
    if args.validate_only:
        evidence = _rehearsal_validation_evidence(config)
        print(
            json.dumps(
                {
                    "mode": "rehearsal-validate-only",
                    "pool_name": config.pool_name,
                    "cpu_arch": config.cpu_arch,
                    "exclusive": config.exclusive,
                    "requested_concurrency": config.requested_concurrency,
                    **evidence,
                },
                sort_keys=True,
            )
        )
        return
```

Delete the old `args.validate_only` branch inside the database session and
change `scale_up_allowed` back to only the global-execution witness result:

```python
    scale_up_allowed = _global_execution_scale_up_allowed(
        args,
        slurm_cluster_id=config.slurm_cluster_id,
    )
```

- [ ] **Step 5: Run the focused builder supervisor tests**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_autoscaler_external_once.py \
  tests/loom_cli/rollout/test_external_supervisor_readiness.py
```

Expected: PASS. The existing rehearsal argv test must still prove the command
ends in `--validate-only` and carries the isolated namespace/kubeconfig.

- [ ] **Step 6: Commit the pure rehearsal boundary**

```bash
git add \
  scripts/ops/task_image_builder_autoscaler_external_once.py \
  tests/ops/test_task_image_builder_autoscaler_external_once.py
git commit -m "fix(rollout): make builder rehearsal side effect free"
```

### Task 2: Prove protected materialization before activation

**Files:**

- Modify: `tests/unit/test_task_image_builder_autoscaler.py`
- Modify: `src/loom_control_plane/task_image_builder_autoscaler.py:130-225`
- Modify: `tests/ops/test_task_image_builder_autoscaler_external_once.py`
- Modify: `scripts/ops/task_image_builder_autoscaler_external_once.py:421-444`
- Modify: `docs/architecture/staging-rollout-preflight.md:417-430,485-510`

**Interfaces:**

- Consumes:
  `build_task_image_builder_sbatch_request(config, *, node) -> SbatchRequest`.
- Produces:
  `build_task_image_builder_sbatch_test_request(config, *, node) -> SbatchRequest`.
- Produces:
  `SubprocessTaskImageBuilderSlurmRunner.validate_builder_request(*, node: str, config: TaskImageBuilderPoolConfig) -> None`.
- Preserves:
  `reconcile_task_image_builder_autoscaler_once(..., scale_up_allowed: bool)` as
  the only operation allowed to submit/cancel builder jobs.

- [ ] **Step 1: Add failing pure Slurm test-request tests**

In `tests/unit/test_task_image_builder_autoscaler.py`, import
`build_task_image_builder_sbatch_test_request` and add:

```python
def test_builder_sbatch_test_request_cannot_submit() -> None:
    live = build_task_image_builder_sbatch_request(_config(), node="gb10-1")
    tested = build_task_image_builder_sbatch_test_request(_config(), node="gb10-1")

    assert tested.args[0] == live.args[0]
    assert tested.args[1] == "--test-only"
    assert "--parsable" not in tested.args
    assert tuple(item for item in live.args[1:] if item != "--parsable") == tested.args[2:]
    assert tested.stdin == live.stdin
```

Add an async runner test by monkeypatching the module-level `_run_command`:

```python
async def test_subprocess_runner_validates_without_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], str]] = []

    async def run(
        args: tuple[str, ...],
        *,
        stdin: str | None = None,
        timeout: float,
    ) -> SimpleNamespace:
        calls.append((args, stdin or ""))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(
        "loom_control_plane.task_image_builder_autoscaler._run_command",
        run,
    )
    runner = SubprocessTaskImageBuilderSlurmRunner(_config())

    await runner.validate_builder_request(node="gb10-1", config=_config())

    assert len(calls) == 1
    assert calls[0][0][1] == "--test-only"
    assert "--parsable" not in calls[0][0]
```

- [ ] **Step 2: Run the tests and observe missing interfaces**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_task_image_builder_autoscaler.py::test_builder_sbatch_test_request_cannot_submit \
  tests/unit/test_task_image_builder_autoscaler.py::test_subprocess_runner_validates_without_submission
```

Expected: collection FAIL because the test-request builder and runner method do
not exist.

- [ ] **Step 3: Derive a non-submitting request from the live request**

Add immediately after `build_task_image_builder_sbatch_request()`:

```python
def build_task_image_builder_sbatch_test_request(
    config: TaskImageBuilderPoolConfig,
    *,
    node: str,
) -> SbatchRequest:
    request = build_task_image_builder_sbatch_request(config, node=node)
    return SbatchRequest(
        args=(
            request.args[0],
            "--test-only",
            *(item for item in request.args[1:] if item != "--parsable"),
        ),
        stdin=request.stdin,
    )
```

Add to `SubprocessTaskImageBuilderSlurmRunner` before `submit_builder()`:

```python
    async def validate_builder_request(
        self,
        *,
        node: str,
        config: TaskImageBuilderPoolConfig,
    ) -> None:
        request = build_task_image_builder_sbatch_test_request(config, node=node)
        await _run_command(
            request.args,
            stdin=request.stdin,
            timeout=config.command_timeout_seconds,
        )
```

Do not add this method to `TaskImageBuilderSlurmRunner`: reconciliation still
depends only on submit/cancel behavior; preactivation validation is a concrete
installed-runtime concern.

- [ ] **Step 4: Add the failing activation-order test**

Extend
`test_scale_up_reconcile_validates_credentials_inside_transaction()` so its
runner is a real test double with a bounded validation method:

```python
    class _Runner:
        async def validate_builder_request(
            self,
            *,
            node: str,
            config: object,
        ) -> None:
            assert node == "trt-gb10-1"
            events.append("slurm-test")

    result = await module._reconcile_with_credentials(
        _Session(),
        config=SimpleNamespace(
            allowed_nodes=("trt-gb10-1",),
            env_file="/secure/builder.env",
            registry_docker_config_dir="/secure/registry-docker",
        ),
        runner=_Runner(),
        scale_up_allowed=True,
    )

    assert events == [
        "begin",
        "materialize",
        "runtime",
        "validate",
        "slurm-test",
        "reconcile",
        "commit",
    ]
```

Keep the existing drain-only test and assert it never invokes the new method.

- [ ] **Step 5: Run the order test and observe the missing Slurm phase**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_autoscaler_external_once.py::test_scale_up_reconcile_validates_credentials_inside_transaction \
  tests/ops/test_task_image_builder_autoscaler_external_once.py::test_drain_only_reconcile_bypasses_builder_credentials
```

Expected: the scale-up test FAILS because `slurm-test` is absent; drain-only
continues to pass.

- [ ] **Step 6: Insert post-materialization Slurm validation before reconcile**

Inside `_reconcile_with_credentials()`, after file and credential validation
and before `reconcile_task_image_builder_autoscaler_once()`, require the concrete
runner and validate every candidate-authorized node:

```python
        if scale_up_allowed:
            _materialize_builder_env(config)
            _validate_builder_runtime_files(config)
            await _validate_builder_credentials(
                session,
                env_file=config.env_file,
                registry_docker_config_dir=config.registry_docker_config_dir,
            )
            if runner is None:
                raise TaskImageBuilderPolicyError(
                    "task-image builder activation runner is unavailable"
                )
            for node in config.allowed_nodes:
                await runner.validate_builder_request(node=node, config=config)
```

This order runs inside the existing database transaction and protected oneshot.
The existing `FixedExternalSupervisorTransport.apply()` publishes exact unit
bytes, starts the oneshot, requires service success, and only then enables and
starts the timer. No new mutation authority is introduced.

- [ ] **Step 7: Document the four distinct phases**

In `docs/architecture/staging-rollout-preflight.md`, add a compact subsection
under Tier 1/Tier 3 explaining:

1. candidate-static rehearsal reads policy and hashes rendered requests only;
2. the protected host-local oneshot atomically derives the release env;
3. the same oneshot validates exact files, dedicated token scope, registry
   authorization, and `sbatch --test-only` for every allowed node;
4. reconciliation is called only after those checks, and the protected
   transport enables the timer only after oneshot success.

State that a failed post-materialization check produces protected rollout
evidence and no builder submission, while drain remains possible without live
credentials.

- [ ] **Step 8: Run all focused builder and rollout-unit tests**

Run:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_task_image_builder_autoscaler.py \
  tests/ops/test_task_image_builder_autoscaler_external_once.py \
  tests/loom_cli/test_environment_state.py \
  tests/loom_cli/rollout/test_external_supervisor_readiness.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_component.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_transition.py
```

Expected: PASS. In particular, existing protected-transport tests must still
prove service success precedes timer enablement and failed activation is
compensated.

- [ ] **Step 9: Commit the protected activation ordering**

```bash
git add \
  src/loom_control_plane/task_image_builder_autoscaler.py \
  scripts/ops/task_image_builder_autoscaler_external_once.py \
  tests/unit/test_task_image_builder_autoscaler.py \
  tests/ops/test_task_image_builder_autoscaler_external_once.py \
  docs/architecture/staging-rollout-preflight.md
git commit -m "fix(rollout): validate builder materialization before activation"
```

### Task 3: Remove generic hosted staging mutation authority

**Files:**

- Modify: `tests/ops/test_environment_isolation.py:145-181`
- Modify: `tests/ops/test_deploy_environment_release_manifest.py:61-75`
- Modify: `.github/workflows/deploy-environment.yml:8-116`
- Modify: `scripts/validate_environment_isolation.py:18-51,351-397`
- Modify: `scripts/ops/deploy_environment.sh:4-20`
- Modify: `docs/runbooks/operator-runbook.md:15-67`
- Modify: `docs/runbooks/staging-launch.md:1-25`
- Modify: `tests/ops/test_release_runbook.py:25-46`

**Interfaces:**

- Consumes: all three `deploy/environments/*.toml` profile identities.
- Produces: hosted deployment workflow environments exactly
  `{"development", "production"}`.
- Produces: a Bash refusal for `LOOM_DEPLOY_ENVIRONMENT=staging` with exit code
  `2` before any credential decode, package install, or cluster command.
- Preserves: installed `loom-staging-rollout --env staging ...` as the only
  staging mutation interface.

- [ ] **Step 1: Replace workflow tests with the desired authority contract**

In `tests/ops/test_environment_isolation.py`, replace
`test_deploy_workflow_keeps_production_secrets_on_main_only()` with:

```python
def test_hosted_deploy_workflow_omits_protected_staging_authority() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/deploy-environment.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict)
    dispatch = triggers["workflow_dispatch"]["inputs"]["environment"]
    jobs = workflow["jobs"]

    assert dispatch["options"] == ["development", "production"]
    assert set(jobs) == {"deploy-development", "deploy-production"}
    assert "deploy-staging" not in jobs

    dev_job = jobs["deploy-development"]
    prod_job = jobs["deploy-production"]
    assert dev_job["environment"]["name"] == "development"
    assert "refs/heads/dev" in dev_job["if"]
    assert prod_job["environment"]["name"] == "production"
    assert "refs/heads/main" in prod_job["if"]
```

In `tests/ops/test_deploy_environment_release_manifest.py`, change the rollout
artifact loop to development/production and add a behavioral script test:

```python
def test_deploy_script_rejects_protected_staging_before_credentials() -> None:
    completed = subprocess.run(
        ["bash", "scripts/ops/deploy_environment.sh"],
        cwd=REPO_ROOT,
        env={
            "PATH": os.environ["PATH"],
            "LOOM_DEPLOY_ENVIRONMENT": "staging",
            "LOOM_IMAGE_TAG": "staging-refused",
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert "installed host-local loom-staging-rollout authority" in completed.stderr
```

- [ ] **Step 2: Run the hosted-authority tests and observe current staging access**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_environment_isolation.py::test_hosted_deploy_workflow_omits_protected_staging_authority \
  tests/ops/test_deploy_environment_release_manifest.py::test_deploy_script_rejects_protected_staging_before_credentials
```

Expected: FAIL because the workflow still contains `deploy-staging` and the
script currently asks for GitHub secrets instead of refusing staging.

- [ ] **Step 3: Remove staging from the hosted workflow**

In `.github/workflows/deploy-environment.yml`:

- remove `staging` from the dispatch `environment.options`;
- delete the complete `deploy-staging` job;
- leave development and production job permissions, branch predicates,
  environment-scoped secrets, evidence upload, and production release-gate
  verification unchanged.

- [ ] **Step 4: Teach the static validator that staging is host-local**

Add:

```python
HOSTED_DEPLOY_ENVIRONMENTS = ("development", "production")
PROTECTED_HOST_LOCAL_ENVIRONMENTS = ("staging",)
```

Change `validate_workflow()` to loop only over
`HOSTED_DEPLOY_ENVIRONMENTS`, then reject any protected job:

```python
    for env_name in PROTECTED_HOST_LOCAL_ENVIRONMENTS:
        if f"deploy-{env_name}" in jobs:
            errors.append(
                f"deploy-{env_name}: protected environment must use host-local rollout authority"
            )
```

Keep `EXPECTED_ENVIRONMENTS`, profile isolation, route isolation, and safe
secret-reference validation unchanged for all three environments.

- [ ] **Step 5: Make the shell entrypoint independently refuse staging**

Move the Kubernetes/cluster/deploy-token `:${VAR:?}` checks below environment
selection and change the case to:

```bash
case "${LOOM_DEPLOY_ENVIRONMENT}" in
  development|production)
    ;;
  staging)
    echo "Staging deploys require the installed host-local loom-staging-rollout authority." >&2
    exit 2
    ;;
  *)
    echo "Unsupported LOOM_DEPLOY_ENVIRONMENT=${LOOM_DEPLOY_ENVIRONMENT}" >&2
    exit 2
    ;;
esac

: "${LOOM_KUBECONFIG_B64:?environment-scoped GitHub secret is required}"
: "${LOOM_CLUSTER_CONFIG_B64:?environment-scoped GitHub secret is required}"
: "${LOOM_DEPLOY_TOKEN:?environment-scoped GitHub secret is required}"
```

Remove the now-dead staging lock fallback. Production must still require
`LOOM_ROLLOUT_LOCK_DIR`; development may retain its existing optional lock.

- [ ] **Step 6: Update current operator documentation**

In `docs/runbooks/operator-runbook.md`, state that
`.github/workflows/deploy-environment.yml` owns generic development/production
deployment only and rejects staging. In the Shared staging section, state that
`loom-staging-rollout --env staging start` is the only mutation path and that
hosted workflow dispatch cannot substitute for its secrets, shared lock,
backup, rehearsal, or final-gate evidence.

In `docs/runbooks/staging-launch.md`, add the same boundary before the candidate
identity commands: this checklist consumes an installed-authority rollout and
does not dispatch the generic workflow.

Extend `test_current_release_docs_cover_executable_validation_and_promotion()`
with behavior-oriented fragments:

```python
    assert "development and production only" in operator
    assert "does not deploy staging" in staging
    assert "loom-staging-rollout --env staging start" in operator
```

- [ ] **Step 7: Run the hosted workflow, validator, and docs tests**

Run:

```bash
uv run --no-sync pytest -q \
  tests/ops/test_environment_isolation.py \
  tests/ops/test_deploy_environment_release_manifest.py \
  tests/ops/test_release_promotion_gate.py \
  tests/ops/test_release_runbook.py \
  tests/unit/test_repo_governance_templates.py \
  tests/ops/test_uv_lock_contract.py
```

Expected: PASS. The validator report must still list and validate development,
staging, and production profiles while accepting only two hosted deploy jobs.

- [ ] **Step 8: Commit the authority correction**

```bash
git add \
  .github/workflows/deploy-environment.yml \
  scripts/validate_environment_isolation.py \
  scripts/ops/deploy_environment.sh \
  tests/ops/test_environment_isolation.py \
  tests/ops/test_deploy_environment_release_manifest.py \
  docs/runbooks/operator-runbook.md \
  docs/runbooks/staging-launch.md \
  tests/ops/test_release_runbook.py
git commit -m "fix(deploy): reserve staging mutation for host authority"
```

### Task 4: Verify Phase 0 as one reviewable increment

**Files:**

- Verify: all files changed in Tasks 1–3
- Verify: `archive/docs/architecture/2026-08-18-dynamic-task-image-builder-design.md`

**Interfaces:**

- Consumes: the complete Phase 0 branch.
- Produces: a PR-ready branch with no activation or operational mutations.

- [ ] **Step 1: Run formatting and lint checks**

```bash
uv run --no-sync ruff check \
  src tests scripts/ops/task_image_builder_autoscaler_external_once.py \
  scripts/validate_environment_isolation.py
```

Expected: PASS with no warnings.

- [ ] **Step 2: Run strict typing**

```bash
uv run --no-sync mypy
```

Expected: PASS for every configured source file.

- [ ] **Step 3: Run the complete non-system test suites used by repository checks**

```bash
uv run --no-sync pytest \
  tests/unit tests/contract tests/property tests/loom_cli tests/ops tests/integration
```

Expected: PASS, with only repository-declared skips. Do not run live staging,
Slurm mutation, Docker integration, or system suites as part of Phase 0.

- [ ] **Step 4: Re-run the exact Phase 0 regression set**

```bash
uv run --no-sync pytest -q \
  tests/ops/test_task_image_builder_autoscaler_external_once.py \
  tests/unit/test_task_image_builder_autoscaler.py \
  tests/loom_cli/rollout/test_external_supervisor_readiness.py \
  tests/loom_cli/rollout/operator/test_protected_external_supervisor_component.py \
  tests/ops/test_environment_isolation.py \
  tests/ops/test_deploy_environment_release_manifest.py \
  tests/ops/test_release_runbook.py
```

Expected: PASS. Confirm the missing-runtime rehearsal test creates none of its
future artifact paths and the staging shell test exits before secrets.

- [ ] **Step 5: Audit the diff for forbidden scope**

```bash
git diff --check origin/dev...HEAD
git diff --stat origin/dev...HEAD
git diff origin/dev...HEAD -- \
  deploy/environment-state/staging.toml \
  deploy/slurm \
  src/loom_worker \
  src/loom_control_plane/task_image_materializations.py
git status --short
```

Expected: no whitespace errors; no changes to activation policy, Slurm
convergence, worker builder backend, or materialization state; only the plan
and intended Phase 0 files are present.

- [ ] **Step 6: Commit any verification-only correction separately**

If verification exposes a real Phase 0 defect, reproduce it with a failing test,
apply only the corresponding fix, re-run the affected and full gates, then:

```bash
git add <exact-files-from-the-failing-test-cycle>
git commit -m "fix(rollout): close phase zero validation gap"
```

If no defect appears, create no empty commit.

- [ ] **Step 7: Prepare the PR without activating production**

Push `feat/task-image-builder-phase0-rollout-validation`, open a PR to `dev`,
and include:

- the root cause: rehearsal used the live materialization/credential path;
- the fixed order and the non-submitting Slurm proof;
- removal of hosted staging mutation authority;
- local Ruff, mypy, pytest evidence;
- explicit exclusions: no builder enablement, reservation mutation, rootless
  executor, production rollout, or incident rerun.

Enable squash auto-merge only through the repository's normal CI-controlled
path. Monitor every required check on the current head, correct failures with
test-first commits, and merge only after all protected gates succeed.
