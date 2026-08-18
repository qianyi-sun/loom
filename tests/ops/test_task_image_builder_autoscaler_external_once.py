from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ops" / "task_image_builder_autoscaler_external_once.py"


@pytest.fixture
def module():
    name = "task_image_builder_autoscaler_external_once_test"
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    spec.loader.exec_module(loaded)
    try:
        yield loaded
    finally:
        sys.modules.pop(name, None)


def _args(module: Any, tmp_path: Path, *extra: str):
    return module._parser().parse_args(
        [
            "--environment",
            "staging",
            "--pool-name",
            "task-image-builder-gb10",
            "--profile",
            str(ROOT / "deploy/environment-state/staging.toml"),
            "--image-tag",
            "staging-abc1234",
            "--env-config-version",
            "staging-abc1234",
            "--git-sha",
            "abc1234def5678901234567890123456789012ab",
            "--expected-slurm-cluster-name",
            "trt-gb10",
            "--expected-slurm-controller-host",
            "gx10-01c7",
            "--kubeconfig",
            str(tmp_path / "kubeconfig"),
            "--global-execution-witness-json",
            str(tmp_path / "global-execution-witness.json"),
            "--manager-public-key",
            str(tmp_path / "global-execution-manager.pub"),
            "--expected-manager-public-key-sha256",
            "a" * 64,
            *extra,
        ]
    )


def _registry_config(tmp_path: Path, *, registry: str = "registry.example") -> Path:
    directory = tmp_path / f"docker-{registry.replace('.', '-')}"
    directory.mkdir(mode=0o700)
    config = directory / "config.json"
    config.write_text(
        '{"auths": {"' + registry + '": {"auth": "dGVzdA=="}}}\n',
        encoding="utf-8",
    )
    config.chmod(0o600)
    return directory


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


def test_committed_disabled_policy_cannot_reconcile(
    module: Any,
    tmp_path: Path,
) -> None:
    with pytest.raises(module.TaskImageBuilderPolicyError, match="disabled"):
        module._load_enabled_builder_config(_args(module, tmp_path))


def test_enabled_policy_maps_to_exclusive_runtime_config(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "builder.env"
    repo_dir = tmp_path / "repo"
    registry_docker_config_dir = tmp_path / "registry-docker"
    policy = {
        "environment": "staging",
        "pool_name": "task-image-builder-gb10",
        "enabled": True,
        "activation_blockers": [],
        "slurm_cluster_id": "gb10",
        "cpu_arch": "arm64",
        "allowed_nodes": ["trt-gb10-1"],
        "env_file": str(env_file),
        "env_template_file": str(tmp_path / "trial-worker.env"),
        "builder_token_file": str(tmp_path / "builder-token"),
        "repo_dir": str(repo_dir),
        "registry_docker_config_dir": str(registry_docker_config_dir),
        "partition": "gb10",
        "time_limit": "04:00:00",
        "requested_cpus": 20,
        "requested_memory_mib": 115000,
        "requested_concurrency": 1,
        "max_jobs": 1,
        "pending_job_cap": 1,
        "idle_exit_after_seconds": 120,
        "exclusive": True,
        "sbatch_path": "sbatch",
        "squeue_path": "squeue",
        "sacct_path": "sacct",
        "scancel_path": "scancel",
        "command_timeout_seconds": 20.0,
        "slurm_account": "loom-staging",
        "slurm_qos": "loom-task-image-builder",
        "slurm_reservation": "loom-builder-exclusive",
        "job_output_dir": str(tmp_path / "output"),
    }
    monkeypatch.setattr(
        module,
        "load_environment_state_profile",
        lambda *_args, **_kwargs: SimpleNamespace(task_image_builder_policies=[policy]),
    )

    config = module._load_enabled_builder_config(_args(module, tmp_path))

    assert config.pool_name == "task-image-builder-gb10"
    assert config.cpu_arch == "arm64"
    assert config.exclusive is True
    assert config.requested_concurrency == 1
    assert config.env_file == str(env_file)
    assert config.env_template_file == str(tmp_path / "trial-worker.env")
    assert config.builder_token_file == str(tmp_path / "builder-token")
    assert config.repo_dir == str(repo_dir)
    assert config.registry_docker_config_dir == str(registry_docker_config_dir)


def test_invalid_global_execution_witness_enters_drain_only_mode(
    module: Any,
    tmp_path: Path,
) -> None:
    assert module._global_execution_scale_up_allowed(
        _args(module, tmp_path),
        slurm_cluster_id="gb10",
    ) is False


async def test_builder_token_validation_requires_dedicated_scope(
    module: Any,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "builder.env"
    env_file.write_text(
        "LOOM_WORKER_TOKEN=ordinary-token\n"
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO=registry.example/loom\n",
        encoding="utf-8",
    )

    class _Session:
        async def execute(self, _query: object) -> Any:
            return SimpleNamespace(one_or_none=lambda: ("worker", ["worker:claim"], None, None))

    with pytest.raises(module.TaskImageBuilderPolicyError, match="task-image:build"):
        await module._validate_builder_credentials(
            _Session(),
            env_file=str(env_file),
            registry_docker_config_dir=str(_registry_config(tmp_path)),
        )


async def test_builder_token_validation_rejects_additional_scopes(
    module: Any,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "builder.env"
    env_file.write_text(
        "LOOM_WORKER_TOKEN=overprivileged-token\n"
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO=registry.example/loom\n",
        encoding="utf-8",
    )

    class _Session:
        async def execute(self, _query: object) -> Any:
            return SimpleNamespace(
                one_or_none=lambda: (
                    "worker",
                    ["task-image:build", "worker:claim"],
                    None,
                    None,
                )
            )

    with pytest.raises(module.TaskImageBuilderPolicyError, match="task-image:build"):
        await module._validate_builder_credentials(
            _Session(),
            env_file=str(env_file),
            registry_docker_config_dir=str(_registry_config(tmp_path)),
        )


async def test_builder_credentials_require_expected_registry_auth_entry(
    module: Any,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "builder.env"
    env_file.write_text(
        "LOOM_WORKER_TOKEN=builder-token\n"
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO=registry.example/loom\n",
        encoding="utf-8",
    )

    class _Session:
        async def execute(self, _query: object) -> Any:
            return SimpleNamespace(
                one_or_none=lambda: ("worker", ["task-image:build"], None, None)
            )

    with pytest.raises(module.TaskImageBuilderPolicyError, match="registry credentials"):
        await module._validate_builder_credentials(
            _Session(),
            env_file=str(env_file),
            registry_docker_config_dir=str(
                _registry_config(tmp_path, registry="other-registry.example")
            ),
        )


async def test_builder_credentials_reject_username_only_registry_entry(
    module: Any,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "builder.env"
    env_file.write_text(
        "LOOM_WORKER_TOKEN=builder-token\n"
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO=registry.example/loom\n",
        encoding="utf-8",
    )
    registry_config_dir = _registry_config(tmp_path)
    registry_config_file = registry_config_dir / "config.json"
    registry_config_file.write_text(
        '{"auths": {"registry.example": {"username": "builder"}}}\n',
        encoding="utf-8",
    )
    registry_config_file.chmod(0o600)

    class _Session:
        async def execute(self, _query: object) -> Any:
            return SimpleNamespace(
                one_or_none=lambda: ("worker", ["task-image:build"], None, None)
            )

    with pytest.raises(module.TaskImageBuilderPolicyError, match="registry credentials"):
        await module._validate_builder_credentials(
            _Session(),
            env_file=str(env_file),
            registry_docker_config_dir=str(registry_config_dir),
        )


async def test_drain_only_reconcile_bypasses_builder_credentials(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Transaction:
        async def __aenter__(self) -> None:
            events.append("begin")

        async def __aexit__(self, *_args: Any) -> None:
            events.append("commit")

    class _Session:
        def begin(self) -> _Transaction:
            return _Transaction()

    class _Runner:
        async def validate_builder_request(
            self,
            *,
            node: str,
            config: object,
        ) -> None:
            raise AssertionError("drain must not validate a builder request")

    async def validate(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("drain must not depend on builder credentials")

    def validate_runtime(_config: object) -> None:
        raise AssertionError("drain must not depend on builder runtime files")

    async def reconcile(*_args: Any, **_kwargs: Any) -> str:
        events.append("reconcile")
        return "result"

    monkeypatch.setattr(module, "_validate_builder_credentials", validate)
    monkeypatch.setattr(
        module,
        "_validate_builder_runtime_files",
        validate_runtime,
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "reconcile_task_image_builder_autoscaler_once",
        reconcile,
    )

    result = await module._reconcile_with_credentials(
        _Session(),
        config=SimpleNamespace(
            env_file="/secure/builder.env",
            registry_docker_config_dir="/secure/registry-docker",
        ),
        runner=_Runner(),
        scale_up_allowed=False,
    )

    assert result == "result"
    assert events == ["begin", "reconcile", "commit"]


async def test_scale_up_reconcile_validates_credentials_inside_transaction(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Transaction:
        async def __aenter__(self) -> None:
            events.append("begin")

        async def __aexit__(self, *_args: Any) -> None:
            events.append("commit")

    class _Session:
        def begin(self) -> _Transaction:
            return _Transaction()

    class _Runner:
        async def validate_builder_request(
            self,
            *,
            node: str,
            config: object,
        ) -> None:
            assert node == "trt-gb10-1"
            events.append("slurm-test")

    async def validate(*_args: Any, **_kwargs: Any) -> None:
        events.append("validate")

    def validate_runtime(_config: object) -> None:
        events.append("runtime")

    def materialize(_config: object) -> None:
        events.append("materialize")

    async def reconcile(*_args: Any, **_kwargs: Any) -> str:
        events.append("reconcile")
        return "result"

    monkeypatch.setattr(module, "_validate_builder_credentials", validate)
    monkeypatch.setattr(
        module,
        "_validate_builder_runtime_files",
        validate_runtime,
        raising=False,
    )
    monkeypatch.setattr(module, "_materialize_builder_env", materialize)
    monkeypatch.setattr(
        module,
        "reconcile_task_image_builder_autoscaler_once",
        reconcile,
    )

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

    assert result == "result"
    assert events == [
        "begin",
        "materialize",
        "runtime",
        "validate",
        "slurm-test",
        "reconcile",
        "commit",
    ]


async def test_scale_up_reconcile_requires_activation_runner(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Transaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_args: Any) -> None:
            return None

    class _Session:
        def begin(self) -> _Transaction:
            return _Transaction()

    async def validate(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(module, "_materialize_builder_env", lambda _config: None)
    monkeypatch.setattr(module, "_validate_builder_runtime_files", lambda _config: None)
    monkeypatch.setattr(module, "_validate_builder_credentials", validate)

    with pytest.raises(
        module.TaskImageBuilderPolicyError,
        match="activation runner is unavailable",
    ):
        await module._reconcile_with_credentials(
            _Session(),
            config=SimpleNamespace(
                allowed_nodes=("trt-gb10-1",),
                env_file="/secure/builder.env",
                registry_docker_config_dir="/secure/registry-docker",
            ),
            runner=None,
            scale_up_allowed=True,
        )


def test_builder_env_is_atomically_derived_from_candidate_trial_env(
    module: Any,
    tmp_path: Path,
) -> None:
    template = tmp_path / "staging-gb10-worker.env"
    template.write_text(
        "LOOM_WORKER_CONTROL_PLANE_URL=http://control.example\n"
        "LOOM_WORKER_TOKEN=ordinary-worker-token\n"
        "LOOM_WORKER_POOL_NAME=gb10\n"
        "LOOM_WORKER_MAX_CONCURRENT=10\n"
        "LOOM_WORKER_MINIO_ACCESS_KEY=access\n"
        "LOOM_WORKER_MINIO_SECRET_KEY=secret\n"
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO=registry.example/loom\n",
        encoding="utf-8",
    )
    template.chmod(0o600)
    token_file = tmp_path / "task-image-builder-token"
    token_file.write_text("dedicated-builder-token\n", encoding="ascii")
    token_file.chmod(0o600)
    target = tmp_path / "staging-task-image-builder-gb10.env"

    record = module._materialize_builder_env(
        SimpleNamespace(
            env_file=str(target),
            env_template_file=str(template),
            builder_token_file=str(token_file),
            pool_name="task-image-builder-gb10",
            requested_concurrency=1,
        )
    )

    rendered = target.read_text(encoding="utf-8")
    assert "LOOM_WORKER_TOKEN=dedicated-builder-token\n" in rendered
    assert "LOOM_WORKER_POOL_NAME=task-image-builder-gb10\n" in rendered
    assert "LOOM_WORKER_MAX_CONCURRENT=1\n" in rendered
    assert "ordinary-worker-token" not in rendered
    assert template.read_text(encoding="utf-8").startswith(
        "LOOM_WORKER_CONTROL_PLANE_URL=http://control.example\n"
    )
    assert target.stat().st_mode & 0o777 == 0o600
    assert record["env_sha256"] == module.hashlib.sha256(target.read_bytes()).hexdigest()


def test_builder_env_refuses_hardlinked_token_without_replacing_target(
    module: Any,
    tmp_path: Path,
) -> None:
    template = tmp_path / "trial-worker.env"
    template.write_text(
        "LOOM_WORKER_TOKEN=ordinary\n"
        "LOOM_WORKER_POOL_NAME=oldlab\n"
        "LOOM_WORKER_MAX_CONCURRENT=6\n",
        encoding="utf-8",
    )
    template.chmod(0o600)
    token_source = tmp_path / "token-source"
    token_source.write_text("dedicated\n", encoding="ascii")
    token_source.chmod(0o600)
    token_file = tmp_path / "builder-token"
    token_file.hardlink_to(token_source)
    target = tmp_path / "builder.env"
    target.write_text("preserve-me\n", encoding="utf-8")
    target.chmod(0o600)

    with pytest.raises(module.TaskImageBuilderPolicyError, match="metadata is unsafe"):
        module._materialize_builder_env(
            SimpleNamespace(
                env_file=str(target),
                env_template_file=str(template),
                builder_token_file=str(token_file),
                pool_name="task-image-builder-oldlab",
                requested_concurrency=1,
            )
        )

    assert target.read_text(encoding="utf-8") == "preserve-me\n"
