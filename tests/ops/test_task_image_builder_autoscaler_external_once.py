from __future__ import annotations

import importlib.util
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
    env_file.write_text("LOOM_WORKER_TOKEN=builder-token\n", encoding="utf-8")
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    registry_docker_config_dir = tmp_path / "registry-docker"
    registry_docker_config_dir.mkdir(mode=0o700)
    registry_config = registry_docker_config_dir / "config.json"
    registry_config.write_text('{"auths": {"registry.example": {}}}\n', encoding="utf-8")
    registry_config.chmod(0o600)
    policy = {
        "environment": "staging",
        "pool_name": "task-image-builder-gb10",
        "enabled": True,
        "activation_blockers": [],
        "slurm_cluster_id": "gb10",
        "cpu_arch": "arm64",
        "allowed_nodes": ["trt-gb10-1"],
        "env_file": str(env_file),
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


async def test_reconcile_validates_credentials_inside_single_transaction(
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

    async def validate(*_args: Any, **_kwargs: Any) -> None:
        events.append("validate")

    async def reconcile(*_args: Any, **_kwargs: Any) -> str:
        events.append("reconcile")
        return "result"

    monkeypatch.setattr(module, "_validate_builder_credentials", validate)
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
        runner=object(),
        scale_up_allowed=False,
    )

    assert result == "result"
    assert events == ["begin", "validate", "reconcile", "commit"]
