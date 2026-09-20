from unittest.mock import AsyncMock

import pytest

from loom.execution_architecture import execution_cpu_arch
from loom.models.capabilities import Capabilities
from loom.models.task import TaskConfig
from loom_service.task_config_validation import split_valid_task_configs
from tests.unit.test_task_image_materialization import _task_config


@pytest.mark.parametrize("architecture", ["x86_64", "any"])
def test_supported_execution_is_x86(architecture):
    assert execution_cpu_arch(architecture) == "x86_64"


def test_historical_arm_models_remain_readable():
    task = _task_config(cpu_arch="arm64", dockerfile="Dockerfile")
    assert TaskConfig.model_validate(task.model_dump()).environment.cpu_arch == "arm64"
    caps = Capabilities(os="linux", cpu_arch="arm64", gpu_vendor="none",
        network_policies=frozenset({"public"}), dynamic_network_policy=False,
        mounted_fs=False, resource_modes=frozenset({"auto"}))
    assert Capabilities.model_validate(caps.model_dump()).cpu_arch == "arm64"


@pytest.mark.asyncio
async def test_submission_rejects_historical_arm_without_invalidating_stored_config():
    task = _task_config(cpu_arch="arm64", dockerfile="Dockerfile")
    result = AsyncMock()
    result.all = lambda: [(task.task.id, task.model_dump(mode="json"))]
    session = AsyncMock()
    session.execute.return_value = result
    valid, invalid = await split_valid_task_configs(session, [task.task.id])
    assert valid == []
    assert len(invalid) == 1
    assert "x86_64 only" in invalid[0].detail
    assert task.environment.cpu_arch == "arm64"


def test_explicit_x86_worker_cannot_reinterpret_an_arm_task():
    from loom.driver.task_image import task_image_tag
    task = _task_config(cpu_arch="arm64", dockerfile="Dockerfile")
    with pytest.raises(ValueError, match="x86_64 only"):
        task_image_tag(task, task_checksum="a" * 64, cpu_arch="x86_64")


@pytest.mark.parametrize("architecture", ["x86_64", "any", "arm64"])
def test_local_publish_validation_enforces_execution_architecture(tmp_path, architecture):
    import tomli_w

    from loom_cli.local_benchmark_validate import (
        LocalBenchmarkValidationError,
        _validate_task_toml,
    )

    task = _task_config(cpu_arch=architecture, dockerfile="Dockerfile")
    path = tmp_path / "task.toml"
    path.write_text(tomli_w.dumps(task.model_dump(mode="json", exclude_none=True)))
    if architecture == "arm64":
        with pytest.raises(LocalBenchmarkValidationError, match="x86_64 only"):
            _validate_task_toml(path)
    else:
        _validate_task_toml(path)


@pytest.mark.asyncio
async def test_builder_cannot_claim_historical_arm_work():
    from loom_control_plane.task_image_materializations import claim_task_image_materialization

    session = AsyncMock()
    with pytest.raises(ValueError, match="x86_64 only"):
        await claim_task_image_materialization(session, builder_id="arm-builder", cpu_arch="arm64")
    session.execute.assert_not_called()


def test_taskset_materialization_rejects_arm_before_upload(tmp_path):
    from unittest.mock import MagicMock
    from uuid import uuid4

    from loom.taskset.materialize import materialize_task_set
    from tests.unit.test_taskset_quota_gc import _inline_manifest_model

    manifest = _inline_manifest_model()
    manifest.task_template["environment"]["cpu_arch"] = "arm64"
    client = MagicMock()
    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team-1/arm", owning_team_id="team-1",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None,
        minio_client=client, artifacts_bucket="artifacts", upstream_cache_root=tmp_path,
    )
    assert result.status == "failed"
    assert "x86_64 only" in str(result.error_summary)
    client.put_object.assert_not_called()
