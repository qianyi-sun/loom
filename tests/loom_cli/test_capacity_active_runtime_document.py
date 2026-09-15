"""Remote activation rendering must not inspect the operator host's filesystem."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

import loom_capacity_executor.runtime as runtime
from loom_capacity_executor.config import PoolExecutorConfig
from loom_cli.capacity_control_plane import (
    render_capacity_pool_executor_active_config,
    render_capacity_pool_executor_active_service_environment,
)
from tests.loom_cli.test_capacity_control_plane import _active_render_fixture


def test_remote_activation_renders_without_inspecting_local_paths(tmp_path, monkeypatch):
    profile, _, artifact = _active_render_fixture(tmp_path)
    expected_config = render_capacity_pool_executor_active_config(profile, artifact.pool_id, artifact)
    expected_environment = render_capacity_pool_executor_active_service_environment(profile, artifact.pool_id, artifact)
    wire = artifact.model_dump_json()
    document_type = getattr(runtime, "ActivationRuntimeDocumentV2", None)
    assert document_type is not None, "remote activation needs a portable document contract"

    def no_local_stat(*args, **kwargs):
        raise AssertionError("remote activation inspected local filesystem")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "lstat", no_local_stat)
        document = document_type.model_validate_json(wire)
        assert not isinstance(document, runtime.ActivationRuntimeArtifactV2)
        assert render_capacity_pool_executor_active_config(profile, artifact.pool_id, document) == expected_config
        assert render_capacity_pool_executor_active_service_environment(profile, artifact.pool_id, document) == expected_environment

    config_path = tmp_path / "active-config.json"
    config_path.write_text(expected_config)
    config_path.chmod(0o600)
    config = PoolExecutorConfig.from_files(config_path)
    with pytest.raises(runtime.RuntimeAssemblyError, match="activation runtime artifact is invalid"):
        runtime.build_executable_runtime(
            config, document, manager_client=object(), current_context=document.execution,
        )

    # Target-side loading continues to require real private runtime paths.
    Path(artifact.handoff_directory).chmod(0o755)
    with pytest.raises(ValidationError, match="0700"):
        runtime.ActivationRuntimeArtifactV2.model_validate_json(wire)


@pytest.mark.parametrize("field", ["handoff_directory", "state_directory", "journal_file", "admission_directory"])
@pytest.mark.parametrize("path", ["relative", "/", "/unsafe/../escape", "/unsafe//double", "/nul\0path"])
def test_portable_activation_document_rejects_unsafe_paths(tmp_path, field, path):
    _, _, artifact = _active_render_fixture(tmp_path)
    document_type = getattr(runtime, "ActivationRuntimeDocumentV2", None)
    assert document_type is not None
    payload = json.loads(artifact.model_dump_json())
    payload[field] = path
    with pytest.raises(ValidationError, match=r"canonical.*absolute"):
        document_type.model_validate_json(json.dumps(payload))
