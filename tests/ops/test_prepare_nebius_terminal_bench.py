from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path

import pytest
import yaml
from scripts.ops.prepare_nebius_terminal_bench import SOURCE, prepare

from loom.models.task import TaskConfig
from loom.models.taskset import UserTaskSetManifest
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import automatic_service_execution_rejections

IMAGE = "registry.example/loom-terminal-bench@sha256:" + "a" * 64


@pytest.mark.parametrize("image", [None, IMAGE])
def test_prepares_uploadable_task_with_original_assertions_and_no_oracle(
    tmp_path: Path, image: str | None,
) -> None:
    result = prepare(image=image, output=tmp_path)
    manifest = UserTaskSetManifest.model_validate(
        yaml.safe_load((tmp_path / "manifest.yaml").read_text())
    )
    assert manifest.source.locator == "bundle.tar.gz"
    with tarfile.open(tmp_path / manifest.source.locator) as archive:
        prefix = "tasks/file-archive-manifest/"
        expected = {
            prefix + name
            for name in (
                "task.toml",
                "instruction.md",
                "tests/test_outputs.py",
                "verifier/run.sh",
                "source-provenance.json",
            )
        }
        if image is None:
            expected.add(prefix + "environment/Dockerfile")
            dockerfile = archive.extractfile(prefix + "environment/Dockerfile")
            assert dockerfile is not None
            assert dockerfile.read() == (SOURCE / "Dockerfile").read_bytes()
        assert set(archive.getnames()) == expected
        for name in ("instruction.md", "tests/test_outputs.py"):
            member = archive.extractfile(prefix + name)
            assert member is not None
            assert member.read() == (SOURCE / "original" / name).read_bytes()
        member = archive.extractfile(prefix + "task.toml")
        assert member is not None
        raw = tomllib.loads(member.read().decode())
        config = TaskConfig.model_validate(raw)
        assert config.environment.docker_image == image
        if image is None:
            assert "docker_image" not in raw["environment"]
            assert str(config.environment.dockerfile) == "environment/Dockerfile"
            assert str(config.environment.docker_build_context) == "environment"
            assert manifest.metadata.name.endswith("-dockerfile")
            # The ordinary materializer supplies the immutable input binding;
            # every environment/compiler constraint already fits this profile.
            assert automatic_service_execution_rejections(
                config, TrialConfig(
                    agent_name="terminus-2",
                    agent_model=ModelSpec(provider="openai", name="glm-5.2"),
                ), source_provenance={}, allow_task_image_preparation=True,
            ) == ("immutable_task_input_unavailable",)
        else:
            assert config.environment.dockerfile is None
        wrapper = archive.extractfile(prefix + "verifier/run.sh")
        assert wrapper is not None
        assert wrapper.read() == (SOURCE / "verifier/run.sh").read_bytes()
        assert str(config.environment.workdir) == "/app"
        assert config.environment.cpu_arch == "x86_64"
        assert config.environment.user == "agent"
        assert config.environment.baseline_network_policy.kind == "gateway-only"
        assert config.verifier.user is None
        assert config.agent.name == "terminus-2"
        assert config.steps[0].required_artifacts == []
        assert config.steps[0].artifacts == ["archive_manifest.json", "build_manifest.py"]
    assert result["trial_submissions"] == 0
    assert result["environment_source"] == ("dockerfile" if image is None else "prebuilt")
    assert result["adaptation_profile"] == "nebius-amd64-nonroot-offline"
    assert result["original_profile_supported"] is False
    assert result["source_provenance"]["benchmark_id"] == "terminal-bench-2-harbor-90"
    assert json.loads((tmp_path / "taskset-build.json").read_text()) == result


@pytest.mark.parametrize("image", ["repo:latest", "repo@sha256:abc", IMAGE + '"\n'])
def test_rejects_mutable_or_malformed_image_before_output(tmp_path: Path, image: str) -> None:
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="immutable OCI"):
        prepare(image=image, output=output)
    assert not output.exists()


@pytest.mark.parametrize("image", [None, IMAGE])
def test_refuses_to_overwrite_and_has_reproducible_archive(
    tmp_path: Path, image: str | None,
) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    prepare(image=image, output=first)
    prepare(image=image, output=second)
    assert (first / "bundle.tar.gz").read_bytes() == (second / "bundle.tar.gz").read_bytes()
    before = (first / "bundle.tar.gz").read_bytes()
    with pytest.raises(ValueError, match="empty directory"):
        prepare(image=image, output=first)
    assert (first / "bundle.tar.gz").read_bytes() == before


def test_cli_defaults_to_dockerfile_preparation(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "scripts/ops/prepare_nebius_terminal_bench.py"),
         "--output", str(tmp_path)],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(root / "src")},
    )
    assert result.returncode == 0, result.stderr
    assert "no Trial submitted" in result.stdout
    evidence = json.loads((tmp_path / "taskset-build.json").read_text())
    assert evidence["environment_source"] == "dockerfile"
    assert evidence["task_image_ref"] is None
