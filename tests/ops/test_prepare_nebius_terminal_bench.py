from __future__ import annotations

import json
import tarfile
import tomllib
from pathlib import Path

import pytest
import yaml
from scripts.ops.prepare_nebius_terminal_bench import SOURCE, prepare

from loom.models.task import TaskConfig
from loom.models.taskset import UserTaskSetManifest

IMAGE = "registry.example/loom-terminal-bench@sha256:" + "a" * 64


def test_prepares_uploadable_task_with_original_assertions_and_no_oracle(tmp_path: Path) -> None:
    result = prepare(image=IMAGE, output=tmp_path)
    manifest = UserTaskSetManifest.model_validate(
        yaml.safe_load((tmp_path / "manifest.yaml").read_text())
    )
    assert manifest.source.locator == "bundle.tar.gz"
    with tarfile.open(tmp_path / manifest.source.locator) as archive:
        prefix = "tasks/file-archive-manifest/"
        assert set(archive.getnames()) == {
            prefix + name
            for name in (
                "task.toml",
                "instruction.md",
                "tests/test_outputs.py",
                "verifier/run.sh",
                "source-provenance.json",
            )
        }
        for name in ("instruction.md", "tests/test_outputs.py"):
            member = archive.extractfile(prefix + name)
            assert member is not None
            assert member.read() == (SOURCE / "original" / name).read_bytes()
        member = archive.extractfile(prefix + "task.toml")
        assert member is not None
        config = TaskConfig.model_validate(tomllib.loads(member.read().decode()))
        assert config.environment.docker_image == IMAGE
        assert str(config.environment.workdir) == "/app"
        assert config.environment.cpu_arch == "x86_64"
        assert config.environment.user == "agent"
        assert config.environment.baseline_network_policy.kind == "gateway-only"
        assert config.verifier.user is None
        assert config.agent.name == "terminus-2"
        assert config.steps[0].required_artifacts == []
        assert config.steps[0].artifacts == ["archive_manifest.json", "build_manifest.py"]
    assert result["trial_submissions"] == 0
    assert result["source_provenance"]["benchmark_id"] == "terminal-bench-2-harbor-90"
    assert json.loads((tmp_path / "taskset-build.json").read_text()) == result


@pytest.mark.parametrize("image", ["repo:latest", "repo@sha256:abc", IMAGE + '"\n'])
def test_rejects_mutable_or_malformed_image_before_output(tmp_path: Path, image: str) -> None:
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="immutable OCI"):
        prepare(image=image, output=output)
    assert not output.exists()


def test_refuses_to_overwrite_and_has_reproducible_archive(tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    prepare(image=IMAGE, output=first)
    prepare(image=IMAGE, output=second)
    assert (first / "bundle.tar.gz").read_bytes() == (second / "bundle.tar.gz").read_bytes()
    before = (first / "bundle.tar.gz").read_bytes()
    with pytest.raises(ValueError, match="empty directory"):
        prepare(image=IMAGE, output=first)
    assert (first / "bundle.tar.gz").read_bytes() == before
