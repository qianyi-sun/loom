from __future__ import annotations

import hashlib
import io
import json
import tarfile
import tomllib
from pathlib import Path

import pytest

from loom.models.task import TaskConfig
from loom.nebius_acceptance_taskset import build_nebius_acceptance_taskset
from tests.support.execution_image_admission import signed_image_admission_bundle

_TASK_IMAGE = "registry.example/loom-service@sha256:" + "a" * 64
_RUNTIME_IMAGE = "registry.example/loom-runtime@sha256:" + "b" * 64


def _profile(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "loom.service-execution-runtime-profile.v1",
                "logical_pool_id": "nebius-cpu",
                "candidate_sha": "c" * 40,
                "execution_class_id": "linux-amd64-cpu-pod-v1",
                "task_image_ref": _TASK_IMAGE,
                "runtime_image_ref": _RUNTIME_IMAGE,
                "runtime_binary_sha256": "sha256:" + "d" * 64,
                "image_admission": signed_image_admission_bundle(
                    (_TASK_IMAGE, _RUNTIME_IMAGE)
                ).model_dump(mode="json"),
            }
        ),
        encoding="utf-8",
    )


def test_builder_emits_deterministic_nebius_compatible_taskset(tmp_path: Path) -> None:
    profile = tmp_path / "profile.json"
    _profile(profile)
    first = tmp_path / "first"
    second = tmp_path / "second"
    evidence = build_nebius_acceptance_taskset(
        runtime_profile_path=profile,
        output_dir=first,
    )
    build_nebius_acceptance_taskset(runtime_profile_path=profile, output_dir=second)

    assert (first / "bundle.tar.gz").read_bytes() == (second / "bundle.tar.gz").read_bytes()
    assert evidence["required_outputs"] == [
        "artifacts/answer.txt",
        "artifacts/reasoning.md",
        "trajectory/events.jsonl",
        "accounting/usage.json",
        "verifier/output.json",
    ]
    with tarfile.open(
        fileobj=io.BytesIO((first / "bundle.tar.gz").read_bytes()), mode="r:gz"
    ) as archive:
        assert archive.getnames() == [
            "tasks/canonical-output/task.toml",
            "tasks/canonical-output/instruction.md",
            "tasks/canonical-output/verifier/check.sh",
        ]
        task_stream = archive.extractfile("tasks/canonical-output/task.toml")
        assert task_stream is not None
        task = TaskConfig.model_validate(tomllib.loads(task_stream.read().decode()))
        assert task.environment.docker_image == _TASK_IMAGE
        assert task.steps[0].required_artifacts == ["answer.txt", "reasoning.md"]
        verifier = archive.getmember("tasks/canonical-output/verifier/check.sh")
        assert verifier.mode == 0o755
        verifier_stream = archive.extractfile(verifier)
        assert verifier_stream is not None
        assert b"sleep 180" in verifier_stream.read()

    build_bytes = (first / "taskset-build.json").read_bytes()
    assert (first / "taskset-build.json.sha256").read_text() == (
        f"{hashlib.sha256(build_bytes).hexdigest()}  taskset-build.json\n"
    )


def test_builder_refuses_output_file(tmp_path: Path) -> None:
    profile = tmp_path / "profile.json"
    _profile(profile)
    output = tmp_path / "output"
    output.write_text("owned", encoding="utf-8")

    with pytest.raises(ValueError, match="not a directory"):
        build_nebius_acceptance_taskset(runtime_profile_path=profile, output_dir=output)
