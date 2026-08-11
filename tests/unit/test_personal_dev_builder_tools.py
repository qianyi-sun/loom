from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

from loom.personal_dev_builder_artifact import verify_personal_dev_build_artifact
from loom.personal_dev_builder_tools import (
    AsyncBoundedCommandRunner,
    SkopeoBuildxPersonalDevRegistryPublisher,
    TrivyPersonalDevImageScanner,
)
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_artifact import _artifact
from tests.unit.test_personal_dev_builder_exporter import _running_registration


class _Runner:
    def __init__(self, outputs: list[bytes]) -> None:
        self.outputs = outputs
        self.calls: list[tuple[list[str], float, int]] = []

    async def run(self, argv, *, timeout_seconds, max_output_bytes):
        self.calls.append((argv, timeout_seconds, max_output_bytes))
        return self.outputs.pop(0)


async def test_bounded_command_runner_uses_only_its_frozen_environment() -> None:
    configured = {"LOOM_TOOL_SCOPE": "scanner-only"}
    runner = AsyncBoundedCommandRunner(environment=configured)
    configured["LOOM_TOOL_SCOPE"] = "mutated"

    output = await runner.run(
        ["/usr/bin/env"],
        timeout_seconds=5,
        max_output_bytes=4096,
    )

    assert output.splitlines() == [b"LOOM_TOOL_SCOPE=scanner-only"]


def _verified_image(tmp_path: Path):
    artifact = tmp_path / "artifact.tar"
    extracted = tmp_path / "images"
    extracted.mkdir()
    _artifact(artifact)
    return verify_personal_dev_build_artifact(
        artifact,
        _registration(),
        platform="linux/amd64",
        output_directory=extracted,
        max_artifact_bytes=2 * 1024 * 1024,
        max_image_archive_bytes=256 * 1024,
    ).images["service"]


def _manifest_bytes(image) -> bytes:
    with tarfile.open(image.archive_path, mode="r:") as archive:
        member = archive.getmember(
            "blobs/sha256/" + image.manifest_digest.removeprefix("sha256:")
        )
        stream = archive.extractfile(member)
        assert stream is not None
        return stream.read()


async def test_trivy_scanner_is_offline_and_returns_only_bounded_evidence(
    tmp_path: Path,
) -> None:
    image = _verified_image(tmp_path)
    report = json.dumps(
        {
            "SchemaVersion": 2,
            "ArtifactType": "container_image",
            "Results": [{"Target": "rootfs", "Vulnerabilities": None, "Secrets": []}],
        }
    ).encode()
    runner = _Runner([report])
    scanner = TrivyPersonalDevImageScanner(
        runner=runner,  # type: ignore[arg-type]
        executable="/usr/local/bin/trivy",
        cache_directory=tmp_path / "trivy-cache",
        scanner_identity="trivy:0.64.1-db:sha256:" + "a" * 64,
        policy_sha256="b" * 64,
    )

    result = await scanner.scan(image, registration=_running_registration())

    argv = runner.calls[0][0]
    assert "--offline-scan" in argv
    assert "--skip-db-update" in argv
    assert "--input" in argv
    assert result.report == report
    assert result.evidence == {
        "policy_sha256": "b" * 64,
        "report_sha256": hashlib.sha256(report).hexdigest(),
        "result": "clean",
        "scanner_identity": "trivy:0.64.1-db:sha256:" + "a" * 64,
    }


async def test_registry_publisher_preserves_platform_digest_and_verifies_joined_index(
    tmp_path: Path,
) -> None:
    image = _verified_image(tmp_path)
    manifest = _manifest_bytes(image)
    arm_digest = "sha256:" + "8" * 64
    index = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "digest": image.manifest_digest,
                    "platform": {"architecture": "amd64", "os": "linux"},
                },
                {
                    "digest": arm_digest,
                    "platform": {"architecture": "arm64", "os": "linux"},
                },
            ],
        },
        separators=(",", ":"),
    ).encode()
    runner = _Runner([b"", manifest, b"", index])
    publisher = SkopeoBuildxPersonalDevRegistryPublisher(
        runner=runner,  # type: ignore[arg-type]
        skopeo_executable="/usr/bin/skopeo",
        docker_executable="/usr/bin/docker",
        registry_auth_file=tmp_path / "registry" / "config.json",
    )
    registration = _running_registration()
    repository = "registry.example/personal-dev/loom-service"

    platform_digest = await publisher.publish_platform(
        image,
        registration=registration,
        repository=repository,
    )
    reference, index_digest = await publisher.publish_index(
        registration=registration,
        repository=repository,
        platform_digests={"linux/amd64": platform_digest, "linux/arm64": arm_digest},
    )

    assert platform_digest == image.manifest_digest
    assert reference == f"{repository}@{index_digest}"
    assert index_digest == "sha256:" + hashlib.sha256(index).hexdigest()
    copy = runner.calls[0][0]
    assert copy[:3] == ["/usr/bin/skopeo", "copy", "--authfile"]
    assert "--preserve-digests" in copy
    assert copy[-1].startswith("docker://registry.example/")
    join = runner.calls[2][0]
    assert join[:4] == ["/usr/bin/docker", "buildx", "imagetools", "create"]
