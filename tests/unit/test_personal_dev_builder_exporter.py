from __future__ import annotations

import hashlib
import io
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom.personal_dev_builder_exporter import (
    PersonalDevImageScanResult,
    S3TrustedPersonalDevBuildPublicationExporter,
)
from loom.personal_dev_builder_runtime import personal_dev_build_artifact_key
from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    validate_personal_dev_candidate_publication,
)
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_artifact import _artifact


def _running_registration():
    registration = _registration()
    assert registration.build_attempt is not None
    return replace(
        registration,
        candidate=replace(
            registration.candidate,
            registry_prefix="registry.example/personal-dev",
        ),
        build_attempt=replace(registration.build_attempt, state="running"),
    )


class _Body(io.BytesIO):
    def close(self) -> None:
        super().close()


class _ObjectStore:
    def __init__(self, artifacts: dict[str, bytes]) -> None:
        self.artifacts = artifacts
        self.requests: list[tuple[str, str]] = []
        self.objects: dict[str, dict[str, object]] = {}

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        platform = "linux/amd64" if "/amd64/" in key else "linux/arm64"
        registration = _registration()
        attempt = registration.build_attempt
        assert attempt is not None
        payload = self.artifacts[platform]
        self.requests.append((kwargs["Bucket"], key))
        return {
            "Body": _Body(payload),
            "ContentLength": len(payload),
            "ContentType": "application/vnd.loom.personal-dev-build.v1+tar",
            "Metadata": {
                "attestation-scope": "personal-dev-only",
                "build-attempt-id": str(attempt.id),
                "build-lease-epoch": str(attempt.lease_epoch),
                "candidate-sha256": registration.candidate.candidate_sha,
                "platform": platform,
            },
        }

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = dict(kwargs)
        return {}

    def head_object(self, **kwargs):
        stored = self.objects[kwargs["Key"]]
        return {
            "ContentLength": stored["ContentLength"],
            "ContentType": stored["ContentType"],
            "Metadata": stored["Metadata"],
        }


class _Scanner:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def scan(self, image, **_kwargs):
        self.events.append(f"scan:{image.platform}:{image.component}")
        report = b'{"ArtifactType":"container_image","Results":[]}'
        return PersonalDevImageScanResult(
            report=report,
            evidence={
                "report_sha256": hashlib.sha256(report).hexdigest(),
                "scanner": "test-v1",
                "result": "clean",
            },
        )


class _Publisher:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def publish_platform(self, image, *, repository, **_kwargs):
        self.events.append(f"push:{image.platform}:{image.component}")
        return image.manifest_digest

    async def publish_index(self, *, repository, platform_digests, **_kwargs):
        self.events.append(f"index:{repository}")
        assert set(platform_digests) == set(PERSONAL_DEV_PLATFORMS)
        digest = "sha256:" + "9" * 64
        return f"{repository}@{digest}", digest


async def test_trusted_exporter_verifies_and_scans_every_image_before_publication(
    tmp_path: Path,
) -> None:
    artifacts: dict[str, bytes] = {}
    for platform in PERSONAL_DEV_PLATFORMS:
        path = tmp_path / f"{platform.rsplit('/', 1)[1]}.tar"
        _artifact(path, platform=platform)
        artifacts[platform] = path.read_bytes()
    events: list[str] = []
    registration = _running_registration()
    store = _ObjectStore(artifacts)
    exporter = S3TrustedPersonalDevBuildPublicationExporter(
        object_store=store,  # type: ignore[arg-type]
        expected_bucket="artifacts",
        max_artifact_bytes=2 * 1024 * 1024,
        max_image_archive_bytes=256 * 1024,
        scanner=_Scanner(events),  # type: ignore[arg-type]
        publisher=_Publisher(events),  # type: ignore[arg-type]
        registry_prefix="registry.example/personal-dev",
        publisher_identity="system:serviceaccount:loom-dev:candidate-exporter",
        trusted_launcher_profile_sha256="f" * 64,
        protocol_versions={"personal-dev-activation": "v1"},
        clock=lambda: datetime(2026, 8, 11, tzinfo=UTC),
    )

    publication = await exporter.publish(registration)

    normalized, publication_sha, image_set_digest = (
        validate_personal_dev_candidate_publication(
            registration.candidate,
            publication,
        )
    )
    assert normalized == publication
    assert len(publication_sha) == 64
    assert image_set_digest.startswith("sha256:")
    assert set(publication["images"]) == set(PERSONAL_DEV_COMPONENTS)
    first_push = next(index for index, event in enumerate(events) if event.startswith("push:"))
    assert first_push == len(PERSONAL_DEV_COMPONENTS) * len(PERSONAL_DEV_PLATFORMS)
    assert all("@sha256:" in image["index"] for image in publication["images"].values())
    evidence = publication["safety_evidence"]
    assert isinstance(evidence, dict)
    assert evidence["key"] in store.objects
    assert len(store.objects) == len(PERSONAL_DEV_COMPONENTS) * 2 + 1


async def test_trusted_exporter_rejects_wrong_object_binding_before_scan(tmp_path: Path) -> None:
    path = tmp_path / "amd64.tar"
    _artifact(path)
    artifacts = {platform: path.read_bytes() for platform in PERSONAL_DEV_PLATFORMS}
    store = _ObjectStore(artifacts)
    original = store.get_object

    def substituted(**kwargs):
        response = original(**kwargs)
        response["Metadata"]["candidate-sha256"] = "0" * 64
        return response

    store.get_object = substituted  # type: ignore[method-assign]
    events: list[str] = []
    exporter = S3TrustedPersonalDevBuildPublicationExporter(
        object_store=store,  # type: ignore[arg-type]
        expected_bucket="artifacts",
        max_artifact_bytes=2 * 1024 * 1024,
        max_image_archive_bytes=256 * 1024,
        scanner=_Scanner(events),  # type: ignore[arg-type]
        publisher=_Publisher(events),  # type: ignore[arg-type]
        registry_prefix="registry.example/personal-dev",
        publisher_identity="publisher",
        trusted_launcher_profile_sha256="f" * 64,
        protocol_versions={"personal-dev-activation": "v1"},
    )

    with pytest.raises(RuntimeError, match="binding"):
        await exporter.publish(_running_registration())
    assert events == []


def test_exporter_requests_attempt_and_lease_unique_artifact_keys(tmp_path: Path) -> None:
    registration = _registration()
    assert all(
        f"/{registration.build_attempt.id}/l{registration.build_attempt.lease_epoch:016x}/"
        in personal_dev_build_artifact_key(registration, platform=platform)
        for platform in PERSONAL_DEV_PLATFORMS
    )
