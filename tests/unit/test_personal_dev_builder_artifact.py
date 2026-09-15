from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from loom.personal_dev_builder_artifact import (
    PersonalDevBuildArtifactError,
    verify_personal_dev_build_artifact,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from tests.unit.test_personal_dev_builder import _registration


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def _oci_archive(*, architecture: str) -> tuple[bytes, str]:
    config = _canonical({"architecture": architecture, "os": "linux"})
    config_digest = hashlib.sha256(config).hexdigest()
    layer = b"candidate image layer"
    layer_digest = hashlib.sha256(layer).hexdigest()
    manifest = _canonical(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": f"sha256:{config_digest}",
                "size": len(config),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": f"sha256:{layer_digest}",
                    "size": len(layer),
                }
            ],
        }
    )
    manifest_digest = hashlib.sha256(manifest).hexdigest()
    index = _canonical(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": f"sha256:{manifest_digest}",
                    "size": len(manifest),
                    "platform": {"architecture": architecture, "os": "linux"},
                }
            ],
        }
    )
    values = {
        "blobs/sha256/" + config_digest: config,
        "blobs/sha256/" + layer_digest: layer,
        "blobs/sha256/" + manifest_digest: manifest,
        "index.json": index,
        "oci-layout": _canonical({"imageLayoutVersion": "1.0.0"}),
    }
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, payload in sorted(values.items()):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue(), "sha256:" + manifest_digest


def _artifact(path: Path, *, platform: str = "linux/amd64", manifest_overrides: dict[str, object] | None = None) -> None:
    registration = _registration()
    architecture = platform.rsplit("/", 1)[1]
    archives: dict[str, bytes] = {}
    components: dict[str, object] = {}
    for component in PERSONAL_DEV_COMPONENTS:
        payload, manifest_digest = _oci_archive(architecture=architecture)
        archive_path = f"images/{component}.oci.tar"
        archives[archive_path] = payload
        components[component] = {
            "archive_path": archive_path,
            "archive_sha256": hashlib.sha256(payload).hexdigest(),
            "archive_size_bytes": len(payload),
            "manifest_digest": manifest_digest,
        }
    attempt = registration.build_attempt
    assert attempt is not None
    manifest = {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": registration.candidate.candidate_sha,
        "source_sha256": registration.candidate.source_sha256,
        "archive_sha256": registration.candidate.archive_sha256,
        "build_contract_sha256": registration.candidate.build_contract_sha256,
        "attempt_id": str(attempt.id),
        "lease_epoch": attempt.lease_epoch,
        "platform": platform,
        "components": components,
    }
    manifest.update(manifest_overrides or {})
    with tarfile.open(path, mode="w", format=tarfile.USTAR_FORMAT) as artifact:
        manifest_payload = _canonical(manifest)
        info = tarfile.TarInfo("manifest.json")
        info.size = len(manifest_payload)
        info.mode = 0o644
        artifact.addfile(info, io.BytesIO(manifest_payload))
        for name, payload in sorted(archives.items()):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644
            artifact.addfile(info, io.BytesIO(payload))


def test_artifact_verifier_binds_and_extracts_complete_native_image_set(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact.tar"
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    _artifact(artifact)

    verified = verify_personal_dev_build_artifact(
        artifact,
        _registration(),
        platform="linux/amd64",
        output_directory=extracted,
        max_artifact_bytes=1024 * 1024,
        max_image_archive_bytes=256 * 1024,
    )

    assert set(verified.images) == set(PERSONAL_DEV_COMPONENTS)
    assert all(image.archive_path.is_file() for image in verified.images.values())
    assert {
        image.manifest_digest for image in verified.images.values()
    } == {"sha256:" + hashlib.sha256(_canonical({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:" + hashlib.sha256(_canonical({"architecture": "amd64", "os": "linux"})).hexdigest(),
            "size": len(_canonical({"architecture": "amd64", "os": "linux"})),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": "sha256:" + hashlib.sha256(b"candidate image layer").hexdigest(),
            "size": len(b"candidate image layer"),
        }],
    })).hexdigest()}


def test_artifact_verifier_rejects_manifest_or_archive_substitution(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.tar"
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    _artifact(artifact, platform="linux/arm64")

    with pytest.raises(PersonalDevBuildArtifactError, match="platform"):
        verify_personal_dev_build_artifact(
            artifact,
            _registration(),
            platform="linux/amd64",
            output_directory=extracted,
            max_artifact_bytes=1024 * 1024,
            max_image_archive_bytes=256 * 1024,
        )


def test_artifact_verifier_rejects_traversal_without_extracting(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.tar"
    with tarfile.open(artifact, mode="w", format=tarfile.USTAR_FORMAT) as value:
        payload = b"escape"
        info = tarfile.TarInfo("../escape")
        info.size = len(payload)
        value.addfile(info, io.BytesIO(payload))
    extracted = tmp_path / "extracted"
    extracted.mkdir()

    with pytest.raises(PersonalDevBuildArtifactError, match="unsafe member"):
        verify_personal_dev_build_artifact(
            artifact,
            _registration(),
            platform="linux/amd64",
            output_directory=extracted,
            max_artifact_bytes=1024 * 1024,
            max_image_archive_bytes=256 * 1024,
        )
    assert not (tmp_path / "escape").exists()


@pytest.mark.parametrize("field,value", [("schema_version", True), ("schema_version", 1.0),
    ("lease_epoch", True), ("lease_epoch", 1.0)])
@pytest.mark.parametrize("adapter", ["registration", "native"])
def test_artifact_identity_rejects_equivalent_non_integer_wire_types(tmp_path, field, value, adapter):
    from loom import personal_dev_builder_artifact as module

    registration = _registration()
    candidate, attempt = registration.candidate, registration.build_attempt
    if field == "lease_epoch" and type(value) is float:
        value = float(attempt.lease_epoch)
    artifact = tmp_path / "artifact.tar"
    _artifact(artifact, manifest_overrides={field: value})
    output = tmp_path / "verified"
    output.mkdir()
    limits = dict(output_directory=output, max_artifact_bytes=1024 * 1024, max_image_archive_bytes=256 * 1024)
    with pytest.raises(PersonalDevBuildArtifactError, match="binding"):
        if adapter == "registration":
            module.verify_personal_dev_build_artifact(artifact, registration, platform="linux/amd64", **limits)
        else:
            binding = module.PersonalDevBuildArtifactBinding(candidate_sha=candidate.candidate_sha,
                source_sha256=candidate.source_sha256, archive_sha256=candidate.archive_sha256,
                build_contract_sha256=candidate.build_contract_sha256, attempt_id=attempt.id,
                lease_epoch=attempt.lease_epoch, platform="linux/amd64")
            module.verify_bound_personal_dev_build_artifact(artifact, binding, **limits)
    assert list(output.iterdir()) == []


@pytest.mark.parametrize("changed", [None, "candidate_sha", "source_sha256", "archive_sha256",
    "build_contract_sha256", "attempt_id", "lease_epoch", "platform"])
def test_bound_artifact_verification_does_not_require_synthetic_registration(tmp_path, changed):
    from dataclasses import replace
    from uuid import UUID

    from loom import personal_dev_builder_artifact as module

    registration = _registration()
    candidate, attempt = registration.candidate, registration.build_attempt
    binding = module.PersonalDevBuildArtifactBinding(candidate_sha=candidate.candidate_sha,
        source_sha256=candidate.source_sha256, archive_sha256=candidate.archive_sha256,
        build_contract_sha256=candidate.build_contract_sha256, attempt_id=attempt.id,
        lease_epoch=attempt.lease_epoch, platform="linux/amd64")
    artifact = tmp_path / "artifact.tar"
    _artifact(artifact)
    output = tmp_path / "verified"
    output.mkdir()
    if changed is not None:
        replacement = {"attempt_id": UUID(int=42), "lease_epoch": attempt.lease_epoch + 1,
            "platform": "linux/arm64"}.get(changed, "9" * 64)
        binding = replace(binding, **{changed: replacement})
        with pytest.raises(PersonalDevBuildArtifactError):
            module.verify_bound_personal_dev_build_artifact(artifact, binding,
                output_directory=output, max_artifact_bytes=1024 * 1024, max_image_archive_bytes=256 * 1024)
        assert list(output.iterdir()) == []
    else:
        result = module.verify_bound_personal_dev_build_artifact(artifact, binding,
            output_directory=output, max_artifact_bytes=1024 * 1024, max_image_archive_bytes=256 * 1024)
        assert set(result.images) == set(PERSONAL_DEV_COMPONENTS)
