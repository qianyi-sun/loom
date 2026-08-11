from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.personal_dev_builder_artifact import verify_personal_dev_build_artifact
from loom.personal_dev_builder_manifest import (
    PersonalDevBuilderManifestConfig,
    personal_dev_builder_manifest_documents,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from loom.personal_dev_sandbox_builder import (
    PersonalDevSandboxBuildContract,
    PersonalDevSandboxBuildError,
    create_personal_dev_build_artifact,
)
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_artifact import _oci_archive


def _contract() -> PersonalDevSandboxBuildContract:
    documents = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/builder@sha256:" + "a" * 64,
            max_artifact_bytes=2 * 1024 * 1024,
            max_image_archive_bytes=256 * 1024,
        ),
    )
    config_map = next(document for document in documents if document["kind"] == "ConfigMap")
    return PersonalDevSandboxBuildContract.parse(
        config_map["data"]["contract.json"].encode("ascii")
    )


def test_sandbox_contract_and_output_round_trip_through_trusted_verifier(
    tmp_path: Path,
) -> None:
    contract = _contract()
    image_payload, manifest_digest = _oci_archive(architecture="amd64")
    images: dict[str, tuple[Path, str]] = {}
    for component in PERSONAL_DEV_COMPONENTS:
        path = tmp_path / f"{component}.oci.tar"
        path.write_bytes(image_payload)
        images[component] = (path, manifest_digest)
    artifact = tmp_path / "artifacts.tar"

    create_personal_dev_build_artifact(contract, images, artifact)

    extracted = tmp_path / "extracted"
    extracted.mkdir()
    verified = verify_personal_dev_build_artifact(
        artifact,
        _registration(),
        platform="linux/amd64",
        output_directory=extracted,
        max_artifact_bytes=contract.max_artifact_bytes,
        max_image_archive_bytes=contract.max_image_archive_bytes,
    )
    assert set(verified.images) == set(PERSONAL_DEV_COMPONENTS)


def test_sandbox_contract_rejects_noncanonical_or_changed_authority() -> None:
    contract = _contract()
    noncanonical = json.dumps(dict(contract.raw), indent=2).encode("ascii")
    with pytest.raises(PersonalDevSandboxBuildError, match="canonical"):
        PersonalDevSandboxBuildContract.parse(noncanonical)

    changed = dict(contract.raw)
    changed["components"] = ["service"]
    payload = json.dumps(changed, sort_keys=True, separators=(",", ":")).encode("ascii")
    with pytest.raises(PersonalDevSandboxBuildError, match="authority"):
        PersonalDevSandboxBuildContract.parse(payload)
