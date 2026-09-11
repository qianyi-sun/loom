"""Allocated builds enter the restricted sandbox without storage capabilities."""

import os
from importlib import import_module

import pytest

from loom import personal_dev_sandbox_builder as sandbox
from loom.personal_dev_builder_artifact import verify_personal_dev_build_artifact
from loom.personal_dev_builder_manifest import (
    PersonalDevBuilderManifestConfig,
    personal_dev_builder_contract,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from tests.unit.test_capacity_build_admission_client import native_registration
from tests.unit.test_native_build_context import claim_for, context_for
from tests.unit.test_native_build_source import sealed_source as sealed_source
from tests.unit.test_personal_dev_builder_artifact import _oci_archive


def bound_context(registration, pool):
    candidate, attempt = registration.candidate, registration.build_attempt
    return context_for(claim_for(native_registration(pool).binding)).model_copy(update={
        "candidate_id": candidate.id, "candidate_sha": candidate.candidate_sha,
        "source_sha256": candidate.source_sha256, "archive_sha256": candidate.archive_sha256,
        "archive_size_bytes": candidate.archive_size_bytes, "build_contract_sha256": candidate.build_contract_sha256,
        "source_commit": candidate.source_commit, "dirty": candidate.dirty,
        "attempt_id": attempt.id, "attempt_sequence": attempt.attempt_sequence,
        "lease_epoch": attempt.lease_epoch, "subject_id": attempt.subject_id,
        "subject_incarnation": attempt.subject_incarnation, "operation_id": attempt.operation_id,
        "operation_epoch": attempt.operation_epoch})


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
async def test_native_context_renders_existing_exact_sandbox_contract(sealed_source, pool):
    module = import_module("loom_capacity_executor.native_sandbox_contract")
    registration, _archive, _workspace = sealed_source
    context = bound_context(registration, pool)
    config = PersonalDevBuilderManifestConfig(builder_image="registry.example/builder@sha256:" + "a" * 64,
        max_artifact_bytes=2 * 1024 * 1024, max_image_archive_bytes=256 * 1024)
    wire = module.render_native_sandbox_contract(context, max_artifact_bytes=config.max_artifact_bytes,
        max_image_archive_bytes=config.max_image_archive_bytes)
    assert wire == personal_dev_builder_contract(registration, platform=context.platform, config=config).encode("ascii")
    assert "credential" not in wire.decode() and "object_key" not in wire.decode()


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "identity", "source", "size", "symlink", "fifo", "existing", "build"])
async def test_allocated_sandbox_consumes_verified_local_source_and_emits_bound_artifact(sealed_source, tmp_path, monkeypatch, pool, boundary):
    module = import_module("loom_capacity_executor.native_sandbox_contract")
    registration, archive, _workspace = sealed_source
    context = bound_context(registration, pool)
    contract_file = tmp_path / "native-contract.json"
    contract_file.write_bytes(module.render_native_sandbox_contract(context, max_artifact_bytes=2 * 1024 * 1024,
        max_image_archive_bytes=256 * 1024))
    incoming = tmp_path / "incoming.tar"
    incoming.write_bytes(archive if boundary != "source" else b"!" + archive[1:])
    if boundary == "size":
        incoming.write_bytes(archive[:-1])
    elif boundary == "symlink":
        link = tmp_path / "linked-source.tar"
        link.symlink_to(incoming)
        incoming = link
    elif boundary == "fifo":
        fifo = tmp_path / "source.fifo"
        os.mkfifo(fifo)
        incoming = fifo
    workspace = tmp_path / "sandbox-work"
    if boundary == "existing":
        workspace.mkdir()
        (workspace / "foreign").write_text("preserve")
    events = []
    def identity():
        events.append("identity")
        if boundary == "identity":
            raise sandbox.PersonalDevSandboxBuildError("wrong runtime")
    def forbidden(*args, **kwargs):
        pytest.fail("allocated sandbox attempted network source/artifact capability IO")
    monkeypatch.setattr(sandbox, "_verify_client_identity", identity)
    monkeypatch.setattr(sandbox, "_download_source", forbidden)
    monkeypatch.setattr(sandbox, "_upload_artifact", forbidden)
    def build(contract, *, source_directory, output_directory, **kwargs):
        assert events == ["identity"]
        assert (source_directory / "feature.txt").read_text() == "feature\n" * 140000
        events.append("build")
        if boundary == "build":
            raise sandbox.PersonalDevSandboxBuildError("build failed")
        output_directory.mkdir()
        data, digest = _oci_archive(architecture="arm64" if pool == "gb10" else "amd64")
        result = {}
        for component in PERSONAL_DEV_COMPONENTS:
            path = output_directory / f"{component}.oci.tar"
            path.write_bytes(data)
            result[component] = path, digest
        return result
    monkeypatch.setattr(sandbox, "_build_images", build)
    run = sandbox.run_allocated_personal_dev_sandbox_build
    if boundary == "exact":
        result = run(contract_file=contract_file, source_archive=incoming, workspace=workspace)
        assert result == workspace / "artifacts.tar"
        output = tmp_path / "verified"
        output.mkdir()
        verified = verify_personal_dev_build_artifact(result, registration, platform=context.platform,
            output_directory=output, max_artifact_bytes=2 * 1024 * 1024, max_image_archive_bytes=256 * 1024)
        assert set(verified.images) == set(PERSONAL_DEV_COMPONENTS)
    else:
        with pytest.raises((OSError, ValueError, RuntimeError)):
            run(contract_file=contract_file, source_archive=incoming, workspace=workspace)
        assert not (workspace / "artifacts.tar").exists()
        assert events == (["identity", "build"] if boundary == "build" else ["identity"])
        if boundary == "identity":
            assert not workspace.exists()
        elif boundary == "existing":
            assert (workspace / "foreign").read_text() == "preserve"


def test_allocated_sandbox_cli_uses_no_capability_arguments(monkeypatch):
    calls = []
    monkeypatch.setattr(sandbox, "run_allocated_personal_dev_sandbox_build", lambda **kwargs: calls.append(kwargs), raising=False)
    assert sandbox.main(["build-allocated", "--contract-file", "/input/contract.json",
        "--source-archive", "/input/source.tar", "--workspace", "/workspace/build"]) == 0
    assert len(calls) == 1 and "capability_directory" not in calls[0]
    assert str(calls[0]["source_archive"]) == "/input/source.tar"


@pytest.mark.parametrize("boundary", ["contract", "artifact-bool", "image-bool", "image-limit", "source-limit"])
async def test_native_sandbox_contract_rejects_changed_contract_or_invalid_limits(sealed_source, boundary):
    module = import_module("loom_capacity_executor.native_sandbox_contract")
    registration, _archive, _workspace = sealed_source
    context = bound_context(registration, "gb10")
    artifact_limit, image_limit = 2 * 1024 * 1024, 256 * 1024
    if boundary == "contract":
        context = context.model_copy(update={"build_contract_sha256": "f" * 64})
    elif boundary == "artifact-bool":
        artifact_limit = True
    elif boundary == "image-bool":
        image_limit = True
    elif boundary == "image-limit":
        image_limit = artifact_limit + 1
    elif boundary == "source-limit":
        artifact_limit = 1
    with pytest.raises((ValueError, RuntimeError)):
        module.render_native_sandbox_contract(context, max_artifact_bytes=artifact_limit, max_image_archive_bytes=image_limit)
