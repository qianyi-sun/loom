"""Registered data descriptors survive issuance and CPU-only replay unchanged."""

import asyncio
import hashlib
import json
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.task_image_build_plan import TaskImageBuildPlanV2
from loom.task_image_bundle_manifest import (
    TaskImageBundleContentManifestV1,
    capture_task_image_bundle_manifest,
)
from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME
from loom_task_image_authority import bundle_capability as module
from loom_task_image_authority.materializations import _bundle_preparation
from tests.unit.test_task_image_bundle_async_capability import Backend, _provider
from tests.unit.test_task_image_bundle_capability import NOW, _plan


@pytest.fixture
def registered(tmp_path):
    (tmp_path / "environment").mkdir()
    (tmp_path / "environment/Dockerfile").write_bytes(b"FROM scratch\n")
    executable = tmp_path / "café +%<&\u2028\u2029.sh"
    executable.write_bytes(b"#!/bin/sh\n")
    executable.chmod(0o755)
    manifest = capture_task_image_bundle_manifest(tmp_path)
    values = _plan().model_dump()
    values.update(
        schema_version="loom.task-image-build-plan.v2",
        bundle_prefix=f"registered/{manifest.digest}/",
        bundle_content_manifest_sha256=manifest.digest,
        task_checksum=manifest.task_checksum,
        bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
    )
    return manifest, TaskImageBuildPlanV2.model_validate(values)


class VerifiedBackend(Backend):
    def __init__(self, manifest):
        super().__init__()
        self.manifest = manifest
        self.verified = []

    async def get_verified_bundle_manifest(self, **options):
        self.verified.append(options)
        if self.wait is not None:
            await self.wait.wait()
        self.now += timedelta(seconds=self.listing_seconds)
        return self.manifest


def test_native_go_manifest_vector_matches_python_canonical_and_legacy_encodings():
    # Paired with Go TestRegisteredManifestMatchesIndependentPythonRFC8785AndLegacyModeVectors.
    paths = ['a"<&>.txt', "café\u2028\u2029.sh", "\ue000.txt", "😀.txt"]
    files = [dict(path=path, size_bytes=i, sha256=hashlib.sha256(bytes(i)).hexdigest(), mode="0755" if i % 2 else "0644") for i, path in enumerate(paths)]
    metadata = json.dumps(dict(schema_version=1, files={item["path"]: dict(mode=item["mode"]) for item in files}), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    manifest = TaskImageBundleContentManifestV1.model_validate_json(json.dumps(dict(
        task_checksum="4" * 64, bundle_file_metadata_sha256=hashlib.sha256(metadata).hexdigest(), files=files,
    )))
    assert manifest.digest == "cb43cfc9ffa01829856e09529fdae263027daa520d680575005f2e2ecf90edbc"
    assert manifest.bundle_file_metadata_sha256 == "a035ff1f504fa372da12d29a7c95d7641915ef59bb2d2f74c2d90a3e45c64b3c"
    assert manifest.mode_metadata_bytes == metadata


async def test_native_issuance_uses_verified_inventory_and_only_registered_data(registered):
    manifest, plan = registered
    backend = VerifiedBackend(manifest)
    backend.listing_seconds, backend.signing_seconds = 1, 1
    provider = _provider(backend)
    capability = await provider.issue(plan, now=NOW)
    assert capability.schema_version == "loom.task-image-bundle-capability.v2"
    assert not isinstance(capability, module.TaskImageBundleCapabilityV1)
    assert capability.bundle_content_manifest_sha256 == manifest.digest
    assert capability.content_manifest == manifest
    assert capability.file_count == len(manifest.files) == 2
    assert capability.total_bytes == sum(item.size_bytes for item in manifest.files)
    assert [item.relative_path for item in capability.objects] == [item.path for item in manifest.files]
    assert BUNDLE_FILE_METADATA_NAME not in [item.relative_path for item in capability.objects]
    for item, file in zip(capability.objects, manifest.files, strict=True):
        assert (item.sha256, item.mode, item.size_bytes) == (file.sha256, file.mode, file.size_bytes)
        assert item.url not in repr(item)
    assert "objects" not in repr(capability)
    assert backend.verified == [dict(
        bucket=plan.bundle_bucket, prefix=plan.bundle_prefix, expected_sha256=manifest.digest,
        task_checksum=plan.task_checksum, bundle_file_metadata_sha256=plan.bundle_file_metadata_sha256,
        maximum_objects=2000, maximum_bytes=536870912, expires_at=NOW + timedelta(seconds=40),
    )]
    assert not backend.listing
    signing_count = len(backend.signing)
    provider.validate(capability, plan, now=backend.now)
    assert len(backend.verified) == 1 and len(backend.signing) == signing_count
    parsed = module.parse_task_image_bundle_capability(capability.model_dump_json())
    assert parsed == capability


@pytest.mark.parametrize("change", [
    "hash", "mode", "size", "path", "sidecar", "duplicate", "manifest", "checksum", "metadata",
    "grant", "materialization", "session", "generation", "expired", "url", "quota", "bytes", "wire_size", "v1",
])
async def test_v2_replay_rejects_tampering_without_storage(registered, change):
    manifest, plan = registered
    backend = VerifiedBackend(manifest)
    provider = _provider(backend)
    capability = await provider.issue(plan, now=NOW)
    now = NOW
    objects = list(capability.objects)
    if change in {"hash", "mode", "size", "path", "sidecar", "url"}:
        field, value = {
            "hash": ("sha256", "6" * 64), "mode": ("mode", "0644"),
            "size": ("size_bytes", 0), "path": ("relative_path", "a-changed"),
            "sidecar": ("relative_path", BUNDLE_FILE_METADATA_NAME),
            "url": ("url", objects[0].url.replace("/loom-bundles/", "/other-bucket/")),
        }[change]
        objects[0] = objects[0].model_copy(update={field: value})
        capability = capability.model_copy(update={"objects": tuple(objects), "total_bytes": sum(item.size_bytes for item in objects)})
    elif change == "duplicate":
        capability = capability.model_copy(update={"objects": (objects[0], objects[0]), "total_bytes": objects[0].size_bytes * 2})
    elif change in {"manifest", "checksum", "metadata", "grant", "materialization", "session", "generation"}:
        field = {"manifest": "bundle_content_manifest_sha256", "checksum": "task_checksum", "metadata": "bundle_file_metadata_sha256", "grant": "grant_id", "materialization": "materialization_id", "session": "session_id", "generation": "session_generation"}[change]
        value = 99 if change == "generation" else uuid4() if change in {"grant", "materialization", "session"} else "6" * 64
        capability = capability.model_copy(update={field: value})
    elif change == "expired":
        now = capability.expires_at
    elif change in {"quota", "bytes", "wire_size"}:
        provider = _provider(backend, **{{"quota": "maximum_objects", "bytes": "maximum_bytes", "wire_size": "maximum_capability_bytes"}[change]: 1})
    else:
        # A strong plan cannot accept a legacy envelope, even with matching IDs.
        raw = capability.model_dump(mode="json")
        raw["schema_version"] = "loom.task-image-bundle-capability.v1"
        raw.pop("bundle_content_manifest_sha256")
        for item in raw["objects"]:
            item.pop("sha256")
            item.pop("mode")
        capability = module.TaskImageBundleCapabilityV1.model_validate_json(json.dumps(raw))
    with pytest.raises(module.TaskImageBundleCapabilityError):
        provider.validate(capability, plan, now=now)
    assert len(backend.verified) == 1 and len(backend.signing) == 2


@pytest.mark.parametrize("change", ["manifest", "quota", "bytes", "expired", "regression", "signing", "wire_size", "legacy_backend"])
async def test_v2_issuance_rejects_bad_backend_and_limits(registered, change):
    manifest, plan = registered
    backend = VerifiedBackend(manifest)
    options = {}
    if change == "manifest":
        backend.manifest = manifest.model_copy(update={"task_checksum": "6" * 64})
    elif change in {"quota", "bytes", "wire_size"}:
        options[{"quota": "maximum_objects", "bytes": "maximum_bytes", "wire_size": "maximum_capability_bytes"}[change]] = 1
    elif change == "expired":
        backend.listing_seconds = 40
    elif change == "regression":
        backend.listing_seconds = -1
    elif change == "signing":
        backend.signing_seconds = 20
    else:
        backend = Backend()
    with pytest.raises(module.TaskImageBundleCapabilityError):
        await _provider(backend, **options).issue(plan, now=NOW)
    assert not backend.listing


async def test_v2_cancellation_owns_inventory_read_and_never_signs(registered):
    manifest, plan = registered
    backend = VerifiedBackend(manifest)
    backend.wait = asyncio.Event()
    task = asyncio.create_task(_provider(backend).issue(plan, now=NOW))
    await asyncio.sleep(0)
    assert backend.verified
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not backend.signing


async def test_parser_requires_explicit_version_and_enforces_utf8_limit(registered):
    manifest, plan = registered
    capability = await _provider(VerifiedBackend(manifest)).issue(plan, now=NOW)
    raw = capability.model_dump(mode="json")
    raw.pop("schema_version")
    with pytest.raises(ValueError):
        module.parse_task_image_bundle_capability(json.dumps(raw))
    for raw_payload in (b" " * (module.MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES + 1), "é" * (module.MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES // 2 + 1)):
        with pytest.raises(ValueError):
            module.parse_task_image_bundle_capability(raw_payload)


@pytest.mark.parametrize("change", ["none", "successor", "digest", "expired", "legacy_provider"])
async def test_encrypted_replay_reader_preserves_v2_and_revalidates_current_session(registered, change):
    manifest, plan = registered
    backend = VerifiedBackend(manifest)
    provider = _provider(backend)
    capability = await provider.issue(plan, now=NOW)
    payload = capability.model_dump_json()
    event = SimpleNamespace(
        secret_response_ref="loom://task-image-bundle-capability/fixture",
        secret_response_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        secret_response_expires_at=capability.expires_at,
    )

    class Secrets:
        async def get(self, ref):
            assert ref == event.secret_response_ref
            return payload

    if change == "successor":
        successor_id = uuid4()
        plan = TaskImageBuildPlanV2.model_validate(dict(
            plan.model_dump(), session_id=successor_id,
            session_generation=plan.session_generation + 1, builder_id=f"rootless:{successor_id.hex}",
        ))
    elif change == "digest":
        event.secret_response_sha256 = "6" * 64
    elif change == "expired":
        event.secret_response_expires_at = NOW
    elif change == "legacy_provider":
        from tests.unit.test_task_image_bundle_capability import _FakeBundleBackend
        from tests.unit.test_task_image_bundle_capability import _provider as legacy_provider

        provider = legacy_provider(_FakeBundleBackend(()))
    # Only the encrypted-reader boundary is isolated. This test does not bypass
    # production claim admission or claim evidence for V2 database derivation.
    state = SimpleNamespace(event=event, plan=plan, checked_at=NOW, valid_until=plan.authorization_expires_at)
    request = _bundle_preparation(state, provider=provider, secret_store=Secrets(), clock=lambda: NOW)
    if change == "none":
        prepared = await request
        assert prepared.plan == plan and prepared.capability == capability
        assert prepared.capability.schema_version == "loom.task-image-bundle-capability.v2"
    else:
        with pytest.raises(module.TaskImageBundleCapabilityError):
            await request
    assert len(backend.verified) == 1 and len(backend.signing) == 2
