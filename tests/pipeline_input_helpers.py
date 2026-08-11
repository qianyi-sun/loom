from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from loom.pipeline.artifact_commit import ArtifactManifestV1, StoredFileV1
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.spec import BindingItemV1, BindingSetV1
from loom.pipeline.work_protocol import ExecutionAttemptClaimV1


@dataclass
class NeverCancelled:
    async def requested(self) -> bool:
        return False


@dataclass
class Cancelled:
    async def requested(self) -> bool:
        return True


@dataclass
class FakeArtifactInputReadClient:
    manifest: ArtifactManifestV1
    payload: bytes
    manifest_opens: int = 0
    file_opens: int = 0

    async def read_manifest(self, **_: object) -> ArtifactManifestV1:
        self.manifest_opens += 1
        return self.manifest

    async def read_file(self, *, destination: Path, **_: object) -> tuple[int, int]:
        self.file_opens += 1
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.payload)
        return len(self.payload), 1


def scalar_artifact() -> tuple[bytes, ArtifactManifestV1, BindingSetV1]:
    payload = canonical_document({"schema_version": "example.input.v1", "value": 7})
    content_sha256 = digest_bytes(payload)
    artifact_id = uuid4()
    manifest = ArtifactManifestV1(
        artifact_id=artifact_id,
        artifact_name="dataset",
        artifact_type="example_dataset.v1",
        content_sha256=content_sha256,
        stored_size_bytes=len(payload),
        unpacked_size_bytes=len(payload),
        file_count=1,
        stored_files=[
            StoredFileV1(
                file_index=0,
                relative_path="artifact.json",
                role="semantic_document",
                archive_format="none",
                media_type="application/json",
                size_bytes=len(payload),
                sha256=content_sha256,
            )
        ],
        lineage_artifact_ids=[],
        lineage_digests=[],
    )
    manifest_sha256 = digest_bytes(canonical_document(manifest))
    binding = BindingSetV1(
        binding_name="dataset",
        artifact_type="example_dataset.v1",
        cardinality="one",
        items=[
            BindingItemV1(
                artifact_id=artifact_id,
                content_sha256=content_sha256,
                file_count=1,
                item_key="singleton",
                manifest_sha256=manifest_sha256,
                stored_size_bytes=len(payload),
                unpacked_size_bytes=len(payload),
            )
        ],
    )
    return payload, manifest, binding


def claim(
    binding: BindingSetV1,
    *,
    attempt_id: UUID | None = None,
    acceptance_preflight: object | None = None,
) -> ExecutionAttemptClaimV1:
    return ExecutionAttemptClaimV1.model_construct(
        execution_attempt_id=attempt_id or uuid4(),
        claim_id=uuid4(),
        lease_epoch=1,
        lease_token="x" * 32,
        input_bindings=[binding],
        stage_request=None,
        resume_checkpoint=None,
        acceptance_preflight=acceptance_preflight,
    )
