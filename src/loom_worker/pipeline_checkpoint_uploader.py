"""Crash-convergent worker adapter from committed local checkpoints to #1214."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID, uuid5

from loom.pipeline.keys import digest_bytes
from loom.pipeline.work_protocol import FinalOutputInventoryItemV1
from loom_worker.control_plane_client import ExecutionAttemptClaimHeaders
from loom_worker.pipeline_checkpoint_watcher import (
    CheckpointWatcherJournal,
    PipelineCheckpointWatcherError,
    PreparedCheckpoint,
)


class CheckpointControlPlaneV1(Protocol):
    async def prepare_checkpoint(self, **kwargs: Any) -> dict[str, Any]: ...
    async def upload_checkpoint_part(self, **kwargs: Any) -> dict[str, Any]: ...
    async def complete_checkpoint_file(self, **kwargs: Any) -> dict[str, Any]: ...
    async def commit_checkpoint_session(self, **kwargs: Any) -> dict[str, Any]: ...


class CommittedLocalCheckpointReleaseV1(Protocol):
    async def release(
        self, *, attempt_id: UUID, sequence: int, artifact_id: UUID
    ) -> None: ...


@dataclass(slots=True)
class PipelineCheckpointUploader:
    """Upload exact watcher bytes and release local state only after DB authority readback."""

    control_plane: CheckpointControlPlaneV1
    journal: CheckpointWatcherJournal
    local_release: CommittedLocalCheckpointReleaseV1

    async def upload(
        self,
        *,
        prepared: PreparedCheckpoint,
        claim: ExecutionAttemptClaimHeaders,
        cancel_drain: bool = False,
    ) -> UUID:
        envelope = prepared.envelope
        attempt_id = envelope.attempt_id
        existing = next(
            (
                item
                for item in self.journal.state.entries
                if item.sequence == envelope.sequence and item.state == "committed"
            ),
            None,
        )
        if existing is not None:
            assert existing.committed_artifact_id is not None
            await self.local_release.release(
                attempt_id=attempt_id,
                sequence=envelope.sequence,
                artifact_id=existing.committed_artifact_id,
            )
            return existing.committed_artifact_id
        request_id = uuid5(attempt_id, f"loom-checkpoint:{envelope.sequence:012d}")
        inventory = [
            FinalOutputInventoryItemV1(
                output_name="checkpoint",
                relative_path=item.descriptor.relative_path,
                size_bytes=item.descriptor.size_bytes,
                sha256=item.descriptor.sha256,
            )
            for item in prepared.local.files
        ]
        grant = await self.control_plane.prepare_checkpoint(
            attempt_id=attempt_id,
            claim=claim,
            request_id=request_id,
            payload={
                "schema_version": "loom.checkpoint-prepare.v1",
                "checkpoint": envelope.model_dump(mode="json"),
                "checkpoint_sha256": digest_bytes(prepared.checkpoint_json),
                "files": [item.model_dump(mode="json") for item in inventory],
                "cancel_drain": cancel_drain,
            },
        )
        session_id = UUID(grant["upload_session_id"])
        token = str(grant["upload_token"])
        plans = sorted(grant["files"], key=lambda item: int(item["file_index"]))
        values = [prepared.checkpoint_json, *(item.value for item in prepared.local.files)]
        if len(plans) != len(values):
            raise PipelineCheckpointWatcherError("checkpoint_upload_plan_drift")
        self.journal.mark_session_started(
            envelope.sequence,
            upload_session_id=session_id,
            cancel_drain=cancel_drain,
        )
        for plan, value in zip(plans, values, strict=True):
            file_index = int(plan["file_index"])
            if (
                int(plan["expected_size"]) != len(value)
                or plan["expected_sha256"] != digest_bytes(value)
            ):
                raise PipelineCheckpointWatcherError("checkpoint_upload_plan_drift")
            receipt = await self.control_plane.upload_checkpoint_part(
                attempt_id=attempt_id,
                session_id=session_id,
                file_index=file_index,
                part_number=1,
                claim=claim,
                request_id=uuid5(request_id, f"part:{file_index}:1"),
                upload_token=token,
                content_sha256=digest_bytes(value),
                content=value,
            )
            await self.control_plane.complete_checkpoint_file(
                attempt_id=attempt_id,
                session_id=session_id,
                file_index=file_index,
                claim=claim,
                request_id=uuid5(request_id, f"complete:{file_index}"),
                upload_token=token,
                payload={
                    "schema_version": "loom.final-output-file-complete.v1",
                    "ordered_parts": [receipt],
                },
            )
        committed = await self.control_plane.commit_checkpoint_session(
            attempt_id=attempt_id,
            session_id=session_id,
            claim=claim,
            request_id=uuid5(request_id, "commit"),
            upload_token=token,
        )
        artifact_id = _committed_artifact_id(committed)
        committed_at = datetime.now(UTC)
        self.journal.mark_committed(
            envelope.sequence,
            committed_at=committed_at,
            artifact_id=artifact_id,
        )
        await self.local_release.release(
            attempt_id=attempt_id,
            sequence=envelope.sequence,
            artifact_id=artifact_id,
        )
        return artifact_id


def _committed_artifact_id(response: Mapping[str, Any]) -> UUID:
    if response.get("state") != "committed":
        raise PipelineCheckpointWatcherError("checkpoint_db_readback_missing")
    artifacts = response.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 1:
        raise PipelineCheckpointWatcherError("checkpoint_db_readback_missing")
    item = artifacts[0]
    if not isinstance(item, Mapping) or item.get("artifact_type") != "loom.execution-checkpoint.v1":
        raise PipelineCheckpointWatcherError("checkpoint_db_readback_missing")
    try:
        return UUID(str(item["id"]))
    except (KeyError, ValueError) as exc:
        raise PipelineCheckpointWatcherError("checkpoint_db_readback_missing") from exc


__all__ = [
    "CheckpointControlPlaneV1",
    "CommittedLocalCheckpointReleaseV1",
    "PipelineCheckpointUploader",
]
