from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import CheckpointPolicyV1
from loom_worker.control_plane_client import ExecutionAttemptClaimHeaders
from loom_worker.pipeline_attempt_workspace import (
    AttemptWorkspace,
    BehaviorRecoveryLedgerV1,
    RecoveryTerminalCandidateV1,
)
from loom_worker.pipeline_checkpoint_uploader import PipelineCheckpointUploader
from loom_worker.pipeline_checkpoint_watcher import (
    CheckpointClaimIdentityV1,
    CheckpointWatcherJournal,
    PipelineCheckpointWatcher,
)

_D = ["sha256:" + str(index) * 64 for index in range(1, 5)]


@dataclass
class _ControlPlane:
    artifact_id: UUID = field(default_factory=uuid4)
    calls: list[str] = field(default_factory=list)

    async def prepare_checkpoint(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("prepare")
        payload = kwargs["payload"]
        files = [
            {
                "file_index": 0,
                "expected_size": len(str(payload["checkpoint"]).encode()),
                "expected_sha256": payload["checkpoint_sha256"],
            }
        ]
        # The test replaces the first expected size with the exact bytes below.
        files[0]["expected_size"] = kwargs.pop("checkpoint_size", files[0]["expected_size"])
        files.extend(
            {
                "file_index": index,
                "expected_size": item["size_bytes"],
                "expected_sha256": item["sha256"],
            }
            for index, item in enumerate(payload["files"], start=1)
        )
        return {
            "upload_session_id": str(UUID(int=7)),
            "upload_token": "u" * 48,
            "files": files,
        }

    async def upload_checkpoint_part(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("part")
        return {
            "file_index": kwargs["file_index"],
            "part_number": 1,
            "size_bytes": len(kwargs["content"]),
            "sha256": kwargs["content_sha256"],
        }

    async def complete_checkpoint_file(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("file")
        return {"state": "verified"}

    async def commit_checkpoint_session(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("commit")
        return {
            "state": "committed",
            "artifacts": [
                {
                    "id": str(self.artifact_id),
                    "artifact_type": "loom.execution-checkpoint.v1",
                }
            ],
        }


@dataclass
class _Release:
    calls: list[tuple[UUID, int, UUID]] = field(default_factory=list)

    async def release(self, *, attempt_id: UUID, sequence: int, artifact_id: UUID) -> None:
        self.calls.append((attempt_id, sequence, artifact_id))


async def test_uploader_releases_local_checkpoint_only_after_db_authority(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    attempt_id = UUID(int=3)
    resume_key = canonical_digest(
        {
            "checkpoint_schema": "loom.execution-checkpoint.v1",
            "execution_spec_digest": _D[2],
            "image_digest": _D[3],
            "resolved_input_bindings_digest": _D[1],
            "recipe_digest": _D[0],
        },
        persisted=False,
    )
    workspace = AttemptWorkspace(
        outputs,
        attempt_id,
        "idempotency",
        output_declarations={},
        final_output_bytes_limit=1,
        checkpoint_bytes_limit=16_777_216,
        checkpoint_min_interval_seconds=5,
        checkpoint_max_committed=20,
        resolved_input_bindings_digest=_D[1],
        execution_spec_digest=_D[2],
        recipe_digest=_D[0],
        image_digest=_D[3],
    )
    workspace.commit_checkpoint(
        0,
        BehaviorRecoveryLedgerV1(
            schema_version="behavior.recovery-ledger.v1",
            stream="mop",
            input_digest=_D[1],
            execution_spec_digest=_D[2],
            resume_compatibility_key=resume_key,
            sample_id=UUID(int=9),
            terminal_candidates=[
                RecoveryTerminalCandidateV1(
                    candidate_id="candidate-0",
                    terminal_state="rejected",
                    output_sha256=None,
                    q_score_delta=None,
                )
            ],
            success_episode_ids=[],
            files=[],
        ),
        [],
    )
    journal = CheckpointWatcherJournal((tmp_path / "journal.json").resolve())
    watcher = PipelineCheckpointWatcher(
        checkpoint_root=(outputs / ".loom" / "checkpoints").resolve(),
        identity=CheckpointClaimIdentityV1(
            pipeline_run_id=UUID(int=1),
            stage_run_id=UUID(int=2),
            attempt_id=attempt_id,
            recipe_digest=_D[0],
            resolved_input_bindings_digest=_D[1],
            execution_spec_digest=_D[2],
            image_digest=_D[3],
        ),
        policy=CheckpointPolicyV1(
            max_bytes=16_777_216,
            min_interval_seconds=5,
            max_committed_per_attempt=20,
        ),
        journal=journal,
    )
    prepared = watcher.prepare(watcher.scan_if_due(monotonic_now=0)[0])
    control = _ControlPlane()
    original_prepare = control.prepare_checkpoint

    async def exact_prepare(**kwargs: Any) -> dict[str, Any]:
        result = await original_prepare(**kwargs, checkpoint_size=len(prepared.checkpoint_json))
        return result

    control.prepare_checkpoint = exact_prepare  # type: ignore[method-assign]
    release = _Release()
    artifact_id = await PipelineCheckpointUploader(control, journal, release).upload(
        prepared=prepared,
        claim=ExecutionAttemptClaimHeaders(
            claim_id=UUID(int=4), lease_epoch=1, lease_token="l" * 40
        ),
    )
    assert artifact_id == control.artifact_id
    assert control.calls[-1] == "commit"
    assert release.calls == [(attempt_id, 0, artifact_id)]
    assert journal.state.entries[0].state == "committed"
    committed_at = journal.state.entries[0].committed_at
    assert committed_at is not None and committed_at <= datetime.now(UTC)

    # Crash after journal authority but before local release converges without
    # a second prepare/upload/commit transaction.
    calls_before = list(control.calls)
    assert await PipelineCheckpointUploader(control, journal, release).upload(
        prepared=prepared,
        claim=ExecutionAttemptClaimHeaders(
            claim_id=UUID(int=4), lease_epoch=1, lease_token="l" * 40
        ),
    ) == artifact_id
    assert control.calls == calls_before
    assert release.calls[-1] == (attempt_id, 0, artifact_id)
