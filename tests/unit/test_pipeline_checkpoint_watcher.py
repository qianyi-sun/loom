from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import CheckpointPolicyV1
from loom_worker.pipeline_attempt_workspace import (
    AttemptWorkspace,
    BehaviorRecoveryLedgerV1,
    RecoveryTerminalCandidateV1,
)
from loom_worker.pipeline_checkpoint_watcher import (
    CheckpointClaimIdentityV1,
    CheckpointWatcherJournal,
    PipelineCheckpointWatcher,
    PipelineCheckpointWatcherError,
    scan_completed_checkpoints,
)

_RECIPE = "sha256:" + "1" * 64
_INPUT = "sha256:" + "2" * 64
_EXECUTION = "sha256:" + "3" * 64
_IMAGE = "sha256:" + "4" * 64


def _resume_key() -> str:
    return canonical_digest(
        {
            "checkpoint_schema": "loom.execution-checkpoint.v1",
            "execution_spec_digest": _EXECUTION,
            "image_digest": _IMAGE,
            "resolved_input_bindings_digest": _INPUT,
            "recipe_digest": _RECIPE,
        },
        persisted=False,
    )


def _ledger(count: int = 1) -> BehaviorRecoveryLedgerV1:
    return BehaviorRecoveryLedgerV1(
        schema_version="behavior.recovery-ledger.v1",
        stream="mop",
        input_digest=_INPUT,
        execution_spec_digest=_EXECUTION,
        resume_compatibility_key=_resume_key(),
        sample_id=UUID(int=10),
        terminal_candidates=[
            RecoveryTerminalCandidateV1(
                candidate_id=f"candidate-{index}",
                terminal_state="rejected",
                output_sha256=None,
                q_score_delta=None,
            )
            for index in range(count)
        ],
        success_episode_ids=[],
        files=[],
    )


def _committed_root(tmp_path: Path) -> Path:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    workspace = AttemptWorkspace(
        outputs,
        UUID(int=3),
        "idempotency",
        output_declarations={},
        final_output_bytes_limit=1,
        checkpoint_bytes_limit=16_777_216,
        checkpoint_min_interval_seconds=5,
        checkpoint_max_committed=20,
        resolved_input_bindings_digest=_INPUT,
        execution_spec_digest=_EXECUTION,
        recipe_digest=_RECIPE,
        image_digest=_IMAGE,
    )
    workspace.commit_checkpoint(0, _ledger(), [])
    return outputs / ".loom" / "checkpoints"


def _watcher(tmp_path: Path, root: Path) -> PipelineCheckpointWatcher:
    return PipelineCheckpointWatcher(
        checkpoint_root=root,
        identity=CheckpointClaimIdentityV1(
            pipeline_run_id=UUID(int=1),
            stage_run_id=UUID(int=2),
            attempt_id=UUID(int=3),
            recipe_digest=_RECIPE,
            resolved_input_bindings_digest=_INPUT,
            execution_spec_digest=_EXECUTION,
            image_digest=_IMAGE,
        ),
        policy=CheckpointPolicyV1(
            max_bytes=16_777_216,
            min_interval_seconds=5,
            max_committed_per_attempt=20,
        ),
        journal=CheckpointWatcherJournal((tmp_path / "journal" / "checkpoint.json").resolve()),
    )


def test_watcher_validates_inner_view_and_builds_outer_envelope(tmp_path: Path) -> None:
    root = _committed_root(tmp_path)
    watcher = _watcher(tmp_path, root)
    discovered = watcher.scan_if_due(monotonic_now=0)
    assert [item.sequence for item in discovered] == [0]
    assert watcher.scan_if_due(monotonic_now=4.999) == []
    assert [item.sequence for item in watcher.scan_if_due(monotonic_now=5.0)] == [0]
    prepared = watcher.prepare(discovered[0])
    assert prepared.envelope.sequence == 0
    assert [item.relative_path for item in prepared.envelope.files] == [
        "COMPLETE.json",
        "ledger.json",
    ]
    assert prepared.exact_artifact_data_bytes == prepared.envelope.exact_artifact_data_bytes


def test_watcher_rejects_hardlinked_inner_file(tmp_path: Path) -> None:
    root = _committed_root(tmp_path)
    ledger = root / "000000000000" / "ledger.json"
    (tmp_path / "ledger-link").hardlink_to(ledger)
    with pytest.raises(PipelineCheckpointWatcherError, match="checkpoint_contract_mismatch"):
        scan_completed_checkpoints(root)


def test_journal_enforces_interval_count_and_cancellation_freeze(tmp_path: Path) -> None:
    root = _committed_root(tmp_path)
    watcher = _watcher(tmp_path, root)
    discovered = watcher.scan_if_due(monotonic_now=0)
    now = datetime(2026, 8, 12, tzinfo=UTC)
    assert watcher.journal.session_admitted(sequence=0, now=now, policy=watcher.policy)
    watcher.journal.mark_session_started(0)
    watcher.journal.mark_committed(0, committed_at=now)
    assert not watcher.journal.session_admitted(
        sequence=0, now=now + timedelta(seconds=5), policy=watcher.policy
    )

    watcher.journal.freeze_cancellation(observed_at=now, checkpoints=discovered)
    assert watcher.journal.cancel_drain_candidate(
        terminal_cause="user_cancel", reservation_fits=True
    ) is None
    assert watcher.journal.cancel_drain_candidate(
        terminal_cause="artifact_budget", reservation_fits=True
    ) is None


def test_empty_cancellation_scan_still_closes_discovery(tmp_path: Path) -> None:
    journal = CheckpointWatcherJournal((tmp_path / "journal.json").resolve())
    observed = datetime(2026, 8, 12, tzinfo=UTC)
    journal.freeze_cancellation(observed_at=observed, checkpoints=[])
    assert journal.state.cancellation_discovery_closed is True
    assert journal.state.cancellation_frozen == []
