from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from loom.pipeline.keys import canonical_digest
from loom_worker.pipeline_attempt_workspace import (
    AttemptWorkspace,
    AttemptWorkspaceError,
    BehaviorRecoveryLedgerV1,
    RecoveryTerminalCandidateV1,
)

_INPUT = "sha256:" + "1" * 64
_EXECUTION = "sha256:" + "2" * 64
_RECIPE = "sha256:" + "3" * 64
_IMAGE = "registry.example/behavior@sha256:" + "4" * 64


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
        sample_id=UUID("00000000-0000-0000-0000-000000000001"),
        terminal_candidates=[
            RecoveryTerminalCandidateV1(
                candidate_id=f"candidate-{index:02d}",
                terminal_state="rejected",
                output_sha256=None,
                q_score_delta=None,
            )
            for index in range(count)
        ],
        success_episode_ids=[],
        files=[],
    )


def _workspace(
    tmp_path: Path,
    *,
    crash_injector=None,
    monotonic=lambda: 10.0,
    sleep=lambda _seconds: None,
    cancelled=lambda: False,
) -> AttemptWorkspace:
    return AttemptWorkspace(
        tmp_path,
        "attempt",
        "key",
        output_declarations={},
        final_output_bytes_limit=1,
        checkpoint_bytes_limit=16_777_216,
        checkpoint_min_interval_seconds=5,
        checkpoint_max_committed=20,
        resolved_input_bindings_digest=_INPUT,
        execution_spec_digest=_EXECUTION,
        recipe_digest=_RECIPE,
        image_digest=_IMAGE,
        crash_injector=crash_injector,
        monotonic=monotonic,
        sleep=sleep,
        cancelled=cancelled,
    )


def test_checkpoint_commit_replay_sequence_and_latest(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    first = workspace.commit_checkpoint(0, _ledger(), [])
    assert first.sequence == 0
    assert first.complete.files == []
    assert workspace.commit_checkpoint(0, _ledger(), []) == first
    assert workspace.latest_committed_checkpoint() == first

    with pytest.raises(AttemptWorkspaceError, match="conflicting"):
        workspace.commit_checkpoint(0, _ledger(2), [])
    with pytest.raises(AttemptWorkspaceError, match="gap"):
        workspace.commit_checkpoint(2, _ledger(2), [])


@pytest.mark.parametrize(
    "boundary",
    [
        "checkpoint_before_payload_rename",
        "checkpoint_after_payload_rename",
        "checkpoint_after_ledger",
        "checkpoint_after_complete",
    ],
)
def test_checkpoint_crash_boundaries(tmp_path: Path, boundary: str) -> None:
    def crash(point: str) -> None:
        if point == boundary:
            raise RuntimeError("injected crash")

    workspace = _workspace(tmp_path, crash_injector=crash)
    with pytest.raises(RuntimeError, match="injected crash"):
        workspace.commit_checkpoint(0, _ledger(), [])
    retry = _workspace(tmp_path)
    if boundary == "checkpoint_after_complete":
        assert retry.commit_checkpoint(0, _ledger(), []).sequence == 0
    else:
        with pytest.raises(AttemptWorkspaceError):
            retry.commit_checkpoint(0, _ledger(), [])


def test_checkpoint_interval_waits_until_exact_boundary(tmp_path: Path) -> None:
    clock = [0.0]
    waits: list[float] = []

    def monotonic() -> float:
        return clock[0]

    def sleep(seconds: float) -> None:
        waits.append(seconds)
        clock[0] += seconds

    workspace = _workspace(tmp_path, monotonic=monotonic, sleep=sleep)
    workspace.commit_checkpoint(0, _ledger(), [])
    clock[0] = 4.999
    workspace.commit_checkpoint(1, _ledger(2), [])
    assert waits
    assert clock[0] >= 5.0


def test_checkpoint_interval_wait_is_cancellation_aware(tmp_path: Path) -> None:
    clock = [0.0]
    workspace = _workspace(
        tmp_path,
        monotonic=lambda: clock[0],
        sleep=lambda _seconds: None,
        cancelled=lambda: clock[0] < 5,
    )
    workspace.commit_checkpoint(0, _ledger(), [])
    clock[0] = 4.999
    with pytest.raises(AttemptWorkspaceError, match="cancelled"):
        workspace.commit_checkpoint(1, _ledger(2), [])


def test_checkpoint_rejects_wrong_resume_key_and_candidate_cap(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    wrong = _ledger().model_copy(update={"resume_compatibility_key": "sha256:" + "f" * 64})
    with pytest.raises(AttemptWorkspaceError, match="resume compatibility"):
        workspace.commit_checkpoint(0, wrong, [])
    with pytest.raises(ValueError):
        _ledger(21)
