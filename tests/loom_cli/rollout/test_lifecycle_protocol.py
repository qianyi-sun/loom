from __future__ import annotations

import pytest

from loom_cli.rollout.lifecycle_protocol import (
    LifecycleAction,
    LifecyclePhase,
    LifecycleState,
    lifecycle_protocol_digest,
    run_lifecycle_self_test,
    transition_lifecycle,
)


def test_success_path_requires_verified_backup_before_launch() -> None:
    state = LifecycleState("req-12345678")
    for action in (
        LifecycleAction.START_BACKUP,
        LifecycleAction.VERIFY_BACKUP,
        LifecycleAction.PUBLISH_LAUNCH,
        LifecycleAction.START_LAUNCH,
    ):
        state = transition_lifecycle(state, action)

    assert state.phase is LifecyclePhase.LAUNCH_RUNNING
    assert state.sequence == 4


@pytest.mark.parametrize(
    ("phase", "action"),
    [
        (LifecyclePhase.BACKUP_PENDING, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.BACKUP_RUNNING, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.BACKUP_CANCEL_REQUESTED, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.BACKUP_FAILED, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.LAUNCH_RUNNING, LifecycleAction.REQUEST_CANCEL),
    ],
)
def test_forbidden_transition_fails_without_changing_state(phase, action) -> None:
    state = LifecycleState("req-12345678", phase=phase)

    with pytest.raises(ValueError, match="not authorized"):
        transition_lifecycle(state, action)

    assert state.phase is phase
    assert state.sequence == 0


def test_self_test_covers_success_cancellation_failure_and_rejections() -> None:
    result = run_lifecycle_self_test()

    assert result.ready
    assert result.scenario_count == 4
    assert result.transition_count == 11
    assert result.rejection_count == 6
    assert result.protocol_digest == lifecycle_protocol_digest()
    assert len(result.evidence_digest) == 64
