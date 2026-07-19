"""Single-source rollout backup and launch state-transition authority."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum


class LifecyclePhase(StrEnum):
    BACKUP_PENDING = "backup_pending"
    BACKUP_RUNNING = "backup_running"
    BACKUP_CANCEL_REQUESTED = "backup_cancel_requested"
    BACKUP_FAILED = "backup_failed"
    BACKUP_VERIFIED = "backup_verified"
    LAUNCH_PENDING = "launch_pending"
    LAUNCH_RUNNING = "launch_running"


class LifecycleAction(StrEnum):
    START_BACKUP = "start_backup"
    REQUEST_CANCEL = "request_cancel"
    SEAL_CANCELLED = "seal_cancelled"
    FAIL_BACKUP = "fail_backup"
    VERIFY_BACKUP = "verify_backup"
    PUBLISH_LAUNCH = "publish_launch"
    START_LAUNCH = "start_launch"


_TRANSITIONS: dict[tuple[LifecyclePhase, LifecycleAction], LifecyclePhase] = {
    (LifecyclePhase.BACKUP_PENDING, LifecycleAction.START_BACKUP): LifecyclePhase.BACKUP_RUNNING,
    (
        LifecyclePhase.BACKUP_PENDING,
        LifecycleAction.REQUEST_CANCEL,
    ): LifecyclePhase.BACKUP_CANCEL_REQUESTED,
    (
        LifecyclePhase.BACKUP_RUNNING,
        LifecycleAction.REQUEST_CANCEL,
    ): LifecyclePhase.BACKUP_CANCEL_REQUESTED,
    (
        LifecyclePhase.BACKUP_CANCEL_REQUESTED,
        LifecycleAction.SEAL_CANCELLED,
    ): LifecyclePhase.BACKUP_FAILED,
    (LifecyclePhase.BACKUP_PENDING, LifecycleAction.FAIL_BACKUP): LifecyclePhase.BACKUP_FAILED,
    (LifecyclePhase.BACKUP_RUNNING, LifecycleAction.FAIL_BACKUP): LifecyclePhase.BACKUP_FAILED,
    (LifecyclePhase.BACKUP_RUNNING, LifecycleAction.VERIFY_BACKUP): LifecyclePhase.BACKUP_VERIFIED,
    (
        LifecyclePhase.BACKUP_VERIFIED,
        LifecycleAction.PUBLISH_LAUNCH,
    ): LifecyclePhase.LAUNCH_PENDING,
    (LifecyclePhase.LAUNCH_PENDING, LifecycleAction.START_LAUNCH): LifecyclePhase.LAUNCH_RUNNING,
}


def lifecycle_protocol_digest() -> str:
    payload = [
        {"action": action.value, "from": source.value, "to": target.value}
        for (source, action), target in sorted(
            _TRANSITIONS.items(), key=lambda item: (item[0][0].value, item[0][1].value)
        )
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class LifecycleState:
    request_id: str
    phase: LifecyclePhase = LifecyclePhase.BACKUP_PENDING
    sequence: int = 0

    def __post_init__(self) -> None:
        if not self.request_id.startswith("req-") or len(self.request_id) > 68 or self.sequence < 0:
            raise ValueError("lifecycle state identity is invalid")


def transition_lifecycle(state: LifecycleState, action: LifecycleAction) -> LifecycleState:
    """Apply exactly one reviewed transition or fail closed without mutation."""
    target = _TRANSITIONS.get((state.phase, action))
    if target is None:
        raise ValueError("lifecycle transition is not authorized")
    return LifecycleState(
        request_id=state.request_id,
        phase=target,
        sequence=state.sequence + 1,
    )


@dataclass(frozen=True, slots=True)
class LifecycleSelfTestEvidence:
    ready: bool
    scenario_count: int
    transition_count: int
    rejection_count: int
    protocol_digest: str
    evidence_digest: str


def run_lifecycle_self_test() -> LifecycleSelfTestEvidence:
    """Exercise success, cancellation, failure, and forbidden-launch paths."""
    scenarios = (
        (
            LifecycleAction.START_BACKUP,
            LifecycleAction.VERIFY_BACKUP,
            LifecycleAction.PUBLISH_LAUNCH,
            LifecycleAction.START_LAUNCH,
        ),
        (LifecycleAction.REQUEST_CANCEL, LifecycleAction.SEAL_CANCELLED),
        (
            LifecycleAction.START_BACKUP,
            LifecycleAction.REQUEST_CANCEL,
            LifecycleAction.SEAL_CANCELLED,
        ),
        (LifecycleAction.START_BACKUP, LifecycleAction.FAIL_BACKUP),
    )
    transition_count = 0
    terminal: list[str] = []
    for index, actions in enumerate(scenarios):
        state = LifecycleState(request_id=f"req-selftest-{index:02d}")
        for action in actions:
            state = transition_lifecycle(state, action)
            transition_count += 1
        terminal.append(state.phase.value)

    rejected = 0
    forbidden = (
        (LifecyclePhase.BACKUP_PENDING, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.BACKUP_RUNNING, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.BACKUP_CANCEL_REQUESTED, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.BACKUP_FAILED, LifecycleAction.PUBLISH_LAUNCH),
        (LifecyclePhase.LAUNCH_PENDING, LifecycleAction.REQUEST_CANCEL),
        (LifecyclePhase.LAUNCH_RUNNING, LifecycleAction.REQUEST_CANCEL),
    )
    for index, (phase, action) in enumerate(forbidden):
        try:
            transition_lifecycle(
                LifecycleState(request_id=f"req-reject-{index:02d}", phase=phase),
                action,
            )
        except ValueError:
            rejected += 1

    protocol_digest = lifecycle_protocol_digest()
    ready = terminal == [
        LifecyclePhase.LAUNCH_RUNNING.value,
        LifecyclePhase.BACKUP_FAILED.value,
        LifecyclePhase.BACKUP_FAILED.value,
        LifecyclePhase.BACKUP_FAILED.value,
    ] and rejected == len(forbidden)
    payload = {
        "protocol_digest": protocol_digest,
        "rejection_count": rejected,
        "scenario_count": len(scenarios),
        "terminal": terminal,
        "transition_count": transition_count,
    }
    return LifecycleSelfTestEvidence(
        ready=ready,
        scenario_count=len(scenarios),
        transition_count=transition_count,
        rejection_count=rejected,
        protocol_digest=protocol_digest,
        evidence_digest=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


__all__ = [
    "LifecycleAction",
    "LifecyclePhase",
    "LifecycleSelfTestEvidence",
    "LifecycleState",
    "lifecycle_protocol_digest",
    "run_lifecycle_self_test",
    "transition_lifecycle",
]
