from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom_control_plane.task_image_build_environment import (
    RootlessBuildResourceRequestV1,
    SlurmBuildEnvironmentPolicyV1,
    SlurmBuildInventoryV1,
    SlurmBuildJobObservationV1,
    issue_slurm_build_grant,
)
from loom_control_plane.task_image_build_grants import classify_task_image_build_inventory

_NOW = datetime(2026, 8, 22, 2, 0, tzinfo=UTC)
_GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")


def _policy() -> SlurmBuildEnvironmentPolicyV1:
    return SlurmBuildEnvironmentPolicyV1(
        schema="loom.task-image-build-environment-policy/v1",
        enabled=False,
        activation_blockers=("guard_missing",),
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        submitting_identity="loom-builder",
        partition="loom-task-builder",
        account="loom-task-builder",
        qos="loom-task-image-builder-rootless-gb10",
        feature_constraint="loom_rootless_buildkit",
        supervisor_path="/usr/local/libexec/loom-task-builder-supervisor",
        sbatch_path="/usr/bin/sbatch",
        resources=RootlessBuildResourceRequestV1(
            cpus=8,
            memory_mib=32768,
            pids=4096,
            scratch_bytes=107374182400,
            scratch_inodes=1000000,
            wall_time="02:00:00",
            swap_bytes=0,
        ),
    )


def _grant():
    return issue_slurm_build_grant(_policy(), grant_id=_GRANT_ID)


def _job(
    job_id: str,
    *,
    state: str = "pending",
    held: bool = True,
    request_drift: bool = False,
    comment: str | None = None,
) -> SlurmBuildJobObservationV1:
    grant = _grant()
    request = grant.request
    if request_drift:
        request = request.model_copy(
            update={
                "resources": request.resources.model_copy(update={"memory_mib": 16384})
            }
        )
    return SlurmBuildJobObservationV1(
        job_id=job_id,
        state=state,  # type: ignore[arg-type]
        held=held,
        comment=comment or grant.comment,
        submitting_identity=grant.request.submitting_identity,
        request=request,
    )


def _inventory(
    *jobs: SlurmBuildJobObservationV1,
    controller_authoritative: bool = True,
    accounting_authoritative: bool = True,
) -> SlurmBuildInventoryV1:
    return SlurmBuildInventoryV1(
        controller_authoritative=controller_authoritative,
        accounting_authoritative=accounting_authoritative,
        observed_at=_NOW,
        jobs=jobs,
    )


@pytest.mark.parametrize(
    "inventory",
    [
        _inventory(controller_authoritative=False),
        _inventory(accounting_authoritative=False),
        _inventory(_job("10", state="unknown")),
    ],
)
def test_incomplete_or_unknown_inventory_waits_without_binding(inventory) -> None:
    decision = classify_task_image_build_inventory(
        _grant(),
        inventory,
        ambiguity_settle_until=_NOW - timedelta(seconds=1),
        now=_NOW,
    )

    assert decision.action == "wait"
    assert decision.bind_job_id is None
    assert decision.cancel_job_ids == ()


def test_authoritative_zero_waits_until_settle_then_revokes() -> None:
    before = classify_task_image_build_inventory(
        _grant(),
        _inventory(),
        ambiguity_settle_until=_NOW + timedelta(seconds=1),
        now=_NOW,
    )
    after = classify_task_image_build_inventory(
        _grant(),
        _inventory(),
        ambiguity_settle_until=_NOW - timedelta(seconds=1),
        now=_NOW,
    )

    assert before.action == "wait"
    assert after.action == "revoke"
    assert after.reason == "authoritative_inventory_empty"


def test_one_exact_pending_held_job_is_the_only_bindable_inventory() -> None:
    decision = classify_task_image_build_inventory(
        _grant(),
        _inventory(_job("12345")),
        ambiguity_settle_until=_NOW - timedelta(seconds=1),
        now=_NOW,
    )

    assert decision.action == "bind"
    assert decision.bind_job_id == "12345"
    assert decision.cancel_job_ids == ()


@pytest.mark.parametrize(
    ("inventory", "expected_cancellations"),
    [
        (_inventory(_job("10", state="running", held=False)), ("10",)),
        (_inventory(_job("11", held=False)), ("11",)),
        (_inventory(_job("12", request_drift=True)), ("12",)),
        (
            _inventory(
                _job("14"),
                _job("13", state="running", held=False),
            ),
            ("13", "14"),
        ),
        (
            _inventory(
                _job("15", state="terminal", held=False),
                _job("16"),
            ),
            ("16",),
        ),
    ],
)
def test_live_nonheld_mismatched_mixed_or_multiple_inventory_requires_cancellation(
    inventory: SlurmBuildInventoryV1,
    expected_cancellations: tuple[str, ...],
) -> None:
    decision = classify_task_image_build_inventory(
        _grant(),
        inventory,
        ambiguity_settle_until=_NOW - timedelta(seconds=1),
        now=_NOW,
    )

    assert decision.action == "cancel_then_reconcile"
    assert decision.bind_job_id is None
    assert decision.cancel_job_ids == expected_cancellations


def test_terminal_inventory_revokes_without_cancelling_or_binding() -> None:
    decision = classify_task_image_build_inventory(
        _grant(),
        _inventory(_job("12345", state="terminal", held=False)),
        ambiguity_settle_until=_NOW - timedelta(seconds=1),
        now=_NOW,
    )

    assert decision.action == "revoke"
    assert decision.reason == "terminal_submission_observed"
    assert decision.bind_job_id is None
    assert decision.cancel_job_ids == ()
