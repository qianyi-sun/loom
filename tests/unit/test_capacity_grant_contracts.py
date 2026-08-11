from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

import loom_capacity_manager.grant_contracts as grant_contracts
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorHeartbeatV1,
    DryRunExecutorInventoryV1,
    DryRunExecutorRegistrationV1,
    DryRunIntentCloseV1,
    DryRunLaunchPermitV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunProtectedReleaseAcknowledgementV1,
    DryRunReservationAcceptanceV1,
    DryRunReservationProposalV1,
    ExecutorInventoryRecordV1,
    ReleasedShapeV1,
    ReservationShapeV1,
    canonical_grant_digest,
)


def test_grant_contract_classes_do_not_redeclare_schema_fields() -> None:
    module_path = Path(grant_contracts.__file__)
    module = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    duplicates: list[str] = []
    for definition in (node for node in module.body if isinstance(node, ast.ClassDef)):
        fields = [
            statement.target.id
            for statement in definition.body
            if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name)
        ]
        repeated = sorted({field for field in fields if fields.count(field) > 1})
        duplicates.extend(f"{definition.name}.{field}" for field in repeated)
    assert duplicates == []


def _shape(
    shape_instance_id: str = "shape-0001",
    *,
    concurrency_slots: int = 1,
    resources: ResourceVectorV1 | None = None,
    rollout_surge_slots: int = 0,
    old_shape_backing_id: str | None = None,
) -> ReservationShapeV1:
    return ReservationShapeV1(
        shape_instance_id=shape_instance_id,
        intent_id=UUID(int=10 + int(shape_instance_id[-1])),
        shape_id="one-slot",
        profile_id="profile-1",
        profile_generation=1,
        profile_digest="a" * 64,
        concurrency_slots=concurrency_slots,
        resources=resources
        or ResourceVectorV1(
            slots=concurrency_slots,
            cpu_millicores=1_000,
            memory_bytes=1_073_741_824,
        ),
        node_ids=("node-1",),
        rollout_surge_slots=rollout_surge_slots,
        old_shape_backing_id=old_shape_backing_id,
    )


def _proposal(*shapes: ReservationShapeV1) -> DryRunReservationProposalV1:
    return DryRunReservationProposalV1(
        tranche_id=UUID(int=1),
        authority_incarnation=UUID(int=90),
        writer_epoch=1,
        configuration_epoch=1,
        allocation_epoch=1,
        subject_id=UUID(int=2),
        subject_incarnation=UUID(int=3),
        account_id="owner-1",
        tier_id="development",
        candidate_generation=1,
        deployment_generation=1,
        pool_id="gb10",
        pool_generation=1,
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        shapes=shapes or (_shape(),),
    )


def test_grant_contract_is_canonical_sorted_strict_and_dry_run_only() -> None:
    proposal = _proposal(_shape("shape-0002"), _shape("shape-0001"))

    assert [shape.shape_instance_id for shape in proposal.shapes] == [
        "shape-0001",
        "shape-0002",
    ]
    assert proposal.executable is False
    assert canonical_grant_digest(proposal) == canonical_grant_digest(
        DryRunReservationProposalV1.model_validate_json(
            proposal.model_dump_json(),
        )
    )
    with pytest.raises(ValidationError):
        DryRunReservationProposalV1.model_validate(
            proposal.model_dump(mode="python") | {"unexpected": True}
        )
    with pytest.raises(ValidationError):
        DryRunReservationProposalV1.model_validate(
            proposal.model_dump(mode="python") | {"executable": True}
        )


def test_reservation_shape_binds_exact_slots_resources_and_surge_backing() -> None:
    with pytest.raises(ValidationError, match="resource slots"):
        _shape(
            concurrency_slots=2,
            resources=ResourceVectorV1(
                slots=1,
                cpu_millicores=1_000,
                memory_bytes=1_073_741_824,
            ),
        )
    with pytest.raises(ValidationError, match="surge backing"):
        _shape(rollout_surge_slots=1)
    with pytest.raises(ValidationError, match="surge backing"):
        _shape(old_shape_backing_id="old-shape")

    shape = _shape(
        rollout_surge_slots=1,
        old_shape_backing_id="old-shape",
    )
    assert shape.rollout_surge_slots == 1


def test_reservation_proposal_rejects_duplicate_shape_or_intent_identity() -> None:
    with pytest.raises(ValidationError, match="duplicate reservation shape"):
        _proposal(_shape(), _shape())

    first = _shape("shape-0001")
    second = _shape("shape-0002").model_copy(update={"intent_id": first.intent_id})
    with pytest.raises(ValidationError, match="duplicate submission intent"):
        _proposal(first, second)


def test_grant_contract_rejects_unbounded_or_noncanonical_values() -> None:
    with pytest.raises(ValidationError):
        _proposal(
            *(_shape(f"shape-{index:04d}") for index in range(257)),
        )
    with pytest.raises(ValidationError):
        DryRunReservationProposalV1(
            tranche_id=UUID(int=1),
            authority_incarnation=UUID(int=90),
            writer_epoch=1,
            configuration_epoch=1,
            allocation_epoch=1,
            subject_id=UUID(int=2),
            subject_incarnation=UUID(int=3),
            account_id="owner-1",
            tier_id="development",
            candidate_generation=1,
            deployment_generation=1,
            pool_id="GB10",
            pool_generation=1,
            executor_id="gb10-executor",
            executor_incarnation=UUID(int=4),
            shapes=(_shape(),),
        )


def test_contract_timestamps_are_not_executor_release_authority() -> None:
    proposal = _proposal()
    payload = proposal.model_dump(mode="python")
    assert "expires_at" not in payload
    assert "accepted_at" not in payload
    assert datetime(2026, 8, 11, tzinfo=UTC).tzinfo is UTC


def test_executor_and_acceptance_contracts_bind_identity_sequence_and_zero_authority() -> None:
    registration = DryRunExecutorRegistrationV1(
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        pool_id="gb10",
        pool_generation=1,
        signing_key_id="gb10-key-1",
        signing_key_sha256="b" * 64,
        local_authority_sha256="c" * 64,
    )
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=1),
        proposal_digest="d" * 64,
        executor_id=registration.executor_id,
        executor_incarnation=registration.executor_incarnation,
        command_sequence=1,
    )

    assert registration.executable is False
    assert acceptance.executable is False
    with pytest.raises(ValidationError):
        DryRunExecutorRegistrationV1.model_validate(
            registration.model_dump(mode="python") | {"executable": True}
        )
    with pytest.raises(ValidationError):
        DryRunReservationAcceptanceV1.model_validate(
            acceptance.model_dump(mode="python") | {"command_sequence": 0}
        )


def test_executor_heartbeat_binds_authority_lease_and_local_journal() -> None:
    heartbeat = DryRunExecutorHeartbeatV1(
        authority_incarnation=UUID(int=90),
        writer_epoch=1,
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        pool_id="gb10",
        pool_generation=1,
        heartbeat_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )

    assert heartbeat.executable is False
    with pytest.raises(ValidationError):
        DryRunExecutorHeartbeatV1.model_validate(
            heartbeat.model_dump(mode="python") | {"heartbeat_sequence": 0}
        )
    with pytest.raises(ValidationError):
        DryRunExecutorHeartbeatV1.model_validate(
            heartbeat.model_dump(mode="python") | {"executable": True}
        )
    with pytest.raises(ValidationError, match="canonical zero digest"):
        DryRunExecutorHeartbeatV1.model_validate(
            heartbeat.model_dump(mode="python") | {"journal_digest": "a" * 64}
        )
    with pytest.raises(ValidationError, match="canonical zero digest"):
        DryRunExecutorHeartbeatV1.model_validate(
            heartbeat.model_dump(mode="python")
            | {"journal_sequence": 1, "journal_digest": "0" * 64}
        )
    with pytest.raises(ValidationError, match="cannot exceed"):
        DryRunExecutorHeartbeatV1.model_validate(
            heartbeat.model_dump(mode="python")
            | {
                "journal_checkpoint_sequence": 1,
                "journal_checkpoint_digest": "b" * 64,
            }
        )


def test_executor_inventory_is_complete_canonical_and_foreign_proof_free() -> None:
    record = ExecutorInventoryRecordV1(
        physical_identity="job-2",
        physical_kind="slurm-job",
        authority_scope="foreign",
        state="terminal",
        resources=ResourceVectorV1(slots=1, cpu_millicores=1_000),
        controller_evidence_sha256="b" * 64,
        terminal_evidence_sha256="c" * 64,
    )
    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=UUID(int=90),
        writer_epoch=1,
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        pool_id="gb10",
        pool_generation=1,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
        records=(record,),
    )

    assert inventory.complete is True
    assert inventory.executable is False
    with pytest.raises(ValidationError, match="terminal evidence"):
        ExecutorInventoryRecordV1.model_validate(
            record.model_dump(mode="python") | {"terminal_evidence_sha256": None}
        )
    with pytest.raises(ValidationError):
        DryRunExecutorInventoryV1.model_validate(
            inventory.model_dump(mode="python") | {"complete": False}
        )


def test_bootstrap_permit_and_consumption_are_exact_dry_run_contracts() -> None:
    bootstrap = DryRunBootstrapRegistrationV1(
        tranche_id=UUID(int=1),
        intent_id=UUID(int=11),
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        command_sequence=2,
        bootstrap_registration_epoch=7,
        bootstrap_evidence_sha256="e" * 64,
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=20),
        intent_id=bootstrap.intent_id,
        allocation_epoch=1,
        configuration_epoch=1,
        executor_id=bootstrap.executor_id,
        executor_incarnation=bootstrap.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    consumption = DryRunPermitConsumptionV1(
        permit_id=permit.permit_id,
        permit_digest=canonical_grant_digest(permit),
        intent_id=permit.intent_id,
        executor_id=permit.executor_id,
        executor_incarnation=permit.executor_incarnation,
        command_sequence=3,
    )

    assert bootstrap.executable is permit.executable is consumption.executable is False
    for contract in (bootstrap, permit, consumption):
        assert not ({"expires_at", "received_at", "consumed_at"} & contract.model_fields_set)


def test_partial_release_is_canonical_unique_and_evidence_bound() -> None:
    first = ReleasedShapeV1(
        shape_instance_id="shape-0001",
        intent_id=UUID(int=11),
        inventory_sequence=9,
        terminal_kind="unused",
        terminal_identity="unused-shape-0001",
        terminal_evidence_sha256="a" * 64,
        protected_registration_epoch=1,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    second = ReleasedShapeV1(
        shape_instance_id="shape-0002",
        intent_id=UUID(int=12),
        inventory_sequence=10,
        terminal_kind="worker",
        terminal_identity="worker-0002",
        terminal_evidence_sha256="c" * 64,
        protected_registration_epoch=8,
        bootstrap_revoked=True,
        protected_release_sha256="d" * 64,
    )
    release = DryRunPartialReleaseV1(
        tranche_id=UUID(int=1),
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        command_sequence=4,
        releases=(second, first),
    )

    assert [item.shape_instance_id for item in release.releases] == [
        "shape-0001",
        "shape-0002",
    ]
    assert release.executable is False
    with pytest.raises(ValidationError, match="duplicate release shape"):
        DryRunPartialReleaseV1.model_validate(
            release.model_dump(mode="python") | {"releases": (first, first)}
        )
    duplicate_terminal = second.model_copy(
        update={
            "terminal_kind": first.terminal_kind,
            "terminal_identity": first.terminal_identity,
        }
    )
    with pytest.raises(ValidationError, match="duplicate terminal identity"):
        DryRunPartialReleaseV1.model_validate(
            release.model_dump(mode="python") | {"releases": (first, duplicate_terminal)}
        )


def test_protected_release_acknowledgement_is_subject_bound_and_non_executable() -> None:
    acknowledgement = DryRunProtectedReleaseAcknowledgementV1(
        authority_incarnation=UUID(int=90),
        writer_epoch=1,
        configuration_epoch=2,
        allocation_epoch=3,
        tranche_id=UUID(int=1),
        shape_instance_id="shape-0001",
        intent_id=UUID(int=11),
        subject_id=UUID(int=2),
        subject_incarnation=UUID(int=3),
        reporter_incarnation=UUID(int=4),
        deployment_generation=5,
        pool_id="gb10",
        pool_generation=1,
        bootstrap_registration_epoch=7,
        protected_registration_epoch=8,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )

    assert acknowledgement.executable is False
    with pytest.raises(ValidationError, match="advance past bootstrap"):
        DryRunProtectedReleaseAcknowledgementV1.model_validate(
            acknowledgement.model_dump(mode="python") | {"protected_registration_epoch": 7}
        )
    with pytest.raises(ValidationError):
        DryRunProtectedReleaseAcknowledgementV1.model_validate(
            acknowledgement.model_dump(mode="python") | {"executable": True}
        )


def test_intent_close_is_monotonic_exact_and_non_executable() -> None:
    close = DryRunIntentCloseV1(
        tranche_id=UUID(int=1),
        intent_id=UUID(int=11),
        executor_id="gb10-executor",
        executor_incarnation=UUID(int=4),
        command_sequence=2,
    )
    assert close.executable is False
    with pytest.raises(ValidationError):
        DryRunIntentCloseV1.model_validate(
            close.model_dump(mode="python") | {"command_sequence": 0}
        )
