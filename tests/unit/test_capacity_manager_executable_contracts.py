from __future__ import annotations

import base64
import importlib.util
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.contracts import MAX_QUANTITY, ResourceVectorV1
from loom_capacity_manager.grant_contracts import (
    DryRunReservationProposalV1,
    ReservationShapeV1,
)

_DEFAULT_INTENT_ID = UUID(int=20)


def _contracts():
    from loom_capacity_manager import executable_contracts

    return executable_contracts


def _authority(*, state: str = "active", ceiling: int = 2):
    return _contracts().ExecutionAuthorityV2(
        authority_incarnation=UUID(int=1),
        writer_epoch=2,
        configuration_epoch=3,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        execution_state=state,
        executable_new_capacity_ceiling=ceiling,
        executable_new_capacity_rate_per_minute=0 if state == "drain-only" else 1,
        trusted_fleet_release_sha256="d" * 64,
    )


def _fence():
    return _contracts().ExecutionFenceV2(
        **_authority().model_dump(mode="python"),
        allocation_epoch=5,
    )


def _shape(
    shape_instance_id: str = "shape-1",
    *,
    intent_id: UUID = _DEFAULT_INTENT_ID,
) -> ReservationShapeV1:
    return ReservationShapeV1(
        shape_instance_id=shape_instance_id,
        intent_id=intent_id,
        shape_id="one-slot",
        profile_id="profile-1",
        profile_generation=1,
        profile_digest="e" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=1_000,
            memory_bytes=1_073_741_824,
        ),
        node_ids=("node-1",),
    )


def _candidate():
    return _contracts().CandidateBindingV2(
        algorithm="source-sha256",
        identity="a" * 64,
        publication_sha256="b" * 64,
    )


def _execution_policy():
    contracts = _contracts()
    controller_digests = {"gb10": "c" * 64, "oldlab": "d" * 64}
    executor_incarnations = {"gb10": UUID(int=71), "oldlab": UUID(int=72)}
    return contracts.ExecutionPreparationPolicyV2(
        trusted_fleet_release_sha256="e" * 64,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        executors=tuple(
            contracts.PreparedExecutorBindingV2(
                pool_id=pool_id,
                pool_generation=1,
                executor_id=f"{pool_id}-executor",
                executor_incarnation=executor_incarnations[pool_id],
                signing_key_sha256=("a" if pool_id == "gb10" else "b") * 64,
                local_authority_sha256=("1" if pool_id == "gb10" else "2") * 64,
                controller_authority_sha256=controller_digests[pool_id],
            )
            for pool_id in ("gb10", "oldlab")
        ),
        subject_acknowledgements=(
            contracts.SubjectExecutionAcknowledgementV2(
                subject_id=UUID(int=81),
                subject_incarnation=UUID(int=82),
                configuration_generation=1,
                deployment_generation=1,
                candidate=_candidate(),
                reporter_incarnation=UUID(int=83),
                protected_admission_sha256="3" * 64,
                legacy_writer_high_water=0,
                acknowledgement_sha256="4" * 64,
            ),
        ),
        rollback_evidence_sha256="6" * 64,
        controller_authorities=tuple(
            contracts.PoolControllerAuthorityV2(
                pool_id=pool_id,
                controller_authority_sha256=controller_digests[pool_id],
            )
            for pool_id in ("gb10", "oldlab")
        ),
        legacy_writer_fences=(
            contracts.LegacyWriterFenceV2(
                writer_id="global-dev-supervisor",
                writer_kind="allocation",
                scope_kind="global",
                scope_id="development",
                high_water=9,
                freeze_evidence_sha256="5" * 64,
                state="frozen",
            ),
        ),
    )


def _retirement_checkpoint(pool_id: str):
    contracts = _contracts()
    suffix = 1 if pool_id == "gb10" else 2
    return contracts.ExecutionRetirementExecutorCheckpointV2(
        executor_id=f"{pool_id}-executor",
        executor_incarnation=UUID(int=100 + suffix),
        pool_id=pool_id,
        pool_generation=1,
        heartbeat_sequence=2,
        command_sequence=0,
        journal_sequence=0,
        journal_digest="0" * 64,
        inventory_sequence=1,
        inventory_digest=str(suffix) * 64,
    )


def _intent_binding():
    contracts = _contracts()
    return contracts.ExecutableIntentBindingV2(
        execution=_fence(),
        tranche_id=UUID(int=10),
        intent_id=UUID(int=20),
        shape_instance_id="shape-1",
        subject_id=UUID(int=11),
        subject_incarnation=UUID(int=12),
        account_id="owner-1",
        tier_id="development",
        candidate=_candidate(),
        candidate_generation=6,
        deployment_generation=7,
        pool_id="oldlab",
        pool_generation=8,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=13),
        shape_id="one-slot",
        profile_id="profile-1",
        profile_generation=1,
        profile_digest="e" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=1_000,
            memory_bytes=1_073_741_824,
        ),
        node_ids=("node-1",),
    )


def test_executable_contract_module_exists() -> None:
    """Removing the separately versioned executable protocol must fail."""

    assert importlib.util.find_spec("loom_capacity_manager.executable_contracts") is not None


def test_personal_candidate_identity_is_preserved_without_translation() -> None:
    """Changing source identities to a Git-shaped value must fail this test."""

    contracts = _contracts()
    assert hasattr(contracts, "CandidateBindingV2")
    binding = contracts.CandidateBindingV2(
        algorithm="source-sha256",
        identity="a" * 64,
        publication_sha256="b" * 64,
    )

    assert binding.identity == "a" * 64
    assert binding.model_dump(mode="json") == {
        "schema_version": 2,
        "algorithm": "source-sha256",
        "identity": "a" * 64,
        "publication_sha256": "b" * 64,
    }


@pytest.mark.parametrize(
    ("algorithm", "identity"),
    [
        ("git-sha1", "a" * 64),
        ("source-sha256", "a" * 40),
        ("git-sha1", "A" * 40),
        ("source-sha256", "g" * 64),
    ],
)
def test_candidate_algorithm_rejects_wrong_identity(
    algorithm: str,
    identity: str,
) -> None:
    """Removing algorithm-specific identity validation must fail this test."""

    contracts = _contracts()
    assert hasattr(contracts, "CandidateBindingV2")
    with pytest.raises(ValidationError, match="candidate identity"):
        contracts.CandidateBindingV2(
            algorithm=algorithm,
            identity=identity,
            publication_sha256="b" * 64,
        )


def test_execution_authority_distinguishes_active_from_drain_only() -> None:
    """Allowing zero-ceiling scale-up authority must fail this test."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutionAuthorityV2")
    common = {
        "authority_incarnation": UUID(int=1),
        "writer_epoch": 2,
        "configuration_epoch": 3,
        "execution_epoch": 4,
        "execution_manifest_sha256": "c" * 64,
        "trusted_fleet_release_sha256": "d" * 64,
    }
    active = contracts.ExecutionAuthorityV2(
        **common,
        execution_state="active",
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    drain = contracts.ExecutionAuthorityV2(
        **common,
        execution_state="drain-only",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
    )

    assert active.executable_new_capacity_ceiling == 1
    assert drain.executable_new_capacity_ceiling == 0
    with pytest.raises(ValidationError, match="positive ceiling"):
        contracts.ExecutionAuthorityV2(
            **common,
            execution_state="active",
            executable_new_capacity_ceiling=0,
            executable_new_capacity_rate_per_minute=1,
        )
    with pytest.raises(ValidationError):
        contracts.ExecutionAuthorityV2(
            **common,
            execution_state="active",
            executable_new_capacity_ceiling=MAX_QUANTITY + 1,
            executable_new_capacity_rate_per_minute=1,
        )


def test_execution_drain_contract_binds_the_exact_positive_active_envelope() -> None:
    """Dropping any active compare-and-set field must fail this drain request."""

    contracts = _contracts()
    request = contracts.ExecutionDrainV2(
        authority_incarnation=UUID(int=1),
        expected_writer_epoch=2,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        expected_executable_new_capacity_ceiling=2,
        expected_executable_new_capacity_rate_per_minute=1,
    )

    assert request.model_dump(mode="json") == {
        "schema_version": 2,
        "authority_incarnation": str(UUID(int=1)),
        "expected_writer_epoch": 2,
        "execution_epoch": 4,
        "execution_manifest_sha256": "c" * 64,
        "expected_executable_new_capacity_ceiling": 2,
        "expected_executable_new_capacity_rate_per_minute": 1,
        "executable": True,
    }
    for field in (
        "expected_writer_epoch",
        "execution_epoch",
        "expected_executable_new_capacity_ceiling",
        "expected_executable_new_capacity_rate_per_minute",
    ):
        with pytest.raises(ValidationError):
            contracts.ExecutionDrainV2.model_validate(
                request.model_dump(mode="python") | {field: 0}
            )
    with pytest.raises(ValidationError):
        contracts.ExecutionDrainV2.model_validate(
            request.model_dump(mode="python") | {"executable": False}
        )


def test_execution_retirement_contract_requires_canonical_distinct_pool_evidence() -> None:
    """Duplicate, missing, reordered, or cross-pool executor evidence must fail."""

    contracts = _contracts()
    gb10 = _retirement_checkpoint("gb10")
    oldlab = _retirement_checkpoint("oldlab")
    request = contracts.ExecutionRetirementV2(
        authority_incarnation=UUID(int=1),
        expected_writer_epoch=2,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        executor_checkpoints=(gb10, oldlab),
    )

    assert tuple(item.pool_id for item in request.executor_checkpoints) == (
        "gb10",
        "oldlab",
    )
    with pytest.raises(ValidationError, match="exactly gb10 and oldlab"):
        contracts.ExecutionRetirementV2.model_validate(
            request.model_dump(mode="python") | {"executor_checkpoints": (gb10, gb10.model_copy())}
        )
    with pytest.raises(ValidationError):
        contracts.ExecutionRetirementV2.model_validate(
            request.model_dump(mode="python") | {"executor_checkpoints": (gb10,)}
        )
    with pytest.raises(ValidationError, match="canonical pool order"):
        contracts.ExecutionRetirementV2.model_validate(
            request.model_dump(mode="python") | {"executor_checkpoints": (oldlab, gb10)}
        )
    with pytest.raises(ValidationError, match="distinct pool executors"):
        contracts.ExecutionRetirementV2.model_validate(
            request.model_dump(mode="python")
            | {
                "executor_checkpoints": (
                    gb10,
                    oldlab.model_copy(
                        update={
                            "executor_id": gb10.executor_id,
                            "executor_incarnation": gb10.executor_incarnation,
                        }
                    ),
                )
            }
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("heartbeat_sequence", 0, None),
        ("heartbeat_sequence", -1, None),
        ("command_sequence", -1, None),
        ("journal_sequence", -1, None),
        ("inventory_sequence", 0, None),
        ("inventory_sequence", -1, None),
        ("journal_digest", "f" * 64, "canonical zero digest"),
    ],
)
def test_execution_retirement_checkpoint_rejects_invalid_sequences_and_journal(
    field: str,
    value: object,
    message: str | None,
) -> None:
    """Weakening sequence or journal-head validation must fail final evidence."""

    contracts = _contracts()
    checkpoint = _retirement_checkpoint("oldlab")
    expectation = (
        pytest.raises(ValidationError, match=message) if message else pytest.raises(ValidationError)
    )
    with expectation:
        contracts.ExecutionRetirementExecutorCheckpointV2.model_validate(
            checkpoint.model_dump(mode="python") | {field: value}
        )


def test_owner_policy_accepts_finite_two_slot_ceiling() -> None:
    """Pinning the policy to one slot would reject an approved finite envelope."""

    payload = _execution_policy().model_dump(mode="python")
    payload["executable_new_capacity_ceiling"] = 2
    assert (
        _contracts()
        .ExecutionPreparationPolicyV2.model_validate(payload)
        .executable_new_capacity_ceiling
        == 2
    )


@pytest.mark.parametrize("ceiling", (0, -1, True, 1.0, MAX_QUANTITY + 1))
def test_owner_policy_rejects_nonpositive_or_nonstrict_finite_ceiling(
    ceiling: object,
) -> None:
    """Widening the owner envelope beyond strict positive quantities must fail."""

    payload = _execution_policy().model_dump(mode="python")
    payload["executable_new_capacity_ceiling"] = ceiling
    with pytest.raises(ValidationError):
        _contracts().ExecutionPreparationPolicyV2.model_validate(payload)


def test_prepared_execution_context_is_zero_ceiling_and_not_authority() -> None:
    """Treating prepared executor rehearsal as launch authority must fail."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutionContextV2")
    prepared = contracts.ExecutionContextV2(
        authority_incarnation=UUID(int=1),
        writer_epoch=2,
        configuration_epoch=3,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256="d" * 64,
    )

    assert prepared.execution_state == "prepared"
    with pytest.raises(ValidationError, match="prepared execution context"):
        contracts.ExecutionContextV2.model_validate(
            prepared.model_dump(mode="python") | {"executable_new_capacity_ceiling": 1}
        )
    with pytest.raises(ValidationError):
        contracts.ExecutionAuthorityV2.model_validate(prepared.model_dump(mode="python"))


def test_execution_fence_adds_exact_allocation_epoch() -> None:
    """Dropping the per-plan allocation fence must fail this test."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutionFenceV2")
    fence = contracts.ExecutionFenceV2(
        authority_incarnation=UUID(int=1),
        writer_epoch=2,
        configuration_epoch=3,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        execution_state="active",
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        trusted_fleet_release_sha256="d" * 64,
        allocation_epoch=5,
    )

    assert fence.allocation_epoch == 5
    assert fence.schema_version == 2


def test_executable_proposal_is_distinct_canonical_and_v1_incompatible() -> None:
    """Widening the dry-run proposal to accept execution must fail this test."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutableReservationProposalV2")
    proposal = contracts.ExecutableReservationProposalV2(
        tranche_id=UUID(int=10),
        execution=_fence(),
        subject_id=UUID(int=11),
        subject_incarnation=UUID(int=12),
        account_id="owner-1",
        tier_id="development",
        candidate=_candidate(),
        candidate_generation=6,
        deployment_generation=7,
        pool_id="oldlab",
        pool_generation=8,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=13),
        shapes=(_shape(),),
    )

    assert proposal.executable is True
    assert proposal.schema_version == 2
    assert proposal.execution.allocation_epoch == 5
    assert contracts.canonical_executable_digest(proposal) == (
        contracts.canonical_executable_digest(
            contracts.ExecutableReservationProposalV2.model_validate_json(
                proposal.model_dump_json()
            )
        )
    )
    with pytest.raises(ValidationError):
        DryRunReservationProposalV1.model_validate(proposal.model_dump(mode="python"))


def test_executable_canonical_encoding_rejects_v1_contract() -> None:
    """Digesting v1 with the executable encoder must fail protocol separation."""

    contracts = _contracts()
    with pytest.raises(ValueError, match="schema-v2"):
        contracts.canonical_executable_digest(
            DryRunReservationProposalV1(
                tranche_id=UUID(int=10),
                authority_incarnation=UUID(int=1),
                writer_epoch=2,
                configuration_epoch=3,
                allocation_epoch=5,
                subject_id=UUID(int=11),
                subject_incarnation=UUID(int=12),
                account_id="owner-1",
                tier_id="development",
                candidate_generation=6,
                deployment_generation=7,
                pool_id="oldlab",
                pool_generation=8,
                executor_id="oldlab-executor",
                executor_incarnation=UUID(int=13),
                shapes=(_shape(),),
            )
        )


def test_executable_proposal_rejects_duplicate_shape_and_intent() -> None:
    """Removing stable shape/intent uniqueness must fail this test."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutableReservationProposalV2")
    base = {
        "tranche_id": UUID(int=10),
        "execution": _fence(),
        "subject_id": UUID(int=11),
        "subject_incarnation": UUID(int=12),
        "account_id": "owner-1",
        "tier_id": "development",
        "candidate": _candidate(),
        "candidate_generation": 6,
        "deployment_generation": 7,
        "pool_id": "oldlab",
        "pool_generation": 8,
        "executor_id": "oldlab-executor",
        "executor_incarnation": UUID(int=13),
    }
    with pytest.raises(ValidationError, match="duplicate reservation shape"):
        contracts.ExecutableReservationProposalV2(
            **base,
            shapes=(_shape(), _shape()),
        )
    with pytest.raises(ValidationError, match="duplicate submission intent"):
        contracts.ExecutableReservationProposalV2(
            **base,
            shapes=(
                _shape("shape-1", intent_id=UUID(int=20)),
                _shape("shape-2", intent_id=UUID(int=20)),
            ),
        )


def test_intent_binding_rejects_resource_or_node_identity_drift() -> None:
    """Changing an approved shape while retaining its intent must fail."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutableIntentBindingV2")
    binding = _intent_binding()
    with pytest.raises(ValidationError, match="resource slots"):
        contracts.ExecutableIntentBindingV2.model_validate(
            binding.model_dump(mode="python")
            | {
                "resources": ResourceVectorV1(
                    slots=2,
                    cpu_millicores=1_000,
                    memory_bytes=1_073_741_824,
                )
            }
        )
    with pytest.raises(ValidationError, match="duplicate intent node"):
        contracts.ExecutableIntentBindingV2.model_validate(
            binding.model_dump(mode="python") | {"node_ids": ("node-1", "node-1")}
        )
    with pytest.raises(ValidationError, match="surge backing"):
        contracts.ExecutableIntentBindingV2.model_validate(
            binding.model_dump(mode="python") | {"rollout_surge_slots": 1}
        )


def test_executable_operation_family_is_v2_true_and_exactly_bound() -> None:
    """Omitting executable protocol types or their true marker must fail."""

    contracts = _contracts()
    required = (
        "ExecutableExecutorRegistrationV2",
        "ExecutableExecutorHeartbeatV2",
        "ExecutableExecutorInventoryV2",
        "ExecutableReservationAcceptanceV2",
        "ExecutableBootstrapRegistrationV2",
        "ExecutableLaunchPermitV2",
        "ExecutablePermitConsumptionV2",
        "ExecutableIntentCloseV2",
        "ExecutableProtectedReleaseV2",
        "ExecutableReleasedShapeV2",
        "ExecutablePartialReleaseV2",
    )
    assert all(hasattr(contracts, name) for name in required)
    binding = _intent_binding()
    context = contracts.ExecutionContextV2(
        **_authority(state="drain-only", ceiling=0).model_dump(
            mode="python", exclude={"executable"}
        )
    )
    registration = contracts.ExecutableExecutorRegistrationV2(
        execution=context,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        signing_key_id="oldlab-key-1",
        signing_key_sha256="1" * 64,
        local_authority_sha256="2" * 64,
        controller_authority_sha256="3" * 64,
    )
    heartbeat = contracts.ExecutableExecutorHeartbeatV2(
        execution=context,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        heartbeat_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    inventory = contracts.ExecutableExecutorInventoryV2(
        execution=context,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
        records=(),
    )
    acceptance = contracts.ExecutableReservationAcceptanceV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        proposal_digest="4" * 64,
        pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )
    bootstrap = contracts.ExecutableBootstrapRegistrationV2(
        binding=binding,
        command_sequence=2,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="5" * 64,
    )
    permit = contracts.ExecutableLaunchPermitV2(
        permit_id=UUID(int=30),
        binding=binding,
        permit_epoch=1,
        launch_rank=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    consumption = contracts.ExecutablePermitConsumptionV2(
        permit_id=permit.permit_id,
        permit_digest="6" * 64,
        binding=binding,
        command_sequence=3,
    )
    close = contracts.ExecutableIntentCloseV2(binding=binding, command_sequence=4)
    protected = contracts.ExecutableProtectedReleaseV2(
        binding=binding,
        reporter_incarnation=UUID(int=40),
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="7" * 64,
    )
    partial = contracts.ExecutablePartialReleaseV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=5,
        releases=(
            contracts.ExecutableReleasedShapeV2(
                binding=binding,
                inventory_sequence=1,
                terminal_kind="unused",
                terminal_identity="unused-shape-1",
                terminal_evidence_sha256="8" * 64,
                protected_registration_epoch=2,
                bootstrap_revoked=True,
                protected_release_sha256="7" * 64,
            ),
        ),
    )

    for value in (
        registration,
        heartbeat,
        inventory,
        acceptance,
        bootstrap,
        permit,
        consumption,
        close,
        protected,
        partial,
    ):
        assert value.schema_version == 2
        assert value.executable is True


def test_journal_permit_and_release_boundaries_fail_closed() -> None:
    """Weakening ordering, UTC, or release epochs must fail this test."""

    contracts = _contracts()
    binding = _intent_binding()
    with pytest.raises(ValidationError, match="canonical zero digest"):
        contracts.ExecutableExecutorHeartbeatV2(
            execution=_authority(state="drain-only", ceiling=0),
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=1,
            journal_digest="0" * 64,
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        contracts.ExecutableLaunchPermitV2(
            permit_id=UUID(int=30),
            binding=binding,
            permit_epoch=1,
            launch_rank=1,
            expires_at=datetime(2026, 8, 12, 12, 0),
        )
    with pytest.raises(ValidationError, match="advance past bootstrap"):
        contracts.ExecutableProtectedReleaseV2(
            binding=binding,
            reporter_incarnation=UUID(int=40),
            bootstrap_registration_epoch=2,
            protected_registration_epoch=2,
            bootstrap_revoked=True,
            protected_release_sha256="7" * 64,
        )


def test_executable_inventory_uses_tagged_candidate_ownership_proof() -> None:
    """Reusing dry-run ownership metadata must fail this executable boundary."""

    contracts = _contracts()
    assert hasattr(contracts, "ExecutableOwnershipMetadataV2")
    assert hasattr(contracts, "SignedExecutableOwnershipProofV2")
    assert hasattr(contracts, "ExecutableInventoryRecordV2")
    binding = _intent_binding()
    metadata = contracts.ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256="1" * 64,
        trusted_launcher_sha256="2" * 64,
        slurm_cluster="oldlab-cluster",
        submitter_identity="loom-oldlab",
        association="loom",
        submitted_at=datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
    )
    proof = contracts.SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id="oldlab-key-1",
        signature_base64=base64.b64encode(b"\0" * 64).decode("ascii"),
    )
    record = contracts.ExecutableInventoryRecordV2(
        physical_identity="job-101",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="active",
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="3" * 64,
        ownership_proof=proof,
    )
    inventory = contracts.ExecutableExecutorInventoryV2(
        execution=_authority(state="drain-only", ceiling=0),
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
        records=(record,),
    )

    assert inventory.records[0].ownership_proof.metadata.binding.candidate == _candidate()
    assert inventory.records[0].ownership_proof.metadata.binding.execution.execution_epoch == 4


def test_executable_inventory_rejects_foreign_proof_or_executor_drift() -> None:
    """Allowing copied ownership proof across pool authority must fail."""

    contracts = _contracts()
    binding = _intent_binding()
    metadata = contracts.ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256="1" * 64,
        trusted_launcher_sha256="2" * 64,
        slurm_cluster="oldlab-cluster",
        submitter_identity="loom-oldlab",
        association="loom",
        submitted_at=datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
    )
    proof = contracts.SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id="oldlab-key-1",
        signature_base64=base64.b64encode(b"\0" * 64).decode("ascii"),
    )
    with pytest.raises(ValidationError, match="foreign inventory"):
        contracts.ExecutableInventoryRecordV2(
            physical_identity="job-101",
            physical_kind="slurm-job",
            authority_scope="foreign",
            state="active",
            resources=binding.resources,
            node_ids=binding.node_ids,
            controller_evidence_sha256="3" * 64,
            ownership_proof=proof,
        )
    record = contracts.ExecutableInventoryRecordV2(
        physical_identity="job-101",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="active",
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="3" * 64,
        ownership_proof=proof,
    )
    with pytest.raises(ValidationError, match="another executor binding"):
        contracts.ExecutableExecutorInventoryV2(
            execution=_authority(state="drain-only", ceiling=0),
            executor_id="other-executor",
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(record,),
        )


def test_executable_inventory_rejects_authority_binding_drift() -> None:
    """Matching numeric epochs cannot hide a different manager authority."""

    contracts = _contracts()
    binding = _intent_binding()
    other_execution = binding.execution.model_copy(update={"authority_incarnation": UUID(int=999)})
    other_binding = binding.model_copy(update={"execution": other_execution})
    metadata = contracts.ExecutableOwnershipMetadataV2(
        binding=other_binding,
        controller_authority_sha256="1" * 64,
        trusted_launcher_sha256="2" * 64,
        slurm_cluster="oldlab-cluster",
        submitter_identity="loom-oldlab",
        association="loom",
        submitted_at=datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
    )
    proof = contracts.SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id="oldlab-key-1",
        signature_base64=base64.b64encode(b"\0" * 64).decode("ascii"),
    )
    record = contracts.ExecutableInventoryRecordV2(
        physical_identity="job-101",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="active",
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="3" * 64,
        ownership_proof=proof,
    )
    with pytest.raises(ValidationError, match="another executor binding"):
        contracts.ExecutableExecutorInventoryV2(
            execution=_authority(state="drain-only", ceiling=0),
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(record,),
        )
