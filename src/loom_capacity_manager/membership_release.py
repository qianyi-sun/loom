"""Read-only predecessor release evidence for personal membership.

This module never releases work. Admission callers must hold the shared authority
lock in the existing SERIALIZABLE write transaction; readers may only verify the
same facts from a consistent snapshot. A release digest alone grants no authority.
"""

from __future__ import annotations

import hashlib
import json

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import SubjectConfigurationV1
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    ExecutableProtectedReleaseV2,
    ExecutableTerminalInventoryEvidenceV2,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import (
    DryRunExecutorInventoryV1,
    DryRunPartialReleaseV1,
    DryRunProtectedReleaseAcknowledgementV1,
    ReleasedShapeV1,
    canonical_grant_digest,
)
from loom_capacity_manager.grant_store import CapacityGrantStore
from loom_capacity_manager.models import (
    CapacityExecutableIntent,
    CapacityExecutableProtectedReleaseReceipt,
    CapacityExecutableTerminalInventoryEvidence,
    CapacityExecutorObservation,
    CapacityObservedCommitment,
    CapacityProtectedReleaseAcknowledgement,
    CapacityReservationReleaseEvidence,
    CapacityReservationShape,
    CapacityReservationTranche,
    CapacitySubmissionIntent,
)
from loom_capacity_manager.store import ConfigurationConflictError


async def _accepted_release_witness(
    session: AsyncSession, intent: CapacityExecutableIntent, binding: ExecutableIntentBindingV2
) -> dict[str, object]:
    if intent.released_at is None:
        raise ConfigurationConflictError("predecessor release timestamp is absent")
    # A release permanently revokes this bootstrap. Pin the earliest exact receipt
    # so later monotonic release acknowledgements cannot change an old certificate.
    protected_row = (
        await session.execute(
            select(CapacityExecutableProtectedReleaseReceipt)
            .where(
                CapacityExecutableProtectedReleaseReceipt.intent_id == intent.intent_id,
            )
            .order_by(CapacityExecutableProtectedReleaseReceipt.protected_registration_epoch)
            .limit(1)
        )
    ).scalar_one_or_none()
    if protected_row is None:
        raise ConfigurationConflictError("predecessor protected release witness is absent")
    try:
        protected = ExecutableProtectedReleaseV2.model_validate_json(
            json.dumps(protected_row.release_payload)
        )
    except ValueError as exc:
        raise ConfigurationConflictError(
            "predecessor protected release witness is invalid"
        ) from exc
    if (
        protected.binding != binding
        or protected.bootstrap_registration_epoch != intent.bootstrap_registration_epoch
        or protected_row.execution_epoch != intent.execution_epoch
        or protected_row.execution_manifest_sha256 != intent.execution_manifest_sha256
        or any(
            getattr(protected, name) != getattr(protected_row, name)
            for name in (
                "reporter_incarnation",
                "bootstrap_registration_epoch",
                "protected_registration_epoch",
                "protected_release_sha256",
            )
        )
        or canonical_executable_digest(protected) != protected_row.acknowledgement_digest
        or intent.inventory_sequence is None
        or intent.terminal_evidence_sha256 is None
    ):
        raise ConfigurationConflictError("predecessor protected release witness changed")
    terminal_digest = intent.terminal_evidence_sha256
    if intent.terminal_kind == "unused":
        if (
            intent.permit_consumed_at is not None
            or intent.terminal_identity != intent.shape_instance_id
        ):
            raise ConfigurationConflictError("predecessor unused release witness changed")
    else:
        terminal_row = (
            await session.execute(
                select(CapacityExecutableTerminalInventoryEvidence).where(
                    CapacityExecutableTerminalInventoryEvidence.intent_id == intent.intent_id,
                )
            )
        ).scalar_one_or_none()
        if terminal_row is None:
            raise ConfigurationConflictError("predecessor terminal release witness is absent")
        try:
            terminal = ExecutableTerminalInventoryEvidenceV2.model_validate_json(
                json.dumps(terminal_row.evidence_payload)
            )
        except ValueError as exc:
            raise ConfigurationConflictError(
                "predecessor terminal release witness is invalid"
            ) from exc
        expected_scalars = {
            "subject_id": binding.subject_id,
            "subject_incarnation": binding.subject_incarnation,
            "execution_epoch": intent.execution_epoch,
            "execution_manifest_sha256": intent.execution_manifest_sha256,
            "executor_id": binding.executor_id,
            "executor_incarnation": binding.executor_incarnation,
            "pool_id": binding.pool_id,
            "pool_generation": binding.pool_generation,
            "inventory_sequence": terminal.inventory_sequence,
            "inventory_digest": terminal.inventory_digest,
            "journal_sequence": terminal.journal_sequence,
            "journal_digest": terminal.journal_digest,
            "physical_kind": terminal.record.physical_kind,
            "physical_identity": terminal.record.physical_identity,
            "controller_evidence_sha256": terminal.record.controller_evidence_sha256,
            "terminal_evidence_sha256": terminal.record.terminal_evidence_sha256,
            "observed_at": terminal.observed_at,
        }
        if (
            terminal.binding != binding
            or terminal.inventory_sequence != intent.inventory_sequence
            or terminal.record.physical_kind != intent.terminal_kind
            or terminal.record.physical_identity != intent.terminal_identity
            or terminal.record.terminal_evidence_sha256 != intent.terminal_evidence_sha256
            or any(getattr(terminal_row, name) != value for name, value in expected_scalars.items())
            or canonical_executable_digest(terminal) != terminal_row.evidence_digest
        ):
            raise ConfigurationConflictError("predecessor terminal release witness changed")
        terminal_digest = terminal_row.evidence_digest
    return {
        "kind": "accepted-executable",
        "intent_id": str(intent.intent_id),
        "binding_digest": intent.binding_digest,
        "protected_witness": protected_row.acknowledgement_digest,
        "terminal_witness": terminal_digest,
        "inventory_sequence": intent.inventory_sequence,
        "terminal_kind": intent.terminal_kind,
        "terminal_identity": intent.terminal_identity,
        "released_at": intent.released_at.isoformat(),
    }


async def _legacy_shape_witness(
    session: AsyncSession,
    tranche: CapacityReservationTranche,
    shape: CapacityReservationShape,
    intent: CapacitySubmissionIntent,
) -> dict[str, object]:
    error = "legacy predecessor release witness changed"
    if (
        shape.state != "released"
        or shape.released_at is None
        or intent.state != "closed"
        or intent.shape_instance_id != shape.shape_instance_id
        or intent.executor_id != tranche.executor_id
        or intent.executor_incarnation != tranche.executor_incarnation
        or intent.ownership_metadata_sha256
        != canonical_grant_digest(CapacityGrantStore._ownership_metadata(tranche, shape))
    ):
        raise ConfigurationConflictError(error)
    evidence = (
        await session.execute(
            select(CapacityReservationReleaseEvidence).where(
                CapacityReservationReleaseEvidence.shape_instance_id == shape.shape_instance_id
            )
        )
    ).scalar_one_or_none()
    protected = (
        await session.execute(
            select(CapacityProtectedReleaseAcknowledgement).where(
                CapacityProtectedReleaseAcknowledgement.shape_instance_id == shape.shape_instance_id
            )
        )
    ).scalar_one_or_none()
    if evidence is None or protected is None:
        raise ConfigurationConflictError("legacy predecessor release witness is absent")
    try:
        item = ReleasedShapeV1.model_validate(
            {
                name: getattr(evidence, name)
                for name in ReleasedShapeV1.model_fields
                if name != "schema_version"
            }
        )
        release = DryRunPartialReleaseV1(
            tranche_id=tranche.id,
            executor_id=tranche.executor_id,
            executor_incarnation=tranche.executor_incarnation,
            command_sequence=evidence.command_sequence,
            releases=(item,),
        )
        acknowledgement = DryRunProtectedReleaseAcknowledgementV1.model_validate(
            {
                name: getattr(protected, name)
                for name in DryRunProtectedReleaseAcknowledgementV1.model_fields
                if name != "schema_version"
            }
        )
    except ValueError as exc:
        raise ConfigurationConflictError(error) from exc
    digest = CapacityGrantStore._release_evidence_digest(release, item)
    if (
        not CapacityGrantStore._release_evidence_matches(evidence, release, item, digest)
        or item.intent_id != intent.id
        or shape.release_evidence_digest != digest
        or canonical_grant_digest(acknowledgement) != protected.acknowledgement_digest
        or any(
            getattr(protected, name) != getattr(tranche, name)
            for name in (
                "authority_incarnation",
                "writer_epoch",
                "configuration_epoch",
                "allocation_epoch",
                "subject_id",
                "subject_incarnation",
                "deployment_generation",
                "pool_id",
                "pool_generation",
            )
        )
        or protected.tranche_id != tranche.id
        or protected.intent_id != intent.id
        or protected.bootstrap_registration_epoch != (intent.bootstrap_registration_epoch or 0)
        or protected.protected_registration_epoch != item.protected_registration_epoch
        or protected.protected_release_sha256 != item.protected_release_sha256
    ):
        raise ConfigurationConflictError(error)
    observation = (
        await session.execute(
            select(CapacityExecutorObservation).where(
                CapacityExecutorObservation.executor_incarnation == tranche.executor_incarnation,
                CapacityExecutorObservation.inventory_sequence == item.inventory_sequence,
            )
        )
    ).scalar_one_or_none()
    if observation is None or observation.validity != "valid":
        raise ConfigurationConflictError("legacy predecessor inventory witness is absent")
    try:
        inventory = DryRunExecutorInventoryV1.model_validate_json(json.dumps(observation.payload))
    except ValueError as exc:
        raise ConfigurationConflictError(error) from exc
    if (
        canonical_grant_digest(inventory) != observation.inventory_digest
        or any(
            getattr(inventory, name) != getattr(observation, name)
            for name in (
                "executor_incarnation",
                "inventory_sequence",
                "pool_id",
                "pool_generation",
                "journal_sequence",
                "journal_digest",
            )
        )
        or inventory.executor_id != tranche.executor_id
        or inventory.pool_id != tranche.pool_id
        or inventory.pool_generation != tranche.pool_generation
    ):
        raise ConfigurationConflictError(error)
    if item.terminal_kind == "unused":
        if (
            item.terminal_identity != shape.shape_instance_id
            or item.terminal_evidence_sha256 != observation.inventory_digest
            or any(
                record.ownership_proof is not None
                and (
                    record.ownership_proof.metadata.intent_id == intent.id
                    or record.ownership_proof.metadata.shape_instance_id == shape.shape_instance_id
                )
                for record in inventory.records
            )
        ):
            raise ConfigurationConflictError(error)
    else:
        record = next(
            (
                record
                for record in inventory.records
                if record.physical_identity == item.terminal_identity
            ),
            None,
        )
        classification = next(
            (
                entry.get("classification")
                for entry in observation.classification_payload
                if entry.get("physical_identity") == item.terminal_identity
            ),
            None,
        )
        if (
            record is None
            or classification != "authenticated"
            or record.state != "terminal"
            or record.physical_kind != item.terminal_kind
            or record.terminal_evidence_sha256 != item.terminal_evidence_sha256
            or record.ownership_proof is None
            or record.ownership_proof.metadata
            != CapacityGrantStore._ownership_metadata(tranche, shape)
        ):
            raise ConfigurationConflictError(error)
    return {
        "shape_instance_id": shape.shape_instance_id,
        "intent_id": str(intent.id),
        "ownership_digest": intent.ownership_metadata_sha256,
        "release_digest": digest,
        "protected_digest": protected.acknowledgement_digest,
        "inventory_digest": observation.inventory_digest,
        "released_at": shape.released_at.isoformat(),
    }


async def _legacy_release_witness(
    session: AsyncSession, tranche: CapacityReservationTranche
) -> dict[str, object]:
    if tranche.state != "closed" or tranche.closed_at is None:
        raise ConfigurationConflictError("predecessor has unreleased legacy reservations")
    shapes = (
        await session.scalars(
            select(CapacityReservationShape)
            .where(CapacityReservationShape.tranche_id == tranche.id)
            .order_by(CapacityReservationShape.shape_instance_id)
        )
    ).all()
    intents = (
        await session.scalars(
            select(CapacitySubmissionIntent).where(
                CapacitySubmissionIntent.tranche_id == tranche.id
            )
        )
    ).all()
    witnesses = []
    if tranche.accepted_at is None:
        if (
            tranche.closure_reason not in {"proposal-expired", "proposal-superseded"}
            or shapes
            or intents
        ):
            raise ConfigurationConflictError("legacy predecessor unaccepted closure changed")
        kind = "never-accepted-legacy"
    else:
        by_id = {intent.id: intent for intent in intents}
        if (
            tranche.closure_reason != "fully-released"
            or not shapes
            or set(by_id) != {shape.intent_id for shape in shapes}
        ):
            raise ConfigurationConflictError("legacy predecessor released shape set changed")
        for shape in shapes:
            witnesses.append(
                await _legacy_shape_witness(session, tranche, shape, by_id[shape.intent_id])
            )
        kind = "accepted-legacy"
    return {
        "kind": kind,
        "tranche_id": str(tranche.id),
        "proposal_digest": tranche.proposal_digest,
        "closure_reason": tranche.closure_reason,
        "closed_at": tranche.closed_at.isoformat(),
        "shapes": witnesses,
    }


async def predecessor_release_sha256(
    session: AsyncSession, predecessor: SubjectConfigurationV1
) -> str:
    """Bind the exact identity's release facts across every execution epoch.

    Lifecycle/ownership and current disabled-generation checks belong to the
    admission caller. This function only verifies the retained capacity ledger.
    """

    identity = (predecessor.subject_id, predecessor.subject_incarnation)
    observed = (
        await session.execute(
            select(CapacityObservedCommitment.id)
            .where(
                or_(
                    and_(
                        CapacityObservedCommitment.subject_id == identity[0],
                        CapacityObservedCommitment.subject_incarnation == identity[1],
                    ),
                    and_(
                        CapacityObservedCommitment.binding_payload["observed_contract"][
                            "subject_id"
                        ].astext
                        == str(identity[0]),
                        CapacityObservedCommitment.binding_payload["observed_contract"][
                            "subject_incarnation"
                        ].astext
                        == str(identity[1]),
                    ),
                )
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if observed is not None:
        raise ConfigurationConflictError("predecessor has unreleased observed commitments")
    legacy = (
        await session.scalars(
            select(CapacityReservationTranche)
            .where(
                CapacityReservationTranche.subject_id == identity[0],
                CapacityReservationTranche.subject_incarnation == identity[1],
            )
            .order_by(CapacityReservationTranche.id)
        )
    ).all()
    witnesses = [await _legacy_release_witness(session, tranche) for tranche in legacy]
    intents = (
        (
            await session.execute(
                select(CapacityExecutableIntent)
                .where(
                    CapacityExecutableIntent.subject_id == identity[0],
                    CapacityExecutableIntent.subject_incarnation == identity[1],
                )
                .order_by(CapacityExecutableIntent.intent_id)
            )
        )
        .scalars()
        .all()
    )
    for intent in intents:
        if intent.state != "released" or intent.released_at is None:
            raise ConfigurationConflictError("predecessor has unreleased executable intents")
        binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(intent.binding_payload))
        if (
            (binding.subject_id, binding.subject_incarnation) != identity
            or binding.intent_id != intent.intent_id
            or binding.shape_instance_id != intent.shape_instance_id
            or binding.execution.execution_epoch != intent.execution_epoch
            or binding.execution.execution_manifest_sha256 != intent.execution_manifest_sha256
            or canonical_executable_digest(binding) != intent.binding_digest
        ):
            raise ConfigurationConflictError("predecessor release binding changed")
        if intent.accepted_at is not None:
            witnesses.append(await _accepted_release_witness(session, intent, binding))
            continue
        if any(
            value is not None
            for value in (
                intent.bootstrap_registration_epoch,
                intent.bootstrap_evidence_sha256,
                intent.permit_id,
                intent.permit_consumed_at,
                intent.inventory_sequence,
                intent.terminal_kind,
                intent.observed_state,
            )
        ):
            raise ConfigurationConflictError("predecessor unaccepted release witness changed")
        witnesses.append(
            {
                "kind": "never-accepted-executable",
                "intent_id": str(intent.intent_id),
                "binding_digest": intent.binding_digest,
                "released_at": intent.released_at.isoformat(),
            }
        )
    payload = {
        "schema_version": 1,
        "subject_id": str(identity[0]),
        "subject_incarnation": str(identity[1]),
        "witnesses": witnesses,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()
