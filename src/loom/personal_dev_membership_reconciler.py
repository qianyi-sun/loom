"""Lease-fenced membership retry protocol, separate from legacy epoch projection."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypeVar

from loom.personal_dev_capacity import (
    PersonalDevCapacityInstallation,
    personal_dev_capacity_projection,
    personal_dev_capacity_retirement_projection,
)
from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom.personal_dev_membership_admission import PersonalDevMembershipAdmissionError
from loom.personal_dev_membership_checkpoint import (
    PersonalDevMembershipEnvelopeV1,
    PersonalDevMembershipObservationV1,
    refresh_membership_checkpoint,
    validate_membership_outcome,
    validate_membership_release,
    validate_membership_response,
)
from loom.personal_dev_membership_cleanup import validated_membership_destroy
from loom.personal_dev_membership_client import (
    PersonalDevMembershipError,
    PersonalDevMembershipRevisionConflictError,
)
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipMutationV1,
    PersonalApplicationMembershipResponseV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_outcomes import (
    PersonalMembershipOperationCommittedV1,
    PersonalMembershipOperationOutcomeV1,
    PersonalMembershipOperationUnresolvedV1,
)
from loom_capacity_manager.membership_subject_status import (
    PersonalMembershipReleaseObservationV1,
    PersonalMembershipReleasePendingV1,
    PersonalMembershipSubjectQueryV1,
    PersonalMembershipSubjectStatusV1,
)

_T = TypeVar("_T")


class MembershipLeaseRunner(Protocol):
    def __call__(self, work: Awaitable[_T]) -> Awaitable[_T]: ...


class MembershipAdmissionGuard(Protocol):
    async def assert_admission_ready(self, *, now: datetime) -> None: ...


class MembershipReconciliationAuthority(Protocol):
    async def prepare_capacity_membership(self, **kwargs: object) -> object: ...

    async def complete_activation(self, **kwargs: object) -> object: ...

    async def record_capacity_membership(self, **kwargs: object) -> object: ...

    async def refresh_capacity_membership(self, **kwargs: object) -> object: ...

    async def record_capacity_membership_outcome(self, **kwargs: object) -> object: ...

    async def record_capacity_membership_release(self, **kwargs: object) -> object: ...

    async def advance_destroy_checkpoint(self, **kwargs: object) -> object: ...


class MembershipClient(Protocol):
    async def mutate_membership(
        self, envelope: PersonalDevMembershipEnvelopeV1
    ) -> PersonalApplicationMembershipResponseV1: ...

    async def membership_checkpoint(self) -> PersonalMembershipCheckpointV1: ...


class MembershipObserver(Protocol):
    """Current unbound read authority, independent of the original delegate."""

    async def membership_operation_outcome(
        self, envelope: PersonalDevMembershipEnvelopeV1
    ) -> PersonalMembershipOperationOutcomeV1: ...

    async def membership_subject_release(
        self, envelope: PersonalDevMembershipEnvelopeV1
    ) -> PersonalMembershipReleaseObservationV1: ...

    async def membership_subject_status(
        self, envelope: PersonalDevMembershipEnvelopeV1
    ) -> PersonalMembershipSubjectStatusV1: ...


class MembershipCleanupExecutor(Protocol):
    async def delete_namespace(self, claim: PersonalDevReconciliationClaim) -> None: ...

    async def delete_buckets(self, claim: PersonalDevReconciliationClaim) -> None: ...

    async def delete_tenant(self, claim: PersonalDevReconciliationClaim) -> None: ...

    async def delete_credentials(self, claim: PersonalDevReconciliationClaim) -> None: ...


class MembershipInstaller(Protocol):
    async def seal(self, claim: PersonalDevReconciliationClaim) -> None: ...

    async def destroy(self, claim: PersonalDevReconciliationClaim) -> None: ...

    def validate_membership_context(
        self,
        claim: PersonalDevReconciliationClaim,
        checkpoint: PersonalMembershipCheckpointV1,
        *,
        observed_at: datetime,
    ) -> None: ...

    async def converge(
        self, claim: PersonalDevReconciliationClaim
    ) -> PersonalDevCapacityInstallation: ...

    async def observe_membership(
        self,
        claim: PersonalDevReconciliationClaim,
        installation: PersonalDevCapacityInstallation,
        checkpoint: PersonalMembershipCheckpointV1,
        *,
        observed_at: datetime,
    ) -> PersonalDevMembershipObservationV1: ...

    async def observe_membership_retirement(
        self,
        claim: PersonalDevReconciliationClaim,
        checkpoint: PersonalMembershipCheckpointV1,
        *,
        observed_at: datetime,
    ) -> PersonalDevMembershipObservationV1: ...

    async def verify_membership_publishing(
        self, claim: PersonalDevReconciliationClaim, envelope: PersonalDevMembershipEnvelopeV1
    ) -> None: ...


@dataclass(slots=True)
class PersonalDevMembershipReconciler:
    authority: MembershipReconciliationAuthority
    client: MembershipClient
    installer: MembershipInstaller
    management_principal_id: str | None = None
    observer: MembershipObserver | None = None
    cleanup_executor: MembershipCleanupExecutor | None = None
    admission: MembershipAdmissionGuard | None = None

    async def _assert_admission(
        self, *, now: Callable[[], datetime], run: MembershipLeaseRunner
    ) -> None:
        await run(self.assert_admission_ready(now=now()))

    async def assert_admission_ready(self, *, now: datetime) -> None:
        """Guard candidate preparation as well as membership mutation/readiness."""
        if self.admission is None:
            raise PersonalDevMembershipAdmissionError("membership admission guard is not configured")
        await self.admission.assert_admission_ready(now=now)

    async def reconcile(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        lease: dict[str, object],
        now: Callable[[], datetime],
        run: MembershipLeaseRunner,
    ) -> None:
        """Advance one explicit membership checkpoint, never the shadow protocol."""

        operation = claim.operation
        if operation.capacity_mode != "membership-v1":
            raise ValueError("operation does not use membership")
        if operation.kind == "destroy" and operation.checkpoint in {
            "cleanup_pending", "membership_outcome_resolved", "release_verified",
            "local_authority_sealed", "namespace_deleted", "database_deleted",
            "buckets_deleted", "tenant_deleted",
        }:
            await self._reconcile_destroy(claim, lease=lease, now=now, run=run)
            return
        if operation.checkpoint == "capacity_projection_pending":
            await self.reconcile_pending(claim, lease=lease, now=now, run=run)
            return
        if operation.checkpoint == "capacity_projected" and operation.kind in {"create", "update"}:
            envelope = operation.capacity_membership_envelope
            if envelope is None or envelope.result is None:
                raise ValueError("membership activation has no accepted receipt")
            await self._assert_admission(now=now, run=run)
            await self.authority.complete_activation(**lease, now=now())
            return
        if operation.checkpoint not in {
            "activation_acknowledged",
            "capacity_projection_requested",
            "capacity_retirement_requested",
        }:
            raise ValueError("membership checkpoint requires an authenticated release transition")
        if (
            self.management_principal_id is None
            or operation.capacity_membership_envelope is not None
        ):
            raise ValueError("initial membership identity or request is invalid")
        if operation.kind != "destroy" and self.admission is None:
            raise PersonalDevMembershipAdmissionError("membership admission guard is not configured")
        checkpoint = await run(self.client.membership_checkpoint())
        if operation.kind != "destroy":
            await self._assert_admission(now=now, run=run)
        self.installer.validate_membership_context(claim, checkpoint, observed_at=now())
        if operation.kind == "destroy":
            observation = await run(
                self.installer.observe_membership_retirement(
                    claim,
                    checkpoint,
                    observed_at=now(),
                )
            )
            projection = personal_dev_capacity_retirement_projection(
                claim,
                expected_configuration_epoch=checkpoint.execution.configuration_epoch,
            )
        else:
            installation = await run(self.installer.converge(claim))
            observation = await run(
                self.installer.observe_membership(
                    claim,
                    installation,
                    checkpoint,
                    observed_at=now(),
                )
            )
            projection = personal_dev_capacity_projection(
                claim,
                installation,
                expected_configuration_epoch=checkpoint.execution.configuration_epoch,
            )
        request = PersonalApplicationMembershipMutationV1(
            execution=checkpoint.execution,
            namespace_id=checkpoint.namespace_id,
            expected_revision=checkpoint.revision,
            projection=projection,
            acknowledgement=observation.acknowledgement,
        )
        envelope = PersonalDevMembershipEnvelopeV1(
            management_principal_id=self.management_principal_id,
            idempotency_key=operation.idempotency_key,
            expected_checkpoint=checkpoint,
            request=request,
            request_sha256=canonical_digest(request),
            observation=observation,
        )
        await self.authority.prepare_capacity_membership(**lease, now=now(), envelope=envelope)

    async def reconcile_pending(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        lease: dict[str, object],
        now: Callable[[], datetime],
        run: MembershipLeaseRunner,
    ) -> None:
        """Send exactly the persisted observation; a new lease is not re-attestation."""

        operation = claim.operation
        if (
            operation.capacity_mode != "membership-v1"
            or operation.checkpoint != "capacity_projection_pending"
            or operation.capacity_membership_envelope is None
        ):
            raise ValueError("operation has no pending membership request")
        saved = PersonalDevMembershipEnvelopeV1.model_validate_json(
            canonical_bytes(operation.capacity_membership_envelope)
        )
        if saved.result is not None or saved.historical_outcome is not None:
            raise ValueError("pending membership already has a durable result")
        projection = saved.request.projection
        if (
            saved.idempotency_key != operation.idempotency_key
            or saved.observation.attempt_id != operation.attempt_id
            or saved.observation.operation_id != operation.id
            or saved.observation.operation_epoch != operation.operation_epoch
            or projection.operation_kind != operation.kind
            or projection.subject_id != operation.subject_id
            or projection.subject_incarnation != operation.subject_incarnation
            or projection.owner_id != operation.owner_user_id
            or projection.environment_name != operation.environment_name
            or projection.configuration_generation != operation.operation_epoch
            or projection.deployment_generation != operation.deployment_generation
            or projection.candidate_sha256 != operation.candidate_sha
            or projection.candidate_publication_sha256 != claim.candidate.publication_sha256
            or projection.local_activation_sha256 != operation.local_activation_sha256
            or (projection.min_slots, projection.max_slots)
            != (
                (0, 0)
                if operation.kind == "destroy"
                else (operation.min_slots, operation.max_slots)
            )
        ):
            raise ValueError("saved membership belongs to a different lifecycle operation")
        if operation.kind != "destroy":
            try:
                await self._assert_admission(now=now, run=run)
            except PersonalDevMembershipAdmissionError:
                if await self._recover_outcome(saved, lease=lease, now=now, run=run):
                    return
                raise
        try:
            response = await run(self.client.mutate_membership(saved))
        except PersonalDevMembershipRevisionConflictError:
            try:
                checkpoint = await run(self.client.membership_checkpoint())
            except PersonalDevMembershipError:
                if await self._recover_outcome(saved, lease=lease, now=now, run=run):
                    return
                raise
            # Check the authority before asking the durable store to refresh.
            # The store independently verifies these bindings under its lease.
            try:
                refresh_membership_checkpoint(saved, checkpoint)
            except ValueError:
                # An authority transition may race a genuine revision rejection.
                # It cannot authorize replacement of the unresolved old request.
                if (
                    checkpoint.execution != saved.expected_checkpoint.execution
                    or checkpoint.namespace_id != saved.expected_checkpoint.namespace_id
                ) and await self._recover_outcome(saved, lease=lease, now=now, run=run):
                    return
                raise
            await self.authority.refresh_capacity_membership(
                **lease, now=now(), checkpoint=checkpoint
            )
            return
        except PersonalDevMembershipError:
            if await self._recover_outcome(saved, lease=lease, now=now, run=run):
                return
            raise
        validate_membership_response(saved, response)
        if operation.kind != "destroy":
            await run(self.installer.verify_membership_publishing(claim, saved))
            # Capacity-only recording itself completes the operation. Network and
            # publication reads must not carry its readiness past authorization.
            # On expiry retain the original pending request for exact recovery.
            await self._assert_admission(now=now, run=run)
        await self.authority.record_capacity_membership(**lease, now=now(), response=response)

    async def _recover_outcome(
        self,
        saved: PersonalDevMembershipEnvelopeV1,
        *,
        lease: dict[str, object],
        now: Callable[[], datetime],
        run: MembershipLeaseRunner,
    ) -> bool:
        """Resolve immutable history, never infer absence or current readiness."""

        if self.observer is None:
            return False
        outcome = await run(self.observer.membership_operation_outcome(saved))
        validate_membership_outcome(saved, outcome)
        if isinstance(outcome, PersonalMembershipOperationUnresolvedV1):
            return False
        if isinstance(outcome, PersonalMembershipOperationCommittedV1):
            historical = PersonalDevMembershipEnvelopeV1.model_validate(
                saved.model_dump(mode="python") | {"historical_outcome": outcome}
            )
            observed = await run(self.observer.membership_subject_status(historical))
            status = PersonalMembershipSubjectStatusV1.model_validate_json(canonical_bytes(observed))
            if status.query_sha256 != canonical_digest(
                PersonalMembershipSubjectQueryV1(membership_receipt=outcome.receipt)
            ):
                raise ValueError("historical membership status identifies a different query")
            validate_membership_response(saved, status.membership_receipt)
            current, original = status.current, saved.request.execution
            if (
                current.authority_incarnation == original.authority_incarnation
                and current.writer_epoch == original.writer_epoch
                and current.execution_epoch == original.execution_epoch
                and current.execution_manifest_sha256 == original.execution_manifest_sha256
                and current.configuration_epoch == original.configuration_epoch
                and current.execution_state == original.execution_state
            ):
                # A lost reply in the same authority is ordinary exact replay,
                # even if other owners advanced the shared membership revision.
                return False
        await self.authority.record_capacity_membership_outcome(
            **lease, now=now(), outcome=outcome
        )
        return True

    async def _reconcile_destroy(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        lease: dict[str, object],
        now: Callable[[], datetime],
        run: MembershipLeaseRunner,
    ) -> None:
        checkpoint = claim.operation.checkpoint
        if checkpoint in {"cleanup_pending", "membership_outcome_resolved"}:
            saved = validated_membership_destroy(
                claim,
                checkpoints=("cleanup_pending", "membership_outcome_resolved"),
                require_release=False,
            )
            if self.observer is None:
                raise ValueError("membership cleanup requires a current authenticated observer")
            release = await run(self.observer.membership_subject_release(saved))
            if isinstance(release, PersonalMembershipReleasePendingV1):
                return
            validate_membership_release(saved, release)
            await self.authority.record_capacity_membership_release(
                **lease, now=now(), release=release
            )
            return
        validated_membership_destroy(
            claim,
            checkpoints=(
                "release_verified", "local_authority_sealed", "namespace_deleted",
                "database_deleted", "buckets_deleted", "tenant_deleted",
            ),
        )
        executor = self.cleanup_executor
        if executor is None:
            raise ValueError("membership cleanup executor is not configured")
        actions: dict[str, tuple[Callable[[], Awaitable[None]], str]] = {
            "release_verified": (lambda: self.installer.seal(claim), "local_authority_sealed"),
            "local_authority_sealed": (lambda: executor.delete_namespace(claim), "namespace_deleted"),
            "namespace_deleted": (
                (lambda: executor.delete_tenant(claim))
                if claim.operation.keep_data else (lambda: self.installer.destroy(claim)),
                "tenant_deleted" if claim.operation.keep_data else "database_deleted",
            ),
            "database_deleted": (lambda: executor.delete_buckets(claim), "buckets_deleted"),
            "buckets_deleted": (lambda: executor.delete_tenant(claim), "tenant_deleted"),
            "tenant_deleted": (lambda: executor.delete_credentials(claim), "complete"),
        }
        action, next_checkpoint = actions[checkpoint]
        await run(action())
        await self.authority.advance_destroy_checkpoint(
            **lease, now=now(), expected_checkpoint=checkpoint, checkpoint=next_checkpoint
        )
