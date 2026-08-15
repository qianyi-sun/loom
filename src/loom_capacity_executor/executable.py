"""Journal-first executable-v2 pool protocol driver and recovery reconciler."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import UUID, uuid5

from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    DrainedExecutableWorkerV2,
    ExecutableDrainRequestV2,
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
    ProtectedIntentObservationV2,
    RevokedExecutableBootstrapV2,
    WithdrawnExecutableWorkerV2,
)
from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffStore,
    bind_bootstrap_handoff_ownership,
)
from loom_capacity_executor.client import (
    AcceptedExecutableReservationReceiptV2,
    ClosingExecutableIntentReceiptV2,
    ConsumedExecutablePermitReceiptV2,
    ExecutableCheckpointReceiptV2,
    ExecutableInventoryReceiptV2,
    ExecutablePoolWorkV2,
    ExecutorRejectedError,
    RegisteredExecutableBootstrapReceiptV2,
    ReleasedExecutableShapesReceiptV2,
)
from loom_capacity_executor.journal import (
    ExecutorJournal,
    JournalRecord,
    JournalRegressionError,
)
from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    RenderedTrustedLaunchV2,
    TrustedLaunchContextV2,
    canonical_launch_policy_digest,
    executable_ownership_token,
    render_signed_launch,
)
from loom_capacity_executor.runtime_profiles import RuntimeAssemblyError, resolve_runtime_profile
from loom_capacity_executor.slurm_contracts import (
    SlurmAccountingHighWaterV2,
    SlurmAuthorityV2,
    SlurmCancelRequestV2,
    SlurmJobObservationV2,
    SlurmLaunchRequestV2,
    SlurmSubmissionV2,
    SlurmTerminalEvidenceV2,
)
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableInventoryRecordV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableReleasedShapeV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionFenceV2,
    PoolControllerAuthorityV2,
    SignedExecutableOwnershipProofV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.ownership import OwnershipKeyring, verify_executable_ownership

_OPERATION_NAMESPACE = UUID("cb359b0c-a844-4bc5-9592-a4c35e344f3d")
_RECOVERY_LOOKBACK = timedelta(days=8)


class _ManagerClient(Protocol):
    async def executable_checkpoint(self) -> ExecutableCheckpointReceiptV2: ...

    async def next_executable_work(self, command_sequence: int) -> ExecutablePoolWorkV2 | None: ...

    async def accept_executable_reservation(
        self, value: ExecutableReservationAcceptanceV2
    ) -> AcceptedExecutableReservationReceiptV2: ...

    async def register_executable_bootstrap(
        self, value: ExecutableBootstrapRegistrationV2
    ) -> RegisteredExecutableBootstrapReceiptV2: ...

    async def consume_executable_permit(
        self, value: ExecutablePermitConsumptionV2
    ) -> ConsumedExecutablePermitReceiptV2: ...

    async def close_executable_intent(
        self, value: ExecutableIntentCloseV2
    ) -> ClosingExecutableIntentReceiptV2: ...

    async def release_executable_shapes(
        self, value: ExecutablePartialReleaseV2
    ) -> ReleasedExecutableShapesReceiptV2: ...

    async def ingest_executable_inventory(
        self, value: ExecutableExecutorInventoryV2
    ) -> ExecutableInventoryReceiptV2: ...


class _AdmissionClient(Protocol):
    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> PreparedExecutableAdmissionV2: ...

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> BoundExecutableWorkerV2: ...

    async def observe_intent(
        self, binding: ExecutableIntentBindingV2
    ) -> ProtectedIntentObservationV2: ...

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> DrainedExecutableWorkerV2: ...

    async def withdraw_unregistered_worker(
        self, request: ExecutableWorkerWithdrawalRequestV2
    ) -> WithdrawnExecutableWorkerV2: ...

    async def revoke_prepared_bootstrap(
        self, request: ExecutablePreparedBootstrapRevocationV2
    ) -> RevokedExecutableBootstrapV2: ...


class _SlurmBackend(Protocol):
    async def submit(self, request: SlurmLaunchRequestV2) -> SlurmSubmissionV2: ...

    async def inventory(self) -> tuple[SlurmJobObservationV2, ...]: ...

    async def accounting_high_water(self, *, since: datetime) -> SlurmAccountingHighWaterV2: ...

    async def cancel_pending(self, request: SlurmCancelRequestV2) -> SlurmJobObservationV2: ...

    async def validate_authority(self) -> SlurmAuthorityV2: ...


if TYPE_CHECKING:
    from loom_capacity_executor.admission_client import DatabaseExecutableAdmissionClient
    from loom_capacity_executor.client import ExecutableCapacityExecutorClient
    from loom_capacity_executor.slurm_backend import AsyncSlurmBackend

    _manager_client_conformance: _ManagerClient = cast(
        "ExecutableCapacityExecutorClient",
        None,
    )
    _admission_client_conformance: _AdmissionClient = cast(
        "DatabaseExecutableAdmissionClient",
        None,
    )
    _slurm_backend_conformance: _SlurmBackend = cast("AsyncSlurmBackend", None)


@dataclass(frozen=True, slots=True)
class ExecutorTickResult:
    status: Literal[
        "accepted",
        "adopted",
        "bootstrap-registered",
        "draining",
        "idle",
        "inventory-published",
        "pending-cancelled",
        "permit-consumed",
        "quarantined",
        "released",
        "submitted",
    ]
    operation_id: UUID | None = None
    detail: str = ""


@dataclass(frozen=True, slots=True)
class _LaunchEnvelope:
    rendered: RenderedTrustedLaunchV2
    bootstrap_registration_epoch: int


@dataclass(frozen=True, slots=True)
class _ProtectedTerminalFence:
    binding: ExecutableIntentBindingV2
    receipt_kind: Literal["release", "withdrawal", "prepared-revocation"]
    protected_registration_epoch: int
    digest: str
    slurm_job_id: str | None = None


def _physical_binding_object_id(intent_id: UUID) -> str:
    return f"physical-bind:{intent_id}"


def _protected_terminal_digest(observation: ProtectedIntentObservationV2) -> str | None:
    receipts = tuple(
        digest
        for digest in (
            observation.release.protected_release_sha256 if observation.release else None,
            observation.withdrawal.withdrawal_digest if observation.withdrawal else None,
            observation.prepared_revocation.protected_release_sha256
            if observation.prepared_revocation
            else None,
        )
        if digest is not None
    )
    return receipts[0] if len(receipts) == 1 else None


def _protected_terminal_fence(
    observation: ProtectedIntentObservationV2,
) -> _ProtectedTerminalFence | None:
    if _protected_terminal_digest(observation) is None:
        return None
    if observation.release is not None:
        return _ProtectedTerminalFence(
            binding=observation.release.binding,
            receipt_kind="release",
            protected_registration_epoch=observation.release.protected_registration_epoch,
            digest=observation.release.protected_release_sha256,
        )
    if observation.withdrawal is not None:
        return _ProtectedTerminalFence(
            binding=observation.binding,
            receipt_kind="withdrawal",
            protected_registration_epoch=observation.withdrawal.protected_registration_epoch,
            digest=observation.withdrawal.withdrawal_digest,
            slurm_job_id=observation.withdrawal.slurm_job_id,
        )
    if observation.prepared_revocation is not None:
        return _ProtectedTerminalFence(
            binding=observation.prepared_revocation.binding,
            receipt_kind="prepared-revocation",
            protected_registration_epoch=(
                observation.prepared_revocation.protected_registration_epoch
            ),
            digest=observation.prepared_revocation.protected_release_sha256,
        )
    return None


class ExecutablePoolExecutor:
    """Apply at most one exact manager-authored executable-v2 operation per tick."""

    def __init__(
        self,
        registration: ExecutableExecutorRegistrationV2,
        journal: ExecutorJournal,
        client: _ManagerClient,
        admission: _AdmissionClient,
        slurm: _SlurmBackend,
        *,
        profile: OperatorLaunchProfileV2,
        controller_authority: PoolControllerAuthorityV2,
        ownership_key: ExecutorOwnershipKey,
        profiles: tuple[OperatorLaunchProfileV2, ...] | None = None,
        now: Callable[[], datetime] | None = None,
        bootstrap_digest: Callable[[ExecutableIntentBindingV2], str] | None = None,
        bootstrap_handoff_store: BootstrapHandoffStore | None = None,
    ) -> None:
        if not isinstance(registration, ExecutableExecutorRegistrationV2):
            raise TypeError("executable executor requires its exact registration")
        if not isinstance(journal, ExecutorJournal):
            raise TypeError("executable executor requires its locked journal")
        approved_profiles = profiles or (profile,)
        if not approved_profiles:
            raise ValueError("executable runtime requires at least one launch profile")
        if profile not in approved_profiles:
            raise ValueError("current executable profile is absent from approved profile set")
        if (
            registration.controller_authority_sha256
            != controller_authority.controller_authority_sha256
        ):
            raise ValueError("executor launch authority differs from registration")
        for approved_profile in approved_profiles:
            if (
                registration.pool_id != approved_profile.pool_id
                or registration.pool_generation != approved_profile.pool_generation
                or approved_profile.controller_authority_sha256
                != controller_authority.controller_authority_sha256
                or canonical_launch_policy_digest(approved_profile)
                != controller_authority.controller_authority_sha256
            ):
                raise ValueError("executor launch authority differs from approved profiles")
        if (
            registration.signing_key_id != ownership_key.signing_key_id
            or registration.signing_key_sha256 != ownership_key.public_key_sha256
        ):
            raise ValueError("executor ownership key differs from registration")
        self.registration = registration
        self.journal = journal
        self.client = client
        self.admission = admission
        self.slurm = slurm
        slurm_authority = getattr(slurm, "authority", None)
        self.expected_slurm_authority = (
            slurm_authority if isinstance(slurm_authority, SlurmAuthorityV2) else None
        )
        self.profile = profile
        self.profiles = tuple(approved_profiles)
        self.controller_authority = controller_authority
        self.ownership_key = ownership_key
        self._now = now or (lambda: datetime.now(UTC))
        self._bootstrap_digest = bootstrap_digest
        self._bootstrap_handoff_store = bootstrap_handoff_store
        if self._bootstrap_digest is None and self._bootstrap_handoff_store is None:
            raise ValueError("executable runtime requires a bootstrap handoff store")

    def _assert_binding(self, binding: ExecutableIntentBindingV2) -> None:
        if (
            binding.executor_id != self.registration.executor_id
            or binding.executor_incarnation != self.registration.executor_incarnation
            or binding.pool_id != self.registration.pool_id
            or binding.pool_generation != self.registration.pool_generation
        ):
            raise ValueError("manager work differs from exact executable binding")
        self._assert_execution(binding.execution)

    def _assert_execution(self, execution: ExecutionFenceV2) -> None:
        actual = execution.model_dump(exclude={"allocation_epoch", "executable"})
        expected = self.registration.execution.model_dump(exclude={"executable"})
        if actual != expected:
            raise ValueError("manager work differs from exact executable execution")

    def _profile_for(self, binding: ExecutableIntentBindingV2) -> OperatorLaunchProfileV2:
        return resolve_runtime_profile(
            binding,
            self.profiles,
            controller_authority_sha256=self.controller_authority.controller_authority_sha256,
        )

    async def _checkpoint(self) -> Any:
        checkpoint = await self.client.executable_checkpoint()
        self.journal.assert_covers(
            checkpoint.journal_sequence,
            checkpoint.journal_digest,
        )
        if self.journal.head.sequence < checkpoint.command_sequence:
            raise JournalRegressionError("local journal is behind central command high-water")
        return checkpoint

    @staticmethod
    def _event_object(value: StrictV2Model) -> tuple[str, str]:
        binding = (
            value
            if isinstance(value, ExecutableIntentBindingV2)
            else getattr(value, "binding", None)
        )
        if isinstance(binding, ExecutableIntentBindingV2):
            return "intent", str(binding.intent_id)
        if isinstance(value, ExecutableReservationAcceptanceV2):
            return "tranche", str(value.tranche_id)
        if isinstance(value, ExecutablePartialReleaseV2):
            return "tranche", str(value.tranche_id)
        raise ValueError("executable operation has no stable journal object")

    async def _central_command(
        self,
        value: StrictV2Model,
        *,
        event: str,
        operation: Callable[[], Any],
    ) -> Any:
        object_kind, object_id = self._event_object(value)
        payload = canonical_executable_bytes(value)
        digest = canonical_executable_digest(value)
        requested = f"{event}-requested"
        pending = self.journal.pending_requests()
        same = (
            len(pending) == 1
            and pending[0].event_kind == requested
            and pending[0].object_kind == object_kind
            and pending[0].object_id == object_id
            and pending[0].payload_digest == digest
        )
        if pending and not same:
            raise JournalRegressionError("another executable command remains unresolved")
        if not same:
            self.journal.append(
                requested,
                digest,
                object_kind=object_kind,
                object_id=object_id,
                payload=payload,
            )
        try:
            result = await operation()
        except ExecutorRejectedError:
            self.journal.append(
                f"{event}-rejected",
                digest,
                object_kind=object_kind,
                object_id=object_id,
                payload=payload,
            )
            raise
        self.journal.append(
            f"{event}-confirmed",
            digest,
            object_kind=object_kind,
            object_id=object_id,
            payload=payload,
        )
        return result

    def render_launch(self, binding: ExecutableIntentBindingV2) -> RenderedTrustedLaunchV2:
        self._assert_binding(binding)
        profile = self._profile_for(binding)
        rendered = render_signed_launch(
            TrustedLaunchContextV2(
                binding=binding,
                profile=profile,
                controller_authority=self.controller_authority,
                ownership_key=self.ownership_key,
                submitted_at=self._now(),
            )
        )
        reference = self._handoff_reference(binding)
        if reference is None:
            return rendered
        return RenderedTrustedLaunchV2(
            request=rendered.request.model_copy(update={"bootstrap_handoff_reference": reference}),
            ownership_proof=rendered.ownership_proof,
        )

    def _handoff_reference(self, binding: ExecutableIntentBindingV2) -> str | None:
        if self._bootstrap_handoff_store is None:
            return None
        return self._bootstrap_handoff_store.reference_for(binding)

    def _remember_launch(
        self,
        rendered: RenderedTrustedLaunchV2,
        *,
        bootstrap_registration_epoch: int,
        event: str,
    ) -> None:
        payload = json.dumps(
            {
                "bootstrap_registration_epoch": bootstrap_registration_epoch,
                "ownership_proof": rendered.ownership_proof.model_dump(
                    mode="json", exclude_none=False
                ),
                "request": rendered.request.model_dump(mode="json", exclude_none=False),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        self.journal.append(
            event,
            hashlib.sha256(payload).hexdigest(),
            object_kind="job",
            object_id=str(rendered.request.operation_id),
            payload=payload,
        )

    def _load_launch(self, intent_id: UUID) -> _LaunchEnvelope | None:
        record = self.journal.latest("job", str(intent_id))
        if record is None or record.event_kind not in {
            "slurm-submit-requested",
            "slurm-submit-unknown",
            "slurm-submit-confirmed",
            "physical-bind-confirmed",
        }:
            return None
        return self._launch_envelope_from_record(intent_id, record)

    def _launch_envelope_from_record(
        self,
        intent_id: UUID,
        record: JournalRecord,
    ) -> _LaunchEnvelope:
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("signed launch envelope is absent from journal")
        value = json.loads(payload.decode("ascii"))
        envelope = _LaunchEnvelope(
            rendered=RenderedTrustedLaunchV2(
                request=SlurmLaunchRequestV2.model_validate_json(json.dumps(value["request"])),
                ownership_proof=SignedExecutableOwnershipProofV2.model_validate_json(
                    json.dumps(value["ownership_proof"])
                ),
            ),
            bootstrap_registration_epoch=int(value["bootstrap_registration_epoch"]),
        )
        self._validate_stored_launch(intent_id, envelope)
        return envelope

    def _historical_launch_envelopes(self, intent_id: UUID) -> tuple[_LaunchEnvelope, ...]:
        envelopes: list[_LaunchEnvelope] = []
        for record in self.journal.records("job", str(intent_id)):
            if record.event_kind not in {
                "slurm-submit-requested",
                "slurm-submit-unknown",
                "slurm-submit-confirmed",
                "physical-bind-confirmed",
            }:
                continue
            envelopes.append(self._launch_envelope_from_record(intent_id, record))
        return tuple(envelopes)

    def _validate_stored_launch(self, intent_id: UUID, envelope: _LaunchEnvelope) -> None:
        rendered = envelope.rendered
        proof = rendered.ownership_proof
        request = rendered.request
        keyring = OwnershipKeyring(
            {self.ownership_key.signing_key_id: (self.ownership_key.private_key.public_key())}
        )
        if not verify_executable_ownership(
            proof,
            keyring=keyring,
            expected_public_key_sha256=self.registration.signing_key_sha256,
        ):
            raise JournalRegressionError("stored ownership proof is not authentic")
        try:
            self._assert_binding(proof.metadata.binding)
            expected = render_signed_launch(
                TrustedLaunchContextV2(
                    binding=proof.metadata.binding,
                    profile=self._profile_for(proof.metadata.binding),
                    controller_authority=self.controller_authority,
                    ownership_key=self.ownership_key,
                    submitted_at=proof.metadata.submitted_at,
                )
            )
            reference = self._handoff_reference(proof.metadata.binding)
            if reference is not None:
                expected = RenderedTrustedLaunchV2(
                    request=expected.request.model_copy(
                        update={"bootstrap_handoff_reference": reference}
                    ),
                    ownership_proof=expected.ownership_proof,
                )
        except (TypeError, ValueError, RuntimeAssemblyError) as exc:
            raise JournalRegressionError(
                "stored ownership evidence differs from launch authority"
            ) from exc
        if (
            request.operation_id != intent_id
            or proof.signing_key_id != self.registration.signing_key_id
            or proof.metadata.controller_authority_sha256
            != self.registration.controller_authority_sha256
            or executable_ownership_token(proof) != request.ownership_token
            or proof != expected.ownership_proof
            or request != expected.request
        ):
            raise JournalRegressionError("stored ownership request and proof are not coherent")

    def _bootstrap_registration(
        self,
        binding: ExecutableIntentBindingV2,
        *,
        command_sequence: int,
    ) -> ExecutableBootstrapRegistrationV2:
        latest = self.journal.latest("bootstrap", str(binding.intent_id))
        if latest is None:
            if self._bootstrap_handoff_store is not None:
                lease = self._bootstrap_handoff_store.prepare(
                    binding,
                    bootstrap_registration_epoch=1,
                    expires_at=self._now() + timedelta(minutes=30),
                    trusted_launcher_release_sha256=(
                        binding.execution.trusted_fleet_release_sha256
                    ),
                    protected_admission_route_sha256=self._handoff_route_sha256(binding),
                )
                bootstrap_evidence_sha256 = lease.bootstrap_sha256
            elif self._bootstrap_digest is not None:
                bootstrap_evidence_sha256 = self._bootstrap_digest(binding)
            else:
                raise JournalRegressionError("bootstrap handoff store is unavailable")
            return ExecutableBootstrapRegistrationV2(
                binding=binding,
                command_sequence=command_sequence,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=bootstrap_evidence_sha256,
            )
        if latest.event_kind not in {
            "protected-bootstrap-requested",
            "protected-bootstrap-confirmed",
        }:
            raise JournalRegressionError("protected bootstrap journal state is invalid")
        payload = latest.durable_payload()
        if payload is None:
            raise JournalRegressionError("protected bootstrap request is absent from journal")
        registration = ExecutableBootstrapRegistrationV2.model_validate_json(payload)
        if registration.binding != binding or registration.command_sequence != command_sequence:
            raise JournalRegressionError("protected bootstrap request binding changed")
        return registration

    def _handoff_route_sha256(self, binding: ExecutableIntentBindingV2) -> str:
        route = getattr(self.admission, "bootstrap_handoff_route_sha256", None)
        if not callable(route):
            raise JournalRegressionError("bootstrap handoff admission route is unavailable")
        value = route(binding)
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(item not in "0123456789abcdef" for item in value)
        ):
            raise JournalRegressionError("bootstrap handoff admission route is invalid")
        return value

    async def tick(self) -> ExecutorTickResult:
        checkpoint = await self._checkpoint()
        replayed_local = await self._replay_local_request(checkpoint)
        if replayed_local is not None:
            return replayed_local
        replayed = await self._replay_central_request(checkpoint)
        if replayed is not None:
            return replayed
        work = await self.client.next_executable_work(checkpoint.command_sequence)
        if work is None:
            return await self._publish_inventory(checkpoint)
        return await self._apply_one(work, checkpoint)

    async def tick_drain_only(self) -> ExecutorTickResult:
        """Apply one structurally drain-only operation without new capacity work."""

        checkpoint = await self._checkpoint()
        replayed_local = await self._replay_local_request(checkpoint)
        if replayed_local is not None:
            return replayed_local
        if self._has_recovering_launch():
            return await self.recover()
        replayed = await self._replay_drain_only_central_request(checkpoint)
        if replayed is not None:
            return replayed
        work = await self.client.next_executable_work(checkpoint.command_sequence)
        if work is None:
            return await self._publish_inventory(checkpoint)
        if isinstance(work, (ExecutableIntentCloseV2, ExecutablePartialReleaseV2)):
            return await self._apply_one(work, checkpoint)
        raise ValueError("drain-only executor rejected new capacity work")

    def _has_recovering_launch(self) -> bool:
        return any(
            record.event_kind
            in {
                "slurm-submit-requested",
                "slurm-submit-unknown",
                "slurm-submit-confirmed",
                "physical-bind-requested",
            }
            for record in (
                *self.journal.latest_records("job"),
                *self.journal.latest_records("intent"),
            )
        )

    def _validate_central_replay(
        self,
        record: JournalRecord,
        value: StrictV2Model,
        checkpoint: ExecutableCheckpointReceiptV2,
    ) -> None:
        if self._event_object(value) != (record.object_kind, record.object_id):
            raise JournalRegressionError("central request object binding changed")
        command_sequence = getattr(value, "command_sequence", None)
        if type(command_sequence) is not int or checkpoint.command_sequence not in {
            command_sequence - 1,
            command_sequence,
        }:
            raise JournalRegressionError("central request command sequence changed")
        if isinstance(value, ExecutableReservationAcceptanceV2):
            self._assert_execution(value.execution)
            if (
                value.executor_id != self.registration.executor_id
                or value.executor_incarnation != self.registration.executor_incarnation
                or value.pool_generation != self.registration.pool_generation
            ):
                raise JournalRegressionError("central acceptance binding changed")
            return
        if isinstance(
            value,
            (
                ExecutableBootstrapRegistrationV2,
                ExecutablePermitConsumptionV2,
                ExecutableIntentCloseV2,
            ),
        ):
            self._assert_binding(value.binding)
            return
        if isinstance(value, ExecutablePartialReleaseV2):
            self._assert_execution(value.execution)
            if (
                value.executor_id != self.registration.executor_id
                or value.executor_incarnation != self.registration.executor_incarnation
            ):
                raise JournalRegressionError("central release binding changed")
            for release in value.releases:
                self._assert_binding(release.binding)
            return
        raise JournalRegressionError("central request contract is unsupported")

    async def _replay_central_request(
        self, checkpoint: ExecutableCheckpointReceiptV2
    ) -> ExecutorTickResult | None:
        central_events = {
            "reservation-accept-requested",
            "bootstrap-register-requested",
            "permit-consume-requested",
            "intent-close-requested",
            "reservation-release-requested",
        }
        records = tuple(
            record
            for record in self.journal.pending_requests()
            if record.event_kind in central_events
        )
        if not records:
            return None
        if len(records) != 1:
            raise JournalRegressionError("multiple central commands remain unresolved")
        record = records[0]
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("central request is absent from journal")
        if record.event_kind == "reservation-accept-requested":
            acceptance = ExecutableReservationAcceptanceV2.model_validate_json(payload)
            self._validate_central_replay(record, acceptance, checkpoint)
            await self._central_command(
                acceptance,
                event="reservation-accept",
                operation=lambda: self.client.accept_executable_reservation(acceptance),
            )
            return ExecutorTickResult("accepted")
        if record.event_kind == "bootstrap-register-requested":
            registration = ExecutableBootstrapRegistrationV2.model_validate_json(payload)
            self._validate_central_replay(record, registration, checkpoint)
            await self._central_command(
                registration,
                event="bootstrap-register",
                operation=lambda: self.client.register_executable_bootstrap(registration),
            )
            return ExecutorTickResult(
                "bootstrap-registered",
                registration.binding.intent_id,
                str(registration.bootstrap_registration_epoch),
            )
        if record.event_kind == "permit-consume-requested":
            consumption = ExecutablePermitConsumptionV2.model_validate_json(payload)
            self._validate_central_replay(record, consumption, checkpoint)
            await self._central_command(
                consumption,
                event="permit-consume",
                operation=lambda: self.client.consume_executable_permit(consumption),
            )
            return ExecutorTickResult("permit-consumed", consumption.binding.intent_id)
        if record.event_kind == "intent-close-requested":
            close = ExecutableIntentCloseV2.model_validate_json(payload)
            self._validate_central_replay(record, close, checkpoint)
            await self._central_command(
                close,
                event="intent-close",
                operation=lambda: self.client.close_executable_intent(close),
            )
            return ExecutorTickResult("draining", close.binding.intent_id)
        release = ExecutablePartialReleaseV2.model_validate_json(payload)
        self._validate_central_replay(record, release, checkpoint)
        return await self._release(release)

    async def _replay_drain_only_central_request(
        self,
        checkpoint: ExecutableCheckpointReceiptV2,
    ) -> ExecutorTickResult | None:
        records = tuple(
            record
            for record in self.journal.pending_requests()
            if record.event_kind
            in {
                "reservation-accept-requested",
                "bootstrap-register-requested",
                "permit-consume-requested",
                "intent-close-requested",
                "reservation-release-requested",
            }
        )
        if not records:
            return None
        if len(records) != 1:
            raise JournalRegressionError("multiple central commands remain unresolved")
        if records[0].event_kind not in {
            "intent-close-requested",
            "reservation-release-requested",
        }:
            raise JournalRegressionError(
                "drain-only executor cannot replay capacity-increasing central work"
            )
        return await self._replay_central_request(checkpoint)

    def _validate_cancel_replay(
        self,
        record: JournalRecord,
        cancel: SlurmCancelRequestV2,
    ) -> None:
        if (
            record.object_kind != "job"
            or record.object_id != cancel.job_id
            or record.payload_digest
            != hashlib.sha256(
                json.dumps(
                    cancel.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("ascii")
            ).hexdigest()
        ):
            raise JournalRegressionError("pending cancellation request binding changed")

    @staticmethod
    def _cancel_matches_job(
        cancel: SlurmCancelRequestV2,
        job: SlurmJobObservationV2,
    ) -> bool:
        return (
            job.cluster == cancel.cluster
            and job.job_id == cancel.job_id
            and job.submitter == cancel.submitter
            and job.account == cancel.account
            and job.partition == cancel.partition
            and job.cpus == cancel.cpus
            and job.memory_bytes == cancel.memory_bytes
            and job.gpus == cancel.gpus
            and job.generic_tres == cancel.generic_tres
            and job.nodes == cancel.nodes
            and job.ownership_token == cancel.ownership_token
        )

    @staticmethod
    def _cancel_matches_terminal(
        cancel: SlurmCancelRequestV2,
        terminal: SlurmTerminalEvidenceV2,
    ) -> bool:
        return (
            terminal.cluster == cancel.cluster
            and terminal.job_id == cancel.job_id
            and terminal.submitter == cancel.submitter
            and terminal.account == cancel.account
            and terminal.partition == cancel.partition
            and terminal.cpus == cancel.cpus
            and terminal.memory_bytes == cancel.memory_bytes
            and terminal.gpus == cancel.gpus
            and terminal.generic_tres == cancel.generic_tres
            and terminal.nodes == cancel.nodes
            and terminal.ownership_token == cancel.ownership_token
        )

    async def _recover_pending_cancel(
        self,
        record: JournalRecord,
        cancel: SlurmCancelRequestV2,
        checkpoint: ExecutableCheckpointReceiptV2,
    ) -> ExecutorTickResult:
        self._validate_cancel_replay(record, cancel)
        jobs = await self.slurm.inventory()
        same_job_live = tuple(job for job in jobs if job.job_id == cancel.job_id)
        live_matches = tuple(job for job in same_job_live if self._cancel_matches_job(cancel, job))
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("pending cancellation request is absent from journal")
        if any(not self._cancel_matches_job(cancel, job) for job in same_job_live):
            self.journal.append(
                "pending-cancel-ambiguous-quarantined",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
            return ExecutorTickResult(
                "quarantined",
                detail="pending cancellation recovery found conflicting live scheduler state",
            )
        if len(live_matches) == 1 and live_matches[0].state == "PENDING":
            await self.slurm.cancel_pending(cancel)
            self.journal.append(
                "pending-cancel-confirmed-cancelled",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            return ExecutorTickResult("pending-cancelled")
        if len(live_matches) == 1 and live_matches[0].state in {
            "CONFIGURING",
            "RUNNING",
            "COMPLETING",
            "SUSPENDED",
        }:
            self.journal.append(
                "pending-cancel-running-drain-only",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            return ExecutorTickResult("draining")
        high_water = await self.slurm.accounting_high_water(since=self._now() - _RECOVERY_LOOKBACK)
        terminal_matches = tuple(
            item for item in high_water.terminal_jobs if self._cancel_matches_terminal(cancel, item)
        )
        if same_job_live and terminal_matches:
            self.journal.append(
                "pending-cancel-ambiguous-quarantined",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
            return ExecutorTickResult(
                "quarantined",
                detail="pending cancellation recovery found live and terminal scheduler state",
            )
        if not live_matches and len(terminal_matches) == 1:
            self.journal.append(
                "pending-cancel-already-terminal",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            return ExecutorTickResult("pending-cancelled")
        self.journal.append(
            "pending-cancel-ambiguous-quarantined",
            record.payload_digest,
            object_kind=record.object_kind,
            object_id=record.object_id,
            payload=payload,
        )
        await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
        return ExecutorTickResult(
            "quarantined",
            detail="pending cancellation recovery could not prove exact scheduler outcome",
        )

    async def _replay_local_request(
        self, checkpoint: ExecutableCheckpointReceiptV2
    ) -> ExecutorTickResult | None:
        for record in self.journal.latest_records("prepared-revocation"):
            if record.event_kind != "protected-prepared-revocation-confirmed":
                continue
            payload = record.durable_payload()
            if payload is None:
                raise JournalRegressionError(
                    "prepared bootstrap revocation request is absent from journal"
                )
            if self._delete_prepared_handoff(record, payload):
                revocation = ExecutablePreparedBootstrapRevocationV2.model_validate_json(payload)
                return ExecutorTickResult("draining", revocation.binding.intent_id)
        local_events = {
            "protected-drain-requested",
            "protected-withdraw-requested",
            "protected-prepared-revocation-requested",
            "pending-cancel-requested",
        }
        records = tuple(
            record
            for record in self.journal.pending_requests()
            if record.event_kind in local_events
        )
        if not records:
            return None
        if len(records) != 1:
            raise JournalRegressionError("multiple local executable requests remain unresolved")
        record = records[0]
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("local executable request is absent from journal")
        if record.event_kind == "protected-drain-requested":
            drain = ExecutableDrainRequestV2.model_validate_json(payload)
            if record.object_kind != "intent" or record.object_id != str(drain.binding.intent_id):
                raise JournalRegressionError("protected drain request object binding changed")
            self._assert_binding(drain.binding)
            await self.admission.begin_drain(drain)
            self.journal.append(
                "protected-drain-confirmed",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            return ExecutorTickResult("draining", drain.binding.intent_id)
        if record.event_kind == "protected-withdraw-requested":
            withdrawal = ExecutableWorkerWithdrawalRequestV2.model_validate_json(payload)
            if record.object_kind != "intent" or record.object_id != str(
                withdrawal.binding.intent_id
            ):
                raise JournalRegressionError("protected withdrawal request object binding changed")
            self._assert_binding(withdrawal.binding)
            await self.admission.withdraw_unregistered_worker(withdrawal)
            self.journal.append(
                "protected-withdraw-confirmed",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            return ExecutorTickResult("quarantined", withdrawal.binding.intent_id)
        if record.event_kind == "protected-prepared-revocation-requested":
            revocation = self._prepared_revocation_from_record(record, payload)
            await self.admission.revoke_prepared_bootstrap(revocation)
            self.journal.append(
                "protected-prepared-revocation-confirmed",
                record.payload_digest,
                object_kind=record.object_kind,
                object_id=record.object_id,
                payload=payload,
            )
            confirmed = self.journal.latest(record.object_kind, record.object_id)
            if confirmed is None:
                raise JournalRegressionError("prepared bootstrap revocation confirmation is absent")
            self._delete_prepared_handoff(confirmed, payload)
            return ExecutorTickResult("draining", revocation.binding.intent_id)
        cancel = SlurmCancelRequestV2.model_validate_json(payload)
        return await self._recover_pending_cancel(record, cancel, checkpoint)

    async def _apply_one(
        self,
        work: ExecutablePoolWorkV2,
        checkpoint: ExecutableCheckpointReceiptV2,
    ) -> ExecutorTickResult:
        if isinstance(work, ExecutableReservationProposalV2):
            acceptance = ExecutableReservationAcceptanceV2(
                execution=work.execution,
                tranche_id=work.tranche_id,
                proposal_digest=canonical_executable_digest(work),
                pool_generation=work.pool_generation,
                executor_id=work.executor_id,
                executor_incarnation=work.executor_incarnation,
                command_sequence=checkpoint.command_sequence + 1,
            )
            await self._central_command(
                acceptance,
                event="reservation-accept",
                operation=lambda: self.client.accept_executable_reservation(acceptance),
            )
            return ExecutorTickResult("accepted")
        if isinstance(work, ExecutableIntentBindingV2):
            self._assert_binding(work)
            registration = self._bootstrap_registration(
                work,
                command_sequence=checkpoint.command_sequence + 1,
            )
            registration_payload = canonical_executable_bytes(registration)
            if self.journal.latest("bootstrap", str(work.intent_id)) is None:
                self.journal.append(
                    "protected-bootstrap-requested",
                    canonical_executable_digest(registration),
                    object_kind="bootstrap",
                    object_id=str(work.intent_id),
                    payload=registration_payload,
                )
            protected = await self.admission.prepare_worker(
                registration,
                bootstrap_sha256=registration.bootstrap_evidence_sha256,
            )
            self.journal.append(
                "protected-bootstrap-confirmed",
                canonical_executable_digest(registration),
                object_kind="bootstrap",
                object_id=str(work.intent_id),
                payload=registration_payload,
            )
            await self._central_command(
                registration,
                event="bootstrap-register",
                operation=lambda: self.client.register_executable_bootstrap(registration),
            )
            return ExecutorTickResult(
                "bootstrap-registered",
                work.intent_id,
                str(protected.bootstrap_registration_epoch),
            )
        if isinstance(work, ExecutableLaunchPermitV2):
            self._assert_binding(work.binding)
            envelope = self._load_launch(work.binding.intent_id)
            if envelope is not None:
                return await self.recover()
            observation = await self.admission.observe_intent(work.binding)
            bootstrap_epoch = max(1, observation.bootstrap_registration_epoch)
            consumption = ExecutablePermitConsumptionV2(
                permit_id=work.permit_id,
                permit_digest=canonical_executable_digest(work),
                binding=work.binding,
                command_sequence=checkpoint.command_sequence + 1,
            )
            await self._central_command(
                consumption,
                event="permit-consume",
                operation=lambda: self.client.consume_executable_permit(consumption),
            )
            rendered = self.render_launch(work.binding)
            if self._bootstrap_handoff_store is not None:
                bind_bootstrap_handoff_ownership(
                    self._bootstrap_handoff_store.directory,
                    self._bootstrap_handoff_store.reference_for(work.binding),
                    work.binding,
                    bootstrap_registration_epoch=bootstrap_epoch,
                    ownership_evidence_sha256=canonical_executable_digest(rendered.ownership_proof),
                    trusted_launcher_release_sha256=(
                        work.binding.execution.trusted_fleet_release_sha256
                    ),
                    now=self._now,
                )
            self._remember_launch(
                rendered,
                bootstrap_registration_epoch=bootstrap_epoch,
                event="slurm-submit-requested",
            )
            try:
                submission = await self.slurm.submit(rendered.request)
            except BaseException:
                self._remember_launch(
                    rendered,
                    bootstrap_registration_epoch=bootstrap_epoch,
                    event="slurm-submit-unknown",
                )
                raise
            self._remember_launch(
                rendered,
                bootstrap_registration_epoch=bootstrap_epoch,
                event="slurm-submit-confirmed",
            )
            await self._bind_physical(
                envelope=_LaunchEnvelope(rendered, bootstrap_epoch), job_id=submission.job_id
            )
            return ExecutorTickResult("submitted", work.binding.intent_id, submission.job_id)
        if isinstance(work, ExecutableIntentCloseV2):
            self._assert_binding(work.binding)
            return await self._close(work, checkpoint)
        if isinstance(work, ExecutablePartialReleaseV2):
            return await self._release(work)
        raise TypeError("capacity manager returned an unsupported executable operation")

    async def _bind_physical(self, *, envelope: _LaunchEnvelope, job_id: str) -> None:
        request = PhysicalJobBindingV2(
            operation_id=uuid5(
                _OPERATION_NAMESPACE,
                f"physical-bind:{envelope.rendered.ownership_proof.metadata.binding.intent_id}",
            ),
            binding=envelope.rendered.ownership_proof.metadata.binding,
            bootstrap_registration_epoch=envelope.bootstrap_registration_epoch,
            slurm_job_id=job_id,
            ownership_evidence_sha256=canonical_executable_digest(
                envelope.rendered.ownership_proof
            ),
        )
        payload = canonical_executable_bytes(request)
        digest = canonical_executable_digest(request)
        dedicated = self.journal.latest(
            "executor",
            _physical_binding_object_id(request.binding.intent_id),
        )
        if dedicated is not None and dedicated.event_kind == "physical-bind-confirmed":
            retained = self._validate_physical_binding_record(dedicated, envelope)
            if retained != request or dedicated.payload_digest != digest:
                raise JournalRegressionError("physical binding request changed during recovery")
            latest_intent = self.journal.latest("intent", str(request.binding.intent_id))
            if latest_intent is not None and latest_intent.event_kind == "physical-bind-requested":
                self.journal.append(
                    "physical-bind-confirmed",
                    digest,
                    object_kind="intent",
                    object_id=str(request.binding.intent_id),
                    payload=payload,
                )
            self._remember_launch(
                envelope.rendered,
                bootstrap_registration_epoch=envelope.bootstrap_registration_epoch,
                event="physical-bind-confirmed",
            )
            return
        latest = self.journal.latest("intent", str(request.binding.intent_id))
        if latest is not None and latest.event_kind in {
            "physical-bind-requested",
            "physical-bind-confirmed",
        }:
            durable = latest.durable_payload()
            if durable is None:
                raise JournalRegressionError("physical binding request is absent from journal")
            retained = PhysicalJobBindingV2.model_validate_json(durable)
            if retained != request or latest.payload_digest != digest:
                raise JournalRegressionError("physical binding request changed during recovery")
            if latest.event_kind == "physical-bind-confirmed":
                self.journal.append(
                    "physical-bind-confirmed",
                    digest,
                    object_kind="executor",
                    object_id=_physical_binding_object_id(request.binding.intent_id),
                    payload=payload,
                )
                self._remember_launch(
                    envelope.rendered,
                    bootstrap_registration_epoch=envelope.bootstrap_registration_epoch,
                    event="physical-bind-confirmed",
                )
                return
        else:
            self.journal.append(
                "physical-bind-requested",
                digest,
                object_kind="intent",
                object_id=str(request.binding.intent_id),
                payload=payload,
            )
        await self.admission.bind_slurm_job(request)
        self.journal.append(
            "physical-bind-confirmed",
            digest,
            object_kind="executor",
            object_id=_physical_binding_object_id(request.binding.intent_id),
            payload=payload,
        )
        self.journal.append(
            "physical-bind-confirmed",
            digest,
            object_kind="intent",
            object_id=str(request.binding.intent_id),
            payload=payload,
        )
        self._remember_launch(
            envelope.rendered,
            bootstrap_registration_epoch=envelope.bootstrap_registration_epoch,
            event="physical-bind-confirmed",
        )

    def _physical_binding(
        self,
        envelope: _LaunchEnvelope | None,
    ) -> PhysicalJobBindingV2 | None:
        if envelope is None:
            return None
        binding = envelope.rendered.ownership_proof.metadata.binding
        dedicated = self.journal.latest(
            "executor",
            _physical_binding_object_id(binding.intent_id),
        )
        if dedicated is not None and dedicated.event_kind == "physical-bind-confirmed":
            return self._validate_physical_binding_record(dedicated, envelope)
        retained = self.journal.latest("intent", str(binding.intent_id))
        if retained is None or retained.event_kind != "physical-bind-confirmed":
            return None
        return self._validate_physical_binding_record(retained, envelope)

    @staticmethod
    def _validate_physical_binding_record(
        retained: JournalRecord,
        envelope: _LaunchEnvelope,
    ) -> PhysicalJobBindingV2:
        payload = retained.durable_payload()
        if payload is None:
            raise JournalRegressionError("physical binding request is absent from journal")
        physical = PhysicalJobBindingV2.model_validate_json(payload)
        binding = envelope.rendered.ownership_proof.metadata.binding
        expected_digest = canonical_executable_digest(envelope.rendered.ownership_proof)
        if (
            physical.binding != binding
            or physical.slurm_job_id == ""
            or physical.ownership_evidence_sha256 != expected_digest
        ):
            raise JournalRegressionError("physical binding request changed after confirmation")
        if retained.payload_digest != canonical_executable_digest(physical):
            raise JournalRegressionError("physical binding record digest changed")
        return physical

    @staticmethod
    def _cancel_request_from_physical(
        envelope: _LaunchEnvelope,
        physical: PhysicalJobBindingV2,
    ) -> SlurmCancelRequestV2:
        request = envelope.rendered.request
        return SlurmCancelRequestV2(
            cluster=request.cluster,
            job_id=physical.slurm_job_id,
            submitter=request.submitter,
            account=request.account,
            partition=request.partition,
            cpus=request.cpus,
            memory_bytes=request.memory_bytes,
            gpus=request.gpus,
            generic_tres=request.generic_tres,
            nodes=request.nodes,
            ownership_token=request.ownership_token,
            ownership_evidence_sha256=physical.ownership_evidence_sha256,
        )

    async def _withdraw_unregistered(
        self,
        *,
        close: ExecutableIntentCloseV2,
        envelope: _LaunchEnvelope,
        observation: ProtectedIntentObservationV2,
        job: SlurmJobObservationV2,
    ) -> None:
        if observation.claim_high_water != 0:
            raise JournalRegressionError("unregistered worker has protected claims")
        bootstrap_epoch = max(
            1,
            observation.bootstrap_registration_epoch,
            envelope.bootstrap_registration_epoch,
        )
        withdrawal = ExecutableWorkerWithdrawalRequestV2(
            operation_id=uuid5(_OPERATION_NAMESPACE, f"withdraw:{close.binding.intent_id}"),
            binding=close.binding,
            bootstrap_registration_epoch=bootstrap_epoch,
            protected_registration_epoch=bootstrap_epoch + 1,
            slurm_job_id=job.job_id,
            ownership_evidence_sha256=canonical_executable_digest(
                envelope.rendered.ownership_proof
            ),
        )
        payload = canonical_executable_bytes(withdrawal)
        digest = canonical_executable_digest(withdrawal)
        self.journal.append(
            "protected-withdraw-requested",
            digest,
            object_kind="intent",
            object_id=str(close.binding.intent_id),
            payload=payload,
        )
        await self.admission.withdraw_unregistered_worker(withdrawal)
        self.journal.append(
            "protected-withdraw-confirmed",
            digest,
            object_kind="intent",
            object_id=str(close.binding.intent_id),
            payload=payload,
        )

    async def _revoke_prepared_bootstrap(
        self,
        *,
        close: ExecutableIntentCloseV2,
        observation: ProtectedIntentObservationV2,
    ) -> None:
        bootstrap_epoch = observation.bootstrap_registration_epoch
        if bootstrap_epoch <= 0 or observation.claim_high_water != 0:
            raise JournalRegressionError("prepared bootstrap revocation evidence is incomplete")
        revocation = ExecutablePreparedBootstrapRevocationV2(
            operation_id=uuid5(
                _OPERATION_NAMESPACE,
                f"revoke-prepared:{close.binding.intent_id}",
            ),
            binding=close.binding,
            bootstrap_registration_epoch=bootstrap_epoch,
            protected_registration_epoch=bootstrap_epoch + 1,
        )
        payload = canonical_executable_bytes(revocation)
        digest = canonical_executable_digest(revocation)
        latest = self.journal.latest("prepared-revocation", str(close.binding.intent_id))
        if latest is None:
            self.journal.append(
                "protected-prepared-revocation-requested",
                digest,
                object_kind="prepared-revocation",
                object_id=str(close.binding.intent_id),
                payload=payload,
            )
        elif (
            latest.event_kind
            not in {
                "protected-prepared-revocation-requested",
                "protected-prepared-revocation-confirmed",
                "prepared-handoff-deleted",
            }
            or latest.payload_digest != digest
            or latest.durable_payload() != payload
        ):
            raise JournalRegressionError("prepared bootstrap revocation journal changed")
        if latest is not None and latest.event_kind == "prepared-handoff-deleted":
            self._prepared_revocation_from_record(latest, payload)
            return
        await self.admission.revoke_prepared_bootstrap(revocation)
        latest = self.journal.latest("prepared-revocation", str(close.binding.intent_id))
        if latest is None or latest.event_kind == "protected-prepared-revocation-requested":
            self.journal.append(
                "protected-prepared-revocation-confirmed",
                digest,
                object_kind="prepared-revocation",
                object_id=str(close.binding.intent_id),
                payload=payload,
            )
        latest = self.journal.latest("prepared-revocation", str(close.binding.intent_id))
        if latest is None:
            raise JournalRegressionError("prepared bootstrap revocation confirmation is absent")
        self._delete_prepared_handoff(latest, payload)

    def _prepared_revocation_from_record(
        self,
        record: JournalRecord,
        payload: bytes,
    ) -> ExecutablePreparedBootstrapRevocationV2:
        revocation = ExecutablePreparedBootstrapRevocationV2.model_validate_json(payload)
        if (
            record.object_kind != "prepared-revocation"
            or record.object_id != str(revocation.binding.intent_id)
            or record.payload_digest != canonical_executable_digest(revocation)
            or record.durable_payload() != payload
        ):
            raise JournalRegressionError("prepared bootstrap revocation request changed")
        self._assert_binding(revocation.binding)
        return revocation

    def _delete_prepared_handoff(
        self,
        record: JournalRecord,
        payload: bytes,
    ) -> bool:
        if self._bootstrap_handoff_store is None:
            return False
        revocation = self._prepared_revocation_from_record(record, payload)
        self._bootstrap_handoff_store.revoke_prepared(
            revocation.binding,
            bootstrap_registration_epoch=revocation.bootstrap_registration_epoch,
        )
        self.journal.append(
            "prepared-handoff-deleted",
            record.payload_digest,
            object_kind=record.object_kind,
            object_id=record.object_id,
            payload=payload,
        )
        return True

    async def _close(
        self,
        close: ExecutableIntentCloseV2,
        checkpoint: ExecutableCheckpointReceiptV2,
    ) -> ExecutorTickResult:
        observation = await self.admission.observe_intent(close.binding)
        envelope = self._load_launch(close.binding.intent_id)
        physical = self._physical_binding(envelope)
        jobs = await self.slurm.inventory()
        matches = self._bound_exact_matches(envelope, physical, jobs)
        bound_conflicts = self._bound_live_conflicts(envelope, physical, jobs)
        unbound_exact_matches = self._exact_matches(envelope, jobs) if envelope is not None else ()
        if observation.worker_id is not None and observation.worker_incarnation is not None:
            if observation.drain is None:
                drain = ExecutableDrainRequestV2(
                    operation_id=uuid5(_OPERATION_NAMESPACE, f"drain:{close.binding.intent_id}"),
                    binding=close.binding,
                    worker_id=observation.worker_id,
                    worker_incarnation=observation.worker_incarnation,
                    expected_claim_high_water=observation.claim_high_water,
                    drain_epoch=max(1, observation.protected_registration_epoch + 1),
                )
                payload = canonical_executable_bytes(drain)
                self.journal.append(
                    "protected-drain-requested",
                    canonical_executable_digest(drain),
                    object_kind="intent",
                    object_id=str(close.binding.intent_id),
                    payload=payload,
                )
                await self.admission.begin_drain(drain)
                self.journal.append(
                    "protected-drain-confirmed",
                    canonical_executable_digest(drain),
                    object_kind="intent",
                    object_id=str(close.binding.intent_id),
                    payload=payload,
                )
            if len(matches) == 1 and matches[0].state == "PENDING":
                job = matches[0]
                if envelope is None or physical is None:
                    raise JournalRegressionError("pending cancellation lacks ownership proof")
                cancel = self._cancel_request_from_physical(envelope, physical)
                cancel_payload = json.dumps(
                    cancel.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("ascii")
                self.journal.append(
                    "pending-cancel-requested",
                    hashlib.sha256(cancel_payload).hexdigest(),
                    object_kind="job",
                    object_id=job.job_id,
                    payload=cancel_payload,
                )
                await self.slurm.cancel_pending(cancel)
                self.journal.append(
                    "pending-cancel-confirmed-cancelled",
                    hashlib.sha256(cancel_payload).hexdigest(),
                    object_kind="job",
                    object_id=job.job_id,
                    payload=cancel_payload,
                )
                await self._central_command(
                    close,
                    event="intent-close",
                    operation=lambda: self.client.close_executable_intent(close),
                )
                return ExecutorTickResult("pending-cancelled", close.binding.intent_id)
            if matches:
                return ExecutorTickResult("draining", close.binding.intent_id)
            if bound_conflicts:
                return ExecutorTickResult(
                    "quarantined",
                    close.binding.intent_id,
                    "protected physical job changed scheduler identity",
                )
            if unbound_exact_matches:
                return ExecutorTickResult(
                    "quarantined",
                    close.binding.intent_id,
                    "physical job differs from protected binding",
                )
        if (
            observation.worker_id is None
            and observation.worker_incarnation is None
            and envelope is not None
            and physical is not None
            and len(matches) == 1
            and matches[0].state == "PENDING"
        ):
            job = matches[0]
            await self._withdraw_unregistered(
                close=close,
                envelope=envelope,
                observation=observation,
                job=job,
            )
            cancel = self._cancel_request_from_physical(envelope, physical)
            cancel_payload = json.dumps(
                cancel.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
            self.journal.append(
                "pending-cancel-requested",
                hashlib.sha256(cancel_payload).hexdigest(),
                object_kind="job",
                object_id=job.job_id,
                payload=cancel_payload,
            )
            await self.slurm.cancel_pending(cancel)
            self.journal.append(
                "pending-cancel-confirmed-cancelled",
                hashlib.sha256(cancel_payload).hexdigest(),
                object_kind="job",
                object_id=job.job_id,
                payload=cancel_payload,
            )
            await self._central_command(
                close,
                event="intent-close",
                operation=lambda: self.client.close_executable_intent(close),
            )
            return ExecutorTickResult("pending-cancelled", close.binding.intent_id)
        if matches:
            return ExecutorTickResult(
                "quarantined",
                close.binding.intent_id,
                "physical job exists without protected worker drain identity",
            )
        if bound_conflicts:
            return ExecutorTickResult(
                "quarantined",
                close.binding.intent_id,
                "protected physical job changed scheduler identity",
            )
        if unbound_exact_matches:
            return ExecutorTickResult(
                "quarantined",
                close.binding.intent_id,
                "physical job differs from protected binding",
            )
        if (
            observation.worker_id is None
            and observation.worker_incarnation is None
            and observation.bootstrap_registration_epoch > 0
            and envelope is None
            and physical is None
        ):
            await self._revoke_prepared_bootstrap(
                close=close,
                observation=observation,
            )
        await self._central_command(
            close,
            event="intent-close",
            operation=lambda: self.client.close_executable_intent(close),
        )
        return ExecutorTickResult("draining", close.binding.intent_id)

    async def _release(self, release: ExecutablePartialReleaseV2) -> ExecutorTickResult:
        if not release.releases:
            return ExecutorTickResult("quarantined", detail="release evidence is empty")
        for item in release.releases:
            self._assert_binding(item.binding)
            protected = await self.admission.observe_intent(item.binding)
            terminal_fence = _protected_terminal_fence(protected)
            if terminal_fence is None:
                return ExecutorTickResult(
                    "quarantined",
                    item.binding.intent_id,
                    "protected terminal evidence is absent or ambiguous",
                )
            if (
                terminal_fence.binding != item.binding
                or terminal_fence.protected_registration_epoch != item.protected_registration_epoch
                or terminal_fence.digest != item.protected_release_sha256
                or (
                    terminal_fence.receipt_kind == "prepared-revocation"
                    and item.terminal_kind != "unused"
                )
                or (terminal_fence.receipt_kind == "release" and item.terminal_kind == "unused")
                or (
                    terminal_fence.receipt_kind == "withdrawal"
                    and item.terminal_kind != "slurm-job"
                )
            ):
                return ExecutorTickResult(
                    "quarantined",
                    item.binding.intent_id,
                    "protected terminal evidence is absent or changed",
                )
            if item.terminal_kind == "unused":
                unused_detail = await self._unused_terminal_detail(item)
                if unused_detail is not None:
                    return ExecutorTickResult(
                        "quarantined",
                        item.binding.intent_id,
                        unused_detail,
                    )
            else:
                terminal_identity = terminal_fence.slurm_job_id or item.terminal_identity
                if item.terminal_identity != terminal_identity:
                    return ExecutorTickResult(
                        "quarantined",
                        item.binding.intent_id,
                        "physical terminal evidence is absent or changed",
                    )
                terminal = await self._terminal_for(terminal_identity)
                if (
                    terminal is None
                    or self._slurm_evidence_digest(terminal) != item.terminal_evidence_sha256
                ):
                    return ExecutorTickResult(
                        "quarantined",
                        item.binding.intent_id,
                        "physical terminal evidence is absent or changed",
                    )
        await self._central_command(
            release,
            event="reservation-release",
            operation=lambda: self.client.release_executable_shapes(release),
        )
        return ExecutorTickResult("released")

    async def _unused_terminal_detail(self, item: ExecutableReleasedShapeV2) -> str | None:
        inventory = self._confirmed_inventory_for_release(item)
        if inventory is None:
            return "unused terminal inventory is absent or changed"
        for record in inventory.records:
            proof = record.ownership_proof
            if proof is not None and proof.metadata.binding.intent_id == item.binding.intent_id:
                return "unused terminal inventory still owns the intent"
        envelope = self._load_launch(item.binding.intent_id)
        if envelope is not None:
            return "unused terminal still has local physical ownership"
        if self._has_physical_binding(item.binding):
            return "unused terminal still has local physical ownership"
        historical_envelopes = self._historical_launch_envelopes(item.binding.intent_id)
        if historical_envelopes:
            jobs = await self.slurm.inventory()
            high_water = await self.slurm.accounting_high_water(
                since=self._now() - _RECOVERY_LOOKBACK
            )
            for historical in historical_envelopes:
                if self._exact_matches(historical, jobs) or self._exact_terminal_matches(
                    historical,
                    high_water.terminal_jobs,
                ):
                    return "unused terminal still has local physical ownership"
        return None

    def _has_physical_binding(self, binding: ExecutableIntentBindingV2) -> bool:
        record = self.journal.latest(
            "executor",
            _physical_binding_object_id(binding.intent_id),
        )
        if record is None or record.event_kind != "physical-bind-confirmed":
            retained = self.journal.latest("intent", str(binding.intent_id))
            if retained is None or retained.event_kind != "physical-bind-confirmed":
                return False
            record = retained
        return record.object_id in {
            _physical_binding_object_id(binding.intent_id),
            str(binding.intent_id),
        }

    def _confirmed_inventory_for_release(
        self,
        item: ExecutableReleasedShapeV2,
    ) -> ExecutableExecutorInventoryV2 | None:
        sequence_records = 0
        matches: list[ExecutableExecutorInventoryV2] = []
        for record in self.journal.records(
            "inventory",
            str(self.registration.executor_incarnation),
        ):
            if record.event_kind != "inventory-publish-confirmed":
                continue
            payload = record.durable_payload()
            if payload is None:
                continue
            try:
                inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
                self._assert_inventory_binding(inventory)
            except (ValueError, JournalRegressionError):
                continue
            if inventory.inventory_sequence != item.inventory_sequence:
                continue
            sequence_records += 1
            if (
                record.payload_digest == item.terminal_evidence_sha256
                and canonical_executable_digest(inventory) == item.terminal_evidence_sha256
            ):
                matches.append(inventory)
        if sequence_records == 1 and len(matches) == 1:
            return matches[0]
        return None

    async def _terminal_for(self, job_id: str) -> SlurmTerminalEvidenceV2 | None:
        high_water = await self.slurm.accounting_high_water(since=self._now() - _RECOVERY_LOOKBACK)
        matches = tuple(item for item in high_water.terminal_jobs if item.job_id == job_id)
        return matches[0] if len(matches) == 1 else None

    def _exact_matches(
        self,
        envelope: _LaunchEnvelope | None,
        jobs: tuple[SlurmJobObservationV2, ...],
    ) -> tuple[SlurmJobObservationV2, ...]:
        if envelope is None:
            return ()
        request = envelope.rendered.request
        return tuple(
            item
            for item in jobs
            if item.cluster == request.cluster
            and item.submitter == request.submitter
            and item.account == request.account
            and item.partition == request.partition
            and item.cpus == request.cpus
            and item.memory_bytes == request.memory_bytes
            and item.gpus == request.gpus
            and item.generic_tres == request.generic_tres
            and item.ownership_token == request.ownership_token
            and item.nodes == request.nodes
        )

    def _exact_terminal_matches(
        self,
        envelope: _LaunchEnvelope | None,
        jobs: tuple[SlurmTerminalEvidenceV2, ...],
    ) -> tuple[SlurmTerminalEvidenceV2, ...]:
        if envelope is None:
            return ()
        request = envelope.rendered.request
        return tuple(
            item
            for item in jobs
            if item.cluster == request.cluster
            and item.submitter == request.submitter
            and item.account == request.account
            and item.partition == request.partition
            and item.cpus == request.cpus
            and item.memory_bytes == request.memory_bytes
            and item.gpus == request.gpus
            and item.generic_tres == request.generic_tres
            and item.nodes == request.nodes
            and item.ownership_token == request.ownership_token
        )

    def _bound_exact_matches(
        self,
        envelope: _LaunchEnvelope | None,
        physical: PhysicalJobBindingV2 | None,
        jobs: tuple[SlurmJobObservationV2, ...],
    ) -> tuple[SlurmJobObservationV2, ...]:
        if physical is None:
            return ()
        return tuple(
            item
            for item in self._exact_matches(envelope, jobs)
            if item.job_id == physical.slurm_job_id
        )

    def _bound_live_conflicts(
        self,
        envelope: _LaunchEnvelope | None,
        physical: PhysicalJobBindingV2 | None,
        jobs: tuple[SlurmJobObservationV2, ...],
    ) -> tuple[SlurmJobObservationV2, ...]:
        if physical is None:
            return ()
        exact = self._bound_exact_matches(envelope, physical, jobs)
        return tuple(
            item for item in jobs if item.job_id == physical.slurm_job_id and item not in exact
        )

    def _live_ownership_conflicts(
        self,
        envelope: _LaunchEnvelope | None,
        jobs: tuple[SlurmJobObservationV2, ...],
    ) -> tuple[SlurmJobObservationV2, ...]:
        if envelope is None:
            return ()
        request = envelope.rendered.request
        return tuple(
            item
            for item in jobs
            if item.ownership_token == request.ownership_token
            and item not in self._exact_matches(envelope, jobs)
        )

    def _terminal_ownership_conflicts(
        self,
        envelope: _LaunchEnvelope | None,
        jobs: tuple[SlurmTerminalEvidenceV2, ...],
    ) -> tuple[SlurmTerminalEvidenceV2, ...]:
        if envelope is None:
            return ()
        request = envelope.rendered.request
        return tuple(
            item
            for item in jobs
            if item.ownership_token == request.ownership_token
            and item not in self._exact_terminal_matches(envelope, jobs)
        )

    def _terminal_live_job_id_conflicts(
        self,
        terminal_matches: tuple[SlurmTerminalEvidenceV2, ...],
        live_matches: tuple[SlurmJobObservationV2, ...],
        jobs: tuple[SlurmJobObservationV2, ...],
    ) -> tuple[SlurmJobObservationV2, ...]:
        terminal_job_ids = {item.job_id for item in terminal_matches}
        return tuple(
            item for item in jobs if item.job_id in terminal_job_ids and item not in live_matches
        )

    def _bound_live_matches(
        self,
        envelope: _LaunchEnvelope | None,
        jobs: tuple[SlurmJobObservationV2, ...],
    ) -> tuple[SlurmJobObservationV2, ...]:
        if envelope is None:
            return ()
        binding = envelope.rendered.ownership_proof.metadata.binding
        retained = self.journal.latest("intent", str(binding.intent_id))
        if retained is None or retained.event_kind != "physical-bind-confirmed":
            return ()
        payload = retained.durable_payload()
        if payload is None:
            raise JournalRegressionError("physical binding request is absent from journal")
        physical = PhysicalJobBindingV2.model_validate_json(payload)
        if physical.binding != binding:
            raise JournalRegressionError("physical binding request changed after confirmation")
        request = envelope.rendered.request
        return tuple(
            item
            for item in jobs
            if item.job_id == physical.slurm_job_id
            and item.cluster == request.cluster
            and item.submitter == request.submitter
            and item.account == request.account
            and item.partition == request.partition
            and item.ownership_token == request.ownership_token
        )

    def _observed_generic_resources(
        self,
        item: SlurmJobObservationV2,
        profile: OperatorLaunchProfileV2,
    ) -> dict[str, int]:
        mappings = {mapping.tres_name: mapping.resource_name for mapping in profile.generic_tres}
        generic: dict[str, int] = {}
        for tres in item.generic_tres:
            resource_name = mappings.get(tres.name)
            if resource_name is None:
                resource_name = (
                    f"slurm_unmapped_{hashlib.sha256(tres.name.encode('ascii')).hexdigest()[:40]}"
                )
            generic[resource_name] = tres.value
        return generic

    @staticmethod
    def _slurm_evidence_digest(
        item: SlurmJobObservationV2 | SlurmTerminalEvidenceV2,
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                item.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()

    async def recover(self) -> ExecutorTickResult:
        checkpoint = await self._checkpoint()
        replayed_local = await self._replay_local_request(checkpoint)
        if replayed_local is not None:
            return replayed_local
        jobs = await self.slurm.inventory()
        recovering = tuple(
            record
            for record in (
                *self.journal.latest_records("job"),
                *self.journal.latest_records("intent"),
            )
            if record.event_kind
            in {
                "slurm-submit-requested",
                "slurm-submit-unknown",
                "slurm-submit-confirmed",
                "physical-bind-requested",
            }
        )
        if not recovering:
            return await self._publish_inventory(checkpoint, jobs=jobs)
        record = min(recovering, key=lambda item: item.sequence)
        intent_id = UUID(record.object_id)
        envelope = self._load_launch(intent_id)
        if envelope is None:
            await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
            return ExecutorTickResult(
                "quarantined",
                intent_id,
                "scheduler association is absent, duplicate, foreign, or resource-mismatched",
            )
        submitted_at = envelope.rendered.ownership_proof.metadata.submitted_at
        high_water = await self.slurm.accounting_high_water(since=submitted_at)
        matches = self._exact_matches(envelope, jobs)
        terminal_matches = self._exact_terminal_matches(envelope, high_water.terminal_jobs)
        live_conflicts = self._live_ownership_conflicts(envelope, jobs)
        terminal_conflicts = self._terminal_ownership_conflicts(envelope, high_water.terminal_jobs)
        terminal_live_conflicts = self._terminal_live_job_id_conflicts(
            terminal_matches,
            matches,
            jobs,
        )
        if (
            live_conflicts
            or terminal_conflicts
            or terminal_live_conflicts
            or (matches and terminal_matches)
            or len(matches) > 1
            or len(terminal_matches) > 1
        ):
            await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
            return ExecutorTickResult(
                "quarantined",
                intent_id,
                "scheduler association is absent, duplicate, foreign, or resource-mismatched",
            )
        if len(matches) == 1:
            await self._bind_physical(envelope=envelope, job_id=matches[0].job_id)
            await self._publish_inventory(checkpoint, jobs=jobs)
            return ExecutorTickResult("adopted", intent_id, matches[0].job_id)
        if high_water.observed_through >= submitted_at and len(terminal_matches) == 1:
            await self._bind_physical(envelope=envelope, job_id=terminal_matches[0].job_id)
            await self._publish_inventory(checkpoint, jobs=jobs)
            return ExecutorTickResult("adopted", intent_id, terminal_matches[0].job_id)
        await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
        return ExecutorTickResult(
            "quarantined",
            intent_id,
            "scheduler association is absent, duplicate, foreign, or resource-mismatched",
        )

    async def _publish_inventory(
        self,
        checkpoint: ExecutableCheckpointReceiptV2,
        *,
        jobs: tuple[SlurmJobObservationV2, ...] | None = None,
        quarantine_all: bool = False,
    ) -> ExecutorTickResult:
        inventory_object_id = str(self.registration.executor_incarnation)
        latest_inventory = self.journal.latest("inventory", inventory_object_id)
        if (
            latest_inventory is not None
            and latest_inventory.event_kind == "inventory-publish-requested"
        ):
            payload = latest_inventory.durable_payload()
            if payload is None:
                raise JournalRegressionError("inventory request is absent from journal")
            inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
            self._assert_inventory_binding(inventory)
            if checkpoint.inventory_sequence not in {
                inventory.inventory_sequence - 1,
                inventory.inventory_sequence,
            }:
                raise JournalRegressionError("inventory replay high-water changed")
            return await self._send_inventory(inventory)
        observed = await self.slurm.inventory() if jobs is None else jobs
        proofs: dict[str, SignedExecutableOwnershipProofV2] = {}
        terminal_proofs: dict[str, SignedExecutableOwnershipProofV2] = {}
        terminals: tuple[SlurmTerminalEvidenceV2, ...] = ()
        if not quarantine_all:
            high_water = await self.slurm.accounting_high_water(
                since=self._now() - _RECOVERY_LOOKBACK
            )
            terminals = high_water.terminal_jobs
            for record in self.journal.latest_records("job"):
                try:
                    envelope = self._load_launch(UUID(record.object_id))
                except (ValueError, JournalRegressionError):
                    continue
                matches = self._exact_matches(envelope, observed)
                if envelope is not None and len(matches) == 1:
                    proofs[matches[0].job_id] = envelope.rendered.ownership_proof
                else:
                    bound_matches = self._bound_live_matches(envelope, observed)
                    if envelope is not None and len(bound_matches) == 1:
                        proofs[bound_matches[0].job_id] = envelope.rendered.ownership_proof
                terminal_matches = self._exact_terminal_matches(envelope, terminals)
                if envelope is not None and len(terminal_matches) == 1:
                    terminal_proofs[terminal_matches[0].job_id] = envelope.rendered.ownership_proof

        def live_record(item: SlurmJobObservationV2) -> ExecutableInventoryRecordV2:
            proof = proofs.get(item.job_id)
            profile = (
                self._profile_for(proof.metadata.binding) if proof is not None else self.profile
            )
            return ExecutableInventoryRecordV2(
                physical_identity=item.job_id,
                physical_kind="slurm-job",
                authority_scope="dedicated-loom-association",
                state=(
                    "pending"
                    if item.state == "PENDING"
                    else "active"
                    if item.state in {"CONFIGURING", "RUNNING", "COMPLETING", "SUSPENDED"}
                    else "unknown"
                ),
                resources=ResourceVectorV1(
                    slots=(proof.metadata.binding.resources.slots if proof is not None else 1),
                    cpu_millicores=item.cpus * 1_000,
                    memory_bytes=item.memory_bytes,
                    gpu_count=item.gpus,
                    generic=self._observed_generic_resources(item, profile),
                ),
                node_ids=item.nodes,
                controller_evidence_sha256=self._slurm_evidence_digest(item),
                ownership_proof=proof,
            )

        live_records = tuple(live_record(item) for item in observed)
        live_identities = {item.physical_identity for item in live_records}
        terminal_records = tuple(
            ExecutableInventoryRecordV2(
                physical_identity=item.job_id,
                physical_kind="slurm-job",
                authority_scope="dedicated-loom-association",
                state="terminal",
                resources=terminal_proofs[item.job_id].metadata.binding.resources,
                node_ids=terminal_proofs[item.job_id].metadata.binding.node_ids,
                controller_evidence_sha256=self._slurm_evidence_digest(item),
                ownership_proof=terminal_proofs[item.job_id],
                terminal_evidence_sha256=self._slurm_evidence_digest(item),
            )
            for item in terminals
            if item.job_id in terminal_proofs and item.job_id not in live_identities
        )
        inventory = ExecutableExecutorInventoryV2(
            execution=self.registration.execution,
            executor_id=self.registration.executor_id,
            executor_incarnation=self.registration.executor_incarnation,
            pool_id=self.registration.pool_id,
            pool_generation=self.registration.pool_generation,
            inventory_sequence=checkpoint.inventory_sequence + 1,
            journal_sequence=self.journal.head.sequence,
            journal_digest=self.journal.head.digest,
            journal_checkpoint_sequence=checkpoint.journal_sequence,
            journal_checkpoint_digest=checkpoint.journal_digest,
            records=live_records + terminal_records,
        )
        inventory_payload = canonical_executable_bytes(inventory)
        self.journal.append(
            "inventory-publish-requested",
            canonical_executable_digest(inventory),
            object_kind="inventory",
            object_id=inventory_object_id,
            payload=inventory_payload,
        )
        return await self._send_inventory(inventory)

    def _assert_inventory_binding(self, inventory: ExecutableExecutorInventoryV2) -> None:
        if (
            inventory.execution != self.registration.execution
            or inventory.executor_id != self.registration.executor_id
            or inventory.executor_incarnation != self.registration.executor_incarnation
            or inventory.pool_id != self.registration.pool_id
            or inventory.pool_generation != self.registration.pool_generation
        ):
            raise JournalRegressionError("inventory request binding changed")

    async def _send_inventory(
        self,
        inventory: ExecutableExecutorInventoryV2,
    ) -> ExecutorTickResult:
        payload = canonical_executable_bytes(inventory)
        digest = canonical_executable_digest(inventory)
        object_id = str(self.registration.executor_incarnation)
        try:
            await self.client.ingest_executable_inventory(inventory)
        except ExecutorRejectedError:
            self.journal.append(
                "inventory-publish-rejected",
                digest,
                object_kind="inventory",
                object_id=object_id,
                payload=payload,
            )
            raise
        self.journal.append(
            "inventory-publish-confirmed",
            digest,
            object_kind="inventory",
            object_id=object_id,
            payload=payload,
        )
        return ExecutorTickResult("inventory-published")


__all__ = [
    "ExecutablePoolExecutor",
    "ExecutorTickResult",
    "ProtectedIntentObservationV2",
]
