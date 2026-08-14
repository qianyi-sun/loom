"""Journal-first executable-v2 pool protocol driver and recovery reconciler."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID, uuid5

from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    PhysicalJobBindingV2,
    ProtectedIntentObservationV2,
)
from loom_capacity_executor.client import ExecutorRejectedError
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
    executable_ownership_token,
    render_signed_launch,
)
from loom_capacity_executor.slurm_contracts import (
    SlurmCancelRequestV2,
    SlurmJobObservationV2,
    SlurmLaunchRequestV2,
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
    async def executable_checkpoint(self) -> Any: ...

    async def next_executable_work(self, command_sequence: int) -> object | None: ...

    async def accept_executable_reservation(
        self, value: ExecutableReservationAcceptanceV2
    ) -> Any: ...

    async def register_executable_bootstrap(
        self, value: ExecutableBootstrapRegistrationV2
    ) -> Any: ...

    async def consume_executable_permit(self, value: ExecutablePermitConsumptionV2) -> Any: ...

    async def close_executable_intent(self, value: ExecutableIntentCloseV2) -> Any: ...

    async def release_executable_shapes(self, value: ExecutablePartialReleaseV2) -> Any: ...

    async def ingest_executable_inventory(self, value: ExecutableExecutorInventoryV2) -> Any: ...


class _AdmissionClient(Protocol):
    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> Any: ...

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> Any: ...

    async def observe_intent(
        self, binding: ExecutableIntentBindingV2
    ) -> ProtectedIntentObservationV2: ...

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> Any: ...


class _SlurmBackend(Protocol):
    async def submit(self, request: Any) -> Any: ...

    async def inventory(self) -> tuple[SlurmJobObservationV2, ...]: ...

    async def accounting_high_water(self, *, since: datetime) -> Any: ...

    async def cancel_pending(self, request: SlurmCancelRequestV2) -> Any: ...


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
        now: Callable[[], datetime] | None = None,
        bootstrap_digest: Callable[[ExecutableIntentBindingV2], str] | None = None,
    ) -> None:
        if not isinstance(registration, ExecutableExecutorRegistrationV2):
            raise TypeError("executable executor requires its exact registration")
        if not isinstance(journal, ExecutorJournal):
            raise TypeError("executable executor requires its locked journal")
        if registration.pool_id != profile.pool_id or (
            registration.controller_authority_sha256
            != controller_authority.controller_authority_sha256
        ):
            raise ValueError("executor launch authority differs from registration")
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
        self.profile = profile
        self.controller_authority = controller_authority
        self.ownership_key = ownership_key
        self._now = now or (lambda: datetime.now(UTC))
        self._bootstrap_digest = bootstrap_digest or (
            lambda binding: hashlib.sha256(canonical_executable_bytes(binding)).hexdigest()
        )

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
        return render_signed_launch(
            TrustedLaunchContextV2(
                binding=binding,
                profile=self.profile,
                controller_authority=self.controller_authority,
                ownership_key=self.ownership_key,
                submitted_at=self._now(),
            )
        )

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
                    profile=self.profile,
                    controller_authority=self.controller_authority,
                    ownership_key=self.ownership_key,
                    submitted_at=proof.metadata.submitted_at,
                )
            )
        except (TypeError, ValueError) as exc:
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
            return ExecutableBootstrapRegistrationV2(
                binding=binding,
                command_sequence=command_sequence,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=self._bootstrap_digest(binding),
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

    async def tick(self) -> ExecutorTickResult:
        checkpoint = await self._checkpoint()
        replayed = await self._replay_central_request(checkpoint)
        if replayed is not None:
            return replayed
        work = await self.client.next_executable_work(checkpoint.command_sequence)
        if work is None:
            return await self._publish_inventory(checkpoint)
        return await self._apply_one(work, checkpoint)

    def _validate_central_replay(
        self,
        record: JournalRecord,
        value: StrictV2Model,
        checkpoint: Any,
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

    async def _replay_central_request(self, checkpoint: Any) -> ExecutorTickResult | None:
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
        await self._central_command(
            release,
            event="reservation-release",
            operation=lambda: self.client.release_executable_shapes(release),
        )
        return ExecutorTickResult("released")

    async def _apply_one(self, work: object, checkpoint: Any) -> ExecutorTickResult:
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
            object_kind="intent",
            object_id=str(request.binding.intent_id),
            payload=payload,
        )
        self._remember_launch(
            envelope.rendered,
            bootstrap_registration_epoch=envelope.bootstrap_registration_epoch,
            event="physical-bind-confirmed",
        )

    async def _close(self, close: ExecutableIntentCloseV2, checkpoint: Any) -> ExecutorTickResult:
        observation = await self.admission.observe_intent(close.binding)
        envelope = self._load_launch(close.binding.intent_id)
        jobs = await self.slurm.inventory()
        matches = self._exact_matches(envelope, jobs) if envelope is not None else ()
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
                cancel = SlurmCancelRequestV2(
                    cluster=job.cluster,
                    job_id=job.job_id,
                    submitter=job.submitter,
                    account=job.account,
                )
                cancel_payload = json.dumps(
                    cancel.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("ascii")
                self.journal.append(
                    "slurm-cancel-requested",
                    hashlib.sha256(cancel_payload).hexdigest(),
                    object_kind="job",
                    object_id=job.job_id,
                    payload=cancel_payload,
                )
                await self.slurm.cancel_pending(cancel)
                self.journal.append(
                    "slurm-cancel-confirmed",
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
        if matches:
            return ExecutorTickResult(
                "quarantined",
                close.binding.intent_id,
                "physical job exists without protected worker drain identity",
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
            if (
                protected.release is None
                or protected.release.protected_release_sha256 != item.protected_release_sha256
            ):
                return ExecutorTickResult(
                    "quarantined",
                    item.binding.intent_id,
                    "protected release is absent or changed",
                )
            terminal = await self._terminal_for(item.terminal_identity)
            if (
                terminal is None
                or hashlib.sha256(
                    json.dumps(
                        terminal.model_dump(mode="json"),
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("ascii")
                ).hexdigest()
                != item.terminal_evidence_sha256
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
            and (item.state == "PENDING" or item.nodes == request.nodes)
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
            and item.cpus == request.cpus
            and item.memory_bytes == request.memory_bytes
            and item.gpus == request.gpus
            and item.generic_tres == request.generic_tres
            and item.nodes == request.nodes
            and item.ownership_token == request.ownership_token
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
        matches = self._exact_matches(envelope, jobs)
        if envelope is not None and len(matches) == 1:
            await self._bind_physical(envelope=envelope, job_id=matches[0].job_id)
            await self._publish_inventory(checkpoint, jobs=jobs)
            return ExecutorTickResult("adopted", intent_id, matches[0].job_id)
        await self._publish_inventory(checkpoint, jobs=jobs, quarantine_all=True)
        return ExecutorTickResult(
            "quarantined",
            intent_id,
            "scheduler association is absent, duplicate, foreign, or resource-mismatched",
        )

    async def _publish_inventory(
        self,
        checkpoint: Any,
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
        live_records = tuple(
            ExecutableInventoryRecordV2(
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
                    slots=(
                        proofs[item.job_id].metadata.binding.resources.slots
                        if item.job_id in proofs
                        else 1
                    ),
                    cpu_millicores=item.cpus * 1_000,
                    memory_bytes=item.memory_bytes,
                    gpu_count=item.gpus,
                    generic=(
                        proofs[item.job_id].metadata.binding.resources.generic
                        if item.job_id in proofs
                        else {}
                    ),
                ),
                node_ids=item.nodes,
                controller_evidence_sha256=self._slurm_evidence_digest(item),
                ownership_proof=proofs.get(item.job_id),
            )
            for item in observed
        )
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
