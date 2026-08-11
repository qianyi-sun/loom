"""Lease-fenced reconciliation for immutable personal-development candidates."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

from loom.dev_instance_provisioner import OwnerAccessSnapshot
from loom.personal_dev_activation import PersonalDevActivationIntent
from loom.personal_dev_capacity import (
    PersonalDevCapacityInstaller,
    PersonalDevCapacityProjectionConflictError,
    PersonalDevCapacityProjector,
    personal_dev_capacity_projection,
)
from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom_capacity_manager.contracts import canonical_digest

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_DEPLOYED_COMPONENTS = ("control-plane", "llm-gateway", "service", "web")
_REQUIRED_ACTIVATION_PROTOCOLS = {
    "capacity-agent": "v1",
    "claim-guard": "v1",
    "control-plane-worker": "v1",
    "database-migrations": "expand-compatible-v1",
    "personal-dev-activation": "v1",
}
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class PersonalDevReadinessObservation:
    """Secret-free observation returned by the trusted deployment executor."""

    deployed_images: Mapping[str, str]
    resource_evidence_sha256: str

    def __post_init__(self) -> None:
        if set(self.deployed_images) != set(_DEPLOYED_COMPONENTS):
            raise ValueError("readiness must cover the complete deployed component set")
        if _DIGEST_RE.fullmatch(self.resource_evidence_sha256) is None:
            raise ValueError("resource readiness evidence must be a SHA-256 digest")


def personal_dev_candidate_images(
    claim: PersonalDevReconciliationClaim,
) -> dict[str, str]:
    """Validate activation-critical candidate bindings before any mutation."""
    operation = claim.operation
    attempt = claim.attempt
    candidate = claim.candidate
    environment = claim.environment
    if (
        operation.id != attempt.operation_id
        or operation.attempt_id != attempt.id
        or operation.attempt_sequence != attempt.attempt_sequence
        or operation.subject_id != attempt.subject_id
        or operation.subject_incarnation != attempt.subject_incarnation
        or operation.operation_epoch != attempt.operation_epoch
        or environment.operation_id != operation.id
        or environment.subject_id != operation.subject_id
        or environment.subject_incarnation != operation.subject_incarnation
        or environment.operation_epoch != operation.operation_epoch
        or candidate.id != operation.candidate_id
        or candidate.candidate_sha != operation.candidate_sha
    ):
        raise ValueError("personal-dev readiness claim bindings are inconsistent")
    if (
        candidate.status != "ready"
        or candidate.publication_json is None
        or candidate.publication_sha256 is None
        or _DIGEST_RE.fullmatch(candidate.publication_sha256) is None
    ):
        raise ValueError("personal-dev candidate publication is not ready")
    raw_images = candidate.publication_json.get("images")
    if not isinstance(raw_images, dict):
        raise ValueError("personal-dev candidate image publication is invalid")
    images: dict[str, str] = {}
    for component, raw_image in raw_images.items():
        if (
            not isinstance(component, str)
            or not isinstance(raw_image, dict)
            or not isinstance(raw_image.get("index"), str)
        ):
            raise ValueError("personal-dev candidate image publication is incomplete")
        images[component] = raw_image["index"]
    protocols = candidate.publication_json.get("protocol_versions")
    if not isinstance(protocols, dict) or any(
        protocols.get(protocol) != version
        for protocol, version in _REQUIRED_ACTIVATION_PROTOCOLS.items()
    ):
        raise ValueError("personal-dev candidate activation protocols are incompatible")
    return images


def personal_dev_readiness_sha256(
    claim: PersonalDevReconciliationClaim,
    observation: PersonalDevReadinessObservation,
) -> str:
    """Canonically bind observed resources to one exact lifecycle attempt."""
    operation = claim.operation
    attempt = claim.attempt
    candidate = claim.candidate
    all_images = personal_dev_candidate_images(claim)
    expected_images: dict[str, str] = {}
    for component in _DEPLOYED_COMPONENTS:
        reference = all_images.get(component)
        if reference is None:
            raise ValueError("personal-dev candidate image publication is incomplete")
        expected_images[component] = reference
    observed_images = dict(observation.deployed_images)
    if observed_images != expected_images:
        raise ValueError("deployed image does not match the candidate publication")
    payload = {
        "attempt_id": str(attempt.id),
        "attempt_sequence": attempt.attempt_sequence,
        "candidate_id": str(candidate.id),
        "candidate_publication_sha256": candidate.publication_sha256,
        "candidate_sha": candidate.candidate_sha,
        "deployed_images": observed_images,
        "deployment_generation": operation.deployment_generation,
        "environment_name": operation.environment_name,
        "max_slots": operation.max_slots,
        "min_slots": operation.min_slots,
        "operation_epoch": operation.operation_epoch,
        "operation_id": str(operation.id),
        "resource_evidence_sha256": observation.resource_evidence_sha256,
        "schema_version": 1,
        "subject_id": str(operation.subject_id),
        "subject_incarnation": str(operation.subject_incarnation),
    }
    return _readiness_payload_sha256(payload)


def _readiness_payload_sha256(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def personal_dev_intent_readiness_sha256(
    intent: PersonalDevActivationIntent,
    observation: PersonalDevReadinessObservation,
) -> str:
    """Recompute central readiness from a secret-free activation intent."""
    expected_images = {component: intent.images[component] for component in _DEPLOYED_COMPONENTS}
    observed_images = dict(observation.deployed_images)
    if observed_images != expected_images:
        raise ValueError("deployed image does not match the activation intent")
    return _readiness_payload_sha256(
        {
            "attempt_id": str(intent.attempt_id),
            "attempt_sequence": intent.attempt_sequence,
            "candidate_id": str(intent.candidate_id),
            "candidate_publication_sha256": intent.candidate_publication_sha256,
            "candidate_sha": intent.candidate_sha,
            "deployed_images": observed_images,
            "deployment_generation": intent.deployment_generation,
            "environment_name": intent.environment_name,
            "max_slots": intent.max_slots,
            "min_slots": intent.min_slots,
            "operation_epoch": intent.operation_epoch,
            "operation_id": str(intent.operation_id),
            "resource_evidence_sha256": observation.resource_evidence_sha256,
            "schema_version": 1,
            "subject_id": str(intent.subject_id),
            "subject_incarnation": str(intent.subject_incarnation),
        }
    )


class PersonalDevReconciliationAuthority(Protocol):
    async def claim_next_reconciliation(
        self,
        *,
        reconciler_id: str,
        now: datetime,
        lease_seconds: int,
    ) -> PersonalDevReconciliationClaim | None: ...

    async def begin_activation(self, **kwargs: object) -> object: ...

    async def heartbeat_reconciliation(self, **kwargs: object) -> object: ...

    async def fail_pre_activation(self, **kwargs: object) -> object: ...

    async def complete_activation(self, **kwargs: object) -> object: ...

    async def prepare_capacity_projection(self, **kwargs: object) -> object: ...

    async def refresh_capacity_projection_epoch(self, **kwargs: object) -> object: ...

    async def record_capacity_projection(self, **kwargs: object) -> object: ...


class PersonalDevPreparationExecutor(Protocol):
    async def prepare(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        access: OwnerAccessSnapshot,
    ) -> PersonalDevReadinessObservation: ...

    async def bootstrap_access(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        access: OwnerAccessSnapshot,
    ) -> None: ...


@dataclass(slots=True)
class PersonalDevEnvironmentReconciler:
    """Advance at most one durable claim without crossing the trust boundary."""

    authority: PersonalDevReconciliationAuthority
    executor: PersonalDevPreparationExecutor
    capacity_installer: PersonalDevCapacityInstaller
    capacity_projector: PersonalDevCapacityProjector
    access_loader: Callable[
        [PersonalDevReconciliationClaim],
        Awaitable[OwnerAccessSnapshot],
    ]
    reconciler_id: str
    lease_seconds: int

    def __post_init__(self) -> None:
        if (
            not self.reconciler_id
            or self.reconciler_id.strip() != self.reconciler_id
            or len(self.reconciler_id) > 128
        ):
            raise ValueError("reconciler_id must be a non-empty bounded identifier")
        if type(self.lease_seconds) is not int or self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")

    async def reconcile_once(self, *, now: datetime) -> bool:
        loop = asyncio.get_running_loop()
        started = loop.time()
        claim = await self.authority.claim_next_reconciliation(
            reconciler_id=self.reconciler_id,
            now=now,
            lease_seconds=self.lease_seconds,
        )
        if claim is None:
            return False
        attempt = claim.attempt
        if (
            attempt.claimed_by != self.reconciler_id
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= now
        ):
            raise RuntimeError("personal-dev reconciliation claim has no live matching lease")
        lease = {
            "operation_id": claim.operation.id,
            "operation_epoch": claim.operation.operation_epoch,
            "attempt_id": attempt.id,
            "reconciler_id": self.reconciler_id,
            "lease_epoch": attempt.lease_epoch,
        }
        if claim.operation.checkpoint == "capacity_projected":
            await self.authority.complete_activation(**lease, now=now)
            return True
        if claim.operation.checkpoint in {
            "activation_acknowledged",
            "capacity_projection_requested",
            "capacity_projection_pending",
        }:
            installation = await self._with_heartbeats(
                self.capacity_installer.converge(claim),
                lease=lease,
                started=started,
                initial_now=now,
            )
            if claim.operation.checkpoint in {
                "activation_acknowledged",
                "capacity_projection_requested",
            }:
                expected_epoch = await self._with_heartbeats(
                    self.capacity_projector.current_configuration_epoch(),
                    lease=lease,
                    started=started,
                    initial_now=now,
                )
                request = personal_dev_capacity_projection(
                    claim,
                    installation,
                    expected_configuration_epoch=expected_epoch,
                )
                callback_now = now + timedelta(seconds=loop.time() - started)
                await self.authority.prepare_capacity_projection(
                    **lease,
                    now=callback_now,
                    expected_configuration_epoch=expected_epoch,
                    projection_request_sha256=canonical_digest(request),
                    reporter_incarnation=installation.reporter_incarnation,
                    reporter_token_sha256=installation.reporter_token_sha256,
                    protected_admission_sha256=(installation.protected_admission_sha256),
                    capacity_agent_installation_sha256=(
                        installation.capacity_agent_installation_sha256
                    ),
                )
                return True
            prepared_epoch = claim.operation.capacity_expected_configuration_epoch
            if prepared_epoch is None:
                raise RuntimeError("prepared capacity projection has no global epoch")
            request = personal_dev_capacity_projection(
                claim,
                installation,
                expected_configuration_epoch=prepared_epoch,
            )
            try:
                result = await self._with_heartbeats(
                    self.capacity_projector.project(
                        request,
                        idempotency_key=claim.operation.idempotency_key,
                    ),
                    lease=lease,
                    started=started,
                    initial_now=now,
                )
            except PersonalDevCapacityProjectionConflictError as exc:
                refreshed_epoch = await self._with_heartbeats(
                    self.capacity_projector.current_configuration_epoch(),
                    lease=lease,
                    started=started,
                    initial_now=now,
                )
                if refreshed_epoch <= request.expected_configuration_epoch:
                    raise RuntimeError(
                        "capacity manager rejected the request at its current epoch"
                    ) from exc
                refreshed = request.model_copy(
                    update={"expected_configuration_epoch": refreshed_epoch}
                )
                callback_now = now + timedelta(seconds=loop.time() - started)
                await self.authority.refresh_capacity_projection_epoch(
                    **lease,
                    now=callback_now,
                    expected_configuration_epoch=refreshed_epoch,
                    projection_request_sha256=canonical_digest(refreshed),
                )
                return True
            await self._with_heartbeats(
                self.capacity_installer.verify_publishing(claim, installation),
                lease=lease,
                started=started,
                initial_now=now,
            )
            callback_now = now + timedelta(seconds=loop.time() - started)
            await self.authority.record_capacity_projection(
                **lease,
                now=callback_now,
                result=result,
            )
            return True
        if claim.operation.state != "running" or claim.operation.checkpoint != "candidate_build":
            raise RuntimeError("personal-dev reconciler received an ineligible checkpoint")
        if claim.candidate.status == "failed":
            await self.authority.fail_pre_activation(
                **lease,
                now=now,
                failure_reason="candidate_build_failed",
            )
            return True
        if claim.candidate.status != "ready":
            raise RuntimeError("personal-dev reconciler claimed a nonterminal candidate build")
        preparation: asyncio.Task[PersonalDevReadinessObservation] | None = None
        try:
            personal_dev_candidate_images(claim)

            async def prepare_and_bootstrap() -> PersonalDevReadinessObservation:
                access = await self.access_loader(claim)
                observation = await self.executor.prepare(claim, access=access)
                # Preparation can take minutes. Reload the exact attempt-bound
                # credential immediately before copying it into the isolated
                # database so revocation or expiry during deployment fails shut.
                current_access = await self.access_loader(claim)
                await self.executor.bootstrap_access(claim, access=current_access)
                return observation

            preparation = asyncio.create_task(
                prepare_and_bootstrap(),
                name=(
                    f"loom-personal-dev-prepare-{claim.operation.environment_name}-"
                    f"{claim.operation.operation_epoch}"
                ),
            )
            heartbeat_interval = max(0.1, self.lease_seconds / 3)
            while True:
                done, _pending = await asyncio.wait(
                    {preparation},
                    timeout=heartbeat_interval,
                )
                if done:
                    observation = await preparation
                    break
                heartbeat_now = now + timedelta(seconds=loop.time() - started)
                await self.authority.heartbeat_reconciliation(
                    **lease,
                    now=heartbeat_now,
                    lease_seconds=self.lease_seconds,
                )
            readiness_evidence_sha256 = personal_dev_readiness_sha256(claim, observation)
        except asyncio.CancelledError:
            if preparation is not None and not preparation.done():
                preparation.cancel()
                await asyncio.gather(preparation, return_exceptions=True)
            raise
        except Exception:
            if preparation is not None and not preparation.done():
                preparation.cancel()
                await asyncio.gather(preparation, return_exceptions=True)
            callback_now = now + timedelta(seconds=loop.time() - started)
            await self.authority.fail_pre_activation(
                **lease,
                now=callback_now,
                failure_reason=(
                    "provisioning_failed" if claim.operation.kind == "create" else "update_failed"
                ),
            )
            return True
        callback_now = now + timedelta(seconds=loop.time() - started)
        await self.authority.begin_activation(
            **lease,
            now=callback_now,
            readiness_evidence_sha256=readiness_evidence_sha256,
        )
        return True

    async def _with_heartbeats(
        self,
        awaitable: Awaitable[_T],
        *,
        lease: Mapping[str, object],
        started: float,
        initial_now: datetime,
    ) -> _T:
        """Keep the attempt lease live around one bounded external operation."""

        loop = asyncio.get_running_loop()
        task = asyncio.ensure_future(awaitable)
        heartbeat_interval = max(0.1, self.lease_seconds / 3)
        try:
            while True:
                done, _pending = await asyncio.wait({task}, timeout=heartbeat_interval)
                if done:
                    return await task
                heartbeat_now = initial_now + timedelta(seconds=loop.time() - started)
                await self.authority.heartbeat_reconciliation(
                    **lease,
                    now=heartbeat_now,
                    lease_seconds=self.lease_seconds,
                )
        except BaseException:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            raise


__all__ = [
    "PersonalDevEnvironmentReconciler",
    "PersonalDevPreparationExecutor",
    "PersonalDevReadinessObservation",
    "PersonalDevReconciliationAuthority",
    "personal_dev_candidate_images",
    "personal_dev_intent_readiness_sha256",
    "personal_dev_readiness_sha256",
]
