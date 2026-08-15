"""Owner registration and agent-only monotonic demand capture."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    ProtectedReleasePublicationCheckpointV2,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    GuardDemandObservationV1,
    GuardLifecycleDemandObservationV2,
)
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest
from loom_capacity_guard.store import CapacityGuardStore
from loom_capacity_manager.executable_contracts import (
    ExecutableProtectedReleaseV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)

_SCHEMA = "loom_capacity_guard"
_ROLE_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_REGISTRATION_BINDINGS = (
    "environment_id",
    "subject_id",
    "subject_incarnation",
    "authority_incarnation",
    "reporter_incarnation",
    "authority_mode",
    "allocation_epoch",
    "candidate_digest",
    "deployment_generation",
    "configuration_generation",
)


class CapacityAgentStoreError(RuntimeError):
    """Protected agent registration or observation integrity failed."""


def _json_payload(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


class CapacityAgentStore:
    """Register one immutable agent through the exact protected owner session."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        expected_owner_role: str,
        expected_agent_role: str,
    ) -> None:
        if _ROLE_RE.fullmatch(expected_agent_role) is None:
            raise CapacityAgentStoreError("expected agent role is not canonical")
        self._session = session
        self._expected_owner_role = expected_owner_role
        self._expected_agent_role = expected_agent_role

    async def register_agent(self, registration: AgentRegistrationV1) -> AgentRegistrationV1:
        guard = CapacityGuardStore(
            self._session,
            expected_owner_role=self._expected_owner_role,
        )
        fence = await guard.read_guard_fence()
        mismatches = tuple(
            field
            for field in _REGISTRATION_BINDINGS
            if getattr(registration, field) != getattr(fence, field)
        )
        if mismatches:
            raise CapacityAgentStoreError(
                f"agent registration binding differs from the guard fence: {', '.join(mismatches)}"
            )
        await self._assert_agent_role_binding()

        payload = registration.model_dump(mode="json", exclude_none=False)
        payload_digest = canonical_digest(registration)
        async with self._session.begin_nested():
            inserted = (
                await self._session.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.agent_registrations "
                        "(agent_incarnation, singleton_id, schema_version, environment_id, "
                        "subject_id, subject_incarnation, authority_incarnation, "
                        "reporter_incarnation, authority_mode, allocation_epoch, "
                        "candidate_digest, candidate_identity_algorithm, candidate_identity, "
                        "candidate_publication_sha256, deployment_generation, "
                        "configuration_generation, "
                        "registration_state) VALUES "
                        "(:agent_incarnation, 1, 1, :environment_id, :subject_id, "
                        ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
                        ":authority_mode, :allocation_epoch, :candidate_digest, "
                        ":candidate_identity_algorithm, :candidate_identity, "
                        ":candidate_publication_sha256, :deployment_generation, "
                        ":configuration_generation, 'registered') "
                        "ON CONFLICT DO NOTHING RETURNING agent_incarnation"
                    ),
                    registration.model_dump(mode="python", exclude_none=False),
                )
            ).scalar_one_or_none()
            stored = await self._read_registration()
            if stored != registration:
                raise CapacityAgentStoreError(
                    "agent registration identity already has a different immutable binding"
                )
            if inserted is not None:
                await self._session.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.agent_reporter_state "
                        "(agent_incarnation, high_water) VALUES (:agent_incarnation, 0)"
                    ),
                    {"agent_incarnation": registration.agent_incarnation},
                )
                await self._session.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.audit_events "
                        "(event_type, payload, payload_digest) "
                        "VALUES ('agent_registered.v1', CAST(:payload AS jsonb), :payload_digest)"
                    ),
                    {
                        "payload": _json_payload(payload),
                        "payload_digest": payload_digest,
                    },
                )
            else:
                await self._verify_registration_audit(registration)
        return registration

    async def reconfigure_agent(
        self,
        registration: AgentRegistrationV1,
        *,
        expected_configuration_generation: int,
    ) -> AgentRegistrationV1:
        """Advance one disabled candidate/config binding without resetting sequence."""

        if (
            type(expected_configuration_generation) is not int
            or expected_configuration_generation <= 0
        ):
            raise ValueError("expected agent configuration generation must be positive")
        guard = CapacityGuardStore(
            self._session,
            expected_owner_role=self._expected_owner_role,
        )
        fence = await guard.read_guard_fence()
        await self._assert_agent_role_binding()
        mismatches = tuple(
            field
            for field in _REGISTRATION_BINDINGS
            if getattr(registration, field) != getattr(fence, field)
        )
        if mismatches:
            raise CapacityAgentStoreError(
                "agent reconfiguration differs from the guard fence: " + ", ".join(mismatches)
            )
        payload = registration.model_dump(mode="json", exclude_none=False)
        payload_digest = canonical_digest(registration)
        async with self._session.begin_nested():
            current = await self._read_registration(lock=True)
            if current == registration:
                await self._verify_registration_audit(registration)
                return registration
            if (
                current.agent_incarnation != registration.agent_incarnation
                or current.environment_id != registration.environment_id
                or current.subject_id != registration.subject_id
                or current.subject_incarnation != registration.subject_incarnation
                or current.authority_incarnation != registration.authority_incarnation
                or current.authority_mode != registration.authority_mode
                or current.allocation_epoch != registration.allocation_epoch
            ):
                raise CapacityAgentStoreError("agent reconfiguration changed an immutable binding")
            if current.configuration_generation != expected_configuration_generation:
                raise CapacityAgentStoreError("agent configuration generation was superseded")
            updated = (
                await self._session.execute(
                    text(
                        f"UPDATE {_SCHEMA}.agent_registrations SET "
                        "reporter_incarnation = :reporter_incarnation, "
                        "candidate_digest = :candidate_digest, "
                        "candidate_identity_algorithm = :candidate_identity_algorithm, "
                        "candidate_identity = :candidate_identity, "
                        "candidate_publication_sha256 = :candidate_publication_sha256, "
                        "deployment_generation = :deployment_generation, "
                        "configuration_generation = :configuration_generation "
                        "WHERE agent_incarnation = :agent_incarnation "
                        "AND configuration_generation = :expected_generation "
                        "RETURNING agent_incarnation"
                    ),
                    {
                        **registration.model_dump(mode="python", exclude_none=False),
                        "expected_generation": expected_configuration_generation,
                    },
                )
            ).scalar_one_or_none()
            if updated is None:
                raise CapacityAgentStoreError("agent configuration generation was superseded")
            await self._session.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.audit_events "
                    "(event_type, payload, payload_digest) "
                    "VALUES ('agent_reconfigured.v1', CAST(:payload AS jsonb), :payload_digest)"
                ),
                {
                    "payload": _json_payload(payload),
                    "payload_digest": payload_digest,
                },
            )
            stored = await self._read_registration(lock=True)
            if stored != registration:
                raise CapacityAgentStoreError("agent reconfiguration was not exact")
        return registration

    async def _assert_agent_role_binding(self) -> None:
        role = (
            await self._session.execute(
                text(
                    f"SELECT agent_role_name FROM {_SCHEMA}.agent_runtime_authority "
                    "WHERE singleton_id = 1"
                )
            )
        ).scalar_one_or_none()
        if role != self._expected_agent_role:
            raise CapacityAgentStoreError("protected agent role binding is missing or mismatched")

    async def _read_registration(self, *, lock: bool = False) -> AgentRegistrationV1:
        row = (
            (
                await self._session.execute(
                    text(
                        f"SELECT agent_incarnation, schema_version, environment_id, subject_id, "
                        "subject_incarnation, authority_incarnation, reporter_incarnation, "
                        "authority_mode, allocation_epoch, candidate_digest, "
                        "candidate_identity_algorithm, candidate_identity, "
                        "candidate_publication_sha256, "
                        "deployment_generation, configuration_generation "
                        f"FROM {_SCHEMA}.agent_registrations WHERE singleton_id = 1 "
                        + ("FOR UPDATE" if lock else "FOR KEY SHARE")
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CapacityAgentStoreError("agent registration insert was not observable")
        try:
            return AgentRegistrationV1.model_validate({**dict(row), "reporter_high_water": 0})
        except ValidationError as exc:
            raise CapacityAgentStoreError("stored agent registration is invalid") from exc

    async def _verify_registration_audit(self, registration: AgentRegistrationV1) -> None:
        rows = (
            (
                await self._session.execute(
                    text(
                        f"SELECT event_type, payload, payload_digest "
                        f"FROM {_SCHEMA}.audit_events "
                        "WHERE event_type IN "
                        "('agent_registered.v1', 'agent_reconfigured.v1') "
                        "ORDER BY event_id DESC LIMIT 1"
                    )
                )
            )
            .mappings()
            .all()
        )
        expected_payload = registration.model_dump(mode="json", exclude_none=False)
        if len(rows) != 1:
            raise CapacityAgentStoreError("expected a current agent registration audit")
        if rows[0]["payload"] != expected_payload or rows[0]["payload_digest"] != canonical_digest(
            registration
        ):
            raise CapacityAgentStoreError("agent registration audit does not match its binding")


async def read_agent_reporter_high_water(
    session: AsyncSession,
    *,
    registration: AgentRegistrationV1,
) -> int:
    """Read only the exact current agent's restart-safe protected sequence."""

    value = (
        await session.execute(
            text(f"SELECT {_SCHEMA}.read_agent_reporter_high_water(:agent_incarnation)"),
            {"agent_incarnation": registration.agent_incarnation},
        )
    ).scalar_one()
    if type(value) is not int or value < 0:
        raise CapacityAgentStoreError("protected reporter high-water is invalid")
    return value


async def read_agent_lifecycle_demand_observation(
    session: AsyncSession,
    *,
    registration: AgentRegistrationV1,
    sequence: int,
) -> GuardLifecycleDemandObservationV2:
    """Recover only the exact current protected observation after a restart."""

    if type(sequence) is not int or sequence <= 0:
        raise CapacityAgentStoreError("protected observation sequence must be positive")
    payload = (
        await session.execute(
            text(
                f"SELECT {_SCHEMA}.read_agent_lifecycle_demand_observation("
                ":agent_incarnation, :sequence)"
            ),
            {
                "agent_incarnation": registration.agent_incarnation,
                "sequence": sequence,
            },
        )
    ).scalar_one()
    if not isinstance(payload, Mapping):
        raise CapacityAgentStoreError("protected recovered observation is not an object")
    try:
        observation = GuardLifecycleDemandObservationV2.model_validate_json(
            _json_payload(payload).encode("ascii")
        )
    except (ValidationError, ValueError) as exc:
        raise CapacityAgentStoreError("protected recovered observation is invalid") from exc
    immutable_mismatches = tuple(
        field
        for field in (
            "environment_id",
            "subject_id",
            "subject_incarnation",
            "authority_incarnation",
            "agent_incarnation",
            "authority_mode",
            "allocation_epoch",
            "reporter_high_water",
        )
        if getattr(observation, field) != getattr(registration, field)
    )
    superseded = observation.configuration_generation < registration.configuration_generation
    invalid_transition = (
        observation.configuration_generation > registration.configuration_generation
        or observation.deployment_generation > registration.deployment_generation
        or (
            observation.deployment_generation == registration.deployment_generation
            and (
                observation.reporter_incarnation != registration.reporter_incarnation
                or observation.candidate_digest != registration.candidate_digest
            )
        )
        or (
            not superseded
            and (
                observation.deployment_generation != registration.deployment_generation
                or observation.reporter_incarnation != registration.reporter_incarnation
                or observation.candidate_digest != registration.candidate_digest
            )
        )
    )
    if immutable_mismatches or invalid_transition or observation.sequence != sequence:
        raise CapacityAgentStoreError("protected recovered observation binding is invalid")
    canonical_bytes(observation)
    return observation


async def capture_demand_observation(
    session: AsyncSession,
    *,
    registration: AgentRegistrationV1,
    expected_high_water: int,
    max_attempts: int,
) -> GuardDemandObservationV1:
    """Atomically advance the reporter sequence and parse one bounded observation."""

    if type(expected_high_water) is not int or expected_high_water < 0:
        raise CapacityAgentStoreError("expected reporter high-water must be nonnegative")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10_000:
        raise CapacityAgentStoreError("capture row bound must be between 1 and 10000")
    # Keep the protected state transition and local contract validation in one
    # savepoint. If validation fails, even a caller that catches the exception
    # cannot commit an advanced high-water mark or malformed observation.
    async with session.begin_nested():
        payload = (
            await session.execute(
                text(
                    f"SELECT {_SCHEMA}.capture_demand_observation("
                    ":agent_incarnation, :expected_high_water, :max_attempts)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "expected_high_water": expected_high_water,
                    "max_attempts": max_attempts,
                },
            )
        ).scalar_one()
        if not isinstance(payload, Mapping):
            raise CapacityAgentStoreError("protected demand capture returned a non-object")
        try:
            observation = GuardDemandObservationV1.model_validate_json(
                _json_payload(payload).encode("ascii")
            )
        except (ValidationError, ValueError) as exc:
            raise CapacityAgentStoreError(
                "protected demand capture returned an invalid contract"
            ) from exc
        mismatches = tuple(
            field
            for field in AgentRegistrationV1.model_fields
            if getattr(observation, field) != getattr(registration, field)
        )
        if mismatches:
            raise CapacityAgentStoreError(
                f"protected demand capture binding mismatch: {', '.join(mismatches)}"
            )
        if observation.sequence != expected_high_water + 1:
            raise CapacityAgentStoreError(
                "protected demand capture returned a nonmonotonic sequence"
            )
        # Exercise the same canonical bound used before manager submission.
        canonical_bytes(observation)
    return observation


async def capture_lifecycle_demand_observation(
    session: AsyncSession,
    *,
    registration: AgentRegistrationV1,
    expected_high_water: int,
    max_attempts: int,
) -> GuardLifecycleDemandObservationV2:
    """Atomically capture and validate one lifecycle-aware protected view."""

    if type(expected_high_water) is not int or expected_high_water < 0:
        raise CapacityAgentStoreError("expected reporter high-water must be nonnegative")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10_000:
        raise CapacityAgentStoreError("capture row bound must be between 1 and 10000")
    async with session.begin_nested():
        payload = (
            await session.execute(
                text(
                    f"SELECT {_SCHEMA}.capture_lifecycle_demand_observation("
                    ":agent_incarnation, :expected_high_water, :max_attempts)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "expected_high_water": expected_high_water,
                    "max_attempts": max_attempts,
                },
            )
        ).scalar_one()
        if not isinstance(payload, Mapping):
            raise CapacityAgentStoreError(
                "protected lifecycle demand capture returned a non-object"
            )
        try:
            observation = GuardLifecycleDemandObservationV2.model_validate_json(
                _json_payload(payload).encode("ascii")
            )
        except (ValidationError, ValueError) as exc:
            raise CapacityAgentStoreError(
                "protected lifecycle demand capture returned an invalid contract"
            ) from exc
        mismatches = tuple(
            field
            for field in AgentRegistrationV1.model_fields
            if getattr(observation, field) != getattr(registration, field)
        )
        if mismatches:
            raise CapacityAgentStoreError(
                f"protected lifecycle demand capture binding mismatch: {', '.join(mismatches)}"
            )
        if observation.sequence != expected_high_water + 1:
            raise CapacityAgentStoreError(
                "protected lifecycle demand capture returned a nonmonotonic sequence"
            )
        canonical_bytes(observation)
    return observation


async def read_next_executable_protected_release(
    session: AsyncSession,
    *,
    registration: AgentRegistrationV1,
) -> PublishableExecutableProtectedReleaseV2 | None:
    """Read the current agent's next protected release publication, without advancing it."""

    returned = (
        await session.execute(
            text(f"SELECT {_SCHEMA}.read_next_executable_protected_release(:agent_incarnation)"),
            {"agent_incarnation": registration.agent_incarnation},
        )
    ).scalar_one()
    if returned is None:
        return None
    if not isinstance(returned, Mapping):
        raise CapacityAgentStoreError("protected release outbox returned a non-object")
    release_payload = returned.get("release")
    if not isinstance(release_payload, Mapping):
        raise CapacityAgentStoreError("protected release outbox returned a non-release")
    try:
        release = ExecutableProtectedReleaseV2.model_validate_json(
            _json_payload(release_payload).encode("ascii")
        )
        publication = PublishableExecutableProtectedReleaseV2.model_validate(
            {
                "event_id": returned.get("event_id"),
                "event_kind": returned.get("event_kind"),
                "release": release,
                "publication_digest": canonical_executable_digest(release),
            }
        )
    except (ValidationError, ValueError) as exc:
        raise CapacityAgentStoreError("protected release outbox payload is invalid") from exc
    if (
        release.reporter_incarnation != registration.reporter_incarnation
        or release.binding.subject_id != registration.subject_id
        or release.binding.subject_incarnation != registration.subject_incarnation
        or release.binding.deployment_generation != registration.deployment_generation
        or release.binding.candidate.algorithm != registration.candidate_identity_algorithm
        or release.binding.candidate.identity != registration.candidate_identity
        or release.binding.candidate.publication_sha256 != registration.candidate_publication_sha256
    ):
        raise CapacityAgentStoreError("protected release outbox binding is invalid")
    canonical_executable_bytes(release)
    return publication


async def acknowledge_executable_protected_release_publication(
    session: AsyncSession,
    *,
    registration: AgentRegistrationV1,
    publication: PublishableExecutableProtectedReleaseV2,
    manager_acknowledgement_digest: str,
) -> ProtectedReleasePublicationCheckpointV2:
    """Advance the protected release cursor only for the exact next manager publication."""

    if not isinstance(publication, PublishableExecutableProtectedReleaseV2):
        raise TypeError("protected release publication is invalid")
    if _DIGEST_RE.fullmatch(manager_acknowledgement_digest) is None:
        raise CapacityAgentStoreError("manager acknowledgement digest is invalid")
    if canonical_executable_digest(publication.release) != publication.publication_digest:
        raise CapacityAgentStoreError("protected release publication digest changed")
    release_payload = canonical_executable_bytes(publication.release).decode("ascii")
    async with session.begin_nested():
        returned = (
            await session.execute(
                text(
                    f"SELECT {_SCHEMA}."
                    "acknowledge_executable_protected_release_publication("
                    ":agent_incarnation, :event_id, CAST(:publication_payload AS jsonb), "
                    ":publication_digest, :manager_acknowledgement_digest)"
                ),
                {
                    "agent_incarnation": registration.agent_incarnation,
                    "event_id": publication.event_id,
                    "publication_payload": release_payload,
                    "publication_digest": publication.publication_digest,
                    "manager_acknowledgement_digest": manager_acknowledgement_digest,
                },
            )
        ).scalar_one()
        if not isinstance(returned, Mapping):
            raise CapacityAgentStoreError("protected release acknowledgement returned a non-object")
        try:
            checkpoint = ProtectedReleasePublicationCheckpointV2.model_validate_json(
                _json_payload(returned).encode("ascii")
            )
        except (ValidationError, ValueError) as exc:
            raise CapacityAgentStoreError(
                "protected release acknowledgement receipt is invalid"
            ) from exc
        if (
            checkpoint.event_id != publication.event_id
            or checkpoint.event_kind != publication.event_kind
            or checkpoint.publication_digest != publication.publication_digest
            or checkpoint.manager_acknowledgement_digest != manager_acknowledgement_digest
        ):
            raise CapacityAgentStoreError("protected release acknowledgement receipt changed")
    return checkpoint


__all__ = [
    "CapacityAgentStore",
    "CapacityAgentStoreError",
    "acknowledge_executable_protected_release_publication",
    "capture_demand_observation",
    "capture_lifecycle_demand_observation",
    "read_agent_lifecycle_demand_observation",
    "read_agent_reporter_high_water",
    "read_next_executable_protected_release",
]
