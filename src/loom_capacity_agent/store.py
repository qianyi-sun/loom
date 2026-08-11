"""Owner registration and agent-only monotonic demand capture."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    GuardDemandObservationV1,
    GuardLifecycleDemandObservationV2,
)
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest
from loom_capacity_guard.store import CapacityGuardStore

_SCHEMA = "loom_capacity_guard"
_ROLE_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
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
                        "candidate_digest, deployment_generation, configuration_generation, "
                        "registration_state) VALUES "
                        "(:agent_incarnation, 1, 1, :environment_id, :subject_id, "
                        ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
                        ":authority_mode, :allocation_epoch, :candidate_digest, "
                        ":deployment_generation, :configuration_generation, 'registered') "
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

    async def _read_registration(self) -> AgentRegistrationV1:
        row = (
            (
                await self._session.execute(
                    text(
                        f"SELECT agent_incarnation, schema_version, environment_id, subject_id, "
                        "subject_incarnation, authority_incarnation, reporter_incarnation, "
                        "authority_mode, allocation_epoch, candidate_digest, "
                        "deployment_generation, configuration_generation "
                        f"FROM {_SCHEMA}.agent_registrations WHERE singleton_id = 1 "
                        "FOR KEY SHARE"
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CapacityAgentStoreError("agent registration insert was not observable")
        try:
            return AgentRegistrationV1.model_validate(
                {**dict(row), "reporter_high_water": 0}
            )
        except ValidationError as exc:
            raise CapacityAgentStoreError("stored agent registration is invalid") from exc

    async def _verify_registration_audit(self, registration: AgentRegistrationV1) -> None:
        rows = (
            (
                await self._session.execute(
                    text(
                        f"SELECT payload, payload_digest FROM {_SCHEMA}.audit_events "
                        "WHERE event_type = 'agent_registered.v1' ORDER BY event_id LIMIT 2"
                    )
                )
            )
            .mappings()
            .all()
        )
        expected_payload = registration.model_dump(mode="json", exclude_none=False)
        if len(rows) != 1:
            raise CapacityAgentStoreError("expected exactly one agent registration audit")
        if (
            rows[0]["payload"] != expected_payload
            or rows[0]["payload_digest"] != canonical_digest(registration)
        ):
            raise CapacityAgentStoreError("agent registration audit does not match its binding")


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
            raise CapacityAgentStoreError("protected lifecycle demand capture returned a non-object")
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
                "protected lifecycle demand capture binding mismatch: "
                f"{', '.join(mismatches)}"
            )
        if observation.sequence != expected_high_water + 1:
            raise CapacityAgentStoreError(
                "protected lifecycle demand capture returned a nonmonotonic sequence"
            )
        canonical_bytes(observation)
    return observation


__all__ = [
    "CapacityAgentStore",
    "CapacityAgentStoreError",
    "capture_demand_observation",
    "capture_lifecycle_demand_observation",
]
