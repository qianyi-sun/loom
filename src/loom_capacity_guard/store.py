"""Privilege-separated, disabled-only protected admission persistence."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_guard.contracts import (
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
    StrictGuardModel,
    canonical_bytes,
    canonical_digest,
)

_OWNER_ROLE_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SCHEMA = "loom_capacity_guard"
_MAX_AUDIT_BYTES = 16 * 1024


class CapacityGuardStoreError(RuntimeError):
    """Base error for protected-admission persistence."""


class GuardOwnerSessionError(CapacityGuardStoreError):
    """The session does not hold the exact non-login owner authority."""


class GuardNotInitializedError(CapacityGuardStoreError):
    """The disabled authority fence has not been initialized."""


class GuardReplayConflictError(CapacityGuardStoreError):
    """A replay reused a protected identity with different immutable data."""


class GuardDataIntegrityError(CapacityGuardStoreError):
    """Persisted protected data does not match its canonical contract."""


def _canonical_payload(model: StrictGuardModel) -> tuple[dict[str, Any], str]:
    encoded = canonical_bytes(model)
    payload = json.loads(encoded)
    if not isinstance(payload, dict):  # pragma: no cover - contract guarantee
        raise GuardDataIntegrityError("canonical protected payload is not an object")
    return payload, canonical_digest(model)


def _json_parameter(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _model_from_payload(
    model_type: type[GuardFenceV1] | type[ProtectedAttemptV1] | type[SealedRequirementsV1],
    payload: object,
    *,
    label: str,
) -> GuardFenceV1 | ProtectedAttemptV1 | SealedRequirementsV1:
    if not isinstance(payload, Mapping):
        raise GuardDataIntegrityError(f"stored {label} payload is not an object")
    try:
        model = model_type.model_validate(dict(payload))
    except ValidationError as exc:
        raise GuardDataIntegrityError(f"stored {label} payload is invalid") from exc
    canonical_payload, _ = _canonical_payload(model)
    if canonical_payload != dict(payload):
        raise GuardDataIntegrityError(f"stored {label} payload is not canonical")
    return model


class CapacityGuardStore:
    """Store immutable Package 2A state through an owner-authorized session.

    The caller owns the outer transaction and must provide a SERIALIZABLE
    ``AsyncSession`` whose current role has already been changed to the exact
    protected non-login owner. Mutations use savepoints so a replay conflict
    cannot leave a requirement or attempt fragment in the caller's transaction.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        expected_owner_role: str,
    ) -> None:
        if not _OWNER_ROLE_RE.fullmatch(expected_owner_role):
            raise GuardOwnerSessionError("expected owner role is not a canonical identifier")
        self._session = session
        self._expected_owner_role = expected_owner_role

    async def _assert_owner_session(self) -> None:
        role = (
            (
                await self._session.execute(
                    text(
                        "SELECT current_role::text AS role, rolcanlogin "
                        "FROM pg_roles WHERE rolname = current_role"
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if role is None:
            raise GuardOwnerSessionError("current role is not present in pg_roles")
        current_role = role["role"]
        if current_role != self._expected_owner_role:
            raise GuardOwnerSessionError(
                f"current role does not match the expected owner role {self._expected_owner_role!r}"
            )
        if role["rolcanlogin"] is not False:
            raise GuardOwnerSessionError("current role must be a non-login owner role")
        isolation = (await self._session.execute(text("SHOW transaction_isolation"))).scalar_one()
        if str(isolation).lower() != "serializable":
            raise GuardOwnerSessionError("protected store requires a SERIALIZABLE session")

    async def initialize_disabled_authority(self, fence: GuardFenceV1) -> GuardFenceV1:
        """Initialize the sole disabled fence, or validate an exact replay."""

        await self._assert_owner_session()
        payload, payload_digest = _canonical_payload(fence)
        if len(_json_parameter(payload).encode("ascii")) > _MAX_AUDIT_BYTES:
            raise GuardDataIntegrityError("authority audit payload exceeds its database bound")
        async with self._session.begin_nested():
            inserted = (
                await self._session.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.authority_state "
                        "(singleton_id, schema_version, environment_id, subject_id, "
                        "subject_incarnation, authority_mode, authority_incarnation, "
                        "reporter_incarnation, reporter_high_water, allocation_epoch, "
                        "deployment_generation, configuration_generation, candidate_digest) "
                        "VALUES (1, :schema_version, :environment_id, :subject_id, "
                        ":subject_incarnation, :authority_mode, :authority_incarnation, "
                        ":reporter_incarnation, :reporter_high_water, :allocation_epoch, "
                        ":deployment_generation, :configuration_generation, :candidate_digest) "
                        "ON CONFLICT (singleton_id) DO NOTHING RETURNING singleton_id"
                    ),
                    fence.model_dump(mode="python", exclude_none=False),
                )
            ).scalar_one_or_none()
            stored = await self._read_guard_fence(lock=True, require_audit=inserted is None)
            if stored != fence:
                raise GuardReplayConflictError(
                    "protected authority identity already has a different immutable binding"
                )
            if inserted is not None:
                await self._insert_audit(
                    event_type="authority_initialized.v1",
                    trial_id=None,
                    protected_attempt_id=None,
                    payload=payload,
                    payload_digest=payload_digest,
                )
        return fence

    async def reconfigure_disabled_authority(
        self,
        fence: GuardFenceV1,
        *,
        expected_configuration_generation: int,
    ) -> GuardFenceV1:
        """Advance the disabled singleton without discarding protected history."""

        await self._assert_owner_session()
        if (
            type(expected_configuration_generation) is not int
            or expected_configuration_generation <= 0
        ):
            raise ValueError("expected protected configuration generation must be positive")
        payload, payload_digest = _canonical_payload(fence)
        async with self._session.begin_nested():
            current = await self._read_guard_fence(lock=True, require_audit=True)
            if current == fence:
                return fence
            immutable = (
                "environment_id",
                "subject_id",
                "subject_incarnation",
                "authority_mode",
                "authority_incarnation",
                "reporter_high_water",
                "allocation_epoch",
            )
            if any(getattr(current, name) != getattr(fence, name) for name in immutable):
                raise GuardReplayConflictError(
                    "protected authority reconfiguration changed an immutable binding"
                )
            if current.configuration_generation != expected_configuration_generation:
                raise GuardReplayConflictError(
                    "protected authority configuration generation was superseded"
                )
            updated = (
                await self._session.execute(
                    text(
                        f"UPDATE {_SCHEMA}.authority_state SET "
                        "reporter_incarnation = :reporter_incarnation, "
                        "candidate_digest = :candidate_digest, "
                        "deployment_generation = :deployment_generation, "
                        "configuration_generation = :configuration_generation, "
                        "updated_at = statement_timestamp() "
                        "WHERE singleton_id = 1 "
                        "AND configuration_generation = :expected_generation "
                        "RETURNING singleton_id"
                    ),
                    {
                        **fence.model_dump(mode="python", exclude_none=False),
                        "expected_generation": expected_configuration_generation,
                    },
                )
            ).scalar_one_or_none()
            if updated is None:
                raise GuardReplayConflictError(
                    "protected authority configuration generation was superseded"
                )
            await self._insert_audit(
                event_type="authority_reconfigured.v1",
                trial_id=None,
                protected_attempt_id=None,
                payload=payload,
                payload_digest=payload_digest,
            )
            stored = await self._read_guard_fence(lock=True, require_audit=True)
            if stored != fence:
                raise GuardDataIntegrityError(
                    "protected authority reconfiguration was not exact"
                )
        return fence

    async def read_guard_fence(self) -> GuardFenceV1:
        """Read and integrity-check the initialized disabled authority fence."""

        await self._assert_owner_session()
        return await self._read_guard_fence(lock=False, require_audit=True)

    async def _read_guard_fence(
        self,
        *,
        lock: bool,
        require_audit: bool,
    ) -> GuardFenceV1:
        suffix = " FOR KEY SHARE" if lock else ""
        row = (
            (
                await self._session.execute(
                    text(
                        "SELECT schema_version, environment_id, subject_id, subject_incarnation, "
                        "authority_mode, authority_incarnation, reporter_incarnation, "
                        "reporter_high_water, allocation_epoch, deployment_generation, "
                        f"configuration_generation, candidate_digest FROM {_SCHEMA}.authority_state "
                        f"WHERE singleton_id = 1{suffix}"
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise GuardNotInitializedError("protected authority is not initialized")
        try:
            fence = GuardFenceV1.model_validate(dict(row))
        except ValidationError as exc:
            raise GuardDataIntegrityError("stored authority fence is invalid") from exc
        if require_audit:
            await self._verify_current_authority_audit(fence)
        return fence

    async def _verify_current_authority_audit(self, fence: GuardFenceV1) -> None:
        rows = (
            (
                await self._session.execute(
                    text(
                        f"SELECT event_type, payload, payload_digest FROM {_SCHEMA}.audit_events "
                        "WHERE event_type IN "
                        "('authority_initialized.v1', 'authority_reconfigured.v1') "
                        "AND trial_id IS NULL AND protected_attempt_id IS NULL "
                        "ORDER BY event_id DESC LIMIT 1"
                    )
                )
            )
            .mappings()
            .all()
        )
        expected_payload, expected_digest = _canonical_payload(fence)
        if len(rows) != 1:
            raise GuardDataIntegrityError("expected a current authority configuration audit")
        if (
            rows[0]["payload"] != expected_payload
            or rows[0]["payload_digest"] != expected_digest
        ):
            raise GuardDataIntegrityError(
                "current authority configuration audit does not match its binding"
            )

    async def register_trial_attempt(
        self,
        attempt: ProtectedAttemptV1,
        requirements: SealedRequirementsV1,
    ) -> ProtectedAttemptV1:
        """Atomically bind a queued protected attempt to sealed requirements."""

        await self._assert_owner_session()
        await self._read_guard_fence(lock=True, require_audit=True)
        requirement_payload, requirement_digest = _canonical_payload(requirements)
        if attempt.requirements_digest != requirement_digest:
            raise GuardReplayConflictError(
                "attempt requirements digest does not match the sealed requirements"
            )
        attempt_payload, attempt_digest = _canonical_payload(attempt)
        if len(_json_parameter(attempt_payload).encode("ascii")) > _MAX_AUDIT_BYTES:
            raise GuardDataIntegrityError("trial registration audit exceeds its database bound")

        async with self._session.begin_nested():
            await self._session.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.trial_requirements "
                    "(trial_id, schema_version, requirements_digest, requirements) "
                    "VALUES (:trial_id, 1, :requirements_digest, CAST(:requirements AS jsonb)) "
                    "ON CONFLICT (trial_id) DO NOTHING"
                ),
                {
                    "trial_id": attempt.trial_id,
                    "requirements_digest": requirement_digest,
                    "requirements": _json_parameter(requirement_payload),
                },
            )
            stored_requirements = await self._read_requirements(attempt.trial_id, lock=True)
            if stored_requirements != requirements:
                raise GuardReplayConflictError(
                    "trial already has a different immutable requirements binding"
                )

            inserted_attempt = (
                await self._session.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.trial_attempts "
                        "(protected_attempt_id, trial_id, execution_generation, "
                        "requirements_digest, claim_state, assigned_pool, assignment_epoch, "
                        "worker_id, claim_epoch) "
                        "VALUES (:protected_attempt_id, :trial_id, :execution_generation, "
                        ":requirements_digest, :claim_state, :assigned_pool, :assignment_epoch, "
                        ":worker_id, :claim_epoch) ON CONFLICT DO NOTHING "
                        "RETURNING protected_attempt_id"
                    ),
                    attempt.model_dump(mode="python", exclude_none=False),
                )
            ).scalar_one_or_none()
            stored_attempts = await self._read_attempt_conflicts(attempt, lock=True)
            if len(stored_attempts) != 1 or stored_attempts[0] != attempt:
                raise GuardReplayConflictError(
                    "protected attempt identity already has a different immutable binding"
                )
            if inserted_attempt is not None:
                await self._insert_audit(
                    event_type="trial_registered.v1",
                    trial_id=attempt.trial_id,
                    protected_attempt_id=attempt.protected_attempt_id,
                    payload=attempt_payload,
                    payload_digest=attempt_digest,
                )
            else:
                await self._verify_single_audit(
                    event_type="trial_registered.v1",
                    trial_id=attempt.trial_id,
                    protected_attempt_id=attempt.protected_attempt_id,
                    model=attempt,
                )
        return attempt

    async def read_protected_attempt(self, protected_attempt_id: UUID) -> ProtectedAttemptV1 | None:
        """Read one queued protected attempt after validating its requirement binding."""

        await self._assert_owner_session()
        await self._read_guard_fence(lock=False, require_audit=True)
        rows = await self._select_attempts(
            "a.protected_attempt_id = :protected_attempt_id",
            {"protected_attempt_id": protected_attempt_id},
            lock=False,
        )
        if not rows:
            return None
        attempt = rows[0]
        await self._verify_single_audit(
            event_type="trial_registered.v1",
            trial_id=attempt.trial_id,
            protected_attempt_id=attempt.protected_attempt_id,
            model=attempt,
        )
        return attempt

    async def _read_requirements(
        self,
        trial_id: UUID,
        *,
        lock: bool,
    ) -> SealedRequirementsV1:
        suffix = " FOR KEY SHARE" if lock else ""
        row = (
            (
                await self._session.execute(
                    text(
                        f"SELECT requirements, requirements_digest FROM {_SCHEMA}.trial_requirements "
                        f"WHERE trial_id = :trial_id{suffix}"
                    ),
                    {"trial_id": trial_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise GuardDataIntegrityError("protected requirements insert was not observable")
        model = _model_from_payload(
            SealedRequirementsV1,
            row["requirements"],
            label="requirements",
        )
        assert isinstance(model, SealedRequirementsV1)
        if row["requirements_digest"] != canonical_digest(model):
            raise GuardDataIntegrityError("stored requirements digest does not match its payload")
        return model

    async def _read_attempt_conflicts(
        self,
        attempt: ProtectedAttemptV1,
        *,
        lock: bool,
    ) -> list[ProtectedAttemptV1]:
        return await self._select_attempts(
            "a.protected_attempt_id = :protected_attempt_id OR "
            "(a.trial_id = :trial_id AND a.execution_generation = :execution_generation)",
            {
                "protected_attempt_id": attempt.protected_attempt_id,
                "trial_id": attempt.trial_id,
                "execution_generation": attempt.execution_generation,
            },
            lock=lock,
        )

    async def _select_attempts(
        self,
        predicate: str,
        parameters: Mapping[str, Any],
        *,
        lock: bool,
    ) -> list[ProtectedAttemptV1]:
        suffix = " FOR KEY SHARE OF a, r" if lock else ""
        rows = (
            (
                await self._session.execute(
                    text(
                        "SELECT a.trial_id, a.protected_attempt_id, a.execution_generation, "
                        "a.requirements_digest, a.claim_state, a.assigned_pool, "
                        "a.assignment_epoch, a.worker_id, a.claim_epoch, "
                        "r.requirements, r.requirements_digest AS sealed_digest "
                        f"FROM {_SCHEMA}.trial_attempts AS a "
                        f"JOIN {_SCHEMA}.trial_requirements AS r ON r.trial_id = a.trial_id "
                        f"AND r.requirements_digest = a.requirements_digest WHERE {predicate} "
                        f"ORDER BY a.protected_attempt_id{suffix}"
                    ),
                    dict(parameters),
                )
            )
            .mappings()
            .all()
        )
        attempts: list[ProtectedAttemptV1] = []
        for row in rows:
            requirement_model = _model_from_payload(
                SealedRequirementsV1,
                row["requirements"],
                label="requirements",
            )
            assert isinstance(requirement_model, SealedRequirementsV1)
            sealed_digest = canonical_digest(requirement_model)
            if row["sealed_digest"] != sealed_digest or row["requirements_digest"] != sealed_digest:
                raise GuardDataIntegrityError(
                    "stored attempt requirements digest does not match its payload"
                )
            values = {
                key: row[key]
                for key in (
                    "trial_id",
                    "protected_attempt_id",
                    "execution_generation",
                    "requirements_digest",
                    "claim_state",
                    "assigned_pool",
                    "assignment_epoch",
                    "worker_id",
                    "claim_epoch",
                )
            }
            try:
                attempts.append(ProtectedAttemptV1.model_validate(values))
            except ValidationError as exc:
                raise GuardDataIntegrityError("stored protected attempt is invalid") from exc
        return attempts

    async def _insert_audit(
        self,
        *,
        event_type: str,
        trial_id: UUID | None,
        protected_attempt_id: UUID | None,
        payload: Mapping[str, Any],
        payload_digest: str,
    ) -> None:
        await self._session.execute(
            text(
                f"INSERT INTO {_SCHEMA}.audit_events "
                "(event_type, trial_id, protected_attempt_id, payload, payload_digest) "
                "VALUES (:event_type, :trial_id, :protected_attempt_id, "
                "CAST(:payload AS jsonb), :payload_digest)"
            ),
            {
                "event_type": event_type,
                "trial_id": trial_id,
                "protected_attempt_id": protected_attempt_id,
                "payload": _json_parameter(payload),
                "payload_digest": payload_digest,
            },
        )

    async def _verify_single_audit(
        self,
        *,
        event_type: str,
        trial_id: UUID | None,
        protected_attempt_id: UUID | None,
        model: StrictGuardModel,
    ) -> None:
        rows = (
            (
                await self._session.execute(
                    text(
                        f"SELECT payload, payload_digest FROM {_SCHEMA}.audit_events "
                        "WHERE event_type = :event_type "
                        "AND trial_id IS NOT DISTINCT FROM :trial_id "
                        "AND protected_attempt_id IS NOT DISTINCT FROM :protected_attempt_id "
                        "ORDER BY event_id LIMIT 2"
                    ),
                    {
                        "event_type": event_type,
                        "trial_id": trial_id,
                        "protected_attempt_id": protected_attempt_id,
                    },
                )
            )
            .mappings()
            .all()
        )
        expected_payload, expected_digest = _canonical_payload(model)
        if len(rows) != 1:
            raise GuardDataIntegrityError(f"expected exactly one {event_type} audit event")
        if rows[0]["payload"] != expected_payload or rows[0]["payload_digest"] != expected_digest:
            raise GuardDataIntegrityError(f"{event_type} audit event does not match its binding")


__all__ = [
    "CapacityGuardStore",
    "CapacityGuardStoreError",
    "GuardDataIntegrityError",
    "GuardNotInitializedError",
    "GuardOwnerSessionError",
    "GuardReplayConflictError",
]
