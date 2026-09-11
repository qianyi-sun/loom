"""Installer-owned runtime facts, not live membership or admission authority.

The protected installer authenticates the member and approved runtime inputs.
Retention neither activates the installation nor installs executable procedures.
Runtime agents cannot use this owner-only interface.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import TypeAdapter, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.personal_dev_build_platform_requests import (
    _installation,
    _member,
    runtime_installation_digest,
)
from loom.personal_dev_build_runtime_installation import PersonalBuildRuntimeInstallation
from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1
from loom_capacity_manager.contracts import Digest, PositiveQuantity, StrictV1Model, canonical_bytes


def _identity(subject: UUID, incarnation: UUID, deployment: int) -> UUID:
    return uuid5(NAMESPACE_URL, f"loom:personal-build-installation:{subject}:{incarnation}:{deployment}")


class BuildGuardInstallationV1(StrictV1Model):
    """Stable deployment facts; membership revision and capacity are not installation."""

    id: UUID
    owner_user_id: UUID
    subject_id: UUID
    subject_incarnation: UUID
    deployment_generation: PositiveQuantity
    candidate_generation: PositiveQuantity
    reporter_incarnation: UUID
    protected_admission_sha256: Digest
    runtime_installation_sha256: Digest
    runtime: PersonalBuildRuntimeInstallation

    @field_validator("runtime", mode="before")
    @classmethod
    def _runtime_json(cls, value: object) -> object:
        # The inherited model pre-validator exposes JSON arrays as Python lists.
        # Decode this standard dataclass in strict JSON mode so its tuples retain
        # JSON array semantics, without enabling numeric/string coercions.
        if isinstance(value, dict):
            return TypeAdapter(PersonalBuildRuntimeInstallation).validate_json(
                json.dumps(value, allow_nan=False), strict=True)
        return value

    @model_validator(mode="after")
    def _stable_identity(self) -> BuildGuardInstallationV1:
        if self.id != _identity(self.subject_id, self.subject_incarnation, self.deployment_generation):
            raise ValueError("build installation identity binding changed")
        if any(value.int == 0 for value in (self.owner_user_id, self.subject_id,
            self.subject_incarnation, self.reporter_incarnation)):
            raise ValueError("build installation identity must be nonzero")
        if self.runtime_installation_sha256 != runtime_installation_digest(self.runtime, self.protected_admission_sha256):
            raise ValueError("build installation runtime digest changed")
        return self


@dataclass(frozen=True, slots=True)
class RetainedBuildInstallation:
    document: BuildGuardInstallationV1
    wire_payload: bytes

    @property
    def id(self) -> UUID:
        return self.document.id


_COLUMNS = ("id", "owner_user_id", "subject_id", "subject_incarnation",
    "deployment_generation", "reporter_incarnation")


class BuildGuardInstallationStore:
    """Retain and verify immutable facts inside the installer's outer transaction."""

    def __init__(self, session: AsyncSession, *, expected_owner_role: str) -> None:
        if not isinstance(session, AsyncSession) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", expected_owner_role):
            raise ValueError("build installation requires an async session and canonical owner role")
        self._session = session
        self._owner = expected_owner_role

    async def _assert_owner(self) -> None:
        if not self._session.in_transaction():
            raise ValueError("build installation requires an outer transaction")
        role = (await self._session.execute(text("""
            SELECT r.rolname, r.rolcanlogin, r.rolinherit, r.rolsuper, r.rolcreatedb,
                r.rolcreaterole, r.rolreplication, r.rolbypassrls,
                EXISTS (SELECT 1 FROM pg_auth_members WHERE member=r.oid) AS memberships,
                pg_get_userbyid(n.nspowner) AS schema_owner
            FROM pg_roles r JOIN pg_namespace n ON n.nspname='loom_capacity_build_guard'
            WHERE r.rolname=current_role
        """))).mappings().one_or_none()
        if role is None or role["rolname"] != self._owner or role["schema_owner"] != self._owner or any(
            role[flag] for flag in ("rolcanlogin", "rolinherit", "rolsuper", "rolcreatedb",
                "rolcreaterole", "rolreplication", "rolbypassrls", "memberships")
        ):
            raise ValueError("build installation requires the exact non-login schema owner")
        if await self._session.scalar(text("SHOW transaction_isolation")) != "serializable":
            raise ValueError("build installation requires a SERIALIZABLE transaction")

    async def retain(self, *, member: PersonalBuildMemberV1,
        runtime: PersonalBuildRuntimeInstallation,
    ) -> RetainedBuildInstallation:
        """Exact replay survives capacity changes but never rebinds a deployment."""
        await self._assert_owner()
        member = _member(member)
        config = member.configuration
        document = BuildGuardInstallationV1(
            id=_identity(config.subject_id, config.subject_incarnation, config.deployment_generation),
            owner_user_id=member.owner_id, subject_id=config.subject_id,
            subject_incarnation=config.subject_incarnation, deployment_generation=config.deployment_generation,
            candidate_generation=config.candidate_generation, reporter_incarnation=config.demand_reporter_incarnation,
            protected_admission_sha256=member.acknowledgement.protected_admission_sha256,
            runtime_installation_sha256=_installation(member, runtime), runtime=runtime)
        wire = canonical_bytes(document)
        parameters = {field: getattr(document, field) for field in _COLUMNS}
        parameters.update(payload=wire.decode("ascii"), wire=wire, digest=sha256(wire).hexdigest())
        async with self._session.begin_nested():
            await self._session.execute(text("""
                INSERT INTO loom_capacity_build_guard.installations
                    (id, owner_user_id, subject_id, subject_incarnation, deployment_generation,
                     reporter_incarnation, payload, wire_payload, payload_sha256)
                VALUES (:id, :owner_user_id, :subject_id, :subject_incarnation, :deployment_generation,
                    :reporter_incarnation, CAST(:payload AS jsonb), :wire, :digest)
                ON CONFLICT (id) DO NOTHING
            """), parameters)
            retained = await self.read(document.id)
            if retained is None or retained.wire_payload != wire:
                raise ValueError("build installation replay binding changed")
        return retained

    async def read(self, installation_id: UUID) -> RetainedBuildInstallation | None:
        """Lock and verify the retained document, canonical bytes and scalar pins."""
        await self._assert_owner()
        row = (await self._session.execute(text("""
            SELECT * FROM loom_capacity_build_guard.installations WHERE id=:id FOR UPDATE
        """), {"id": installation_id})).mappings().one_or_none()
        if row is None:
            return None
        wire = bytes(row["wire_payload"])
        document = BuildGuardInstallationV1.model_validate_json(wire)
        if (canonical_bytes(document) != wire or sha256(wire).hexdigest() != row["payload_sha256"]
            or document.model_dump(mode="json") != row["payload"]
            or any(getattr(document, field) != row[field] for field in _COLUMNS)):
            raise ValueError("build installation canonical payload or scalar binding changed")
        return RetainedBuildInstallation(document=document, wire_payload=wire)
