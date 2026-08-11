"""Strict inert contracts for the pre-cutover legacy-authority fence.

These records describe evidence needed to stop the legacy writers.  They do
not activate legacy compatibility, global admission, claims, or capacity.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import (
    Digest,
    GuardIdentifier,
    NonNegativeSequence,
    PositiveGeneration,
    StrictGuardModel,
)

LEGACY_MUTATION_INVENTORY_DIGEST = (
    "81b48ba31d00111a532b2317248357f8af05a40b53e4b2b8bf7cd00c3ad59616"
)
MAX_LEGACY_WRITER_CURSORS = 64
LEGACY_MUTATION_PATH_IDS = (
    "batch-hard-budget-cancel",
    "batch-user-cancel",
    "dead-worker-reclaim",
    "dev-environment-destroy",
    "family-finalize-cascade",
    "legacy-compatibility-writer",
    "neutral-pool-assignment",
    "pre-start-heartbeat",
    "pre-start-retry-requeue",
    "queued-to-claimed",
    "single-trial-cancel",
    "slurm-job-launch-registry-release",
    "stale-running-failure",
    "trial-requirement-and-lifecycle-binding",
    "trial-submission",
    "worker-drain-and-release",
    "worker-heartbeat-status",
    "worker-registration",
    "worker-result-state",
    "worker-token-issuance",
)


class LegacyWriterCursorV1(StrictGuardModel):
    """One exact high-water observation for an inventoried mutation path."""

    mutation_path_id: GuardIdentifier
    writer_domain: GuardIdentifier
    writer_incarnation: UUID
    writer_epoch: PositiveGeneration
    high_water: NonNegativeSequence
    authority_digest: Digest
    observation_state: Literal["observed"] = "observed"
    freeze_supported: Literal[True] = True

    @field_validator("mutation_path_id")
    @classmethod
    def _known_mutation_path(cls, value: str) -> str:
        if value not in LEGACY_MUTATION_PATH_IDS:
            raise ValueError("legacy cursor references an unknown mutation path")
        return value


class LegacyWriterFreezeCursorV1(StrictGuardModel):
    """One exact writer acknowledgement at its frozen high-water mark."""

    mutation_path_id: GuardIdentifier
    writer_domain: GuardIdentifier
    writer_incarnation: UUID
    writer_epoch: PositiveGeneration
    high_water: NonNegativeSequence
    authority_digest: Digest
    freeze_acknowledgement_digest: Digest
    freeze_state: Literal["frozen"] = "frozen"

    @field_validator("mutation_path_id")
    @classmethod
    def _known_mutation_path(cls, value: str) -> str:
        if value not in LEGACY_MUTATION_PATH_IDS:
            raise ValueError("legacy freeze cursor references an unknown mutation path")
        return value


def _validate_cursor_inventory(
    value: tuple[LegacyWriterCursorV1, ...] | tuple[LegacyWriterFreezeCursorV1, ...],
) -> tuple[LegacyWriterCursorV1, ...] | tuple[LegacyWriterFreezeCursorV1, ...]:
    paths = frozenset(cursor.mutation_path_id for cursor in value)
    if paths != frozenset(LEGACY_MUTATION_PATH_IDS):
        raise ValueError("legacy cursors must cover the complete mutation inventory")
    identities = tuple((cursor.mutation_path_id, cursor.writer_domain) for cursor in value)
    if len(identities) != len(set(identities)):
        raise ValueError("legacy cursor mutation-path domains must be unique")
    if identities != tuple(sorted(identities)):
        raise ValueError("legacy cursors must use canonical mutation-path/domain order")
    return value


class _InertLegacyAuthorityV1(AgentRegistrationV1):
    """Common zero-authority ceiling for preparation and freeze evidence."""

    mutation_inventory_digest: Literal[
        "81b48ba31d00111a532b2317248357f8af05a40b53e4b2b8bf7cd00c3ad59616"
    ]
    activation_epoch: Literal[0] = 0
    new_submission_authority: Literal[False] = False
    new_claim_authority: Literal[False] = False
    scale_up_authority: Literal[False] = False
    cross_pool_placement_authority: Literal[False] = False
    global_allowance_authority: Literal[False] = False
    new_worker_authority: Literal[False] = False
    executable: Literal[False] = False


class LegacyCompatibilityPreparationV1(_InertLegacyAuthorityV1):
    """One inert proposal to mirror the exact current legacy authority."""

    preparation_id: UUID
    compatibility_incarnation: UUID
    fleet_migration_epoch: PositiveGeneration
    compatibility_not_after: datetime
    proposed_authority_mode: Literal["legacy-compatibility"] = "legacy-compatibility"
    compatibility_state: Literal["prepared"] = "prepared"
    writer_cursors: Annotated[
        tuple[LegacyWriterCursorV1, ...],
        Field(
            min_length=len(LEGACY_MUTATION_PATH_IDS),
            max_length=MAX_LEGACY_WRITER_CURSORS,
        ),
    ]

    @field_validator("writer_cursors")
    @classmethod
    def _complete_canonical_inventory(
        cls,
        value: tuple[LegacyWriterCursorV1, ...],
    ) -> tuple[LegacyWriterCursorV1, ...]:
        _validate_cursor_inventory(value)
        return value

    @field_validator("compatibility_not_after", mode="before")
    @classmethod
    def _parse_expiry(cls, value: datetime | str) -> datetime | str:
        if isinstance(value, str):
            timestamp = f"{value[:-1]}+00:00" if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                return value
        return value

    @field_validator("compatibility_not_after")
    @classmethod
    def _expiry_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("compatibility expiry must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _distinct_identities(self) -> LegacyCompatibilityPreparationV1:
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.preparation_id,
            self.compatibility_incarnation,
        }
        if len(identities) != 7:
            raise ValueError("legacy compatibility identities must be distinct")
        return self


class LegacyCompatibilityFreezeV1(_InertLegacyAuthorityV1):
    """Monotonic acknowledgement that every prepared writer cursor is frozen."""

    freeze_id: UUID
    preparation_id: UUID
    compatibility_incarnation: UUID
    fleet_migration_epoch: PositiveGeneration
    preparation_digest: Digest
    freeze_state: Literal["frozen"] = "frozen"
    writer_cursors: Annotated[
        tuple[LegacyWriterFreezeCursorV1, ...],
        Field(
            min_length=len(LEGACY_MUTATION_PATH_IDS),
            max_length=MAX_LEGACY_WRITER_CURSORS,
        ),
    ]

    @field_validator("writer_cursors")
    @classmethod
    def _complete_canonical_inventory(
        cls,
        value: tuple[LegacyWriterFreezeCursorV1, ...],
    ) -> tuple[LegacyWriterFreezeCursorV1, ...]:
        _validate_cursor_inventory(value)
        return value

    @model_validator(mode="after")
    def _distinct_identities(self) -> LegacyCompatibilityFreezeV1:
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.freeze_id,
            self.preparation_id,
            self.compatibility_incarnation,
        }
        if len(identities) != 8:
            raise ValueError("legacy freeze identities must be distinct")
        return self


__all__ = [
    "LEGACY_MUTATION_INVENTORY_DIGEST",
    "LEGACY_MUTATION_PATH_IDS",
    "MAX_LEGACY_WRITER_CURSORS",
    "LegacyCompatibilityFreezeV1",
    "LegacyCompatibilityPreparationV1",
    "LegacyWriterCursorV1",
    "LegacyWriterFreezeCursorV1",
]
