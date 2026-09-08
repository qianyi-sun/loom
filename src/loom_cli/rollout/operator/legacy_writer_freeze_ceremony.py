"""Fail-closed owner ceremony for freezing legacy workload mutation writers."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, cast
from uuid import UUID

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.legacy_fence import (
    LEGACY_MUTATION_INVENTORY_DIGEST,
    LEGACY_MUTATION_PATH_IDS,
    MAX_LEGACY_WRITER_CURSORS,
    LegacyCompatibilityFreezeV1,
    LegacyCompatibilityPreparationV1,
    LegacyWriterCursorV1,
    LegacyWriterFreezeCursorV1,
)
from loom_capacity_guard.contracts import canonical_digest

from .installed_execution_authority import (
    InstalledExecutionAuthorityPublication,
    InstalledExecutionAuthorityPublisher,
    InstalledExecutionAuthorityReader,
    execution_subject_acknowledgement_sha256,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RuntimeKind = Literal["database", "deployment", "process", "timer"]
RuntimeState = Literal["active", "frozen"]


@dataclass(frozen=True, slots=True)
class LegacyWriterRuntimeObservation:
    """One live runtime observation bound to one canonical legacy cursor."""

    cursor: LegacyWriterCursorV1
    runtime_kind: RuntimeKind
    runtime_state: RuntimeState
    freeze_acknowledgement_digest: str | None

    def __post_init__(self) -> None:
        acknowledgement = self.freeze_acknowledgement_digest
        if (
            not isinstance(self.cursor, LegacyWriterCursorV1)
            or self.runtime_kind not in {"database", "deployment", "process", "timer"}
            or self.runtime_state not in {"active", "frozen"}
            or (self.runtime_state == "active" and acknowledgement is not None)
            or (
                self.runtime_state == "frozen"
                and (
                    not isinstance(acknowledgement, str)
                    or _SHA256_RE.fullmatch(acknowledgement) is None
                    or acknowledgement == "0" * 64
                )
            )
        ):
            raise ValueError("legacy writer runtime observation is invalid")


@dataclass(frozen=True, slots=True)
class LegacyWriterRuntimeSnapshot:
    """One complete, canonical observation of every legacy mutation path."""

    observations: tuple[LegacyWriterRuntimeObservation, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.observations, tuple)
            or not self.observations
            or len(self.observations) > MAX_LEGACY_WRITER_CURSORS
            or any(
                not isinstance(observation, LegacyWriterRuntimeObservation)
                for observation in self.observations
            )
        ):
            raise ValueError("legacy writer snapshot is invalid")
        paths = frozenset(observation.cursor.mutation_path_id for observation in self.observations)
        if paths != frozenset(LEGACY_MUTATION_PATH_IDS):
            raise ValueError("legacy writer snapshot must cover the complete mutation inventory")
        identities = tuple(
            (observation.cursor.mutation_path_id, observation.cursor.writer_domain)
            for observation in self.observations
        )
        if len(identities) != len(set(identities)) or identities != tuple(sorted(identities)):
            raise ValueError("legacy writer snapshot order or identity is invalid")

    @property
    def writer_cursors(self) -> tuple[LegacyWriterCursorV1, ...]:
        return tuple(observation.cursor for observation in self.observations)


@dataclass(frozen=True, slots=True)
class LegacyWriterFreezeBinding:
    """Preselected identities that make retries byte-for-byte idempotent."""

    registration: AgentRegistrationV1
    preparation_id: UUID
    freeze_id: UUID
    compatibility_incarnation: UUID
    fleet_migration_epoch: int
    compatibility_not_after: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.registration, AgentRegistrationV1)
            or not isinstance(self.preparation_id, UUID)
            or not isinstance(self.freeze_id, UUID)
            or not isinstance(self.compatibility_incarnation, UUID)
            or any(
                value.int == 0
                for value in (
                    self.preparation_id,
                    self.freeze_id,
                    self.compatibility_incarnation,
                )
            )
            or type(self.fleet_migration_epoch) is not int
            or self.fleet_migration_epoch < 1
            or not isinstance(self.compatibility_not_after, datetime)
            or self.compatibility_not_after.tzinfo is None
            or self.compatibility_not_after.utcoffset() is None
        ):
            raise ValueError("legacy writer freeze binding is invalid")


class LegacyWriterRuntimeSource(Protocol):
    def capture(self) -> LegacyWriterRuntimeSnapshot: ...


class LegacyWriterRuntimeFreezer(Protocol):
    def freeze(self, snapshot: LegacyWriterRuntimeSnapshot) -> None: ...


class LegacyWriterFencePersistence(Protocol):
    async def prepare(
        self,
        preparation: LegacyCompatibilityPreparationV1,
    ) -> LegacyCompatibilityPreparationV1: ...

    async def freeze(
        self,
        freeze: LegacyCompatibilityFreezeV1,
    ) -> LegacyCompatibilityFreezeV1: ...


PublicationFactory = Callable[
    [LegacyCompatibilityFreezeV1],
    InstalledExecutionAuthorityPublication,
]
PublicationPublisher = Callable[
    [InstalledExecutionAuthorityPublication],
    InstalledExecutionAuthorityPublication,
]


@dataclass(frozen=True, slots=True)
class LegacyWriterFreezeCeremony:
    """Freeze real writers, prove stability, persist fences, then publish authority."""

    binding: LegacyWriterFreezeBinding
    runtime_source: LegacyWriterRuntimeSource
    runtime_freezer: LegacyWriterRuntimeFreezer | Callable[[LegacyWriterRuntimeSnapshot], None]
    fence_store: LegacyWriterFencePersistence
    publication_factory: PublicationFactory
    publisher: PublicationPublisher

    def __post_init__(self) -> None:
        if (
            not isinstance(self.binding, LegacyWriterFreezeBinding)
            or not callable(getattr(self.runtime_source, "capture", None))
            or not (
                callable(getattr(self.runtime_freezer, "freeze", None))
                or callable(self.runtime_freezer)
            )
            or not callable(getattr(self.fence_store, "prepare", None))
            or not callable(getattr(self.fence_store, "freeze", None))
            or not callable(self.publication_factory)
            or not callable(self.publisher)
        ):
            raise ValueError("legacy writer freeze ceremony authority is invalid")

    async def execute(self) -> InstalledExecutionAuthorityPublication:
        before = self._capture()
        self._freeze_runtime(before)
        frozen = self._capture()
        self._assert_freeze_transition(before, frozen)

        preparation = LegacyCompatibilityPreparationV1(
            **self.binding.registration.model_dump(mode="python"),
            preparation_id=self.binding.preparation_id,
            compatibility_incarnation=self.binding.compatibility_incarnation,
            fleet_migration_epoch=self.binding.fleet_migration_epoch,
            compatibility_not_after=self.binding.compatibility_not_after,
            mutation_inventory_digest=cast(
                Literal["b9ec5d44880251d00237463a9f534199087a13f9056107078b3bac2d2d7fb1e1"],
                LEGACY_MUTATION_INVENTORY_DIGEST,
            ),
            writer_cursors=before.writer_cursors,
        )
        freeze = LegacyCompatibilityFreezeV1(
            **self.binding.registration.model_dump(mode="python"),
            freeze_id=self.binding.freeze_id,
            preparation_id=self.binding.preparation_id,
            compatibility_incarnation=self.binding.compatibility_incarnation,
            fleet_migration_epoch=self.binding.fleet_migration_epoch,
            mutation_inventory_digest=cast(
                Literal["b9ec5d44880251d00237463a9f534199087a13f9056107078b3bac2d2d7fb1e1"],
                LEGACY_MUTATION_INVENTORY_DIGEST,
            ),
            preparation_digest=canonical_digest(preparation),
            writer_cursors=tuple(
                LegacyWriterFreezeCursorV1(
                    schema_version=observation.cursor.schema_version,
                    mutation_path_id=observation.cursor.mutation_path_id,
                    writer_domain=observation.cursor.writer_domain,
                    writer_incarnation=observation.cursor.writer_incarnation,
                    writer_epoch=observation.cursor.writer_epoch,
                    high_water=observation.cursor.high_water,
                    authority_digest=observation.cursor.authority_digest,
                    freeze_acknowledgement_digest=cast(
                        str,
                        observation.freeze_acknowledgement_digest,
                    ),
                )
                for observation in frozen.observations
            ),
        )

        prepared = await self.fence_store.prepare(preparation)
        if prepared != preparation:
            raise ValueError("legacy writer preparation replay changed")
        persisted_freeze = await self.fence_store.freeze(freeze)
        if persisted_freeze != freeze:
            raise ValueError("legacy writer freeze replay changed")

        stable = self._capture()
        if stable != frozen:
            raise ValueError("legacy writer changed after database freeze")
        publication = self.publication_factory(freeze)
        if not isinstance(publication, InstalledExecutionAuthorityPublication):
            raise ValueError("legacy writer publication factory returned invalid authority")
        matching_freezes = tuple(
            item for item in publication.subject_freezes if item.subject_id == freeze.subject_id
        )
        if matching_freezes != (freeze,):
            raise ValueError("legacy writer publication omitted the exact subject freeze")
        admitted = self.publisher(publication)
        if admitted != publication:
            raise ValueError("legacy writer owner publication replay changed")
        return admitted

    def _capture(self) -> LegacyWriterRuntimeSnapshot:
        snapshot = self.runtime_source.capture()
        if not isinstance(snapshot, LegacyWriterRuntimeSnapshot):
            raise ValueError("legacy writer runtime source returned invalid evidence")
        return snapshot

    def _freeze_runtime(self, snapshot: LegacyWriterRuntimeSnapshot) -> None:
        freeze = getattr(self.runtime_freezer, "freeze", None)
        if callable(freeze):
            freeze(snapshot)
            return
        assert callable(self.runtime_freezer)
        self.runtime_freezer(snapshot)

    @staticmethod
    def _assert_freeze_transition(
        before: LegacyWriterRuntimeSnapshot,
        frozen: LegacyWriterRuntimeSnapshot,
    ) -> None:
        if len(before.observations) != len(frozen.observations):
            raise ValueError("legacy writer changed while freezing")
        for first, second in zip(before.observations, frozen.observations, strict=True):
            if first.cursor != second.cursor or first.runtime_kind != second.runtime_kind:
                raise ValueError("legacy writer changed while freezing")
            if second.runtime_state != "frozen":
                raise ValueError(
                    f"legacy writer {second.runtime_kind} "
                    f"{second.cursor.writer_domain} remains active"
                )


__all__ = [
    "InstalledExecutionAuthorityPublisher",
    "InstalledExecutionAuthorityReader",
    "LegacyWriterFreezeBinding",
    "LegacyWriterFreezeCeremony",
    "LegacyWriterRuntimeFreezer",
    "LegacyWriterRuntimeObservation",
    "LegacyWriterRuntimeSnapshot",
    "LegacyWriterRuntimeSource",
    "execution_subject_acknowledgement_sha256",
]
