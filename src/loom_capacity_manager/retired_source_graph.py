"""Bounded read-only retired-source traversal, not runtime successor admission.

One immediate source per epoch makes the graph a descending chain. Each complete
member set is authenticated bottom-up; retained own-event anchors are compared to
those authenticated exports rather than independently trusting caller pointers.
Source-bearing epochs with local events remain closed until typed consumers join.
"""

from __future__ import annotations

import json

from sqlalchemy import Text, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityExecutionEpoch,
    CapacityPersonalMembershipEvent,
)
from loom_capacity_manager.retired_application_import import _require_retired
from loom_capacity_manager.retired_member_export import _origins_from_authenticated_history
from loom_capacity_manager.retired_member_origin_contracts import (
    RetiredMembershipSnapshotReferenceV1,
)
from loom_capacity_manager.store import ConfigurationConflictError, _write_transaction
from loom_capacity_manager.typed_membership_store import _load_typed_history_node, _TypedHistory

MAX_RETIRED_SOURCE_EPOCHS = 1024
MAX_RETIRED_SOURCE_BYTES = 64 * 1024 * 1024
MAX_RETIRED_SOURCE_EVENTS = 65536
MAX_RETIRED_SOURCE_EVENT_BYTES = 64 * 1024 * 1024


async def _check_event_work_bound(session: AsyncSession, execution_epoch: int) -> None:
    """Stream only scalar sizes before the leaf loader materializes event JSON.

    Inherited nodes cannot have local events yet, so only the oldest leaf may
    contribute payloads. No JSON bodies are returned by this bounded preflight.
    """
    event = CapacityPersonalMembershipEvent
    sizes = await session.stream(select(
        func.octet_length(cast(event.request_payload, Text)) + func.octet_length(cast(event.result_payload, Text))
    ).where(event.execution_epoch == execution_epoch).order_by(event.revision)
        .limit(MAX_RETIRED_SOURCE_EVENTS + 1).execution_options(yield_per=128))
    count = byte_count = 0
    try:
        async for size in sizes.scalars():
            count += 1
            byte_count += size
            if count > MAX_RETIRED_SOURCE_EVENTS or byte_count > MAX_RETIRED_SOURCE_EVENT_BYTES:
                raise ConfigurationConflictError("retired source graph exceeds event work bound")
    finally:
        await sizes.close()


def _authenticate_source_edge(
    preparation: ExecutionPreparationV4, execution_epoch: int, history: _TypedHistory,
) -> None:
    """Compare to an authenticated predecessor node, never to mutable projection."""
    old, source = history.preparation, preparation.retired_source
    _require_retired(history.epoch)
    if (source is None or source.execution_epoch != history.epoch.execution_epoch
        or source.execution_epoch >= execution_epoch
        or preparation.authority_incarnation != old.authority_incarnation
        or preparation.configuration_epoch <= old.configuration_epoch
        or preparation.fleet_generation != old.fleet_generation or preparation.fleet_digest != old.fleet_digest
        or preparation.trusted_fleet_release_sha256 != old.trusted_fleet_release_sha256
        or preparation.personal_builds != old.personal_builds
        or preparation.personal_membership.namespace_id != old.personal_membership.namespace_id
        or preparation.personal_membership.development_template_sha256 != old.personal_membership.development_template_sha256):
        raise ConfigurationConflictError("retired source edge authority or runtime changed")
    exported = _origins_from_authenticated_history(history)
    if exported.source != source:
        raise ConfigurationConflictError("retired source edge final snapshot changed")
    actual = {origin.configuration.subject_id: canonical_bytes(origin) for origin in exported.applications}
    actual.update({origin.configuration.subject_id: canonical_bytes(origin) for origin in exported.builds})
    claimed = {origin.configuration.subject_id: canonical_bytes(origin) for origin in preparation.managed_application_origins}
    claimed.update({origin.configuration.subject_id: canonical_bytes(origin) for origin in preparation.managed_build_origins})
    if claimed != actual:
        raise ConfigurationConflictError("successor origins differ from complete retired source")


async def load_retired_source_graph(
    session: AsyncSession, source: RetiredMembershipSnapshotReferenceV1,
) -> _TypedHistory:
    """Authenticate one exact retired root in a bounded SERIALIZABLE read.

    No global cache, recursive Python calls, reporter-currentness requirement,
    configuration activation or installation mutation is involved.
    """
    if session.new or session.dirty or session.deleted:
        raise ConfigurationConflictError("retired source graph requires no pending edits")
    try:
        source = RetiredMembershipSnapshotReferenceV1.model_validate_json(canonical_bytes(source))
        async with _write_transaction(session):
            authority = (await session.scalars(select(CapacityAuthorityState).where(
                CapacityAuthorityState.singleton_id == 1).with_for_update()
                .execution_options(populate_existing=True))).one_or_none()
            if authority is None:
                raise ConfigurationConflictError("retired source graph authority is unavailable")
            references: list[RetiredMembershipSnapshotReferenceV1] = []
            reference = source
            total_bytes = 0
            while True:
                if len(references) >= MAX_RETIRED_SOURCE_EPOCHS:
                    raise ConfigurationConflictError("retired source graph exceeds epoch work bound")
                epoch = await session.get(CapacityExecutionEpoch, reference.execution_epoch, populate_existing=True)
                if epoch is None:
                    raise ConfigurationConflictError("retired source graph epoch is missing")
                _require_retired(epoch)
                payload = json.dumps(epoch.manifest_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
                total_bytes += len(payload)
                if total_bytes > MAX_RETIRED_SOURCE_BYTES:
                    raise ConfigurationConflictError("retired source graph exceeds byte work bound")
                preparation = ExecutionPreparationV4.model_validate_json(payload)
                # Full node verification below checks every durable binding; reject
                # substituted lookup refs before following their source pointer.
                if (epoch.authority_incarnation != authority.authority_incarnation
                    or epoch.execution_manifest_sha256 != reference.execution_manifest_sha256
                    or preparation.personal_membership.namespace_id != reference.namespace_id):
                    raise ConfigurationConflictError("retired source graph reference changed")
                if canonical_executable_bytes(preparation) != payload:
                    raise ConfigurationConflictError("retired source graph manifest encoding changed")
                references.append(reference)
                if preparation.retired_source is None:
                    await _check_event_work_bound(session, reference.execution_epoch)
                    break
                if preparation.retired_source.execution_epoch >= reference.execution_epoch:
                    raise ConfigurationConflictError("retired source epochs must strictly descend")
                reference = preparation.retired_source
            history: _TypedHistory | None = None
            for reference in reversed(references):
                history = await _load_typed_history_node(session, reference.execution_epoch, source_history=history)
                exported = _origins_from_authenticated_history(history)
                if exported.source != reference:
                    raise ConfigurationConflictError("retired source graph final snapshot changed")
            assert history is not None
            return history
    except (ValueError, RecursionError) as exc:
        raise ConfigurationConflictError("retired source graph evidence is invalid") from exc
