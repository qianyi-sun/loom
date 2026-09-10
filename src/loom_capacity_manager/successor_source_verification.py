"""Bounded immediate-source preflight, not successor or executable admission.

The existing immutable reader still rejects recursively inherited preparations.
This verifies an initial source edge and exact set retention before the iterative
source-graph reader and purpose-aware successor consumers are connected together.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from loom_capacity_manager.models import CapacityAuthorityState
from loom_capacity_manager.retired_member_export import (
    RetiredMemberOrigins,
    export_retired_member_origins,
)
from loom_capacity_manager.store import ConfigurationConflictError, _write_transaction
from loom_capacity_manager.typed_membership_store import _load_typed_immutable_history


async def verify_successor_source(
    session: AsyncSession, preparation: ExecutionPreparationV4, *, execution_epoch: int,
) -> RetiredMemberOrigins:
    """Authenticate all source members; a caller-selected subset is never enough.

    Result is historical evidence, not a durable receipt or permission to launch.
    Static configuration, current reporters and activation retain their separate
    admission checks. Both source and candidate identities are revalidated.
    """
    if session.new or session.dirty or session.deleted:
        raise ConfigurationConflictError("successor source verification requires no pending edits")
    try:
        preparation = ExecutionPreparationV4.model_validate_json(canonical_executable_bytes(preparation))
        source = preparation.retired_source
        if (type(execution_epoch) is not int or execution_epoch <= 0
            or source is None or source.execution_epoch >= execution_epoch):
            raise ConfigurationConflictError("successor source epochs must strictly descend")
        async with _write_transaction(session):
            authority = (await session.scalars(select(CapacityAuthorityState).where(
                CapacityAuthorityState.singleton_id == 1).with_for_update()
                .execution_options(populate_existing=True))).one_or_none()
            history = await _load_typed_immutable_history(session, source.execution_epoch)
            old = history.preparation
            if (authority is None or authority.authority_incarnation != preparation.authority_incarnation
                or preparation.authority_incarnation != old.authority_incarnation
                or preparation.configuration_epoch <= old.configuration_epoch
                or preparation.fleet_generation != old.fleet_generation
                or preparation.fleet_digest != old.fleet_digest
                or preparation.trusted_fleet_release_sha256 != old.trusted_fleet_release_sha256
                or preparation.personal_builds != old.personal_builds
                or preparation.personal_membership.namespace_id != old.personal_membership.namespace_id
                or preparation.personal_membership.development_template_sha256 != old.personal_membership.development_template_sha256):
                raise ConfigurationConflictError("successor source authority, configuration or runtime changed")
            # The exporter independently checks retirement and actual installations.
            exported = await export_retired_member_origins(session, execution_epoch=source.execution_epoch,
                expected_snapshot=history.snapshot())
            if exported.source != source:
                raise ConfigurationConflictError("successor source manifest or final snapshot changed")
            actual = {origin.configuration.subject_id: canonical_bytes(origin)
                for origin in exported.applications}
            actual.update({build.configuration.subject_id: canonical_bytes(build) for build in exported.builds})
            claimed = {origin.configuration.subject_id: canonical_bytes(origin)
                for origin in preparation.managed_application_origins}
            claimed.update({build.configuration.subject_id: canonical_bytes(build) for build in preparation.managed_build_origins})
            if claimed != actual:
                raise ConfigurationConflictError("successor origins differ from the complete retired source")
            return exported
    except ValueError as exc:
        raise ConfigurationConflictError("successor source contract is invalid") from exc
