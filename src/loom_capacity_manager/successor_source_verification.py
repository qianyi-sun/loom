"""Retired source-chain preflight, not successor or executable admission.

Read-only graph traversal authenticates empty inherited epochs and exact set
retention. Source-bearing mutation and executable consumers remain closed.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from loom_capacity_manager.retired_member_export import (
    RetiredMemberOrigins,
    _origins_from_authenticated_history,
)
from loom_capacity_manager.retired_source_graph import (
    _authenticate_source_edge,
    load_retired_source_graph,
)
from loom_capacity_manager.store import ConfigurationConflictError, _write_transaction


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
            history = await load_retired_source_graph(session, source)
            _authenticate_source_edge(preparation, execution_epoch, history)
            return _origins_from_authenticated_history(history)
    except ValueError as exc:
        raise ConfigurationConflictError("successor source contract is invalid") from exc
