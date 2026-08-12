"""Durable, idempotent Pipeline GPU backend selection evidence."""

from __future__ import annotations

import json
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PipelineRun, PipelineRunGpuBackendSelection
from loom.pipeline.gpu_backend import (
    PipelineGpuSelectionError,
    PipelineRunGpuBackendSelectionV1,
    select_ordinary_gpu_backend,
    validate_gpu_selection_set,
)
from loom.pipeline.keys import canonical_document, digest_bytes


def _selection_bytes(selection: PipelineRunGpuBackendSelectionV1) -> bytes:
    return canonical_document(selection.model_dump(mode="json"))


def _validated_row(
    row: PipelineRunGpuBackendSelection,
) -> PipelineRunGpuBackendSelectionV1:
    selection = PipelineRunGpuBackendSelectionV1.model_validate_json(
        json.dumps(row.selection_json, separators=(",", ":"))
    )
    encoded = _selection_bytes(selection)
    if (
        encoded != row.selection_bytes
        or digest_bytes(encoded) != row.gpu_backend_selection_sha256
        or selection.pipeline_run_id != row.pipeline_run_id
        or selection.scope != row.scope
        or selection.variant_id != row.variant_id
        or selection.policy_id != row.policy_id
        or selection.selection_source != row.selection_source
        or selection.selected_at != row.selected_at
    ):
        raise PipelineGpuSelectionError("persisted GPU backend selection drift")
    return selection


async def get_gpu_backend_selection(
    session: AsyncSession,
    *,
    pipeline_run_id: UUID,
    scope: str,
) -> PipelineRunGpuBackendSelectionV1 | None:
    row = (
        await session.execute(
            select(PipelineRunGpuBackendSelection).where(
                PipelineRunGpuBackendSelection.pipeline_run_id == pipeline_run_id,
                PipelineRunGpuBackendSelection.scope == scope,
            )
        )
    ).scalar_one_or_none()
    return None if row is None else _validated_row(row)


async def persist_gpu_backend_selection(
    session: AsyncSession,
    selection: PipelineRunGpuBackendSelectionV1,
) -> PipelineRunGpuBackendSelectionV1:
    """Create one immutable scope row or return the byte-equal replay."""

    encoded = _selection_bytes(selection)
    digest = digest_bytes(encoded)
    values = {
        "id": uuid4(),
        "pipeline_run_id": selection.pipeline_run_id,
        "scope": selection.scope,
        "variant_id": selection.variant_id,
        "policy_id": selection.policy_id,
        "selection_source": selection.selection_source,
        "selected_at": selection.selected_at,
        "selection_json": selection.model_dump(mode="json"),
        "selection_bytes": encoded,
        "gpu_backend_selection_sha256": digest,
    }
    await session.execute(
        pg_insert(PipelineRunGpuBackendSelection)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[
                PipelineRunGpuBackendSelection.pipeline_run_id,
                PipelineRunGpuBackendSelection.scope,
            ]
        )
    )
    row = (
        await session.execute(
            select(PipelineRunGpuBackendSelection).where(
                PipelineRunGpuBackendSelection.pipeline_run_id
                == selection.pipeline_run_id,
                PipelineRunGpuBackendSelection.scope == selection.scope,
            )
        )
    ).scalar_one()
    persisted = _validated_row(row)
    if _selection_bytes(persisted) != encoded:
        raise PipelineGpuSelectionError("GPU backend scope already has different evidence")
    return persisted


async def persist_gpu_backend_selection_set(
    session: AsyncSession,
    *,
    recipe_name: str,
    selections: list[PipelineRunGpuBackendSelectionV1],
) -> tuple[PipelineRunGpuBackendSelectionV1, ...]:
    validate_gpu_selection_set(recipe_name=recipe_name, selections=selections)
    run_ids = {selection.pipeline_run_id for selection in selections}
    if len(run_ids) != 1:
        raise PipelineGpuSelectionError("GPU selection set spans Pipeline Runs")
    return tuple(
        [
            await persist_gpu_backend_selection(session, selection)
            for selection in selections
        ]
    )


async def ensure_ordinary_gpu_backend_selection(
    session: AsyncSession,
    *,
    pipeline_run_id: UUID,
    recipe_digest: str,
    selected_at: datetime,
) -> PipelineRunGpuBackendSelectionV1:
    persisted_recipe_digest = (
        await session.execute(
            select(PipelineRun.recipe_digest).where(PipelineRun.id == pipeline_run_id)
        )
    ).scalar_one_or_none()
    if persisted_recipe_digest is None or persisted_recipe_digest != recipe_digest:
        raise PipelineGpuSelectionError("Pipeline Run recipe digest drift")
    existing = await get_gpu_backend_selection(
        session,
        pipeline_run_id=pipeline_run_id,
        scope="all_gpu_nodes",
    )
    expected = select_ordinary_gpu_backend(
        recipe_digest=recipe_digest,
        pipeline_run_id=pipeline_run_id,
        selected_at=selected_at,
    )
    if existing is not None:
        if (
            existing.variant_id != expected.variant_id
            or existing.policy_id != expected.policy_id
            or existing.selection_source != "recipe_hash"
        ):
            raise PipelineGpuSelectionError("ordinary GPU backend selection drift")
        return existing
    return await persist_gpu_backend_selection(session, expected)
