"""Trajectory index PATCH + read endpoints + event ingest (#5 Slice 3a)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import bindparam, delete, select
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB

from loom.auth import verify_bearer_token
from loom.data_lifecycle_registry import (
    bind_existing_trial_lifecycle_authority,
    ensure_artifact_lifecycle_authority,
    ensure_trial_event_lifecycle_authority,
    register_lifecycle_object,
)
from loom.db.schema import Artifact, ArtifactLineageEdge, Batch
from loom.db.schema import Trial as TrialRow
from loom.trajectory.object_identity import (
    TrajectoryObjectFilename,
    resolve_trajectory_object_key,
)

router = APIRouter()


# #5 Slice 3a: batched event ingest. Workers POST batches of typed
# trajectory events here; CP appends them to `trial_events`. The
# UNIQUE (trial_id, seq) index doubles as the idempotency key — a
# worker that retries after a partial ack gets `inserted=N` reflecting
# only the rows that actually landed, with no error on dupes.
#
# Worker fence: the row is gated on `worker_id = :worker_id` matching
# the trial's current owner — same pattern as `_INDEX_PATCH` above.
# A reclaim that nulled the trial's worker_id 409s the batch; the
# worker should give up and let the reclaim-sweep / runner reassign.
_INSERT_EVENT_SQL = sql_text("""
INSERT INTO trial_events (
    trial_id, seq, kind, source, schema_version, payload, lifecycle_authority_id
)
VALUES (
    (:trial_id)::uuid,
    (:seq)::bigint,
    (:kind)::text,
    (:source)::text,
    (:schema_version)::int,
    :payload,
    (:lifecycle_authority_id)::uuid
)
ON CONFLICT (trial_id, seq) DO NOTHING
RETURNING seq;
""").bindparams(bindparam("payload", type_=JSONB))


_MAX_BATCH = 500
_MAX_PAYLOAD_BYTES = 256 * 1024  # 256 KiB per event payload


_INDEX_PATCH = sql_text("""
UPDATE trials
   SET trajectory_index = :index_payload,
       result = CASE WHEN (:has_result)::boolean
                     THEN :result_payload ELSE result END
 WHERE id = (:trial_id)::uuid AND worker_id = (:worker_id)::uuid
 RETURNING id;
""").bindparams(
    bindparam("index_payload", type_=JSONB),
    bindparam("result_payload", type_=JSONB),
)

_VALID_SHARE_STATUS = frozenset({"pending_scan", "shared", "blocked"})


class TrajectoryLifecycleEvidenceError(RuntimeError):
    pass


def _artifact_filename(key: str) -> str:
    name = key.rstrip("/").rsplit("/", 1)[-1]
    return name or "artifact"


def _content_hash(value: Any, default: str = "pending:legacy-unhashed") -> str:
    if isinstance(value, str) and value.strip():
        text = value.strip()
        return text if ":" in text else f"sha256:{text}"
    return default


def _exact_sha256(content_hash: str) -> str:
    prefix = "sha256:"
    digest = content_hash.removeprefix(prefix)
    if not content_hash.startswith(prefix) or len(digest) != 64:
        raise TrajectoryLifecycleEvidenceError(
            "artifact object requires an exact SHA-256"
        )
    if any(ch not in "0123456789abcdef" for ch in digest):
        raise TrajectoryLifecycleEvidenceError(
            "artifact object SHA-256 must be lowercase hexadecimal"
        )
    return digest


def _object_version_id(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise TrajectoryLifecycleEvidenceError(
            f"{field} must be null or a normalized non-empty string"
        )
    return value


def _required_object_version_id(
    payload: dict[str, Any],
    *,
    key: str,
    field: str,
) -> str | None:
    if key not in payload:
        raise TrajectoryLifecycleEvidenceError(f"{field} is required")
    return _object_version_id(payload[key], field=field)


def _share_status(value: Any, default: str = "pending_scan") -> str:
    if isinstance(value, str) and value in _VALID_SHARE_STATUS:
        return value
    return default


def _policy_from_share_status(status: str) -> tuple[str, str]:
    if status == "shared":
        return "safe", "not_required"
    if status == "blocked":
        return "unsafe", "blocked"
    return "unknown", "pending"


def _trajectory_storage_from_uri(
    uri: Any,
    *,
    trial: TrialRow,
    expected_bucket: str,
    filename: TrajectoryObjectFilename,
    media_type: str,
    size_bytes: int = 0,
    version_id: Any = None,
) -> dict[str, Any]:
    try:
        key = resolve_trajectory_object_key(
            uri=uri,
            expected_bucket=expected_bucket,
            team_id=trial.team_id,
            trial_id=trial.id,
            filename=filename,
        )
    except ValueError as exc:
        raise TrajectoryLifecycleEvidenceError(str(exc)) from exc
    return {
        "backend": "object_store",
        "bucket": expected_bucket,
        "key": key,
        "media_type": media_type,
        "size_bytes": max(int(size_bytes), 0),
        "version_id": _object_version_id(
            version_id,
            field=f"{filename} version_id",
        ),
    }


def _artifact_type_from_item(item: dict[str, Any]) -> str:
    raw_type = item.get("artifact_type")
    if isinstance(raw_type, str) and raw_type:
        return raw_type
    role = item.get("role") or item.get("artifact_role")
    normalized = role.strip().lower().replace("-", "_") if isinstance(role, str) else ""
    if normalized in {"trajectory", "trajectories"}:
        return "trajectory"
    if normalized in {"report", "reports", "atif"}:
        return "atif_projection"
    if normalized in {
        "log",
        "logs",
        "diagnostic",
        "diagnostics",
        "logs_diagnostics",
        "raw",
        "raw_diagnostic",
        "raw_diagnostics",
        "internal_diagnostics",
    }:
        return "debug_bundle"
    key = item.get("key")
    key_text = key.lower() if isinstance(key, str) else ""
    if key_text.endswith("atif.json") or "report" in key_text:
        return "atif_projection"
    if key_text.endswith("events.jsonl") or "trajectory" in key_text:
        return "trajectory"
    if any(marker in key_text for marker in ("debug", "raw", "internal", "log")):
        return "debug_bundle"
    return "evidence_bundle"


def _artifact_storage_from_item(
    item: dict[str, Any],
    *,
    default_bucket: str = "artifacts",
) -> dict[str, Any] | None:
    key = item.get("key")
    if not isinstance(key, str) or not key:
        return None
    bucket = item.get("bucket")
    raw_size = item.get("size_bytes", item.get("size"))
    try:
        size_bytes = max(int(raw_size), 0) if raw_size is not None else 0
    except (TypeError, ValueError):
        size_bytes = 0
    media_type = item.get("media_type")
    return {
        "backend": "object_store",
        "bucket": bucket if isinstance(bucket, str) and bucket else default_bucket,
        "key": key,
        "media_type": (
            media_type if isinstance(media_type, str) and media_type
            else "application/octet-stream"
        ),
        "size_bytes": size_bytes,
        "version_id": _required_object_version_id(
            item,
            key="version_id",
            field="artifact version_id",
        ),
    }


def _artifact_provenance(
    trial: TrialRow,
    *,
    relation: str = "produced_from",
) -> dict[str, Any]:
    return {
        "batch_id": str(trial.batch_id) if trial.batch_id else None,
        "trial_id": str(trial.id),
        "source_trial_ids": [str(trial.id)],
        "relation": relation,
    }


def _artifact_descriptor_base(
    trial: TrialRow,
    batch: Batch | None,
    *,
    artifact_type: str,
    name: str,
    content_hash: str,
    storage: dict[str, Any],
    share_status: str,
    safety_state: str,
    redaction_state: str,
    blocked_reason: str | None,
    retention_class: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    created_by = {
        "kind": "trial",
        "batch_id": str(trial.batch_id) if trial.batch_id else None,
        "trial_id": str(trial.id),
    }
    return {
        "artifact_type": artifact_type,
        "artifact_schema_version": "1.0",
        "name": name,
        "team_id": trial.team_id,
        "batch_id": trial.batch_id,
        "trial_id": trial.id,
        "created_by": created_by,
        "content_hash": content_hash,
        "storage": storage,
        "visibility": trial.visibility or (batch.visibility if batch else "team"),
        "share_status": share_status,
        "redaction_state": redaction_state,
        "safety_state": safety_state,
        "blocked_reason": blocked_reason,
        "retention": {"class": retention_class, "expires_at": None},
        "provenance": _artifact_provenance(trial),
        "artifact_metadata": metadata or {},
    }


def _artifact_descriptors_from_index(
    trial: TrialRow,
    batch: Batch | None,
    index_payload: dict[str, Any],
    *,
    artifacts_bucket: str = "artifacts",
    trajectories_bucket: str = "trajectories",
) -> list[dict[str, Any]]:
    descriptors: list[dict[str, Any]] = []
    trial_share_status = _share_status(trial.share_status, "pending_scan")
    trial_safety, trial_redaction = _policy_from_share_status(trial_share_status)

    if index_payload.get("trajectory_uri"):
        descriptors.append(_artifact_descriptor_base(
            trial,
            batch,
            artifact_type="trajectory",
            name="Trajectory events",
            content_hash=_content_hash(
                index_payload.get("trajectory_sha256")
                or index_payload.get("checksum_sha256")
            ),
            storage=_trajectory_storage_from_uri(
                index_payload.get("trajectory_uri"),
                trial=trial,
                expected_bucket=trajectories_bucket,
                filename="events.jsonl",
                media_type="application/x-ndjson",
                size_bytes=index_payload.get("trajectory_size_bytes", 0),
                version_id=_required_object_version_id(
                    index_payload,
                    key="trajectory_version_id",
                    field="events.jsonl version_id",
                ),
            ),
            share_status=trial_share_status,
            safety_state=trial_safety,
            redaction_state=trial_redaction,
            blocked_reason=None,
            retention_class="release_evidence",
        ))

    if index_payload.get("atif_uri"):
        descriptors.append(_artifact_descriptor_base(
            trial,
            batch,
            artifact_type="atif_projection",
            name="ATIF projection",
            content_hash=_content_hash(index_payload.get("atif_sha256")),
            storage=_trajectory_storage_from_uri(
                index_payload.get("atif_uri"),
                trial=trial,
                expected_bucket=trajectories_bucket,
                filename="atif.json",
                media_type="application/json",
                size_bytes=index_payload.get("atif_size_bytes", 0),
                version_id=_required_object_version_id(
                    index_payload,
                    key="atif_version_id",
                    field="atif.json version_id",
                ),
            ),
            share_status=trial_share_status,
            safety_state=trial_safety,
            redaction_state=trial_redaction,
            blocked_reason=None,
            retention_class="release_evidence",
            metadata={
                "atif_schema_version": index_payload.get("atif_schema_version"),
            },
        ))

    artifacts = index_payload.get("artifacts")
    if isinstance(artifacts, list):
        for item in artifacts:
            if not isinstance(item, dict):
                continue
            storage = _artifact_storage_from_item(
                item,
                default_bucket=artifacts_bucket,
            )
            if storage is None:
                continue
            status = _share_status(item.get("share_status"), "pending_scan")
            safety_state, redaction_state = _policy_from_share_status(status)
            blocked_reason = item.get("blocked_reason")
            descriptors.append(_artifact_descriptor_base(
                trial,
                batch,
                artifact_type=_artifact_type_from_item(item),
                name=str(item.get("name") or _artifact_filename(storage["key"])),
                content_hash=_content_hash(item.get("content_hash")),
                storage=storage,
                share_status=status,
                safety_state=safety_state,
                redaction_state=redaction_state,
                blocked_reason=(
                    blocked_reason
                    if isinstance(blocked_reason, str) and blocked_reason
                    else None
                ),
                retention_class=(
                    "owner_only_debug"
                    if safety_state == "unsafe" or status == "blocked"
                    else "shared_reusable"
                ),
                metadata={
                    "legacy_role": item.get("role") or item.get("artifact_role"),
                    "step_name": item.get("step_name"),
                },
            ))
    return descriptors


def _artifact_storage_key(artifact: Artifact) -> str | None:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    key = storage.get("key")
    return key if isinstance(key, str) and key else None


def _source_provenance_items(
    trial: TrialRow,
    batch: Batch | None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for candidate in (batch.source_provenance if batch else None, trial.source_provenance):
        if isinstance(candidate, list):
            items.extend(item for item in candidate if isinstance(item, dict))
    return items


def _lineage_parent_specs(
    trial: TrialRow,
    batch: Batch | None,
) -> list[tuple[UUID, str, dict[str, Any]]]:
    specs: list[tuple[UUID, str, dict[str, Any]]] = []
    seen: set[tuple[UUID, str]] = set()
    for item in _source_provenance_items(trial, batch):
        parent_raw = item.get("source_artifact_id")
        if not isinstance(parent_raw, str):
            continue
        try:
            parent_id = UUID(parent_raw)
        except ValueError:
            continue
        relation = item.get("relation")
        if not isinstance(relation, str) or not relation:
            kind = item.get("kind")
            relation = (
                "reused_as_input"
                if kind == "reused_artifact"
                else "produced_from"
            )
        metadata = {
            key: value for key, value in {
                "kind": item.get("kind"),
                "source_batch_id": item.get("source_batch_id"),
                "source_trial_id": item.get("source_trial_id"),
                "source_artifact_key": item.get("source_artifact_key"),
            }.items()
            if value is not None
        }
        if (parent_id, relation) in seen:
            continue
        seen.add((parent_id, relation))
        specs.append((parent_id, relation, metadata))
    return specs


async def _sync_artifact_lineage_edges(
    session: Any,
    *,
    children: list[Artifact],
    trial: TrialRow,
    batch: Batch | None,
) -> None:
    child_ids = [artifact.id for artifact in children if artifact.id is not None]
    if not child_ids:
        return
    await session.execute(
        delete(ArtifactLineageEdge).where(
            ArtifactLineageEdge.child_artifact_id.in_(child_ids),
        ),
    )
    parent_specs = _lineage_parent_specs(trial, batch)
    if not parent_specs:
        return
    parent_ids = [parent_id for parent_id, _relation, _metadata in parent_specs]
    existing_parent_ids = set((await session.execute(
        select(Artifact.id).where(Artifact.id.in_(parent_ids)),
    )).scalars().all())
    for child_id in child_ids:
        for parent_id, relation, metadata in parent_specs:
            if parent_id not in existing_parent_ids:
                continue
            session.add(ArtifactLineageEdge(
                child_artifact_id=child_id,
                parent_artifact_id=parent_id,
                relation=relation,
                edge_metadata=metadata,
            ))


async def _sync_typed_artifacts_from_index(
    session: Any,
    *,
    trial_id: UUID,
    index_payload: dict[str, Any],
    artifacts_bucket: str = "artifacts",
    trajectories_bucket: str = "trajectories",
) -> None:
    trial = (await session.execute(
        select(TrialRow).where(TrialRow.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        return
    await bind_existing_trial_lifecycle_authority(
        session,
        trial_id=trial.id,
        expected_team_id=trial.team_id,
    )
    batch: Batch | None = None
    if trial.batch_id is not None:
        batch = (await session.execute(
            select(Batch).where(Batch.id == trial.batch_id),
        )).scalar_one_or_none()

    descriptors = _artifact_descriptors_from_index(
        trial,
        batch,
        index_payload,
        artifacts_bucket=artifacts_bucket,
        trajectories_bucket=trajectories_bucket,
    )
    if not descriptors:
        return

    existing = list((await session.execute(
        select(Artifact)
        .where(Artifact.trial_id == trial.id)
        .order_by(Artifact.created_at.asc(), Artifact.id.asc()),
    )).scalars().all())
    existing_by_key = {
        (artifact.artifact_type, _artifact_storage_key(artifact)): artifact
        for artifact in existing
        if _artifact_storage_key(artifact) is not None
    }
    synced: list[Artifact] = []
    for descriptor in descriptors:
        storage = descriptor["storage"]
        artifact_key = storage.get("key")
        artifact = existing_by_key.get(
            (descriptor["artifact_type"], artifact_key),
        )
        if artifact is None:
            artifact_id = uuid4()
            created_at = datetime.now(UTC)
            lifecycle_authority_id = await ensure_artifact_lifecycle_authority(
                session,
                artifact_id=artifact_id,
                team_id=trial.team_id,
                created_at=created_at,
            )
            artifact = Artifact(
                id=artifact_id,
                created_at=created_at,
                lifecycle_authority_id=lifecycle_authority_id,
                **descriptor,
            )
            session.add(artifact)
        else:
            lifecycle_authority_id = await ensure_artifact_lifecycle_authority(
                session,
                artifact_id=artifact.id,
                team_id=artifact.team_id,
                created_at=artifact.created_at,
            )
            if artifact.lifecycle_authority_id is None:
                artifact.lifecycle_authority_id = lifecycle_authority_id
            elif artifact.lifecycle_authority_id != lifecycle_authority_id:
                raise RuntimeError("artifact lifecycle authority conflicts")
            for field, value in descriptor.items():
                setattr(artifact, field, value)
        storage = artifact.storage if isinstance(artifact.storage, dict) else {}
        bucket = storage.get("bucket")
        object_key = storage.get("key")
        size_bytes = storage.get("size_bytes")
        version_id = storage.get("version_id")
        if (
            not isinstance(bucket, str)
            or not bucket
            or not isinstance(object_key, str)
            or not object_key
            or not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise TrajectoryLifecycleEvidenceError(
                "artifact object identity is incomplete"
            )
        await register_lifecycle_object(
            session,
            authority_id=lifecycle_authority_id,
            bucket=bucket,
            object_key=object_key,
            version_id=version_id,
            content_sha256=_exact_sha256(artifact.content_hash),
            size_bytes=size_bytes,
            created_at=artifact.created_at,
        )
        synced.append(artifact)

    await session.flush()
    await _sync_artifact_lineage_edges(
        session,
        children=synced,
        trial=trial,
        batch=batch,
    )


@router.patch("/trials/{trial_id}/trajectory_index")
async def patch_trajectory_index(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:index" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc
    result_payload = payload.get("result")
    index_payload = {
        k: v for k, v in payload.items()
        if k not in {"worker_id", "result"}
    }

    async with request.app.state.session_factory() as session:
        row = (await session.execute(_INDEX_PATCH, {
            "trial_id": trial_id, "worker_id": worker_id,
            "index_payload": index_payload,
            "result_payload": result_payload,
            "has_result": result_payload is not None,
        })).mappings().one_or_none()
        if row is not None:
            try:
                await _sync_typed_artifacts_from_index(
                    session,
                    trial_id=trial_id,
                    index_payload=index_payload,
                    artifacts_bucket=request.app.state.settings.artifacts_bucket,
                    trajectories_bucket=(
                        request.app.state.settings.trajectories_bucket
                    ),
                )
            except TrajectoryLifecycleEvidenceError as exc:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "trajectory_lifecycle_evidence_invalid",
                        "message": str(exc),
                    },
                ) from exc
        await session.commit()
    if row is None:
        raise HTTPException(status_code=409, detail="worker lost claim")
    return {"trial_id": str(row["id"])}


@router.get("/trials/{trial_id}/trajectory")
async def get_trajectory_url(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> RedirectResponse:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    async with request.app.state.session_factory() as session:
        row = (await session.execute(
            select(TrialRow).where(TrialRow.id == trial_id),
        )).scalar_one_or_none()
    if row is None or not row.trajectory_index:
        raise HTTPException(status_code=404, detail="no trajectory recorded")
    if ctx.team_id is not None and row.team_id != ctx.team_id:
        raise HTTPException(
            status_code=403, detail="trajectory belongs to another team",
        )

    settings = request.app.state.settings
    index = row.trajectory_index if isinstance(row.trajectory_index, dict) else {}
    try:
        key = resolve_trajectory_object_key(
            uri=index.get("trajectory_uri"),
            expected_bucket=settings.trajectories_bucket,
            team_id=row.team_id,
            trial_id=trial_id,
            filename="events.jsonl",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "trajectory_object_identity_invalid",
                "message": str(exc),
            },
        ) from exc
    url = request.app.state.minio_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": settings.trajectories_bucket,
            "Key": key,
        },
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return RedirectResponse(url=url, status_code=302)


@router.post("/trials/{trial_id}/events")
async def append_events(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Append a batch of typed trajectory events to `trial_events`.

    Body shape:
        {
            "worker_id": "<uuid>",
            "events": [
                {
                    "seq": 0,
                    "kind": "trial_start",
                    "source": "worker",
                    "schema_version": 1,
                    "payload": {<TrajectoryEvent body>},
                },
                ...
            ],
        }

    Per-event behavior:
    - INSERT ... ON CONFLICT (trial_id, seq) DO NOTHING
    - Duplicates from worker retries return inserted=N reflecting only
      newly-landed rows; no error.
    - Worker fence: the trial's current `worker_id` must match the
      `worker_id` in the body. Mismatch = 409 (worker lost claim);
      writers should give up and let reclaim re-route.

    Limits:
    - At most `_MAX_BATCH` events per request (500).
    - Each event's payload at most `_MAX_PAYLOAD_BYTES` (256 KiB).
    - Both limits are 413 / 400 respectively — bigger payloads or
      bigger batches indicate an upstream bug, not a normal flow.
    """
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:index" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc

    events = payload.get("events")
    if not isinstance(events, list) or not events:
        raise HTTPException(
            status_code=400, detail="events must be a non-empty list",
        )
    if len(events) > _MAX_BATCH:
        raise HTTPException(
            status_code=413,
            detail=f"batch too large: {len(events)} > {_MAX_BATCH}",
        )

    # Pre-validate every event up front so we either accept or reject
    # the whole batch — partial inserts followed by a 400 would force
    # workers into per-event recovery logic.
    rows: list[dict[str, Any]] = []
    for i, ev in enumerate(events):
        if not isinstance(ev, dict):
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}] must be an object",
            )
        try:
            seq = int(ev["seq"])
            kind = str(ev["kind"])
            source = str(ev["source"])
            schema_version = int(ev.get("schema_version", 1))
            evt_payload = ev["payload"]
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"events[{i}] missing/invalid field: {exc}. Required "
                    "keys: seq (int>=0), kind (str), source (str), "
                    "payload (object); optional schema_version (int>=1)."
                ),
            ) from exc
        if seq < 0:
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}].seq must be >= 0",
            )
        if schema_version < 1:
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}].schema_version must be >= 1",
            )
        if not isinstance(evt_payload, dict):
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}].payload must be an object",
            )
        # Cheap bytes-bound on payload — gate against an oversized
        # event slipping past the multipart layer. Approximate via
        # repr length; a tighter check would re-serialize but the
        # repr is close enough for the safety floor.
        if len(repr(evt_payload)) > _MAX_PAYLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"events[{i}].payload exceeds "
                    f"{_MAX_PAYLOAD_BYTES} bytes"
                ),
            )
        rows.append({
            "trial_id": trial_id,
            "seq": seq,
            "kind": kind,
            "source": source,
            "schema_version": schema_version,
            "payload": evt_payload,
        })

    # Fence check: refuse the whole batch if the trial's current
    # worker_id doesn't match. Worker reclaim nulls worker_id, so a
    # reclaim mid-batch surfaces here as a 409.
    async with request.app.state.session_factory() as session:
        owner_row = (await session.execute(
            select(TrialRow.worker_id).where(TrialRow.id == trial_id),
        )).one_or_none()
        if owner_row is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if owner_row[0] != worker_id:
            raise HTTPException(
                status_code=409, detail="worker lost claim",
            )

        lifecycle_authority_id = await ensure_trial_event_lifecycle_authority(
            session,
            trial_id=trial_id,
        )

        inserted = 0
        for row_params in rows:
            row_params["lifecycle_authority_id"] = lifecycle_authority_id
            result = await session.execute(_INSERT_EVENT_SQL, row_params)
            if result.first() is not None:
                inserted += 1
        await session.commit()

    return {
        "trial_id": str(trial_id),
        "submitted": len(rows),
        "inserted": inserted,
        "deduped": len(rows) - inserted,
    }
