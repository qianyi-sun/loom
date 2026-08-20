"""Team-scoped read boundary for published TerminalGen corpus versions."""

from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from loom.auth import AuthContext
from loom.db.schema import TerminalGenCorpusAlias, TerminalGenCorpusVersion
from loom_service.auth_guards import require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter()


class TerminalGenCorpusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alias: str
    alias_generation: int
    corpus_version_id: UUID
    corpus_id: str
    corpus_version: int
    corpus_version_sha256: str
    recipe_digest: str
    plan_identity_sha256: str
    task_count: int
    runtime_corpus_artifact_id: UUID
    final_audit_artifact_id: UUID
    taskset_smoke_task_count: int
    taskset_smoke_sha256: str
    taskset_smoke_size_bytes: int
    published_at: datetime
    taskset_manifest_download_path: str
    taskset_archive_download_path: str


def _team_id(ctx: AuthContext) -> UUID:
    if ctx.team_id is None:
        raise HTTPException(status_code=403, detail="team token required for corpus reads")
    return ctx.team_id


async def _visible_version(
    sc: SessionAndCtx,
    *,
    alias: str,
) -> tuple[TerminalGenCorpusAlias, TerminalGenCorpusVersion]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    team_id = _team_id(ctx)
    row = (
        await session.execute(
            select(TerminalGenCorpusAlias, TerminalGenCorpusVersion)
            .join(
                TerminalGenCorpusVersion,
                TerminalGenCorpusVersion.id == TerminalGenCorpusAlias.corpus_version_id,
            )
            .where(
                TerminalGenCorpusAlias.team_id == team_id,
                TerminalGenCorpusAlias.alias == alias,
                TerminalGenCorpusVersion.team_id == team_id,
            )
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="TerminalGen corpus was not found")
    return row[0], row[1]


def _response(
    alias: TerminalGenCorpusAlias,
    version: TerminalGenCorpusVersion,
) -> TerminalGenCorpusResponse:
    base = f"/api/v1/terminalgen-corpora/{alias.alias}/taskset-smoke"
    return TerminalGenCorpusResponse(
        alias=alias.alias,
        alias_generation=alias.generation,
        corpus_version_id=version.id,
        corpus_id=version.corpus_id,
        corpus_version=version.corpus_version,
        corpus_version_sha256=version.version_sha256,
        recipe_digest=version.recipe_digest,
        plan_identity_sha256=version.plan_identity_sha256,
        task_count=version.task_count,
        runtime_corpus_artifact_id=version.runtime_corpus_artifact_id,
        final_audit_artifact_id=version.final_audit_artifact_id,
        taskset_smoke_task_count=version.taskset_smoke_task_count,
        taskset_smoke_sha256=version.taskset_smoke_sha256,
        taskset_smoke_size_bytes=version.taskset_smoke_size_bytes,
        published_at=version.published_at,
        taskset_manifest_download_path=f"{base}/manifest",
        taskset_archive_download_path=f"{base}/archive",
    )


@router.get(
    "/terminalgen-corpora/{alias}",
    response_model=TerminalGenCorpusResponse,
)
async def get_terminalgen_corpus(alias: str, sc: SessionAndCtx) -> TerminalGenCorpusResponse:
    alias_row, version = await _visible_version(sc, alias=alias)
    return _response(alias_row, version)


def _checksum_from_head(head: dict[str, Any]) -> str | None:
    encoded = head.get("ChecksumSHA256")
    if not isinstance(encoded, str):
        return None
    try:
        value = base64.b64decode(encoded, validate=True)
    except ValueError:
        return None
    return f"sha256:{value.hex()}" if len(value) == 32 else None


def _verify_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    expected_size: int,
    expected_sha256: str,
) -> None:
    try:
        head = client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except Exception:
        try:
            head = client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="corpus object readback unavailable",
            ) from exc
    try:
        size = int(head["ContentLength"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail="corpus object readback unavailable",
        ) from exc
    observed = _checksum_from_head(head)
    if observed is None:
        try:
            response = client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            digest = hashlib.sha256()
            size = 0
            try:
                while chunk := body.read(1024 * 1024):
                    size += len(chunk)
                    if size > expected_size:
                        raise ValueError("object exceeded its published size")
                    digest.update(chunk)
            finally:
                body.close()
            observed = f"sha256:{digest.hexdigest()}"
        except Exception as exc:
            raise HTTPException(status_code=503, detail="corpus object readback unavailable") from exc
    if size != expected_size or not hmac.compare_digest(observed, expected_sha256):
        raise HTTPException(status_code=409, detail="corpus object integrity drift")


@router.get("/terminalgen-corpora/{alias}/taskset-smoke/archive")
async def download_terminalgen_taskset_smoke_archive(
    alias: str,
    request: Request,
    sc: SessionAndCtx,
) -> object:
    _alias_row, version = await _visible_version(sc, alias=alias)
    settings = request.app.state.settings
    client = request.app.state.minio_client
    _verify_object(
        client,
        bucket=settings.artifacts_bucket,
        key=version.taskset_smoke_object_key,
        expected_size=version.taskset_smoke_size_bytes,
        expected_sha256=version.taskset_smoke_sha256,
    )
    return stream_object_response(
        client=client,
        bucket=settings.artifacts_bucket,
        key=version.taskset_smoke_object_key,
        filename=f"{version.corpus_id}-v{version.corpus_version}-smoke.tar",
        artifact_kind="terminalgen_taskset_smoke",
        media_type="application/x-tar",
    )


@router.get("/terminalgen-corpora/{alias}/taskset-smoke/manifest")
async def download_terminalgen_taskset_smoke_manifest(
    alias: str,
    request: Request,
    sc: SessionAndCtx,
) -> object:
    _alias_row, version = await _visible_version(sc, alias=alias)
    settings = request.app.state.settings
    client = request.app.state.minio_client
    manifest_bytes = canonical_manifest_bytes(version.taskset_manifest_json)
    _verify_object(
        client,
        bucket=settings.artifacts_bucket,
        key=version.taskset_manifest_object_key,
        expected_size=len(manifest_bytes),
        expected_sha256=version.taskset_manifest_sha256,
    )
    return stream_object_response(
        client=client,
        bucket=settings.artifacts_bucket,
        key=version.taskset_manifest_object_key,
        filename=f"{version.corpus_id}-v{version.corpus_version}-smoke.yaml",
        artifact_kind="terminalgen_taskset_manifest",
        media_type="application/yaml",
    )


def canonical_manifest_bytes(value: dict[str, Any]) -> bytes:
    import yaml  # type: ignore[import-untyped]

    rendered = yaml.safe_dump(value, sort_keys=True)
    if not isinstance(rendered, str):
        raise ValueError("TaskSet manifest serialization failed")
    return rendered.encode("utf-8")


__all__ = ["router"]
