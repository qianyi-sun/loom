"""One-command delivery bundle export for completed batch families (#390)."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import tarfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

from botocore.exceptions import ClientError
from sqlalchemy import select

from loom.auth import AuthContext
from loom.db.schema import Artifact, Batch, Trial

SELECTION_RULE = "highest_priority_succeeded_by_task_sample_combination"
SCHEMA_VERSION = "1"
TERMINAL_BATCH_STATES = {"finished", "cancelled"}
PAYLOAD_CHECKSUMS_FILE = "checksums/SHA256SUMS"


class DeliveryExportError(Exception):
    """Base class for user-actionable delivery export failures."""

    code = "delivery_export_failed"
    status_code = 409

    def __init__(self, detail: dict[str, Any]) -> None:
        super().__init__(self.code)
        self.detail = {"code": self.code, **detail}


class MissingDeliveryObjectsError(DeliveryExportError):
    code = "delivery_export_objects_missing"


class UnreadableDeliveryObjectsError(DeliveryExportError):
    code = "delivery_export_objects_unreadable"


class UnresolvedDeliveryTrialsError(DeliveryExportError):
    code = "delivery_export_unresolved_trials"


class InvalidDeliveryBatchFamilyError(DeliveryExportError):
    code = "delivery_export_invalid_batch_family"
    status_code = 400


@dataclass(frozen=True)
class ObjectRef:
    kind: str
    trial_id: UUID
    bucket: str
    key: str

    def as_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "trial_id": str(self.trial_id),
            "bucket": self.bucket,
            "key": self.key,
        }


@dataclass(frozen=True)
class SelectedTrial:
    trial: Trial
    batch: Batch
    priority: int
    selection_source: str
    trajectory: ObjectRef
    atif: ObjectRef
    reward: float | None

    @property
    def coordinate(self) -> tuple[str, int, int]:
        return (
            str(self.trial.task_id),
            int(self.trial.sample_idx),
            int(self.trial.combination_idx),
        )


def _json_bytes(data: Any) -> bytes:
    return (
        json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode()


def _public_json_bytes(data: Any) -> bytes:
    return (json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def _safe_slug(value: str, *, max_len: int = 96) -> str:
    out = []
    for ch in value:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("-")
    slug = "".join(out).strip("-._") or "task"
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug[:max_len].rstrip("-._") or "task"


def _extract_reward(result: dict[str, Any] | None) -> float | None:
    if not isinstance(result, dict):
        return None
    for key in ("aggregate_reward", "reward", "score"):
        raw = result.get(key)
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return None


def _reward_key(reward: float | None) -> str:
    if reward is None:
        return "null"
    if reward.is_integer():
        return f"{reward:.1f}"
    return format(reward, ".12g")


def _trial_coordinate(trial: Trial) -> tuple[str, int, int]:
    return (
        str(trial.task_id),
        int(trial.sample_idx),
        int(trial.combination_idx),
    )


def _object_error_code(exc: ClientError) -> str | None:
    code = exc.response.get("Error", {}).get("Code")
    return code if isinstance(code, str) else None


MISSING_OBJECT_ERROR_CODES = {"NoSuchBucket", "NoSuchKey", "404"}
UNREADABLE_OBJECT_ERROR_CODES = {"AccessDenied", "Forbidden", "403"}


def _object_error_detail(
    ref: ObjectRef,
    *,
    operation: str,
    exc: ClientError,
) -> dict[str, str]:
    detail = ref.as_dict()
    detail["operation"] = operation
    detail["error_code"] = _object_error_code(exc) or "unknown"
    return detail


def _parse_s3_uri(uri: str) -> tuple[str, str] | None:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        return None
    key = parsed.path.lstrip("/")
    if not key:
        return None
    return parsed.netloc, key


def _object_ref_for_trial(
    trial: Trial,
    *,
    kind: str,
    trajectories_bucket: str,
) -> ObjectRef:
    index = trial.trajectory_index if isinstance(trial.trajectory_index, dict) else {}
    uri_key = "trajectory_uri" if kind == "trajectory" else "atif_uri"
    parsed = None
    raw_uri = index.get(uri_key)
    if isinstance(raw_uri, str):
        parsed = _parse_s3_uri(raw_uri)
    if parsed is None:
        filename = "events.jsonl" if kind == "trajectory" else "atif.json"
        parsed = (trajectories_bucket, f"{trial.team_id}/{trial.id}/{filename}")
    bucket, key = parsed
    return ObjectRef(kind=kind, trial_id=trial.id, bucket=bucket, key=key)


def _model_info(batch: Batch) -> tuple[str | None, str | None]:
    config = batch.trial_config if isinstance(batch.trial_config, dict) else {}
    model = config.get("agent_model")
    if isinstance(model, dict):
        provider = model.get("provider")
        name = model.get("name")
        return (
            str(provider) if provider is not None else None,
            str(name) if name is not None else batch.provider_model_id,
        )
    if batch.provider_model_id:
        return None, batch.provider_model_id
    return None, None


async def _load_batch(session: Any, batch_id: UUID) -> Batch:
    batch = (
        await session.execute(select(Batch).where(Batch.id == batch_id))
    ).scalar_one_or_none()
    if batch is None:
        raise InvalidDeliveryBatchFamilyError({"message": "batch not found"})
    return cast(Batch, batch)


async def _auto_supplemental_batches(session: Any, main_batch_id: UUID) -> list[Batch]:
    seen = {main_batch_id}
    frontier = [main_batch_id]
    out: list[Batch] = []
    while frontier:
        rows = list(
            (
                await session.execute(
                    select(Batch)
                    .where(Batch.rerun_of_batch_id.in_(frontier))
                    .order_by(Batch.created_at.asc(), Batch.id.asc()),
                )
            )
            .scalars()
            .all()
        )
        frontier = []
        for batch in rows:
            if batch.id in seen:
                continue
            seen.add(batch.id)
            frontier.append(batch.id)
            out.append(batch)
    return out


async def _load_batch_family(
    session: Any,
    main_batch_id: UUID,
    supplemental_batch_ids: list[UUID] | None,
) -> tuple[Batch, list[Batch]]:
    main = await _load_batch(session, main_batch_id)
    if supplemental_batch_ids is None:
        supplements = await _auto_supplemental_batches(session, main_batch_id)
    else:
        supplements = []
        for batch_id in supplemental_batch_ids:
            if batch_id == main_batch_id:
                raise InvalidDeliveryBatchFamilyError(
                    {"message": "supplemental_batch_ids must not include the main batch"}
                )
            supplements.append(await _load_batch(session, batch_id))
    for batch in supplements:
        if batch.team_id != main.team_id:
            raise InvalidDeliveryBatchFamilyError(
                {
                    "message": "all supplemental batches must belong to the main batch team",
                    "batch_id": str(batch.id),
                }
            )
    for batch in [main, *supplements]:
        if str(batch.state) not in TERMINAL_BATCH_STATES:
            raise InvalidDeliveryBatchFamilyError(
                {
                    "message": "batch family must be terminal before delivery export",
                    "batch_id": str(batch.id),
                    "state": str(batch.state),
                }
            )
    if supplemental_batch_ids is not None:
        allowed_parents = {main.id}
        for batch in supplements:
            if batch.rerun_of_batch_id not in allowed_parents:
                raise InvalidDeliveryBatchFamilyError(
                    {
                        "message": (
                            "supplemental batches must be linked rerun descendants "
                            "of the main batch in priority order"
                        ),
                        "batch_id": str(batch.id),
                        "rerun_of_batch_id": (
                            str(batch.rerun_of_batch_id)
                            if batch.rerun_of_batch_id is not None
                            else None
                        ),
                    }
                )
            allowed_parents.add(batch.id)
    return main, supplements


async def _trials_for_batches(
    session: Any,
    batch_ids: list[UUID],
) -> dict[UUID, list[Trial]]:
    rows = list(
        (
            await session.execute(
                select(Trial)
                .where(Trial.batch_id.in_(batch_ids))
                .order_by(
                    Trial.task_id.asc(),
                    Trial.sample_idx.asc(),
                    Trial.combination_idx.asc(),
                    Trial.submitted_at.asc(),
                    Trial.id.asc(),
                ),
            )
        )
        .scalars()
        .all()
    )
    out: dict[UUID, list[Trial]] = {batch_id: [] for batch_id in batch_ids}
    for trial in rows:
        if trial.batch_id is not None:
            out.setdefault(trial.batch_id, []).append(trial)
    return out


def _select_trials(
    *,
    main: Batch,
    supplements: list[Batch],
    trials_by_batch: dict[UUID, list[Trial]],
    trajectories_bucket: str,
) -> list[SelectedTrial]:
    batches = [main, *supplements]
    priority_by_batch = {batch.id: index for index, batch in enumerate(batches)}
    main_keys = {_trial_coordinate(trial) for trial in trials_by_batch.get(main.id, [])}
    if not main_keys:
        raise UnresolvedDeliveryTrialsError(
            {"message": "main batch has no trials", "unresolved_trials": []}
        )

    selected_by_key: dict[tuple[str, int, int], SelectedTrial] = {}
    unresolved: list[dict[str, Any]] = []
    all_by_key: dict[tuple[str, int, int], list[Trial]] = {key: [] for key in main_keys}
    for batch in batches:
        for trial in trials_by_batch.get(batch.id, []):
            key = _trial_coordinate(trial)
            if key in all_by_key:
                all_by_key[key].append(trial)
            if key not in main_keys or str(trial.state) != "succeeded":
                continue
            priority = priority_by_batch[batch.id]
            current = selected_by_key.get(key)
            if current is not None and current.priority > priority:
                continue
            if current is not None and current.priority == priority:
                current_submitted = current.trial.submitted_at or datetime.min.replace(tzinfo=UTC)
                trial_submitted = trial.submitted_at or datetime.min.replace(tzinfo=UTC)
                if (trial_submitted, str(trial.id)) <= (current_submitted, str(current.trial.id)):
                    continue
            reward = _extract_reward(trial.result)
            selected_by_key[key] = SelectedTrial(
                trial=trial,
                batch=batch,
                priority=priority,
                selection_source="main" if batch.id == main.id else "supplemental",
                trajectory=_object_ref_for_trial(
                    trial,
                    kind="trajectory",
                    trajectories_bucket=trajectories_bucket,
                ),
                atif=_object_ref_for_trial(
                    trial,
                    kind="atif",
                    trajectories_bucket=trajectories_bucket,
                ),
                reward=reward,
            )

    for key in sorted(main_keys):
        if key in selected_by_key:
            continue
        candidates = all_by_key.get(key, [])
        latest = candidates[-1] if candidates else None
        unresolved.append(
            {
                "task_id": key[0],
                "sample_idx": key[1],
                "combination_idx": key[2],
                "latest_trial_id": str(latest.id) if latest is not None else None,
                "latest_state": str(latest.state) if latest is not None else None,
                "latest_failure_reason": latest.failure_reason if latest is not None else None,
            }
        )
    if unresolved:
        raise UnresolvedDeliveryTrialsError(
            {
                "message": "batch family has unresolved platform failures",
                "unresolved_trials": unresolved,
            }
        )
    return [
        selected_by_key[key]
        for key in sorted(selected_by_key, key=lambda item: (item[0], item[1], item[2]))
    ]


def _head_delivery_objects(client: Any, selected: list[SelectedTrial]) -> dict[str, Any]:
    missing: list[dict[str, str]] = []
    unreadable: list[dict[str, str]] = []
    checked = 0
    for item in selected:
        for ref in (item.trajectory, item.atif):
            checked += 1
            try:
                client.head_object(Bucket=ref.bucket, Key=ref.key)
            except ClientError as exc:
                code = _object_error_code(exc)
                if code in MISSING_OBJECT_ERROR_CODES:
                    missing.append(ref.as_dict())
                    continue
                if code in UNREADABLE_OBJECT_ERROR_CODES:
                    unreadable.append(
                        _object_error_detail(ref, operation="HeadObject", exc=exc),
                    )
                    continue
                raise
    if missing:
        raise MissingDeliveryObjectsError(
            {
                "message": "delivery bundle cannot be prepared because objects are missing",
                "missing_objects": missing,
            }
        )
    if unreadable:
        raise UnreadableDeliveryObjectsError(
            {
                "message": "delivery bundle cannot be prepared because objects are unreadable",
                "unreadable_objects": unreadable,
            }
        )
    return {"checked": checked, "missing": []}


def _get_object_bytes(client: Any, ref: ObjectRef) -> bytes:
    try:
        obj = client.get_object(Bucket=ref.bucket, Key=ref.key)
    except ClientError as exc:
        code = _object_error_code(exc)
        if code in MISSING_OBJECT_ERROR_CODES:
            raise MissingDeliveryObjectsError(
                {
                    "message": (
                        "delivery bundle cannot be prepared because objects "
                        "became missing during archive assembly"
                    ),
                    "missing_objects": [ref.as_dict()],
                }
            ) from exc
        if code in UNREADABLE_OBJECT_ERROR_CODES:
            raise UnreadableDeliveryObjectsError(
                {
                    "message": (
                        "delivery bundle cannot be prepared because objects "
                        "became unreadable during archive assembly"
                    ),
                    "unreadable_objects": [
                        _object_error_detail(ref, operation="GetObject", exc=exc),
                    ],
                }
            ) from exc
        raise
    body = obj["Body"]
    try:
        return bytes(body.read())
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _ledger_rows(selected: list[SelectedTrial]) -> list[dict[str, Any]]:
    rows = []
    for index, item in enumerate(selected, start=1):
        task_slug = _safe_slug(item.trial.task_id)
        prefix = f"{index:05d}-{task_slug}-{item.trial.id}"
        rows.append(
            {
                "task_id": item.trial.task_id,
                "sample_idx": int(item.trial.sample_idx),
                "combination_idx": int(item.trial.combination_idx),
                "selected_trial_id": str(item.trial.id),
                "selected_batch_id": str(item.batch.id),
                "selection_priority": item.priority,
                "selection_source": item.selection_source,
                "state": item.trial.state,
                "reward": item.reward,
                "trajectory_bucket": item.trajectory.bucket,
                "trajectory_key": item.trajectory.key,
                "atif_bucket": item.atif.bucket,
                "atif_key": item.atif.key,
                "trajectory_file": f"trajectories/{prefix}-events.jsonl",
                "atif_file": f"atif/{prefix}-atif.json",
            }
        )
    return rows


def _csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    fieldnames = [
        "task_id",
        "sample_idx",
        "combination_idx",
        "selected_trial_id",
        "selected_batch_id",
        "selection_priority",
        "selection_source",
        "state",
        "reward",
        "trajectory_bucket",
        "trajectory_key",
        "atif_bucket",
        "atif_key",
        "trajectory_file",
        "atif_file",
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


def _summary(
    *,
    main: Batch,
    supplements: list[Batch],
    selected: list[SelectedTrial],
    object_validation: dict[str, Any],
) -> dict[str, Any]:
    provider, model = _model_info(main)
    reward_distribution: dict[str, int] = {}
    source_counts = {str(batch.id): 0 for batch in [main, *supplements]}
    for item in selected:
        reward_distribution[_reward_key(item.reward)] = (
            reward_distribution.get(_reward_key(item.reward), 0) + 1
        )
        source_counts[str(item.batch.id)] = source_counts.get(str(item.batch.id), 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ready",
        "selection_rule": SELECTION_RULE,
        "batch_family": {
            "main_batch_id": str(main.id),
            "supplemental_batch_ids": [str(batch.id) for batch in supplements],
        },
        "task_count": len({item.trial.task_id for item in selected}),
        "trial_count": len(selected),
        "source_counts": source_counts,
        "reward_distribution": dict(sorted(reward_distribution.items())),
        "model_provider": provider,
        "model_name": model,
        "object_counts": {
            "atif": len(selected),
            "trajectory": len(selected),
        },
        "object_validation": object_validation,
        "created_at": datetime.now(UTC).isoformat(),
    }


def _add_tar_bytes(tar: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(data))


def _payload_checksums_bytes(files: dict[str, bytes]) -> bytes:
    lines = [
        f"{hashlib.sha256(data).hexdigest()}  {name}\n"
        for name, data in sorted(files.items())
    ]
    return "".join(lines).encode()


def _build_archive(
    *,
    client: Any,
    manifest: dict[str, Any],
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    selected: list[SelectedTrial],
) -> bytes:
    files: dict[str, bytes] = {
        "manifest.json": _public_json_bytes(manifest),
        "summary.json": _public_json_bytes(summary),
        "ledger/trials.jsonl": b"".join(_json_bytes(row) for row in rows),
        "ledger/trials.csv": _csv_bytes(rows),
    }
    for row, item in zip(rows, selected, strict=True):
        files[str(row["trajectory_file"])] = _get_object_bytes(client, item.trajectory)
        files[str(row["atif_file"])] = _get_object_bytes(client, item.atif)
    files[PAYLOAD_CHECKSUMS_FILE] = _payload_checksums_bytes(files)

    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            for name in sorted(files):
                _add_tar_bytes(tar, name, files[name])
    return raw.getvalue()


def _archive_filename(batch: Batch) -> str:
    return f"{_safe_slug(batch.name, max_len=120)}-delivery.tar.gz"


def _artifact_response(
    *,
    artifact: Artifact,
    batch_id: UUID,
    manifest: dict[str, Any],
    sha256: str,
) -> dict[str, Any]:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    filename = str(storage.get("filename") or artifact.name)
    return {
        "id": str(artifact.id),
        "status": "ready",
        "archive_filename": filename,
        "sha256": sha256,
        "download_url": (
            f"/api/v1/batches/{batch_id}/delivery-export/{artifact.id}/download"
        ),
        "manifest": manifest,
        "object_validation": manifest.get("object_validation", {"checked": 0, "missing": []}),
        "storage": {
            "bucket": storage.get("bucket"),
            "key": storage.get("key"),
            "size_bytes": storage.get("size_bytes"),
        },
        "created_at": artifact.created_at.isoformat() if artifact.created_at else None,
    }


async def latest_delivery_export(
    session: Any,
    *,
    batch_id: UUID,
) -> dict[str, Any]:
    artifacts = list(
        (
            await session.execute(
                select(Artifact)
                .where(
                    Artifact.batch_id == batch_id,
                    Artifact.artifact_type == "trajectory_bundle",
                )
                .order_by(Artifact.created_at.desc(), Artifact.id.desc()),
            )
        )
        .scalars()
        .all()
    )
    for artifact in artifacts:
        metadata = artifact.artifact_metadata if isinstance(artifact.artifact_metadata, dict) else {}
        delivery = metadata.get("delivery_export")
        if not isinstance(delivery, dict):
            continue
        manifest = delivery.get("manifest")
        if not isinstance(manifest, dict):
            continue
        sha256 = str(delivery.get("sha256") or artifact.content_hash).removeprefix("sha256:")
        return _artifact_response(
            artifact=artifact,
            batch_id=batch_id,
            manifest=manifest,
            sha256=sha256,
        )
    return {"status": "not_ready", "reason": "no_delivery_export"}


async def load_delivery_artifact(
    session: Any,
    *,
    batch_id: UUID,
    artifact_id: UUID,
) -> Artifact:
    artifact = (
        await session.execute(
            select(Artifact).where(
                Artifact.id == artifact_id,
                Artifact.batch_id == batch_id,
                Artifact.artifact_type == "trajectory_bundle",
            )
        )
    ).scalar_one_or_none()
    if artifact is None:
        raise InvalidDeliveryBatchFamilyError({"message": "delivery export not found"})
    return cast(Artifact, artifact)


async def create_delivery_export(
    session: Any,
    *,
    minio_client: Any,
    settings: Any,
    ctx: AuthContext,
    main_batch_id: UUID,
    supplemental_batch_ids: list[UUID] | None,
) -> dict[str, Any]:
    main, supplements = await _load_batch_family(session, main_batch_id, supplemental_batch_ids)
    batch_ids = [main.id, *[batch.id for batch in supplements]]
    trials_by_batch = await _trials_for_batches(session, batch_ids)
    selected = _select_trials(
        main=main,
        supplements=supplements,
        trials_by_batch=trials_by_batch,
        trajectories_bucket=settings.trajectories_bucket,
    )
    object_validation = _head_delivery_objects(minio_client, selected)
    rows = _ledger_rows(selected)
    summary = _summary(
        main=main,
        supplements=supplements,
        selected=selected,
        object_validation=object_validation,
    )
    archive_manifest = dict(summary)
    archive_manifest["payload_checksums"] = {
        "algorithm": "sha256",
        "file": PAYLOAD_CHECKSUMS_FILE,
        "scope": f"archive payload files excluding {PAYLOAD_CHECKSUMS_FILE}",
    }
    archive_bytes = _build_archive(
        client=minio_client,
        manifest=archive_manifest,
        summary=summary,
        rows=rows,
        selected=selected,
    )
    sha256 = hashlib.sha256(archive_bytes).hexdigest()
    manifest = dict(archive_manifest)
    manifest["archive_sha256"] = sha256
    summary["archive_sha256"] = sha256

    artifact_id = uuid4()
    filename = _archive_filename(main)
    storage_key = f"delivery-exports/{main.team_id}/{main.id}/{artifact_id}/{filename}"
    minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=storage_key,
        Body=archive_bytes,
    )
    minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=f"{storage_key}.sha256",
        Body=f"{sha256}  {filename}\n".encode(),
    )
    source_batch_ids = [str(batch_id) for batch_id in batch_ids]
    artifact = Artifact(
        id=artifact_id,
        artifact_type="trajectory_bundle",
        artifact_schema_version=SCHEMA_VERSION,
        name=filename,
        team_id=main.team_id,
        batch_id=main.id,
        trial_id=None,
        created_by={
            "kind": "delivery_export",
            "token_prefix": ctx.token_hash.hex()[:8] if ctx.token_hash else None,
            "user_id": str(ctx.user_id) if ctx.user_id else None,
        },
        content_hash=f"sha256:{sha256}",
        storage={
            "backend": "object_store",
            "bucket": settings.artifacts_bucket,
            "key": storage_key,
            "filename": filename,
            "media_type": "application/gzip",
            "size_bytes": len(archive_bytes),
            "sha256_key": f"{storage_key}.sha256",
        },
        visibility="team",
        share_status="shared",
        redaction_state="not_required",
        safety_state="safe",
        blocked_reason=None,
        retention={"policy": "keep_forever", "reason": "delivery_export"},
        provenance={
            "relation": "delivery_export",
            "selection_rule": SELECTION_RULE,
            "source_batch_ids": source_batch_ids,
            "selected_trial_ids": [str(item.trial.id) for item in selected],
        },
        artifact_metadata={
            "delivery_export": {
                "status": "ready",
                "sha256": sha256,
                "manifest": manifest,
                "task_count": manifest["task_count"],
                "trial_count": manifest["trial_count"],
                "object_counts": manifest["object_counts"],
                "object_validation": object_validation,
                "ledger_rows": len(rows),
            }
        },
    )
    session.add(artifact)
    await session.commit()
    await session.refresh(artifact)
    return _artifact_response(
        artifact=artifact,
        batch_id=main.id,
        manifest=manifest,
        sha256=sha256,
    )


def artifact_storage_for_download(artifact: Artifact) -> tuple[str, str, str]:
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    bucket = storage.get("bucket")
    key = storage.get("key")
    filename = storage.get("filename") or artifact.name
    if not isinstance(bucket, str) or not isinstance(key, str):
        raise InvalidDeliveryBatchFamilyError({"message": "delivery export storage missing"})
    return bucket, key, str(Path(str(filename)).name or artifact.name)
