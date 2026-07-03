"""TaskSet materialization orchestration (#242 sub-plan 3)."""

from __future__ import annotations

import json
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import tomli_w
from pydantic import ValidationError

from loom.models.task import TaskConfig, normalize_steps
from loom.models.task_checksum import task_checksum
from loom.models.taskset import TaskSetVerifier, UserTaskSetManifest
from loom.task_bundle_compat import (
    CompatibilitySeverity,
    TaskBundleCompatibilityIssue,
    collect_task_dir_compatibility_issues,
)
from loom.taskset.instance_mapping import MappingError, resolve_mapping
from loom.taskset.status import cap_error_summary, compute_task_set_status
from loom.taskset.template_render import render_task_template
from loom.taskset.transform_sandbox import (
    TransformSandboxConfig,
    TransformSandboxError,
    run_transform,
)
from loom.taskset.upstream_rows import UpstreamFetchError, iter_upstream_rows

_NOOP_VERIFIER = b"#!/bin/sh\nexit 0\n"
_UNSAFE_TASK_ID = re.compile(r"[^a-zA-Z0-9._-]+")


@dataclass(frozen=True)
class TaskRowDraft:
    id: str
    checksum: str
    config: dict[str, Any]
    source: str


@dataclass
class MaterializeOutput:
    task_rows: list[TaskRowDraft] = field(default_factory=list)
    task_count: int = 0
    status: str = "failed"
    status_reason: str | None = None
    evaluation_ready: bool = False
    error_summary: list[dict[str, str]] = field(default_factory=list)
    job_failure_reason: str | None = None
    job_failure_message: str | None = None
    retry_source: bool = False


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3 uri, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri!r}")
    return bucket, key


def _storage_prefix(*, team_id: str, slug: str) -> str:
    return f"tasksets/user/{team_id}/{slug}"


def _safe_task_segment(task_id: str) -> str:
    cleaned = _UNSAFE_TASK_ID.sub("_", task_id.strip())
    if not cleaned:
        raise ValueError("task id rendered empty after sanitization")
    return cleaned


def _db_task_id(*, task_set_id: str, rendered_task_id: str) -> str:
    return f"{task_set_id}/tasks/{_safe_task_segment(rendered_task_id)}"


def _apply_verifier_defaults(
    config: dict[str, Any],
    *,
    manifest_verifier: TaskSetVerifier | None,
    has_evaluation_intent: bool,
) -> dict[str, Any]:
    if "verifier" in config:
        return config
    if manifest_verifier is not None and has_evaluation_intent:
        verifier_block: dict[str, Any] = {"name": manifest_verifier.type}
        if manifest_verifier.type == "script":
            verifier_block["args"] = {
                "script_path": manifest_verifier.file.replace("\\", "/"),
            }
        return {**config, "verifier": verifier_block}
    return {
        **config,
        "verifier": {
            "name": "script",
            "args": {"script_path": "verifier/noop.sh"},
        },
    }


def _put_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def _fetch_blob_bytes(client: Any, blob_uri: str | None) -> bytes | None:
    if blob_uri is None:
        return None
    bucket, key = _parse_s3_uri(blob_uri)
    resp = client.get_object(Bucket=bucket, Key=key)
    body: bytes = resp["Body"].read()
    return body


def _fetch_verifier_bytes(client: Any, verifier_blob_uri: str | None) -> bytes | None:
    return _fetch_blob_bytes(client, verifier_blob_uri)


def _write_local_bundle(
    bundle_dir: Path,
    *,
    task_config: TaskConfig,
    instruction: str | None,
    verifier_bytes: bytes | None,
    manifest_verifier: TaskSetVerifier | None,
    has_evaluation_intent: bool,
) -> None:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "task.toml").write_bytes(
        tomli_w.dumps(task_config.model_dump(mode="json", exclude_none=True)).encode(),
    )
    if instruction is not None:
        (bundle_dir / "instruction.md").write_text(instruction, encoding="utf-8")
    if manifest_verifier is not None and has_evaluation_intent and verifier_bytes is not None:
        rel = manifest_verifier.file.replace("\\", "/").lstrip("/")
        target = bundle_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(verifier_bytes)
        if manifest_verifier.type == "pytest" and rel.endswith(".py"):
            tests_target = bundle_dir / "tests" / PurePosixPath(rel).name
            tests_target.parent.mkdir(parents=True, exist_ok=True)
            tests_target.write_bytes(verifier_bytes)
    else:
        noop_path = bundle_dir / "verifier" / "noop.sh"
        noop_path.parent.mkdir(parents=True, exist_ok=True)
        noop_path.write_bytes(_NOOP_VERIFIER)


def _upload_bundle_dir(
    client: Any,
    *,
    bucket: str,
    bundle_prefix: str,
    bundle_dir: Path,
) -> None:
    for path in bundle_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(bundle_dir).as_posix()
        _put_object(
            client,
            bucket=bucket,
            key=f"{bundle_prefix}/{rel}",
            body=path.read_bytes(),
            content_type="application/octet-stream",
        )


def _compatibility_issue_error(
    *,
    row_index: int,
    issue: TaskBundleCompatibilityIssue,
) -> dict[str, str]:
    return {
        "row": str(row_index),
        "code": issue.code,
        "severity": issue.severity.value,
        "path": issue.path,
        "line": str(issue.line),
        "phase": issue.phase,
        "message": issue.message,
        "hint": issue.hint,
        "evidence": json.dumps(issue.evidence, sort_keys=True, separators=(",", ":")),
    }


def materialize_task_set(
    *,
    manifest: UserTaskSetManifest,
    task_set_id: str,
    owning_team_id: str,
    intents: list[str],
    verifier_blob_uri: str | None,
    transform_blob_uri: str | None = None,
    transform_config: TransformSandboxConfig | None = None,
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
) -> MaterializeOutput:
    """Materialize one TaskSet synchronously. Caller owns DB writes."""
    slug = manifest.slug
    prefix = _storage_prefix(team_id=owning_team_id, slug=slug)
    has_evaluation = "evaluation" in intents
    max_instances = (manifest.limits.max_instances if manifest.limits else 500)
    manifest_timeout_s = manifest.limits.timeout_per_task_s if manifest.limits else None

    transform_bytes: bytes | None = None
    if manifest.transform is not None:
        if transform_config is None or not transform_config.enabled or not transform_config.network_isolated:
            return MaterializeOutput(
                status="failed",
                status_reason="transform_unsupported_on_host",
                job_failure_reason="transform_unsupported_on_host",
                job_failure_message=(
                    "transform manifests require taskset_materializer_transforms_enabled "
                    "and taskset_materializer_transform_network_isolated"
                ),
            )
        try:
            transform_bytes = _fetch_blob_bytes(minio_client, transform_blob_uri)
        except Exception as exc:
            return MaterializeOutput(
                status="failed",
                status_reason="transform_blob_missing",
                job_failure_reason="transform_blob_missing",
                job_failure_message=str(exc),
            )
        if not transform_bytes:
            return MaterializeOutput(
                status="failed",
                status_reason="transform_blob_missing",
                job_failure_reason="transform_blob_missing",
                job_failure_message="transform blob uri missing or empty",
            )

    verifier_bytes = _fetch_verifier_bytes(minio_client, verifier_blob_uri)

    try:
        row_iter = iter_upstream_rows(
            manifest.source,
            cache_root=upstream_cache_root,
        )
    except UpstreamFetchError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="source_unreachable",
            job_failure_reason="source_unreachable",
            job_failure_message=str(exc),
            retry_source=True,
        )

    errors: list[dict[str, str]] = []
    drafts: list[TaskRowDraft] = []
    skipped = 0
    attempted = 0

    try:
        for row in row_iter:
            if attempted >= max_instances:
                break
            attempted += 1
            row_index = attempted
            try:
                working_row = row
                if manifest.transform is not None and transform_bytes is not None:
                    assert transform_config is not None
                    working_row = run_transform(
                        transform_script=transform_bytes,
                        row=row,
                        config=transform_config,
                        manifest_timeout_s=manifest_timeout_s,
                    )
                instance = resolve_mapping(working_row, manifest.instance_mapping)
                rendered = render_task_template(
                    manifest.task_template,
                    instance=instance,
                    metadata_name=slug,
                    metadata_display_name=manifest.metadata.display_name,
                )
                if "schema_version" not in rendered:
                    rendered = {"schema_version": "1", **rendered}
                rendered = _apply_verifier_defaults(
                    rendered,
                    manifest_verifier=manifest.verifier,
                    has_evaluation_intent=has_evaluation,
                )
                task_config = normalize_steps(TaskConfig.model_validate(rendered))
                rendered_task_id = task_config.task.id
                short_id = _safe_task_segment(rendered_task_id)
                db_id = _db_task_id(
                    task_set_id=task_set_id,
                    rendered_task_id=rendered_task_id,
                )
                prompt = instance.get("prompt")
                instruction = None
                if prompt is not None:
                    instruction = (
                        f"{prompt}\n" if not str(prompt).endswith("\n") else str(prompt)
                    )

                with tempfile.TemporaryDirectory() as tmp:
                    bundle_dir = Path(tmp)
                    _write_local_bundle(
                        bundle_dir,
                        task_config=task_config,
                        instruction=instruction,
                        verifier_bytes=verifier_bytes,
                        manifest_verifier=manifest.verifier,
                        has_evaluation_intent=has_evaluation,
                    )
                    compatibility_issues = [
                        issue
                        for issue in collect_task_dir_compatibility_issues(bundle_dir)
                        if issue.severity == CompatibilitySeverity.ERROR
                    ]
                    if compatibility_issues:
                        skipped += 1
                        errors.extend(
                            _compatibility_issue_error(
                                row_index=row_index,
                                issue=issue,
                            )
                            for issue in compatibility_issues
                        )
                        continue
                    checksum = task_checksum(bundle_dir)
                    bundle_prefix = f"{prefix}/tasks/{short_id}"
                    _upload_bundle_dir(
                        minio_client,
                        bucket=artifacts_bucket,
                        bundle_prefix=bundle_prefix,
                        bundle_dir=bundle_dir,
                    )
                source = f"s3://{artifacts_bucket}/{bundle_prefix}/"
                drafts.append(
                    TaskRowDraft(
                        id=db_id,
                        checksum=checksum,
                        config=task_config.model_dump(mode="json"),
                        source=source,
                    ),
                )
            except MappingError as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": "mapping_error",
                    "message": str(exc),
                })
            except TransformSandboxError as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": exc.code,
                    "message": exc.message,
                })
            except ValidationError as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": "task_config_invalid",
                    "message": str(exc.errors()),
                })
            except Exception as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": "materialize_error",
                    "message": str(exc),
                })
    except UpstreamFetchError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="source_unreachable",
            job_failure_reason="source_unreachable",
            job_failure_message=str(exc),
            retry_source=True,
        )

    materialized = len(drafts)
    status, status_reason = compute_task_set_status(
        materialized=materialized,
        skipped=skipped,
    )
    if materialized == 0 and any(
        error.get("code", "").startswith("TASK_COMPAT_")
        for error in errors
    ):
        status_reason = "bundle_compatibility_error"
    evaluation_ready = (
        has_evaluation
        and manifest.verifier is not None
        and verifier_bytes is not None
        and materialized > 0
        and status in {"ready", "partial"}
    )
    return MaterializeOutput(
        task_rows=drafts,
        task_count=materialized,
        status=status,
        status_reason=status_reason,
        evaluation_ready=evaluation_ready,
        error_summary=cap_error_summary(errors),
        job_failure_reason=(
            status_reason if status == "failed" and status_reason else None
        ),
    )
