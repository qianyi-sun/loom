"""MinIO storage preflight evidence for large staging runs."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

StorageOutcome = Literal["pass", "warn", "stop", "unknown"]

DEFAULT_BUCKETS: tuple[str, ...] = (
    "artifacts",
    "trajectories",
    "loom-benchmarks",
    "loom-tasks",
)

DEFAULT_WARN_FREE_PERCENT = 25.0
DEFAULT_STOP_FREE_PERCENT = 15.0
DEFAULT_WARN_GROWTH_BYTES_PER_HOUR = 100 * 1024**3


@dataclass(frozen=True)
class MinioStorageThresholds:
    warn_free_percent: float = DEFAULT_WARN_FREE_PERCENT
    stop_free_percent: float = DEFAULT_STOP_FREE_PERCENT
    warn_growth_bytes_per_hour: int = DEFAULT_WARN_GROWTH_BYTES_PER_HOUR


@dataclass(frozen=True)
class StoragePreflightValidation:
    ok: bool
    outcome: str
    message: str
    artifact: dict[str, Any] | None = None


RunCommand = Callable[[list[str]], str]


def _run_text(argv: list[str]) -> str:
    completed = subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(detail or f"{argv[0]} exited {completed.returncode}")
    return completed.stdout


def _parse_used_percent(value: str) -> float:
    return float(value.strip().removesuffix("%"))


def _parse_df_pk(output: str, *, data_path: str) -> dict[str, Any]:
    lines = [line for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        raise ValueError("df output did not include a data row")
    fields = lines[-1].split()
    if len(fields) < 6:
        raise ValueError(f"df output has unexpected shape: {lines[-1]!r}")
    size_bytes = int(fields[1]) * 1024
    used_bytes = int(fields[2]) * 1024
    free_bytes = int(fields[3]) * 1024
    used_percent = _parse_used_percent(fields[4])
    free_percent = max(0.0, 100.0 - used_percent)
    return {
        "path": data_path,
        "filesystem": fields[0],
        "mount": fields[5],
        "size_bytes": size_bytes,
        "used_bytes": used_bytes,
        "free_bytes": free_bytes,
        "used_percent": used_percent,
        "free_percent": free_percent,
    }


def _parse_du_sk(
    output: str, *, bucket_names: Sequence[str], data_path: str
) -> list[dict[str, Any]]:
    usage_by_name = {name: 0 for name in bucket_names}
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        if fields[0].startswith(data_path):
            path = fields[0]
            size_kib = int(fields[1])
        else:
            size_kib = int(fields[0])
            path = fields[1]
        name = Path(path).name
        if name in usage_by_name:
            usage_by_name[name] = size_kib * 1024
    buckets: list[dict[str, Any]] = []
    for name in bucket_names:
        if name in {"artifacts", "trajectories"}:
            category = name
        else:
            category = "benchmark-task-data"
        buckets.append(
            {
                "name": name,
                "path": f"{data_path.rstrip('/')}/{name}",
                "category": category,
                "usage_bytes": usage_by_name[name],
            }
        )
    return buckets


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_generated_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _bucket_usage_map(report: Mapping[str, Any]) -> dict[str, int]:
    buckets = report.get("buckets")
    if not isinstance(buckets, list):
        return {}
    result: dict[str, int] = {}
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        name = bucket.get("name")
        usage = bucket.get("usage_bytes")
        if isinstance(name, str) and isinstance(usage, int):
            result[name] = usage
    return result


def _growth_check(
    *,
    current: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
    thresholds: MinioStorageThresholds,
) -> dict[str, Any]:
    if previous is None:
        return {
            "name": "minio-artifact-trajectory-growth",
            "outcome": "unknown",
            "detail": "no previous storage preflight artifact supplied; growth rate unavailable",
        }
    current_at = _parse_generated_at(current.get("generated_at"))
    previous_at = _parse_generated_at(previous.get("generated_at"))
    if current_at is None or previous_at is None or current_at <= previous_at:
        return {
            "name": "minio-artifact-trajectory-growth",
            "outcome": "unknown",
            "detail": "storage preflight timestamps do not allow growth-rate calculation",
        }
    hours = (current_at - previous_at).total_seconds() / 3600
    current_buckets = _bucket_usage_map(current)
    previous_buckets = _bucket_usage_map(previous)
    growth_bytes = 0
    for name in ("artifacts", "trajectories"):
        growth_bytes += max(0, current_buckets.get(name, 0) - previous_buckets.get(name, 0))
    growth_per_hour = int(growth_bytes / hours) if hours > 0 else 0
    outcome: StorageOutcome = (
        "warn" if growth_per_hour >= thresholds.warn_growth_bytes_per_hour else "pass"
    )
    return {
        "name": "minio-artifact-trajectory-growth",
        "outcome": outcome,
        "detail": (
            f"artifacts+trajectories growth is {growth_per_hour} bytes/hour over {hours:.2f} hours"
        ),
        "growth_bytes": growth_bytes,
        "growth_bytes_per_hour": growth_per_hour,
        "warn_growth_bytes_per_hour": thresholds.warn_growth_bytes_per_hour,
    }


def _free_space_check(
    *,
    filesystem: Mapping[str, Any],
    thresholds: MinioStorageThresholds,
    estimated_batch_bytes: int | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    free_bytes = int(filesystem["free_bytes"])
    size_bytes = int(filesystem["size_bytes"])
    free_percent = float(filesystem["free_percent"])
    free_after_bytes = (
        free_bytes - estimated_batch_bytes if estimated_batch_bytes is not None else free_bytes
    )
    free_after_percent = (free_after_bytes / size_bytes) * 100.0 if size_bytes > 0 else 0.0
    threshold_percent = free_after_percent if estimated_batch_bytes is not None else free_percent
    threshold_bytes = free_after_bytes if estimated_batch_bytes is not None else free_bytes
    if threshold_percent < thresholds.stop_free_percent or threshold_bytes < 0:
        outcome: StorageOutcome = "stop"
        detail = (
            f"free space {threshold_percent:.1f}% is below stop threshold "
            f"{thresholds.stop_free_percent:.1f}%"
        )
    elif threshold_percent < thresholds.warn_free_percent:
        outcome = "warn"
        detail = (
            f"free space {threshold_percent:.1f}% is below warning threshold "
            f"{thresholds.warn_free_percent:.1f}%"
        )
    else:
        outcome = "pass"
        detail = f"free space {threshold_percent:.1f}% is above configured thresholds"
    check = {
        "name": "minio-data-free-space",
        "outcome": outcome,
        "detail": detail,
        "free_bytes": free_bytes,
        "free_percent": free_percent,
        "free_after_estimated_batch_bytes": free_after_bytes,
        "free_after_estimated_batch_percent": free_after_percent,
        "remediation": (
            "Reclaim MinIO space or provision storage before submitting a large batch."
            if outcome == "stop"
            else None
        ),
    }
    headroom = {
        "estimated_batch_bytes": estimated_batch_bytes,
        "free_after_estimated_batch_bytes": free_after_bytes,
        "free_after_estimated_batch_percent": free_after_percent,
    }
    return check, headroom


def _overall_outcome(checks: Sequence[Mapping[str, Any]]) -> StorageOutcome:
    outcomes = {str(check.get("outcome")) for check in checks}
    if "stop" in outcomes:
        return "stop"
    if "warn" in outcomes:
        return "warn"
    return "pass"


def build_minio_storage_preflight(
    *,
    namespace: str,
    pod: str = "loom-minio-0",
    data_path: str = "/data",
    bucket_names: Sequence[str] = DEFAULT_BUCKETS,
    thresholds: MinioStorageThresholds | None = None,
    estimated_batch_bytes: int | None = None,
    previous_report: Mapping[str, Any] | None = None,
    run_command: RunCommand = _run_text,
    generated_at: str | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or MinioStorageThresholds()
    bucket_list = list(bucket_names)
    df_cmd = [
        "kubectl",
        "-n",
        namespace,
        "exec",
        f"pod/{pod}",
        "--",
        "sh",
        "-c",
        f"df -Pk {data_path}",
    ]
    du_paths = " ".join(f"{data_path.rstrip('/')}/{bucket}" for bucket in bucket_list)
    du_cmd = [
        "kubectl",
        "-n",
        namespace,
        "exec",
        f"pod/{pod}",
        "--",
        "sh",
        "-c",
        f"du -sk {du_paths} 2>/dev/null || true",
    ]
    filesystem = _parse_df_pk(run_command(df_cmd), data_path=data_path)
    buckets = _parse_du_sk(
        run_command(du_cmd),
        bucket_names=bucket_list,
        data_path=data_path,
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": generated_at or _utc_now_iso(),
        "namespace": namespace,
        "pod": pod,
        "data_path": data_path,
        "storage_contract": {
            "mode": "hostpath-local-pv",
            "description": (
                "Staging MinIO capacity is governed by the /data backing "
                "filesystem. PVC/PV capacity is allocation metadata, not the "
                "effective stop threshold."
            ),
        },
        "thresholds": {
            "warn_free_percent": thresholds.warn_free_percent,
            "stop_free_percent": thresholds.stop_free_percent,
            "warn_growth_bytes_per_hour": thresholds.warn_growth_bytes_per_hour,
        },
        "filesystem": filesystem,
        "buckets": buckets,
    }
    free_check, headroom = _free_space_check(
        filesystem=filesystem,
        thresholds=thresholds,
        estimated_batch_bytes=estimated_batch_bytes,
    )
    report["headroom"] = headroom
    checks = [
        free_check,
        _growth_check(
            current=report,
            previous=previous_report,
            thresholds=thresholds,
        ),
    ]
    report["checks"] = checks
    report["outcome"] = _overall_outcome(checks)
    return report


def render_minio_storage_preflight_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def render_minio_storage_preflight_table(report: Mapping[str, Any]) -> str:
    filesystem = report.get("filesystem")
    fs = filesystem if isinstance(filesystem, dict) else {}
    lines = [
        f"namespace: {report.get('namespace', '')}",
        f"pod:       {report.get('pod', '')}",
        f"outcome:   {report.get('outcome', 'unknown')}",
        "",
        "FILESYSTEM                  SIZE_BYTES      USED_BYTES      FREE_BYTES  USED%  FREE%",
        (
            f"{fs.get('filesystem', '')!s:<24} "
            f"{int(fs.get('size_bytes', 0)):>14} "
            f"{int(fs.get('used_bytes', 0)):>14} "
            f"{int(fs.get('free_bytes', 0)):>14} "
            f"{float(fs.get('used_percent', 0.0)):>5.1f} "
            f"{float(fs.get('free_percent', 0.0)):>5.1f}"
        ),
        "",
        "BUCKET                     CATEGORY                 USAGE_BYTES",
    ]
    buckets = report.get("buckets")
    if isinstance(buckets, list):
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            lines.append(
                f"{bucket.get('name', '')!s:<26} "
                f"{bucket.get('category', '')!s:<24} "
                f"{int(bucket.get('usage_bytes', 0)):>12}"
            )
    lines.extend(["", "CHECK                         OUTCOME  DETAIL"])
    checks = report.get("checks")
    if isinstance(checks, list):
        for check in checks:
            if not isinstance(check, dict):
                continue
            lines.append(
                f"{check.get('name', '')!s:<30} "
                f"{check.get('outcome', '')!s:<8} "
                f"{check.get('detail', '')!s}"
            )
    return "\n".join(lines) + "\n"


def validate_minio_storage_preflight_artifact(
    path: Path,
    *,
    allow_stop_override: bool,
) -> StoragePreflightValidation:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("storage preflight JSON root must be an object")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return StoragePreflightValidation(
            ok=False,
            outcome="invalid",
            message=f"MinIO storage preflight artifact is invalid: {exc}",
        )
    outcome = str(raw.get("outcome") or "unknown")
    if outcome == "stop" and not allow_stop_override:
        return StoragePreflightValidation(
            ok=False,
            outcome=outcome,
            message=(
                "MinIO storage preflight is at stop threshold; pass an explicit "
                "storage override only after reclaiming/provisioning space or "
                "accepting the operator risk."
            ),
            artifact=raw,
        )
    return StoragePreflightValidation(
        ok=True,
        outcome=outcome,
        message=f"MinIO storage preflight outcome: {outcome}",
        artifact=raw,
    )
