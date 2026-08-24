"""Generate secret-safe HF mirror/token-boundary release evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from loom_benchmark_tool.db_url import normalize_db_url
from loom_cli.benchmark_readiness import (
    BUNDLE_VERIFICATION_KIND,
    BUNDLE_VERIFICATION_SCHEMA_VERSION,
)
from loom_cli.canary_task_filter import task_filter_targets_only_benchmark

_RAW_SECRET_RE = re.compile(r"(?:hf_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16})")
_BUNDLE_VERIFICATION_KEYS = frozenset(
    {
        "schema_version",
        "verification_kind",
        "s3_tasks",
        "verified",
        "failed",
        "checksum_mismatches",
        "verification_errors",
        "failures",
        "missing",
        "missing_sources",
    }
)

_REMOTE_WORKER_ENV_SCRIPT = r"""
import json
import pathlib
import re
import subprocess
import sys

repo = pathlib.Path(sys.argv[1])
env_file = pathlib.Path(sys.argv[2])
env_keys = []
if env_file.exists():
    for raw in env_file.read_text(errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            env_keys.append(key)
ps = subprocess.run(
    ["docker", "ps", "--filter", "name=worker", "--format", "{{.Names}}"],
    capture_output=True,
    text=True,
)
containers = [line.strip() for line in ps.stdout.splitlines() if line.strip()]
checks = []
for name in containers:
    inspect = subprocess.run(
        ["docker", "inspect", name, "--format", "{{json .Config.Env}}"],
        capture_output=True,
        text=True,
    )
    if inspect.returncode != 0:
        checks.append({"container": name, "inspect_ok": False, "hf_token_present": None})
        continue
    env = json.loads(inspect.stdout or "[]")
    present = any(str(item).split("=", 1)[0] == "HF_TOKEN" for item in env)
    checks.append({"container": name, "inspect_ok": True, "hf_token_present": present})
print(json.dumps({
  "env_file_exists": env_file.exists(),
  "env_file_hf_token_present": "HF_TOKEN" in env_keys,
  "env_file_key_count": len(env_keys),
  "docker_ps_ok": ps.returncode == 0,
  "containers": checks,
}, sort_keys=True))
"""


class HfBoundaryEvidenceError(RuntimeError):
    """Raised when boundary evidence cannot be collected or written safely."""


@dataclass(frozen=True, slots=True)
class KubernetesServiceTarget:
    namespace: str
    deployment: str = "loom-service"
    container: str = "loom-service"


def _required_counter(values: Mapping[str, Any], key: str, *, label: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise HfBoundaryEvidenceError(f"{label} counter {key!r} must be a nonnegative integer")
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _candidate_binding_from_gb10_status(
    status: dict[str, Any],
    *,
    environment: str,
) -> dict[str, Any]:
    desired_states = status.get("desired_states")
    if not isinstance(desired_states, list) or len(desired_states) != 1:
        raise HfBoundaryEvidenceError(
            "--gb10-workers-status must contain exactly one desired state"
        )
    desired = desired_states[0]
    if not isinstance(desired, dict):
        raise HfBoundaryEvidenceError("GB10 desired state must be an object")
    if desired.get("environment") != environment:
        raise HfBoundaryEvidenceError(
            "GB10 desired-state environment does not match HF evidence environment"
        )
    image_tag = desired.get("image_tag")
    source_git_commit = desired.get("source_git_commit")
    if not isinstance(image_tag, str) or not image_tag:
        raise HfBoundaryEvidenceError("GB10 desired state image_tag is required")
    if not isinstance(source_git_commit, str) or not source_git_commit:
        raise HfBoundaryEvidenceError("GB10 desired state source_git_commit is required")
    return {
        "environment": environment,
        "release_image_tag": image_tag,
        "release_git_sha": source_git_commit,
        "gb10_workers_status_sha256": _canonical_json_sha256(status),
    }


def _secret_leak_paths(value: Any, *, path: str = "") -> list[str]:
    if isinstance(value, str):
        return [path or "$"] if _RAW_SECRET_RE.search(value) else []
    if isinstance(value, Mapping):
        paths: list[str] = []
        for key, child in value.items():
            child_path = str(key) if not path else f"{path}.{key}"
            paths.extend(_secret_leak_paths(child, path=child_path))
        return paths
    if isinstance(value, list):
        paths = []
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            paths.extend(_secret_leak_paths(child, path=child_path))
        return paths
    return []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HfBoundaryEvidenceError(f"failed to read JSON {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise HfBoundaryEvidenceError(f"JSON root must be an object: {path}")
    return data


def _audit_item(audit_report: Mapping[str, Any], benchmark_id: str) -> dict[str, Any]:
    items = audit_report.get("items")
    if not isinstance(items, list):
        raise HfBoundaryEvidenceError("audit report is missing items[]")
    for item in items:
        if isinstance(item, Mapping) and item.get("id") == benchmark_id:
            return dict(item)
    raise HfBoundaryEvidenceError(f"audit report does not include {benchmark_id!r}")


def _non_internal_sources(source_summary: Mapping[str, Any]) -> list[str]:
    rows = source_summary.get("non_internal_sources")
    if not isinstance(rows, list):
        return []
    out: list[str] = []
    for row in rows:
        source = row.get("source") if isinstance(row, Mapping) else row
        if source is None:
            out.append("")
        elif isinstance(source, str):
            out.append(source)
    return out


def compose_boundary_evidence(
    *,
    benchmark_id: str,
    environment: str,
    audit_report: Mapping[str, Any],
    source_summary: Mapping[str, Any],
    canary_summary: Mapping[str, Any],
    worker_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    """Compose the release-gate artifact from separately collected facts.

    ``benchmarks.upstream_*`` is adapter/source provenance. The runtime HF
    mirror provenance used by staging/prod is task-level metadata written when
    the HF publication was mirrored to internal object storage.
    """

    audit_item = _audit_item(audit_report, benchmark_id)
    bundle_verification = _mapping(audit_report.get("bundle_presence"))
    if set(bundle_verification) != _BUNDLE_VERIFICATION_KEYS:
        raise HfBoundaryEvidenceError(
            "audit does not use the required full-bundle verification contract",
        )
    bundle_tasks = bundle_verification.get("s3_tasks")
    bundle_verified = bundle_verification.get("verified")
    bundle_failed = bundle_verification.get("failed")
    bundle_checksum_mismatches = bundle_verification.get("checksum_mismatches")
    bundle_verification_errors = bundle_verification.get("verification_errors")
    bundle_missing = bundle_verification.get("missing")
    if (
        bundle_verification.get("schema_version") != BUNDLE_VERIFICATION_SCHEMA_VERSION
        or bundle_verification.get("verification_kind") != BUNDLE_VERIFICATION_KIND
    ):
        raise HfBoundaryEvidenceError(
            "audit does not use the required full-bundle verification contract",
        )
    if (
        isinstance(bundle_tasks, bool)
        or not isinstance(bundle_tasks, int)
        or isinstance(bundle_verified, bool)
        or not isinstance(bundle_verified, int)
        or isinstance(bundle_failed, bool)
        or not isinstance(bundle_failed, int)
        or isinstance(bundle_checksum_mismatches, bool)
        or not isinstance(bundle_checksum_mismatches, int)
        or isinstance(bundle_verification_errors, bool)
        or not isinstance(bundle_verification_errors, int)
        or isinstance(bundle_missing, bool)
        or not isinstance(bundle_missing, int)
        or bundle_tasks <= 0
        or bundle_verified != bundle_tasks
        or bundle_failed != 0
        or bundle_checksum_mismatches != 0
        or bundle_verification_errors != 0
        or bundle_verification.get("failures") != []
        or bundle_missing != 0
        or bundle_verification.get("missing_sources") != []
    ):
        raise HfBoundaryEvidenceError(
            "audit bundle verification is incomplete or contains failures",
        )
    source_counts = _mapping(source_summary.get("source_counts"))
    sample_task = _mapping(source_summary.get("sample_task"))
    tags = _mapping(sample_task.get("tags"))
    config = _mapping(sample_task.get("config"))
    task_environment = _mapping(config.get("environment"))
    worker_summary = _mapping(worker_boundary.get("summary"))
    raw_tasks = _required_counter(audit_item, "raw_task_count", label="audit")
    valid_tasks = _required_counter(audit_item, "valid_task_config_count", label="audit")
    total_sources = _required_counter(
        source_counts,
        "total_task_sources",
        label="source summary",
    )
    internal_sources = _required_counter(
        source_counts,
        "internal_s3_sources",
        label="source summary",
    )
    non_internal_count = _required_counter(
        source_counts,
        "non_internal_sources",
        label="source summary",
    )
    artifact_contract_classified_tasks = _required_counter(
        source_counts,
        "artifact_contract_classified_tasks",
        label="source summary",
    )
    apd5_required_artifact_contract_tasks = _required_counter(
        source_counts,
        "apd5_required_artifact_contract_tasks",
        label="source summary",
    )
    non_internal_rows = source_summary.get("non_internal_sources")
    if not isinstance(non_internal_rows, list):
        raise HfBoundaryEvidenceError("source summary non_internal_sources must be a list")
    non_internal = _non_internal_sources(source_summary)
    if (
        raw_tasks <= 0
        or not (
            raw_tasks
            == valid_tasks
            == total_sources
            == internal_sources
            == bundle_tasks
            == bundle_verified
        )
        or non_internal_count != 0
        or non_internal
    ):
        raise HfBoundaryEvidenceError(
            "full-bundle count binding is incomplete or inconsistent",
        )
    worker_hf_token_present = bool(
        worker_summary.get("env_file_hf_token_present_hosts")
        or worker_summary.get("hosts_with_container_hf_token_present")
    )
    upstream_locator = str(tags.get("hf_repo_id") or "").strip()
    upstream_revision = str(tags.get("hf_revision") or "").strip()
    if not upstream_locator or not upstream_revision:
        raise HfBoundaryEvidenceError(
            "sample task tags must include hf_repo_id and hf_revision",
        )
    return {
        "schema_version": 1,
        "environment": environment,
        "benchmark_id": benchmark_id,
        "bundle_verification": {
            "schema_version": BUNDLE_VERIFICATION_SCHEMA_VERSION,
            "verification_kind": BUNDLE_VERIFICATION_KIND,
            "s3_tasks": bundle_tasks,
            "verified": bundle_verified,
            "failed": bundle_failed,
        },
        "catalog": {
            "runnable_tasks": valid_tasks,
            "artifact_contract_classified_tasks": artifact_contract_classified_tasks,
            "apd5_required_artifact_contract_tasks": (
                apd5_required_artifact_contract_tasks
            ),
            "requires_caps": {
                "cpu_arch": str(task_environment.get("cpu_arch") or ""),
            },
        },
        "runtime_sources": {
            "total_task_sources": total_sources,
            "internal_s3_sources": internal_sources,
            "non_internal_sources": non_internal,
            "sample_s3_source": source_counts.get("sample_s3_source"),
            "sample_task_id": sample_task.get("id"),
        },
        "hf_provenance": {
            "upstream_kind": "huggingface",
            "upstream_locator": upstream_locator,
            "upstream_revision": upstream_revision,
            "sample_hf_path": tags.get("hf_path"),
            "sample_hf_checksum": tags.get("hf_checksum"),
            "source": "task.tags",
            "benchmark_adapter_origin": source_summary.get("benchmark"),
        },
        "worker_boundary": {
            "canary_started": canary_summary.get("canary_started") is True,
            "terminal_state": canary_summary.get("terminal_state"),
            "hf_token_present": worker_hf_token_present,
            "hf_token_isolated": not worker_hf_token_present,
            "direct_hf_egress_required": False,
            "materialized_from_internal_source": (
                total_sources > 0 and internal_sources == total_sources and not non_internal
            ),
            "canary_batch_id": canary_summary.get("batch_id"),
            "canary_task_filter": canary_summary.get("task_filter"),
            "canary_worker_pools": canary_summary.get("worker_pools"),
            "expected_trial_count": canary_summary.get("expected_trial_count"),
            "succeeded_trials": canary_summary.get("succeeded_trials"),
            "canary_task_provenance": canary_summary.get("task_provenance"),
            "gb10_hf_token_check_summary": worker_summary,
        },
        "secret_scan": {"raw_secret_values_present": False},
    }


def write_secret_safe_json(evidence: Mapping[str, Any], path: Path) -> None:
    raw = json.dumps(dict(evidence), indent=2, sort_keys=True) + "\n"
    leaks = _secret_leak_paths(json.loads(raw))
    if leaks:
        joined = ", ".join(leaks[:5])
        raise HfBoundaryEvidenceError(
            f"refusing to write evidence with secret-looking value(s) at {joined}",
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(raw, encoding="utf-8")


async def collect_source_summary_from_db(
    *,
    db_url: str,
    benchmark_id: str,
) -> dict[str, Any]:
    engine = create_async_engine(normalize_db_url(db_url))
    try:
        async with engine.connect() as conn:
            bench = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT id, display_name, upstream_kind,
                               upstream_locator, upstream_revision
                        FROM benchmarks
                        WHERE id = :benchmark_id
                        """
                        ),
                        {"benchmark_id": benchmark_id},
                    )
                )
                .mappings()
                .first()
            )
            counts = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT
                          COUNT(*) AS total_task_sources,
                          COUNT(*) FILTER (WHERE source LIKE 's3://%')
                            AS internal_s3_sources,
                          COUNT(*) FILTER (
                            WHERE source IS NULL OR source NOT LIKE 's3://%'
                          ) AS non_internal_sources,
                          COUNT(*) FILTER (
                            WHERE tags->>'required_artifacts_contract'
                              IN ('declared', 'none')
                          ) AS artifact_contract_classified_tasks,
                          COUNT(*) FILTER (
                            WHERE id = 'skilllearnbench/anthropic-poster-design/anthropic-poster-design-5'
                              AND tags->>'required_artifacts_contract' = 'declared'
                              AND jsonb_path_exists(
                                config,
                                '$.steps[*].required_artifacts[*]'
                              )
                          ) AS apd5_required_artifact_contract_tasks,
                          MIN(source) FILTER (WHERE source LIKE 's3://%')
                            AS sample_s3_source
                        FROM tasks
                        WHERE benchmark_id = :benchmark_id
                        """
                        ),
                        {"benchmark_id": benchmark_id},
                    )
                )
                .mappings()
                .first()
            )
            non_internal = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT id, source
                        FROM tasks
                        WHERE benchmark_id = :benchmark_id
                          AND (source IS NULL OR source NOT LIKE 's3://%')
                        ORDER BY id
                        LIMIT 20
                        """
                        ),
                        {"benchmark_id": benchmark_id},
                    )
                )
                .mappings()
                .all()
            )
            sample_task = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT id, source, config, tags
                        FROM tasks
                        WHERE benchmark_id = :benchmark_id
                          AND source LIKE 's3://%'
                        ORDER BY id
                        LIMIT 1
                        """
                        ),
                        {"benchmark_id": benchmark_id},
                    )
                )
                .mappings()
                .first()
            )
    finally:
        await engine.dispose()
    return {
        "benchmark": dict(bench) if bench else None,
        "source_counts": dict(counts) if counts else None,
        "non_internal_sources": [dict(row) for row in non_internal],
        "sample_task": dict(sample_task) if sample_task else None,
    }


def _is_successful_canary_row(
    row: Mapping[str, Any],
    *,
    benchmark_id: str,
    worker_pool: str,
) -> bool:
    task_filter = _mapping(row.get("task_filter"))
    required_pools = row.get("required_worker_pools")
    return (
        row.get("state") == "finished"
        and row.get("result_status") == "succeeded"
        and task_filter_targets_only_benchmark(task_filter, benchmark_id)
        and isinstance(required_pools, list)
        and worker_pool in required_pools
    )


def _summarize_canary_trials(
    trial_rows: Sequence[Mapping[str, Any]],
    *,
    benchmark_id: str,
) -> dict[str, Any]:
    terminal: dict[str, int] = {}
    active: dict[str, int] = {}
    succeeded_trials = 0
    target_benchmark_trials = 0
    non_target_trials = 0
    task_set_trials = 0
    task_benchmark_ids: set[str] = set()
    worker_ids: list[str | None] = []
    for row in trial_rows:
        state = str(row.get("state") or "")
        pool = str(row.get("pool_name") or "unknown")
        task_benchmark_id = row.get("task_benchmark_id")
        task_set_id = row.get("task_set_id")
        worker_id = row.get("worker_id")
        worker_ids.append(
            worker_id if isinstance(worker_id, str) and worker_id else None,
        )
        if state == "succeeded":
            succeeded_trials += 1
        if task_benchmark_id == benchmark_id:
            target_benchmark_trials += 1
        else:
            non_target_trials += 1
        if isinstance(task_benchmark_id, str) and task_benchmark_id:
            task_benchmark_ids.add(task_benchmark_id)
        if task_set_id is not None:
            task_set_trials += 1
        bucket = terminal if state in {"succeeded", "failed", "cancelled"} else active
        bucket[pool] = bucket.get(pool, 0) + 1
    return {
        "worker_pools": {"active": active, "terminal": terminal},
        "succeeded_trials": succeeded_trials,
        "task_provenance": {
            "trial_count": len(trial_rows),
            "target_benchmark_trial_count": target_benchmark_trials,
            "non_target_trial_count": non_target_trials,
            "task_set_trial_count": task_set_trials,
            "benchmark_ids": sorted(task_benchmark_ids),
            "worker_ids": worker_ids,
        },
    }


async def collect_canary_summary_from_db(
    *,
    db_url: str,
    benchmark_id: str,
    worker_pool: str,
    canary_batch_id: str | None = None,
) -> dict[str, Any]:
    engine = create_async_engine(normalize_db_url(db_url))
    try:
        async with engine.connect() as conn:
            if canary_batch_id:
                rows = (
                    (
                        await conn.execute(
                            text(
                                """
                            SELECT id::text, name, task_filter, state,
                                   result_status, created_at, finished_at,
                                   expected_trial_count, required_worker_pools
                            FROM batches
                            WHERE id = CAST(:batch_id AS uuid)
                            """
                            ),
                            {"batch_id": canary_batch_id},
                        )
                    )
                    .mappings()
                    .all()
                )
            else:
                rows = (
                    (
                        await conn.execute(
                            text(
                                """
                            SELECT id::text, name, task_filter, state,
                                   result_status, created_at, finished_at,
                                   expected_trial_count, required_worker_pools
                            FROM batches
                            WHERE state = 'finished'
                              AND result_status = 'succeeded'
                            ORDER BY finished_at DESC NULLS LAST,
                                     created_at DESC
                            LIMIT 100
                            """
                            )
                        )
                    )
                    .mappings()
                    .all()
                )

            selected: dict[str, Any] | None = None
            for row in rows:
                if _is_successful_canary_row(
                    dict(row),
                    benchmark_id=benchmark_id,
                    worker_pool=worker_pool,
                ):
                    selected = dict(row)
                    break
            if selected is None:
                raise HfBoundaryEvidenceError(
                    "no succeeded SkillLearnBench GB10 canary batch found; "
                    "run one or pass --canary-batch-id",
                )

            trial_rows = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT t.state, t.worker_id::text AS worker_id,
                               w.pool_name,
                               task.benchmark_id AS task_benchmark_id,
                               task.task_set_id
                        FROM trials t
                        LEFT JOIN workers w ON w.id = t.worker_id
                        LEFT JOIN tasks task ON task.id = t.task_id
                        WHERE t.batch_id = CAST(:batch_id AS uuid)
                        ORDER BY t.id
                        """
                        ),
                        {"batch_id": selected["id"]},
                    )
                )
                .mappings()
                .all()
            )
    finally:
        await engine.dispose()

    trial_summary = _summarize_canary_trials(
        [dict(row) for row in trial_rows],
        benchmark_id=benchmark_id,
    )

    return {
        "batch_id": selected["id"],
        "canary_started": bool(selected.get("created_at")),
        "terminal_state": selected.get("result_status") or selected.get("state"),
        "task_filter": selected.get("task_filter"),
        "worker_pools": trial_summary["worker_pools"],
        "expected_trial_count": selected.get("expected_trial_count"),
        "succeeded_trials": trial_summary["succeeded_trials"],
        "task_provenance": trial_summary["task_provenance"],
    }


def _run_kubectl_json(
    target: KubernetesServiceTarget,
    argv: Sequence[str],
) -> dict[str, Any]:
    cmd = [
        "kubectl",
        "-n",
        target.namespace,
        "exec",
        f"deploy/{target.deployment}",
        "-c",
        target.container,
        "--",
        *argv,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise HfBoundaryEvidenceError(
            f"kubectl exec {' '.join(argv[:3])} exited {proc.returncode}: {proc.stderr.strip()}"
        )
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise HfBoundaryEvidenceError(
            f"kubectl exec output was not JSON for {' '.join(argv[:3])}: {exc}",
        ) from exc
    if not isinstance(data, dict):
        raise HfBoundaryEvidenceError("kubectl exec JSON root must be an object")
    return data


def collect_audit_from_kubernetes(
    *,
    target: KubernetesServiceTarget,
    benchmark_id: str,
) -> dict[str, Any]:
    return _run_kubectl_json(
        target,
        [
            "loom",
            "datasets",
            "audit",
            benchmark_id,
            "--json",
            "--verify-bundles",
        ],
    )


def collect_source_summary_from_kubernetes(
    *,
    target: KubernetesServiceTarget,
    benchmark_id: str,
) -> dict[str, Any]:
    script = (
        "import asyncio,json,os;"
        "from loom_cli.hf_boundary_evidence import collect_source_summary_from_db;"
        "data=asyncio.run(collect_source_summary_from_db("
        "db_url=os.environ['LOOM_SVC_DB_URL'], "
        f"benchmark_id={benchmark_id!r}));"
        "print(json.dumps(data, sort_keys=True, default=str))"
    )
    return _run_kubectl_json(target, ["python3", "-c", script])


def collect_canary_summary_from_kubernetes(
    *,
    target: KubernetesServiceTarget,
    benchmark_id: str,
    worker_pool: str,
    canary_batch_id: str | None,
) -> dict[str, Any]:
    script = (
        "import asyncio,json,os;"
        "from loom_cli.hf_boundary_evidence import collect_canary_summary_from_db;"
        "data=asyncio.run(collect_canary_summary_from_db("
        "db_url=os.environ['LOOM_SVC_DB_URL'], "
        f"benchmark_id={benchmark_id!r}, "
        f"worker_pool={worker_pool!r}, "
        f"canary_batch_id={canary_batch_id!r}));"
        "print(json.dumps(data, sort_keys=True, default=str))"
    )
    return _run_kubectl_json(target, ["python3", "-c", script])


def collect_worker_boundary_from_gb10(
    *,
    cluster_config_path: Path,
    timeout_sec: float,
) -> dict[str, Any]:
    from loom_cli.cluster_config import load_cluster_config

    cfg = load_cluster_config(cluster_config_path)
    pool = getattr(cfg, "gb10_pool", None)
    hosts = list(getattr(pool, "hosts", []) or [])
    if not hosts:
        raise HfBoundaryEvidenceError("cluster config has no [gb10_pool] hosts")
    ssh_config = Path(str(getattr(pool, "ssh_config", "") or ""))
    if not str(ssh_config):
        raise HfBoundaryEvidenceError("[gb10_pool].ssh_config is required")
    if not ssh_config.is_absolute():
        ssh_config = cluster_config_path.parent / ssh_config

    def _optional_pool_path(value: object) -> Path | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = cluster_config_path.parent / path
        return path.resolve(strict=False)

    ssh_identity = _optional_pool_path(getattr(pool, "ssh_identity_file", ""))
    ssh_certificate = _optional_pool_path(getattr(pool, "ssh_certificate_file", ""))

    results: list[dict[str, Any]] = []
    for host in hosts:
        target = str(host.get("ssh_target") or "")
        if not target:
            continue
        repo_path = str(host.get("repo_path") or "").strip()
        env_file_path = str(host.get("env_file_path") or "").strip()
        if not repo_path:
            image_tag = str(getattr(cfg, "image_tag", ""))
            if (
                getattr(cfg, "runtime_environment", None) != "staging"
                or re.fullmatch(r"staging-[a-z0-9][a-z0-9-]{5,63}", image_tag) is None
            ):
                raise HfBoundaryEvidenceError(
                    "GB10 worker boundary has no service-owned candidate binding"
                )
            repo_path = (
                "/shared_work2/loom-staging-rollout/worker-repos/"
                f"loom-remote-worker-{image_tag}"
            )
            env_file_path = (
                "/shared_work2/loom-staging-rollout/worker-envs/"
                f"staging-gb10-worker-{image_tag}.env"
            )
        elif not env_file_path:
            env_file_path = str(Path(repo_path) / ".env")
        argv = ["ssh", "-F", str(ssh_config)]
        if ssh_identity is not None:
            argv.extend(["-i", str(ssh_identity), "-o", "IdentitiesOnly=yes"])
        if ssh_certificate is not None:
            argv.extend(["-o", f"CertificateFile={ssh_certificate}"])
        argv.extend(
            [
                target,
                "python3",
                "-",
                repo_path,
                env_file_path,
            ]
        )
        proc = subprocess.run(
            argv,
            capture_output=True,
            input=_REMOTE_WORKER_ENV_SCRIPT,
            text=True,
            check=False,
            timeout=timeout_sec,
        )
        entry: dict[str, Any] = {"host": target, "ssh_ok": proc.returncode == 0}
        if proc.returncode == 0:
            try:
                entry.update(json.loads(proc.stdout))
            except json.JSONDecodeError as exc:
                entry["parse_error"] = str(exc)
                entry["stdout_prefix"] = proc.stdout[:200]
        else:
            entry["stderr_prefix"] = proc.stderr[:300]
        results.append(entry)
    summary = {
        "checked_hosts": len(results),
        "checked_host_names": sorted(r["host"] for r in results),
        "ssh_failed_hosts": [r["host"] for r in results if not r.get("ssh_ok")],
        "docker_ps_failed_hosts": sorted(
            r["host"] for r in results if r.get("ssh_ok") and r.get("docker_ps_ok") is not True
        ),
        "hosts_without_containers": sorted(
            r["host"]
            for r in results
            if r.get("ssh_ok") and r.get("docker_ps_ok") is True and not (r.get("containers") or [])
        ),
        "env_file_missing_hosts": [
            r["host"] for r in results if r.get("ssh_ok") and not r.get("env_file_exists")
        ],
        "env_file_hf_token_present_hosts": [
            r["host"] for r in results if r.get("env_file_hf_token_present") is True
        ],
        "containers_checked": sum(len(r.get("containers") or []) for r in results),
        "hosts_with_container_hf_token_present": sorted(
            {
                r["host"]
                for r in results
                for c in (r.get("containers") or [])
                if c.get("hf_token_present") is True
            }
        ),
        "inspect_failed": [
            {"host": r["host"], "container": c.get("container")}
            for r in results
            for c in (r.get("containers") or [])
            if c.get("inspect_ok") is not True
        ],
    }
    return {"summary": summary, "results": results}


def _kube_target_from_args(args: Any) -> KubernetesServiceTarget | None:
    if not args.namespace:
        return None
    return KubernetesServiceTarget(
        namespace=args.namespace,
        deployment=args.kube_service_deployment,
        container=args.kube_service_container,
    )


def _load_or_collect_audit(
    args: Any,
    target: KubernetesServiceTarget | None,
) -> dict[str, Any]:
    if args.audit_json:
        return _read_json(Path(args.audit_json))
    if target is not None:
        return collect_audit_from_kubernetes(target=target, benchmark_id=args.benchmark)
    if args.db_url:
        from loom.trajectory.storage import MinioObjectStore
        from loom_cli.benchmark_readiness import (
            render_readiness_json,
            run_bundle_presence_audit,
            run_readiness_audit,
        )

        async def _run() -> dict[str, Any]:
            items = await run_readiness_audit(
                db_url=args.db_url,
                benchmark=args.benchmark,
            )
            bundle = None
            if args.minio_endpoint and args.minio_access_key and args.minio_secret_key:
                bundle = await run_bundle_presence_audit(
                    db_url=args.db_url,
                    benchmark=args.benchmark,
                    object_store=MinioObjectStore(
                        endpoint_url=args.minio_endpoint,
                        access_key=args.minio_access_key,
                        secret_key=args.minio_secret_key,
                    ),
                )
            data = json.loads(render_readiness_json(items))
            if not isinstance(data, dict):
                raise HfBoundaryEvidenceError("audit renderer returned non-object JSON")
            if bundle is not None:
                data["bundle_presence"] = bundle.to_dict()
            return data

        return asyncio.run(_run())
    raise HfBoundaryEvidenceError(
        "hf-boundary-evidence requires --audit-json, --namespace, or --db-url",
    )


def _load_or_collect_source_summary(
    args: Any,
    target: KubernetesServiceTarget | None,
) -> dict[str, Any]:
    if args.source_summary_json:
        return _read_json(Path(args.source_summary_json))
    if target is not None:
        return collect_source_summary_from_kubernetes(
            target=target,
            benchmark_id=args.benchmark,
        )
    if args.db_url:
        return asyncio.run(
            collect_source_summary_from_db(
                db_url=args.db_url,
                benchmark_id=args.benchmark,
            )
        )
    raise HfBoundaryEvidenceError(
        "hf-boundary-evidence requires --source-summary-json, --namespace, or --db-url",
    )


def _load_or_collect_canary(
    args: Any,
    target: KubernetesServiceTarget | None,
) -> dict[str, Any]:
    if args.canary_summary_json:
        return _read_json(Path(args.canary_summary_json))
    if target is not None:
        return collect_canary_summary_from_kubernetes(
            target=target,
            benchmark_id=args.benchmark,
            worker_pool=args.worker_pool,
            canary_batch_id=args.canary_batch_id,
        )
    if args.db_url:
        return asyncio.run(
            collect_canary_summary_from_db(
                db_url=args.db_url,
                benchmark_id=args.benchmark,
                worker_pool=args.worker_pool,
                canary_batch_id=args.canary_batch_id,
            )
        )
    raise HfBoundaryEvidenceError(
        "hf-boundary-evidence requires --canary-summary-json, --namespace, or --db-url",
    )


def _load_or_collect_worker_boundary(args: Any) -> dict[str, Any]:
    if args.worker_boundary_json:
        return _read_json(Path(args.worker_boundary_json))
    if args.cluster_config:
        return collect_worker_boundary_from_gb10(
            cluster_config_path=Path(args.cluster_config),
            timeout_sec=args.ssh_timeout_sec,
        )
    raise HfBoundaryEvidenceError(
        "hf-boundary-evidence requires --worker-boundary-json or --cluster-config",
    )


def run_hf_boundary_evidence_command(args: Any) -> int:
    try:
        target = _kube_target_from_args(args)
        audit = _load_or_collect_audit(args, target)
        source_summary = _load_or_collect_source_summary(args, target)
        canary = _load_or_collect_canary(args, target)
        worker_boundary = _load_or_collect_worker_boundary(args)
        evidence = compose_boundary_evidence(
            benchmark_id=args.benchmark,
            environment=args.environment,
            audit_report=audit,
            source_summary=source_summary,
            canary_summary=canary,
            worker_boundary=worker_boundary,
        )
        if not args.gb10_workers_status:
            raise HfBoundaryEvidenceError(
                "hf-boundary-evidence requires --gb10-workers-status for candidate binding"
            )
        gb10_status_path = Path(args.gb10_workers_status)
        gb10_status = _read_json(gb10_status_path)
        evidence["candidate_binding"] = _candidate_binding_from_gb10_status(
            gb10_status,
            environment=args.environment,
        )
        evidence["evidence_inputs"] = {
            "audit_json": str(args.audit_json) if args.audit_json else None,
            "source_summary_json": (
                str(args.source_summary_json) if args.source_summary_json else None
            ),
            "canary_summary_json": (
                str(args.canary_summary_json) if args.canary_summary_json else None
            ),
            "worker_boundary_json": (
                str(args.worker_boundary_json) if args.worker_boundary_json else None
            ),
            "gb10_workers_status": (
                str(args.gb10_workers_status) if args.gb10_workers_status else None
            ),
            "namespace": args.namespace,
        }
        write_secret_safe_json(evidence, Path(args.output))
    except HfBoundaryEvidenceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"wrote HF boundary evidence: {args.output}")
    return 0
