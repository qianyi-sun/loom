"""Shared persistence/projection helpers for trial resource accounting."""

from __future__ import annotations

from typing import Any

from loom.db.schema import TrialResourceUsage
from loom.models.resource_usage import (
    ResourceCounters,
    ResourceLimits,
    TrialResourceUsageReport,
    aggregate_resource_usage,
)

_COUNTER_COLUMNS = tuple(ResourceCounters.model_fields)


def report_values(
    report: TrialResourceUsageReport,
    *,
    lifecycle_authority_id: object | None,
) -> dict[str, Any]:
    return {
        "trial_id": report.trial_id,
        "worker_id": report.worker_id,
        "lifecycle_authority_id": lifecycle_authority_id,
        "attempt_count": report.attempt_count,
        "execution_key": report.execution_key,
        "runtime_id_hash": report.runtime_id_hash,
        "container_role": report.container_role,
        "role_name": report.role_name,
        "backend": report.backend,
        "architecture": report.architecture,
        "candidate_sha": report.candidate_sha,
        "image_digest": report.image_digest,
        "source": report.source,
        "observation_seq": report.observation_seq,
        "container_started_at": report.container_started_at,
        "first_observed_at": report.first_observed_at,
        "last_observed_at": report.last_observed_at,
        "finalized_at": report.finalized_at,
        "terminal_reason": report.terminal_reason,
        "completeness": report.completeness,
        "diagnostic_code": report.diagnostic_code,
        "cpu_limit_cores": report.limits.cpu_cores,
        "memory_limit_bytes": report.limits.memory_bytes,
        "pids_limit": report.limits.pids,
        "resource_profile": report.limits.resource_profile,
        "schema_version": report.schema_version,
        **report.counters.model_dump(),
    }


def row_to_report(row: TrialResourceUsage) -> TrialResourceUsageReport:
    return TrialResourceUsageReport(
        schema_version=1,
        trial_id=row.trial_id,
        worker_id=row.worker_id,
        attempt_count=row.attempt_count,
        execution_key=row.execution_key,
        runtime_id_hash=row.runtime_id_hash,
        container_role=row.container_role,
        role_name=row.role_name,
        backend=row.backend,
        architecture=row.architecture,
        candidate_sha=row.candidate_sha,
        image_digest=row.image_digest,
        source=row.source,
        observation_seq=row.observation_seq,
        container_started_at=row.container_started_at,
        first_observed_at=row.first_observed_at,
        last_observed_at=row.last_observed_at,
        finalized_at=row.finalized_at,
        terminal_reason=row.terminal_reason,
        completeness=row.completeness,
        diagnostic_code=row.diagnostic_code,
        limits=ResourceLimits(
            cpu_cores=row.cpu_limit_cores,
            memory_bytes=row.memory_limit_bytes,
            pids=row.pids_limit,
            resource_profile=row.resource_profile,
        ),
        counters=ResourceCounters(
            **{column: getattr(row, column) for column in _COUNTER_COLUMNS},
        ),
    )


def resource_usage_response(rows: list[TrialResourceUsage]) -> dict[str, Any]:
    reports = [row_to_report(row) for row in rows]
    return {
        "schema_version": 1,
        "aggregate": aggregate_resource_usage(reports),
        "items": [report.model_dump(mode="json") for report in reports],
    }
