from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import Engine

from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.sandbox.docker_terminal import (
    DockerOwnedContainerCleaner,
    DockerOwnedContainerCleanupResult,
)
from agentic_data_platform.service.config import ServiceSettings, load_service_settings
from agentic_data_platform.service.run_event_fanout import build_run_event_fanout, configure_run_event_fanout


@dataclass(frozen=True)
class SchedulerDispatchResult:
    scheduler_id: str
    dispatched_run_ids: list[str]
    capacity_blocked_runs: list[dict[str, object]]

    @property
    def dispatched_count(self) -> int:
        return len(self.dispatched_run_ids)

    @property
    def capacity_blocked_count(self) -> int:
        return len(self.capacity_blocked_runs)

    def to_dict(self) -> dict[str, object]:
        return {
            "scheduler_id": self.scheduler_id,
            "dispatched_count": self.dispatched_count,
            "dispatched_run_ids": list(self.dispatched_run_ids),
            "capacity_blocked_count": self.capacity_blocked_count,
            "capacity_blocked_runs": list(self.capacity_blocked_runs),
        }


@dataclass(frozen=True)
class SchedulerDockerCleanupRun:
    run_id: str
    cleanup_status: str
    container_ids: list[str]
    removed_container_ids: list[str]
    list_exit_code: int | None
    removal_exit_code: int | None
    attempt_filter: str | None = None
    cleanup_error_reason: str | None = None

    @classmethod
    def from_cleanup_result(cls, result: DockerOwnedContainerCleanupResult) -> SchedulerDockerCleanupRun:
        return cls(
            run_id=result.run_id,
            cleanup_status="completed",
            container_ids=list(result.container_ids),
            removed_container_ids=list(result.removed_container_ids),
            list_exit_code=result.list_exit_code,
            removal_exit_code=result.removal_exit_code,
            attempt_filter=result.attempt_id,
        )

    @classmethod
    def failed(cls, *, run_id: str, error_reason: str) -> SchedulerDockerCleanupRun:
        return cls(
            run_id=run_id,
            cleanup_status="failed",
            container_ids=[],
            removed_container_ids=[],
            list_exit_code=None,
            removal_exit_code=None,
            cleanup_error_reason=error_reason,
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "run_id": self.run_id,
            "cleanup_status": self.cleanup_status,
            "container_ids": list(self.container_ids),
            "removed_container_ids": list(self.removed_container_ids),
            "container_count": len(self.container_ids),
            "removed_container_count": len(self.removed_container_ids),
            "list_exit_code": self.list_exit_code,
            "removal_exit_code": self.removal_exit_code,
        }
        if self.attempt_filter is not None:
            payload["attempt_filter"] = self.attempt_filter
        if self.cleanup_error_reason is not None:
            payload["cleanup_error_reason"] = self.cleanup_error_reason
        return payload


@dataclass(frozen=True)
class SchedulerRecoveryResult:
    scheduler_id: str
    requeued_run_ids: list[str]
    failed_run_ids: list[str]
    terminal_mismatch_run_ids: list[str]
    projection_refreshed_run_ids: list[str]
    artifact_expired_artifact_ids: list[str]
    docker_cleanup_runs: list[dict[str, object]]

    @property
    def requeued_count(self) -> int:
        return len(self.requeued_run_ids)

    @property
    def failed_count(self) -> int:
        return len(self.failed_run_ids)

    @property
    def terminal_mismatch_count(self) -> int:
        return len(self.terminal_mismatch_run_ids)

    @property
    def projection_refreshed_count(self) -> int:
        return len(self.projection_refreshed_run_ids)

    @property
    def artifact_expired_count(self) -> int:
        return len(self.artifact_expired_artifact_ids)

    @property
    def docker_cleanup_count(self) -> int:
        return len(self.docker_cleanup_runs)

    @property
    def docker_cleanup_error_count(self) -> int:
        return sum(1 for item in self.docker_cleanup_runs if item.get("cleanup_status") == "failed")

    def to_dict(self) -> dict[str, object]:
        return {
            "scheduler_id": self.scheduler_id,
            "requeued_count": self.requeued_count,
            "requeued_run_ids": list(self.requeued_run_ids),
            "failed_count": self.failed_count,
            "failed_run_ids": list(self.failed_run_ids),
            "terminal_mismatch_count": self.terminal_mismatch_count,
            "terminal_mismatch_run_ids": list(self.terminal_mismatch_run_ids),
            "projection_refreshed_count": self.projection_refreshed_count,
            "projection_refreshed_run_ids": list(self.projection_refreshed_run_ids),
            "artifact_expired_count": self.artifact_expired_count,
            "artifact_expired_artifact_ids": list(self.artifact_expired_artifact_ids),
            "docker_cleanup_count": self.docker_cleanup_count,
            "docker_cleanup_error_count": self.docker_cleanup_error_count,
            "docker_cleanup_runs": list(self.docker_cleanup_runs),
        }


class RunScheduler:
    def __init__(
        self,
        *,
        engine: Engine,
        scheduler_id: str,
        settings: ServiceSettings,
        docker_container_cleaner: DockerOwnedContainerCleaner | None = None,
    ) -> None:
        _require_non_empty("scheduler_id", scheduler_id)
        self.engine = engine
        self.scheduler_id = scheduler_id
        self.settings = settings
        self.docker_container_cleaner = docker_container_cleaner

    def dispatch_once(self, *, request_id: str | None = None) -> SchedulerDispatchResult:
        observed_usage_since = self._observed_usage_since()
        with session_scope(self.engine) as session:
            result = RunRepository(session).dispatch_queued_runs_with_diagnostics(
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_global_max_active_runs,
                backend_limits=self.settings.scheduler_backend_max_active_runs,
                project_limits=self.settings.scheduler_project_max_active_runs,
                provider_limits=self.settings.scheduler_provider_max_active_runs,
                model_limits=self.settings.scheduler_model_max_active_runs,
                agent_limits=self.settings.scheduler_agent_max_active_runs,
                benchmark_limits=self.settings.scheduler_benchmark_max_active_runs,
                provider_cost_limits_usd=self.settings.scheduler_provider_max_estimated_cost_usd,
                model_cost_limits_usd=self.settings.scheduler_model_max_estimated_cost_usd,
                provider_observed_cost_limits_usd=self.settings.scheduler_provider_max_observed_cost_usd,
                model_observed_cost_limits_usd=self.settings.scheduler_model_max_observed_cost_usd,
                provider_token_limits=self.settings.scheduler_provider_max_estimated_tokens,
                model_token_limits=self.settings.scheduler_model_max_estimated_tokens,
                provider_observed_token_limits=self.settings.scheduler_provider_max_observed_tokens,
                model_observed_token_limits=self.settings.scheduler_model_max_observed_tokens,
                provider_observed_request_limits=self.settings.scheduler_provider_max_observed_requests,
                model_observed_request_limits=self.settings.scheduler_model_max_observed_requests,
                observed_usage_since=observed_usage_since,
                request_id=request_id,
            )
        return SchedulerDispatchResult(
            scheduler_id=self.scheduler_id,
            dispatched_run_ids=[run.run_id for run in result.dispatched_runs],
            capacity_blocked_runs=[block.to_dict() for block in result.capacity_blocked_runs],
        )

    def _observed_usage_since(self) -> datetime | None:
        if self.settings.scheduler_observed_usage_window_seconds <= 0:
            return None
        if not (
            self.settings.scheduler_provider_max_observed_tokens
            or self.settings.scheduler_model_max_observed_tokens
            or self.settings.scheduler_provider_max_observed_requests
            or self.settings.scheduler_model_max_observed_requests
            or self.settings.scheduler_provider_max_observed_cost_usd
            or self.settings.scheduler_model_max_observed_cost_usd
        ):
            return None
        return datetime.now(timezone.utc) - timedelta(seconds=self.settings.scheduler_observed_usage_window_seconds)

    def recover_once(self, *, request_id: str | None = None) -> SchedulerRecoveryResult:
        stale_dispatched_older_than = datetime.now(timezone.utc) - timedelta(
            seconds=self.settings.scheduler_stale_dispatched_timeout_seconds
        )
        stale_active_older_than = datetime.now(timezone.utc) - timedelta(
            seconds=self.settings.scheduler_stale_active_heartbeat_timeout_seconds
        )
        stale_artifact_upload_older_than = datetime.now(timezone.utc) - timedelta(
            seconds=self.settings.scheduler_stale_artifact_upload_timeout_seconds
        )
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            requeued = repository.requeue_stale_dispatched_runs(
                older_than=stale_dispatched_older_than,
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_recovery_batch_size,
                request_id=request_id,
            )
            terminal_mismatches = repository.recover_terminal_result_mismatches(
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_recovery_batch_size,
                request_id=request_id,
            )
            failed = repository.fail_stale_active_runs_by_heartbeat(
                older_than=stale_active_older_than,
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_recovery_batch_size,
                request_id=request_id,
            )
            artifact_expired = repository.expire_stale_artifact_uploads(
                older_than=stale_artifact_upload_older_than,
                scheduler_id=self.scheduler_id,
                max_artifacts=self.settings.scheduler_recovery_batch_size,
                request_id=request_id,
            )
            projection_refreshed = repository.refresh_terminal_dashboard_projections(
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_recovery_batch_size,
                request_id=request_id,
            )
            projection_refreshed_ids = [projection.run_id for projection in projection_refreshed]
            for run in [*terminal_mismatches, *failed]:
                if run.run_id not in projection_refreshed_ids:
                    projection_refreshed_ids.append(run.run_id)
        docker_cleanup_runs = self._cleanup_recovered_docker_containers(
            run_ids=[run.run_id for run in [*terminal_mismatches, *failed]],
            request_id=request_id,
        )
        return SchedulerRecoveryResult(
            scheduler_id=self.scheduler_id,
            requeued_run_ids=[run.run_id for run in requeued],
            failed_run_ids=[run.run_id for run in [*terminal_mismatches, *failed]],
            terminal_mismatch_run_ids=[run.run_id for run in terminal_mismatches],
            projection_refreshed_run_ids=projection_refreshed_ids,
            artifact_expired_artifact_ids=[artifact.artifact_id for artifact in artifact_expired],
            docker_cleanup_runs=[record.to_dict() for record in docker_cleanup_runs],
        )

    def _cleanup_recovered_docker_containers(
        self,
        *,
        run_ids: list[str],
        request_id: str | None,
    ) -> list[SchedulerDockerCleanupRun]:
        if not self.settings.scheduler_docker_cleanup_enabled or not run_ids:
            return []

        cleaner = self.docker_container_cleaner or DockerOwnedContainerCleaner(
            timeout_seconds=self.settings.scheduler_docker_cleanup_timeout_seconds,
        )
        cleanup_records: list[SchedulerDockerCleanupRun] = []
        for run_id in run_ids:
            try:
                cleanup = cleaner.cleanup_run(run_id=run_id)
                cleanup_records.append(SchedulerDockerCleanupRun.from_cleanup_result(cleanup))
            except Exception as exc:
                cleanup_records.append(
                    SchedulerDockerCleanupRun.failed(
                        run_id=run_id,
                        error_reason=_bounded_error_reason(exc),
                    )
                )

        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            for record in cleanup_records:
                repository.record_sandbox_container_cleanup(
                    run_id=record.run_id,
                    scheduler_id=self.scheduler_id,
                    cleanup_status=record.cleanup_status,
                    container_ids=record.container_ids,
                    removed_container_ids=record.removed_container_ids,
                    list_exit_code=record.list_exit_code,
                    removal_exit_code=record.removal_exit_code,
                    attempt_filter=record.attempt_filter,
                    cleanup_error_reason=record.cleanup_error_reason,
                    request_id=request_id,
                )
        return cleanup_records


def build_configured_scheduler(
    settings: ServiceSettings | None = None,
    *,
    scheduler_id: str = "scheduler-dev-1",
) -> RunScheduler:
    service_settings = settings or load_service_settings()
    if not service_settings.database_url:
        raise ValueError("DATABASE_URL is required for scheduler service")
    configure_run_event_fanout(build_run_event_fanout(service_settings))
    return RunScheduler(
        engine=create_database_engine(service_settings.database_url, pool_pre_ping=True),
        scheduler_id=scheduler_id,
        settings=service_settings,
    )


def run_scheduler_loop(
    scheduler: RunScheduler,
    *,
    poll_interval_seconds: float = 5.0,
) -> None:
    while True:
        recovery_result = scheduler.recover_once()
        if (
            recovery_result.requeued_count
            or recovery_result.failed_count
            or recovery_result.projection_refreshed_count
            or recovery_result.artifact_expired_count
            or recovery_result.docker_cleanup_count
        ):
            print(json.dumps({"action": "recover", **recovery_result.to_dict()}, sort_keys=True), flush=True)
        result = scheduler.dispatch_once()
        if result.dispatched_count or result.capacity_blocked_count:
            print(json.dumps({"action": "dispatch", **result.to_dict()}, sort_keys=True), flush=True)
        time.sleep(poll_interval_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Agentic Data Platform run scheduler.")
    parser.add_argument("--once", action="store_true", help="dispatch at most one capacity window and exit")
    parser.add_argument("--recover-once", action="store_true", help="recover stale dispatched runs and exit")
    parser.add_argument("--scheduler-id", default="scheduler-dev-1")
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)

    scheduler = build_configured_scheduler(scheduler_id=args.scheduler_id)
    if args.recover_once:
        result = scheduler.recover_once()
        print(json.dumps(result.to_dict(), sort_keys=True))
        return 0
    if args.once:
        result = scheduler.dispatch_once()
        print(json.dumps(result.to_dict(), sort_keys=True))
        return 0

    run_scheduler_loop(scheduler, poll_interval_seconds=args.poll_interval_seconds)
    return 0


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _bounded_error_reason(error: Exception, *, limit: int = 500) -> str:
    message = str(error).strip() or error.__class__.__name__
    if len(message) <= limit:
        return message
    return f"{message[:limit]}..."


if __name__ == "__main__":
    raise SystemExit(main())
