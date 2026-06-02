from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import Engine

from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.service.config import ServiceSettings, load_service_settings


@dataclass(frozen=True)
class SchedulerDispatchResult:
    scheduler_id: str
    dispatched_run_ids: list[str]

    @property
    def dispatched_count(self) -> int:
        return len(self.dispatched_run_ids)

    def to_dict(self) -> dict[str, object]:
        return {
            "scheduler_id": self.scheduler_id,
            "dispatched_count": self.dispatched_count,
            "dispatched_run_ids": list(self.dispatched_run_ids),
        }


@dataclass(frozen=True)
class SchedulerRecoveryResult:
    scheduler_id: str
    requeued_run_ids: list[str]
    failed_run_ids: list[str]
    projection_refreshed_run_ids: list[str]

    @property
    def requeued_count(self) -> int:
        return len(self.requeued_run_ids)

    @property
    def failed_count(self) -> int:
        return len(self.failed_run_ids)

    @property
    def projection_refreshed_count(self) -> int:
        return len(self.projection_refreshed_run_ids)

    def to_dict(self) -> dict[str, object]:
        return {
            "scheduler_id": self.scheduler_id,
            "requeued_count": self.requeued_count,
            "requeued_run_ids": list(self.requeued_run_ids),
            "failed_count": self.failed_count,
            "failed_run_ids": list(self.failed_run_ids),
            "projection_refreshed_count": self.projection_refreshed_count,
            "projection_refreshed_run_ids": list(self.projection_refreshed_run_ids),
        }


class RunScheduler:
    def __init__(
        self,
        *,
        engine: Engine,
        scheduler_id: str,
        settings: ServiceSettings,
    ) -> None:
        _require_non_empty("scheduler_id", scheduler_id)
        self.engine = engine
        self.scheduler_id = scheduler_id
        self.settings = settings

    def dispatch_once(self, *, request_id: str | None = None) -> SchedulerDispatchResult:
        with session_scope(self.engine) as session:
            dispatched = RunRepository(session).dispatch_queued_runs(
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_global_max_active_runs,
                backend_limits=self.settings.scheduler_backend_max_active_runs,
                project_limits=self.settings.scheduler_project_max_active_runs,
                provider_limits=self.settings.scheduler_provider_max_active_runs,
                model_limits=self.settings.scheduler_model_max_active_runs,
                agent_limits=self.settings.scheduler_agent_max_active_runs,
                benchmark_limits=self.settings.scheduler_benchmark_max_active_runs,
                request_id=request_id,
            )
        return SchedulerDispatchResult(
            scheduler_id=self.scheduler_id,
            dispatched_run_ids=[run.run_id for run in dispatched],
        )

    def recover_once(self, *, request_id: str | None = None) -> SchedulerRecoveryResult:
        stale_dispatched_older_than = datetime.now(timezone.utc) - timedelta(
            seconds=self.settings.scheduler_stale_dispatched_timeout_seconds
        )
        stale_active_older_than = datetime.now(timezone.utc) - timedelta(
            seconds=self.settings.scheduler_stale_active_heartbeat_timeout_seconds
        )
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            requeued = repository.requeue_stale_dispatched_runs(
                older_than=stale_dispatched_older_than,
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
            projection_refreshed = repository.refresh_terminal_dashboard_projections(
                scheduler_id=self.scheduler_id,
                max_runs=self.settings.scheduler_recovery_batch_size,
                request_id=request_id,
            )
            projection_refreshed_ids = [projection.run_id for projection in projection_refreshed]
            for run in failed:
                if run.run_id not in projection_refreshed_ids:
                    projection_refreshed_ids.append(run.run_id)
        return SchedulerRecoveryResult(
            scheduler_id=self.scheduler_id,
            requeued_run_ids=[run.run_id for run in requeued],
            failed_run_ids=[run.run_id for run in failed],
            projection_refreshed_run_ids=projection_refreshed_ids,
        )


def build_configured_scheduler(
    settings: ServiceSettings | None = None,
    *,
    scheduler_id: str = "scheduler-dev-1",
) -> RunScheduler:
    service_settings = settings or load_service_settings()
    if not service_settings.database_url:
        raise ValueError("DATABASE_URL is required for scheduler service")
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
        ):
            print(json.dumps({"action": "recover", **recovery_result.to_dict()}, sort_keys=True), flush=True)
        result = scheduler.dispatch_once()
        if result.dispatched_count:
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


if __name__ == "__main__":
    raise SystemExit(main())
