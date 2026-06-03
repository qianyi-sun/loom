from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Sequence

from agentic_data_platform.sandbox.docker_terminal import DockerOwnedContainerCleaner


@dataclass(frozen=True)
class ContainerLeakAuditResult:
    checked_run_count: int
    leaked_containers: dict[str, list[str]]
    audit_attempt_count: int = 1

    @property
    def leaked_run_count(self) -> int:
        return len(self.leaked_containers)

    @property
    def leaked_container_count(self) -> int:
        return sum(len(container_ids) for container_ids in self.leaked_containers.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "checked_run_count": self.checked_run_count,
            "audit_attempt_count": self.audit_attempt_count,
            "leaked_run_count": self.leaked_run_count,
            "leaked_container_count": self.leaked_container_count,
            "leaked_containers": {
                run_id: list(container_ids) for run_id, container_ids in self.leaked_containers.items()
            },
        }


def audit_run_containers(
    run_ids: Sequence[str],
    *,
    cleaner: DockerOwnedContainerCleaner | None = None,
    max_attempts: int = 1,
    poll_interval_seconds: float = 0.0,
    sleeper: Callable[[float], object] = time.sleep,
) -> ContainerLeakAuditResult:
    if not run_ids:
        raise ValueError("at least one run id is required")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if poll_interval_seconds < 0:
        raise ValueError("poll_interval_seconds must be non-negative")

    docker_cleaner = cleaner or DockerOwnedContainerCleaner()
    leaked_containers: dict[str, list[str]] = {}
    audit_attempt_count = 0
    for attempt in range(1, max_attempts + 1):
        audit_attempt_count = attempt
        leaked_containers = {}
        for run_id in run_ids:
            container_ids = docker_cleaner.list_run_containers(run_id=run_id)
            if container_ids:
                leaked_containers[run_id] = container_ids
        if not leaked_containers:
            break
        if attempt < max_attempts and poll_interval_seconds > 0:
            sleeper(poll_interval_seconds)

    return ContainerLeakAuditResult(
        checked_run_count=len(run_ids),
        leaked_containers=leaked_containers,
        audit_attempt_count=audit_attempt_count,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit platform-owned Docker containers left after smoke runs.")
    parser.add_argument("--run-id", dest="run_ids", action="append", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.0)
    args = parser.parse_args(argv)

    result = audit_run_containers(
        args.run_ids,
        cleaner=DockerOwnedContainerCleaner(timeout_seconds=args.timeout_seconds),
        max_attempts=args.max_attempts,
        poll_interval_seconds=args.poll_interval_seconds,
    )
    print(json.dumps(result.to_dict(), sort_keys=True))
    return 1 if result.leaked_container_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
