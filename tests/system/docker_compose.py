"""Helpers for spinning the deploy/docker-compose.test.yml stack up + down
around a single pytest session.

System tests are explicitly OUT of the default `pytest tests/` collection
(see pyproject.toml: `addopts = "--ignore=tests/system"`). Run them with
`pytest tests/system -v` from a host with Docker + docker-compose v2.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "deploy" / "docker-compose.test.yml"


def _compose(*args: str, check: bool = True, **kwargs: object) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    return subprocess.run(  # type: ignore[no-any-return]
        cmd, check=check, cwd=REPO_ROOT, capture_output=True, text=True,
        **kwargs,  # type: ignore[arg-type]
    )


def stack_up(timeout_sec: float = 180.0) -> None:
    _compose("up", "-d", "--build")
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        ps = _compose("ps", "--format", "json")
        statuses = [
            json.loads(line)
            for line in ps.stdout.splitlines()
            if line.strip()
        ]
        if statuses and all(
            s.get("Health") in (None, "", "healthy")
            and s.get("State") == "running"
            for s in statuses
        ):
            return
        time.sleep(1.0)
    raise RuntimeError(
        f"compose stack did not become healthy within {timeout_sec}s; "
        "inspect with `docker compose -f deploy/docker-compose.test.yml ps`.",
    )


def stack_down() -> None:
    _compose("down", "-v", check=False)


def kill_service(name: str) -> None:
    _compose("kill", name, check=False)


def start_service(name: str) -> None:
    _compose("start", name, check=False)


def run_seed(task_id: str = "hello-world", which: str = "team") -> str:
    """Run scripts/seed_test_data.py against the running stack.

    Returns the chosen token (stripped). `which` is forwarded to --print."""
    out = subprocess.run(
        ["python", "scripts/seed_test_data.py",
         "--task-id", task_id, "--print", which],
        check=True, cwd=REPO_ROOT, capture_output=True, text=True,
    )
    return out.stdout.strip()
