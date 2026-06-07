"""Helpers for spinning the deploy/docker-compose.test.yml stack up + down
around a single pytest session.

System tests are explicitly OUT of the default `pytest tests/` collection
(see pyproject.toml: `addopts = "--ignore=tests/system"`). Run them with
`pytest tests/system -v` from a host with Docker + docker-compose v2.

Compose-up has TWO stages because the worker container needs a real
worker token in its environment before it can register:

  1. Bring up infra + control-plane + gateway (worker excluded by
     `profiles: ["worker"]`).
  2. Run seed_test_data.py against the now-running Postgres to mint a
     team token + worker token + seed the fixture task.
  3. Re-invoke `docker compose up -d` with the worker profile enabled
     and `LOOM_WORKER_TOKEN` exported into the subprocess env.

stack_up() returns both raw tokens so tests can use the team token for
HTTP calls; the worker token lives inside the worker container env.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "deploy" / "docker-compose.test.yml"


def _compose(
    *args: str,
    check: bool = True,
    env_extra: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    env = os.environ.copy()
    # Stub LOOM_WORKER_TOKEN so compose can parse the file even when the
    # worker service is not in the active profile. The :? required-form
    # in compose only triggers when the service is selected.
    env.setdefault("LOOM_WORKER_TOKEN", "unused-no-profile")
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        cmd, check=check, cwd=REPO_ROOT, capture_output=True, text=True,
        env=env,
    )


def _wait_services_healthy(
    services: list[str], timeout_sec: float,
) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        ps = _compose("ps", "--format", "json")
        statuses = [
            json.loads(line)
            for line in ps.stdout.splitlines()
            if line.strip()
        ]
        observed = {s.get("Service"): s for s in statuses}
        healthy = all(
            observed.get(name, {}).get("Health") in (None, "", "healthy")
            and observed.get(name, {}).get("State") == "running"
            for name in services
        )
        if healthy and all(name in observed for name in services):
            return
        time.sleep(1.0)
    raise RuntimeError(
        f"services {services} did not become healthy within {timeout_sec}s; "
        f"inspect with `docker compose -f {COMPOSE_FILE} ps`.",
    )


def stack_up(
    task_id: str = "hello-world",
    timeout_sec: float = 300.0,
) -> tuple[str, str]:
    """Two-stage compose-up. Returns (team_token, worker_token)."""
    # Stage 1: deps + control plane + gateway. Worker excluded by profile.
    _compose("up", "-d", "--build")
    _wait_services_healthy(
        ["postgres", "minio", "llm-gateway", "control-plane"],
        timeout_sec=timeout_sec,
    )

    # Stage 2: mint a team + worker token (and seed the fixture).
    seed = subprocess.run(
        ["python", "scripts/seed_test_data.py",
         "--task-id", task_id, "--print", "both"],
        check=True, cwd=REPO_ROOT, capture_output=True, text=True,
    )
    team_token, worker_token = seed.stdout.strip().splitlines()[:2]

    # Stage 3: start the worker with the real token wired in.
    _compose(
        "--profile", "worker", "up", "-d",
        env_extra={"LOOM_WORKER_TOKEN": worker_token},
    )
    _wait_services_healthy(["worker"], timeout_sec=60.0)

    return team_token, worker_token


def stack_down() -> None:
    _compose("--profile", "worker", "down", "-v", check=False)


def kill_service(name: str) -> None:
    _compose("kill", name, check=False)


def start_service(name: str, worker_token: str | None = None) -> None:
    """Restart a service. For the worker, pass the original worker_token
    so the compose env-var substitution doesn't fail on restart."""
    env_extra = {"LOOM_WORKER_TOKEN": worker_token} if worker_token else None
    if name == "worker":
        _compose(
            "--profile", "worker", "start", name,
            check=False, env_extra=env_extra,
        )
    else:
        _compose("start", name, check=False, env_extra=env_extra)


def run_seed(task_id: str = "hello-world", which: str = "team") -> str:
    """Run scripts/seed_test_data.py against the running stack.

    Used by tests that need to seed an ADDITIONAL fixture after the
    session-scoped stack_up() (which seeds one). Returns the chosen
    token (stripped). `which` is forwarded to --print.
    """
    out = subprocess.run(
        ["python", "scripts/seed_test_data.py",
         "--task-id", task_id, "--print", which],
        check=True, cwd=REPO_ROOT, capture_output=True, text=True,
    )
    return out.stdout.strip()
