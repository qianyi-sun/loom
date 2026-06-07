"""Session fixtures: bring the compose stack up once per session.

`compose_stack` returns dict with service URLs + the team token + the
worker token. Tests that submit trials use `compose_stack["team_token"]`;
the worker_crash test passes `compose_stack["worker_token"]` back to
`start_service("worker", ...)` after killing the worker container.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tests.system.docker_compose import stack_down, stack_up


@pytest.fixture(scope="session")
def compose_stack() -> Iterator[dict[str, str]]:
    if os.environ.get("LOOM_SKIP_SYSTEM_TESTS") == "1":
        pytest.skip("LOOM_SKIP_SYSTEM_TESTS=1 — skipping compose-based suite")
    team_token, worker_token = stack_up()
    try:
        yield {
            "control_plane": "http://localhost:58080",
            "gateway": "http://localhost:59100",
            "minio": "http://localhost:59000",
            "team_token": team_token,
            "worker_token": worker_token,
        }
    finally:
        stack_down()
