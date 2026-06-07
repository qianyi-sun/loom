"""Session fixtures: bring the compose stack up once per session."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tests.system.docker_compose import stack_down, stack_up


@pytest.fixture(scope="session")
def compose_stack() -> Iterator[dict[str, str]]:
    """Spin the compose stack up, hand back service URLs, tear down on exit."""
    if os.environ.get("LOOM_SKIP_SYSTEM_TESTS") == "1":
        pytest.skip("LOOM_SKIP_SYSTEM_TESTS=1 — skipping compose-based suite")
    stack_up()
    try:
        yield {
            "control_plane": "http://localhost:58080",
            "gateway": "http://localhost:59100",
            "minio": "http://localhost:59000",
        }
    finally:
        stack_down()
