"""Top-level test fixtures live here. Per-package fixtures live in
nested conftest.py files.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer

from tests.support.executable_capacity_harness import ExecutableCapacityHarness

_TEST_STEP_JWT_SIGNING_KEY = "test-step-jwt-signing-key-do-not-use-in-prod"


@pytest.fixture(autouse=True)
def _default_step_jwt_signing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """ControlPlaneSettings + GatewaySettings now require this env var.
    Existing tests don't know about it; this autouse fixture supplies a
    fixed-but-distinct key so they continue to construct settings without
    each fixture needing the explicit setenv call. Production deploys
    must override via loom-secrets/step-jwt-signing-key.
    """
    monkeypatch.setenv("LOOM_CP_STEP_JWT_SIGNING_KEY", _TEST_STEP_JWT_SIGNING_KEY)
    monkeypatch.setenv("LOOM_GW_STEP_JWT_SIGNING_KEY", _TEST_STEP_JWT_SIGNING_KEY)
    monkeypatch.setenv("LOOM_GW_MINIO_ACCESS_KEY", "test-minio-access-key")
    monkeypatch.setenv("LOOM_GW_MINIO_SECRET_KEY", "test-minio-secret-key")


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """Session-scoped Postgres 16 container with Loom's Alembic schema applied.

    Tests that need a real DB import this fixture; the container is created
    once per pytest session (~1.5s overhead) and shared.
    """
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[1]
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root,
            check=True,
        )
        yield url


@pytest.fixture
async def executable_capacity_harness(
    tmp_path: Path,
    postgres_url: str,
    capacity_guard_template_database: dict[str, object],
) -> AsyncIterator[ExecutableCapacityHarness]:
    """Create the isolated two-pool executable bridge proof deployment."""

    harness = await ExecutableCapacityHarness.create(
        tmp_path,
        postgres_url,
        capacity_guard_template_database,
    )
    try:
        yield harness
    finally:
        await harness.aclose()
