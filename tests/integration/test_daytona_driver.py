"""LIVE Daytona integration test.

Skipped unless BOTH of these are set:
  LOOM_RUN_DAYTONA_INTEGRATION=1
  DAYTONA_API_KEY=<a real key>

Creates a real sandbox, runs commands, applies network policies,
uploads/downloads, then deletes. Burns roughly $0.01 per run at
default rates. Do NOT enable this in default CI.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

import pytest

from loom.models.networking import Allowlist, NoNetwork
from loom_drivers.daytona.config import DaytonaConfig
from loom_drivers.daytona.driver import DaytonaDriver

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DAYTONA_INTEGRATION") != "1",
    reason="opt-in: set LOOM_RUN_DAYTONA_INTEGRATION=1 to run live Daytona",
)


@pytest.fixture(scope="module")
def cfg() -> DaytonaConfig:
    return DaytonaConfig.from_env()


async def test_full_lifecycle_round_trip(
    cfg: DaytonaConfig, tmp_path: Path,
) -> None:
    drv = DaytonaDriver(image="python:3.12-slim", config=cfg)
    await drv.start()
    try:
        r = await drv.exec("echo loom-was-here")
        assert r.return_code == 0
        assert b"loom-was-here" in r.stdout

        src = tmp_path / "payload.txt"
        src.write_bytes(b"upload-test")
        await drv.upload(src, PurePosixPath("/workspace/payload.txt"))
        dst = tmp_path / "round-trip.txt"
        await drv.download(PurePosixPath("/workspace/payload.txt"), dst)
        assert dst.read_bytes() == b"upload-test"
    finally:
        await drv.stop(delete=True)


async def test_no_network_policy_blocks_egress(cfg: DaytonaConfig) -> None:
    drv = DaytonaDriver(image="python:3.12-slim", config=cfg)
    await drv.start()
    try:
        r_pre = await drv.exec(
            "python -c 'import urllib.request; "
            "urllib.request.urlopen(\"https://1.1.1.1\", timeout=5); "
            "print(\"ok\")'",
            timeout_sec=15,
        )
        assert r_pre.return_code == 0

        await drv.set_network_policy(NoNetwork())

        r_post = await drv.exec(
            "python -c 'import urllib.request\n"
            "try: urllib.request.urlopen(\"https://1.1.1.1\", timeout=5); "
            "print(\"open\")\n"
            "except Exception: print(\"blocked\")'",
            timeout_sec=15,
        )
        assert b"blocked" in r_post.stdout
    finally:
        await drv.stop(delete=True)


async def test_allowlist_permits_specific_cidr_only(cfg: DaytonaConfig) -> None:
    drv = DaytonaDriver(image="python:3.12-slim", config=cfg)
    await drv.start()
    try:
        await drv.set_network_policy(
            Allowlist.model_construct(domains=(), cidrs=("1.1.1.1/32",)),
        )
        r_allowed = await drv.exec(
            "python -c 'import urllib.request; "
            "urllib.request.urlopen(\"https://1.1.1.1\", timeout=5); "
            "print(\"allowed\")'",
            timeout_sec=15,
        )
        assert b"allowed" in r_allowed.stdout

        r_blocked = await drv.exec(
            "python -c 'import urllib.request\n"
            "try: urllib.request.urlopen(\"https://8.8.8.8\", timeout=5); "
            "print(\"open\")\n"
            "except Exception: print(\"blocked\")'",
            timeout_sec=15,
        )
        assert b"blocked" in r_blocked.stdout
    finally:
        await drv.stop(delete=True)


async def test_orphan_cleanup_when_stop_is_skipped(cfg: DaytonaConfig) -> None:
    """Simulate cancel: start a driver, skip stop(), then run the
    registry cleanup and verify Daytona reports the sandbox as gone."""
    from daytona import AsyncDaytona, DaytonaError

    from loom_drivers.daytona.registry import get_process_registry

    drv = DaytonaDriver(image="python:3.12-slim", config=cfg)
    await drv.start()
    sandbox_id = drv._sandbox.id  # type: ignore[union-attr]
    deleted = await get_process_registry().cleanup(budget_sec=30.0)
    assert deleted >= 1
    async with AsyncDaytona(cfg.to_sdk_config()) as dt:
        with pytest.raises(DaytonaError):
            await dt.get(sandbox_id)
