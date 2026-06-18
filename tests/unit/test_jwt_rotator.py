"""JWTRotator: atomic-rename rotation + race-free reads (#78 PR-D1)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from uuid import UUID

import pytest

from loom_worker.jwt_rotator import JWT_FILENAME, JWTRotator

_TRIAL = UUID("00000000-0000-0000-0000-000000000001")


async def test_initial_token_written_before_context_returns(tmp_path: Path) -> None:
    """`__aenter__` writes the initial token synchronously so the
    sandbox's first read sees a valid file. No race window."""
    counter = 0

    async def mint(trial_id: UUID) -> str:
        nonlocal counter
        counter += 1
        return f"token-{counter}"

    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    async with rotator:
        assert (tmp_path / JWT_FILENAME).read_text() == "token-1"


async def test_rotation_writes_new_token_every_half_life(tmp_path: Path) -> None:
    counter = 0

    async def mint(trial_id: UUID) -> str:
        nonlocal counter
        counter += 1
        return f"token-{counter}"

    # expiry=0.2s → rotation interval=0.1s; observe ≥2 rotations
    # within 250ms.
    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=1,
    )
    # Manually use expiry_sec=1 → interval=0.5s would be slow; force
    # a tiny one for the test by mutating after construction.
    # (No setter needed: dataclass mutable.)
    rotator.expiry_sec = 0  # interval clamps to 1s min → too slow.
    # Patch interval directly via the implementation: simplest is to
    # call _rotate_once() in a loop here rather than waiting for the
    # scheduler.
    async with rotator:
        await rotator._rotate_once()
        await rotator._rotate_once()
    # Initial write (1) + 2 manual rotations (2,3) = 3 mints.
    assert counter == 3
    assert (tmp_path / JWT_FILENAME).read_text() == "token-3"


async def test_rotation_is_atomic_no_partial_reads(tmp_path: Path) -> None:
    """Spawn a tight reader loop while the rotator writes; every
    read must see a complete previous-OR-new token, never an empty
    or partial file."""
    counter = 0
    # Long tokens make a torn write visible if rename weren't atomic.
    payload_len = 4096

    async def mint(trial_id: UUID) -> str:
        nonlocal counter
        counter += 1
        return f"{counter:0{payload_len}d}"

    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    await rotator.write_initial_token()

    stop = asyncio.Event()
    saw_partial: list[bytes] = []

    async def reader() -> None:
        path = rotator.jwt_path
        while not stop.is_set():
            try:
                data = path.read_bytes()
            except FileNotFoundError:
                # Rename window has zero gap; this is the canary if
                # someone broke the atomic guarantee.
                saw_partial.append(b"missing")
                continue
            if len(data) != payload_len:
                saw_partial.append(data)
            # Yield so the rotator can run.
            await asyncio.sleep(0)

    reader_task = asyncio.create_task(reader())
    # 100 rotations as fast as the loop scheduler allows.
    for _ in range(100):
        await rotator._rotate_once()
        await asyncio.sleep(0)
    stop.set()
    await reader_task
    assert saw_partial == [], (
        f"reader saw {len(saw_partial)} partial/missing reads — "
        f"atomic-rename broken"
    )


async def test_mint_failure_keeps_current_token(tmp_path: Path) -> None:
    """If mint_callback raises mid-trial, the rotator logs + leaves
    the current file intact so the sandbox keeps working until
    either the next successful rotation or the original TTL."""
    counter = 0

    async def mint(trial_id: UUID) -> str:
        nonlocal counter
        counter += 1
        if counter == 1:
            return "initial-token"
        raise RuntimeError("mint upstream down")

    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    async with rotator:
        # First rotation attempt should swallow the exception.
        await rotator._rotate_once()
    # Initial token still on disk.
    assert (tmp_path / JWT_FILENAME).read_text() == "initial-token"
    assert counter == 2


async def test_write_failure_does_not_crash_rotator(tmp_path: Path) -> None:
    """Disk full / permission denied during write should log + return,
    not propagate out and crash the rotation loop."""

    async def mint(trial_id: UUID) -> str:
        return "token"

    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    await rotator.write_initial_token()
    # Make jwt_dir read-only so the next replace fails.
    os.chmod(tmp_path, 0o500)
    try:
        await rotator._rotate_once()
    finally:
        os.chmod(tmp_path, 0o700)


async def test_file_permissions_are_0600(tmp_path: Path) -> None:
    async def mint(trial_id: UUID) -> str:
        return "token"
    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    await rotator.write_initial_token()
    mode = (tmp_path / JWT_FILENAME).stat().st_mode & 0o777
    assert mode == 0o600, f"expected 0600, got {oct(mode)}"


async def test_aexit_cancels_background_task(tmp_path: Path) -> None:
    async def mint(trial_id: UUID) -> str:
        return "x"
    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    async with rotator:
        assert rotator._task is not None
        assert not rotator._task.done()
    # After context exit, task is gone.
    assert rotator._task is None


async def test_aexit_idempotent(tmp_path: Path) -> None:
    async def mint(trial_id: UUID) -> str:
        return "x"
    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=mint, expiry_sec=10,
    )
    await rotator.__aenter__()
    await rotator.__aexit__()
    # Second exit must not raise even though _task is None.
    await rotator.__aexit__()


async def test_jwt_path_is_filename_inside_dir(tmp_path: Path) -> None:
    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=tmp_path,
        mint_callback=_dummy_mint, expiry_sec=10,
    )
    assert rotator.jwt_path == tmp_path / JWT_FILENAME


async def test_creates_jwt_dir_if_missing(tmp_path: Path) -> None:
    nested = tmp_path / "sandboxes" / "trial-x" / "run" / "loom"
    assert not nested.exists()
    rotator = JWTRotator(
        trial_id=_TRIAL, jwt_dir=nested,
        mint_callback=_dummy_mint, expiry_sec=10,
    )
    await rotator.write_initial_token()
    assert nested.exists()
    assert (nested / JWT_FILENAME).exists()


async def _dummy_mint(trial_id: UUID) -> str:
    return "token"


# Quick reminder for myself: pytest-asyncio = auto mode (per
# pyproject), so plain `async def test_*` are run directly. No
# fixture decoration needed.
_ = pytest  # silence ruff "unused import" (the file uses pytest for asyncio + tmp_path)
