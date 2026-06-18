"""Step-JWT rotation for sandbox isolation (#78 Phase D — PR-D1).

A long-running trial may outlive any single step-JWT's TTL. Rather
than fail the trial when the token expires mid-step, the worker
rotates the file the sandbox reads on each call:

1. Worker prepares a per-trial dir `/var/lib/loom/sandboxes/<trial>/run/loom/`.
2. Bind-mounts it into the container at `/run/loom/` via
   `StartOptions.volumes` (Phase D extension).
3. Drops the loom-CA cert and an initial JWT there.
4. Every `expiry_sec/2`, mints a fresh JWT, writes
   `step-jwt.tmp`, then `os.replace(tmp, step-jwt)`. The replace is
   atomic on POSIX so any concurrent reader sees either the old or
   the new contents — never a partial file.
5. The sandbox SDK re-reads `/run/loom/step-jwt` per call. No fsnotify
   watcher, no signal handling — the OS page cache makes a per-call
   open+read cheap, and the always-fresh read closes the obvious
   "started reading the file, rotator overwrote it" race that any
   read-once-and-cache approach would have.

Failure modes:
- mint_callback raises → log + skip this rotation (the next tick
  retries). The current JWT is still valid because we only rotated
  to the half-life mark, so the trial keeps working until the next
  successful rotation OR until the original TTL expires.
- atomic-rename fails → log + skip (operator should check disk
  space + permissions on the bind-mount source).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

logger = logging.getLogger(__name__)

# Mint callback signature. Returns the raw token string (without
# any prefix — the rotator writes the bytes verbatim).
MintCallback = Callable[[UUID], Awaitable[str]]

# The file inside `jwt_dir` the sandbox reads on each call. Stable
# name so the agent SDK doesn't need to know the rotation cadence.
JWT_FILENAME = "step-jwt"


@dataclass
class JWTRotator:
    """Background rotator that refreshes `<jwt_dir>/step-jwt` every
    `expiry_sec/2`.

    Use as `async with JWTRotator(...) as r:` — `__aenter__` writes
    the initial token (so the sandbox can read immediately) and
    spawns the rotation task; `__aexit__` cancels the task and
    awaits cleanly. The token file itself is left on disk for the
    caller (worker) to clean up alongside the bind-mount source dir.
    """

    trial_id: UUID
    jwt_dir: Path
    mint_callback: MintCallback
    expiry_sec: int
    _task: asyncio.Task[None] | None = None

    @property
    def jwt_path(self) -> Path:
        return self.jwt_dir / JWT_FILENAME

    @property
    def _tmp_path(self) -> Path:
        # `.tmp` suffix on the SAME directory so `os.replace` is
        # atomic (renames across filesystems are not).
        return self.jwt_dir / f"{JWT_FILENAME}.tmp"

    async def write_initial_token(self) -> None:
        """Mint + write once, synchronously enough that the sandbox's
        first read sees a valid token. Raises on mint failure — a
        trial that can't get an initial JWT is unrunnable, no point
        deferring."""
        self.jwt_dir.mkdir(parents=True, exist_ok=True)
        token = await self.mint_callback(self.trial_id)
        await self._write_atomic(token)

    async def _rotate_once(self) -> None:
        """Mint + atomic-write one rotation cycle. Failures log + return;
        the next tick retries."""
        try:
            token = await self.mint_callback(self.trial_id)
        except Exception:
            logger.exception(
                "jwt_rotator_mint_failed trial=%s — keeping current token",
                self.trial_id,
            )
            return
        try:
            await self._write_atomic(token)
        except OSError:
            logger.exception(
                "jwt_rotator_write_failed trial=%s path=%s",
                self.trial_id, self.jwt_path,
            )

    async def _write_atomic(self, token: str) -> None:
        """Write `token` to `<jwt_dir>/step-jwt.tmp` then atomically
        replace `step-jwt`. `os.replace` is POSIX-atomic on the same
        filesystem; the temp suffix MUST stay on the same dir for
        that guarantee to hold."""
        tmp = self._tmp_path
        # `bytes` mode so we don't accidentally write a stray
        # trailing newline via text-mode os.write semantics.
        token_bytes = token.encode("utf-8")
        # Mode 0o600: the bind-mount source is host-side; the file
        # should not be world-readable even though docker
        # bind-mounts ignore mode bits inside the container.
        # `aiofile`-grade async IO is overkill for a ≤1KB write
        # every ~minutes; the to_thread bound is fine.
        await asyncio.to_thread(self._write_sync, tmp, token_bytes)
        await asyncio.to_thread(os.replace, tmp, self.jwt_path)

    @staticmethod
    def _write_sync(path: Path, data: bytes) -> None:
        # Open with O_CREAT|O_WRONLY|O_TRUNC + 0o600. Don't use
        # path.write_bytes (it doesn't set permissions atomically).
        fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)

    async def _run(self) -> None:
        """Periodic rotation loop. Cancels via task.cancel(); the
        next iteration's `asyncio.sleep` raises CancelledError and
        the loop exits cleanly."""
        # Half-life rotation: each iteration sleeps half the TTL so
        # the file is always within the second half of its
        # validity — never expired when read.
        interval = max(1.0, self.expiry_sec / 2.0)
        try:
            while True:
                await asyncio.sleep(interval)
                await self._rotate_once()
        except asyncio.CancelledError:
            raise

    async def __aenter__(self) -> JWTRotator:
        await self.write_initial_token()
        self._task = asyncio.create_task(self._run())
        logger.info(
            "jwt_rotator_started trial=%s expiry_sec=%d interval_sec=%.1f",
            self.trial_id, self.expiry_sec, self.expiry_sec / 2.0,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except (Exception, asyncio.CancelledError):
            pass
        self._task = None
        logger.info("jwt_rotator_stopped trial=%s", self.trial_id)
