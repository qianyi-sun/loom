"""Process-wide registry of live Daytona sandboxes + signal/exit cleanup.

Why: a SIGINT during a trial can leave Daytona sandboxes running on the
user's account, costing money until the auto-stop interval triggers
(default 30 min). The DaytonaDriver registers each successful start()
here; stop() unregisters. The atexit + SIGINT handlers walk the
registry and synchronously call AsyncDaytona.delete().

Total cleanup is budgeted (default 30 s) so a SIGINT that just wants to
exit doesn't hang on the network.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import logging
import signal
import threading
from dataclasses import dataclass
from types import FrameType
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class _Entry:
    sdk: Any
    sandbox: Any


class LiveSandboxRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}

    def register(self, sdk: Any, sandbox: Any) -> None:
        with self._lock:
            self._entries[sandbox.id] = _Entry(sdk=sdk, sandbox=sandbox)

    def unregister(self, sandbox: Any) -> None:
        with self._lock:
            self._entries.pop(sandbox.id, None)

    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    async def cleanup(self, *, budget_sec: float = 30.0) -> int:
        """Delete every registered sandbox, bounded by budget_sec.

        Drains in a loop until empty or budget exhausted — a register()
        racing the snapshot path (e.g. a sandbox started just after a
        SIGINT) lands in _entries and gets caught on the next pass
        instead of leaking until process exit.

        Per-entry failures are logged but leave the entry in the
        registry so a subsequent cleanup pass (atexit -> SIGINT
        cascade, or a follow-up explicit call) retries it.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_sec
        deleted = 0
        attempted_ids: set[str] = set()
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            with self._lock:
                pending = [
                    e for e in self._entries.values()
                    if e.sandbox.id not in attempted_ids
                ]
            if not pending:
                break
            for entry in pending:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                attempted_ids.add(entry.sandbox.id)
                try:
                    await asyncio.wait_for(
                        entry.sdk.delete(
                            entry.sandbox, timeout=min(remaining, 5.0),
                        ),
                        timeout=remaining,
                    )
                    deleted += 1
                    with self._lock:
                        self._entries.pop(entry.sandbox.id, None)
                except Exception:
                    logger.warning(
                        "registry cleanup: delete failed for %s "
                        "(leaving in registry for retry)",
                        entry.sandbox.id, exc_info=True,
                    )
        return deleted


_PROCESS_REGISTRY: LiveSandboxRegistry | None = None
_HANDLERS_INSTALLED: bool = False


def get_process_registry() -> LiveSandboxRegistry:
    global _PROCESS_REGISTRY
    if _PROCESS_REGISTRY is None:
        _PROCESS_REGISTRY = LiveSandboxRegistry()
        _install_handlers(_PROCESS_REGISTRY)
    return _PROCESS_REGISTRY


def run_cleanup_sync(
    registry: LiveSandboxRegistry, *, budget_sec: float = 30.0,
) -> int:
    """Drive cleanup() to completion in a fresh event loop. Used by atexit
    where no loop is running."""
    try:
        return asyncio.run(registry.cleanup(budget_sec=budget_sec))
    except RuntimeError:
        # A loop was already running (e.g. tested inside pytest-asyncio).
        # Use a new thread to spin up an independent loop.
        result = {"deleted": 0}

        def _run() -> None:
            result["deleted"] = asyncio.run(
                registry.cleanup(budget_sec=budget_sec),
            )

        th = threading.Thread(target=_run, daemon=True)
        th.start()
        th.join(timeout=budget_sec + 5.0)
        return int(result["deleted"])


def _install_handlers(registry: LiveSandboxRegistry) -> None:
    global _HANDLERS_INSTALLED
    if _HANDLERS_INSTALLED:
        return
    _HANDLERS_INSTALLED = True

    def _atexit_handler() -> None:
        if registry.count() == 0:
            return
        logger.info(
            "daytona registry: deleting %d live sandbox(es) at exit",
            registry.count(),
        )
        run_cleanup_sync(registry, budget_sec=30.0)

    atexit.register(_atexit_handler)

    previous = signal.getsignal(signal.SIGINT)

    def _sigint_handler(signum: int, frame: FrameType | None) -> None:
        # CAVEAT: Python's signal docs warn that handlers should do
        # minimal work; run_cleanup_sync allocates + asyncio.run-s a
        # fresh loop or spawns a thread, which is technically not
        # async-signal-safe. In practice this works for the Ctrl-C
        # exit path (handlers run between bytecode ops and the main
        # thread is itself in asyncio.run already), but if a future
        # workload introduces signal-from-non-main-thread or holds
        # internal CPython locks across the handler boundary, prefer
        # the atexit path. Atexit is always called on normal exit;
        # the SIGINT branch is a best-effort fast path.
        logger.info(
            "daytona registry: SIGINT received; deleting %d live sandbox(es)",
            registry.count(),
        )
        with contextlib.suppress(Exception):
            run_cleanup_sync(registry, budget_sec=30.0)
        if callable(previous):
            previous(signum, frame)
        else:
            raise KeyboardInterrupt

    with contextlib.suppress(ValueError):
        signal.signal(signal.SIGINT, _sigint_handler)
