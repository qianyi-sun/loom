"""SIGTERM handler — sets a shared `shutting_down` flag (spec §3.2)."""

from __future__ import annotations

import asyncio
import logging
import signal
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ShutdownState:
    shutting_down: bool = False


def install_signal_handlers(state: ShutdownState) -> None:
    loop = asyncio.get_event_loop()

    def _on_sig(signo: int) -> None:
        logger.info("shutdown_signal_received signo=%s", signo)
        state.shutting_down = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_sig, sig)
        except NotImplementedError:
            # Windows fallback (Loom is Linux-only in v1 — handle gracefully).
            signal.signal(sig, lambda s, _f: _on_sig(s))
