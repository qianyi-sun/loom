import asyncio
import os
import signal

from loom_worker.signal_handler import ShutdownState, install_signal_handlers


async def test_sigterm_sets_state() -> None:
    state = ShutdownState()
    install_signal_handlers(state)
    os.kill(os.getpid(), signal.SIGTERM)
    # Give the event loop several ticks to deliver the signal.
    for _ in range(10):
        if state.shutting_down:
            break
        await asyncio.sleep(0.01)
    assert state.shutting_down is True


async def test_sigint_sets_state() -> None:
    state = ShutdownState()
    install_signal_handlers(state)
    os.kill(os.getpid(), signal.SIGINT)
    for _ in range(10):
        if state.shutting_down:
            break
        await asyncio.sleep(0.01)
    assert state.shutting_down is True
