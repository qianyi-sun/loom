import threading
import time
from uuid import uuid4

from loom_worker.heartbeat import HeartbeatThread


def test_heartbeat_runs_in_dedicated_thread() -> None:
    worker_id = uuid4()
    ticks: list[float] = []
    stop_evt = threading.Event()
    main_thread_ident = threading.get_ident()
    tick_threads: list[int] = []

    def fake_tick() -> None:
        tick_threads.append(threading.get_ident())
        ticks.append(time.monotonic())
        if len(ticks) >= 3:
            stop_evt.set()

    hb = HeartbeatThread(
        worker_id=worker_id, interval_sec=0.02, tick_fn=fake_tick,
    )
    hb.start()
    assert stop_evt.wait(timeout=2.0)
    hb.stop()
    hb.join(timeout=2.0)
    assert not hb.is_alive()
    assert len(ticks) >= 3
    # All ticks ran on the heartbeat thread, NOT the main test thread.
    assert all(t != main_thread_ident for t in tick_threads)


def test_heartbeat_swallows_tick_exceptions() -> None:
    """One failing tick mustn't kill the thread — heartbeat must keep going
    so the worker can recover from transient Control Plane outages."""
    worker_id = uuid4()
    ticks: list[bool] = []
    stop_evt = threading.Event()

    def tick() -> None:
        ticks.append(True)
        if len(ticks) == 1:
            raise RuntimeError("transient network blip")
        if len(ticks) >= 3:
            stop_evt.set()

    hb = HeartbeatThread(
        worker_id=worker_id, interval_sec=0.02, tick_fn=tick,
    )
    hb.start()
    assert stop_evt.wait(timeout=2.0)
    hb.stop()
    hb.join(timeout=2.0)
    assert len(ticks) >= 3


def test_stop_unblocks_wait() -> None:
    """stop() should wake the thread promptly even mid-interval."""
    worker_id = uuid4()
    started = threading.Event()

    def tick() -> None:
        started.set()

    hb = HeartbeatThread(
        worker_id=worker_id, interval_sec=10.0, tick_fn=tick,
    )
    t0 = time.monotonic()
    hb.start()
    assert started.wait(timeout=1.0)
    hb.stop()
    hb.join(timeout=1.0)
    assert not hb.is_alive()
    assert time.monotonic() - t0 < 2.0
