import asyncio

from loom_worker.runner_pool import RunnerPool


async def test_pool_respects_max_concurrent() -> None:
    pool = RunnerPool(max_concurrent=2)
    in_flight: list[int] = []
    max_seen = 0

    async def slow(idx: int) -> None:
        nonlocal max_seen
        in_flight.append(idx)
        max_seen = max(max_seen, len(in_flight))
        await asyncio.sleep(0.02)
        in_flight.remove(idx)

    for i in range(5):
        await pool.spawn(slow(i))
    await pool.wait_all()
    assert max_seen <= 2
    assert pool.in_flight == 0


async def test_in_flight_count_during_execution() -> None:
    pool = RunnerPool(max_concurrent=3)
    gate = asyncio.Event()

    async def held() -> None:
        await gate.wait()

    for _ in range(3):
        await pool.spawn(held())
    # Give the event loop a tick so tasks register
    await asyncio.sleep(0)
    assert pool.in_flight == 3
    gate.set()
    await pool.wait_all()
    assert pool.in_flight == 0


async def test_cancel_all() -> None:
    pool = RunnerPool(max_concurrent=2)

    async def forever() -> None:
        await asyncio.sleep(10.0)

    for _ in range(2):
        await pool.spawn(forever())
    await asyncio.sleep(0)
    pool.cancel_all()
    await pool.wait_all(timeout=1.0)
    assert pool.in_flight == 0
