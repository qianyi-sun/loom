"""Run one complete read-only capacity collection and publish transaction."""

from __future__ import annotations

import asyncio
import json

from loom_execution_capacity_collector.collector import collect_capacity_observation
from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings


async def _run() -> None:
    settings = ExecutionCapacityCollectorSettings()
    receipt = await collect_capacity_observation(settings)
    print(json.dumps(receipt.model_dump(mode="json"), sort_keys=True, separators=(",", ":")))


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
