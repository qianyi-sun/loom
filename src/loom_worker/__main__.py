"""Entry point: `python -m loom_worker`."""

from __future__ import annotations

import asyncio
import logging

from loom_worker.config import WorkerSettings
from loom_worker.main_loop import run_worker


def main() -> None:
    settings = WorkerSettings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper()))
    asyncio.run(run_worker(settings))


if __name__ == "__main__":
    main()
