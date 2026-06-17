"""Entry point: `python -m loom_worker`."""

from __future__ import annotations

import asyncio
import logging

from prometheus_client import start_http_server

from loom_worker.config import WorkerSettings
from loom_worker.main_loop import run_worker


def main() -> None:
    settings = WorkerSettings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper()))
    # /metrics for prometheus scraping (#81 slice B-4). Worker is a
    # long-running asyncio process — no FastAPI app to mount an ASGI
    # sub-app on — so we use prometheus_client's threaded HTTP server.
    # Listens on `metrics_port` (default 9090, configurable via
    # LOOM_WORKER_METRICS_PORT).
    start_http_server(settings.metrics_port)
    asyncio.run(run_worker(settings))


if __name__ == "__main__":
    main()
