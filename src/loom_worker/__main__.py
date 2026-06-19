"""Entry point: `python -m loom_worker`."""

from __future__ import annotations

import asyncio
import logging

from prometheus_client import start_http_server

from loom_worker.config import WorkerSettings
from loom_worker.main_loop import run_worker

_S3_SDK_LOGGER_NAMES = (
    "boto3",
    "botocore",
    "botocore.auth",
    "botocore.endpoint",
    "s3transfer",
    "urllib3",
)


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    for logger_name in _S3_SDK_LOGGER_NAMES:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def main() -> None:
    settings = WorkerSettings()
    _configure_logging(settings.log_level)
    # /metrics for prometheus scraping (#81 slice B-4). Worker is a
    # long-running asyncio process — no FastAPI app to mount an ASGI
    # sub-app on — so we use prometheus_client's threaded HTTP server.
    # Listens on `metrics_port` (default 9090, configurable via
    # LOOM_WORKER_METRICS_PORT).
    start_http_server(settings.metrics_port)
    asyncio.run(run_worker(settings))


if __name__ == "__main__":
    main()
