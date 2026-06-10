"""`python -m loom_service` — uvicorn entrypoint.

Reads `LOOM_SVC_*` env vars, builds the app, and starts uvicorn on
`bind_host:bind_port` (default 0.0.0.0:8090). Production deploys use
this directly (the Dockerfile's CMD).

Dev-only: `LOOM_SVC_DEV_RELOAD=1` switches uvicorn into `--reload` mode
watching `/app/src` so Python edits hot-restart the process. The dev
compose sets this; production leaves it off.
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _build_app() -> FastAPI:  # uvicorn reload-mode factory
    return create_app(LoomServiceSettings())


def main() -> None:
    settings = LoomServiceSettings()
    if settings.dev_reload:
        uvicorn.run(
            "loom_service.__main__:_build_app",
            factory=True,
            reload=True,
            reload_dirs=["/app/src"],
            host=settings.bind_host,
            port=settings.bind_port,
            log_level=settings.log_level,
        )
    else:
        uvicorn.run(
            create_app(settings),
            host=settings.bind_host,
            port=settings.bind_port,
            log_level=settings.log_level,
        )


if __name__ == "__main__":
    main()
