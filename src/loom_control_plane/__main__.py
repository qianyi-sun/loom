"""Entry point: `python -m loom_control_plane`.

Dev-only: `LOOM_CP_DEV_RELOAD=1` switches uvicorn into `--reload` mode
watching `/app/src` so Python edits hot-restart the process. The dev
compose sets this; production leaves it off.
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


def _build_app() -> FastAPI:  # uvicorn reload-mode factory
    return create_app(ControlPlaneSettings())


def main() -> None:
    settings = ControlPlaneSettings()
    if settings.dev_reload:
        uvicorn.run(
            "loom_control_plane.__main__:_build_app",
            factory=True,
            reload=True,
            reload_dirs=["/app/src"],
            host=settings.bind_host,
            port=settings.bind_port,
            log_level=settings.log_level.lower(),
        )
    else:
        uvicorn.run(
            create_app(settings),
            host=settings.bind_host,
            port=settings.bind_port,
            log_level=settings.log_level.lower(),
        )


if __name__ == "__main__":
    main()
