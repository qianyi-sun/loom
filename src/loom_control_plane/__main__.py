"""Entry point: `python -m loom_control_plane`."""

from __future__ import annotations

import uvicorn

from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


def main() -> None:
    settings = ControlPlaneSettings()
    uvicorn.run(
        create_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
