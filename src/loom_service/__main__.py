"""`python -m loom_service` — uvicorn entrypoint.

Reads `LOOM_SVC_*` env vars, builds the app, and starts uvicorn on
`bind_host:bind_port` (default 0.0.0.0:8090). Production deploys use
this directly (the Dockerfile's CMD).
"""

from __future__ import annotations

import uvicorn

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def main() -> None:
    settings = LoomServiceSettings()  # type: ignore[call-arg]
    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
