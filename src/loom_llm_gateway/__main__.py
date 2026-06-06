"""Entry point: `python -m loom_llm_gateway`."""

from __future__ import annotations

import uvicorn

from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings


def main() -> None:
    settings = GatewaySettings()
    uvicorn.run(
        create_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
