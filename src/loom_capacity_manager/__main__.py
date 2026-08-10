"""Run the mutually authenticated capacity-manager service."""

from __future__ import annotations

from typing import Any, cast

import uvicorn

from loom_capacity_manager.api import create_app
from loom_capacity_manager.config import CapacityManagerSettings, build_uvicorn_kwargs


def main() -> None:
    settings = CapacityManagerSettings()
    uvicorn.run(
        create_app(settings),
        **cast(dict[str, Any], build_uvicorn_kwargs(settings)),
    )


if __name__ == "__main__":
    main()
