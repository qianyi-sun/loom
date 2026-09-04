"""Run the mutually authenticated task-image authority service."""

from __future__ import annotations

from typing import Any, cast

import uvicorn

from loom_task_image_authority.api import create_app
from loom_task_image_authority.config import (
    TaskImageAuthoritySettings,
    build_uvicorn_kwargs,
)


def main() -> None:
    settings = TaskImageAuthoritySettings()
    uvicorn.run(
        create_app(settings),
        **cast(dict[str, Any], build_uvicorn_kwargs(settings)),
    )


if __name__ == "__main__":
    main()


__all__ = ["main"]
