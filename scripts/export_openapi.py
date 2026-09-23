"""Export the service contract offline, without settings, lifespan or credentials."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from fastapi import FastAPI

from loom_service.app import register_api_routes


def export_openapi() -> str:
    app = FastAPI(title="Loom Service", version="0.0.1")
    # Include historical/local Pipeline routes in the wire contract without
    # enabling them on any hosted service or starting an execution backend.
    register_api_routes(app, include_local_execution=True)
    return json.dumps(app.openapi(), sort_keys=True, indent=2) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    content = export_openapi()
    if args.check:
        if not args.output.exists() or args.output.read_text() != content:
            raise SystemExit("OpenAPI contract differs; regenerate before committing")
    else:
        args.output.write_text(content)


if __name__ == "__main__":
    main()
