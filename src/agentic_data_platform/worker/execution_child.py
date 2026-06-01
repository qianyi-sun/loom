from __future__ import annotations

import argparse
import json

from agentic_data_platform.persistence import create_database_engine
from agentic_data_platform.service.config import load_service_settings
from agentic_data_platform.worker.service import build_configured_executor, execute_claimed_run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute one claimed platform run in an isolated subprocess.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--request-id", default=None)
    args = parser.parse_args(argv)

    settings = load_service_settings()
    if not settings.database_url:
        raise ValueError("DATABASE_URL is required for worker execution child")
    engine = create_database_engine(settings.database_url, pool_pre_ping=True)
    try:
        result = execute_claimed_run(
            engine=engine,
            worker_id=args.worker_id,
            run_id=args.run_id,
            request_id=args.request_id,
            executor=build_configured_executor(settings),
        )
        print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
