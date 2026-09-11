"""Worker-side native startup settings; native claim/start admission stays closed.

The fixed launcher supplies settings through the same bounded one-use stdin as
the scoped credential and public root. This module never loads dotenv, external
secret files or environment fallbacks. The ordinary entrypoint is unchanged.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Sequence

from prometheus_client import start_http_server
from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from loom_capacity_executor.launch_renderer import NativeTaskImageExecutionV2
from loom_capacity_executor.native_worker_bootstrap import (
    NativeBootstrapError,
    NativeWorkerBootstrap,
    consume_native_worker_bootstrap,
)
from loom_worker.__main__ import _configure_logging
from loom_worker.config import WorkerSettings
from loom_worker.main_loop import run_worker


class NativeWorkerSettings(WorkerSettings):
    model_config = SettingsConfigDict(env_file=None)
    native_execution: NativeTaskImageExecutionV2 = Field(exclude=True, repr=False, frozen=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


def native_worker_settings(bootstrap: NativeWorkerBootstrap) -> NativeWorkerSettings:
    """Construct only from approved handoff bytes; never echo invalid settings."""

    try:
        return NativeWorkerSettings(
            native_execution=bootstrap.native_execution,
            LOOM_EXECUTOR_WORKER_CREDENTIAL=bootstrap.worker_credential,
            **bootstrap.worker_settings(),
        )
    except (ValueError, TypeError):
        raise NativeBootstrapError("native worker bootstrap unavailable or malformed") from None


def main(argv: Sequence[str] | None = None) -> int:
    """Start the existing worker only after accepting the memory-only handoff.

    No native capability is advertised here. Current selectors remain V1-only;
    the protected native claim/start adapter and owner projection must be wired
    together before any native readiness is eligible for this worker.
    """

    try:
        if tuple(sys.argv[1:] if argv is None else argv):
            raise NativeBootstrapError("native startup accepts no argument overrides")
        bootstrap = consume_native_worker_bootstrap()
        settings = native_worker_settings(bootstrap)
    except NativeBootstrapError:
        print("native worker bootstrap unavailable or malformed", file=sys.stderr)
        return 65
    _configure_logging(settings.log_level)
    start_http_server(settings.metrics_port)
    asyncio.run(run_worker(settings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
