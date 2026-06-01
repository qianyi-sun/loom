from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module as default_import_module
from importlib.metadata import PackageNotFoundError, version as default_package_version
from typing import Any, Callable


EXPECTED_TRIAL_EVENTS = ("START", "ENVIRONMENT_START", "AGENT_START", "VERIFICATION_START", "END", "CANCEL")


@dataclass(frozen=True)
class HarborNativeCapabilityReport:
    package_version: str | None
    native_runner_available: bool
    missing_symbols: tuple[str, ...]
    trial_events: tuple[str, ...]


def probe_harbor_native_capabilities(
    *,
    import_module: Callable[[str], Any] = default_import_module,
    package_version: Callable[[str], str] = default_package_version,
) -> HarborNativeCapabilityReport:
    version = _version(package_version)
    missing_symbols: list[str] = []

    job_module = _module_or_none("harbor.job", import_module=import_module, missing_symbols=missing_symbols)
    cli_jobs_module = _module_or_none("harbor.cli.jobs", import_module=import_module, missing_symbols=missing_symbols)

    for symbol in ("Job", "JobConfig", "DatasetConfig", "TaskConfig", "TrialHookEvent", "JobResult"):
        _require_symbol(job_module, "harbor.job", symbol, missing_symbols=missing_symbols)
    trial_event = _require_symbol(job_module, "harbor.job", "TrialEvent", missing_symbols=missing_symbols)
    for symbol in ("AgentConfig", "EnvironmentConfig"):
        _require_symbol(cli_jobs_module, "harbor.cli.jobs", symbol, missing_symbols=missing_symbols)

    trial_events = _trial_event_names(trial_event)
    if trial_event is not None:
        for event_name in EXPECTED_TRIAL_EVENTS:
            if event_name not in trial_events:
                missing_symbols.append(f"harbor.job.TrialEvent.{event_name}")

    return HarborNativeCapabilityReport(
        package_version=version,
        native_runner_available=not missing_symbols,
        missing_symbols=tuple(missing_symbols),
        trial_events=trial_events,
    )


def _version(package_version: Callable[[str], str]) -> str | None:
    try:
        return package_version("harbor")
    except PackageNotFoundError:
        return None


def _module_or_none(
    module_name: str,
    *,
    import_module: Callable[[str], Any],
    missing_symbols: list[str],
) -> Any | None:
    try:
        return import_module(module_name)
    except ImportError:
        missing_symbols.append(module_name)
        return None


def _require_symbol(
    module: Any | None,
    module_name: str,
    symbol_name: str,
    *,
    missing_symbols: list[str],
) -> Any | None:
    if module is None or not hasattr(module, symbol_name):
        missing_symbols.append(f"{module_name}.{symbol_name}")
        return None
    return getattr(module, symbol_name)


def _trial_event_names(trial_event: Any | None) -> tuple[str, ...]:
    if trial_event is None:
        return ()
    members = getattr(trial_event, "__members__", None)
    if isinstance(members, Mapping):
        return tuple(members.keys())
    return ()
