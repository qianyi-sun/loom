"""Single-source runtime tool and Python import readiness checks."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

REQUIRED_EXECUTABLES = (
    "git",
    "docker",
    "kind",
    "kubectl",
    "ssh",
    "systemd-run",
    "systemctl",
    "journalctl",
)
REQUIRED_IMPORTS = (
    "boto3",
    "yaml",
    "loom_benchmark_tool.register_cmd",
    "loom_benchmarks.registry",
    "loom_benchmarks.adapters.skilllearnbench",
    "loom_benchmark_terminal_bench_2.adapter",
)

_REQUIREMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

ExecutableLookup = Callable[[str], str | None]
ModuleImporter = Callable[[str], object]


def _requirement_digest() -> str:
    payload = {
        "executables": list(REQUIRED_EXECUTABLES),
        "imports": list(REQUIRED_IMPORTS),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


RUNTIME_REQUIREMENT_DIGEST = _requirement_digest()


@dataclass(frozen=True, slots=True)
class RuntimeReadinessEvidence:
    """Bounded, non-diagnostic evidence for the fixed rollout runtime."""

    executables: Mapping[str, str]
    imports: Mapping[str, str]
    requirement_digest: str = RUNTIME_REQUIREMENT_DIGEST

    def __post_init__(self) -> None:
        executable_status = dict(self.executables)
        import_status = dict(self.imports)
        if tuple(executable_status) != REQUIRED_EXECUTABLES:
            raise ValueError("runtime executable evidence does not match the fixed requirements")
        if tuple(import_status) != REQUIRED_IMPORTS:
            raise ValueError("runtime import evidence does not match the fixed requirements")
        for requirement, status in (*executable_status.items(), *import_status.items()):
            if _REQUIREMENT_RE.fullmatch(requirement) is None or status not in {
                "available",
                "missing",
            }:
                raise ValueError("runtime readiness evidence is invalid")
        if self.requirement_digest != RUNTIME_REQUIREMENT_DIGEST:
            raise ValueError("runtime requirement digest does not match the fixed requirements")
        object.__setattr__(self, "executables", MappingProxyType(executable_status))
        object.__setattr__(self, "imports", MappingProxyType(import_status))

    @property
    def executables_ready(self) -> bool:
        return all(status == "available" for status in self.executables.values())

    @property
    def imports_ready(self) -> bool:
        return all(status == "available" for status in self.imports.values())

    @property
    def ready(self) -> bool:
        return self.executables_ready and self.imports_ready

    @property
    def evidence_digest(self) -> str:
        payload = {
            "executables": dict(self.executables),
            "imports": dict(self.imports),
            "requirement_digest": self.requirement_digest,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def probe_runtime_readiness(
    *,
    executable_lookup: ExecutableLookup,
    importer: ModuleImporter = importlib.import_module,
) -> RuntimeReadinessEvidence:
    """Probe every fixed requirement without exposing paths or import diagnostics."""
    executables: dict[str, str] = {}
    for name in REQUIRED_EXECUTABLES:
        try:
            available = executable_lookup(name) is not None
        except Exception:
            available = False
        executables[name] = "available" if available else "missing"

    imports: dict[str, str] = {}
    for module in REQUIRED_IMPORTS:
        try:
            importer(module)
        except Exception:
            available = False
        else:
            available = True
        imports[module] = "available" if available else "missing"

    return RuntimeReadinessEvidence(executables=executables, imports=imports)


__all__ = [
    "REQUIRED_EXECUTABLES",
    "REQUIRED_IMPORTS",
    "RUNTIME_REQUIREMENT_DIGEST",
    "ExecutableLookup",
    "ModuleImporter",
    "RuntimeReadinessEvidence",
    "probe_runtime_readiness",
]
