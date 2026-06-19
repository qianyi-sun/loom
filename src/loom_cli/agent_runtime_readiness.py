"""Agent runtime readiness audit helpers.

The service catalog declares what each displayed agent requires. This module
checks those requirements against a concrete trial sandbox image, which is the
execution boundary used by SubprocessAgent.
"""

from __future__ import annotations

import json
import shlex
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import Literal

from loom_service.agent_catalog import AgentEntry, list_agents

RuntimeCheckKind = Literal["executable", "python_module"]
DependencyState = Literal["not_required", "satisfied", "missing"]
RuntimeReadinessState = Literal["ready", "blocked", "gated"]


@dataclass(frozen=True)
class RuntimeCheck:
    kind: RuntimeCheckKind
    name: str


@dataclass(frozen=True)
class AgentRuntimeAuditItem:
    image: str
    name: str
    kind: str
    catalog_ready: bool
    dependency_state: DependencyState
    readiness_state: RuntimeReadinessState
    blocker_reason: str | None
    required_executables: list[str]
    required_python_modules: list[str]
    required_packages: list[str]
    missing_executables: list[str]
    missing_python_modules: list[str]


RuntimeCheckRunner = Callable[[RuntimeCheck], bool]


def _python_module_script(module: str) -> str:
    module_literal = json.dumps(module)
    return (
        "py=$(command -v python3 || command -v python || true); "
        'if [ -z "$py" ]; then echo python-not-found >&2; exit 127; fi; '
        f"$py -c 'import importlib; importlib.import_module({module_literal})'"
    )


class DockerRuntimeCheckRunner:
    def __init__(self, *, image: str, timeout_sec: float = 20.0) -> None:
        self.image = image
        self.timeout_sec = timeout_sec

    def __call__(self, check: RuntimeCheck) -> bool:
        if check.kind == "executable":
            script = f"command -v {shlex.quote(check.name)} >/dev/null"
        else:
            script = _python_module_script(check.name)

        try:
            result = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--pull=never",
                    "--network",
                    "none",
                    self.image,
                    "sh",
                    "-lc",
                    script,
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_sec,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
        return result.returncode == 0


def _dependency_state(
    *,
    required_executables: list[str],
    required_modules: list[str],
    missing_executables: list[str],
    missing_modules: list[str],
) -> DependencyState:
    if not required_executables and not required_modules:
        return "not_required"
    if missing_executables or missing_modules:
        return "missing"
    return "satisfied"


def _readiness_state(
    *,
    agent: AgentEntry,
    dependency_state: DependencyState,
) -> tuple[RuntimeReadinessState, str | None]:
    if dependency_state == "missing":
        return "blocked", "missing_runtime_dependency"
    if agent.service_mode_ready:
        return "ready", None
    return "gated", "catalog_not_enabled"


def _audit_agent(
    *,
    image: str,
    agent: AgentEntry,
    check_runner: RuntimeCheckRunner,
) -> AgentRuntimeAuditItem:
    contract = agent.runtime_contract
    required_executables = list(contract.required_executables)
    required_modules = list(contract.required_python_modules)
    missing_executables = [
        executable
        for executable in required_executables
        if not check_runner(RuntimeCheck("executable", executable))
    ]
    missing_modules = [
        module
        for module in required_modules
        if not check_runner(RuntimeCheck("python_module", module))
    ]
    dep_state = _dependency_state(
        required_executables=required_executables,
        required_modules=required_modules,
        missing_executables=missing_executables,
        missing_modules=missing_modules,
    )
    readiness_state, blocker_reason = _readiness_state(
        agent=agent,
        dependency_state=dep_state,
    )
    return AgentRuntimeAuditItem(
        image=image,
        name=agent.name,
        kind=agent.kind,
        catalog_ready=agent.service_mode_ready,
        dependency_state=dep_state,
        readiness_state=readiness_state,
        blocker_reason=blocker_reason,
        required_executables=required_executables,
        required_python_modules=required_modules,
        required_packages=list(contract.required_packages),
        missing_executables=missing_executables,
        missing_python_modules=missing_modules,
    )


def build_runtime_audit_items(
    *,
    image: str,
    agents: Sequence[str] | None = None,
    check_runner: RuntimeCheckRunner | None = None,
    timeout_sec: float = 20.0,
) -> list[AgentRuntimeAuditItem]:
    catalog = list_agents()
    if agents:
        requested = set(agents)
        known = {agent.name for agent in catalog}
        unknown = sorted(requested - known)
        if unknown:
            raise ValueError(f"unknown agent(s): {', '.join(unknown)}")
        catalog = [agent for agent in catalog if agent.name in requested]

    runner = check_runner or DockerRuntimeCheckRunner(
        image=image,
        timeout_sec=timeout_sec,
    )
    return [_audit_agent(image=image, agent=agent, check_runner=runner) for agent in catalog]


def render_runtime_audit_json(items: list[AgentRuntimeAuditItem]) -> str:
    image = items[0].image if items else None
    return json.dumps(
        {
            "image": image,
            "count": len(items),
            "items": [asdict(item) for item in items],
        },
        indent=2,
        sort_keys=True,
    )


def render_runtime_audit_table(items: list[AgentRuntimeAuditItem]) -> str:
    name_w = max(7, max((len(item.name) for item in items), default=0))
    state_w = max(5, max((len(item.readiness_state) for item in items), default=0))
    dep_w = max(12, max((len(item.dependency_state) for item in items), default=0))
    blocker_w = max(
        7,
        max((len(item.blocker_reason or "-") for item in items), default=0),
    )
    rows = [
        f"{'AGENT':<{name_w}} {'STATE':<{state_w}} "
        f"{'DEPS':<{dep_w}} {'MISSING':<24} {'BLOCKER':<{blocker_w}}"
    ]
    for item in items:
        missing = (
            ",".join(
                [
                    *item.missing_executables,
                    *item.missing_python_modules,
                ]
            )
            or "-"
        )
        rows.append(
            f"{item.name:<{name_w}} {item.readiness_state:<{state_w}} "
            f"{item.dependency_state:<{dep_w}} {missing:<24} "
            f"{item.blocker_reason or '-':<{blocker_w}}"
        )
    return "\n".join(rows)
