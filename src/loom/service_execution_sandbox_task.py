"""Trusted phase entry points for Harbor against private native Pod sandboxes.

Only the controller sees the immutable task bundle and durable outputs. The
agent task and fresh verifier receive their inputs over different Unix sockets.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import sys
import tomllib
from pathlib import Path, PurePosixPath
from uuid import UUID

import httpx

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import AgentError, DriverError
from loom.models.capabilities import Capabilities
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.models.verifier import VerifierResult
from loom.service_execution_task import (
    ServiceExecutionTaskError,
    _safe_workspace_path,
    _write_json_atomic,
)
from loom.service_execution_terminus2 import TASK_IMAGE_TOOLS_REQUIRED, run_terminus2
from loom.service_execution_terminus_trace import parse_terminus_events, terminus_usage
from loom.trial.workspace import WorkspaceStagingPolicy, materialize_workspace
from loom.trial.workspace_snapshot import (
    _export_workspace_archive,
    _import_workspace_archive,
    _strip_private_entries,
    _validate_workspace_archive,
)

_PRIVATE_PATHS = ("tests/**", "verifier/**", "solution/**", "upstream-task.toml", ".loom/**")
_POLICY = WorkspaceStagingPolicy(_PRIVATE_PATHS, _PRIVATE_PATHS, ())


def sandbox_driver(role: str, task: TaskConfig) -> ServiceSandboxDriver:
    return ServiceSandboxDriver(
        Path(f"/loom/sandboxes/{role}/sandbox.sock"),
        capabilities=Capabilities(
            os="linux", cpu_arch="x86_64", gpu_vendor="none",
            network_policies=frozenset({"gateway-only"}), dynamic_network_policy=False,
            mounted_fs=False, resource_modes=frozenset({"limit"}),
        ),
        network_policy=task.environment.baseline_network_policy,
    )


async def _execution_identity(gateway: str) -> tuple[UUID, UUID]:
    # No caller-selected identity. Go authenticates this request as its own Pod.
    from urllib.parse import urlsplit

    url = urlsplit(gateway)
    if url.scheme != "http" or url.hostname not in {"127.0.0.1", "::1"}:
        raise ServiceExecutionTaskError("execution requires a loopback Gateway")
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        async with client.stream("GET", gateway.rstrip("/") + "/internal/loom/llm-calls") as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 16 * 1024 * 1024:
                    raise ServiceExecutionTaskError("execution ledger exceeds limit")
    envelope = json.loads(body)
    if envelope.get("step_id") != "agent":
        raise ServiceExecutionTaskError("execution ledger has an invalid step")
    return UUID(envelope["trial_id"]), UUID(envelope["team_id"])


async def run_agent(workspace: Path, task: TaskConfig, trial: TrialConfig) -> None:
    gateway = os.environ["LOOM_GATEWAY_URL"]
    trial_id, team_id = await _execution_identity(gateway)
    driver = sandbox_driver("task-sandbox", task)
    await driver.start()
    output = workspace / ".loom/agent"
    output.mkdir(parents=True, exist_ok=True)
    try:
        await materialize_workspace(
            driver=driver, task_dir=workspace, dst=task.environment.workdir, policy=_POLICY,
            excluded_paths=(".loom/**",),
        )
        instruction = _safe_workspace_path(workspace, str(task.steps[0].instruction_file)).read_text()
        try:
            await run_terminus2(
                driver=driver, workspace=output, task_config=task, trial_config=trial,
                trial_id=trial_id, team_id=team_id, instruction=instruction, gateway_url=gateway,
            )
        finally:
            # tmux can outlive Harbor. Quiesce only this sandbox PID namespace
            # before reading files; it cannot reach the controller or verifier.
            await driver.stop_processes()
            trace = output / "trajectory.jsonl"
            if trace.exists():
                events = parse_terminus_events(trace.read_bytes(), trial=trial, trial_id=trial_id)
                _write_json_atomic(output / "usage.json", terminus_usage(events, trial))
            archive = workspace / ".loom/workspace.tar"
            await _export_workspace_archive(driver, task.environment.workdir, archive)
            await asyncio.to_thread(_strip_private_entries, archive, _POLICY)
            await asyncio.to_thread(_validate_workspace_archive, archive, _POLICY)
        paths = json.loads(os.environ["LOOM_TASK_ARTIFACTS_JSON"])
        for path in paths:
            destination = _safe_workspace_path(workspace / ".loom/collected", path)
            try:
                await driver.download(task.environment.workdir / path, destination)
            except (DriverError, FileNotFoundError):
                # Go checks required declarations after the verifier phase,
                # preserving reward/feedback even when a required file is absent.
                print(f"task artifact unavailable: {path}", file=sys.stderr)
    finally:
        await driver.stop()


async def run_verifier(workspace: Path, task: TaskConfig, trial: TrialConfig) -> None:
    driver = sandbox_driver("verifier-sandbox", task)
    await driver.start()
    try:
        await materialize_workspace(
            driver=driver, task_dir=workspace, dst=task.environment.workdir,
            policy=_POLICY, phase="verifier",
            excluded_paths=(".loom/**",),
        )
        archive = workspace / ".loom/workspace.tar"
        # The archive was validated by the agent phase before durable capture;
        # it stays in the private controller workspace between phases.
        await _import_workspace_archive(driver, archive, task.environment.workdir)
        remote_output = task.environment.workdir / ".loom/verifier/output.json"
        result = await driver.exec(
            "/bin/sh " + shlex.quote(str(task.verifier.args["script_path"])),
            cwd=task.environment.workdir,
            env={"LOOM_TASK_DIR": str(task.environment.workdir),
                 "LOOM_VERIFIER_OUTPUT": str(remote_output)},
            timeout_sec=(trial.override_verifier_timeout_sec or task.verifier.timeout_sec)
            * trial.verifier_timeout_multiplier,
        )
        sys.stdout.buffer.write(result.stdout)
        sys.stderr.buffer.write(result.stderr)
        await driver.stop_processes()
        if result.return_code != 0:
            raise ServiceExecutionTaskError("isolated verifier process failed")
        output = workspace / ".loom/verifier/output.json"
        await driver.download(remote_output, output)
        # Numeric zero is a valid evaluated result; missing/invalid feedback is not.
        VerifierResult.model_validate_json(output.read_bytes())
        try:
            await driver.download(PurePosixPath("/logs/verifier/ctrf.json"), output.with_name("ctrf.json"))
        except (DriverError, FileNotFoundError):
            # The script verifier contract requires structured rewards. CTRF
            # is an optional native report, not another acceptance gate.
            print("optional verifier CTRF report unavailable", file=sys.stderr)
    finally:
        try:
            await driver.stop_processes()
        finally:
            await driver.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("terminus-2", "verify-sandbox"))
    parser.add_argument("--workspace", required=True, type=Path)
    args = parser.parse_args()
    workspace = args.workspace
    if not workspace.is_absolute():
        raise ServiceExecutionTaskError("isolated execution requires an absolute workspace")
    # The interpreter runs from the trusted image directory. Some dependencies
    # add cwd to their import path or load .env; never chdir into task inputs.
    with (workspace / "task.toml").open("rb") as stream:
        task = normalize_steps(TaskConfig.model_validate(tomllib.load(stream)))
    trial = TrialConfig.model_validate_json(os.environ["LOOM_TASK_TRIAL_JSON"])
    phase = run_agent if args.phase == "terminus-2" else run_verifier
    asyncio.run(phase(workspace, task, trial))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # HTTP exceptions can embed response/request details; never persist them.
        message = f"isolated execution failed ({type(exc).__name__})"
        if isinstance(exc, AgentError) and exc.args == (TASK_IMAGE_TOOLS_REQUIRED,):
            message += f": {TASK_IMAGE_TOOLS_REQUIRED}"
        print(message, file=sys.stderr)
        raise SystemExit(1) from None
