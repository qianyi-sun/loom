"""Closed process specifications for Pipeline whole-episode judging."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Literal, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from loom.integrations.behavior.contracts import ProviderUsageV1
from loom.integrations.behavior.errors import (
    BehaviorContractError,
    BehaviorInfrastructureTransientError,
    BehaviorProviderTransientError,
)
from loom.integrations.behavior.provider import (
    OFFLINE_JUDGE_STEP_ID,
    PIPELINE_STEP_JWT_PATH,
    RotatingPipelineStepJwtReader,
)
from loom.integrations.behavior.stages.offline_judge import (
    OfflineJudgeRunRequest,
    OfflineJudgeRunResult,
)
from loom_worker.pipeline_codex import (
    CODEX_HOME,
    OFFICIAL_CODEX_VERSION,
    PipelineLockedHomeProcessSpec,
    build_pipeline_codex_process_spec,
    build_pipeline_locked_home_process_spec,
)

BASELINE_CODEX_PROFILE: Final = "codex"
SYNTHETIC_PROFILE: Final = "synthetic"
_SHIM = "/opt/behavior/bin/loom-codex-gateway-shim"
_SYNTHETIC_RUNNER = "/opt/behavior/bin/loom-synthetic-judge"
_SYNTHETIC_SHIM = "/opt/behavior/bin/loom-synthetic-gateway-shim"


class OfflineJudgeAuthError(ValueError):
    """A judge profile attempted to inherit or redirect credential state."""


@dataclass(frozen=True)
class OfflineJudgeProcessSpec:
    profile: Literal["codex", "synthetic"]
    agent_version: str
    runner_argv: tuple[str, ...]
    runner_env: MappingProxyType[str, str]
    shim_argv: tuple[str, ...]
    shim_env: MappingProxyType[str, str]
    token_reader: RotatingPipelineStepJwtReader
    new_process_group: bool = True


@dataclass(frozen=True)
class OfflineJudgeGatewayShim:
    """Replace runner-supplied authorization with the current step JWT."""

    gateway_responses_url: str
    attempt_id: UUID
    binding_sha256: str
    token_reader: RotatingPipelineStepJwtReader

    def __post_init__(self) -> None:
        _validate_responses_url(self.gateway_responses_url)

    def headers_for_request(self, incoming: Mapping[str, str]) -> MappingProxyType[str, str]:
        # Do not copy Authorization, x-api-key, cookies, or arbitrary profile
        # headers from the runner.  Only content negotiation is safe to retain.
        normalized = {name.lower(): value for name, value in incoming.items()}
        headers = {
            "Authorization": f"Bearer {self.token_reader.read_for_request()}",
            "content-type": normalized.get("content-type", "application/json"),
            "x-loom-control-binding-sha256": self.binding_sha256,
            "x-loom-execution-attempt-id": str(self.attempt_id),
        }
        if "accept" in normalized:
            headers["accept"] = normalized["accept"]
        return MappingProxyType(headers)


PassFailure = Literal["provider_429", "provider_5xx", "gateway_transport", "stage_helper_transient"]


@dataclass(frozen=True)
class LockedCodexPassSpec:
    process: PipelineLockedHomeProcessSpec
    argv: tuple[str, ...]
    stdin: bytes
    resume_session_id: UUID | None


@dataclass(frozen=True)
class LockedCodexPassResult:
    events_jsonl: bytes
    report: bytes | None
    seed: bytes | None
    returncode: int
    failure: PassFailure | None = None


class LockedCodexExecutor(Protocol):
    """Outer-container supervisor implemented by the worker runtime in #1363."""

    def verify_binary(self, path: str, sha256: str, *, version: str | None) -> None: ...

    def execute(self, spec: LockedCodexPassSpec) -> LockedCodexPassResult: ...

    def cleanup(
        self,
        *,
        paths: tuple[str, ...],
        term_grace_seconds: int,
        kill_after_grace: bool,
    ) -> None: ...


class GatewaySettlementReader(Protocol):
    """Read authoritative Attempt/provider totals, never child-reported usage."""

    def read(self, *, attempt_id: UUID, control_binding_sha256: str) -> ProviderUsageV1: ...


class LockedCodexOfflineJudgeRunner:
    """Closed initial/resume state machine for the official Codex profile.

    The executor owns host operations and the rotating-JWT loopback shim.  This
    class owns the immutable lock, one-session rule, at-most-one resume, stable
    failure classes, cleanup, and independent Gateway settlement readback.
    """

    def __init__(
        self,
        *,
        gateway_responses_url: str,
        shim_port: int,
        executor: LockedCodexExecutor,
        settlement_reader: GatewaySettlementReader,
    ) -> None:
        _validate_responses_url(gateway_responses_url)
        if isinstance(shim_port, bool) or not 1024 <= shim_port <= 65535:
            raise ValueError("shim_port must be uint16 1024..65535")
        self._gateway_responses_url = gateway_responses_url
        self._shim_port = shim_port
        self._executor = executor
        self._settlement_reader = settlement_reader

    def run(self, run: OfflineJudgeRunRequest) -> OfflineJudgeRunResult:
        if run.profile.agent_adapter != "codex_pipeline_locked_home_v1":
            raise BehaviorContractError("runner/profile adapter mismatch")
        lock = run.assets.runner_lock
        process = build_pipeline_locked_home_process_spec(
            runner_lock=lock,
            gateway_responses_url=self._gateway_responses_url,
            attempt_id=run.request.attempt_id,
            task_id=run.inputs.task_instance.payload.behavior_task_id,
            shim_port=self._shim_port,
        )
        # Both immutable executables are verified before execute() may expose a
        # JWT or open the Gateway route.
        self._executor.verify_binary(
            lock.codex.binary_path,
            lock.codex.binary_sha256,
            version=lock.codex.version,
        )
        self._executor.verify_binary(
            lock.shim.binary_path,
            lock.shim.binary_sha256,
            version=None,
        )
        try:
            initial = self._executor.execute(
                LockedCodexPassSpec(process, process.initial_argv, run.prompt, None)
            )
            session_id = _one_thread_started(initial.events_jsonl)
            _raise_pass_failure(initial.failure)
            report, seed = initial.report, initial.seed
            final_returncode = initial.returncode
            if report is None or seed is None:
                resumed_process = build_pipeline_locked_home_process_spec(
                    runner_lock=lock,
                    gateway_responses_url=self._gateway_responses_url,
                    attempt_id=run.request.attempt_id,
                    task_id=run.inputs.task_instance.payload.behavior_task_id,
                    shim_port=self._shim_port,
                    resume_session_id=session_id,
                )
                resumed = self._executor.execute(
                    LockedCodexPassSpec(
                        resumed_process,
                        resumed_process.resume_argv,
                        _locked_resume_prompt(report is None, seed is None),
                        session_id,
                    )
                )
                _raise_pass_failure(resumed.failure)
                if report is not None and resumed.report not in {None, report}:
                    raise BehaviorContractError("resume rewrote the existing report")
                if seed is not None and resumed.seed not in {None, seed}:
                    raise BehaviorContractError("resume rewrote the existing seed")
                report = report if report is not None else resumed.report
                seed = seed if seed is not None else resumed.seed
                final_returncode = resumed.returncode
            if report is None or seed is None:
                raise BehaviorContractError("Codex left report.md or seed.json missing")
            control = run.request.provenance.control_binding
            if control is None:
                raise BehaviorContractError("offline judge lacks frozen control binding")
            usage = self._settlement_reader.read(
                attempt_id=run.request.attempt_id,
                control_binding_sha256=control.snapshot_sha256,
            )
            return OfflineJudgeRunResult(report, seed, usage, final_returncode)
        finally:
            self._executor.cleanup(
                paths=tuple(lock.cleanup.scrub_paths),
                term_grace_seconds=lock.cleanup.term_grace_seconds,
                kill_after_grace=lock.cleanup.kill_after_grace,
            )


def _one_thread_started(events: bytes) -> UUID:
    if len(events) > 16 * 1024 * 1024:
        raise BehaviorContractError("Codex event log exceeds 16 MiB")
    sessions: list[UUID] = []
    for raw_line in events.splitlines():
        try:
            item = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BehaviorContractError("Codex event log is invalid JSONL") from exc
        if not isinstance(item, dict):
            raise BehaviorContractError("Codex event must be an object")
        if item.get("type") != "thread.started":
            continue
        raw_id = item.get("thread_id")
        try:
            session = UUID(raw_id) if isinstance(raw_id, str) else None
        except ValueError as exc:
            raise BehaviorContractError("thread.started UUID is invalid") from exc
        if session is None or str(session) != raw_id:
            raise BehaviorContractError("thread.started UUID is not canonical")
        sessions.append(session)
    if len(sessions) != 1:
        raise BehaviorContractError("Codex must emit exactly one thread.started session")
    return sessions[0]


def _locked_resume_prompt(report_missing: bool, seed_missing: bool) -> bytes:
    missing = [
        name
        for name, is_missing in (("report.md", report_missing), ("seed.json", seed_missing))
        if is_missing
    ]
    return (
        "Write only the missing output file(s): "
        + ", ".join(missing)
        + ". Do not re-investigate or rewrite an existing output.\n"
    ).encode()


def _raise_pass_failure(failure: PassFailure | None) -> None:
    if failure in {"provider_429", "provider_5xx", "gateway_transport"}:
        raise BehaviorProviderTransientError(failure)
    if failure == "stage_helper_transient":
        raise BehaviorInfrastructureTransientError(failure)


def build_pipeline_offline_judge_process_spec(
    *,
    profile: str,
    gateway_responses_url: str,
    attempt_id: UUID,
    binding_sha256: str,
) -> OfflineJudgeProcessSpec:
    """Resolve only registered judge runners with isolated environments."""

    _validate_responses_url(gateway_responses_url)
    token_reader = RotatingPipelineStepJwtReader(
        PIPELINE_STEP_JWT_PATH,
        attempt_id=attempt_id,
        step_id=OFFLINE_JUDGE_STEP_ID,
        binding_sha256=binding_sha256,
    )
    shim_env = MappingProxyType(
        {
            "LOOM_STEP_JWT_FILE": str(PIPELINE_STEP_JWT_PATH),
            "LOOM_GATEWAY_RESPONSES_URL": gateway_responses_url,
            "LOOM_EXECUTION_ATTEMPT_ID": str(attempt_id),
            "LOOM_CONTROL_BINDING_SHA256": binding_sha256,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        }
    )
    if profile == BASELINE_CODEX_PROFILE:
        codex = build_pipeline_codex_process_spec(gateway_responses_url=gateway_responses_url)
        return OfflineJudgeProcessSpec(
            profile="codex",
            agent_version=OFFICIAL_CODEX_VERSION,
            runner_argv=codex.argv,
            runner_env=codex.codex_env,
            shim_argv=(_SHIM,),
            shim_env=shim_env,
            token_reader=token_reader,
        )
    if profile == SYNTHETIC_PROFILE:
        return OfflineJudgeProcessSpec(
            profile="synthetic",
            agent_version="1",
            runner_argv=(_SYNTHETIC_RUNNER, "--strict-profile"),
            runner_env=MappingProxyType(
                {
                    "HOME": "/scratch/synthetic-home",
                    "NO_PROXY": "127.0.0.1,localhost",
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                }
            ),
            shim_argv=(_SYNTHETIC_SHIM,),
            shim_env=shim_env,
            token_reader=token_reader,
        )
    raise OfflineJudgeAuthError("judge profile is not registered")


def terminate_process_group(
    process: subprocess.Popen[bytes],
    *,
    grace_seconds: float = 30.0,
) -> None:
    """Terminate and reap the complete runner process group.

    Registered runners are always launched with a new session.  Cleanup never
    targets the caller's group and escalates only after a bounded grace period.
    """

    if (
        isinstance(grace_seconds, bool)
        or not isinstance(grace_seconds, int | float)
        or grace_seconds < 0
    ):
        raise ValueError("grace_seconds must be non-negative")
    if process.poll() is not None:
        process.wait()
        return
    try:
        process_group = os.getpgid(process.pid)
    except ProcessLookupError:
        process.wait()
        return
    if process_group == os.getpgrp():
        raise OfflineJudgeAuthError("refusing to terminate the caller process group")
    os.killpg(process_group, signal.SIGTERM)
    deadline = time.monotonic() + float(grace_seconds)
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    if process.poll() is None:
        os.killpg(process_group, signal.SIGKILL)
    process.wait()


def _validate_responses_url(value: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != "/v1/responses"
    ):
        raise OfflineJudgeAuthError(
            "judge shim requires the server-owned HTTPS /v1/responses route"
        )


def assert_closed_judge_environments(spec: OfflineJudgeProcessSpec) -> None:
    """Fail closed if a future profile adds an ambient credential/cache key."""

    forbidden = {
        "ANTHROPIC_API_KEY",
        "AWS_PROFILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "HF_HOME",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "OPENAI_CONFIG_FILE",
        "XDG_CONFIG_HOME",
    }
    if forbidden.intersection(spec.runner_env) or forbidden.intersection(spec.shim_env):
        raise OfflineJudgeAuthError("judge environment contains ambient credentials")
    if spec.runner_env.get("HOME") not in {CODEX_HOME, "/scratch/synthetic-home"}:
        raise OfflineJudgeAuthError("judge HOME is not Attempt-private")
    if set(spec.shim_env) != {
        "LOOM_STEP_JWT_FILE",
        "LOOM_GATEWAY_RESPONSES_URL",
        "LOOM_EXECUTION_ATTEMPT_ID",
        "LOOM_CONTROL_BINDING_SHA256",
        "LANG",
        "LC_ALL",
        "PATH",
    }:
        raise OfflineJudgeAuthError("judge shim environment is not closed")


__all__ = [
    "BASELINE_CODEX_PROFILE",
    "SYNTHETIC_PROFILE",
    "OfflineJudgeAuthError",
    "OfflineJudgeGatewayShim",
    "OfflineJudgeProcessSpec",
    "assert_closed_judge_environments",
    "build_pipeline_offline_judge_process_spec",
    "terminate_process_group",
]
