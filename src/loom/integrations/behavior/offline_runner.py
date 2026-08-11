"""Closed process specifications for Pipeline whole-episode judging."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Literal
from urllib.parse import urlsplit
from uuid import UUID

from loom.integrations.behavior.provider import (
    OFFLINE_JUDGE_STEP_ID,
    PIPELINE_STEP_JWT_PATH,
    RotatingPipelineStepJwtReader,
)
from loom_worker.pipeline_codex import (
    CODEX_HOME,
    OFFICIAL_CODEX_VERSION,
    build_pipeline_codex_process_spec,
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
    grace_seconds: float = 2.0,
) -> None:
    """Terminate and reap the complete runner process group.

    Registered runners are always launched with a new session.  Cleanup never
    targets the caller's group and escalates only after a bounded grace period.
    """

    if not isinstance(grace_seconds, int | float) or grace_seconds < 0:
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
