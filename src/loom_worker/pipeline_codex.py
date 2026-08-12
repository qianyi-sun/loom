"""Strict official Codex subprocess seam for Pipeline offline judging."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit
from uuid import UUID

from loom.integrations.behavior.offline_judge_assets import BehaviorOfflineRunnerLockV1

OFFICIAL_CODEX_VERSION = "0.146.0"
OFFICIAL_MCP_SERVERS = ("video", "video_demo")
CODEX_HOME = "/scratch/codex-home"
CODEX_PATH = (
    "/opt/behavior/provider-assets/behavior_offline_judge/tools:"
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)


class PipelineCodexContractError(ValueError):
    pass


@dataclass(frozen=True)
class PipelineCodexProcessSpec:
    codex_version: str
    install_script: None
    argv: tuple[str, ...]
    codex_env: MappingProxyType[str, str]
    shim_env: MappingProxyType[str, str]
    mcp_servers: tuple[str, str]
    new_process_group: bool = True


@dataclass(frozen=True)
class PipelineLockedHomeProcessSpec:
    binary_sha256: str
    initial_argv: tuple[str, ...]
    resume_argv: tuple[str, ...]
    config_toml: bytes
    codex_env: MappingProxyType[str, str]
    shim_argv: tuple[str, ...]
    shim_env: MappingProxyType[str, str]
    install_script: None = None
    new_process_group: bool = True


def build_pipeline_codex_process_spec(
    *, gateway_responses_url: str,
) -> PipelineCodexProcessSpec:
    parsed = urlsplit(gateway_responses_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != "/v1/responses"
    ):
        raise PipelineCodexContractError(
            "Gateway Responses URL must be server-owned HTTPS /v1/responses"
        )
    codex_env = MappingProxyType(
        {
            "HOME": CODEX_HOME,
            "CODEX_HOME": CODEX_HOME,
            "OPENAI_API_KEY": "loom-loopback-dummy",
            "NO_PROXY": "127.0.0.1,localhost",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": CODEX_PATH,
        }
    )
    shim_env = MappingProxyType(
        {
            "LOOM_STEP_JWT_FILE": "/run/loom/step-jwt",
            "LOOM_GATEWAY_RESPONSES_URL": gateway_responses_url,
        }
    )
    return PipelineCodexProcessSpec(
        codex_version=OFFICIAL_CODEX_VERSION,
        install_script=None,
        argv=(
            "/opt/loom/bin/codex",
            "exec",
            "--strict-config",
            "--config",
            f"{CODEX_HOME}/config.toml",
        ),
        codex_env=codex_env,
        shim_env=shim_env,
        mcp_servers=OFFICIAL_MCP_SERVERS,
    )


def build_pipeline_locked_home_process_spec(
    *,
    runner_lock: BehaviorOfflineRunnerLockV1,
    gateway_responses_url: str,
    attempt_id: UUID,
    task_id: int,
    shim_port: int,
    resume_session_id: UUID | None = None,
) -> PipelineLockedHomeProcessSpec:
    """Render the #1223 exact Codex mode without changing Trial Codex behavior."""

    parsed = urlsplit(gateway_responses_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != "/v1/responses"
    ):
        raise PipelineCodexContractError(
            "Gateway Responses URL must be server-owned HTTPS /v1/responses"
        )
    initial = runner_lock.initial_argv()
    resume = (
        runner_lock.resume_argv(resume_session_id)
        if resume_session_id is not None
        else runner_lock.resume_argv(UUID(int=0))
    )
    if "--ignore-user-config" in (*initial, *resume):
        raise PipelineCodexContractError("locked home cannot suppress its own config")
    return PipelineLockedHomeProcessSpec(
        binary_sha256=runner_lock.codex.binary_sha256,
        initial_argv=initial,
        resume_argv=resume,
        config_toml=runner_lock.render_config_toml(task_id=task_id, shim_port=shim_port),
        codex_env=MappingProxyType(dict(runner_lock.codex_env)),
        shim_argv=runner_lock.shim_argv(attempt_id=attempt_id, shim_port=shim_port),
        shim_env=MappingProxyType(dict(runner_lock.shim_env(gateway_responses_url))),
    )
class RotatingStepJwtReader:
    """Open and verify the current token inode for every shim request."""

    def __init__(self, path: Path, *, expected_uid: int, expected_gid: int) -> None:
        if not path.is_absolute():
            raise PipelineCodexContractError("step JWT path must be absolute")
        self._path = path
        self._expected_uid = expected_uid
        self._expected_gid = expected_gid

    def read_for_request(self) -> str:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(self._path, flags)
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise PipelineCodexContractError("step JWT must be one regular inode")
            if stat.S_IMODE(metadata.st_mode) != 0o400:
                raise PipelineCodexContractError("step JWT mode must be 0400")
            if (metadata.st_uid, metadata.st_gid) != (
                self._expected_uid,
                self._expected_gid,
            ):
                raise PipelineCodexContractError("step JWT owner drift")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(fd, 4096)
                if not chunk:
                    break
                chunks.append(chunk)
                if sum(map(len, chunks)) > 16_384:
                    raise PipelineCodexContractError("step JWT exceeds the closed size limit")
        finally:
            os.close(fd)
        value = b"".join(chunks).decode("utf-8").strip()
        if not value.startswith("loom_step_") or not value:
            raise PipelineCodexContractError("step JWT is malformed")
        return value
