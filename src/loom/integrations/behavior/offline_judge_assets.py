"""Closed provider-asset and runner-lock contract for BEHAVIOR offline judging."""

from __future__ import annotations

import json
import os
import re
import stat
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, model_validator

from loom.integrations.behavior.canonical_json import (
    canonical_digest,
    canonical_document,
    digest_bytes,
)
from loom.integrations.behavior.contracts import ProviderAssetManifestV1
from loom.integrations.behavior.errors import BehaviorContractError
from loom.pipeline.control_bindings import McpServerLockV1
from loom.pipeline.spec import Digest, PipelineModel

PROVIDER_ROOT = "/opt/behavior/provider-assets/behavior_offline_judge"
CODEX_BINARY = "/opt/behavior/codex/bin/codex"
SHIM_BINARY = "/opt/behavior/bin/loom-codex-gateway-shim"
CODEX_HOME = "/scratch/codex-home"
JUDGE_WORKDIR = "/outputs/judge"
STEP_JWT_FILE = "/run/loom/step-jwt"

MAX_ASSET_BYTES = 32 * 1024 * 1024
UINT32_MAX = 4_294_967_295

ASSET_FILES = (
    "inspect_rollout.md",
    "looking.md",
    "mcp-lock.json",
    "runner-lock.json",
    "seed.schema.json",
    "skill_vocabulary.md",
    "system.md",
    "tools/mosaic.py",
    "validate_outputs.py",
)
ROOT_FILES = (*ASSET_FILES, "manifest.json")

INITIAL_ARGV = (
    CODEX_BINARY,
    "exec",
    "--strict-config",
    "--json",
    "--model",
    "gpt-5.6-sol",
    "--dangerously-bypass-approvals-and-sandbox",
    "--ignore-rules",
    "--skip-git-repo-check",
    "--cd",
    JUDGE_WORKDIR,
    "-",
)
RESUME_ARGV = (
    CODEX_BINARY,
    "exec",
    "resume",
    "--strict-config",
    "--json",
    "--model",
    "gpt-5.6-sol",
    "--dangerously-bypass-approvals-and-sandbox",
    "--ignore-rules",
    "--skip-git-repo-check",
    "<session_id>",
    "-",
)

CONFIG_TOML_LINES = (
    'model = "gpt-5.6-sol"',
    'model_provider = "loom"',
    'approval_policy = "never"',
    'sandbox_mode = "danger-full-access"',
    "[model_providers.loom]",
    'name = "Loom Gateway Responses"',
    'base_url = "http://127.0.0.1:<shim_port>/v1"',
    'env_key = "OPENAI_API_KEY"',
    'wire_api = "responses"',
    "[mcp_servers.video]",
    'command = "/opt/behavior/mcp-deep-video/.venv/bin/python"',
    'args = ["-m","mcp_deep_video.server","--video-root","/inputs/rollout/payload/videos/task-<task_id:04d>","--cache-dir","/scratch/mcp/video/cache","--debug-dir","/scratch/mcp/video/debug"]',
    "startup_timeout_sec = 60",
    "tool_timeout_sec = 600",
    'env = { PYTHONNOUSERSITE = "1", PYTHONUNBUFFERED = "1" }',
    "[mcp_servers.video_demo]",
    'command = "/opt/behavior/mcp-deep-video/.venv/bin/python"',
    'args = ["-m","mcp_deep_video.server","--video-root","/inputs/dataset/payload/videos/task-<task_id:04d>","--cache-dir","/scratch/mcp/video_demo/cache","--debug-dir","/scratch/mcp/video_demo/debug"]',
    "startup_timeout_sec = 60",
    "tool_timeout_sec = 600",
    'env = { PYTHONNOUSERSITE = "1", PYTHONUNBUFFERED = "1" }',
)

CODEX_ENV = {
    "HOME": CODEX_HOME,
    "CODEX_HOME": CODEX_HOME,
    "OPENAI_API_KEY": "loom-loopback-dummy",
    "NO_PROXY": "127.0.0.1,localhost",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": (
        f"{PROVIDER_ROOT}/tools:"
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    ),
}

SCRUB_PATHS = (
    "/scratch/codex-home",
    "/scratch/offline-judge/codex-events.jsonl",
    "/scratch/offline-judge/sweep-prompt.md",
    "/scratch/offline-judge/resume-prompt.md",
    "/scratch/mcp/video/cache",
    "/scratch/mcp/video/debug",
    "/scratch/mcp/video_demo/cache",
    "/scratch/mcp/video_demo/debug",
)

_FORBIDDEN_ARGV = frozenset(
    {"--ignore-user-config", "--last", "--all", "--ephemeral", "--profile", "-c"}
)
_PLACEHOLDER_RE = re.compile(r"<[^>]+>")
_TASK_CARD_MAX_BYTES = 4 * 1024 * 1024


class OfflineJudgeCodexLockV1(PipelineModel):
    version: Literal["0.146.0"]
    binary_path: Literal["/opt/behavior/codex/bin/codex"]
    binary_sha256: Digest
    install_script: None


class OfflineJudgePathsV1(PipelineModel):
    home: Literal["/scratch/codex-home"]
    home_mode: Literal["0700"]
    workdir: Literal["/outputs/judge"]
    events: Literal["/scratch/offline-judge/codex-events.jsonl"]
    initial_prompt: Literal["/scratch/offline-judge/sweep-prompt.md"]
    resume_prompt: Literal["/scratch/offline-judge/resume-prompt.md"]


class OfflineJudgeArgvV1(PipelineModel):
    initial: list[str]
    resume: list[str]

    @model_validator(mode="after")
    def validate_exact_argv(self) -> OfflineJudgeArgvV1:
        if tuple(self.initial) != INITIAL_ARGV or tuple(self.resume) != RESUME_ARGV:
            raise ValueError("offline judge argv differs from the exact locked forms")
        if _FORBIDDEN_ARGV.intersection((*self.initial, *self.resume)):
            raise ValueError("offline judge argv contains a forbidden generic Codex flag")
        return self


class OfflineJudgeShimReadinessV1(PipelineModel):
    kind: Literal["tcp_connect"]
    host: Literal["127.0.0.1"]
    interval_milliseconds: Literal[100]
    timeout_seconds: Literal[30]
    require_child_alive: Literal[True]


class OfflineJudgeShimEnvV1(PipelineModel):
    LOOM_STEP_JWT_FILE: Literal["/run/loom/step-jwt"]
    LOOM_GATEWAY_RESPONSES_URL: Literal["<gateway_responses_url>"]


class OfflineJudgeShimV1(PipelineModel):
    binary_path: Literal["/opt/behavior/bin/loom-codex-gateway-shim"]
    binary_sha256: Digest
    argv: list[str]
    env: OfflineJudgeShimEnvV1
    readiness: OfflineJudgeShimReadinessV1
    dummy_api_key: Literal["loom-loopback-dummy"]

    @model_validator(mode="after")
    def validate_exact_shim(self) -> OfflineJudgeShimV1:
        expected = [
            SHIM_BINARY,
            "--listen",
            "127.0.0.1:<shim_port>",
            "--jwt-file",
            STEP_JWT_FILE,
            "--attempt-id",
            "<attempt_id>",
            "--provider-logical-name",
            "behavior_offline_judge",
        ]
        if self.argv != expected:
            raise ValueError("shim argv differs from the exact locked form")
        return self


class OfflineJudgeCodexEnvV1(PipelineModel):
    HOME: Literal["/scratch/codex-home"]
    CODEX_HOME: Literal["/scratch/codex-home"]
    OPENAI_API_KEY: Literal["loom-loopback-dummy"]
    NO_PROXY: Literal["127.0.0.1,localhost"]
    LANG: Literal["C.UTF-8"]
    LC_ALL: Literal["C.UTF-8"]
    PATH: Literal[
        "/opt/behavior/provider-assets/behavior_offline_judge/tools:"
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    ]


class OfflineJudgeEventProtocolV1(PipelineModel):
    format: Literal["codex_jsonl_v1"]
    thread_started_type: Literal["thread.started"]
    thread_id_field: Literal["thread_id"]
    required_session_count: Literal[1]
    max_resume_count: Literal[1]
    resume_when: Literal["report_or_seed_missing"]
    resume_same_attempt_home_ledger: Literal[True]


class OfflineJudgeCleanupV1(PipelineModel):
    process_group: list[str]
    term_grace_seconds: Literal[30]
    kill_after_grace: Literal[True]
    scrub_paths: list[str]

    @model_validator(mode="after")
    def validate_exact_cleanup(self) -> OfflineJudgeCleanupV1:
        if self.process_group != ["codex", "shim", "video", "video_demo"]:
            raise ValueError("cleanup process group differs from the closed four-child set")
        if tuple(self.scrub_paths) != SCRUB_PATHS:
            raise ValueError("cleanup scrub paths differ from the exact scratch inventory")
        return self


class BehaviorOfflineRunnerLockV1(PipelineModel):
    schema_version: Literal["behavior.offline-runner-lock.v1"]
    codex: OfflineJudgeCodexLockV1
    paths: OfflineJudgePathsV1
    argv: OfflineJudgeArgvV1
    config_toml_lines: list[str]
    shim: OfflineJudgeShimV1
    mcp_servers: Annotated[list[McpServerLockV1], Field(min_length=2, max_length=2)]
    env: OfflineJudgeCodexEnvV1
    event_protocol: OfflineJudgeEventProtocolV1
    cleanup: OfflineJudgeCleanupV1

    @model_validator(mode="after")
    def validate_exact_lock(self) -> BehaviorOfflineRunnerLockV1:
        if tuple(self.config_toml_lines) != CONFIG_TOML_LINES:
            raise ValueError("config_toml_lines differs from the exact ordered template")
        if [item.logical_name for item in self.mcp_servers] != ["video", "video_demo"]:
            raise ValueError("runner lock requires the exact bytewise-sorted two MCP locks")
        return self

    def render_config_toml(self, *, task_id: int, shim_port: int) -> bytes:
        task = _uint32(task_id, "task_id")
        port = _shim_port(shim_port)
        encoded = (
            "\n".join(self.config_toml_lines)
            .replace("<shim_port>", str(port))
            .replace("<task_id:04d>", f"{task:04d}")
            + "\n"
        ).encode("utf-8")
        if _PLACEHOLDER_RE.search(encoded.decode("utf-8")):
            raise BehaviorContractError("rendered config contains an unresolved placeholder")
        return encoded

    def initial_argv(self) -> tuple[str, ...]:
        return tuple(self.argv.initial)

    def resume_argv(self, session_id: UUID | str) -> tuple[str, ...]:
        session = _canonical_uuid(session_id, "session_id")
        return tuple(session if item == "<session_id>" else item for item in self.argv.resume)

    def shim_argv(self, *, attempt_id: UUID | str, shim_port: int) -> tuple[str, ...]:
        attempt = _canonical_uuid(attempt_id, "attempt_id")
        port = _shim_port(shim_port)
        replacements = {
            "127.0.0.1:<shim_port>": f"127.0.0.1:{port}",
            "<attempt_id>": attempt,
        }
        return tuple(replacements.get(item, item) for item in self.shim.argv)

    def shim_env(self, gateway_responses_url: str) -> Mapping[str, str]:
        _gateway_url(gateway_responses_url)
        return MappingProxyType(
            {
                "LOOM_STEP_JWT_FILE": STEP_JWT_FILE,
                "LOOM_GATEWAY_RESPONSES_URL": gateway_responses_url,
            }
        )

    @property
    def codex_env(self) -> Mapping[str, str]:
        return MappingProxyType(self.env.model_dump())

    @property
    def mcp_server_locks_sha256(self) -> str:
        return canonical_digest(self.mcp_servers, persisted=False)


@dataclass(frozen=True)
class ProviderAssetBundle:
    root: Path
    manifest: ProviderAssetManifestV1
    runner_lock: BehaviorOfflineRunnerLockV1
    mcp_locks: tuple[McpServerLockV1, ...]
    bytes_by_path: Mapping[str, bytes]
    digests: Mapping[str, str]
    manifest_sha256: str

    @classmethod
    def load(
        cls,
        root: Path | str,
        *,
        expected_manifest_sha256: str | None = None,
    ) -> ProviderAssetBundle:
        asset_root = Path(root)
        _validate_inventory(asset_root)
        contents = {relative: _read_regular(asset_root / relative) for relative in ROOT_FILES}
        manifest_value = _canonical_value(contents["manifest.json"], label="manifest.json")
        try:
            manifest = ProviderAssetManifestV1.model_validate(manifest_value)
        except ValueError as exc:
            raise BehaviorContractError("provider asset manifest violates its closed schema") from exc
        if manifest.logical_name != "behavior_offline_judge":
            raise BehaviorContractError("provider asset manifest has the wrong logical_name")
        manifest_digest = digest_bytes(contents["manifest.json"])
        if expected_manifest_sha256 is not None and expected_manifest_sha256 != manifest_digest:
            raise BehaviorContractError("provider asset manifest digest disagrees with authority")
        descriptors = {item.relative_path: item for item in manifest.files}
        for relative in ASSET_FILES:
            descriptor = descriptors[relative]
            encoded = contents[relative]
            if descriptor.size_bytes != len(encoded) or descriptor.sha256 != digest_bytes(encoded):
                raise BehaviorContractError(f"provider asset bytes disagree with manifest: {relative}")

        runner_value = _canonical_value(contents["runner-lock.json"], label="runner-lock.json")
        mcp_value = _canonical_value(contents["mcp-lock.json"], label="mcp-lock.json")
        try:
            runner_lock = BehaviorOfflineRunnerLockV1.model_validate(runner_value)
            if not isinstance(mcp_value, list):
                raise ValueError("MCP lock must be an array")
            mcp_locks = tuple(McpServerLockV1.model_validate(item) for item in mcp_value)
        except ValueError as exc:
            raise BehaviorContractError("provider runner/MCP lock violates its closed schema") from exc
        if list(mcp_locks) != runner_lock.mcp_servers:
            raise BehaviorContractError("runner lock and mcp-lock.json do not contain identical locks")

        digests = {relative: digest_bytes(encoded) for relative, encoded in contents.items()}
        return cls(
            root=asset_root,
            manifest=manifest,
            runner_lock=runner_lock,
            mcp_locks=mcp_locks,
            bytes_by_path=MappingProxyType(contents),
            digests=MappingProxyType(digests),
            manifest_sha256=manifest_digest,
        )


def compose_sweep_prompt(
    bundle: ProviderAssetBundle,
    task_card: bytes,
    task_id: int,
    demo_id: int,
) -> bytes:
    """Compose the one predicate-log prompt from locked assets and a signed card."""

    task = _uint32(task_id, "task_id")
    demo = _uint32(demo_id, "demo_id")
    card = _task_card(task_card)
    separator = b"\n---\n\n"
    static = separator.join(
        bundle.bytes_by_path[name]
        for name in ("system.md", "looking.md", "skill_vocabulary.md", "inspect_rollout.md")
    )
    task_tag = f"task-{task:04d}"
    episode = f"episode_{demo:08d}"
    run_parameters = f"""\n---\n\n# Run parameters

Work in `/outputs/judge`. Write tool outputs and cache only below `/scratch`; write the two
declared final outputs only to `/outputs/judge`.

Signed read-only inputs:
- Immutable task card: `/inputs/dataset/payload/agentic_sweep/task_cards/{task_tag}.md`
- BDDL transitions: `/inputs/rollout/payload/meta/episodes/{task_tag}/{episode}_bddl_transitions.json`
- Rollout video root: `/inputs/rollout/payload/videos/{task_tag}`
- Demo video root: `/inputs/dataset/payload/videos/{task_tag}`

Write exactly `/outputs/judge/report.md` and `/outputs/judge/seed.json`. Re-read and validate
both files before finishing.

---

# Task card — {task_tag}

""".encode()
    prompt = static + run_parameters + card
    if not prompt.endswith(b"\n"):
        prompt += b"\n"
    return prompt


def resume_prompt(*, report_missing: bool, seed_missing: bool) -> bytes:
    missing = [name for name, absent in (("report.md", report_missing), ("seed.json", seed_missing)) if absent]
    if not missing:
        raise BehaviorContractError("resume requires at least one missing declared output")
    rendered = " ".join(f"`{name}`" for name in missing)
    return (
        "You ended without writing every output the task asked for. Do not re-investigate — "
        f"use what you already established. Write {rendered} now under `/outputs/judge/`, "
        "in the formats the task specified.\n"
    ).encode()


def bundled_provider_asset_root() -> Path:
    return Path(__file__).with_name("provider_assets") / "offline_judge"


def _task_card(encoded: bytes) -> bytes:
    if len(encoded) > _TASK_CARD_MAX_BYTES or encoded.startswith(b"\xef\xbb\xbf"):
        raise BehaviorContractError("task card is oversized or contains a UTF-8 BOM")
    if b"\x00" in encoded or b"\r" in encoded:
        raise BehaviorContractError("task card contains forbidden NUL/CR bytes")
    try:
        text = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BehaviorContractError("task card is not UTF-8") from exc
    if unicodedata.normalize("NFC", text) != text:
        raise BehaviorContractError("task card must already be NFC")
    if not text.strip():
        raise BehaviorContractError("task card is empty")
    return encoded


def _uint32(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= UINT32_MAX:
        raise BehaviorContractError(f"{label} must be a strict uint32")
    return value


def _shim_port(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1024 <= value <= 65535:
        raise BehaviorContractError("shim_port must be a strict uint16 in 1024..65535")
    return value


def _canonical_uuid(value: UUID | str, label: str) -> str:
    rendered = str(value)
    try:
        parsed = UUID(rendered)
    except ValueError as exc:
        raise BehaviorContractError(f"{label} is not a UUID") from exc
    canonical = str(parsed)
    if rendered != canonical:
        raise BehaviorContractError(f"{label} is not a canonical lowercase UUID")
    return canonical


def _gateway_url(value: str) -> None:
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
        raise BehaviorContractError("Gateway Responses URL must be HTTPS with exact /v1/responses")


def _validate_inventory(root: Path) -> None:
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise BehaviorContractError("provider asset root is unavailable") from exc
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise BehaviorContractError("provider asset root must be a real directory")
    observed_files: set[str] = set()
    observed_dirs: set[str] = set()
    try:
        for directory, directories, files in os.walk(root, followlinks=False):
            relative_dir = Path(directory).relative_to(root)
            for name in directories:
                relative = (relative_dir / name).as_posix()
                metadata = (Path(directory) / name).lstat()
                if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                    raise BehaviorContractError("provider asset tree contains a symlink")
                observed_dirs.add(relative)
            for name in files:
                relative = (relative_dir / name).as_posix()
                metadata = (Path(directory) / name).lstat()
                if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                    raise BehaviorContractError("provider asset tree contains a non-regular file")
                observed_files.add(relative)
    except OSError as exc:
        raise BehaviorContractError("provider asset inventory cannot be inspected") from exc
    if observed_dirs != {"tools"} or observed_files != set(ROOT_FILES):
        raise BehaviorContractError("provider asset root has an extra or missing path")


def _read_regular(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise BehaviorContractError(f"provider asset cannot be opened: {path.name}") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise BehaviorContractError("provider asset must be one private regular inode")
        if before.st_size > MAX_ASSET_BYTES:
            raise BehaviorContractError("provider asset exceeds the fixed size limit")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(fd, min(65_536, MAX_ASSET_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > MAX_ASSET_BYTES:
                raise BehaviorContractError("provider asset exceeds the fixed size limit")
        after = os.fstat(fd)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise BehaviorContractError("provider asset changed while being read")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _canonical_value(encoded: bytes, *, label: str) -> Any:
    if encoded.startswith(b"\xef\xbb\xbf") or b"\r" in encoded:
        raise BehaviorContractError(f"{label} contains BOM or CR")
    if not encoded.endswith(b"\n") or encoded.endswith(b"\n\n"):
        raise BehaviorContractError(f"{label} must end in exactly one LF")
    try:
        value = json.loads(
            encoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BehaviorContractError(f"{label} is not strict JSON") from exc
    if canonical_document(value) != encoded:
        raise BehaviorContractError(f"{label} is not canonical RFC8785/JCS+LF")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_non_finite(value: str) -> None:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


__all__ = [
    "ASSET_FILES",
    "CODEX_ENV",
    "CONFIG_TOML_LINES",
    "INITIAL_ARGV",
    "PROVIDER_ROOT",
    "RESUME_ARGV",
    "BehaviorOfflineRunnerLockV1",
    "ProviderAssetBundle",
    "bundled_provider_asset_root",
    "compose_sweep_prompt",
    "resume_prompt",
]
