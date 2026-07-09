"""SkillFlow-style LLM-driven skill-patcher adapter (#672 PR-2).

Ports the SkillFlow ``SkillPatchEvolver``: after each trial in a
family, feed a compacted trajectory + current skill directory listing
into an evolver LLM, receive a JSON patch, apply it to the scratch
skills directory, and upload the mutated tree back to the state
backend.

Prompt shape and patch schema are held stable across benchmarks - all
family-run consumers of this adapter share the same evolver contract.
Model selection precedence (highest wins):

1. ``params["model"]`` on the batch's resolved adapter ref.
2. Cluster default ``settings.skill_evolver_default_model``.
3. Framework default ``anthropic/claude-sonnet-4-6``.

Billing: every gateway call runs through the standard OpenAI-chat
dialect with ``dialect="family_evolver"`` so the row lands in
``llm_calls`` next to the trial's agent-side rows and cost dashboards
attribute the spend correctly.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from loom.family_run.protocols import FamilyStateLike, StateBackend, TrialLike
from loom.family_run.spec import ResolvedFamilyRunSpec

_DEFAULT_MODEL = "anthropic/claude-sonnet-4-6"
_DEFAULT_MAX_STEPS = 20
_DEFAULT_MAX_OBS_CHARS = 3000
_DEFAULT_MAX_TOKENS = 8192
_DEFAULT_CALL_TIMEOUT_SEC = 300.0

_SYSTEM_PROMPT = """You are the shared-skill librarian for an autonomous \
agent evaluation. Between trials in a task family, you review the trial's \
compacted trajectory and the current shared-skill directory, then propose a \
JSON patch that adds, modifies, or deletes markdown skill files so future \
trials in the same family can reuse the learned technique.

Return STRICT JSON matching this schema and nothing else - no prose, no \
markdown fences:

{
  "add":    [{"path": "relative/name.md", "content": "..."}],
  "modify": [{"path": "relative/name.md", "content": "..."}],
  "delete": ["relative/name.md"]
}

All three keys MUST be present. Paths MUST be relative and MUST NOT contain \
'..'. Empty lists are valid. If the trajectory adds no new signal, return \
all-empty lists."""


class _GatewayClient(Protocol):
    """Minimal shape the adapter needs from the gateway HTTP client.

    Any callable that accepts (model, messages, dialect, max_tokens,
    timeout_sec) and returns a dict with an OpenAI-chat-shaped
    ``choices[0].message.content`` satisfies this protocol.
    """

    async def chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        dialect: str,
        max_tokens: int,
        timeout_sec: float,
        provider_connection_id: str | None = None,
    ) -> dict[str, Any]: ...


class PatchValidationError(RuntimeError):
    """Raised when the evolver response cannot be parsed or would
    escape the state directory. Caller (orchestrator) translates
    into failure_policy input."""


@dataclass(frozen=True)
class SkillPatch:
    add: list[tuple[str, str]]
    modify: list[tuple[str, str]]
    delete: list[str]

    @property
    def is_empty(self) -> bool:
        return not self.add and not self.modify and not self.delete


@dataclass
class SkillPatcherLLMAdapter:
    """Reference LLM-driven adapter. See module docstring for contract.

    The gateway client and evolver-model default are passed at
    call-time via ``params`` so this adapter has no import-time
    dependency on the orchestrator's settings module.
    """

    default_params: dict[str, Any] = field(default_factory=dict)

    async def initialize_state(
        self,
        *,
        family_key: str,
        spec: ResolvedFamilyRunSpec,
        backend: StateBackend,
        state_uri: str,
        params: dict[str, Any],
    ) -> str:
        init_from = params.get("init_from_skill_method")
        if not init_from:
            return state_uri
        upstream_root = params.get("upstream_root")
        if not upstream_root:
            raise ValueError(
                "skill_patcher_llm.initialize_state: init_from_skill_method "
                "requires params['upstream_root'] pointing at the extracted "
                "upstream tree",
            )
        src = Path(upstream_root) / "skills" / str(init_from) / family_key
        if not src.is_dir():
            raise FileNotFoundError(
                f"upstream skill dir not found for family {family_key!r}: {src}",
            )
        scratch = Path(tempfile.mkdtemp(prefix=f"skill-init-{family_key}-"))
        try:
            for entry in src.rglob("*"):
                if entry.is_file():
                    dst_path = scratch / entry.relative_to(src)
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(entry, dst_path)
            return await backend.upload(state_uri, scratch, {})
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    async def evolve(
        self,
        *,
        trial: TrialLike,
        family: FamilyStateLike,
        state_uri: str,
        backend: StateBackend,
        params: dict[str, Any],
    ) -> str:
        gateway: _GatewayClient | None = params.get("gateway")
        if gateway is None:
            raise ValueError(
                "skill_patcher_llm.evolve: params['gateway'] is required",
            )
        settings_default_model: str | None = params.get(
            "settings_default_model",
        )
        model = str(
            params.get("model")
            or settings_default_model
            or _DEFAULT_MODEL,
        )
        max_steps = int(params.get("max_steps", _DEFAULT_MAX_STEPS))
        max_obs_chars = int(params.get("max_obs_chars", _DEFAULT_MAX_OBS_CHARS))
        max_tokens = int(params.get("max_tokens", _DEFAULT_MAX_TOKENS))
        timeout_sec = float(
            params.get("call_timeout_sec", _DEFAULT_CALL_TIMEOUT_SEC),
        )
        trajectory_uri = params.get("trajectory_uri")
        # #672 blocker #695: BYO provider-connection routing for the
        # evolver call. When the batch's resolved family_run adapter
        # spec sets ``params.provider_connection_id``, forward it so
        # the gateway routes via the operator's stored credential
        # rather than the platform's default upstream. Non-BYO
        # (platform-credentialed) callers omit the field and the
        # gateway falls back to the legacy path.
        provider_connection_id = params.get("provider_connection_id")
        if provider_connection_id is not None:
            provider_connection_id = str(provider_connection_id)

        scratch = Path(tempfile.mkdtemp(prefix=f"skill-evolve-{family.family_key}-"))
        try:
            await backend.download(state_uri, scratch, {})
            trajectory_text = _compact_trajectory(
                trajectory_uri=trajectory_uri,
                max_steps=max_steps,
                max_obs_chars=max_obs_chars,
            )
            skills_listing = _skills_listing(scratch)
            user_prompt = _build_user_prompt(
                family_key=family.family_key,
                task_id=trial.task_id,
                trajectory_text=trajectory_text,
                skills_listing=skills_listing,
            )
            response = await gateway.chat_completion(
                model=model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                dialect="family_evolver",
                max_tokens=max_tokens,
                timeout_sec=timeout_sec,
                provider_connection_id=provider_connection_id,
            )
            raw = _extract_content(response)
            patch = _parse_patch(raw)
            if patch.is_empty:
                return state_uri
            _apply_patch(scratch, patch)
            return await backend.upload(state_uri, scratch, {})
        finally:
            shutil.rmtree(scratch, ignore_errors=True)


# ─── helpers ─────────────────────────────────────────────────────────


def _compact_trajectory(
    *,
    trajectory_uri: str | None,
    max_steps: int,
    max_obs_chars: int,
) -> str:
    """Read the trial's JSONL trajectory (if present) and produce a
    compact plain-text summary. Reads via
    :class:`loom.trajectory.reader.TrajectoryReader` when the URI is a
    local path; returns ``"(no trajectory available)"`` otherwise so
    the caller can still exercise the prompt / patch path in tests.
    """
    if trajectory_uri is None:
        return "(no trajectory available)"
    path = Path(trajectory_uri)
    if not path.exists():
        return f"(trajectory not found at {trajectory_uri})"
    # Import late so the adapter module has no side-effect import chain
    # on ``loom.trajectory.reader``.
    from loom.models.trajectory import EventKind
    from loom.trajectory.reader import TrajectoryReader

    reader = TrajectoryReader(path)
    lines: list[str] = []
    step_count = 0
    for event in reader.iter_all():
        kind = getattr(event, "kind", None)
        if kind == EventKind.STEP_START:
            step_count += 1
            if step_count > max_steps:
                lines.append(f"[+{max_steps - step_count} more steps truncated]")
                break
            instruction = getattr(event, "instruction_excerpt", "")
            lines.append(f"### Step {step_count}\n{instruction}")
        elif kind == EventKind.AGENT_THOUGHT:
            thought = getattr(event, "content", "")
            if thought:
                lines.append(f"THOUGHT: {thought[:max_obs_chars]}")
        elif kind == EventKind.ENV_EXEC:
            cmd = getattr(event, "cmd", "")
            rc = getattr(event, "return_code", "?")
            lines.append(f"EXEC (rc={rc}): {cmd[:max_obs_chars]}")
        elif kind == EventKind.TOOL_USE:
            name = getattr(event, "tool_name", "")
            result = getattr(event, "result", None)
            snippet = json.dumps(result, default=str)[:max_obs_chars] if result else ""
            lines.append(f"TOOL {name}: {snippet}")
    return "\n".join(lines) or "(empty trajectory)"


def _skills_listing(scratch: Path) -> str:
    """Human-readable listing of the current state directory."""
    entries: list[str] = []
    for path in sorted(scratch.rglob("*")):
        if path.is_file():
            rel = path.relative_to(scratch).as_posix()
            size = path.stat().st_size
            entries.append(f"- {rel} ({size} bytes)")
    if not entries:
        return "(empty skill library)"
    return "\n".join(entries)


def _build_user_prompt(
    *,
    family_key: str,
    task_id: str,
    trajectory_text: str,
    skills_listing: str,
) -> str:
    return (
        f"Family: {family_key}\nJust-completed task: {task_id}\n\n"
        f"## Current skill library\n{skills_listing}\n\n"
        f"## Compacted trajectory\n{trajectory_text}\n\n"
        "Return the JSON patch."
    )


def _extract_content(response: dict[str, Any]) -> str:
    try:
        return str(response["choices"][0]["message"]["content"])
    except (KeyError, IndexError, TypeError) as exc:
        raise PatchValidationError(
            f"evolver response missing choices[0].message.content: {response!r}",
        ) from exc


def _parse_patch(raw: str) -> SkillPatch:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PatchValidationError(
            f"evolver returned non-JSON: {exc}: {raw[:200]!r}",
        ) from exc
    if not isinstance(payload, dict):
        raise PatchValidationError(
            f"evolver JSON is not an object: {type(payload).__name__}",
        )
    missing = {"add", "modify", "delete"} - payload.keys()
    if missing:
        raise PatchValidationError(
            f"evolver patch missing required keys: {sorted(missing)}",
        )

    add = _validate_write_entries("add", payload["add"])
    modify = _validate_write_entries("modify", payload["modify"])
    delete = _validate_delete_entries(payload["delete"])
    return SkillPatch(add=add, modify=modify, delete=delete)


def _validate_write_entries(
    field_name: str, raw: Any,
) -> list[tuple[str, str]]:
    if not isinstance(raw, list):
        raise PatchValidationError(
            f"evolver patch '{field_name}' must be a list; got {type(raw).__name__}",
        )
    out: list[tuple[str, str]] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise PatchValidationError(
                f"'{field_name}'[{i}] must be an object with path+content",
            )
        path = entry.get("path")
        content = entry.get("content")
        if not isinstance(path, str) or not isinstance(content, str):
            raise PatchValidationError(
                f"'{field_name}'[{i}] must have string 'path' and 'content'",
            )
        _validate_relative_path(field_name, path)
        out.append((path, content))
    return out


def _validate_delete_entries(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        raise PatchValidationError(
            f"evolver patch 'delete' must be a list; got {type(raw).__name__}",
        )
    out: list[str] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, str):
            raise PatchValidationError(
                f"'delete'[{i}] must be a string path",
            )
        _validate_relative_path("delete", entry)
        out.append(entry)
    return out


def _validate_relative_path(field_name: str, path: str) -> None:
    if not path:
        raise PatchValidationError(
            f"'{field_name}' path must be non-empty",
        )
    if path.startswith("/"):
        raise PatchValidationError(
            f"'{field_name}' path {path!r} must be relative, not absolute",
        )
    parts = Path(path).parts
    if ".." in parts:
        raise PatchValidationError(
            f"'{field_name}' path {path!r} may not contain '..'",
        )


def _apply_patch(scratch: Path, patch: SkillPatch) -> None:
    for path, content in patch.add:
        target = scratch / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    for path, content in patch.modify:
        target = scratch / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    for path in patch.delete:
        target = scratch / path
        try:
            target.unlink()
        except FileNotFoundError:
            # Idempotent delete - evolver may reference a file that
            # was already cleaned up. Log-worthy but not fatal.
            continue
