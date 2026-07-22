"""Unit tests for the SkillFlow-style LLM patcher adapter (#672 PR-2)."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom.family_run.skill_patcher_llm import (
    PatchValidationError,
    SkillPatcherLLMAdapter,
    _parse_patch,
    _validate_relative_path,
)
from loom.family_run.spec import PluginRef, ResolvedFamilyRunSpec


def _spec() -> ResolvedFamilyRunSpec:
    return ResolvedFamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="skill_patcher_llm"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )


@dataclass
class _FakeBackend:
    """Filesystem-backed fake - the "URI" is a directory."""

    root: Path
    upload_calls: list[Path] = field(default_factory=list)
    download_calls: list[str] = field(default_factory=list)

    async def initialize(self, *, batch_id, family_key, params):
        state_dir = self.root / str(batch_id) / family_key
        state_dir.mkdir(parents=True, exist_ok=True)
        return str(state_dir)

    async def download(self, state_uri, dst, params):
        self.download_calls.append(state_uri)
        src = Path(state_uri)
        dst.mkdir(parents=True, exist_ok=True)
        if src.exists():
            for entry in src.rglob("*"):
                if entry.is_file():
                    d = dst / entry.relative_to(src)
                    d.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(entry, d)

    async def upload(self, state_uri, src, params):
        # Overwrite the "URI directory" contents from the scratch dir.
        self.upload_calls.append(Path(src))
        dst = Path(state_uri)
        if dst.exists():
            shutil.rmtree(dst)
        dst.mkdir(parents=True, exist_ok=True)
        for entry in src.rglob("*"):
            if entry.is_file():
                d = dst / entry.relative_to(src)
                d.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(entry, d)
        return state_uri


@dataclass
class _FakeGateway:
    response: dict[str, Any]
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        dialect: str,
        max_tokens: int,
        timeout_sec: float,
        team_id: str,
        trial_id: str,
        provider_connection_id: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "model": model,
                "messages": messages,
                "dialect": dialect,
                "max_tokens": max_tokens,
                "timeout_sec": timeout_sec,
                "team_id": team_id,
                "trial_id": trial_id,
                "provider_connection_id": provider_connection_id,
            },
        )
        return self.response


def _fake_response(patch: dict[str, Any]) -> dict[str, Any]:
    return {
        "choices": [
            {"message": {"content": json.dumps(patch)}},
        ],
    }


@dataclass
class _Trial:
    id: object = field(default_factory=uuid4)
    team_id: object = field(default_factory=uuid4)
    task_id: str = "family_a/task_1"
    state: str = "succeeded"
    reward: float | None = 1.0
    attempt_count: int = 1


@dataclass
class _Family:
    batch_id: object = field(default_factory=uuid4)
    family_key: str = "family_a"
    task_sequence: list[str] = field(
        default_factory=lambda: ["family_a/task_1", "family_a/task_2"],
    )
    current_index: int = 0
    attempt_count: int = 0


# ─── initialize_state ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_initialize_state_without_init_returns_uri_unchanged(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    adapter = SkillPatcherLLMAdapter()
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    out = await adapter.initialize_state(
        family_key="family_a",
        spec=_spec(),
        backend=backend,
        state_uri=seed_uri,
        params={},
    )
    assert out == seed_uri
    assert backend.upload_calls == []


@pytest.mark.asyncio
async def test_initialize_state_with_init_from_skill_method_copies_upstream(
    tmp_path,
):
    upstream_root = tmp_path / "upstream"
    fam_src = upstream_root / "skills" / "human_authored" / "family_a"
    fam_src.mkdir(parents=True)
    (fam_src / "seed.md").write_text("# seed skill\n")
    (fam_src / "sub" / "nested.md").parent.mkdir()
    (fam_src / "sub" / "nested.md").write_text("nested\n")

    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    adapter = SkillPatcherLLMAdapter()
    out = await adapter.initialize_state(
        family_key="family_a",
        spec=_spec(),
        backend=backend,
        state_uri=seed_uri,
        params={
            "init_from_skill_method": "human_authored",
            "upstream_root": str(upstream_root),
        },
    )
    assert out == seed_uri
    dst = Path(out)
    assert (dst / "seed.md").read_text() == "# seed skill\n"
    assert (dst / "sub" / "nested.md").read_text() == "nested\n"


@pytest.mark.asyncio
async def test_initialize_state_without_upstream_root_raises(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(ValueError, match="upstream_root"):
        await adapter.initialize_state(
            family_key="family_a",
            spec=_spec(),
            backend=backend,
            state_uri=seed_uri,
            params={"init_from_skill_method": "human_authored"},
        )


@pytest.mark.asyncio
async def test_initialize_state_missing_upstream_family_raises(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="missing", params={},
    )
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(FileNotFoundError):
        await adapter.initialize_state(
            family_key="missing",
            spec=_spec(),
            backend=backend,
            state_uri=seed_uri,
            params={
                "init_from_skill_method": "human_authored",
                "upstream_root": str(tmp_path / "no-such-tree"),
            },
        )


# ─── evolve happy paths ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_evolve_applies_add_modify_delete_patch(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    # Pre-populate the state dir with a file that gets modified + a
    # file that gets deleted.
    Path(seed_uri, "existing.md").write_text("v1\n")
    Path(seed_uri, "old.md").write_text("stale\n")

    gateway = _FakeGateway(response=_fake_response({
        "add": [{"path": "brand_new.md", "content": "brand new\n"}],
        "modify": [{"path": "existing.md", "content": "v2\n"}],
        "delete": ["old.md"],
    }))
    adapter = SkillPatcherLLMAdapter()
    out = await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={
            "gateway": gateway,
            "model": "anthropic/claude-sonnet-4-6",
        },
    )
    assert out == seed_uri
    assert len(gateway.calls) == 1
    call = gateway.calls[0]
    assert call["dialect"] == "family_evolver"
    assert call["model"] == "anthropic/claude-sonnet-4-6"

    dst = Path(out)
    assert (dst / "brand_new.md").read_text() == "brand new\n"
    assert (dst / "existing.md").read_text() == "v2\n"
    assert not (dst / "old.md").exists()


@pytest.mark.asyncio
async def test_evolve_forwards_provider_connection_id_from_params(tmp_path):
    """#672 blocker #695: when ``params['provider_connection_id']`` is
    set on the batch's adapter spec, the adapter MUST hand it to the
    gateway client so the evolver call is routed via the caller's BYO
    upstream credential."""
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={
            "gateway": gateway,
            "provider_connection_id": "78964dda-638b-4ca1-ae19-6355d35e826c",
        },
    )
    assert gateway.calls[0]["provider_connection_id"] == (
        "78964dda-638b-4ca1-ae19-6355d35e826c"
    )


@pytest.mark.asyncio
async def test_evolve_omits_provider_connection_id_when_absent(tmp_path):
    """Default path: no BYO connection ⇒ pass ``None`` through so the
    gateway falls back to the platform-credentialed legacy route."""
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={"gateway": gateway},
    )
    assert gateway.calls[0]["provider_connection_id"] is None


@pytest.mark.asyncio
async def test_evolve_empty_patch_is_idempotent(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    Path(seed_uri, "keep.md").write_text("keep\n")
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    out = await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={"gateway": gateway},
    )
    assert out == seed_uri
    # Empty patch skips the upload entirely - the caller's state_uri
    # is still valid without touching the store.
    assert backend.upload_calls == []
    assert Path(out, "keep.md").read_text() == "keep\n"


@pytest.mark.asyncio
async def test_evolve_model_precedence_uses_params_over_settings(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={
            "gateway": gateway,
            "model": "anthropic/claude-opus-4-6",
            "settings_default_model": "anthropic/claude-haiku-4-6",
        },
    )
    assert gateway.calls[0]["model"] == "anthropic/claude-opus-4-6"


@pytest.mark.asyncio
async def test_evolve_falls_back_to_settings_default_model(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={
            "gateway": gateway,
            "settings_default_model": "anthropic/claude-haiku-4-6",
        },
    )
    assert gateway.calls[0]["model"] == "anthropic/claude-haiku-4-6"


@pytest.mark.asyncio
async def test_evolve_falls_back_to_framework_default_model(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={"gateway": gateway},
    )
    assert gateway.calls[0]["model"] == "anthropic/claude-sonnet-4-6"


# ─── evolve trajectory compaction ──────────────────────────────────


@pytest.mark.asyncio
async def test_evolve_with_trajectory_file_includes_events_in_prompt(tmp_path):
    from uuid import uuid4 as _u
    trial_id = _u()
    trajectory_path = tmp_path / "traj.jsonl"
    # Two step_start events + a thought + env_exec.
    events = [
        {
            "kind": "step_start",
            "emitted_at": "2026-01-01T00:00:00Z",
            "trial_id": str(trial_id),
            "step_id": "s1",
            "seq": 0,
            "instruction_excerpt": "do the thing",
        },
        {
            "kind": "agent_thought",
            "emitted_at": "2026-01-01T00:00:01Z",
            "trial_id": str(trial_id),
            "step_id": "s1",
            "seq": 1,
            "content": "I should try X",
        },
        {
            "kind": "env_exec",
            "emitted_at": "2026-01-01T00:00:02Z",
            "trial_id": str(trial_id),
            "step_id": "s1",
            "seq": 2,
            "cmd": "ls -la",
            "user": None,
            "cwd": None,
            "return_code": 0,
            "stdout_bytes": 100,
            "stderr_bytes": 0,
            "truncated": False,
            "duration_sec": 0.1,
        },
    ]
    with trajectory_path.open("w") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")

    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(id=trial_id),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={
            "gateway": gateway,
            "trajectory_uri": str(trajectory_path),
        },
    )
    prompt = gateway.calls[0]["messages"][1]["content"]
    assert "do the thing" in prompt
    assert "I should try X" in prompt
    assert "ls -la" in prompt


@pytest.mark.asyncio
async def test_evolve_step_cap_truncates_trajectory(tmp_path):
    from uuid import uuid4 as _u
    trial_id = _u()
    trajectory_path = tmp_path / "traj.jsonl"
    events = []
    for i in range(30):
        events.append({
            "kind": "step_start",
            "emitted_at": "2026-01-01T00:00:00Z",
            "trial_id": str(trial_id),
            "step_id": f"s{i}",
            "seq": i,
            "instruction_excerpt": f"step {i}",
        })
    with trajectory_path.open("w") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")

    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [], "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    await adapter.evolve(
        trial=_Trial(id=trial_id),
        family=_Family(),
        state_uri=seed_uri,
        backend=backend,
        params={
            "gateway": gateway,
            "trajectory_uri": str(trajectory_path),
            "max_steps": 5,
        },
    )
    prompt = gateway.calls[0]["messages"][1]["content"]
    assert "step 0" in prompt
    assert "step 4" in prompt
    # Step 6 should not appear (5-step cap).
    assert "step 20" not in prompt


# ─── evolve rejection paths ────────────────────────────────────────


def test_parse_patch_strips_markdown_json_fence() -> None:
    """Live smoke on staging: Claude Haiku 4.5 wraps the patch JSON in
    a ``json ... ``` fence by default. Regression so the parser
    tolerates fenced output instead of crashing every family_evolver
    round trip.
    """
    raw = "```json\n{\n  \"add\": [],\n  \"modify\": [],\n  \"delete\": []\n}\n```"
    patch = _parse_patch(raw)
    assert list(patch.add) == []
    assert list(patch.modify) == []
    assert list(patch.delete) == []


def test_parse_patch_strips_bare_triple_backticks_fence() -> None:
    raw = "```\n{\n  \"add\": [],\n  \"modify\": [],\n  \"delete\": []\n}\n```"
    patch = _parse_patch(raw)
    assert list(patch.add) == []


def test_parse_patch_still_rejects_true_junk() -> None:
    with pytest.raises(PatchValidationError, match="non-JSON"):
        _parse_patch("not JSON at all {{{")


@pytest.mark.asyncio
async def test_evolve_rejects_malformed_json(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response={
        "choices": [{"message": {"content": "not json {{{"}}],
    })
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(PatchValidationError, match="non-JSON"):
        await adapter.evolve(
            trial=_Trial(),
            family=_Family(),
            state_uri=seed_uri,
            backend=backend,
            params={"gateway": gateway},
        )


@pytest.mark.asyncio
async def test_evolve_rejects_missing_keys(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [],
        # missing "modify" and "delete"
    }))
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(PatchValidationError, match="missing required keys"):
        await adapter.evolve(
            trial=_Trial(),
            family=_Family(),
            state_uri=seed_uri,
            backend=backend,
            params={"gateway": gateway},
        )


@pytest.mark.asyncio
async def test_evolve_rejects_absolute_path(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [{"path": "/etc/passwd", "content": "bad"}],
        "modify": [],
        "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(PatchValidationError, match="must be relative"):
        await adapter.evolve(
            trial=_Trial(),
            family=_Family(),
            state_uri=seed_uri,
            backend=backend,
            params={"gateway": gateway},
        )


@pytest.mark.asyncio
async def test_evolve_rejects_dotdot_traversal(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [],
        "modify": [{"path": "../escaped.md", "content": "no"}],
        "delete": [],
    }))
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(PatchValidationError, match=r"'\.\.'"):
        await adapter.evolve(
            trial=_Trial(),
            family=_Family(),
            state_uri=seed_uri,
            backend=backend,
            params={"gateway": gateway},
        )


@pytest.mark.asyncio
async def test_evolve_rejects_delete_dotdot(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    gateway = _FakeGateway(response=_fake_response({
        "add": [], "modify": [],
        "delete": ["../../secret"],
    }))
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(PatchValidationError):
        await adapter.evolve(
            trial=_Trial(),
            family=_Family(),
            state_uri=seed_uri,
            backend=backend,
            params={"gateway": gateway},
        )


@pytest.mark.asyncio
async def test_evolve_without_gateway_param_raises(tmp_path):
    backend = _FakeBackend(root=tmp_path / "state")
    seed_uri = await backend.initialize(
        batch_id=uuid4(), family_key="family_a", params={},
    )
    adapter = SkillPatcherLLMAdapter()
    with pytest.raises(ValueError, match="gateway"):
        await adapter.evolve(
            trial=_Trial(),
            family=_Family(),
            state_uri=seed_uri,
            backend=backend,
            params={},
        )


# ─── pure-function tests ───────────────────────────────────────────


def test_parse_patch_accepts_all_empty():
    patch = _parse_patch(json.dumps({"add": [], "modify": [], "delete": []}))
    assert patch.is_empty


def test_parse_patch_rejects_non_object():
    with pytest.raises(PatchValidationError, match="not an object"):
        _parse_patch(json.dumps([1, 2, 3]))


def test_parse_patch_rejects_non_string_delete():
    with pytest.raises(PatchValidationError, match="must be a string path"):
        _parse_patch(json.dumps({"add": [], "modify": [], "delete": [1]}))


def test_validate_relative_path_accepts_normal():
    _validate_relative_path("add", "sub/dir/file.md")


def test_validate_relative_path_rejects_empty():
    with pytest.raises(PatchValidationError, match="non-empty"):
        _validate_relative_path("add", "")


# ─── entry-point registration ─────────────────────────────────────


def test_skill_patcher_llm_is_registered_entry_point():
    from loom.family_run.registry import resolve_plugin
    from loom.family_run.spec import PluginRef

    plugin = resolve_plugin(
        "loom.family.adapters", PluginRef(name="skill_patcher_llm"),
    )
    assert isinstance(plugin, SkillPatcherLLMAdapter)
