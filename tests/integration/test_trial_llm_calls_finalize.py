"""A11.1 regression: Trial.run() queries TrialContext.llm_calls_fetcher
at finalize and appends each row as an LLMCallEvent on the local
trajectory JSONL BEFORE the ATIF projection runs.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

from loom.agent.oracle import OracleAgent
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.verifier import VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom.trial.trial import Trial, TrialContext
from tests._trial_config_defaults import stub_trial_config


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


async def test_trial_appends_llm_calls_before_finalize(tmp_path: Path) -> None:
    """Seed two llm_calls rows via a fetcher; assert the local JSONL has
    them as `kind=llm_call` events alongside the agent's normal events."""

    # Fixture task: hello-world style.
    sol = tmp_path / "task" / "solution"
    sol.mkdir(parents=True)
    (sol / "solve.sh").write_text("#!/bin/sh\necho hello\n")
    (sol / "solve.sh").chmod(0o755)
    (sol.parent / "task.toml").write_text('schema_version = "1"\n')

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })
    trial_id = uuid4()

    captured_calls: list[UUID] = []

    async def fetcher(tid: UUID) -> list[dict]:
        captured_calls.append(tid)
        return [
            {
                "captured_at": "2026-06-07T00:00:00Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "claude-opus-4-7",
                "dialect": "anthropic",
                "input_tokens": 100,
                "output_tokens": 50,
                "provider_extras": {"cache_read_input_tokens": 20},
                "cost_usd": 0.001,
                "rate_card_hash": "abc",
            },
            {
                "captured_at": "2026-06-07T00:00:01Z",
                "trial_id": str(tid),
                "step_id": "main",
                "model": "claude-opus-4-7",
                "dialect": "anthropic",
                "input_tokens": 80,
                "output_tokens": 40,
                "provider_extras": {},
                "cost_usd": 0.0008,
                "rate_card_hash": "abc",
            },
        ]

    store = FakeObjectStore()
    local_path = tmp_path / "trajectory.jsonl"
    ctx = TrialContext(
        trial_id=trial_id, team_id=uuid4(),
        task_config=task, task_checksum="0" * 64,
        task_dir=sol.parent,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=sol.parent, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),  # type: ignore[arg-type]
        object_store=store,
        local_trajectory_path=local_path,
        llm_calls_fetcher=fetcher,
    )

    trial = Trial(ctx=ctx)
    result = await trial.run()
    assert result.state.value == "succeeded"

    # The fetcher was called with the trial_id.
    assert captured_calls == [trial_id]

    # The local JSONL has both llm_call events appended.
    lines = local_path.read_text().splitlines()
    llm_call_lines = [
        line for line in lines
        if json.loads(line).get("kind") == "llm_call"
    ]
    assert len(llm_call_lines) == 2
    parsed = [json.loads(line) for line in llm_call_lines]
    assert parsed[0]["input_tokens"] == 100
    assert parsed[1]["input_tokens"] == 80
    # Dialect is encoded as ModelSpec.provider on the synthetic event.
    assert parsed[0]["model"]["provider"] == "anthropic"
    assert parsed[0]["cached_input_tokens"] == 20
    assert parsed[0]["cost_usd_snapshot"] == 0.001


async def test_trial_skips_fetcher_when_none(tmp_path: Path) -> None:
    """Backwards-compat: a context without llm_calls_fetcher (v0.7 path)
    finalizes normally with no extra events injected."""
    sol = tmp_path / "task" / "solution"
    sol.mkdir(parents=True)
    (sol / "solve.sh").write_text("#!/bin/sh\necho hello\n")
    (sol / "solve.sh").chmod(0o755)
    (sol.parent / "task.toml").write_text('schema_version = "1"\n')

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })

    trial_id = uuid4()
    local_path = tmp_path / "trajectory.jsonl"
    ctx = TrialContext(
        trial_id=trial_id, team_id=uuid4(),
        task_config=task, task_checksum="0" * 64,
        task_dir=sol.parent,
        trial_config=stub_trial_config(),
        driver=FakeDriver(exec_handler=handler),
        agent=OracleAgent(task_dir=sol.parent, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),  # type: ignore[arg-type]
        object_store=FakeObjectStore(),
        local_trajectory_path=local_path,
        # No llm_calls_fetcher.
    )

    trial = Trial(ctx=ctx)
    result = await trial.run()
    assert result.state.value == "succeeded"

    # JSONL has no llm_call events.
    lines = local_path.read_text().splitlines()
    assert all(
        json.loads(line).get("kind") != "llm_call" for line in lines
    )
