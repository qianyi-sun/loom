"""Unit tests for `loom qa matrix` helpers."""
from __future__ import annotations

from loom_cli import qa_cmd

# ─── _provider_compatible ──────────────────────────────────────────


def test_provider_compatible_wildcard() -> None:
    """Generic agents (litellm, aider) accept any provider."""
    a = {"supported_providers": ["*"]}
    assert qa_cmd._provider_compatible(a, "openai") is True
    assert qa_cmd._provider_compatible(a, "anthropic") is True


def test_provider_compatible_locked() -> None:
    """Provider-locked agents (claude-code, gemini-cli) reject non-matching."""
    a = {"supported_providers": ["anthropic"]}
    assert qa_cmd._provider_compatible(a, "anthropic") is True
    assert qa_cmd._provider_compatible(a, "openai") is False


def test_provider_compatible_empty_treated_as_no_match() -> None:
    """No declared providers = no compat (oracle has none but
    needs_model=False so this branch isn't hit in practice)."""
    a = {"supported_providers": []}
    assert qa_cmd._provider_compatible(a, "openai") is False


# ─── _build_cells_and_combinations ─────────────────────────────────


def _agent(name: str, *, needs_model: bool = True,
           supported: list[str] | None = None) -> dict:
    return {
        "name": name,
        "needs_model": needs_model,
        "supported_providers": supported or ["*"],
    }


def _bench(bid: str) -> dict:
    return {"id": bid}


def test_build_cells_marks_no_task_skipped() -> None:
    cells, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("aider")],
        benchmarks=[_bench("humaneval"), _bench("empty-bench")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    states = {(c.benchmark, c.state) for c in cells}
    assert ("humaneval", "PENDING") in states
    assert ("empty-bench", "SKIPPED") in states
    empty = next(c for c in cells if c.benchmark == "empty-bench")
    assert "no runnable tasks" in (empty.reason or "")
    # aider stays in combinations because at least one cell is PENDING
    assert any(c["agent_name"] == "aider" for c in combos)


def test_build_cells_marks_provider_mismatch_skipped() -> None:
    cells, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("claude-code", supported=["anthropic"])],
        benchmarks=[_bench("humaneval")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    assert len(cells) == 1
    assert cells[0].state == "SKIPPED"
    assert "anthropic" in (cells[0].reason or "")
    # No combinations submitted for an agent with zero PENDING cells.
    assert combos == []


def test_build_cells_no_model_agent_includes_oracle() -> None:
    """needs_model=False agents (oracle) always compat and get a
    null agent_model in the combination."""
    cells, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("oracle", needs_model=False, supported=[])],
        benchmarks=[_bench("humaneval")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    assert len(cells) == 1
    assert cells[0].state == "PENDING"
    assert combos == [{"agent_name": "oracle", "agent_model": None}]


def test_build_cells_model_combination_carries_provider_and_model() -> None:
    _, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("aider")],
        benchmarks=[_bench("humaneval")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    assert combos == [{
        "agent_name": "aider",
        "agent_model": {
            "provider": "openai",
            "name": "gpt-4o-mini",
            "source": "api",
        },
    }]


# ─── _classify_trial ───────────────────────────────────────────────


def test_classify_trial_succeeded_with_reward() -> None:
    trial = {
        "state": "succeeded",
        "result": {"aggregate_reward": 0.75},
    }
    state, reason, reward = qa_cmd._classify_trial(trial)
    assert state == "PASS_PLATFORM"
    assert reason is None
    assert reward == 0.75


def test_classify_trial_succeeded_with_zero_reward_still_passes() -> None:
    """Per the #316 spec: reward=0 with verifier output IS a platform pass."""
    trial = {"state": "succeeded", "result": {"aggregate_reward": 0}}
    state, _, reward = qa_cmd._classify_trial(trial)
    assert state == "PASS_PLATFORM"
    assert reward == 0.0


def test_classify_trial_failed_records_reason() -> None:
    trial = {
        "state": "failed",
        "failure_reason": "agent_error",
        "failure_message": "exited rc=127; stderr: claude: not found",
    }
    state, reason, reward = qa_cmd._classify_trial(trial)
    assert state == "FAIL_PLATFORM"
    assert "agent_error" in (reason or "")
    assert "rc=127" in (reason or "")
    assert reward is None


def test_classify_trial_non_terminal_is_stuck() -> None:
    state, reason, _ = qa_cmd._classify_trial({"state": "running"})
    assert state == "STUCK"
    assert "running" in (reason or "")


def test_classify_trial_oracle_solve_sh_missing_is_skipped() -> None:
    """OracleAgent fails when the task bundle has no `solution/solve.sh`.
    That's a declared (agent, benchmark) capability mismatch, not a
    platform failure — re-classify to SKIPPED so PASS/FAIL counts
    reflect real platform health."""
    trial = {
        "state": "failed",
        "failure_reason": "agent_error",
        "failure_message": (
            "OracleAgent requires /tmp/loom-trial-abc/solution/solve.sh; "
            "not found"
        ),
    }
    state, reason, _ = qa_cmd._classify_trial(trial)
    assert state == "SKIPPED"
    assert "capability mismatch" in (reason or "")


def test_is_capability_mismatch_recognizes_oracle_solve_sh() -> None:
    assert qa_cmd._is_capability_mismatch(
        "OracleAgent requires /tmp/x/solution/solve.sh; not found",
    )


def test_is_capability_mismatch_does_not_match_generic_failures() -> None:
    """Real platform failures (rc=127, ModuleNotFoundError, gateway
    timeout) must NOT be downgraded to SKIPPED."""
    for msg in [
        "aider exited rc=127 on step main",
        "ModuleNotFoundError: openhands_sdk",
        "Loom gateway timeout",
        "verifier_error: test runner crashed",
    ]:
        assert not qa_cmd._is_capability_mismatch(msg), msg


def test_classify_trial_reads_top_level_aggregate_reward() -> None:
    """The real /api/v1/trials list response exposes
    `aggregate_reward` at the TOP level, not nested under `result`.
    The unit test that uses `result.aggregate_reward` covers the
    detail-view fallback; this one covers the live list shape."""
    trial = {
        "state": "succeeded",
        "aggregate_reward": 1.0,
    }
    state, _, reward = qa_cmd._classify_trial(trial)
    assert state == "PASS_PLATFORM"
    assert reward == 1.0


def test_classify_cells_reads_top_level_agent_name_and_derives_benchmark() -> None:
    """Real /api/v1/trials list shape: `agent_name` at top level,
    no `benchmark_id` key — benchmark is derived from `task_id`'s
    first path segment. Bug caught when running the matrix live
    against a real cluster."""
    cells = [
        qa_cmd.MatrixCell(agent="oracle", benchmark="mbpp", state="PENDING"),
    ]
    trials = [
        {
            "id": "t-1",
            "agent_name": "oracle",
            "task_id": "mbpp/100",
            "state": "succeeded",
            "aggregate_reward": 1.0,
        },
    ]
    qa_cmd._classify_cells(cells, trials)
    assert cells[0].state == "PASS_PLATFORM"
    assert cells[0].trial_id == "t-1"
    assert cells[0].reward == 1.0


def test_classify_trial_succeeded_without_reward_is_failure() -> None:
    """If a trial says succeeded but emitted no aggregate_reward, the
    matrix should treat that as a platform failure (verifier didn't
    actually produce a numeric reward)."""
    trial = {"state": "succeeded", "result": {}}
    state, _, reward = qa_cmd._classify_trial(trial)
    assert state == "STUCK"
    assert reward is None


# ─── _classify_cells ───────────────────────────────────────────────


def test_classify_cells_matches_trial_to_cell() -> None:
    cells = [
        qa_cmd.MatrixCell(agent="aider", benchmark="humaneval", state="PENDING"),
        qa_cmd.MatrixCell(agent="aider", benchmark="mbpp", state="PENDING"),
    ]
    trials = [
        {
            "id": "trial-1",
            "config": {"agent_name": "aider"},
            "benchmark_id": "humaneval",
            "state": "succeeded",
            "result": {"aggregate_reward": 1.0},
        },
        {
            "id": "trial-2",
            "config": {"agent_name": "aider"},
            "benchmark_id": "mbpp",
            "state": "failed",
            "failure_reason": "verifier_error",
        },
    ]
    qa_cmd._classify_cells(cells, trials)
    assert cells[0].state == "PASS_PLATFORM"
    assert cells[0].trial_id == "trial-1"
    assert cells[0].reward == 1.0
    assert cells[1].state == "FAIL_PLATFORM"
    assert cells[1].trial_id == "trial-2"
    assert cells[1].failure_reason == "verifier_error"


def test_classify_cells_marks_unmatched_stuck() -> None:
    cells = [
        qa_cmd.MatrixCell(agent="aider", benchmark="humaneval", state="PENDING"),
    ]
    qa_cmd._classify_cells(cells, [])  # no trials at all
    assert cells[0].state == "STUCK"
    assert "no trial" in (cells[0].reason or "")


def test_classify_cells_leaves_already_skipped_alone() -> None:
    """SKIPPED cells from the build step (no task, provider mismatch)
    should not be re-classified by trial matching."""
    cells = [
        qa_cmd.MatrixCell(
            agent="claude-code", benchmark="humaneval",
            state="SKIPPED", reason="provider mismatch",
        ),
    ]
    qa_cmd._classify_cells(cells, [])
    assert cells[0].state == "SKIPPED"
    assert cells[0].reason == "provider mismatch"


# ─── _render_markdown ──────────────────────────────────────────────


def test_render_markdown_includes_summary_and_cells() -> None:
    result = qa_cmd.MatrixResult(
        started_at="2026-06-23T00:00:00+00:00",
        finished_at="2026-06-23T00:05:00+00:00",
        cluster_url="http://cluster",
        provider_connection="qa-relay",
        model="gpt-4o-mini",
        batch_ids=["b-1"],
        cells=[
            qa_cmd.MatrixCell(
                agent="aider", benchmark="humaneval",
                state="PASS_PLATFORM", reward=1.0, trial_id="t-1",
            ),
            qa_cmd.MatrixCell(
                agent="aider", benchmark="mbpp",
                state="FAIL_PLATFORM",
                reason="agent_error: rc=127", failure_reason="agent_error",
                trial_id="t-2",
            ),
            qa_cmd.MatrixCell(
                agent="claude-code", benchmark="humaneval",
                state="SKIPPED", reason="provider mismatch",
            ),
        ],
    )
    md = qa_cmd._render_markdown(result)
    assert "qa-relay" in md
    assert "gpt-4o-mini" in md
    assert "b-1" in md
    # Summary counts
    assert "PASS_PLATFORM: 1" in md
    assert "FAIL_PLATFORM: 1" in md
    assert "SKIPPED: 1" in md
    # Cell rows
    assert "aider" in md
    assert "humaneval" in md
    assert "agent_error: rc=127" in md
    # Failures sort first
    fail_idx = md.index("FAIL_PLATFORM")
    pass_idx = md.index("PASS_PLATFORM", fail_idx)  # after fail block
    assert fail_idx < pass_idx
