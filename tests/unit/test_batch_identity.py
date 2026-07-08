from __future__ import annotations

from loom_service.batch_identity import build_batch_identity


def test_batch_identity_compacts_benchmarks_models_and_suffix() -> None:
    identity = build_batch_identity(
        task_filter={
            "benchmark_ids": ["humaneval", "mbpp", "aime-24", "gpqa"],
            "subset_kind": "random_n",
            "n": 5,
            "seed": 17,
        },
        trial_config={},
        combinations=[
            {
                "agent_name": "litellm",
                "agent_model": {
                    "provider": "openai",
                    "name": "openai/gpt-4o-mini",
                },
                "n_per_task": 2,
                "label": "",
            },
            {
                "agent_name": "codex",
                "agent_model": {
                    "provider": "yibuapi",
                    "name": "models/qwen3.6-35b-a3b",
                },
                "n_per_task": 1,
                "label": "paper table",
            },
            {
                "agent_name": "oracle",
                "agent_model": None,
                "n_per_task": 1,
                "label": "",
            },
        ],
        n_per_task=1,
        backend="local",
        suffix=" canary | shard ",
    )

    assert identity.name == (
        "humaneval+mbpp+aime-24+1 random5 | "
        "litellm/gpt-4o-mini x2+paper-table x1+1 - canary - shard"
    )
    assert "Tasks: humaneval, mbpp, aime-24, gpqa" in identity.description
    assert "subset: random 5 seed 17" in identity.description
    assert "litellm/openai/gpt-4o-mini x2" in identity.description
    assert "codex/yibuapi/qwen3.6-35b-a3b x1" in identity.description
    assert "oracle/no-model x1" in identity.description
    assert identity.description.endswith("Backend: local.")
    assert "++" not in identity.name


def test_batch_identity_prefers_combination_provider_model_id() -> None:
    identity = build_batch_identity(
        task_filter={"benchmark_id": "source-useful-frontier-5003"},
        trial_config={},
        combinations=[
            {
                "agent_name": "terminus-2",
                "agent_model": {
                    "provider": "openai",
                    "name": "facade-default",
                },
                "provider_model_id": "glm-5.1-thinking",
                "n_per_task": 1,
                "label": "",
            },
        ],
        n_per_task=1,
        backend="docker",
    )

    assert identity.name == (
        "source-useful-frontier-5003 | terminus-2/glm-5.1-thinking x1"
    )
    assert "terminus-2/openai/glm-5.1-thinking x1" in identity.description


def test_batch_identity_uses_trial_config_when_combinations_are_absent() -> None:
    identity = build_batch_identity(
        task_filter={"benchmark_id": "livecodebench", "subset_kind": "all"},
        trial_config={
            "agent_name": "litellm",
            "agent_model": {"provider": "openai", "name": "/models/o3-mini"},
        },
        combinations=[],
        n_per_task=3,
        backend="kubernetes",
    )

    assert identity.name == "livecodebench | litellm/o3-mini x3"
    assert "subset: all tasks" in identity.description
    assert "litellm/openai/o3-mini x3" in identity.description


def test_batch_identity_describes_task_set_sources() -> None:
    identity = build_batch_identity(
        task_filter={
            "benchmark_ids": ["humaneval"],
            "task_set_ids": ["ts/team-uuid/sample-tasks"],
            "subset_kind": "all",
        },
        trial_config={"agent_name": "litellm"},
        combinations=[],
        n_per_task=1,
        backend="docker",
    )

    assert identity.name.startswith(
        "humaneval+ts/team-uuid/sample-tasks | litellm x1",
    )
    assert "Tasks: humaneval, ts/team-uuid/sample-tasks" in identity.description


def test_batch_identity_describes_tag_filters() -> None:
    identity = build_batch_identity(
        task_filter={
            "benchmark_id": "aime-24",
            "subset_kind": "all",
            "tag_filters": {
                "year": ["2024"],
                "exam": ["I", "II"],
            },
        },
        trial_config={"agent_name": "litellm"},
        combinations=[],
        n_per_task=1,
        backend="docker",
    )

    assert "tags: exam=I|II, year=2024" in identity.description


def test_batch_identity_handles_explicit_tasks_and_no_model_agents() -> None:
    identity = build_batch_identity(
        task_filter={
            "subset_kind": "explicit",
            "task_ids": ["task-1", "task-2"],
        },
        trial_config={},
        combinations=[
            {
                "agent_name": "oracle",
                "agent_model": None,
                "n_per_task": 4,
                "label": None,
            },
        ],
        n_per_task=1,
        backend="local",
        suffix="",
    )

    assert identity.name == "explicit2 | oracle x4"
    assert "2 explicit task id(s)" in identity.description
    assert "oracle/no-model x4" in identity.description


def test_batch_identity_handles_license_and_subset_variants() -> None:
    first = build_batch_identity(
        task_filter={"license": "mit license", "subset_kind": "first_n", "n": 10},
        trial_config={"agent": {"name": "codex", "model": {"name": "gpt-5"}}},
        combinations=[],
        n_per_task=1,
        backend="local",
    )
    last = build_batch_identity(
        task_filter={"benchmark_id": "mmlu-pro", "subset_kind": "last_n", "n": 3},
        trial_config={"agent": {"name": "aider"}},
        combinations=[],
        n_per_task=2,
        backend="batch",
    )
    custom = build_batch_identity(
        task_filter={"subset_kind": "tagged_tasks"},
        trial_config={},
        combinations=[],
        n_per_task=1,
        backend="custom",
        suffix="x" * 80,
    )

    assert first.name == "mit-license first10 | codex/gpt-5 x1"
    assert "license mit license" in first.description
    assert last.name == "mmlu-pro last3 | aider x2"
    assert "last 3" in last.description
    assert custom.name.startswith("custom tagged_tasks | default x1 - ")
    assert len(custom.name.rsplit(" - ", 1)[1]) == 48
