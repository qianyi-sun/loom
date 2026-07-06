from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "alignment" / "skilllearnbench_effective_params.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "skilllearnbench_effective_params",
        SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_codex_template_extra_flags_not_consumed_aligns_by_defaults() -> None:
    mod = _load_module()

    evidence = mod.build_evidence(
        {
            "run_id": "runtime-clean-42",
            "tasks": [
                {
                    "task_id": "skilllearnbench/poster/poster-2",
                    "agent_id": "codex",
                    "template_id": "codex-no-extra-flags",
                    "model": "qwen3.6-35b-a3b",
                    "computed_extra_flags": "--settings '{\"temperature\":0}'",
                    "command_template": "codex run --model {model} --task {task_dir}",
                    "rendered_command": (
                        "codex run --model qwen3.6-35b-a3b --task /tmp/task "
                        "--prompt 'SECRET_PROMPT with sk-live-token'"
                    ),
                },
            ],
        },
        {
            "trials": [
                {
                    "task_id": "skilllearnbench/poster/poster-2",
                    "trial_id": "trial-1",
                    "trial_config": {"request_params": {}},
                    "provider": {
                        "request_params_status_counts": {"available": 1},
                        "request_params": [
                            {
                                "llm_call_id": "call-1",
                                "status": "available",
                                "parameters": {},
                            },
                        ],
                    },
                },
            ],
        },
    )

    official = evidence["official_tasks"][0]
    assert official["template_consumed_extra_flags"] is False
    assert official["effective_parameter_conclusion"] == (
        "provider_defaults_extra_flags_not_consumed"
    )
    assert official["official_effective_request_params"] == {
        "status": "available",
        "parameters": {},
    }
    assert official["redacted_rendered_command"] == [
        "codex",
        "run",
        "--model",
        "qwen3.6-35b-a3b",
        "--task",
        "<path>",
        "--prompt",
        "<omitted>",
    ]
    assert evidence["comparisons"][0]["alignment_classification"] == (
        "aligned_by_provider_defaults"
    )
    serialized = json.dumps(evidence, sort_keys=True)
    assert "SECRET_PROMPT" not in serialized
    assert "sk-live-token" not in serialized


def test_consumed_settings_mismatch_against_loom_defaults() -> None:
    mod = _load_module()

    evidence = mod.build_evidence(
        {
            "tasks": [
                {
                    "task_id": "skilllearnbench/poster/poster-3",
                    "agent_id": "codex",
                    "template_id": "codex-extra-flags",
                    "model": "qwen3.6-35b-a3b",
                    "computed_extra_flags": "--settings '{\"temperature\":0}'",
                    "command_template": (
                        "codex run --model {model} {extra_flags} --task {task_dir}"
                    ),
                    "rendered_command": (
                        "codex run --model qwen3.6-35b-a3b "
                        "--settings '{\"temperature\":0}' --task /tmp/task"
                    ),
                },
            ],
        },
        {
            "trials": [
                {
                    "task_id": "skilllearnbench/poster/poster-3",
                    "trial_id": "trial-2",
                    "trial_config": {"request_params": {}},
                    "provider": {
                        "request_params_status_counts": {"available": 1},
                        "request_params": [
                            {
                                "llm_call_id": "call-2",
                                "status": "available",
                                "parameters": {},
                            },
                        ],
                    },
                },
            ],
        },
    )

    official = evidence["official_tasks"][0]
    assert official["template_consumed_extra_flags"] is True
    assert official["official_effective_request_params"] == {
        "status": "available",
        "parameters": {"temperature": 0},
    }
    assert official["effective_parameter_conclusion"] == "explicit_template_params"
    assert evidence["comparisons"][0]["alignment_classification"] == (
        "official_explicit_params_missing_in_loom"
    )


def test_cli_writes_redacted_json_and_markdown(tmp_path: Path) -> None:
    mod = _load_module()
    official_path = tmp_path / "official.json"
    loom_path = tmp_path / "loom.json"
    out_json = tmp_path / "effective-params.json"
    out_md = tmp_path / "effective-params.md"
    official_path.write_text(
        json.dumps(
            {
                "tasks": [
                    {
                        "task_id": "skilllearnbench/github/github-5",
                        "agent_id": "codex",
                        "template_id": "codex-no-extra-flags",
                        "model": "qwen3.6-35b-a3b",
                        "computed_extra_flags": "--settings '{\"temperature\":0}'",
                        "command_template": "codex run --model {model}",
                        "rendered_command": (
                            "codex run --model qwen3.6-35b-a3b "
                            "--env GITHUB_TOKEN=ghp_secret --task /tmp/private"
                        ),
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    loom_path.write_text(
        json.dumps(
            {
                "trials": [
                    {
                        "task_id": "skilllearnbench/github/github-5",
                        "trial_id": "trial-3",
                        "trial_config": {
                            "request_params": {
                                "messages": [{"role": "user", "content": "private"}],
                                "temperature": 0.0,
                                "api_key": "sk-secret",
                            },
                        },
                        "provider": {
                            "request_params_status_counts": {"available": 1},
                            "request_params": [
                                {
                                    "llm_call_id": "call-3",
                                    "status": "available",
                                    "parameters": {"temperature": 0.0},
                                },
                            ],
                        },
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    assert (
        mod.main(
            [
                "--official-plan-json",
                str(official_path),
                "--loom-debug-json",
                str(loom_path),
                "--out-json",
                str(out_json),
                "--out-md",
                str(out_md),
            ],
        )
        == 0
    )

    rendered = out_json.read_text(encoding="utf-8") + out_md.read_text(encoding="utf-8")
    assert "loom_params_not_in_official" in rendered
    assert "GITHUB_TOKEN" not in rendered
    assert "ghp_secret" not in rendered
    assert "sk-secret" not in rendered
    assert "private" not in rendered
