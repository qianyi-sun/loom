from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_operator_runbook_uses_current_provider_model_syntax() -> None:
    runbook = _read("docs/operator-runbook.md")

    assert "loom providers models refresh" not in runbook
    assert "loom providers models list" not in runbook
    assert "loom providers models smoke-openai --refresh" in runbook
    assert "loom providers models smoke-openai" in runbook


def test_operator_runbook_staging_batch_smoke_matches_cli_contract() -> None:
    runbook = _read("docs/operator-runbook.md")

    assert "--benchmark hello-world" not in runbook
    assert "--provider smoke-openai --model gpt-4o-mini --agent oracle" not in runbook
    assert "--task-filter '{\"task_ids\":[\"hello-world\"]}'" in runbook
    assert "--provider smoke-openai --model gpt-4o-mini --agent litellm" in runbook


def test_cluster_deploy_docs_do_not_advertise_missing_trial_download_commands() -> None:
    cluster_deploy = _read("docs/architecture/cluster-deploy.md")

    assert "loom eval run --provider N --model M --agent A --benchmark B" not in cluster_deploy
    assert "loom eval run --provider N --model M --agent A --task ID" in cluster_deploy
    assert "loom eval trial {list,show} | trajectory ID | atif ID" not in cluster_deploy
    assert "loom eval trial {list,show}" in cluster_deploy
    assert "loom eval trial show TRIAL_ID" in cluster_deploy
