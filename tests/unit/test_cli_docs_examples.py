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


def test_operator_runbook_public_beta_gate_matches_current_launch_scope() -> None:
    runbook = _read("docs/operator-runbook.md")
    gate_section = runbook.split("## Staging smoke gate", maxsplit=1)[1].split(
        "## Capacity planning", maxsplit=1,
    )[0]

    assert "scripts/public_beta_smoke_gate.py" in gate_section
    assert "SPA Tasks page" not in gate_section
    assert "quota rejection" not in gate_section.lower()
    assert "rate-limit rejection" not in gate_section.lower()
    assert "My team" in gate_section
    assert "All teams" in gate_section
    assert "owner-team label" in gate_section
    assert "clone config" in gate_section
    assert "reuse artifact" in gate_section
    assert "provenance" in gate_section
    assert "blocked artifact" in gate_section


def test_cluster_deploy_docs_do_not_advertise_missing_trial_download_commands() -> None:
    cluster_deploy = _read("docs/architecture/cluster-deploy.md")

    assert "loom eval run --provider N --model M --agent A --benchmark B" not in cluster_deploy
    assert "loom eval run --provider N --model M --agent A --task ID" in cluster_deploy
    assert "loom eval trial {list,show} | trajectory ID | atif ID" not in cluster_deploy
    assert "loom eval trial {list,show}" in cluster_deploy
    assert "loom eval trial show TRIAL_ID" in cluster_deploy


def test_cluster_deploy_eval_run_example_matches_supported_options() -> None:
    cluster_deploy = _read("docs/architecture/cluster-deploy.md")

    assert (
        "loom eval run --provider N --model M --agent A --task ID\n"
        "    [--backend B] [--name N]"
    ) not in cluster_deploy
    assert "loom eval batch create" in cluster_deploy
    assert "[--benchmark B | --task-filter JSON] [--n-per-task N] [--backend B]" in cluster_deploy
