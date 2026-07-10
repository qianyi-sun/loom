from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_operator_runbook_uses_current_provider_model_syntax() -> None:
    runbook = _read("docs/runbooks/operator-runbook.md")

    assert "loom providers models refresh" not in runbook
    assert "loom providers models list" not in runbook
    assert "loom providers models mz_tn_canada_qianyi --refresh" in runbook
    assert "loom providers models mz_tn_canada_qianyi --preflight glm-5.1-thinking" in runbook


def test_operator_runbook_staging_batch_smoke_matches_cli_contract() -> None:
    runbook = _read("docs/runbooks/operator-runbook.md")

    assert "--benchmark hello-world" not in runbook
    assert "--provider smoke-openai --model gpt-4o-mini --agent oracle" not in runbook
    assert '--task-filter \'{"task_ids":["loom-smoke/gb10-oracle-hello-world"]}\'' in runbook
    assert "--provider mz_tn_canada_qianyi" in runbook
    assert "--model glm-5.1-thinking" in runbook
    assert "--agent opencode" in runbook
    assert "--required-worker-pool gb10-arm64" in runbook


def test_operator_runbook_staging_gate_matches_current_launch_scope() -> None:
    runbook = _read("docs/runbooks/operator-runbook.md")
    gate_section = runbook.split("## Staging smoke gate", maxsplit=1)[1].split(
        "## Capacity planning",
        maxsplit=1,
    )[0]

    assert "scripts/staging_smoke_gate.py" in gate_section
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


def test_taskset_fence_canary_uses_task_7_deployment_runner() -> None:
    runbook = _read("docs/runbooks/operator-runbook.md")
    canary_section = runbook.split(
        "### Disposable TaskSet lease-fencing canary (#756)",
        maxsplit=1,
    )[1].split("### Protected workload-trust contract (#755)", maxsplit=1)[0]
    normalized_canary_section = " ".join(canary_section.split())

    assert "Task 6 tests are not staging proof" in normalized_canary_section
    assert (
        "Task 7 supplies the deployment-side, authorization-restricted cooperative "
        "runner. It runs only through `loom cluster taskset-fence-canary`"
    ) in normalized_canary_section
    assert "taskset-fence-canary-token" in canary_section
    assert "evidence.json" in canary_section
    assert '--rollout-dir "$ROLLOUT_DIR"' in canary_section
    assert "--task-set-id" not in canary_section
    assert "--expected-task-checksum" not in canary_section
    assert "candidate-bound JSON" in normalized_canary_section
    assert "durable one-use authorization" in normalized_canary_section
    assert "fixed `loom-system-taskset-fence-canary`" in canary_section
    assert "Migration `0065` reserves this Team" in canary_section
    assert "never accepts a TaskSet id or checksum" in normalized_canary_section
    assert "fixed staging Kubernetes context" in normalized_canary_section
    assert "atomically published without replacement" in normalized_canary_section

    for prohibited_action in [
        "killing a driver or pod",
        "SIGSTOP",
        "manual SQL",
        "mutating the object store",
        "injecting a failure",
        "deleting a prefix",
    ]:
        assert prohibited_action in canary_section


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
        "loom eval run --provider N --model M --agent A --task ID\n    [--backend B] [--name N]"
    ) not in cluster_deploy
    assert "loom eval batch create" in cluster_deploy
    assert "[--name N | --name-suffix S] [--benchmark B | --task-filter JSON]" in cluster_deploy
    assert "[--n-per-task N] [--backend B]" in cluster_deploy
