from __future__ import annotations

import tomllib
from pathlib import Path

from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_client_and_sudoers_fix_the_broker_command() -> None:
    client = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout").read_text()
    broker = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout-broker").read_text()
    sudoers = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout.sudoers").read_text()
    tmpfiles = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout.tmpfiles").read_text()

    assert "sudo -n -u loom-rollout -- /usr/local/libexec/loom-staging-rollout-broker" in client
    assert "%loom-staging-operators ALL=(loom-rollout) NOPASSWD:NOSETENV:" in sudoers
    assert "/usr/local/libexec/loom-staging-rollout-broker *" in sudoers
    assert "/usr/bin/env -i" in broker
    assert 'SUDO_USER="${SUDO_USER}"' in broker
    assert "PYTHONPATH" not in broker
    assert tmpfiles.strip() == "d /run/loom-staging-rollout 0700 loom-rollout loom-rollout -"


def test_config_template_is_non_secret_and_fixed_to_merged_dev() -> None:
    path = REPO_ROOT / "deploy/staging-rollout/staging-rollout.toml"
    config = tomllib.loads(path.read_text(encoding="utf-8"))

    assert config["remote_url"] == "https://github.com/qianyi-sun/loom.git"
    assert config["target_ref"] == "refs/heads/dev"
    assert config["service_user"] == "loom-rollout"
    assert config["cluster_config_path"] == (
        "/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml"
    )
    assert config["expect_admin_token_fingerprint"] == "__ADMIN_TOKEN_FINGERPRINT__"
    assert config["smoke_on_behalf_team_id"] == "__SMOKE_ON_BEHALF_TEAM_ID__"
    assert "loom_admin_" not in path.read_text(encoding="utf-8")


def test_repo_configs_no_longer_reference_qianyi_private_deploy_key() -> None:
    files = [
        REPO_ROOT / "deploy/environments/staging.cluster.toml",
        REPO_ROOT / "deploy/worker-pools/gb10/ssh_config",
    ]
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "staging-gb10-rollout-ed25519" not in text
        assert "/var/lib/loom-staging-rollout/gb10-deploy-ed25519" in text

    env_state = (REPO_ROOT / "deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    assert "/shared_work/qianyi/loom-worker-capacity/staging-gb10-worker-" not in env_state
    assert "/var/lib/loom-staging-rollout/generated/staging-gb10-worker-" in env_state
    assert "/shared_work/qianyi/loom-worker-capacity/staging-catalog-provisioning.env" in env_state


def test_installed_gb10_trust_tool_uses_the_root_owned_candidate_inventory() -> None:
    source = (REPO_ROOT / "scripts/ops/staging_rollout_gb10_trust.py").read_text(encoding="utf-8")

    assert "/opt/loom-staging-runner/repo/deploy/worker-pools/gb10/ssh_config" in source
    assert "Path(__file__).resolve().parents[2]" not in source


def test_privileged_runner_paths_have_codeowners_and_full_ci_selection() -> None:
    owners = (REPO_ROOT / ".github/CODEOWNERS").read_text(encoding="utf-8")
    assert "/deploy/staging-rollout/" in owners
    assert "/src/loom_cli/rollout/operator/" in owners
    plan = plan_validations(
        changed_paths=["deploy/staging-rollout/loom-staging-rollout.sudoers"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert plan.unowned_runtime is False
    assert all("protected-staging-rollout" in plan.reasons[name] for name in HEAVY_CHECKS)
