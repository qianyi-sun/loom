from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_clients_and_sudoers_fix_broker_commands_and_clean_environment() -> None:
    client = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout").read_text()
    compatibility_broker = (
        REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout-broker"
    ).read_text()
    broker = (REPO_ROOT / "deploy/staging-rollout/loom-rollout-broker").read_text()
    sudoers = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout.sudoers").read_text()
    tmpfiles = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout.tmpfiles").read_text()
    sysctl = (REPO_ROOT / "deploy/staging-rollout/loom-staging-rollout.sysctl").read_text()

    assert "sudo -n -u loom-rollout -- /usr/local/libexec/loom-staging-rollout-broker" in client
    assert "%loom-staging-operators ALL=(loom-rollout) NOPASSWD:NOSETENV:" in sudoers
    assert "/usr/local/libexec/loom-staging-rollout-broker *" in sudoers
    assert compatibility_broker == (
        '#!/bin/sh\nset -eu\n\nexec /usr/local/libexec/loom-rollout-broker --env staging "$@"\n'
    )
    assert "/usr/bin/env -i" in broker
    assert broker.index("umask 077") < broker.index("exec /usr/bin/env -i")
    assert 'SUDO_USER="${SUDO_USER}"' in broker
    python_command = "__CANDIDATE_VENV__/bin/python -I -B -m loom_cli.rollout.operator.broker"
    assert python_command in broker
    for setting in (
        "GIT_CONFIG_NOSYSTEM=1",
        "GIT_CONFIG_GLOBAL=/dev/null",
        "GIT_TERMINAL_PROMPT=0",
    ):
        assert (
            broker.index("exec /usr/bin/env -i")
            < broker.index(setting)
            < broker.index(python_command)
        )
    assert "PYTHONPATH" not in broker
    assert tmpfiles.strip() == "d /run/loom-staging-rollout 0700 loom-rollout loom-rollout -"
    assert sysctl == "fs.inotify.max_user_instances = 1024\n"


def test_isolated_broker_interpreter_ignores_caller_working_directory(tmp_path: Path) -> None:
    shadow = tmp_path / "loom_cli"
    shadow.mkdir()
    marker = tmp_path / "shadow-executed"
    (shadow / "__init__.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            "import pathlib, loom_cli; print(pathlib.Path(loom_cli.__file__).resolve())",
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(tmp_path)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert str(shadow) not in result.stdout
    assert not marker.exists()


def test_config_template_is_non_secret_and_fixed_to_merged_dev() -> None:
    path = REPO_ROOT / "deploy/staging-rollout/staging-rollout.toml"
    config = tomllib.loads(path.read_text(encoding="utf-8"))

    assert config["remote_url"] == "https://github.com/qianyi-sun/loom.git"
    assert config["target_ref"] == "refs/heads/dev"
    assert config["service_user"] == "loom-rollout"
    assert config["runner_repo"] == ("/opt/loom-staging-runner/candidates/__SOURCE_SHA__/repo")
    assert config["cluster_config_path"] == (
        "/opt/loom-staging-runner/candidates/__SOURCE_SHA__/repo/"
        "deploy/environments/staging.multinode.cluster.toml"
    )
    assert config["expect_admin_token_fingerprint"] == "__ADMIN_TOKEN_FINGERPRINT__"
    assert config["smoke_on_behalf_team_id"] == "__SMOKE_ON_BEHALF_TEAM_ID__"
    assert config["backup_max_objects"] == 1_000_000
    assert config["backup_max_entries"] == 16_000_000
    protected_root = "/shared_work/loom/staging-rollout/credentials"
    assert config["admin_token_source"] == f"file:{protected_root}/staging-admin-token"
    assert config["worker_token_source"] == f"file:{protected_root}/staging-worker-token"
    assert config["service_token_source"] == f"file:{protected_root}/staging-service-token"
    assert "loom_admin_" not in path.read_text(encoding="utf-8")


def test_repo_configs_no_longer_reference_qianyi_private_deploy_key() -> None:
    files = [
        REPO_ROOT / "deploy/environments/staging.multinode.cluster.toml",
        REPO_ROOT / "deploy/worker-pools/gb10/ssh_config",
    ]
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "staging-gb10-rollout-ed25519" not in text
        assert "/var/lib/loom-staging-rollout/gb10-deploy-ed25519" in text

    env_state = (REPO_ROOT / "deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    assert "/shared_work/qianyi/loom-worker-capacity/staging-gb10-worker-" not in env_state
    assert "/shared_work2/loom-staging-rollout/worker-envs/staging-gb10-worker-" in env_state
    assert (
        'env_file = "/shared_work/loom/staging-rollout/credentials/'
        'staging-catalog-provisioning.env"'
    ) in env_state


def test_installed_gb10_trust_tool_uses_the_root_owned_candidate_inventory() -> None:
    source = (REPO_ROOT / "scripts/ops/staging_rollout_gb10_trust.py").read_text(encoding="utf-8")

    assert "_CANDIDATE_SSH_CONFIG_RE" in source
    assert "/opt/loom-staging-runner/candidates/" in source
    assert "/opt/loom-staging-runner/repo/deploy/worker-pools/gb10/ssh_config" not in source
    assert "Path(__file__).resolve().parents[2]" not in source


def test_gb10_ssh_authority_is_strict_and_repo_owned() -> None:
    ssh_config = (REPO_ROOT / "deploy/worker-pools/gb10/ssh_config").read_text(encoding="utf-8")
    known_hosts = (REPO_ROOT / "deploy/worker-pools/gb10/known_hosts").read_text(encoding="ascii")

    assert "StrictHostKeyChecking yes" in ssh_config
    assert "UserKnownHostsFile /etc/loom/staging-rollout-gb10-known-hosts" in ssh_config
    assert "GlobalKnownHostsFile /dev/null" in ssh_config
    assert "UpdateHostKeys no" in ssh_config
    assert "accept-new" not in ssh_config
    entries = [line for line in known_hosts.splitlines() if line and not line.startswith("#")]
    assert len(entries) == 15
    assert all(" ssh-ed25519 " in entry for entry in entries)


def test_privileged_runner_paths_use_full_ci_without_owner_review() -> None:
    assert not (REPO_ROOT / ".github/CODEOWNERS").exists()
    plan = plan_validations(
        changed_paths=["deploy/staging-rollout/loom-staging-rollout.sudoers"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert plan.unowned_runtime is False
    assert all("protected-staging-rollout" in plan.reasons[name] for name in HEAVY_CHECKS)
