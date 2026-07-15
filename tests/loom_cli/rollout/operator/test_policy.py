from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator import policy
from loom_cli.rollout.operator.config import OperatorConfig

SERVICE_UID = 1900


def make_config() -> OperatorConfig:
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=Path("/opt/loom-staging-runner/repo"),
        state_root=Path("/var/lib/loom-staging-rollout"),
        runtime_root=Path("/run/loom-staging-rollout"),
        rollout_root=Path("/data/loom-staging"),
        kubeconfig_path=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        cluster_config_path=Path(
            "/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml"
        ),
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=Path("/etc/loom/staging-rollout.toml"),
        config_sha256="1" * 64,
    )


@pytest.fixture
def passwd(monkeypatch: pytest.MonkeyPatch) -> None:
    entries = {
        "loom-rollout": SimpleNamespace(pw_name="loom-rollout", pw_uid=SERVICE_UID),
        "hongjian": SimpleNamespace(pw_name="hongjian", pw_uid=2002),
        "root": SimpleNamespace(pw_name="root", pw_uid=0),
    }

    def getpwnam(username: str) -> SimpleNamespace:
        if username not in entries:
            raise KeyError(username)
        return entries[username]

    monkeypatch.setattr(policy.pwd, "getpwnam", getpwnam)


def approved_groups(_username: str) -> set[str]:
    return {"loom-staging-operators"}


def test_caller_comes_only_from_verified_sudo_identity(passwd: None) -> None:
    identity = policy.caller_from_sudo(
        config=make_config(),
        environ={
            "SUDO_USER": "hongjian",
            "SUDO_UID": "2002",
            "SUDO_GID": "2002",
            "ACTOR": "qianyi",
            "USER": "qianyi",
            "LOOM_ACTOR": "qianyi",
        },
        euid=SERVICE_UID,
        groups=approved_groups,
    )

    assert identity.username == "hongjian"
    assert identity.uid == 2002


@pytest.mark.parametrize(
    "environ",
    [
        {"SUDO_UID": "2002"},
        {"SUDO_USER": "hongjian"},
        {"SUDO_USER": "", "SUDO_UID": "2002"},
    ],
)
def test_caller_rejects_missing_sudo_metadata(
    passwd: None,
    environ: dict[str, str],
) -> None:
    with pytest.raises(policy.PolicyError, match="SUDO_USER and SUDO_UID"):
        policy.caller_from_sudo(
            make_config(),
            environ,
            euid=SERVICE_UID,
            groups=approved_groups,
        )


@pytest.mark.parametrize("sudo_uid", ["", " 2002", "+2002", "-1", "2e3", "20_02"])
def test_caller_rejects_malformed_numeric_uid(passwd: None, sudo_uid: str) -> None:
    with pytest.raises(policy.PolicyError, match="SUDO_UID"):
        policy.caller_from_sudo(
            make_config(),
            {"SUDO_USER": "hongjian", "SUDO_UID": sudo_uid},
            euid=SERVICE_UID,
            groups=approved_groups,
        )


def test_caller_rejects_username_uid_mismatch(passwd: None) -> None:
    with pytest.raises(policy.PolicyError, match="username/UID pair"):
        policy.caller_from_sudo(
            make_config(),
            {"SUDO_USER": "hongjian", "SUDO_UID": "2003"},
            euid=SERVICE_UID,
            groups=approved_groups,
        )


def test_caller_rejects_unknown_sudo_username(passwd: None) -> None:
    with pytest.raises(policy.PolicyError, match="unknown sudo user"):
        policy.caller_from_sudo(
            make_config(),
            {"SUDO_USER": "nobody-here", "SUDO_UID": "2003"},
            euid=SERVICE_UID,
            groups=approved_groups,
        )


def test_caller_rejects_wrong_broker_effective_uid(passwd: None) -> None:
    with pytest.raises(policy.PolicyError, match="service account"):
        policy.caller_from_sudo(
            make_config(),
            {"SUDO_USER": "hongjian", "SUDO_UID": "2002"},
            euid=0,
            groups=approved_groups,
        )


def test_caller_rejects_non_member(passwd: None) -> None:
    with pytest.raises(policy.PolicyError, match="operator group"):
        policy.caller_from_sudo(
            make_config(),
            {"SUDO_USER": "hongjian", "SUDO_UID": "2002"},
            euid=SERVICE_UID,
            groups=lambda _username: {"docker", "sharedwork"},
        )


def test_caller_rejects_unapproved_root(passwd: None) -> None:
    with pytest.raises(policy.PolicyError, match="operator group"):
        policy.caller_from_sudo(
            make_config(),
            {"SUDO_USER": "root", "SUDO_UID": "0"},
            euid=SERVICE_UID,
            groups=lambda _username: {"root"},
        )


def test_caller_accepts_root_only_when_explicitly_approved(passwd: None) -> None:
    identity = policy.caller_from_sudo(
        make_config(),
        {"SUDO_USER": "root", "SUDO_UID": "0"},
        euid=SERVICE_UID,
        groups=approved_groups,
    )

    assert identity.username == "root"
    assert identity.uid == 0


def test_child_environment_is_an_exact_fixed_allowlist() -> None:
    env = policy.sanitized_child_environment(make_config(), service_uid=SERVICE_UID)

    assert env == {
        "HOME": "/var/lib/loom-staging-rollout",
        "USER": "loom-rollout",
        "LOGNAME": "loom-rollout",
        "PATH": "/opt/loom-staging-runner/venv/bin:/usr/local/bin:/usr/bin:/bin",
        "XDG_RUNTIME_DIR": f"/run/user/{SERVICE_UID}",
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{SERVICE_UID}/bus",
        "KUBECONFIG": "/var/lib/loom-staging-rollout/kubeconfig",
        "LC_ALL": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "LOOM_STAGING_ROLLOUT_CONFIG": "/etc/loom/staging-rollout.toml",
    }
    assert "PYTHONPATH" not in env
    assert "GIT_CONFIG_COUNT" not in env
    assert "GIT_DIR" not in env
    assert "GIT_WORK_TREE" not in env
    assert "LD_PRELOAD" not in env
    assert not any(key.startswith("SUDO_") for key in env)


def test_public_policy_apis_expose_no_actor_or_candidate_overrides() -> None:
    caller_signature = inspect.signature(policy.caller_from_sudo)
    child_signature = inspect.signature(policy.sanitized_child_environment)

    assert tuple(caller_signature.parameters) == ("config", "environ", "euid", "groups")
    assert tuple(child_signature.parameters) == ("config", "service_uid")
    forbidden = {"actor", "caller", "ref", "tag", "remote", "candidate", "environment"}
    assert forbidden.isdisjoint(caller_signature.parameters)
    assert forbidden.isdisjoint(child_signature.parameters)
    assert caller_signature.parameters["euid"].kind is inspect.Parameter.KEYWORD_ONLY
    assert caller_signature.parameters["groups"].kind is inspect.Parameter.KEYWORD_ONLY
    assert child_signature.parameters["service_uid"].kind is inspect.Parameter.KEYWORD_ONLY
