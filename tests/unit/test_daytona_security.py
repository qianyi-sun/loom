from __future__ import annotations

import pytest

from loom.models.networking import Allowlist, NoNetwork
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom_drivers.daytona.security import (
    DaytonaSecurityError,
    build_daytona_trial_security,
)


def _task(
    *,
    steps: list[dict[str, object]] | None = None,
    **environment_overrides: object,
) -> TaskConfig:
    environment: dict[str, object] = {
        "os": "linux",
        "docker_image": "registry.example/loom/task@sha256:" + "a" * 64,
    }
    environment.update(environment_overrides)
    raw: dict[str, object] = {
        "task": {"id": "daytona/security", "name": "Daytona security"},
        "environment": environment,
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
    }
    if steps is not None:
        raw["steps"] = steps
    return TaskConfig.model_validate(raw)


def _trial(agent_name: str, **overrides: object) -> TrialConfig:
    values: dict[str, object] = {
        "agent_name": agent_name,
        "agent_model": None,
    }
    values.update(overrides)
    return TrialConfig.model_validate(values)


def test_in_process_agent_defaults_to_no_network() -> None:
    security = build_daytona_trial_security(
        task_config=_task(),
        trial_config=_trial("oracle"),
        sandbox_gateway_url=None,
    )

    assert isinstance(security.baseline_network_policy, NoNetwork)
    assert security.allowed_network_domains == frozenset()
    assert security.sandbox_gateway_url is None


def test_subprocess_agent_defaults_to_gateway_only_allowlist() -> None:
    security = build_daytona_trial_security(
        task_config=_task(),
        trial_config=_trial("codex"),
        sandbox_gateway_url="https://gateway.example.com/openai/v1",
    )

    assert security.baseline_network_policy == Allowlist(
        domains=("gateway.example.com",)
    )
    assert security.allowed_network_domains == frozenset({"gateway.example.com"})
    assert security.sandbox_gateway_url == "https://gateway.example.com/openai/v1"


@pytest.mark.parametrize(
    "url",
    [
        None,
        "http://gateway.example.com/openai/v1",
        "https://user:password@gateway.example.com/openai/v1",
        "https://gateway.internal/openai/v1",
        "https://127.0.0.1/openai/v1",
        "https://gateway.example.com:8443/openai/v1",
        "https://gateway.example.com/openai/v1?token=unsafe",
    ],
)
def test_subprocess_agent_rejects_unsafe_gateway_url(url: str | None) -> None:
    with pytest.raises(DaytonaSecurityError, match="daytona_gateway_url_"):
        build_daytona_trial_security(
            task_config=_task(),
            trial_config=_trial("codex"),
            sandbox_gateway_url=url,
        )


@pytest.mark.parametrize(
    ("environment", "expected_name"),
    [
        ({"OPENAI_API_KEY": "not-printed"}, "OPENAI_API_KEY"),
        ({"SAFE_NAME": "sk-1234567890"}, "SAFE_NAME"),
    ],
)
def test_task_environment_rejects_provider_secrets_without_value_in_error(
    environment: dict[str, str],
    expected_name: str,
) -> None:
    with pytest.raises(DaytonaSecurityError) as captured:
        build_daytona_trial_security(
            task_config=_task(environment=environment),
            trial_config=_trial("oracle"),
            sandbox_gateway_url=None,
        )

    assert captured.value.code == "daytona_secret_environment_denied"
    assert expected_name in str(captured.value)
    assert next(iter(environment.values())) not in str(captured.value)


def test_secret_like_environment_name_is_redacted_from_error() -> None:
    secret_name = "sk-secret-in-variable-name"
    secret_value = "sk-secret-in-variable-value"
    with pytest.raises(DaytonaSecurityError) as captured:
        build_daytona_trial_security(
            task_config=_task(environment={secret_name: secret_value}),
            trial_config=_trial("oracle"),
            sandbox_gateway_url=None,
        )

    assert secret_name not in str(captured.value)
    assert secret_value not in str(captured.value)
    assert "[REDACTED:api-key]" in str(captured.value)


def test_custom_dns_and_hosts_are_rejected() -> None:
    with pytest.raises(
        DaytonaSecurityError,
        match="daytona_network_override_denied",
    ):
        build_daytona_trial_security(
            task_config=_task(extra_hosts={"gateway.example.com": "127.0.0.1"}),
            trial_config=_trial("oracle"),
            sandbox_gateway_url=None,
        )


@pytest.mark.parametrize(
    "policy",
    [
        {"kind": "public"},
        {"kind": "allowlist", "domains": ["api.openai.com"]},
        {
            "kind": "allowlist",
            "domains": ["gateway.example.com"],
            "cidrs": ["1.1.1.1/32"],
        },
    ],
)
def test_step_phase_cannot_expand_gateway_network_authority(
    policy: dict[str, object],
) -> None:
    task = _task(
        steps=[
            {
                "name": "main",
                "network": {"agent_phase": policy},
            }
        ]
    )

    with pytest.raises(DaytonaSecurityError, match=r"daytona_.*denied"):
        build_daytona_trial_security(
            task_config=task,
            trial_config=_trial("codex"),
            sandbox_gateway_url="https://gateway.example.com/openai/v1",
        )


def test_explicit_no_network_remains_supported_for_subprocess_agent() -> None:
    security = build_daytona_trial_security(
        task_config=_task(),
        trial_config=_trial(
            "codex",
            baseline_network_policy_override={"kind": "no-network"},
        ),
        sandbox_gateway_url="https://gateway.example.com/openai/v1",
    )

    assert isinstance(security.baseline_network_policy, NoNetwork)
