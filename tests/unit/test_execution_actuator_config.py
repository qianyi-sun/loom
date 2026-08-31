import pytest

from loom_execution_actuator.config import ExecutionActuatorSettings


def test_execution_actuator_settings_parse_target_placement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_DB_URL", "postgresql+asyncpg://loom@db/loom")
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID", "actuator-test")
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_TARGET_ID", "nebius-eu-north1-development"
    )
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_NAMESPACE", "loom-nebius-development")
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR",
        '{"loom.nebius/node-role":"execution"}',
    )
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_TOLERATIONS",
        '[{"key":"loom.nebius/execution","operator":"Equal",'
        '"value":"true","effect":"NoSchedule"}]',
    )

    settings = ExecutionActuatorSettings()

    assert settings.node_selector == {"loom.nebius/node-role": "execution"}
    assert settings.tolerations == (
        {
            "key": "loom.nebius/execution",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        },
    )
