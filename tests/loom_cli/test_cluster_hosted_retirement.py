"""The disposable cluster command cannot deploy hosted environments."""

import pytest

from loom_cli.__main__ import main


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_hosted_cluster_up_is_rejected_before_clients(monkeypatch, capsys, environment):
    def unexpected(*args, **kwargs):
        pytest.fail("retired hosted entrypoint opened Kubernetes clients")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", unexpected)
    assert main(["cluster", "up", "--environment", environment]) == 1
    assert "Nebius deployment" in capsys.readouterr().err


@pytest.mark.parametrize("declaration", [
    'runtime_environment = "staging"',
    'namespace = "loom-production"',
    'persistent_storage_host_path_root = "/data/loom-staging"',
])
def test_local_arguments_cannot_override_hosted_configuration(monkeypatch, tmp_path, capsys, declaration):
    config = tmp_path / "hosted.toml"
    config.write_text(declaration + "\n")

    def unexpected(*args, **kwargs):
        pytest.fail("hosted configuration reached Kubernetes clients")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", unexpected)
    assert main(["cluster", "up", "--environment", "development", "--config", str(config)]) == 1
    assert "Nebius deployment" in capsys.readouterr().err
