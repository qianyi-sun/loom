from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main

_SHA = "abcdef1234567890abcdef1234567890abcdef12"
_ROLLOUT_ID = "staging-abcdef1"
_REQUEST_ID = "request-20260713-hongjian"
_ATTRIBUTION = {
    "request_id": _REQUEST_ID,
    "initiating_operator": "hongjian",
    "initiating_uid": 2011,
    "attempt_number": 2,
    "attempt_operator": "devansh",
    "attempt_uid": 2501,
}


def test_manual_cluster_up_cannot_hide_staging_behind_development_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    staging_config = tmp_path / "staging.cluster.toml"
    staging_config.write_text(
        'namespace = "loom-staging"\n'
        'runtime_environment = "staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(staging_config),
        ]
    )

    assert rc == 1
    assert "Nebius deployment" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


@pytest.mark.parametrize(
    "staging_root_alias",
    [
        "/data//loom-staging",
        "/data/./loom-staging",
        "//data/loom-staging",
        "/data/loom-staging/custom",
    ],
)
def test_manual_cluster_up_cannot_hide_staging_behind_host_root_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    staging_root_alias: str,
) -> None:
    staging_config = tmp_path / "staging-alias.cluster.toml"
    staging_config.write_text(
        f'persistent_storage_host_path_root = "{staging_root_alias}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(staging_config),
        ]
    )

    assert rc == 1
    assert "Nebius deployment" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


@pytest.mark.parametrize(
    "symlink_target",
    ["/data/loom-staging", "/data/loom-staging/custom"],
)
def test_manual_cluster_up_cannot_hide_staging_behind_symlinked_host_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    symlink_target: str,
) -> None:
    staging_root_alias = tmp_path / "staging-root-alias"
    staging_root_alias.symlink_to(symlink_target, target_is_directory=True)
    staging_config = tmp_path / "staging-symlink.cluster.toml"
    staging_config.write_text(
        f'persistent_storage_host_path_root = "{staging_root_alias}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(staging_config),
        ]
    )

    assert rc == 1
    assert "Nebius deployment" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


def test_cluster_up_rejects_unresolvable_host_root_before_lock_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    loop_a = tmp_path / "loop-a"
    loop_b = tmp_path / "loop-b"
    loop_a.symlink_to(loop_b, target_is_directory=True)
    loop_b.symlink_to(loop_a, target_is_directory=True)
    config_path = tmp_path / "loop.cluster.toml"
    config_path.write_text(
        f'persistent_storage_host_path_root = "{loop_a}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
        ]
    )

    assert rc == 2
    assert "preflight config invalid" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


@pytest.mark.parametrize(
    "production_declaration",
    ["runtime", "namespace", "host-root"],
)
def test_cluster_up_rejects_production_config_hidden_behind_development_argv_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    production_declaration: str,
) -> None:
    values = {
        "namespace": "loom",
        "runtime_environment": "development",
        "persistent_storage_host_path_root": "/tmp/loom-development",
    }
    if production_declaration == "runtime":
        values["runtime_environment"] = " production "
    elif production_declaration == "namespace":
        values["namespace"] = "loom-prod"
    else:
        values["persistent_storage_host_path_root"] = "/data//loom-prod"
    config_path = tmp_path / "hidden-production.cluster.toml"
    config_path.write_text(
        f'namespace = "{values["namespace"]}"\n'
        f'runtime_environment = "{values["runtime_environment"]}"\n'
        'persistent_storage_backend = "static-host-path"\n'
        "persistent_storage_host_path_root = "
        f'"{values["persistent_storage_host_path_root"]}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    client_loads = 0

    def _track_clients(_context: str | None) -> tuple[object, object, object, object]:
        nonlocal client_loads
        client_loads += 1
        return object(), object(), object(), object()

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _track_clients)

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "Nebius deployment" in err
    assert client_loads == 0
    assert not (tmp_path / "locks").exists()


def test_cluster_up_rejects_conflicting_protected_config_declarations_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "conflicting.cluster.toml"
    config_path.write_text(
        'namespace = "loom-prod"\n'
        'runtime_environment = "staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    client_loads = 0

    def _track_clients(_context: str | None) -> tuple[object, object, object, object]:
        nonlocal client_loads
        client_loads += 1
        return object(), object(), object(), object()

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _track_clients)

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
        ]
    )

    assert rc == 2
    assert "conflicting protected cluster config targets" in capsys.readouterr().err
    assert client_loads == 0


def test_cluster_up_preserves_relative_host_root_render_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "relative-root.cluster.toml"
    config_path.write_text(
        'namespace = "loom"\n'
        'runtime_environment = "development"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "data/loom-staging"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
            "--skip-preflight",
        ]
    )

    err = capsys.readouterr().err
    assert rc == 2
    assert "render failed" in err
    assert "must be an absolute host path" in err
    assert "broker-created request envelope" not in err
