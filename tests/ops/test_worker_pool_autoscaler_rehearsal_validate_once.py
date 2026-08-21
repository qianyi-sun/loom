from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


@pytest.fixture
def module() -> Any:
    try:
        return importlib.import_module(
            "loom_cli.rollout.rehearsal_external_supervisor_policy_probe"
        )
    except ModuleNotFoundError:
        pytest.fail("rehearsal-only external supervisor policy validator is missing")


def _args(module: Any, *extra: str) -> Any:
    return module._parser().parse_args(
        [
            "--environment",
            "staging",
            "--pool-name",
            "gb10",
            "--expected-slurm-cluster-name",
            "trt-gb10",
            "--expected-slurm-controller-host",
            "gx10-01c7",
            "--namespace",
            "loom-rehearsal-abc123",
            "--kubeconfig",
            "/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig",
            "--global-execution-manager-export",
            "deployment/loom-capacity-manager",
            "--global-execution-manager-namespace",
            "loom-dev",
            "--global-execution-manager-kubeconfig",
            "/var/lib/loom-staging-rollout/kubeconfig",
            "--expected-manager-public-key-sha256",
            "a" * 64,
            "--validate-only",
            *extra,
        ]
    )


def _builder_argv(tmp_path: Path) -> list[str]:
    return [
        "--environment",
        "staging",
        "--pool-name",
        "task-image-builder-gb10",
        "--profile",
        str(tmp_path / "staging.toml"),
        "--image-tag",
        "staging-aaaaaaaa",
        "--env-config-version",
        "staging-aaaaaaaa",
        "--git-sha",
        "a" * 40,
        "--expected-slurm-cluster-name",
        "trt-gb10",
        "--expected-slurm-controller-host",
        "gx10-01c7",
        "--namespace",
        "loom-rehearsal-abc123",
        "--kubeconfig",
        "/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig",
        "--global-execution-manager-export",
        "deployment/loom-capacity-manager",
        "--global-execution-manager-namespace",
        "loom-dev",
        "--global-execution-manager-kubeconfig",
        "/var/lib/loom-staging-rollout/kubeconfig",
        "--expected-manager-public-key-sha256",
        "a" * 64,
        "--validate-only",
    ]


def test_rehearsal_policy_parser_selects_task_image_builder_contract(
    module: Any,
    tmp_path: Path,
) -> None:
    assert hasattr(module, "_parse_args")

    args = module._parse_args(_builder_argv(tmp_path))

    assert args.pool_name == ["task-image-builder-gb10"]
    assert args.profile == tmp_path / "staging.toml"
    assert args.image_tag == "staging-aaaaaaaa"


def test_rehearsal_policy_parser_preserves_help_without_pool(
    module: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        module._parse_args(["--help"])

    assert raised.value.code == 0
    assert "Run one scoped external worker-pool autoscaler reconcile" in capsys.readouterr().out


def test_rehearsal_task_image_builder_policy_is_read_only_and_exclusive(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    assert hasattr(module, "builder_once")
    args = module._parse_args(_builder_argv(tmp_path))
    captured: dict[str, object] = {}
    witness = object()
    config = SimpleNamespace(
        pool_name="task-image-builder-gb10",
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        exclusive=True,
        requested_concurrency=1,
    )

    monkeypatch.setattr(
        module.builder_once,
        "_load_enabled_builder_config",
        lambda value: captured.update({"config_args": value}) or config,
    )
    monkeypatch.setattr(
        module.builder_once,
        "_rehearsal_validation_evidence",
        lambda value: (
            captured.update({"evidence_config": value})
            or {
                "request_nodes": ["trt-gb10-2"],
                "request_set_sha256": "b" * 64,
            }
        ),
    )
    monkeypatch.setattr(
        module.external_once,
        "_load_current_global_execution_witness",
        lambda value, *, pool_id: (
            captured.update({"witness_args": value, "witness_pool_id": pool_id})
            or witness
        ),
    )
    monkeypatch.setattr(
        module,
        "assert_legacy_scale_up_allowed",
        lambda value, **kwargs: captured.update(
            {"asserted_witness": value, "witness_expectations": kwargs}
        ),
    )
    monkeypatch.setattr(
        module.builder_once.transport,
        "_validate_local_slurm_authority",
        lambda _args: (_ for _ in ()).throw(
            AssertionError("rehearsal builder policy must not probe local Slurm")
        ),
    )

    result = asyncio.run(module._main_async(args))

    assert captured["config_args"] is args
    assert captured["evidence_config"] is config
    assert captured["witness_args"] is args
    assert captured["witness_pool_id"] == "gb10"
    assert captured["asserted_witness"] is witness
    witness_expectations = captured["witness_expectations"]
    assert isinstance(witness_expectations, dict)
    assert witness_expectations["expected_authority"] == "global-capacity-manager"
    assert witness_expectations["expected_pool_id"] == "gb10"
    assert witness_expectations["now"] is not None
    assert result == {
        "expected_slurm_authority": {
            "cluster_name": "trt-gb10",
            "controller_host": "gx10-01c7",
        },
        "mode": "rehearsal-task-image-builder-policy-only",
        "policy": {
            "cpu_arch": "arm64",
            "exclusive": True,
            "pool_name": "task-image-builder-gb10",
            "request_nodes": ["trt-gb10-2"],
            "request_set_sha256": "b" * 64,
            "requested_concurrency": 1,
        },
    }


def test_rehearsal_policy_validation_is_read_only_and_does_not_claim_local_slurm(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    witness = object()

    async def validate(args: Any, *, authority: Any) -> list[dict[str, object]]:
        captured["args"] = args
        captured["authority"] = authority
        return [
            {
                "enabled_external_policy_count": 1,
                "environment": "staging",
                "pool_name": "gb10",
            }
        ]

    monkeypatch.setattr(module.external_once, "_validate_external_policies_once", validate)
    monkeypatch.setattr(
        module.external_once,
        "_load_current_global_execution_witness",
        lambda args, *, pool_id: (
            captured.update({"witness_args": args, "witness_pool_id": pool_id})
            or witness
        ),
    )
    monkeypatch.setattr(
        module,
        "assert_legacy_scale_up_allowed",
        lambda value, **kwargs: captured.update(
            {"asserted_witness": value, "witness_expectations": kwargs}
        ),
    )
    monkeypatch.setattr(
        module.external_once,
        "_validate_local_slurm_authority",
        lambda _args: (_ for _ in ()).throw(
            AssertionError("rehearsal policy validation must not probe local Slurm")
        ),
    )

    result = asyncio.run(module._main_async(_args(module)))

    authority = captured["authority"]
    assert authority == module.external_once.SlurmPolicyAuthority(
        cluster_name="trt-gb10",
        controller_host="gx10-01c7",
    )
    assert captured["witness_args"] is captured["args"]
    assert captured["witness_pool_id"] == "gb10"
    assert captured["asserted_witness"] is witness
    assert captured["witness_expectations"]["expected_authority"] == (
        "global-capacity-manager"
    )
    assert captured["witness_expectations"]["expected_pool_id"] == "gb10"
    assert result == {
        "database_reachable": True,
        "expected_slurm_authority": {
            "cluster_name": "trt-gb10",
            "controller_host": "gx10-01c7",
        },
        "mode": "rehearsal-policy-only",
        "pools": [
            {
                "enabled_external_policy_count": 1,
                "environment": "staging",
                "pool_name": "gb10",
            }
        ],
    }
    assert "local_hostname" not in str(result)


def test_rehearsal_policy_validation_blocks_when_manager_export_is_unavailable(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def validate(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        return []

    monkeypatch.setattr(module.external_once, "_validate_external_policies_once", validate)
    monkeypatch.setattr(
        module.external_once,
        "_load_current_global_execution_witness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            module.GlobalExecutionFenceError(
                "global execution witness export is unavailable"
            )
        ),
    )

    with pytest.raises(module.RehearsalPolicyValidationError, match="witness"):
        asyncio.run(module._main_async(_args(module)))


@pytest.mark.parametrize(
    "extra",
    [
        ("--namespace", "loom-staging"),
        ("--kubeconfig", "/var/lib/loom-staging-rollout/kubeconfig"),
    ],
)
def test_rehearsal_policy_validation_rejects_nonisolated_authority_before_database(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    extra: tuple[str, str],
) -> None:
    monkeypatch.setattr(
        module.external_once,
        "_validate_external_policies_once",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("nonisolated authority must not reach the database")
        ),
    )

    with pytest.raises(module.RehearsalPolicyValidationError, match="authority"):
        asyncio.run(module._main_async(_args(module, *extra)))
