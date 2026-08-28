from __future__ import annotations

import hashlib
import importlib
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.config import OperatorConfig, environment_authority
from tests.loom_cli.rollout.operator.test_broker import make_config

CURRENT_SHA = "c" * 40
HISTORICAL_SHA = "a" * 40
HISTORICAL_TREE = "b" * 40


def _runtime_config(tmp_path: Path) -> tuple[OperatorConfig, bytes, bytes]:
    authority = environment_authority("staging")
    current_repo = authority.candidate_runtime_root / CURRENT_SHA / "repo"
    current_cluster = current_repo / authority.candidate_cluster_config
    payload = (
        'schema_version = 1\n'
        f'runner_repo = "{current_repo}"\n'
        f'cluster_config_path = "{current_cluster}"\n'
        'environment = "staging"\n'
    ).encode()
    config = replace(
        make_config(tmp_path),
        runner_repo=current_repo,
        cluster_config_path=current_cluster,
        config_sha256=hashlib.sha256(payload).hexdigest(),
    )
    historical_repo = authority.candidate_runtime_root / HISTORICAL_SHA / "repo"
    historical_payload = payload.replace(str(current_repo).encode(), str(historical_repo).encode())
    return config, payload, historical_payload


def test_runtime_upgrade_authority_reconstructs_exact_historical_config(
    tmp_path: Path,
) -> None:
    module = importlib.import_module("loom_cli.rollout.operator.resume_runtime_upgrade")
    config, current_payload, historical_payload = _runtime_config(tmp_path)
    cluster_payload = b"container_registry = 'registry.internal'\n"

    authority = module.ResumeRuntimeUpgradeAuthority(
        current_config_payload=current_payload,
        verify_runtime=lambda _config, _sha, _tree: None,
        prove_ancestor=lambda _repo, historical, current: (
            historical == HISTORICAL_SHA and current == CURRENT_SHA
        ),
        read_cluster_config=lambda _path: cluster_payload,
    )

    resolved = authority.resolve(
        config,
        candidate_sha=HISTORICAL_SHA,
        candidate_tree=HISTORICAL_TREE,
        runner_config_sha256=hashlib.sha256(historical_payload).hexdigest(),
        cluster_config_path=str(
            environment_authority("staging").candidate_runtime_root
            / HISTORICAL_SHA
            / "repo"
            / environment_authority("staging").candidate_cluster_config
        ),
    )

    expected_repo = (
        environment_authority("staging").candidate_runtime_root
        / HISTORICAL_SHA
        / "repo"
    )
    assert resolved.runner_repo == expected_repo
    assert resolved.cluster_config_path == (
        expected_repo / environment_authority("staging").candidate_cluster_config
    )
    assert resolved.config_sha256 == hashlib.sha256(historical_payload).hexdigest()


def test_installed_runtime_upgrade_authority_uses_trusted_candidate_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("loom_cli.rollout.operator.resume_runtime_upgrade")
    config, current_payload, historical_payload = _runtime_config(tmp_path)
    cluster_payload = b"container_registry = 'registry.internal'\n"

    monkeypatch.setattr(
        module,
        "read_trusted_file",
        lambda path, **_kwargs: SimpleNamespace(
            payload=(current_payload if path == config.config_path else cluster_payload)
        ),
    )
    verified: list[tuple[str, str | None]] = []

    def verify_runtime(
        _config: OperatorConfig,
        candidate_sha: str,
        candidate_tree: str | None,
        *,
        run: object,
    ) -> None:
        assert callable(run)
        verified.append((candidate_sha, candidate_tree))

    monkeypatch.setattr(module, "verify_resume_runtime_candidate", verify_runtime)

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        assert argv[-4:] == ["merge-base", "--is-ancestor", HISTORICAL_SHA, CURRENT_SHA]
        return subprocess.CompletedProcess(argv, 0, "", "")

    authority = module.build_installed_resume_runtime_upgrade_authority(
        config,
        service_uid=2002,
        run=run,
    )
    resolved = authority.resolve(
        config,
        candidate_sha=HISTORICAL_SHA,
        candidate_tree=HISTORICAL_TREE,
        runner_config_sha256=hashlib.sha256(historical_payload).hexdigest(),
        cluster_config_path=str(
            environment_authority("staging").candidate_runtime_root
            / HISTORICAL_SHA
            / "repo"
            / environment_authority("staging").candidate_cluster_config
        ),
    )

    assert resolved.runner_repo.parent.name == HISTORICAL_SHA
    assert verified == [(CURRENT_SHA, None), (HISTORICAL_SHA, HISTORICAL_TREE)]


@pytest.mark.parametrize(
    "drift",
    ["config", "cluster", "ancestry", "path", "runtime", "same-runtime", "sealed"],
)
def test_runtime_upgrade_authority_rejects_every_unproven_transition(
    tmp_path: Path,
    drift: str,
) -> None:
    module = importlib.import_module("loom_cli.rollout.operator.resume_runtime_upgrade")
    config, current_payload, historical_payload = _runtime_config(tmp_path)
    historical_cluster = (
        environment_authority("staging").candidate_runtime_root
        / HISTORICAL_SHA
        / "repo"
        / environment_authority("staging").candidate_cluster_config
    )
    if drift == "sealed":
        config = replace(
            config,
            source_mode="sealed-cumulative",
            source_commit_sha=CURRENT_SHA,
            source_tree_sha="d" * 40,
            source_base_sha="e" * 40,
        )

    def verify_runtime(
        _config: OperatorConfig,
        candidate_sha: str,
        _candidate_tree: str | None,
    ) -> None:
        if drift == "runtime" and candidate_sha == HISTORICAL_SHA:
            raise FileNotFoundError("historical runtime is unavailable")

    authority = module.ResumeRuntimeUpgradeAuthority(
        current_config_payload=(current_payload + b"# drift\n" if drift == "config" else current_payload),
        verify_runtime=verify_runtime,
        prove_ancestor=lambda _repo, _historical, _current: drift != "ancestry",
        read_cluster_config=lambda path: (
            b"old\n" if drift == "cluster" and path == historical_cluster else b"current\n"
        ),
    )

    with pytest.raises(module.ResumeRuntimeUpgradeError):
        authority.resolve(
            config,
            candidate_sha=(CURRENT_SHA if drift == "same-runtime" else HISTORICAL_SHA),
            candidate_tree=HISTORICAL_TREE,
            runner_config_sha256=hashlib.sha256(historical_payload).hexdigest(),
            cluster_config_path=str(
                historical_cluster.with_name("other.toml")
                if drift == "path"
                else historical_cluster
            ),
        )
