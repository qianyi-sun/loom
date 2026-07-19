from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.install_attestation import (
    INSTALL_ASSETS,
    RunnerInstallAttestation,
    verify_runner_install,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_registered_checks import build_runner_install_check


def _asset_fixture(tmp_path: Path) -> tuple[dict[str, tuple[Path, int, bool]], dict[str, str]]:
    selected: dict[str, tuple[Path, int, bool]] = {}
    digests: dict[str, str] = {}
    for label, (_fixed_path, mode, private) in INSTALL_ASSETS.items():
        path = tmp_path / "assets" / label
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = f"fixed-{label}\n".encode()
        path.write_bytes(payload)
        path.chmod(mode)
        selected[label] = (path, mode, private)
        digests[label] = hashlib.sha256(payload).hexdigest()
    return selected, digests


def _statement(asset_sha256: dict[str, str]) -> RunnerInstallAttestation:
    return RunnerInstallAttestation(
        source_mode="sealed-cumulative",
        source_sha="1" * 40,
        source_tree_sha="2" * 40,
        source_base_sha="3" * 40,
        install_record_sha256="4" * 64,
        asset_sha256=asset_sha256,
    )


def _write_statement(path: Path, statement: RunnerInstallAttestation) -> None:
    path.write_bytes(statement.to_payload())
    path.chmod(0o640)


def _config(tmp_path: Path, *, config_sha256: str) -> OperatorConfig:
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=tmp_path / "runner",
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=tmp_path / "rollout",
        kubeconfig_path=tmp_path / "kubeconfig",
        cluster_config_path=tmp_path / "staging.cluster.toml",
        admin_token_source=f"file:{tmp_path / 'admin'}",
        worker_token_source=f"file:{tmp_path / 'worker'}",
        service_token_source=f"file:{tmp_path / 'service'}",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "staging-rollout.toml",
        config_sha256=config_sha256,
        source_mode="sealed-cumulative",
        source_commit_sha="1" * 40,
        source_tree_sha="2" * 40,
        source_base_sha="3" * 40,
    )


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="1" * 40,
        image_tag="staging-1111111",
        fetched_at="2026-07-19T16:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="2" * 40,
        approved_base_sha="3" * 40,
    )


def _passing_candidate_check() -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id="candidate.identity",
            failure_code="candidate.identity.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=300,
            remediation="restore candidate",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )


def test_runner_install_attestation_round_trip_and_live_asset_verification(
    tmp_path: Path,
) -> None:
    assets, digests = _asset_fixture(tmp_path)
    statement = _statement(digests)
    path = tmp_path / "install-attestation.json"
    _write_statement(path, statement)

    verified = verify_runner_install(
        service_uid=os.geteuid(),
        attestation_path=path,
        assets=assets,
        expected_root_uid=os.geteuid(),
    )

    assert verified.ready
    assert verified.attestation == statement
    assert RunnerInstallAttestation.from_payload(statement.to_payload()) == statement
    assert len(verified.metadata_fingerprint) == 64
    assert len(verified.acl_fingerprint) == 64


def test_runner_install_attestation_reports_all_asset_drift_without_diagnostics(
    tmp_path: Path,
) -> None:
    assets, digests = _asset_fixture(tmp_path)
    statement = _statement(digests)
    path = tmp_path / "install-attestation.json"
    _write_statement(path, statement)
    for label in ("broker", "tmpfiles"):
        asset_path, _mode, _private = assets[label]
        asset_path.write_text("changed private-looking payload\n", encoding="utf-8")

    verified = verify_runner_install(
        service_uid=os.geteuid(),
        attestation_path=path,
        assets=assets,
        expected_root_uid=os.geteuid(),
    )

    assert not verified.ready
    assert verified.failed_assets == ("broker", "tmpfiles")
    assert "payload" not in str(verified)


def test_runner_install_attestation_rejects_duplicate_keys_and_symlink(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        RunnerInstallAttestation.from_payload(b'{"schema_version":1,"schema_version":1}\n')

    assets, digests = _asset_fixture(tmp_path)
    target = tmp_path / "target.json"
    _write_statement(target, _statement(digests))
    link = tmp_path / "statement.json"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="traversal"):
        verify_runner_install(
            service_uid=os.geteuid(),
            attestation_path=link,
            assets=assets,
            expected_root_uid=os.geteuid(),
        )


def test_registered_runner_install_binds_candidate_config_and_live_assets(
    tmp_path: Path,
) -> None:
    assets, digests = _asset_fixture(tmp_path)
    statement = _statement(digests)
    path = tmp_path / "install-attestation.json"
    _write_statement(path, statement)
    config = _config(tmp_path, config_sha256=digests["config"])
    check = build_runner_install_check(
        config=config,
        candidate=_candidate(),
        service_uid=os.geteuid(),
        expected_attestation_digest=statement.payload_digest,
        attestation_path=path,
        assets=assets,
        expected_root_uid=os.geteuid(),
    )
    context = CheckContext(
        {
            "candidate.base.sha": "3" * 40,
            "candidate.sha": "1" * 40,
            "candidate.source-mode": "sealed-cumulative",
            "runner.config.sha256": digests["config"],
            "runner.install.sha256": statement.payload_digest,
            "service.uid": os.geteuid(),
        }
    )

    executions = PreflightDag((_passing_candidate_check(), check)).run(
        context,
        through_tier=0,
        now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
    )

    install = next(item for item in executions if item.check_id == "runner.install")
    assert install.passed
    assert install.evidence["source-sha"] == "1" * 40
    assert install.evidence["asset-digests"] == digests


def test_registered_runner_install_fails_closed_on_config_or_asset_drift(tmp_path: Path) -> None:
    assets, digests = _asset_fixture(tmp_path)
    statement = _statement(digests)
    path = tmp_path / "install-attestation.json"
    _write_statement(path, statement)
    config = _config(tmp_path, config_sha256="f" * 64)
    check = build_runner_install_check(
        config=config,
        candidate=_candidate(),
        service_uid=os.geteuid(),
        expected_attestation_digest=statement.payload_digest,
        attestation_path=path,
        assets=assets,
        expected_root_uid=os.geteuid(),
    )
    context = CheckContext(
        {
            "candidate.base.sha": "3" * 40,
            "candidate.sha": "1" * 40,
            "candidate.source-mode": "sealed-cumulative",
            "runner.config.sha256": "f" * 64,
            "runner.install.sha256": statement.payload_digest,
            "service.uid": os.geteuid(),
        }
    )

    executions = PreflightDag((_passing_candidate_check(), check)).run(
        context,
        through_tier=0,
    )

    install = next(item for item in executions if item.check_id == "runner.install")
    assert not install.passed
    assert install.evidence["failed-assets"] == {"install-binding": "identity-drift"}
