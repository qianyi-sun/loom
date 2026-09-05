"""Public pre-request failures remain diagnosable without launch authority."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_host as host

from loom_cli.rollout.operator import broker
from loom_cli.rollout.operator import preflight_diagnostics as diagnostics
from loom_cli.rollout.operator.broker import main
from loom_cli.rollout.operator.config import ConfigError
from loom_cli.rollout.operator.policy import CallerIdentity, PolicyError
from loom_cli.rollout.operator.preflight import PreflightCheck, PreflightReport
from loom_cli.rollout.preflight_artifact_store import (
    PreflightArtifactStore,
    PreflightArtifactStoreError,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_pipeline import PreflightPipeline
from tests.loom_cli.rollout.operator.test_broker import (
    NOW,
    FakeMutationGuard,
    _published_assessment,
    fakes,
    pipeline_context,
    pipeline_registry,
)
from tests.loom_cli.rollout.test_preflight_artifact_store import _publish as publish_artifacts


@pytest.mark.parametrize("argv", [["preflight"], ["start"], ["start", "--dry-run"]])
@pytest.mark.parametrize("error_type", [ValueError, PolicyError, RuntimeError])
def test_late_exception_is_immutable_and_publicly_retrievable(
    tmp_path: Path, argv: list[str], error_type: type[Exception]
) -> None:
    bundle = fakes(tmp_path)
    bundle.config.state_root.mkdir(mode=0o700)
    guard = FakeMutationGuard(bundle.order)
    publications = []

    def assess(_candidate, _epoch):  # type: ignore[no-untyped-def]
        publications.append(publish_artifacts(PreflightArtifactStore(bundle.config.state_root)))
        bundle.order.append("images-published")
        raise error_type("arbitrary-unlabelled-secret-do-not-emit")

    deps = replace(
        bundle.dependencies,
        assess_preflight=assess,
        read_mutation_epoch=lambda: 7,
        mutation_guard=guard,
    )
    assert main(argv, dependencies=deps) == 1
    result = json.loads(bundle.stderr.getvalue())
    assert result["failure_code"] == "preflight-internal-error"
    assert result["stage"] == "assessment"
    assert result["diagnostic_recorded"] is True
    digest = result["diagnostic_sha256"]
    assert "arbitrary-unlabelled-secret" not in bundle.stderr.getvalue()
    assert main(["preflight-diagnostics", digest], dependencies=deps) == 0
    recorded = json.loads(bundle.stdout.getvalue())
    assert recorded["failure_code"] == result["failure_code"]
    assert bundle.store.requests == bundle.store.preflight_requests == {}
    assert bundle.backup.create_count == bundle.systemd.start_count == 0
    assert guard.acquired == []
    assert bundle.order.count("images-published") == 1
    assert len(publications) == 1
    assert publications[0].descriptor_path.is_file()

    class BrokerRunner:
        def run(self, argv, **_kwargs):  # type: ignore[no-untyped-def]
            return subprocess.CompletedProcess(argv, 1, "", bundle.stderr.getvalue())

    normalized = host.HostSystem(BrokerRunner()).run_post_install_preflight()
    assert normalized["blocker_codes"] == ["preflight-internal-error"]


@pytest.mark.parametrize(
    "phase,code",
    [
        ("configuration", "preflight-identity-drift"),
        ("authorization", "preflight-authorization-rejected"),
        ("dependency-initialization", "preflight-internal-error"),
    ],
)
def test_early_cli_failures_are_typed_without_unsafe_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    phase: str,
    code: str,
) -> None:
    bundle = fakes(tmp_path)
    bundle.config.state_root.mkdir(mode=0o700)

    def fail(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise (
            ConfigError("unlabelled-secret")
            if phase == "configuration"
            else PolicyError("unlabelled-secret")
        )

    monkeypatch.setattr(broker.OperatorConfig, "load", lambda *args, **kwargs: bundle.config)
    monkeypatch.setattr(
        broker, "caller_from_sudo", lambda *args, **kwargs: bundle.dependencies.authenticate()
    )
    monkeypatch.setattr(broker, "_default_dependencies", fail)
    if phase == "configuration":
        monkeypatch.setattr(broker.OperatorConfig, "load", fail)
    elif phase == "authorization":
        monkeypatch.setattr(broker, "caller_from_sudo", fail)
    assert main(["--env", "staging", "preflight"]) == 1
    output = capsys.readouterr()
    payload = json.loads(output.err)
    assert payload["failure_code"] == code
    assert payload["stage"] == phase
    assert payload["diagnostic_recorded"] == (phase == "dependency-initialization")
    assert output.out == ""
    assert "unlabelled-secret" not in output.err
    if phase != "dependency-initialization":
        assert list(bundle.config.state_root.iterdir()) == []


@pytest.mark.parametrize("argv", [["preflight"], ["start"], ["start", "--dry-run"]])
def test_sealed_noncoordinator_rejected_before_factory_or_diagnostic_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
) -> None:
    bundle = fakes(tmp_path)
    config = replace(bundle.config, source_mode="sealed-cumulative")
    config.state_root.mkdir(mode=0o700)
    calls: list[str] = []

    def fail_factory(*_args):  # type: ignore[no-untyped-def]
        calls.append("factory")
        raise RuntimeError("unlabelled-factory-secret")

    real_store = broker.PreflightDiagnosticStore

    def track_store(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("diagnostic-store")
        return real_store(*args, **kwargs)

    monkeypatch.setattr(broker.OperatorConfig, "load", lambda *_args, **_kwargs: config)
    monkeypatch.setattr(
        broker, "caller_from_sudo", lambda *_args, **_kwargs: CallerIdentity("outsider", 2999)
    )
    monkeypatch.setattr(broker, "_default_dependencies", fail_factory)
    monkeypatch.setattr(broker, "PreflightDiagnosticStore", track_store)
    assert main(["--env", "staging", *argv]) == 1
    output = capsys.readouterr()
    payload = json.loads(output.err)
    assert payload["failure_code"] == "preflight-authorization-rejected"
    assert payload["stage"] == "authorization"
    assert payload["diagnostic_recorded"] is False
    assert calls == []
    assert list(config.state_root.iterdir()) == []
    assert "unlabelled-factory-secret" not in output.err
    assert output.out == ""


@pytest.mark.parametrize(
    "name,code", [("checkout", "preflight-identity-drift"), ("docker", "preflight-check-failed")]
)
def test_failed_report_records_only_curated_projection(
    tmp_path: Path, name: str, code: str
) -> None:
    bundle = fakes(tmp_path)
    bundle.config.state_root.mkdir(mode=0o700)
    deps = replace(
        bundle.dependencies,
        assess_preflight=lambda *_args: None,
        read_mutation_epoch=lambda: 7,
        preflight=lambda: PreflightReport(
            (PreflightCheck(name, False, "safe explanation", "safe evidence"),)
        ),
    )
    assert main(["preflight"], dependencies=deps) == 1
    result = json.loads(bundle.stderr.getvalue())
    assert result["failure_code"] == code
    recorded = diagnostics.PreflightDiagnosticStore(bundle.config.state_root).read(
        result["diagnostic_sha256"]
    )
    assert recorded["check_ids"] == [name]
    assert "safe explanation" not in json.dumps(recorded)
    assert "safe evidence" not in json.dumps(recorded)
    assert bundle.candidate.fetch_count == 0


def test_artifact_exception_has_distinct_code(tmp_path: Path) -> None:
    bundle = fakes(tmp_path)

    def fail(*_args):  # type: ignore[no-untyped-def]
        raise PreflightArtifactStoreError("unlabelled-secret")

    deps = replace(bundle.dependencies, assess_preflight=fail, read_mutation_epoch=lambda: 7)
    assert main(["preflight"], dependencies=deps) == 1
    assert (
        json.loads(bundle.stderr.getvalue())["failure_code"]
        == "preflight-artifact-publication-failed"
    )


@pytest.mark.parametrize(
    "failed_check,code",
    [
        ("candidate.identity", "preflight-identity-drift"),
        ("artifacts.publish", "preflight-artifact-publication-failed"),
    ],
)
def test_structured_assessment_is_durably_projected(
    tmp_path: Path, failed_check: str, code: str
) -> None:
    bundle = fakes(tmp_path)
    bundle.config.state_root.mkdir(mode=0o700)
    registry = pipeline_registry(failed_check=failed_check)
    assessment = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: NOW,
    ).assess(context=pipeline_context(registry))
    deps = replace(
        bundle.dependencies,
        assess_preflight=lambda *_args: assessment,
        read_mutation_epoch=lambda: 7,
    )
    assert main(["preflight"], dependencies=deps) == 1
    result = json.loads(bundle.stderr.getvalue())
    assert result["failure_code"] == code
    assert result["diagnostic_recorded"] is True
    record = diagnostics.PreflightDiagnosticStore(bundle.config.state_root).read(
        result["diagnostic_sha256"]
    )
    assert failed_check in record["check_ids"]
    assert "blockers" not in record
    assert "assessment_digest" not in record
    assert not (bundle.config.state_root / "requests").exists()


@pytest.mark.parametrize("argv", [["preflight"], ["start"], ["start", "--dry-run"]])
def test_artifact_reference_failure_does_not_reach_mutation_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> None:
    bundle = fakes(tmp_path)
    guard = FakeMutationGuard(bundle.order)
    assessment = _published_assessment(tmp_path)

    def fail(_assessment):  # type: ignore[no-untyped-def]
        raise ValueError("plain-secret")

    monkeypatch.setattr(broker, "_preflight_artifact_reference", fail)
    deps = replace(
        bundle.dependencies,
        assess_preflight=lambda *_args: assessment,
        read_mutation_epoch=lambda: 7,
        mutation_guard=guard,
    )
    assert main(argv, dependencies=deps) == 1
    result = json.loads(bundle.stderr.getvalue())
    assert result["failure_code"] == "preflight-artifact-publication-failed"
    assert result["stage"] == "artifact-reference"
    assert bundle.store.preflight_requests == {}
    assert guard.acquired == []
    assert bundle.backup.create_count == bundle.systemd.start_count == 0


def _record(tmp_path: Path) -> tuple[diagnostics.PreflightDiagnosticStore, dict[str, object]]:
    tmp_path.chmod(0o700)
    store = diagnostics.PreflightDiagnosticStore(tmp_path)
    context = diagnostics.PreflightDiagnosticContext(
        "staging", "preflight", stage="assessment", initiator_uid=os.getuid(), store=store
    )
    result = context.failure("preflight-internal-error")
    assert result["diagnostic_recorded"] is True
    return store, store.read(str(result["diagnostic_sha256"]))


def test_store_no_replace_and_growth_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store, record = _record(tmp_path)
    digest = store.publish(record)
    assert store.publish(record) == digest
    monkeypatch.setattr(diagnostics, "MAX_RECORDS", 1)
    with pytest.raises(diagnostics.DiagnosticStoreError, match="capacity"):
        store.publish({**record, "stage": "report"})
    context = diagnostics.PreflightDiagnosticContext(
        "staging", "preflight", stage="assessment", initiator_uid=os.getuid(), store=store
    )
    result = context.failure("preflight-artifact-publication-failed")
    assert result["failure_code"] == "preflight-artifact-publication-failed"
    assert result["diagnostic_recorded"] is False
    assert "diagnostic_sha256" not in result
    assert len(list((tmp_path / "preflight-diagnostics").iterdir())) == 1


@pytest.mark.parametrize(
    "attack", ["symlink", "hardlink", "mode", "tamper", "oversized", "duplicate-json"]
)
def test_store_rejects_unsafe_records(tmp_path: Path, attack: str) -> None:
    store, record = _record(tmp_path)
    digest = store.publish(record)
    path = tmp_path / "preflight-diagnostics" / (digest + ".json")
    if attack == "symlink":
        target = tmp_path / "target"
        path.rename(target)
        path.symlink_to(target)
    elif attack == "hardlink":
        os.link(path, tmp_path / "second-link")
    elif attack == "mode":
        path.chmod(0o644)
    elif attack == "tamper":
        path.write_text("{}")
    elif attack == "oversized":
        path.write_bytes(b"x" * (diagnostics.MAX_RECORD_BYTES + 1))
    else:
        payload = b'{"passed":false,"passed":false}\n'
        path.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        path.rename(path.with_name(digest + ".json"))
    with pytest.raises((diagnostics.DiagnosticStoreError, OSError)):
        store.read(digest)


def test_store_rejects_root_symlinks_wrong_uid_and_traversal(tmp_path: Path) -> None:
    store, record = _record(tmp_path)
    digest = store.publish(record)
    with pytest.raises(diagnostics.DiagnosticStoreError):
        diagnostics.PreflightDiagnosticStore(tmp_path, service_uid=os.getuid() + 1).read(digest)
    with pytest.raises(diagnostics.DiagnosticStoreError):
        store.read("../" + digest)
    link = tmp_path / "state-link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(OSError):
        diagnostics.PreflightDiagnosticStore(link).read(digest)


def test_public_retrieval_authenticates_without_building_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bundle = fakes(tmp_path)
    bundle.config.state_root.mkdir(mode=0o700)
    store, record = _record(bundle.config.state_root)
    digest = store.publish(record)
    monkeypatch.setattr(broker.OperatorConfig, "load", lambda *args, **kwargs: bundle.config)
    monkeypatch.setattr(
        broker, "caller_from_sudo", lambda *args, **kwargs: bundle.dependencies.authenticate()
    )

    def forbidden(*args, **kwargs):  # type: ignore[no-untyped-def]
        pytest.fail("retrieval must not compose live dependencies")

    monkeypatch.setattr(broker, "_default_dependencies", forbidden)
    assert main(["--env", "staging", "preflight-diagnostics", digest]) == 0
    assert json.loads(capsys.readouterr().out) == record

    def reject(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise PolicyError("no authority")

    monkeypatch.setattr(broker, "caller_from_sudo", reject)
    assert main(["--env", "staging", "preflight-diagnostics", digest]) == 1
    assert capsys.readouterr().out == ""


def test_public_retrieval_rejects_cross_environment_record(tmp_path: Path) -> None:
    bundle = fakes(tmp_path)
    bundle.config.state_root.mkdir(mode=0o700)
    store, record = _record(bundle.config.state_root)
    digest = store.publish({**record, "environment": "prod"})
    assert main(["preflight-diagnostics", digest], dependencies=bundle.dependencies) == 1
    assert bundle.stdout.getvalue() == ""
    assert bundle.order == []


@pytest.mark.parametrize("argv", [["preflight"], ["start"], ["start", "--dry-run"]])
def test_bad_epoch_keeps_failure_before_guard_and_request(tmp_path: Path, argv: list[str]) -> None:
    bundle = fakes(tmp_path)
    guard = FakeMutationGuard(bundle.order)
    deps = replace(
        bundle.dependencies,
        read_mutation_epoch=lambda: -1,
        assess_preflight=lambda *_args: pytest.fail("no assessment on invalid epoch"),
        mutation_guard=guard,
    )
    assert main(argv, dependencies=deps) == 1
    result = json.loads(bundle.stderr.getvalue())
    assert result["failure_code"] == "preflight-internal-error"
    assert result["stage"] == "mutation-epoch"
    assert guard.acquired == []
    assert bundle.store.requests == bundle.store.preflight_requests == {}


def test_fsync_failure_preserves_primary_code_and_does_not_advertise_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _record_value = _record(tmp_path)

    def fail(_fd):  # type: ignore[no-untyped-def]
        raise OSError("arbitrary-secret-from-filesystem")

    monkeypatch.setattr(diagnostics.os, "fsync", fail)
    context = diagnostics.PreflightDiagnosticContext(
        "staging", "start", stage="assessment", initiator_uid=os.getuid(), store=store
    )
    result = context.failure("preflight-internal-error")
    assert result["failure_code"] == "preflight-internal-error"
    assert result["diagnostic_recorded"] is False
    assert "diagnostic_sha256" not in result
    assert "arbitrary-secret" not in json.dumps(result)
    assert not list((tmp_path / "preflight-diagnostics").glob("*.tmp"))


def test_store_refuses_unsafe_root_mode_without_repair(tmp_path: Path) -> None:
    store, record = _record(tmp_path)
    tmp_path.chmod(0o755)
    with pytest.raises(diagnostics.DiagnosticStoreError):
        store.publish(record)
    assert tmp_path.stat().st_mode & 0o777 == 0o755


@pytest.mark.parametrize(
    "identity",
    [
        {},
        {"SUDO_USER": "secret-bad/user"},
        {"SUDO_USER": "qianyi", "SUDO_UID": "secret", "SUDO_GID": "123"},
        {"SUDO_USER": "qianyi", "SUDO_UID": "123", "SUDO_GID": ""},
    ],
)
def test_installed_shell_rejects_identity_with_typed_json_before_python(
    identity: dict[str, str],
) -> None:
    repository = Path(__file__).resolve().parents[4]
    wrapper = repository / "deploy/staging-rollout/loom-rollout-broker"
    result = subprocess.run(
        ["/bin/sh", str(wrapper), "--env", "staging", "preflight"],
        env=identity,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert payload["failure_code"] == "preflight-authorization-rejected"
    assert payload["stage"] == "authorization"
    assert payload["diagnostic_recorded"] is False
    assert "secret" not in result.stderr

    class ShellRunner:
        def run(self, _argv, **_kwargs):  # type: ignore[no-untyped-def]
            return result

    normalized = host.HostSystem(ShellRunner()).run_post_install_preflight()
    assert normalized["blocker_codes"] == ["preflight-authorization-rejected"]
