from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_backup_guard import BackupTraversalLimits, write_backup_manifest
from loom_cli.rollout.operator import envelope as envelope_module
from loom_cli.rollout.operator.backup_limits import operator_backup_traversal_limits
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.envelope import EnvelopeValidationError, load_validated_envelope
from loom_cli.rollout.operator.model import (
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    RolloutRequest,
)


def _private_dir(path: Path) -> Path:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.chmod(0o700)
    return path


def _private_file(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _config(tmp_path: Path) -> OperatorConfig:
    runner = tmp_path / "runner" / "repo"
    state = _private_dir(tmp_path / "state")
    rollout = _private_dir(tmp_path / "rollout")
    runtime = _private_dir(tmp_path / "runtime")
    cluster_config = _private_file(
        runner / "deploy" / "environments" / "staging.cluster.toml",
        b'image_tag = "staging-placeholder"\n',
    )
    config_path = _private_file(tmp_path / "staging-rollout.toml", b"fixed config\n")
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=runner,
        state_root=state,
        runtime_root=runtime,
        rollout_root=rollout,
        kubeconfig_path=state / "kubeconfig",
        cluster_config_path=cluster_config,
        admin_token_source=f"file:{state}/credentials/admin-token",
        worker_token_source=f"file:{state}/credentials/worker-token",
        service_token_source=f"file:{state}/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=config_path,
        config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
    )


def _backup(config: OperatorConfig, request_id: str) -> tuple[Path, str]:
    root = _private_dir(config.rollout_root / "backups" / f"20260713T200000Z-{request_id}")
    _private_dir(root / "postgres")
    postgres = _private_file(root / "postgres" / "loom.dump", b"postgres backup\n")
    minio = _private_dir(root / "minio")
    _private_dir(minio / "objects")
    _private_file(minio / "objects" / "one.bin", b"minio object\n")
    secrets = _private_dir(root / "k8s-secrets")
    _private_file(secrets / "loom-secrets.yaml", b"apiVersion: v1\nkind: Secret\n")
    manifest = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
        now=datetime.now(UTC),
    )
    manifest.chmod(0o600)
    return manifest, hashlib.sha256(manifest.read_bytes()).hexdigest()


def _envelope(config: OperatorConfig, **overrides: object) -> DriverEnvelope:
    request_id = str(overrides.get("request_id", "stg-20260713-abcdef12"))
    manifest, digest = _backup(config, request_id)
    values: dict[str, object] = {
        "schema_version": 1,
        "request_id": request_id,
        "rollout_id": "staging-abcdef1",
        "initiating_operator": "hongjian",
        "initiating_uid": 2002,
        "attempt_number": 1,
        "attempt_operator": "hongjian",
        "attempt_uid": 2002,
        "remote_url": config.remote_url,
        "target_ref": "origin/dev",
        "resolved_sha": "abcdef1234567890abcdef1234567890abcdef12",
        "image_tag": "staging-abcdef1",
        "fetched_at": "2026-07-13T20:00:00Z",
        "backup_manifest_path": str(manifest),
        "backup_manifest_sha256": digest,
        "runner_config_sha256": config.config_sha256,
        "cluster_name": config.cluster_name,
        "namespace": config.namespace,
        "environment": config.environment,
        "cp_url": config.cp_url,
        "cluster_config_path": str(config.cluster_config_path),
        "rollout_root": str(config.rollout_root),
        "admin_token_source": config.admin_token_source,
        "worker_token_source": config.worker_token_source,
        "service_token_source": config.service_token_source,
        "expect_admin_token_fingerprint": config.expect_admin_token_fingerprint,
        "smoke_on_behalf_username": config.smoke_on_behalf_username,
        "smoke_on_behalf_team_id": config.smoke_on_behalf_team_id,
        "scope": config.scope,
        "gb10_prep_concurrency": config.gb10_prep_concurrency,
        "resume": False,
    }
    values.update(overrides)
    return DriverEnvelope(**values)  # type: ignore[arg-type]


def _publish(config: OperatorConfig, envelope: DriverEnvelope) -> Path:
    _private_dir(config.state_root / "requests")
    request_dir = _private_dir(config.state_root / "requests" / envelope.request_id)
    request = RolloutRequest(
        request_id=envelope.request_id,
        rollout_id=envelope.rollout_id,
        caller=CallerIdentity(
            username=envelope.initiating_operator,
            uid=envelope.initiating_uid,
        ),
        candidate=CandidateBinding(
            remote_url=envelope.remote_url,
            target_ref=envelope.target_ref,
            resolved_sha=envelope.resolved_sha,
            image_tag=envelope.image_tag,
            fetched_at=envelope.fetched_at,
        ),
        requested_at="2026-07-13T20:00:01Z",
        runner_config_sha256=envelope.runner_config_sha256,
    )
    _private_file(
        request_dir / "request.json",
        (json.dumps(request.to_dict(), sort_keys=True) + "\n").encode(),
    )
    _private_dir(request_dir / "attempts")
    attempt_dir = _private_dir(request_dir / "attempts" / str(envelope.attempt_number))
    path = attempt_dir / "envelope.json"
    _private_file(
        path,
        (json.dumps(envelope.to_dict(), sort_keys=True) + "\n").encode(),
    )
    return path


def test_load_validated_envelope_accepts_exact_private_binding(tmp_path: Path) -> None:
    config = _config(tmp_path)
    envelope = _envelope(config)
    path = _publish(config, envelope)

    loaded = load_validated_envelope(path, config, effective_uid=os.geteuid())

    assert loaded == envelope


def test_operator_backup_limits_follow_reviewed_config(tmp_path: Path) -> None:
    config = _config(tmp_path)

    limits = operator_backup_traversal_limits(config)

    assert limits.max_files == config.backup_max_objects + 4
    assert limits.max_entries == config.backup_max_entries
    assert limits.max_total_bytes == 16 * 1024**4


def test_envelope_revalidation_uses_operator_backup_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    envelope = _envelope(config)
    path = _publish(config, envelope)
    captured: list[BackupTraversalLimits] = []
    original = envelope_module.validate_backup_manifest

    def validating_with_capture(*args: object, **kwargs: object) -> list[str]:
        limits = kwargs.get("limits")
        assert isinstance(limits, BackupTraversalLimits)
        captured.append(limits)
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        envelope_module,
        "validate_backup_manifest",
        validating_with_capture,
    )

    loaded = load_validated_envelope(path, config, effective_uid=os.geteuid())

    assert loaded == envelope
    assert captured == [operator_backup_traversal_limits(config)]


@pytest.mark.parametrize("mode", [0o640, 0o660, 0o644])
def test_envelope_rejects_non_private_mode(tmp_path: Path, mode: int) -> None:
    config = _config(tmp_path)
    path = _publish(config, _envelope(config))
    path.chmod(mode)

    with pytest.raises(EnvelopeValidationError, match="mode 0600"):
        load_validated_envelope(path, config, effective_uid=os.geteuid())


def test_envelope_rejects_symlink_and_path_outside_request_store(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = _publish(config, _envelope(config))
    link = path.with_name("linked-envelope.json")
    link.symlink_to(path)
    outside = _private_file(tmp_path / "outside-envelope.json", path.read_bytes())

    with pytest.raises(EnvelopeValidationError, match="request store"):
        load_validated_envelope(link, config, effective_uid=os.geteuid())
    with pytest.raises(EnvelopeValidationError, match="request store"):
        load_validated_envelope(outside, config, effective_uid=os.geteuid())


def test_envelope_rejects_wrong_effective_owner(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = _publish(config, _envelope(config))

    with pytest.raises(EnvelopeValidationError, match="service UID"):
        load_validated_envelope(path, config, effective_uid=os.geteuid() + 1)


def test_envelope_rejects_runner_config_drift(tmp_path: Path) -> None:
    config = _config(tmp_path)
    envelope = replace(_envelope(config), runner_config_sha256="f" * 64)
    path = _publish(config, envelope)

    with pytest.raises(EnvelopeValidationError, match="runner config digest"):
        load_validated_envelope(path, config, effective_uid=os.geteuid())


def test_envelope_rejects_original_attribution_rewritten_after_request(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    original = _envelope(config)
    path = _publish(config, original)
    rewritten = replace(
        original,
        initiating_operator="devansh",
        initiating_uid=2501,
    )
    _private_file(
        path,
        (json.dumps(rewritten.to_dict(), sort_keys=True) + "\n").encode(),
    )

    with pytest.raises(EnvelopeValidationError, match="immutable request binding"):
        load_validated_envelope(path, config, effective_uid=os.geteuid())


def test_resume_envelope_rejects_candidate_rewritten_with_request_record(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    first = _envelope(config)
    _publish(config, first)
    second = replace(
        first,
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2501,
        resolved_sha="b" * 40,
        image_tag="staging-bbbbbbb",
        resume=True,
    )
    second_path = _publish(config, second)

    with pytest.raises(EnvelopeValidationError, match="first attempt binding"):
        load_validated_envelope(second_path, config, effective_uid=os.geteuid())


def test_envelope_rejects_preview_request_record(tmp_path: Path) -> None:
    config = _config(tmp_path)
    envelope = _envelope(config)
    path = _publish(config, envelope)
    request_path = path.parents[2] / "request.json"
    request_payload = json.loads(request_path.read_text(encoding="utf-8"))
    request_payload["status"] = "preview"
    _private_file(
        request_path,
        (json.dumps(request_payload, sort_keys=True) + "\n").encode(),
    )

    with pytest.raises(EnvelopeValidationError, match="pending request"):
        load_validated_envelope(path, config, effective_uid=os.geteuid())


def test_resume_rejects_non_initial_attempt_one_anchor(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first = _envelope(config)
    first_path = _publish(config, first)
    second = replace(
        first,
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2501,
        resume=True,
    )
    second_path = _publish(config, second)
    invalid_anchor = replace(first, attempt_number=2, resume=True)
    _private_file(
        first_path,
        (json.dumps(invalid_anchor.to_dict(), sort_keys=True) + "\n").encode(),
    )

    with pytest.raises(EnvelopeValidationError, match="first attempt identity"):
        load_validated_envelope(second_path, config, effective_uid=os.geteuid())


def test_envelope_rejects_replaced_backup_manifest(tmp_path: Path) -> None:
    config = _config(tmp_path)
    envelope = _envelope(config)
    path = _publish(config, envelope)
    manifest = Path(envelope.backup_manifest_path)
    manifest.write_text("{}\n", encoding="utf-8")
    manifest.chmod(0o600)

    with pytest.raises(EnvelopeValidationError, match="backup manifest"):
        load_validated_envelope(path, config, effective_uid=os.geteuid())


def test_envelope_rejects_missing_backup_component(tmp_path: Path) -> None:
    config = _config(tmp_path)
    envelope = _envelope(config)
    path = _publish(config, envelope)
    manifest = Path(envelope.backup_manifest_path)
    document = json.loads(manifest.read_text())
    component = Path(document["components"]["postgres"]["path"])
    component.unlink()

    with pytest.raises(EnvelopeValidationError, match="backup component 'postgres'"):
        load_validated_envelope(path, config, effective_uid=os.geteuid())


def test_non_dry_staging_without_envelope_refuses_before_git_or_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", "staging"])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert not (tmp_path / "rollouts").exists()


def test_envelope_mode_rejects_explicit_manual_override_by_argv_presence(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "cluster",
            "rollout",
            "staging",
            "--request-envelope",
            "/var/lib/loom-staging-rollout/requests/request-a/attempts/1/envelope.json",
            "--image-tag",
            "staging-abcdef1",
        ]
    )

    assert rc == 2
    assert "manual rollout overrides are forbidden in envelope mode" in capsys.readouterr().err


@pytest.mark.parametrize(
    "override",
    [
        ["--ref", "origin/dev"],
        ["--image-tag", "staging-abcdef1"],
        ["--cluster-name", "loom-staging"],
        ["--namespace", "loom-staging"],
        ["--environment", "staging"],
        ["--cp-url", "http://127.0.0.1:18081"],
        ["--admin-token", "file:/private/staging-admin-token"],
        ["--expect-admin-token-fingerprint", "sha256:abc123def456 len=64"],
        ["--worker-token", "file:/private/staging-worker-token"],
        ["--service-token", "file:/private/staging-service-token"],
        ["--smoke-submit-mode", "admin-on-behalf"],
        ["--smoke-api-token", "file:/private/staging-smoke-token"],
        ["--smoke-task-id", "loom-smoke/gb10-oracle-hello-world"],
        ["--smoke-required-worker-pool", "gb10"],
        ["--smoke-agent", "oracle"],
        ["--smoke-on-behalf-username", "devansh"],
        ["--smoke-on-behalf-team-id", "11111111-1111-4111-8111-111111111111"],
        ["--smoke-admin-actor", "loom-staging-rollout"],
        ["--cluster-config", "/srv/loom/deploy/environments/staging.cluster.toml"],
        ["--backup-manifest", "/data/loom-staging/backups/fixed/backup-manifest.json"],
        ["--backup-manifest-min-remaining-hours", "2"],
        ["--rollout-root", "/data/loom-staging"],
        ["--scope", "current-gb10"],
        ["--gb10-prep-concurrency", "8"],
        ["--exclude-oldlab"],
        ["--dry-run"],
    ],
    ids=lambda value: value[0].removeprefix("--"),
)
def test_envelope_mode_rejects_every_explicit_manual_option_before_load(
    override: list[str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "loom_cli.rollout.cli.OperatorConfig.load",
        lambda *_args, **_kwargs: pytest.fail(
            "manual override rejection must precede config/envelope load"
        ),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "staging",
            "--request-envelope",
            "/var/lib/loom-staging-rollout/requests/request-a/attempts/1/envelope.json",
            *override,
        ]
    )

    assert rc == 2
    assert "manual rollout overrides are forbidden in envelope mode" in capsys.readouterr().err


@pytest.mark.parametrize(
    "protected_alias",
    [
        ["--environment", "staging"],
        ["--cluster-name", "loom-staging"],
        ["--namespace", "loom-staging"],
        ["--rollout-root", "/data/loom-staging"],
        ["--rollout-root", "/data/loom-staging/child"],
        ["--rollout-root", "/data//loom-staging"],
        ["--rollout-root", "/data/./loom-staging"],
        ["--rollout-root", "//data/loom-staging"],
    ],
    ids=(
        "environment",
        "cluster",
        "namespace",
        "root",
        "root-child",
        "root-double-separator",
        "root-dot-segment",
        "root-double-leading-separator",
    ),
)
def test_full_argv_staging_alias_refuses_before_git_or_evidence(
    protected_alias: list[str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", *protected_alias])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_symlinked_rollout_root_is_fail_closed_without_following_alias(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = tmp_path / "rollout-root-alias"
    loop.symlink_to(loop)
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--rollout-root",
            str(loop / "child"),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_broken_symlink_rollout_root_refuses_before_git_or_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias = tmp_path / "broken-rollout-root-alias"
    alias.symlink_to(tmp_path / "missing-rollout-root")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--rollout-root",
            str(alias / "child"),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_symlink_to_staging_rollout_root_refuses_before_git_or_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias = tmp_path / "staging-rollout-root-alias"
    alias.symlink_to("/data/loom-staging")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", "--rollout-root", str(alias / "child")])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_benign_symlink_rollout_root_is_not_misclassified_as_staging(
    tmp_path: Path,
) -> None:
    from loom_cli.rollout.cli import _rollout_root_is_protected_or_unsafe

    target = tmp_path / "ordinary-rollout-root"
    (target / "child").mkdir(parents=True)
    alias = tmp_path / "ordinary-rollout-root-alias"
    alias.symlink_to(target)

    assert not _rollout_root_is_protected_or_unsafe(str(alias / "child"))


def test_existing_physical_staging_alias_is_classified_as_protected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout import cli as rollout_cli

    staging_root = _private_dir(tmp_path / "physical-staging")
    protected_child = _private_dir(staging_root / "backups")
    alias = tmp_path / "staging-alias"
    alias.symlink_to(staging_root)
    monkeypatch.setattr(rollout_cli, "_STAGING_DATA_ROOT", str(staging_root))

    assert rollout_cli._rollout_root_is_protected_or_unsafe(str(alias / protected_child.name))


def test_non_dry_manual_rollout_rejects_stable_symlink_root_before_git_or_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _private_dir(tmp_path / "ordinary-rollout-root")
    alias = tmp_path / "ordinary-rollout-root-alias"
    alias.symlink_to(target)
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        b'image_tag = "ordinary"\n',
    )
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run for an unsafe path binding"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(alias),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_non_dry_manual_rollout_rejects_symlinked_rollouts_child_before_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rollout_root = _private_dir(tmp_path / "ordinary-rollout-root")
    fake_staging_rollouts = _private_dir(tmp_path / "fake-staging" / "rollouts")
    (rollout_root / "rollouts").symlink_to(fake_staging_rollouts)
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        b'image_tag = "ordinary"\n',
    )
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run for an unsafe evidence root"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(rollout_root),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


@pytest.mark.parametrize(
    "changed_path",
    ["rollout-root", "cluster-config", "backup-manifest", "config-storage-root"],
)
def test_non_dry_manual_rollout_rejects_path_identity_change_before_git_or_evidence(
    changed_path: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout import cli as rollout_cli

    rollout_root = _private_dir(tmp_path / "ordinary-rollout-root")
    storage_root = _private_dir(tmp_path / "ordinary-storage-root")
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        (
            f'image_tag = "ordinary"\npersistent_storage_host_path_root = "{storage_root}"\n'
        ).encode(),
    )
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")
    original_validate = rollout_cli._validate_required_args

    def validate_then_change_path(args: object) -> str | None:
        error = original_validate(args)  # type: ignore[arg-type]
        if changed_path == "rollout-root":
            rollout_root.rename(tmp_path / "original-rollout-root")
            rollout_root.symlink_to("/data/loom-staging")
        elif changed_path == "cluster-config":
            cluster_config.rename(tmp_path / "original-cluster.toml")
            cluster_config.symlink_to(
                rollout_cli._REPO_ROOT / "deploy" / "environments" / "staging.cluster.toml"
            )
        elif changed_path == "backup-manifest":
            backup_manifest.rename(tmp_path / "original-backup.json")
            backup_manifest.symlink_to("/data/loom-staging/backups/latest/backup-manifest.json")
        else:
            storage_root.rename(tmp_path / "original-storage-root")
            storage_root.symlink_to("/data/loom-staging")
        return error

    monkeypatch.setattr(
        "loom_cli.rollout.cli._validate_required_args",
        validate_then_change_path,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run after path identity drift"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(rollout_root),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_non_dry_manual_rollout_revalidates_path_identity_before_driver(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rollout_root = _private_dir(tmp_path / "ordinary-rollout-root")
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        b'image_tag = "ordinary"\n',
    )
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")

    def dependency_check_then_change_path() -> None:
        rollout_root.rename(tmp_path / "original-rollout-root")
        rollout_root.symlink_to("/data/loom-staging")
        return None

    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: "a" * 40,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli._rollout_runner_dependency_error",
        dependency_check_then_change_path,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.run_rollout",
        lambda *_args, **_kwargs: pytest.fail("driver must not run after path identity drift"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(rollout_root),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_non_dry_manual_rollout_binds_cluster_config_content_digest(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout import cli as rollout_cli

    rollout_root = _private_dir(tmp_path / "ordinary-rollout-root")
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        b'image_tag = "ordinary"\n',
    )
    original_config_metadata = cluster_config.stat()
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")
    original_validate = rollout_cli._validate_required_args

    def validate_then_rewrite_config(args: object) -> str | None:
        error = original_validate(args)  # type: ignore[arg-type]
        replacement = b'image_tag = "changed!"\n'
        assert len(replacement) == original_config_metadata.st_size
        cluster_config.write_bytes(replacement)
        os.utime(
            cluster_config,
            ns=(
                original_config_metadata.st_atime_ns,
                original_config_metadata.st_mtime_ns,
            ),
        )
        return error

    monkeypatch.setattr(
        "loom_cli.rollout.cli._validate_required_args",
        validate_then_rewrite_config,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run after config content drift"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(rollout_root),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_non_dry_manual_rollout_classifies_the_captured_config_before_git(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout import cli as rollout_cli

    rollout_root = _private_dir(tmp_path / "ordinary-rollout-root")
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        b'image_tag = "ordinary"\n',
    )
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")
    original_classifier = rollout_cli._is_protected_staging_request
    first_classification = True

    def classify_then_rewrite_config(args: object) -> bool:
        nonlocal first_classification
        protected = original_classifier(args)  # type: ignore[arg-type]
        if first_classification:
            first_classification = False
            cluster_config.write_text('namespace = "loom-staging"\n', encoding="utf-8")
        return protected

    monkeypatch.setattr(
        "loom_cli.rollout.cli._is_protected_staging_request",
        classify_then_rewrite_config,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail(
            "Git must not run when the captured config targets staging"
        ),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(rollout_root),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_non_dry_manual_rollout_binds_every_parent_component(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout import cli as rollout_cli

    rollout_parent = _private_dir(tmp_path / "rollout-parent")
    rollout_root = _private_dir(rollout_parent / "ordinary-rollout-root")
    cluster_config = _private_file(
        tmp_path / "cluster.toml",
        b'image_tag = "ordinary"\n',
    )
    backup_manifest = _private_file(tmp_path / "backup.json", b"{}\n")
    original_validate = rollout_cli._validate_required_args

    def validate_then_replace_parent(args: object) -> str | None:
        error = original_validate(args)  # type: ignore[arg-type]
        old_parent = tmp_path / "original-rollout-parent"
        rollout_parent.rename(old_parent)
        replacement_parent = _private_dir(rollout_parent)
        (old_parent / rollout_root.name).rename(replacement_parent / rollout_root.name)
        return error

    monkeypatch.setattr(
        "loom_cli.rollout.cli._validate_required_args",
        validate_then_replace_parent,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run after parent identity drift"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "--ref",
            "origin/dev",
            "--image-tag",
            "ordinary-abcdef1",
            "--cluster-name",
            "loom-test",
            "--namespace",
            "loom",
            "--environment",
            "test",
            "--cp-url",
            "http://127.0.0.1:18081",
            "--cluster-config",
            str(cluster_config),
            "--backup-manifest",
            str(backup_manifest),
            "--rollout-root",
            str(rollout_root),
        ]
    )

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


@pytest.mark.parametrize(
    "config_body",
    [
        'namespace = "loom-staging"\n',
        'runtime_environment = "staging"\n',
        'persistent_storage_host_path_root = "/data//loom-staging"\n',
    ],
    ids=("namespace", "runtime", "storage-root-alias"),
)
def test_staging_cluster_config_identity_refuses_before_git_or_evidence(
    config_body: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "cluster.toml"
    config.write_text(config_body, encoding="utf-8")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", "--cluster-config", str(config)])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_staging_storage_root_symlink_in_cluster_config_refuses_early(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_alias = tmp_path / "staging-storage"
    storage_alias.symlink_to("/data/loom-staging")
    config = tmp_path / "cluster.toml"
    config.write_text(
        f'persistent_storage_host_path_root = "{storage_alias}"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", "--cluster-config", str(config)])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


@pytest.mark.parametrize("unsafe_kind", ["symlink", "oversized"])
def test_unsafe_cluster_config_fails_closed_before_git_or_evidence(
    unsafe_kind: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "cluster.toml"
    if unsafe_kind == "symlink":
        target = tmp_path / "ordinary.toml"
        target.write_text('namespace = "loom"\n', encoding="utf-8")
        config.symlink_to(target)
    else:
        config.write_text("#" + ("x" * 300_000) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", "--cluster-config", str(config)])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


@pytest.mark.parametrize("via_symlink", [False, True], ids=("direct", "symlink"))
def test_staging_backup_path_refuses_before_git_or_evidence(
    via_symlink: bool,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if via_symlink:
        backup_root = tmp_path / "staging-backups"
        backup_root.symlink_to("/data/loom-staging/backups")
        manifest = backup_root / "latest" / "backup-manifest.json"
    else:
        manifest = Path("/data/./loom-staging/backups/latest/backup-manifest.json")
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("Git must not run before envelope refusal"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("evidence must not be inspected"),
    )

    rc = main(["cluster", "rollout", "--backup-manifest", str(manifest)])

    assert rc == 2
    assert "broker-created request envelope is required" in capsys.readouterr().err


def test_envelope_mode_constructs_context_without_git_or_evidence_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    envelope = _envelope(config)
    path = _publish(config, envelope)
    captured: dict[str, object] = {}

    monkeypatch.setenv("LOOM_STAGING_ROLLOUT_CONFIG", str(config.config_path))
    monkeypatch.setattr(
        "loom_cli.rollout.cli.OperatorConfig.load",
        lambda *_args, **_kwargs: config,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.resolve_ref_to_sha",
        lambda *_args, **_kwargs: pytest.fail("envelope mode must not resolve Git refs"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli.EvidenceDirectory.find_in_progress",
        lambda *_args, **_kwargs: pytest.fail("envelope mode uses exact rollout id"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.cli._rollout_runner_dependency_error",
        lambda: None,
    )

    def fake_run_rollout(ctx, steps, evidence):
        captured.update(ctx=ctx, evidence=evidence)
        return 0

    monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

    rc = main(
        [
            "cluster",
            "rollout",
            "staging",
            "--request-envelope",
            str(path),
        ]
    )

    assert rc == 0
    ctx = captured["ctx"]
    assert ctx.request_id == envelope.request_id
    assert ctx.initiating_operator == "hongjian"
    assert ctx.initiating_uid == 2002
    assert ctx.attempt_number == 1
    assert ctx.attempt_operator == "hongjian"
    assert ctx.attempt_uid == 2002
    assert ctx.request_envelope_path == path
    limits = operator_backup_traversal_limits(config)
    assert ctx.backup_traversal_limits() == (
        limits.max_files,
        limits.max_entries,
        limits.max_total_bytes,
    )
    assert ctx.to_inputs_dict()["backup_manifest_traversal_limits"] == {
        "max_files": limits.max_files,
        "max_entries": limits.max_entries,
        "max_total_bytes": limits.max_total_bytes,
    }
    assert captured["evidence"].rollout_id == envelope.rollout_id


def test_envelope_mode_requires_resume_flag_to_match_envelope(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    path = _publish(config, _envelope(config))
    monkeypatch.setenv("LOOM_STAGING_ROLLOUT_CONFIG", str(config.config_path))
    monkeypatch.setattr(
        "loom_cli.rollout.cli.OperatorConfig.load",
        lambda *_args, **_kwargs: config,
    )

    rc = main(
        [
            "cluster",
            "rollout",
            "staging",
            "--request-envelope",
            str(path),
            "--resume",
        ]
    )

    assert rc == 2
    assert "resume flag does not match request envelope" in capsys.readouterr().err
