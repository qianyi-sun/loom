from __future__ import annotations

import hashlib
import importlib
import io
import json
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from loom.personal_dev_control_plane_config import (
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_acceptance_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    PersonalDevAcceptanceStatus,
    PersonalDevShadowComponent,
    PersonalDevShadowStatus,
)
from loom_cli.__main__ import main
from loom_cli.admin_cmd import dispatch

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_NOW = datetime(2026, 8, 17, 21, 0, 0, tzinfo=UTC)


def _release_value() -> dict[str, object]:
    return {
        "schema_version": 2,
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "personal_dev_scanner_cache": (
                "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "a" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
        },
        "scanner": {
            "binary_platform": "linux/amd64",
            "binary_sha256": "b" * 64,
            "cache_identity_sha256": (
                "b1c136b8577f3813c62588d6930db21b0f2343b7f70278836741387c43c33761"
            ),
            "database_metadata_sha256": "c" * 64,
            "database_sha256": "d" * 64,
            "java_database_metadata_sha256": "e" * 64,
            "java_database_sha256": "f" * 64,
            "lock_sha256": "1" * 64,
            "trivy_version": "v0.70.0",
        },
        "release_evidence_sha256": "8" * 64,
    }


def _release(tmp_path: Path) -> tuple[Path, str]:
    payload = json.dumps(
        _release_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    path = tmp_path / "trusted-release.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    return path, hashlib.sha256(payload).hexdigest()


def _acceptance_plan(
    tmp_path: Path,
    release_path: Path,
    release_digest: str,
):
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    shadow = render_shadow_personal_dev_control_plane(profile, release)
    protocol_sha256 = hashlib.sha256(
        json.dumps(
            dict(profile.protocol_versions),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    value = {
        "acceptance_owners": [
            {
                "team_id": "00000000-0000-0000-0000-000000000201",
                "user_id": "00000000-0000-0000-0000-000000000301",
            },
            {
                "team_id": "00000000-0000-0000-0000-000000000202",
                "user_id": "00000000-0000-0000-0000-000000000302",
            },
        ],
        "activation": {
            "key_id": "personal-dev-agent-v1",
            "public_key_sha256": "c" * 64,
        },
        "builder": {
            "protocol_map_sha256": protocol_sha256,
            "publisher_identity": profile.builder.publisher_identity,
            "registry_prefix": profile.builder.registry_prefix,
            "runtime_class_name": profile.builder.runtime_class_name,
            "runtime_handler": profile.builder.runtime_handler,
            "runtime_profile_sha256": profile.builder.runtime_profile_sha256,
            "scanner_binary_sha256": release.scanner.binary_sha256,
            "scanner_cache_identity_sha256": release.scanner.cache_identity_sha256,
            "scanner_database_sha256": release.scanner.database_sha256,
            "scanner_database_metadata_sha256": (release.scanner.database_metadata_sha256),
            "scanner_finding_policy_sha256": "3" * 64,
            "scanner_java_database_sha256": release.scanner.java_database_sha256,
            "scanner_java_database_metadata_sha256": (
                release.scanner.java_database_metadata_sha256
            ),
            "trusted_launcher_profile_sha256": "e" * 64,
        },
        "manager": {
            "authority_incarnation": "00000000-0000-0000-0000-000000000101",
            "configuration_epoch": 7,
            "executable_new_capacity_ceiling": 0,
            "execution_epoch": 11,
            "execution_state": "prepared",
        },
        "principals": {
            "lifecycle_principal_id": "personal-dev-lifecycle",
            "reporter_principal_id": "personal-dev-reporter",
        },
        "quotas": {
            "builder_global_concurrency": profile.limits.builder_global_concurrency,
            "builder_per_owner_concurrency": profile.limits.builder_per_owner_concurrency,
            "candidate_retained_bytes": profile.limits.candidate_retained_bytes,
            "candidate_retained_count": profile.limits.candidate_retained_count,
            "global_live_instances": profile.limits.global_live_instances,
            "per_owner_aggregate_max_slots": profile.limits.per_owner_aggregate_max_slots,
            "per_owner_aggregate_min_slots": profile.limits.per_owner_aggregate_min_slots,
            "per_owner_live_instances": profile.limits.per_owner_live_instances,
            "source_max_archive_bytes": profile.limits.source_max_archive_bytes,
        },
        "release": {
            "images": release.canonical_value()["images"],
            "release_evidence_sha256": release.release_evidence_sha256,
            "shadow_manifest_sha256": hashlib.sha256(shadow.yaml_text.encode("utf-8")).hexdigest(),
            "trusted_release_sha256": hashlib.sha256(release.canonical_bytes()).hexdigest(),
        },
        "schema_version": 1,
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "storage": {
            "backup_restore_evidence_sha256": "b" * 64,
            "schema_head": "0107",
        },
        "window": {
            "expires_at": "2099-12-31T23:00:00Z",
            "rollback_expires_at": "2100-01-31T23:00:00Z",
            "started_at": "2026-01-01T00:00:00Z",
        },
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    path = tmp_path / "acceptance-plan.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    digest = hashlib.sha256(payload).hexdigest()
    return path, digest, load_personal_dev_acceptance_plan(path, digest)


def _reviewed_kubeconfig(tmp_path: Path) -> Path:
    path = tmp_path / "reviewed-kubeconfig"
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "clusters": [
                    {
                        "cluster": {
                            "certificate-authority-data": "cmV2aWV3ZWQtY2E=",
                            "server": "https://127.0.0.1:6443",
                        },
                        "name": "reviewed",
                    }
                ],
                "contexts": [
                    {
                        "context": {"cluster": "reviewed", "user": "reviewed"},
                        "name": "reviewed-loom-dev",
                    }
                ],
                "current-context": "reviewed-loom-dev",
                "kind": "Config",
                "preferences": {},
                "users": [{"name": "reviewed", "user": {"token": "reviewed-token"}}],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _argv(release: Path, digest: str, *, profile: Path = _PROFILE) -> list[str]:
    return [
        "personal-dev-control-plane",
        "render",
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
    ]


def _status_argv(
    release: Path,
    digest: str,
    kubeconfig: Path,
    *,
    profile: Path = _PROFILE,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
    ]


def _acceptance_argv(
    release: Path,
    digest: str,
    plan: Path,
    plan_digest: str,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "render-acceptance",
        "--file",
        str(_PROFILE),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--acceptance-plan-file",
        str(plan),
        "--acceptance-plan-sha256",
        plan_digest,
    ]


def _acceptance_status_argv(
    release: Path,
    digest: str,
    plan: Path,
    plan_digest: str,
    kubeconfig: Path,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status-acceptance",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(_PROFILE),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--acceptance-plan-file",
        str(plan),
        "--acceptance-plan-sha256",
        plan_digest,
    ]


def test_render_emits_exact_yaml_and_canonical_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_shadow_personal_dev_control_plane(profile, release)

    result = dispatch(_argv(release_path, release_digest))

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected.yaml_text
    documents = [item for item in yaml.safe_load_all(captured.out) if item]
    assert len(documents) == expected.resource_count
    evidence = json.loads(captured.err)
    assert (
        captured.err
        == json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    )
    assert evidence == {
        "input_sha256": expected.input_sha256,
        "mode": "shadow",
        "release_sha256": expected.release_sha256,
        "resource_count": expected.resource_count,
        "schema": "loom-personal-dev-control-plane-render-v1",
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "yaml_sha256": hashlib.sha256(expected.yaml_text.encode("utf-8")).hexdigest(),
    }


def test_render_acceptance_requires_exact_plan_and_emits_only_yaml_and_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_acceptance_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )

    result = dispatch(_acceptance_argv(release_path, release_digest, plan_path, plan_digest))

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected.yaml_text
    evidence = json.loads(captured.err)
    assert captured.err == (
        json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    )
    assert evidence == {
        "acceptance_plan_sha256": plan.sha256,
        "input_sha256": expected.input_sha256,
        "mode": "acceptance",
        "release_sha256": expected.release_sha256,
        "resource_count": expected.resource_count,
        "schema": "loom-personal-dev-control-plane-render-v1",
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "yaml_sha256": hashlib.sha256(expected.yaml_text.encode("utf-8")).hexdigest(),
    }


@pytest.mark.parametrize(
    ("operation", "omitted"),
    [
        ("render-acceptance", "--acceptance-plan-file"),
        ("render-acceptance", "--acceptance-plan-sha256"),
        ("status-acceptance", "--acceptance-plan-file"),
        ("status-acceptance", "--acceptance-plan-sha256"),
    ],
)
def test_acceptance_commands_reject_partial_plan_bindings(
    tmp_path: Path,
    operation: str,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    plan_path, plan_digest, _plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    if operation == "render-acceptance":
        argv = _acceptance_argv(release_path, release_digest, plan_path, plan_digest)
    else:
        kubeconfig = _reviewed_kubeconfig(tmp_path)
        argv = _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
        )
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


def test_render_is_byte_deterministic_across_repeated_invocations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)

    assert dispatch(_argv(release_path, release_digest)) == 0
    first = capsys.readouterr()
    assert dispatch(_argv(release_path, release_digest)) == 0
    second = capsys.readouterr()

    assert second == first


@pytest.mark.parametrize(
    "omitted",
    ["--file", "--trusted-release-file", "--trusted-release-sha256"],
)
def test_render_requires_every_trust_binding_argument(
    tmp_path: Path,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    argv = _argv(release_path, release_digest)
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


@pytest.mark.parametrize("option", ["--trusted-release-sha", "--unknown-option"])
def test_render_rejects_abbreviated_and_unknown_options(
    tmp_path: Path,
    option: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    argv = _argv(release_path, release_digest)
    argv.extend([option, "do-not-accept"])

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"unrecognized arguments: {option} do-not-accept" in captured.err


def test_render_rejects_unsafe_release_before_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    release_path.chmod(0o644)

    result = dispatch(_argv(release_path, release_digest))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane render inputs are invalid\n"


def test_render_redacts_invalid_profile_payload_before_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    secret_value = "do-not-log-this-accidental-secret"
    profile = tmp_path / "profile.toml"
    profile.write_text(
        _PROFILE.read_text(encoding="utf-8") + f'\naccidental_secret = "{secret_value}"\n',
        encoding="utf-8",
    )

    result = dispatch(_argv(release_path, release_digest, profile=profile))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane render inputs are invalid\n"
    assert secret_value not in captured.err


def test_render_handles_broken_stdout_without_false_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_path, release_digest = _release(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    class _BrokenStdout:
        def write(self, _value: str) -> int:
            raise BrokenPipeError

    errors = io.StringIO()
    monkeypatch.setattr(command.sys, "stdout", _BrokenStdout())
    monkeypatch.setattr(command.sys, "stderr", errors)

    assert dispatch(_argv(release_path, release_digest)) == 0
    assert errors.getvalue() == ""


def test_help_describes_only_render_and_read_only_zero_capacity_authority(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["personal-dev-control-plane", "--help"])

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert "render-only" in captured.out
    assert "shadow" in captured.out
    assert "acceptance" in captured.out
    assert "read-only" in captured.out
    assert "physical capacity unchanged" in captured.out
    assert "apply" not in captured.out.casefold()
    assert "activate" not in captured.out.casefold()


@pytest.mark.parametrize("operation", ["apply", "activate"])
def test_personal_control_plane_has_no_apply_or_activate_operation(
    operation: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["personal-dev-control-plane", operation])

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "invalid choice" in captured.err


def test_admin_help_lists_personal_control_plane_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["--help"])

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert "personal-dev-control-plane" in captured.out


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (
            ["service", "up", "--help"],
            "--environment ENVIRONMENT",
        ),
        (["dev", "--help"], "{create,list,status,destroy}"),
    ],
)
def test_personal_control_plane_registration_does_not_extend_service_or_dev(
    argv: list[str],
    expected: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        main(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert expected in captured.out
    assert "personal-dev-control-plane" not in captured.out


@pytest.mark.parametrize("ready", [True, False])
def test_status_emits_one_canonical_record_and_readiness_exit_code(
    tmp_path: Path,
    ready: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    status = PersonalDevShadowStatus(
        ready=ready,
        blockers=() if ready else ("manager_probe_unavailable",),
        input_sha256="a" * 64,
        release_sha256="b" * 64,
        manager_ceiling=0 if ready else None,
        components=(PersonalDevShadowComponent("manager", int(ready), ready),),
    )

    class _Runner:
        def __init__(self, path: Path) -> None:
            assert path == kubeconfig

    def _observe(
        runner: object,
        *,
        expected: object,
        namespace: str,
    ) -> PersonalDevShadowStatus:
        assert isinstance(runner, _Runner)
        assert expected.resource_count == 33
        assert namespace == "loom-dev"
        return status

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    monkeypatch.setattr(command, "observe_personal_dev_shadow_status", _observe)

    result = dispatch(_status_argv(release_path, release_digest, kubeconfig.resolve()))

    captured = capsys.readouterr()
    expected_output = (
        json.dumps(
            status.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    )
    assert result == (0 if ready else 1)
    assert captured.out == expected_output
    assert captured.err == ""


@pytest.mark.parametrize("ready", [True, False])
def test_status_acceptance_emits_one_canonical_read_only_record(
    tmp_path: Path,
    ready: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    status = PersonalDevAcceptanceStatus(
        ready=ready,
        blockers=() if ready else ("manager_binding_drift",),
        input_sha256="a" * 64,
        release_sha256="b" * 64,
        acceptance_plan_sha256=plan.sha256,
        manager_ceiling=0,
        components=(PersonalDevShadowComponent("manager", 1, ready),),
        application_ready=True,
        capacity_publication_ready=ready,
        worker_available=False,
    )

    class _Runner:
        def __init__(self, path: Path) -> None:
            assert path == kubeconfig

    def _observe(
        runner: object,
        *,
        expected: object,
        plan: object,
        namespace: str,
    ) -> PersonalDevAcceptanceStatus:
        assert isinstance(runner, _Runner)
        assert expected.resource_count == 33
        assert plan.sha256 == plan_digest
        assert namespace == "loom-dev"
        return status

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    monkeypatch.setattr(command, "observe_personal_dev_acceptance_status", _observe)

    result = dispatch(
        _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
        )
    )

    captured = capsys.readouterr()
    expected_output = (
        json.dumps(status.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    )
    assert result == (0 if ready else 1)
    assert captured.out == expected_output
    assert captured.err == ""


def test_status_acceptance_rejects_invalid_plan_before_constructing_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    plan_path, plan_digest, _plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    plan_path.chmod(0o644)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    class _UnexpectedRunner:
        def __init__(self, _path: Path) -> None:
            raise AssertionError("invalid acceptance plan reached kubectl")

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _UnexpectedRunner)

    result = dispatch(
        _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
        )
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane status inputs are invalid\n"


@pytest.mark.parametrize(
    "omitted",
    [
        "--kubeconfig",
        "--file",
        "--trusted-release-file",
        "--trusted-release-sha256",
    ],
)
def test_status_requires_kubeconfig_and_every_trust_binding_argument(
    tmp_path: Path,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    argv = _status_argv(release_path, release_digest, kubeconfig.resolve())
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


def test_status_rejects_abbreviated_option(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    argv = _status_argv(release_path, release_digest, kubeconfig.resolve())
    argv.extend(["--trusted-release-sha", "do-not-accept"])

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "unrecognized arguments: --trusted-release-sha do-not-accept" in captured.err


@pytest.mark.parametrize(
    "unsafe",
    [
        "relative",
        "symlink",
        "parent-symlink",
        "symlink-loop",
        "world-readable",
        "hardlink",
        "empty",
        "oversized",
        "external-cluster-ca",
        "external-client-key",
        "exec-credential-plugin",
    ],
)
def test_status_rejects_nonabsolute_or_symlink_kubeconfig_before_observation(
    tmp_path: Path,
    unsafe: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    if unsafe == "relative":
        selected = Path("relative-kubeconfig")
    elif unsafe == "symlink":
        selected = tmp_path / "linked-kubeconfig"
        selected.symlink_to(kubeconfig)
    elif unsafe == "parent-symlink":
        real_parent = tmp_path / "real-parent"
        real_parent.mkdir()
        nested_kubeconfig = real_parent / "reviewed-kubeconfig"
        nested_kubeconfig.write_text("reviewed", encoding="utf-8")
        nested_kubeconfig.chmod(0o600)
        linked_parent = tmp_path / "linked-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)
        selected = linked_parent / "reviewed-kubeconfig"
    elif unsafe == "symlink-loop":
        selected = tmp_path / "looped-kubeconfig"
        selected.symlink_to(selected)
    elif unsafe == "world-readable":
        kubeconfig.chmod(0o644)
        selected = kubeconfig
    elif unsafe == "hardlink":
        selected = tmp_path / "hardlinked-kubeconfig"
        os.link(kubeconfig, selected)
    elif unsafe == "empty":
        kubeconfig.write_bytes(b"")
        selected = kubeconfig
    elif unsafe == "oversized":
        kubeconfig.write_bytes(b"x" * (1024 * 1024 + 1))
        selected = kubeconfig
    else:
        document = yaml.safe_load(kubeconfig.read_text(encoding="utf-8"))
        if unsafe == "external-cluster-ca":
            document["clusters"][0]["cluster"] = {
                "certificate-authority": "/tmp/external-ca.pem",
                "server": "https://127.0.0.1:6443",
            }
        elif unsafe == "external-client-key":
            document["users"][0]["user"] = {"client-key": "/tmp/external-key.pem"}
        else:
            document["users"][0]["user"] = {
                "exec": {"apiVersion": "client.authentication.k8s.io/v1", "command": "helper"}
            }
        kubeconfig.write_text(yaml.safe_dump(document, sort_keys=True), encoding="utf-8")
        selected = kubeconfig
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    def _unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("unsafe kubeconfig reached observation")

    monkeypatch.setattr(command, "observe_personal_dev_shadow_status", _unexpected)

    result = dispatch(_status_argv(release_path, release_digest, selected))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane status inputs are invalid\n"


def test_status_redacts_invalid_release_before_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    release_path.chmod(0o644)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    def _unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid release reached observation")

    monkeypatch.setattr(command, "observe_personal_dev_shadow_status", _unexpected)

    result = dispatch(_status_argv(release_path, release_digest, kubeconfig.resolve()))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane status inputs are invalid\n"


def test_status_subprocess_runner_stops_at_combined_output_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\nimport sys\nsys.stdout.write('x' * (4 * 1024 * 1024 + 1))\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    runner = command._SubprocessKubectlRunner(kubeconfig)

    with pytest.raises(OSError, match="output exceeds"):
        runner.run(["get", "namespaces"], timeout_seconds=5)


def test_status_subprocess_runner_returns_bounded_stdout_stderr_and_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "sys.stdout.write('bounded-out')\n"
        "sys.stderr.write('bounded-err')\n"
        "raise SystemExit(3)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    runner = command._SubprocessKubectlRunner(kubeconfig)

    result = runner.run(["get", "namespaces"], timeout_seconds=5)

    assert result.args == [
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "get",
        "namespaces",
    ]
    assert result.returncode == 3
    assert result.stdout == "bounded-out"
    assert result.stderr == "bounded-err"


def test_status_subprocess_runner_rejects_kubeconfig_change_during_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "pathlib.Path(os.environ['LOOM_TEST_KUBECONFIG']).write_text('changed', encoding='utf-8')\n"
        "sys.stdout.write('{}')\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    monkeypatch.setenv("LOOM_TEST_KUBECONFIG", str(kubeconfig))
    runner = command._SubprocessKubectlRunner(kubeconfig)

    with pytest.raises(OSError, match="kubeconfig changed during observation"):
        runner.run(["get", "namespaces"], timeout_seconds=5)


def test_status_subprocess_runner_pins_kubeconfig_against_swap_and_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = tmp_path / "ready"
    proceed = tmp_path / "proceed"
    consumed = tmp_path / "consumed"
    restored = tmp_path / "restored"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "import time\n"
        "ready = pathlib.Path(os.environ['LOOM_TEST_READY'])\n"
        "proceed = pathlib.Path(os.environ['LOOM_TEST_PROCEED'])\n"
        "consumed = pathlib.Path(os.environ['LOOM_TEST_CONSUMED'])\n"
        "restored = pathlib.Path(os.environ['LOOM_TEST_RESTORED'])\n"
        "ready.touch()\n"
        "for _ in range(500):\n"
        "    if proceed.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(2)\n"
        "value = pathlib.Path(sys.argv[2]).read_text(encoding='utf-8')\n"
        "consumed.write_text(value, encoding='utf-8')\n"
        "for _ in range(500):\n"
        "    if restored.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(3)\n"
        "sys.stdout.write(value)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_READY", str(ready))
    monkeypatch.setenv("LOOM_TEST_PROCEED", str(proceed))
    monkeypatch.setenv("LOOM_TEST_CONSUMED", str(consumed))
    monkeypatch.setenv("LOOM_TEST_RESTORED", str(restored))
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    original = tmp_path / "reviewed-kubeconfig.original"
    reviewed_payload = kubeconfig.read_text(encoding="utf-8")
    runner = command._SubprocessKubectlRunner(kubeconfig)

    def swap_path() -> None:
        for _ in range(500):
            if ready.exists():
                break
            time.sleep(0.01)
        else:
            return
        kubeconfig.rename(original)
        kubeconfig.write_text("attacker", encoding="utf-8")
        proceed.touch()
        for _ in range(500):
            if consumed.exists():
                break
            time.sleep(0.01)
        kubeconfig.unlink()
        original.rename(kubeconfig)
        restored.touch()

    swapper = threading.Thread(target=swap_path)
    swapper.start()
    with pytest.raises(OSError, match="kubeconfig changed during observation"):
        runner.run(["get", "namespaces"], timeout_seconds=10)
    swapper.join(timeout=10)

    assert not swapper.is_alive()
    assert consumed.read_text(encoding="utf-8") == reviewed_payload


def test_status_subprocess_runner_pins_kubeconfig_bytes_against_in_place_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = tmp_path / "ready"
    proceed = tmp_path / "proceed"
    consumed = tmp_path / "consumed"
    restored = tmp_path / "restored"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "import time\n"
        "ready = pathlib.Path(os.environ['LOOM_TEST_READY'])\n"
        "proceed = pathlib.Path(os.environ['LOOM_TEST_PROCEED'])\n"
        "consumed = pathlib.Path(os.environ['LOOM_TEST_CONSUMED'])\n"
        "restored = pathlib.Path(os.environ['LOOM_TEST_RESTORED'])\n"
        "ready.touch()\n"
        "for _ in range(500):\n"
        "    if proceed.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(2)\n"
        "value = pathlib.Path(sys.argv[2]).read_text(encoding='utf-8')\n"
        "consumed.write_text(value, encoding='utf-8')\n"
        "for _ in range(500):\n"
        "    if restored.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(3)\n"
        "sys.stdout.write(value)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_READY", str(ready))
    monkeypatch.setenv("LOOM_TEST_PROCEED", str(proceed))
    monkeypatch.setenv("LOOM_TEST_CONSUMED", str(consumed))
    monkeypatch.setenv("LOOM_TEST_RESTORED", str(restored))
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    reviewed_payload = kubeconfig.read_text(encoding="utf-8")
    runner = command._SubprocessKubectlRunner(kubeconfig)

    def rewrite_path() -> None:
        for _ in range(500):
            if ready.exists():
                break
            time.sleep(0.01)
        else:
            return
        kubeconfig.write_text("attacker-controlled", encoding="utf-8")
        proceed.touch()
        for _ in range(500):
            if consumed.exists():
                break
            time.sleep(0.01)
        kubeconfig.write_text(reviewed_payload, encoding="utf-8")
        restored.touch()

    rewriter = threading.Thread(target=rewrite_path)
    rewriter.start()
    with pytest.raises(OSError, match="kubeconfig changed during observation"):
        runner.run(["get", "namespaces"], timeout_seconds=10)
    rewriter.join(timeout=10)

    assert not rewriter.is_alive()
    assert consumed.read_text(encoding="utf-8") == reviewed_payload
