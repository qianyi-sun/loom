from __future__ import annotations

import hashlib
import importlib
import io
import json
import os
from pathlib import Path

import pytest
import yaml

from loom.personal_dev_control_plane_config import (
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    PersonalDevShadowComponent,
    PersonalDevShadowStatus,
)
from loom_cli.__main__ import main
from loom_cli.admin_cmd import dispatch

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"


def _release_value() -> dict[str, object]:
    return {
        "schema_version": 1,
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
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
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


def test_help_describes_only_render_only_inert_authority(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["personal-dev-control-plane", "--help"])

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert "render-only" in captured.out
    assert "shadow" in captured.out
    assert "personal mutations disabled" in captured.out
    assert "physical capacity unchanged" in captured.out
    assert "apply" not in captured.out.casefold()
    assert "activate" not in captured.out.casefold()


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
    kubeconfig = tmp_path / "reviewed-kubeconfig"
    kubeconfig.write_text("reviewed", encoding="utf-8")
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
        assert expected.resource_count == 32
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
    kubeconfig = tmp_path / "reviewed-kubeconfig"
    kubeconfig.write_text("reviewed", encoding="utf-8")
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
    kubeconfig = tmp_path / "reviewed-kubeconfig"
    kubeconfig.write_text("reviewed", encoding="utf-8")
    argv = _status_argv(release_path, release_digest, kubeconfig.resolve())
    argv.extend(["--trusted-release-sha", "do-not-accept"])

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "unrecognized arguments: --trusted-release-sha do-not-accept" in captured.err


@pytest.mark.parametrize("unsafe", ["relative", "symlink", "parent-symlink", "symlink-loop"])
def test_status_rejects_nonabsolute_or_symlink_kubeconfig_before_observation(
    tmp_path: Path,
    unsafe: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = tmp_path / "reviewed-kubeconfig"
    kubeconfig.write_text("reviewed", encoding="utf-8")
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
        linked_parent = tmp_path / "linked-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)
        selected = linked_parent / "reviewed-kubeconfig"
    else:
        selected = tmp_path / "looped-kubeconfig"
        selected.symlink_to(selected)
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
    kubeconfig = tmp_path / "reviewed-kubeconfig"
    kubeconfig.write_text("reviewed", encoding="utf-8")
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
    runner = command._SubprocessKubectlRunner(tmp_path / "reviewed-kubeconfig")

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
    kubeconfig = tmp_path / "reviewed-kubeconfig"
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
