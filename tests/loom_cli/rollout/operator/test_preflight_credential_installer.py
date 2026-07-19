from __future__ import annotations

import base64
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.operator.preflight_credential_installer import (
    CredentialInstallError,
    CredentialPaths,
    PreflightCredentialInstaller,
)

NOW = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)


@dataclass
class Result:
    returncode: int = 0
    stdout: str = "ok\n"
    stderr: str = ""


class Runner:
    def __init__(self, source: bytes) -> None:
        self.source = source
        self.calls: list[tuple[tuple[str, ...], str | None]] = []

    def __call__(
        self,
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        command = tuple(argv)
        self.calls.append((command, input))
        assert timeout <= 60
        if "config" in command and "view" in command:
            return Result(stdout=self.source.decode())
        if "token" in command:
            account = command[command.index("token") + 1]
            namespace = command[command.index("--namespace") + 1]
            return Result(stdout=_token(namespace, account) + "\n")
        return Result()


def _token(namespace: str, account: str, *, now: datetime = NOW) -> str:
    claims = {
        "aud": ["https://kubernetes.default.svc"],
        "exp": int((now + timedelta(hours=6)).timestamp()),
        "iat": int(now.timestamp()),
        "sub": f"system:serviceaccount:{namespace}:{account}",
    }
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def _source() -> bytes:
    certificate = base64.b64encode(b"-----BEGIN CERTIFICATE-----\nca\n").decode()
    return yaml.safe_dump(
        {
            "apiVersion": "v1",
            "clusters": [
                {
                    "cluster": {
                        "certificate-authority-data": certificate,
                        "server": "https://127.0.0.1:6443",
                    },
                    "name": "loom-staging",
                }
            ],
            "contexts": [
                {
                    "context": {"cluster": "loom-staging", "user": "root"},
                    "name": "loom-staging",
                }
            ],
            "current-context": "loom-staging",
            "kind": "Config",
            "users": [{"name": "root", "user": {"client-key-data": "secret"}}],
        }
    ).encode()


def _private(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _public(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o644)


def _installer(tmp_path: Path) -> tuple[PreflightCredentialInstaller, Runner]:
    paths = CredentialPaths(
        root_kubeconfig=tmp_path / "root/kubeconfig",
        readonly_manifest=tmp_path / "repo/readonly.yaml",
        rehearsal_manifest=tmp_path / "repo/rehearsal.yaml",
        application_token_source=tmp_path / "etc/readonly-token",
        credential_root=tmp_path / "state/credentials",
        readonly_kubeconfig=tmp_path / "state/credentials/readonly-kubeconfig",
        readonly_token=tmp_path / "state/credentials/readonly-probe-token",
        rehearsal_kubeconfig=tmp_path / "state/credentials/rehearsal-kubeconfig",
    )
    source = _source()
    _private(paths.root_kubeconfig, source)
    _public(paths.readonly_manifest, b"apiVersion: v1\nkind: ServiceAccount\n")
    _public(paths.rehearsal_manifest, b"apiVersion: v1\nkind: Namespace\n")
    raw_application_token = b"loom_readonly_probe_token_fixture_0123456789\n"
    _private(paths.application_token_source, raw_application_token)
    paths.credential_root.parent.mkdir(parents=True, exist_ok=True)
    runner = Runner(source)
    uid = os.getuid()
    gid = os.getgid()
    installer = PreflightCredentialInstaller(
        paths=paths,
        run=runner,
        now=lambda: NOW,
        euid=0,
        service_uid=uid,
        service_gid=gid,
        root_uid=uid,
        root_gid=gid,
    )
    return installer, runner


def test_install_applies_exact_authority_and_publishes_no_secret_evidence(
    tmp_path: Path,
) -> None:
    installer, runner = _installer(tmp_path)

    result = installer.install()

    assert result["ok"] is True
    assert len(result["changed"]) == 3
    assert installer.check()["ok"] is True
    assert sum("apply" in command for command, _ in runner.calls) == 2
    assert sum("token" in command for command, _ in runner.calls) == 2
    assert all(
        "--duration=6h" in command and "--audience=https://kubernetes.default.svc" in command
        for command, _ in runner.calls
        if "token" in command
    )
    rendered = json.dumps(result, sort_keys=True)
    assert "loom_readonly_probe_token_fixture" not in rendered
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in (
        installer.paths.readonly_kubeconfig,
        installer.paths.readonly_token,
        installer.paths.rehearsal_kubeconfig,
    ))
    assert installer.paths.credential_root.stat().st_mode & 0o777 == 0o700


def test_install_is_idempotent_only_while_bounded_tokens_are_unchanged(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.install()
    second = installer.install()

    assert second["changed"] == []
    assert second["ok"] is True


def test_check_fails_closed_after_token_freshness_expires(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.install()
    installer.now = lambda: NOW + timedelta(hours=5)

    result = installer.check()

    assert result["ok"] is False
    assert result["failures"] == ["readonly", "rehearsal"]


def test_install_requires_root_and_rejects_unsafe_source_mode(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.euid = 501
    with pytest.raises(CredentialInstallError, match="requires root"):
        installer.install()

    installer.euid = 0
    installer.paths.application_token_source.chmod(0o644)
    with pytest.raises(CredentialInstallError, match="authority is unsafe"):
        installer.install()
