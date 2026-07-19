from __future__ import annotations

import base64
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.operator.preflight_credential_installer import (
    CredentialInstallError,
    CredentialPaths,
    PreflightCredentialInstaller,
    render_readonly_probe_sql,
)
from loom_cli.rollout.preflight_kubeconfig_authority import render_token_request_kubeconfig

NOW = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)
TEAM_ID = UUID("9b1de3bf-9655-489a-813f-e8a7adf81290")


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


def _token(
    namespace: str,
    account: str,
    *,
    now: datetime = NOW,
    audience: str = "https://kubernetes.default.svc.cluster.local",
) -> str:
    claims = {
        "aud": [audience],
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
        database_credential_source=tmp_path / "etc/readonly-db.json",
        credential_root=tmp_path / "state/credentials",
        readonly_kubeconfig=tmp_path / "state/credentials/readonly-kubeconfig",
        readonly_token=tmp_path / "state/credentials/readonly-probe-token",
        readonly_database_credential=tmp_path / "state/credentials/readonly-database.json",
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

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert len(result["changed"]) == 4
    assert installer.check()["ok"] is True
    assert sum("apply" in command for command, _ in runner.calls) == 2
    assert sum("token" in command for command, _ in runner.calls) == 2
    database_calls = [
        (command, stdin)
        for command, stdin in runner.calls
        if "statefulset/loom-postgres" in command
    ]
    assert len(database_calls) == 2
    assert all(stdin is not None for _command, stdin in database_calls)
    assert all("--stdin" in command for command, _stdin in database_calls)
    assert any("PASSWORD" in (stdin or "") for _command, stdin in database_calls)
    assert any("readonly_probe" in (stdin or "") for _command, stdin in database_calls)
    assert all(
        "--duration=6h" in command
        and "--audience=https://kubernetes.default.svc.cluster.local" in command
        for command, _ in runner.calls
        if "token" in command
    )
    rendered = json.dumps(result, sort_keys=True)
    assert "loom_readonly_probe_token_fixture" not in rendered
    database_password = json.loads(installer.paths.database_credential_source.read_bytes())[
        "password"
    ]
    assert database_password not in rendered
    assert all(database_password not in item for command, _ in runner.calls for item in command)
    assert all(
        path.stat().st_mode & 0o777 == 0o600
        for path in (
            installer.paths.readonly_kubeconfig,
            installer.paths.readonly_database_credential,
            installer.paths.readonly_token,
            installer.paths.rehearsal_kubeconfig,
        )
    )
    assert installer.paths.credential_root.stat().st_mode & 0o777 == 0o700
    assert installer.paths.database_credential_source.stat().st_mode & 0o777 == 0o600


def test_install_creates_exact_root_probe_authority_without_exposing_token(
    tmp_path: Path,
) -> None:
    installer, runner = _installer(tmp_path)
    installer.paths.application_token_source.unlink()

    result = installer.install(TEAM_ID)

    raw = installer.paths.application_token_source.read_bytes().strip()
    sql = next(
        stdin
        for command, stdin in runner.calls
        if "statefulset/loom-postgres" in command
        and stdin is not None
        and "readonly_probe" in stdin
    )
    assert result["ok"] is True
    assert raw.startswith(b"loom_readonly_probe_")
    assert installer.paths.application_token_source.stat().st_mode & 0o777 == 0o600
    assert raw.decode() not in sql
    assert hashlib.sha256(raw).hexdigest() in sql
    assert str(TEAM_ID) in sql
    assert "competing readonly probe authority exists" in sql
    assert raw.decode() not in json.dumps(result, sort_keys=True)


def test_install_rotates_tokens_with_a_non_authoritative_cluster_audience(
    tmp_path: Path,
) -> None:
    installer, _runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    source = _source()
    for path, namespace, account in (
        (
            installer.paths.readonly_kubeconfig,
            "loom-staging",
            "loom-rollout-readonly",
        ),
        (
            installer.paths.rehearsal_kubeconfig,
            "loom-rollout-system",
            "loom-rollout-rehearsal",
        ),
    ):
        old_token = _token(
            namespace,
            account,
            audience="https://kubernetes.default.svc",
        )
        _private(
            path,
            render_token_request_kubeconfig(
                source,
                old_token,
                namespace=namespace,
                service_account=account,
                now=NOW,
            ).payload,
        )

    assert installer.check()["failures"] == ["readonly", "rehearsal"]
    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert result["changed"] == sorted(
        [
            str(installer.paths.readonly_kubeconfig),
            str(installer.paths.rehearsal_kubeconfig),
        ]
    )
    assert installer.check()["ok"] is True


@pytest.mark.parametrize(
    ("token_hash", "team_id"),
    (("0" * 63, TEAM_ID), ("0" * 64, UUID(int=0))),
)
def test_readonly_probe_sql_rejects_invalid_identity(
    token_hash: str,
    team_id: UUID,
) -> None:
    with pytest.raises(CredentialInstallError, match="SQL authority"):
        render_readonly_probe_sql(token_hash=token_hash, team_id=team_id)


def test_install_accepts_silent_success_only_for_mutating_convergence(
    tmp_path: Path,
) -> None:
    installer, runner = _installer(tmp_path)
    original_run = runner.__call__

    def silent_mutations(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        command = tuple(argv)
        if "apply" in command or "statefulset/loom-postgres" in command:
            runner.calls.append((command, input))
            return Result(stdout="")
        return original_run(argv, input=input, timeout=timeout)

    installer.run = silent_mutations

    assert installer.install(TEAM_ID)["ok"] is True
    assert installer.check()["ok"] is True


def test_install_rejects_silent_success_for_required_token_output(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    original_run = runner.__call__

    def missing_token(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        command = tuple(argv)
        if "token" in command:
            runner.calls.append((command, input))
            return Result(stdout="")
        return original_run(argv, input=input, timeout=timeout)

    installer.run = missing_token

    with pytest.raises(CredentialInstallError, match="returned no output"):
        installer.install(TEAM_ID)


def test_install_is_idempotent_only_while_bounded_tokens_are_unchanged(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    second = installer.install(TEAM_ID)

    assert second["changed"] == []
    assert second["ok"] is True


def test_check_fails_closed_after_token_freshness_expires(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    installer.now = lambda: NOW + timedelta(hours=5)

    result = installer.check()

    assert result["ok"] is False
    assert result["failures"] == ["readonly", "rehearsal"]


def test_install_requires_root_and_rejects_unsafe_source_mode(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.euid = 501
    with pytest.raises(CredentialInstallError, match="requires root"):
        installer.install(TEAM_ID)

    installer.euid = 0
    installer.paths.application_token_source.chmod(0o644)
    with pytest.raises(CredentialInstallError, match="authority is unsafe"):
        installer.install(TEAM_ID)


def test_install_rejects_corrupt_or_public_database_credential(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    _private(installer.paths.database_credential_source, b'{"password":"exposed"}\n')

    with pytest.raises(CredentialInstallError, match="database credential authority"):
        installer.install(TEAM_ID)

    installer.paths.database_credential_source.chmod(0o644)
    with pytest.raises(CredentialInstallError, match="authority is unsafe"):
        installer.install(TEAM_ID)
