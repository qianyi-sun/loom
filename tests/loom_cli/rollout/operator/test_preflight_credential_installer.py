from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
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
from loom_cli.rollout.readonly_minio_bootstrap import (
    READONLY_MINIO_POLICY_NAME,
    readonly_minio_policy,
)

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
        self.now = NOW
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
        if "--raw" in command:
            assert input is not None
            kubeconfig = Path(command[command.index("--kubeconfig") + 1])
            if "readonly" in kubeconfig.name:
                subject = "system:serviceaccount:loom-staging:loom-rollout-readonly"
            else:
                subject = "system:serviceaccount:loom-rollout-system:loom-rollout-rehearsal"
            if any("selfsubjectreviews" in item for item in command):
                return Result(stdout=json.dumps({"status": {"userInfo": {"username": subject}}}))
            spec = json.loads(input)
            attributes = spec["spec"]["resourceAttributes"]
            allowed = attributes.get("subresource") != "token"
            return Result(stdout=json.dumps({"status": {"allowed": allowed}}))
        if "token" in command:
            account = command[command.index("token") + 1]
            namespace = command[command.index("--namespace") + 1]
            return Result(stdout=_token(namespace, account, now=self.now) + "\n")
        if "pod/loom-minio-0" in command:
            if input is not None and "--stdin" not in command:
                return Result(returncode=1, stderr="stdin was not forwarded")
            script = command[-1]
            if "admin user info" in script:
                return Result(
                    stdout="\n".join(
                        (
                            json.dumps(
                                {
                                    "status": "success",
                                    "policyName": READONLY_MINIO_POLICY_NAME,
                                }
                            ),
                            json.dumps(
                                {
                                    "status": "success",
                                    "policy": READONLY_MINIO_POLICY_NAME,
                                    "policyInfo": {
                                        "PolicyName": READONLY_MINIO_POLICY_NAME,
                                        "Policy": readonly_minio_policy(),
                                    },
                                }
                            ),
                        )
                    )
                    + "\n"
                )
            return Result(stdout="")
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
        minio_credential_source=tmp_path / "etc/readonly-minio.json",
        credential_root=tmp_path / "state/credentials",
        readonly_kubeconfig=tmp_path / "state/credentials/readonly-kubeconfig",
        readonly_token=tmp_path / "state/credentials/readonly-probe-token",
        readonly_database_credential=tmp_path / "state/credentials/readonly-database.json",
        readonly_minio_credential=tmp_path / "state/credentials/readonly-minio.json",
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
        sleep=lambda _seconds: None,
        euid=0,
        service_uid=uid,
        service_gid=gid,
        root_uid=uid,
        root_gid=gid,
    )
    return installer, runner


def test_default_root_kubeconfig_is_the_host_k3s_authority() -> None:
    assert CredentialPaths().root_kubeconfig == Path("/etc/rancher/k3s/k3s.yaml")


def test_install_accepts_root_owned_k3s_default_kubeconfig_mode(tmp_path: Path) -> None:
    installer, _runner = _installer(tmp_path)
    installer.paths.root_kubeconfig.chmod(0o644)

    assert installer.install(TEAM_ID)["ok"] is True


def test_install_applies_exact_authority_and_publishes_no_secret_evidence(
    tmp_path: Path,
) -> None:
    installer, runner = _installer(tmp_path)

    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert len(result["changed"]) == 5
    assert installer.check()["ok"] is True
    source_command = next(
        command
        for command, _stdin in runner.calls
        if "config" in command and "view" in command
    )
    assert source_command == (
        "kubectl",
        "--kubeconfig",
        str(installer.paths.root_kubeconfig),
        "config",
        "view",
        "--raw",
        "--minify",
    )
    assert sum("apply" in command for command, _ in runner.calls) == 2
    assert sum("token" in command for command, _ in runner.calls) == 2
    database_calls = [
        (command, stdin)
        for command, stdin in runner.calls
        if "service/loom-postgres-rw" in command
    ]
    assert len(database_calls) == 2
    assert all(stdin is not None for _command, stdin in database_calls)
    assert all(
        command[command.index("-U") : command.index("-U") + 4]
        == ("-U", "postgres", "-d", "loom")
        for command, _stdin in database_calls
    )
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
            installer.paths.readonly_minio_credential,
            installer.paths.readonly_token,
            installer.paths.rehearsal_kubeconfig,
        )
    )
    assert installer.paths.credential_root.stat().st_mode & 0o777 == 0o700
    assert installer.paths.database_credential_source.stat().st_mode & 0o777 == 0o600
    assert installer.paths.minio_credential_source.stat().st_mode & 0o777 == 0o600
    assert any("admin user info" in command[-1] for command, _ in runner.calls)
    minio_calls = [
        (command, stdin) for command, stdin in runner.calls if "pod/loom-minio-0" in command
    ]
    assert minio_calls
    assert all("--stdin" in command for command, _stdin in minio_calls)
    assert all(stdin is None for _command, stdin in minio_calls)
    minio_secret = json.loads(installer.paths.minio_credential_source.read_bytes())["secret_key"]
    assert minio_secret not in rendered
    assert all(minio_secret not in item for command, _ in runner.calls for item in command)


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
        if "service/loom-postgres-rw" in command
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
        if "apply" in command or "service/loom-postgres-rw" in command:
            runner.calls.append((command, input))
            return Result(stdout="")
        return original_run(argv, input=input, timeout=timeout)

    installer.run = silent_mutations

    assert installer.install(TEAM_ID)["ok"] is True
    assert installer.check()["ok"] is True


def test_install_accepts_success_with_bounded_command_warning(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    original_run = runner.__call__

    def warning_on_apply(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        command = tuple(argv)
        if "apply" in command:
            runner.calls.append((command, input))
            return Result(stdout="applied\n", stderr="Warning: unchanged authority\n")
        return original_run(argv, input=input, timeout=timeout)

    installer.run = warning_on_apply

    assert installer.install(TEAM_ID)["ok"] is True
    assert installer.check()["ok"] is True


def test_check_rejects_extra_or_rebound_minio_policy(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    original_run = runner.__call__
    drift_reads = 0

    def drifted_policy(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        nonlocal drift_reads
        command = tuple(argv)
        if "pod/loom-minio-0" in command and "admin user info" in command[-1]:
            drift_reads += 1
            return Result(
                stdout="\n".join(
                    (
                        json.dumps({"status": "success", "policyName": "readonly,writeonly"}),
                        json.dumps(
                            {
                                "status": "success",
                                "policy": READONLY_MINIO_POLICY_NAME,
                                "policyInfo": {
                                    "PolicyName": READONLY_MINIO_POLICY_NAME,
                                    "Policy": readonly_minio_policy(),
                                },
                            }
                        ),
                    )
                )
                + "\n"
            )
        return original_run(argv, input=input, timeout=timeout)

    installer.run = drifted_policy

    assert installer.check()["failures"] == ["readonly-minio"]
    assert drift_reads == 5


def test_check_retries_one_stale_minio_authority_read(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    original_run = runner.__call__
    stale_reads = 0

    def stale_once(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        nonlocal stale_reads
        command = tuple(argv)
        if "pod/loom-minio-0" in command and "admin user info" in command[-1] and stale_reads == 0:
            stale_reads += 1
            return Result(
                stdout="\n".join(
                    (
                        json.dumps(
                            {
                                "status": "success",
                                "policyName": READONLY_MINIO_POLICY_NAME,
                            }
                        ),
                        json.dumps(
                            {
                                "status": "success",
                                "policy": READONLY_MINIO_POLICY_NAME,
                                "policyInfo": {
                                    "PolicyName": READONLY_MINIO_POLICY_NAME,
                                    "Policy": {"Version": "stale"},
                                },
                            }
                        ),
                    )
                )
                + "\n"
            )
        return original_run(argv, input=input, timeout=timeout)

    installer.run = stale_once

    assert installer.check()["ok"] is True
    assert stale_reads == 1


def test_install_converges_minio_only_after_exact_authority_check_fails(
    tmp_path: Path,
) -> None:
    installer, runner = _installer(tmp_path)
    original_run = runner.__call__
    drift_reads = 0

    def initially_drifted(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        nonlocal drift_reads
        command = tuple(argv)
        if "pod/loom-minio-0" in command and "admin user info" in command[-1] and drift_reads < 5:
            drift_reads += 1
            runner.calls.append((command, input))
            return Result(
                stdout=json.dumps({"status": "success", "policyName": "drifted-policy"})
                + "\n"
                + json.dumps(
                    {
                        "status": "success",
                        "policy": READONLY_MINIO_POLICY_NAME,
                        "policyInfo": {
                            "PolicyName": READONLY_MINIO_POLICY_NAME,
                            "Policy": readonly_minio_policy(),
                        },
                    }
                )
                + "\n"
            )
        return original_run(argv, input=input, timeout=timeout)

    installer.run = initially_drifted

    assert installer.install(TEAM_ID)["ok"] is True
    assert drift_reads == 5
    assert (
        sum(
            "admin policy create" in command[-1]
            for command, _input in runner.calls
            if "pod/loom-minio-0" in command
        )
        == 1
    )


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
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    minio_mutations_before = sum(
        "admin policy create" in command[-1]
        for command, _input in runner.calls
        if "pod/loom-minio-0" in command
    )
    second = installer.install(TEAM_ID)
    minio_mutations_after = sum(
        "admin policy create" in command[-1]
        for command, _input in runner.calls
        if "pod/loom-minio-0" in command
    )

    assert second["changed"] == []
    assert second["ok"] is True
    assert minio_mutations_before == 0
    assert minio_mutations_after == minio_mutations_before


def test_install_rotates_tokens_before_runtime_freshness_can_expire(
    tmp_path: Path,
) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    old_readonly = installer.paths.readonly_kubeconfig.read_bytes()
    old_rehearsal = installer.paths.rehearsal_kubeconfig.read_bytes()
    later = NOW + timedelta(hours=2, minutes=30)
    installer.now = lambda: later
    runner.now = later

    assert installer.check()["ok"] is True
    result = installer.install(TEAM_ID)

    assert result["ok"] is True
    assert result["changed"] == sorted(
        [
            str(installer.paths.readonly_kubeconfig),
            str(installer.paths.rehearsal_kubeconfig),
        ]
    )
    assert installer.paths.readonly_kubeconfig.read_bytes() != old_readonly
    assert installer.paths.rehearsal_kubeconfig.read_bytes() != old_rehearsal
    assert installer.check()["ok"] is True


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
    with pytest.raises(CredentialInstallError, match="requires root"):
        installer.refresh()

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


def test_refresh_is_noop_while_both_token_requests_are_fresh(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    token_calls = sum("token" in command for command, _input in runner.calls)

    result = installer.refresh()

    assert result["ok"] is True
    assert result["changed"] == []
    assert sum("token" in command for command, _input in runner.calls) == token_calls
    assert result["authority"]["readonly"]["token_mint_allowed"] is False
    assert result["authority"]["rehearsal"]["token_mint_allowed"] is False
    reviewed_pods = {
        json.loads(input)["spec"]["resourceAttributes"].get("name")
        for command, input in runner.calls
        if input is not None and any("selfsubjectaccessreviews" in item for item in command)
    }
    assert {
        "loom-postgres-1",
        "loom-postgres-2",
        "loom-postgres-3",
        "loom-minio-0",
    }.issubset(reviewed_pods)


def test_refresh_rotates_both_token_requests_near_expiry(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    before = (
        installer.paths.readonly_kubeconfig.read_bytes(),
        installer.paths.rehearsal_kubeconfig.read_bytes(),
    )
    later = NOW + timedelta(hours=2, minutes=30)
    installer.now = lambda: later
    runner.now = later

    result = installer.refresh()

    assert result["changed"] == sorted(
        [
            str(installer.paths.readonly_kubeconfig),
            str(installer.paths.rehearsal_kubeconfig),
        ]
    )
    assert (
        installer.paths.readonly_kubeconfig.read_bytes(),
        installer.paths.rehearsal_kubeconfig.read_bytes(),
    ) != before
    assert all(
        stat.st_mode & 0o777 == 0o600 and stat.st_uid == os.getuid() and stat.st_gid == os.getgid()
        for stat in (
            installer.paths.readonly_kubeconfig.stat(),
            installer.paths.rehearsal_kubeconfig.stat(),
        )
    )


def test_refresh_second_token_failure_preserves_both_installed_files(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)
    before = (
        installer.paths.readonly_kubeconfig.read_bytes(),
        installer.paths.rehearsal_kubeconfig.read_bytes(),
    )
    later = NOW + timedelta(hours=2, minutes=30)
    installer.now = lambda: later
    runner.now = later
    original_run = runner.__call__

    def fail_rehearsal_token(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        command = tuple(argv)
        if "token" in command and "loom-rollout-rehearsal" in command:
            return Result(returncode=1, stderr="secret-looking-token")
        return original_run(argv, input=input, timeout=timeout)

    installer.run = fail_rehearsal_token
    with pytest.raises(CredentialInstallError, match="command failed") as raised:
        installer.refresh()

    assert "secret-looking-token" not in str(raised.value)
    assert (
        installer.paths.readonly_kubeconfig.read_bytes(),
        installer.paths.rehearsal_kubeconfig.read_bytes(),
    ) == before


def test_refresh_redacts_timeout_and_fails_closed_on_live_readback(tmp_path: Path) -> None:
    installer, runner = _installer(tmp_path)
    installer.install(TEAM_ID)

    def timeout(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        del input
        raise subprocess.TimeoutExpired(argv, timeout, output="secret-token")

    installer.run = timeout
    with pytest.raises(CredentialInstallError, match="command failed") as raised:
        installer.refresh()

    assert "secret-token" not in str(raised.value)
    installer.run = runner
    original_run = runner.__call__

    def wrong_subject(
        argv: Sequence[str],
        *,
        input: str | None,
        timeout: int,
    ) -> Result:
        command = tuple(argv)
        if any("selfsubjectreviews" in item for item in command):
            return Result(stdout=json.dumps({"status": {"userInfo": {"username": "root"}}}))
        return original_run(argv, input=input, timeout=timeout)

    installer.run = wrong_subject
    with pytest.raises(CredentialInstallError, match="subject readback"):
        installer.refresh()
