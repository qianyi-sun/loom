"""Root-only convergence for installed least-privilege preflight credentials."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import re
import secrets
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.preflight_credential_paths import (
    PREFLIGHT_CREDENTIAL_ROOT,
    READONLY_DATABASE_CREDENTIAL_PATH,
    READONLY_KUBECONFIG_PATH,
    READONLY_TOKEN_PATH,
    REHEARSAL_KUBECONFIG_PATH,
)
from loom_cli.rollout.preflight_kubeconfig_authority import (
    render_token_request_kubeconfig,
    validate_token_request_kubeconfig,
)
from loom_cli.rollout.readonly_database_bootstrap import (
    ReadonlyDatabaseCredential,
    render_readonly_role_sql,
)

_ROOT_KUBECONFIG = Path("/root/.kube/config")
_RUNNER_REPO = Path("/opt/loom-staging-runner/source")
_READONLY_MANIFEST = _RUNNER_REPO / "deploy/k8s/staging-rollout-readonly.yaml"
_REHEARSAL_MANIFEST = _RUNNER_REPO / "deploy/k8s/staging-rollout-rehearsal-authority.yaml"
_APPLICATION_TOKEN_SOURCE = Path("/etc/loom/staging-rollout-readonly-probe-token")
_DATABASE_CREDENTIAL_SOURCE = Path("/etc/loom/staging-rollout-readonly-db.json")
_SERVICE_USER = "loom-rollout"
_TOKEN_DURATION = "6h"
_TOKEN_RE = re.compile(r"^[A-Za-z0-9._~-]{32,1024}$")
_READONLY_PROBE_NAME = "staging-rollout-readonly-probe"
_READONLY_PROBE_ACTOR = "deployment:staging-rollout"
_CHILD_ENVIRONMENT = {
    "HOME": "/root",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "USER": "root",
}


class CredentialInstallError(RuntimeError):
    """Fail-closed preflight credential convergence error."""


def render_readonly_probe_sql(*, token_hash: str, team_id: UUID) -> str:
    """Render one exact deployment-owned readonly-probe convergence transaction."""
    if re.fullmatch(r"[0-9a-f]{64}", token_hash) is None or team_id.int == 0:
        raise CredentialInstallError("readonly probe SQL authority is invalid")
    return (
        f"""
\\set ON_ERROR_STOP on
BEGIN;
INSERT INTO tokens (
  token_hash, name, type, scopes, team_id, created_by_user_id,
  created_by_actor, issued_at, expires_at, revoked_at, last_used_at, last_seen_at
)
VALUES (
  decode('{token_hash}', 'hex'), '{_READONLY_PROBE_NAME}', 'readonly_probe',
  ARRAY['read:own']::varchar[], '{team_id}'::uuid, NULL, '{_READONLY_PROBE_ACTOR}',
  CURRENT_TIMESTAMP, NULL, NULL, NULL, NULL
)
ON CONFLICT (token_hash) DO NOTHING;
DO $loom$
DECLARE
  exact_rows integer;
  competing_rows integer;
BEGIN
  SELECT count(*) INTO exact_rows
  FROM tokens
  WHERE token_hash = decode('{token_hash}', 'hex')
    AND name = '{_READONLY_PROBE_NAME}'
    AND type = 'readonly_probe'
    AND scopes = ARRAY['read:own']::varchar[]
    AND team_id = '{team_id}'::uuid
    AND created_by_user_id IS NULL
    AND created_by_actor = '{_READONLY_PROBE_ACTOR}'
    AND expires_at IS NULL
    AND revoked_at IS NULL;
  IF exact_rows <> 1 THEN
    RAISE EXCEPTION 'readonly probe authority drifted';
  END IF;
  SELECT count(*) INTO competing_rows
  FROM tokens
  WHERE type = 'readonly_probe'
    AND team_id = '{team_id}'::uuid
    AND revoked_at IS NULL
    AND token_hash <> decode('{token_hash}', 'hex');
  IF competing_rows <> 0 THEN
    RAISE EXCEPTION 'competing readonly probe authority exists';
  END IF;
END
$loom$;
COMMIT;
SELECT 'readonly-probe-converged-v1';
""".strip()
        + "\n"
    )


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


Run = Callable[..., CommandResult]
Now = Callable[[], datetime]


def _run(
    argv: Sequence[str],
    *,
    input: str | None,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        env=_CHILD_ENVIRONMENT,
        input=input,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


@dataclass(frozen=True, slots=True)
class CredentialPaths:
    root_kubeconfig: Path = _ROOT_KUBECONFIG
    readonly_manifest: Path = _READONLY_MANIFEST
    rehearsal_manifest: Path = _REHEARSAL_MANIFEST
    application_token_source: Path = _APPLICATION_TOKEN_SOURCE
    database_credential_source: Path = _DATABASE_CREDENTIAL_SOURCE
    credential_root: Path = PREFLIGHT_CREDENTIAL_ROOT
    readonly_kubeconfig: Path = READONLY_KUBECONFIG_PATH
    readonly_database_credential: Path = READONLY_DATABASE_CREDENTIAL_PATH
    readonly_token: Path = READONLY_TOKEN_PATH
    rehearsal_kubeconfig: Path = REHEARSAL_KUBECONFIG_PATH


@dataclass(slots=True)
class PreflightCredentialInstaller:
    """Converge two bounded SA kubeconfigs and one fixed readonly API token."""

    paths: CredentialPaths = CredentialPaths()
    run: Run = _run
    now: Now = lambda: datetime.now(UTC)
    euid: int = -1
    service_uid: int = -1
    service_gid: int = -1
    root_uid: int = 0
    root_gid: int = 0

    def __post_init__(self) -> None:
        if self.euid == -1:
            self.euid = os.geteuid()
        if self.service_uid == -1 or self.service_gid == -1:
            try:
                identity = pwd.getpwnam(_SERVICE_USER)
            except KeyError as exc:
                raise CredentialInstallError("rollout service identity is unavailable") from exc
            self.service_uid = identity.pw_uid
            self.service_gid = identity.pw_gid
        if self.service_uid < 1 or self.service_gid < 1 or self.root_uid < 0 or self.root_gid < 0:
            raise CredentialInstallError("rollout service identity is unsafe")
        for field in fields(self.paths):
            path = getattr(self.paths, field.name)
            if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts:
                raise CredentialInstallError("preflight credential path authority is invalid")

    def install(self, team_id: UUID) -> dict[str, object]:
        if self.euid != 0:
            raise CredentialInstallError("preflight credential install requires root")
        if team_id.int == 0:
            raise CredentialInstallError("readonly probe team authority is invalid")
        source_kubeconfig = self._minified_source_kubeconfig()
        for manifest in (self.paths.readonly_manifest, self.paths.rehearsal_manifest):
            self._apply_authority(manifest)
        database_credential = self._load_or_create_database_credential()
        self._converge_database_role(database_credential)
        application_token = self._load_or_create_application_token()
        self._converge_application_probe(application_token, team_id=team_id)
        existing = self.check()
        installed_application_token: bytes | None = None
        installed_database_credential: ReadonlyDatabaseCredential | None = None
        if existing["ok"]:
            installed_application_token = self._read_private(
                self.paths.readonly_token,
                expected_uid=self.service_uid,
                expected_gid=self.service_gid,
            ).strip()
            installed_database_credential = ReadonlyDatabaseCredential.from_bytes(
                self._read_private(
                    self.paths.readonly_database_credential,
                    expected_uid=self.service_uid,
                    expected_gid=self.service_gid,
                )
            )
        if (
            existing["ok"]
            and installed_application_token == application_token
            and installed_database_credential == database_credential
        ):
            return {"ok": True, "changed": [], "authority": existing["authority"]}
        readonly = self._request_kubeconfig(
            source_kubeconfig,
            namespace="loom-staging",
            service_account="loom-rollout-readonly",
        )
        rehearsal = self._request_kubeconfig(
            source_kubeconfig,
            namespace="loom-rollout-system",
            service_account="loom-rollout-rehearsal",
        )
        self._ensure_private_directory(self.paths.credential_root)
        changed = {
            str(self.paths.readonly_kubeconfig): self._atomic_write(
                self.paths.readonly_kubeconfig,
                readonly,
            ),
            str(self.paths.readonly_token): self._atomic_write(
                self.paths.readonly_token,
                application_token.rstrip() + b"\n",
            ),
            str(self.paths.readonly_database_credential): self._atomic_write(
                self.paths.readonly_database_credential,
                database_credential.to_bytes(),
            ),
            str(self.paths.rehearsal_kubeconfig): self._atomic_write(
                self.paths.rehearsal_kubeconfig,
                rehearsal,
            ),
        }
        result = self.check()
        if not result["ok"]:
            raise CredentialInstallError("installed preflight credentials did not verify")
        return {
            "ok": True,
            "changed": sorted(path for path, value in changed.items() if value),
            "authority": result["authority"],
        }

    def _load_or_create_application_token(self) -> bytes:
        path = self.paths.application_token_source
        if path.exists() or path.is_symlink():
            payload = self._read_private(
                path,
                expected_uid=self.root_uid,
                expected_gid=self.root_gid,
            ).strip()
        else:
            self._require_root_directory(path.parent)
            payload = f"loom_readonly_probe_{secrets.token_urlsafe(32)}".encode("ascii")
            self._atomic_write_private(
                path,
                payload + b"\n",
                uid=self.root_uid,
                gid=self.root_gid,
                parent=path.parent,
            )
        try:
            rendered = payload.decode("ascii")
        except UnicodeDecodeError as exc:
            raise CredentialInstallError("readonly application token is not ASCII") from exc
        if _TOKEN_RE.fullmatch(rendered) is None:
            raise CredentialInstallError("readonly application token payload is invalid")
        return payload

    def _converge_application_probe(self, token: bytes, *, team_id: UUID) -> None:
        token_hash = hashlib.sha256(token).hexdigest()
        self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.paths.root_kubeconfig),
                "--namespace",
                "loom-staging",
                "exec",
                "statefulset/loom-postgres",
                "--",
                "psql",
                "--no-psqlrc",
                "-AtX",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "loom",
                "-d",
                "loom",
                "-f",
                "-",
            ),
            input=render_readonly_probe_sql(token_hash=token_hash, team_id=team_id),
            timeout=60,
            require_output=False,
        )

    def check(self) -> dict[str, object]:
        now = self.now()
        failures: list[str] = []
        authority: dict[str, object] = {}
        for label, path, namespace, account in (
            (
                "readonly",
                self.paths.readonly_kubeconfig,
                "loom-staging",
                "loom-rollout-readonly",
            ),
            (
                "rehearsal",
                self.paths.rehearsal_kubeconfig,
                "loom-rollout-system",
                "loom-rollout-rehearsal",
            ),
        ):
            try:
                payload = self._read_private(
                    path,
                    expected_uid=self.service_uid,
                    expected_gid=self.service_gid,
                )
                evidence = validate_token_request_kubeconfig(
                    payload,
                    namespace=namespace,
                    service_account=account,
                    now=now,
                )
                authority[label] = {
                    "audiences": list(evidence.audiences),
                    "expires_at": evidence.expires_at,
                    "metadata_digest": evidence.metadata_digest,
                    "subject": evidence.subject,
                }
            except (CredentialInstallError, ValueError, yaml.YAMLError):
                failures.append(label)
        try:
            application_payload = self._read_private(
                self.paths.readonly_token,
                expected_uid=self.service_uid,
                expected_gid=self.service_gid,
            ).strip()
            if _TOKEN_RE.fullmatch(application_payload.decode("ascii")) is None:
                raise ValueError
        except (CredentialInstallError, UnicodeDecodeError, ValueError):
            failures.append("readonly-application")
        try:
            database_credential = ReadonlyDatabaseCredential.from_bytes(
                self._read_private(
                    self.paths.readonly_database_credential,
                    expected_uid=self.service_uid,
                    expected_gid=self.service_gid,
                )
            )
            authority["readonly-database"] = {
                "database": database_credential.database,
                "role": database_credential.role,
                "schema_version": 1,
            }
        except (CredentialInstallError, ValueError):
            failures.append("readonly-database")
        return {"ok": not failures, "failures": sorted(failures), "authority": authority}

    def _load_or_create_database_credential(self) -> ReadonlyDatabaseCredential:
        path = self.paths.database_credential_source
        if path.exists() or path.is_symlink():
            try:
                return ReadonlyDatabaseCredential.from_bytes(
                    self._read_private(
                        path,
                        expected_uid=self.root_uid,
                        expected_gid=self.root_gid,
                    )
                )
            except ValueError as exc:
                raise CredentialInstallError(
                    "readonly database credential authority is invalid"
                ) from exc
        self._require_root_directory(path.parent)
        credential = ReadonlyDatabaseCredential(
            role="loom_rollout_readonly",
            database="loom",
            password=secrets.token_hex(32),
        )
        self._atomic_write_private(
            path,
            credential.to_bytes(),
            uid=self.root_uid,
            gid=self.root_gid,
            parent=path.parent,
        )
        return credential

    def _converge_database_role(self, credential: ReadonlyDatabaseCredential) -> None:
        self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.paths.root_kubeconfig),
                "--namespace",
                "loom-staging",
                "exec",
                "statefulset/loom-postgres",
                "--",
                "psql",
                "--no-psqlrc",
                "-AtX",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "loom",
                "-d",
                credential.database,
                "-f",
                "-",
            ),
            input=render_readonly_role_sql(credential),
            timeout=60,
            require_output=False,
        )

    def _minified_source_kubeconfig(self) -> bytes:
        self._read_private(
            self.paths.root_kubeconfig,
            expected_uid=self.root_uid,
            expected_gid=self.root_gid,
        )
        result = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.paths.root_kubeconfig),
                "config",
                "view",
                "--raw",
                "--minify",
                "--context",
                "loom-staging",
            ),
            timeout=30,
        )
        return result.stdout.encode()

    def _apply_authority(self, path: Path) -> None:
        payload = self._read_public_root(path).decode("utf-8")
        self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.paths.root_kubeconfig),
                "apply",
                "--server-side",
                "--field-manager=loom-staging-rollout-installer",
                "--force-conflicts=false",
                "--validate=strict",
                "--request-timeout=30s",
                "-f",
                "-",
            ),
            input=payload,
            timeout=60,
            require_output=False,
        )

    def _request_kubeconfig(
        self,
        source_kubeconfig: bytes,
        *,
        namespace: str,
        service_account: str,
    ) -> bytes:
        result = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.paths.root_kubeconfig),
                "--namespace",
                namespace,
                "create",
                "token",
                service_account,
                f"--duration={_TOKEN_DURATION}",
                "--audience=https://kubernetes.default.svc",
            ),
            timeout=30,
        )
        token = result.stdout.strip()
        return render_token_request_kubeconfig(
            source_kubeconfig,
            token,
            namespace=namespace,
            service_account=service_account,
            now=self.now(),
        ).payload

    def _command(
        self,
        argv: Sequence[str],
        *,
        input: str | None = None,
        timeout: int,
        require_output: bool = True,
    ) -> CommandResult:
        result = self.run(argv, input=input, timeout=timeout)
        if result.returncode != 0 or result.stderr.strip():
            raise CredentialInstallError("preflight credential command failed")
        if require_output and not result.stdout.strip():
            raise CredentialInstallError("preflight credential command returned no output")
        return result

    def _ensure_private_directory(self, path: Path) -> None:
        try:
            if path.exists() or path.is_symlink():
                metadata = os.lstat(path)
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or stat.S_ISLNK(metadata.st_mode)
                    or metadata.st_uid != self.service_uid
                    or metadata.st_gid != self.service_gid
                    or stat.S_IMODE(metadata.st_mode) != 0o700
                ):
                    raise CredentialInstallError("preflight credential directory is unsafe")
                return
            path.mkdir(mode=0o700)
            os.chown(path, self.service_uid, self.service_gid)
            os.chmod(path, 0o700)
        except OSError as exc:
            raise CredentialInstallError("preflight credential directory is unavailable") from exc

    def _require_root_directory(self, path: Path) -> None:
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            raise CredentialInstallError("root credential directory is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.root_uid
            or metadata.st_gid != self.root_gid
            or stat.S_IMODE(metadata.st_mode) not in {0o700, 0o755}
        ):
            raise CredentialInstallError("root credential directory is unsafe")

    def _atomic_write(self, path: Path, payload: bytes) -> bool:
        return self._atomic_write_private(
            path,
            payload,
            uid=self.service_uid,
            gid=self.service_gid,
            parent=self.paths.credential_root,
        )

    def _atomic_write_private(
        self,
        path: Path,
        payload: bytes,
        *,
        uid: int,
        gid: int,
        parent: Path,
    ) -> bool:
        if not payload or len(payload) > 1 << 20 or path.parent != parent:
            raise CredentialInstallError("preflight credential payload is invalid")
        previous: bytes | None = None
        if path.exists() and not path.is_symlink():
            previous = self._read_private(
                path,
                expected_uid=uid,
                expected_gid=gid,
            )
        if previous == payload:
            return False
        temporary: Path | None = None
        try:
            descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
            temporary = Path(raw)
            os.fchmod(descriptor, 0o600)
            os.fchown(descriptor, uid, gid)
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            temporary = None
            parent_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        except OSError as exc:
            raise CredentialInstallError("preflight credential publication failed") from exc
        finally:
            if temporary is not None:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
        return True

    @staticmethod
    def _read_private(path: Path, *, expected_uid: int, expected_gid: int) -> bytes:
        try:
            metadata = os.lstat(path)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != expected_uid
                or metadata.st_gid != expected_gid
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_nlink != 1
                or metadata.st_size <= 0
                or metadata.st_size > 1 << 20
            ):
                raise CredentialInstallError("private credential authority is unsafe")
            return path.read_bytes()
        except OSError as exc:
            raise CredentialInstallError("private credential authority is unavailable") from exc

    def _read_public_root(self, path: Path) -> bytes:
        try:
            metadata = os.lstat(path)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.root_uid
                or metadata.st_gid != self.root_gid
                or stat.S_IMODE(metadata.st_mode) not in {0o444, 0o644}
                or metadata.st_nlink != 1
                or metadata.st_size <= 0
                or metadata.st_size > 4 << 20
            ):
                raise CredentialInstallError("preflight authority manifest is unsafe")
            return path.read_bytes()
        except OSError as exc:
            raise CredentialInstallError("preflight authority manifest is unavailable") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("command", choices=("install", "check"))
    parser.add_argument("--team-id", type=UUID)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
        installer = PreflightCredentialInstaller()
        if args.command == "install":
            if args.team_id is None:
                raise CredentialInstallError("credential install requires exact team ID")
            result = installer.install(args.team_id)
        else:
            if args.team_id is not None:
                raise CredentialInstallError("credential check rejects team override")
            result = installer.check()
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0 if result["ok"] else 1
    except CredentialInstallError as exc:
        sys.stderr.write(json.dumps({"error": str(exc)}, sort_keys=True) + "\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CredentialInstallError",
    "CredentialPaths",
    "PreflightCredentialInstaller",
    "main",
    "render_readonly_probe_sql",
]
