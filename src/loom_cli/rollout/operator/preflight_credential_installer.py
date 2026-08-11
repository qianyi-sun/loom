"""Root-only convergence for installed least-privilege preflight credentials."""

from __future__ import annotations

import argparse
import base64
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
import time
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
    READONLY_MINIO_CREDENTIAL_PATH,
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
from loom_cli.rollout.readonly_minio_bootstrap import (
    READONLY_MINIO_ACCESS_KEY,
    READONLY_MINIO_POLICY_NAME,
    ReadonlyMinioCredential,
    readonly_minio_policy,
    readonly_minio_policy_bytes,
    readonly_minio_policy_digest,
)

_ROOT_KUBECONFIG = Path("/etc/rancher/k3s/k3s.yaml")
_RUNNER_REPO = Path("/opt/loom-staging-runner/source")
_READONLY_MANIFEST = _RUNNER_REPO / "deploy/k8s/staging-rollout-readonly.yaml"
_REHEARSAL_MANIFEST = _RUNNER_REPO / "deploy/k8s/staging-rollout-rehearsal-authority.yaml"
_APPLICATION_TOKEN_SOURCE = Path("/etc/loom/staging-rollout-readonly-probe-token")
_DATABASE_CREDENTIAL_SOURCE = Path("/etc/loom/staging-rollout-readonly-db.json")
_MINIO_CREDENTIAL_SOURCE = Path("/etc/loom/staging-rollout-readonly-minio.json")
_SERVICE_USER = "loom-rollout"
_TOKEN_DURATION = "6h"
_TOKEN_AUDIENCE = "https://kubernetes.default.svc.cluster.local"
_RUNTIME_MIN_REMAINING_SECONDS = 2 * 60 * 60
_INSTALL_MIN_REMAINING_SECONDS = 4 * 60 * 60
_TOKEN_RE = re.compile(r"^[A-Za-z0-9._~-]{32,1024}$")
_READONLY_PROBE_NAME = "staging-rollout-readonly-probe"
_READONLY_PROBE_ACTOR = "deployment:staging-rollout"
_MINIO_AUTHORITY_RETRY_DELAYS = (0.25, 0.5, 1.0, 1.0)
_CHILD_ENVIRONMENT = {
    "HOME": "/root",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "USER": "root",
}


def _canonical_minio_policy(value: object) -> tuple[object, ...]:
    """Canonicalize the set-valued fields in one exact read-only policy."""
    if not isinstance(value, dict) or set(value) != {"Version", "Statement"}:
        raise ValueError("MinIO policy shape is invalid")
    version = value["Version"]
    statements = value["Statement"]
    if not isinstance(version, str) or not isinstance(statements, list) or not statements:
        raise ValueError("MinIO policy shape is invalid")
    canonical_statements: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = []
    for statement in statements:
        if not isinstance(statement, dict) or set(statement) != {
            "Effect",
            "Action",
            "Resource",
        }:
            raise ValueError("MinIO policy statement is invalid")
        effect = statement["Effect"]
        actions = statement["Action"]
        resources = statement["Resource"]
        if (
            not isinstance(effect, str)
            or not isinstance(actions, list)
            or not actions
            or not all(isinstance(action, str) and action for action in actions)
            or len(set(actions)) != len(actions)
            or not isinstance(resources, list)
            or not resources
            or not all(isinstance(resource, str) and resource for resource in resources)
            or len(set(resources)) != len(resources)
        ):
            raise ValueError("MinIO policy statement is invalid")
        canonical_statements.append(
            (effect, tuple(sorted(actions)), tuple(sorted(resources)))
        )
    if len(set(canonical_statements)) != len(canonical_statements):
        raise ValueError("MinIO policy contains duplicate statements")
    return (version, *sorted(canonical_statements))


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
ON CONFLICT (token_hash) DO UPDATE
SET team_id = EXCLUDED.team_id
WHERE tokens.team_id IS DISTINCT FROM EXCLUDED.team_id
  AND tokens.name = EXCLUDED.name
  AND tokens.type = EXCLUDED.type
  AND tokens.scopes = EXCLUDED.scopes
  AND tokens.created_by_user_id IS NULL
  AND tokens.created_by_actor = EXCLUDED.created_by_actor
  AND tokens.expires_at IS NULL
  AND tokens.revoked_at IS NULL;
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
Sleep = Callable[[float], None]


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
    minio_credential_source: Path = _MINIO_CREDENTIAL_SOURCE
    credential_root: Path = PREFLIGHT_CREDENTIAL_ROOT
    readonly_kubeconfig: Path = READONLY_KUBECONFIG_PATH
    readonly_database_credential: Path = READONLY_DATABASE_CREDENTIAL_PATH
    readonly_minio_credential: Path = READONLY_MINIO_CREDENTIAL_PATH
    readonly_token: Path = READONLY_TOKEN_PATH
    rehearsal_kubeconfig: Path = REHEARSAL_KUBECONFIG_PATH


@dataclass(slots=True)
class PreflightCredentialInstaller:
    """Converge two bounded SA kubeconfigs and one fixed readonly API token."""

    paths: CredentialPaths = CredentialPaths()
    run: Run = _run
    now: Now = lambda: datetime.now(UTC)
    sleep: Sleep = time.sleep
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
        minio_credential = self._load_or_create_minio_credential()
        self._converge_minio_authority(minio_credential)
        application_token = self._load_or_create_application_token()
        self._converge_application_probe(application_token, team_id=team_id)
        kubeconfigs = self._refresh_kubeconfigs(source_kubeconfig=source_kubeconfig)
        self._ensure_private_directory(self.paths.credential_root)
        changed = {
            str(self.paths.readonly_token): self._atomic_write(
                self.paths.readonly_token,
                application_token.rstrip() + b"\n",
            ),
            str(self.paths.readonly_database_credential): self._atomic_write(
                self.paths.readonly_database_credential,
                database_credential.to_bytes(),
            ),
            str(self.paths.readonly_minio_credential): self._atomic_write(
                self.paths.readonly_minio_credential,
                minio_credential.to_bytes(),
            ),
        }
        result = self.check(minimum_token_remaining_seconds=_INSTALL_MIN_REMAINING_SECONDS)
        if not result["ok"]:
            raise CredentialInstallError("installed preflight credentials did not verify")
        authority = result["authority"]
        kubeconfig_authority = kubeconfigs["authority"]
        if isinstance(authority, dict) and isinstance(kubeconfig_authority, dict):
            for label in ("readonly", "rehearsal"):
                installed = authority.get(label)
                refreshed = kubeconfig_authority.get(label)
                if isinstance(installed, dict) and isinstance(refreshed, dict):
                    installed.update(refreshed)
        kubeconfig_changes = kubeconfigs["changed"]
        if not isinstance(kubeconfig_changes, list) or any(
            not isinstance(path, str) for path in kubeconfig_changes
        ):
            raise CredentialInstallError("preflight kubeconfig change ledger is invalid")
        changed_paths = set(kubeconfig_changes)
        changed_paths.update(path for path, value in changed.items() if value)
        return {
            "ok": True,
            "changed": sorted(changed_paths),
            "authority": authority,
        }

    def refresh(self) -> dict[str, object]:
        """Refresh only the two root-minted TokenRequest kubeconfigs."""
        if self.euid != 0:
            raise CredentialInstallError("preflight credential refresh requires root")
        return self._refresh_kubeconfigs()

    def _refresh_kubeconfigs(
        self,
        *,
        source_kubeconfig: bytes | None = None,
    ) -> dict[str, object]:
        existing = self._check_kubeconfigs(
            minimum_token_remaining_seconds=_INSTALL_MIN_REMAINING_SECONDS
        )
        if existing["ok"]:
            authority = existing["authority"]
            self._add_live_kubeconfig_authority(authority)
            return {"ok": True, "changed": [], "authority": authority}
        source = source_kubeconfig or self._minified_source_kubeconfig()
        # Both TokenRequests must succeed before either installed file changes.
        readonly = self._request_kubeconfig(
            source,
            namespace="loom-staging",
            service_account="loom-rollout-readonly",
        )
        rehearsal = self._request_kubeconfig(
            source,
            namespace="loom-rollout-system",
            service_account="loom-rollout-rehearsal",
        )
        self._ensure_private_directory(self.paths.credential_root)
        changed = {
            str(self.paths.readonly_kubeconfig): self._atomic_write(
                self.paths.readonly_kubeconfig,
                readonly,
            ),
            str(self.paths.rehearsal_kubeconfig): self._atomic_write(
                self.paths.rehearsal_kubeconfig,
                rehearsal,
            ),
        }
        verified = self._check_kubeconfigs(
            minimum_token_remaining_seconds=_INSTALL_MIN_REMAINING_SECONDS
        )
        if not verified["ok"]:
            raise CredentialInstallError("refreshed preflight kubeconfigs did not verify")
        authority = verified["authority"]
        self._add_live_kubeconfig_authority(authority)
        return {
            "ok": True,
            "changed": sorted(path for path, value in changed.items() if value),
            "authority": authority,
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
                "--stdin",
                "service/loom-postgres-rw",
                "--",
                "psql",
                "--no-psqlrc",
                "-AtX",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "postgres",
                "-d",
                "loom",
                "-f",
                "-",
            ),
            input=render_readonly_probe_sql(token_hash=token_hash, team_id=team_id),
            timeout=60,
            require_output=False,
        )

    def check(
        self,
        *,
        minimum_token_remaining_seconds: int = _RUNTIME_MIN_REMAINING_SECONDS,
    ) -> dict[str, object]:
        kubeconfigs = self._check_kubeconfigs(
            minimum_token_remaining_seconds=minimum_token_remaining_seconds
        )
        kubeconfig_failures = kubeconfigs["failures"]
        kubeconfig_authority = kubeconfigs["authority"]
        if not isinstance(kubeconfig_failures, list) or any(
            not isinstance(label, str) for label in kubeconfig_failures
        ):
            raise CredentialInstallError("preflight kubeconfig failure ledger is invalid")
        if not isinstance(kubeconfig_authority, dict):
            raise CredentialInstallError("preflight kubeconfig authority is invalid")
        failures = list(kubeconfig_failures)
        authority = dict(kubeconfig_authority)
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
        try:
            minio_credential = ReadonlyMinioCredential.from_bytes(
                self._read_private(
                    self.paths.readonly_minio_credential,
                    expected_uid=self.service_uid,
                    expected_gid=self.service_gid,
                )
            )
            self._check_minio_authority(minio_credential)
            authority["readonly-minio"] = {
                "credential-metadata-digest": minio_credential.metadata_digest,
                "policy-digest": readonly_minio_policy_digest(),
                "schema-version": 1,
            }
        except (CredentialInstallError, ValueError):
            failures.append("readonly-minio")
        if not {"readonly", "rehearsal"}.intersection(failures):
            try:
                self._add_live_kubeconfig_authority(authority)
            except CredentialInstallError:
                failures.append("kubeconfig-readback")
        return {"ok": not failures, "failures": sorted(failures), "authority": authority}

    def _check_kubeconfigs(
        self,
        *,
        minimum_token_remaining_seconds: int,
    ) -> dict[str, object]:
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
                    minimum_remaining_seconds=minimum_token_remaining_seconds,
                )
                if evidence.audiences != (_TOKEN_AUDIENCE,):
                    raise ValueError("preflight TokenRequest audience is invalid")
                authority[label] = {
                    "audiences": list(evidence.audiences),
                    "expires_at": evidence.expires_at,
                    "metadata_digest": evidence.metadata_digest,
                    "subject": evidence.subject,
                }
            except (CredentialInstallError, ValueError, yaml.YAMLError):
                failures.append(label)
        return {"ok": not failures, "failures": sorted(failures), "authority": authority}

    def _add_live_kubeconfig_authority(self, authority: object) -> None:
        if not isinstance(authority, dict):
            raise CredentialInstallError("preflight kubeconfig authority is invalid")
        for label, path, namespace, account, capabilities in (
            (
                "readonly",
                self.paths.readonly_kubeconfig,
                "loom-staging",
                "loom-rollout-readonly",
                (
                    {
                        "group": "",
                        "name": "loom-postgres-1",
                        "namespace": "loom-staging",
                        "resource": "pods",
                        "subresource": "portforward",
                        "verb": "create",
                    },
                    {
                        "group": "",
                        "name": "loom-postgres-2",
                        "namespace": "loom-staging",
                        "resource": "pods",
                        "subresource": "portforward",
                        "verb": "create",
                    },
                    {
                        "group": "",
                        "name": "loom-postgres-3",
                        "namespace": "loom-staging",
                        "resource": "pods",
                        "subresource": "portforward",
                        "verb": "create",
                    },
                    {
                        "group": "",
                        "name": "loom-minio-0",
                        "namespace": "loom-staging",
                        "resource": "pods",
                        "subresource": "portforward",
                        "verb": "create",
                    },
                ),
            ),
            (
                "rehearsal",
                self.paths.rehearsal_kubeconfig,
                "loom-rollout-system",
                "loom-rollout-rehearsal",
                ({"group": "", "resource": "namespaces", "verb": "create"},),
            ),
        ):
            installed = authority.get(label)
            if not isinstance(installed, dict):
                raise CredentialInstallError("preflight kubeconfig authority is incomplete")
            installed.update(
                self._live_kubeconfig_authority(
                    path,
                    namespace=namespace,
                    service_account=account,
                    capabilities=capabilities,
                )
            )

    def _live_kubeconfig_authority(
        self,
        path: Path,
        *,
        namespace: str,
        service_account: str,
        capabilities: tuple[dict[str, str], ...],
    ) -> dict[str, object]:
        subject_review = self._server_review(
            path,
            "/apis/authentication.k8s.io/v1/selfsubjectreviews",
            {"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"},
        )
        status = subject_review.get("status")
        user_info = status.get("userInfo") if isinstance(status, dict) else None
        subject = user_info.get("username") if isinstance(user_info, dict) else None
        expected = f"system:serviceaccount:{namespace}:{service_account}"
        if subject != expected:
            raise CredentialInstallError("preflight kubeconfig subject readback failed")
        required_allowed = all(
            self._self_subject_access(path, capability) for capability in capabilities
        )
        mint_allowed = self._self_subject_access(
            path,
            {
                "group": "",
                "namespace": namespace,
                "resource": "serviceaccounts",
                "subresource": "token",
                "verb": "create",
            },
        )
        if not required_allowed or mint_allowed:
            raise CredentialInstallError("preflight kubeconfig capability readback failed")
        digest = hashlib.sha256(
            json.dumps(
                {
                    "required_capabilities": capabilities,
                    "subject": subject,
                    "token_mint_allowed": False,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return {
            "capability_digest": digest,
            "subject": subject,
            "token_mint_allowed": False,
        }

    def _self_subject_access(self, path: Path, attributes: dict[str, str]) -> bool:
        review = self._server_review(
            path,
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            {
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {"resourceAttributes": attributes},
            },
        )
        status = review.get("status")
        if not isinstance(status, dict) or type(status.get("allowed")) is not bool:
            raise CredentialInstallError("preflight kubeconfig access readback failed")
        if status.get("evaluationError") not in {None, ""}:
            raise CredentialInstallError("preflight kubeconfig access readback failed")
        return bool(status["allowed"])

    def _server_review(
        self,
        path: Path,
        uri: str,
        spec: dict[str, object],
    ) -> dict[str, object]:
        result = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(path),
                "create",
                "--raw",
                uri,
                "--request-timeout=10s",
                "-f",
                "-",
            ),
            input=json.dumps(spec, sort_keys=True, separators=(",", ":")),
            timeout=15,
        )
        if len(result.stdout.encode()) > 1 << 20:
            raise CredentialInstallError("preflight kubeconfig server readback is invalid")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise CredentialInstallError("preflight kubeconfig server readback is invalid") from exc
        if not isinstance(value, dict):
            raise CredentialInstallError("preflight kubeconfig server readback is invalid")
        return value

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
                "--stdin",
                "service/loom-postgres-rw",
                "--",
                "psql",
                "--no-psqlrc",
                "-AtX",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "postgres",
                "-d",
                credential.database,
                "-f",
                "-",
            ),
            input=render_readonly_role_sql(credential),
            timeout=60,
            require_output=False,
        )

    def _load_or_create_minio_credential(self) -> ReadonlyMinioCredential:
        path = self.paths.minio_credential_source
        if path.exists() or path.is_symlink():
            try:
                return ReadonlyMinioCredential.from_bytes(
                    self._read_private(
                        path,
                        expected_uid=self.root_uid,
                        expected_gid=self.root_gid,
                    )
                )
            except ValueError as exc:
                raise CredentialInstallError(
                    "readonly MinIO credential authority is invalid"
                ) from exc
        self._require_root_directory(path.parent)
        credential = ReadonlyMinioCredential(
            access_key=READONLY_MINIO_ACCESS_KEY,
            secret_key=secrets.token_urlsafe(48),
        )
        self._atomic_write_private(
            path,
            credential.to_bytes(),
            uid=self.root_uid,
            gid=self.root_gid,
            parent=path.parent,
        )
        return credential

    @staticmethod
    def _minio_admin_prefix() -> tuple[str, ...]:
        return (
            "kubectl",
            "--kubeconfig",
            str(_ROOT_KUBECONFIG),
            "--namespace",
            "loom-staging",
            "exec",
            "--stdin",
            "pod/loom-minio-0",
            "--",
            "/bin/sh",
            "-eu",
            "-c",
        )

    def _converge_minio_authority(self, credential: ReadonlyMinioCredential) -> None:
        # MinIO's policy-create operation is not a portable idempotent update:
        # some supported ``mc`` versions reject an already-present policy even
        # when its document and user binding are exact.  Prove the complete
        # authority first and avoid all writes when it already converged.
        try:
            self._check_minio_authority(credential)
        except CredentialInstallError:
            pass
        else:
            return
        policy_payload = base64.b64encode(readonly_minio_policy_bytes()).decode("ascii")
        script = "\n".join(
            (
                "IFS= read -r access_key",
                "IFS= read -r secret_key",
                'test -n "${MINIO_ROOT_USER:-}"',
                'test -n "${MINIO_ROOT_PASSWORD:-}"',
                "umask 077",
                "policy=$(mktemp)",
                "trap 'rm -f -- \"$policy\"' EXIT HUP INT TERM",
                f'printf %s {policy_payload!r} | base64 -d >"$policy"',
                'export MC_HOST_rollout="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"',
                f'/usr/bin/mc admin policy create rollout {READONLY_MINIO_POLICY_NAME!r} "$policy" >/dev/null',
                'printf "%s\\n%s\\n" "$access_key" "$secret_key" | /usr/bin/mc admin user add rollout >/dev/null',
                f'/usr/bin/mc admin policy attach rollout {READONLY_MINIO_POLICY_NAME!r} --user "$access_key" >/dev/null',
            )
        )
        self._command(
            (*self._minio_admin_prefix(), script),
            input=f"{credential.access_key}\n{credential.secret_key}\n",
            timeout=60,
            require_output=False,
        )
        self._check_minio_authority(credential)

    def _check_minio_authority(self, credential: ReadonlyMinioCredential) -> None:
        last_error: CredentialInstallError | None = None
        for attempt in range(len(_MINIO_AUTHORITY_RETRY_DELAYS) + 1):
            try:
                self._check_minio_authority_once(credential)
                return
            except CredentialInstallError as exc:
                last_error = exc
                if attempt < len(_MINIO_AUTHORITY_RETRY_DELAYS):
                    self.sleep(_MINIO_AUTHORITY_RETRY_DELAYS[attempt])
        if last_error is None:  # pragma: no cover - fixed non-empty range owns this
            raise CredentialInstallError("readonly MinIO authority check did not run")
        raise last_error

    def _check_minio_authority_once(self, credential: ReadonlyMinioCredential) -> None:
        script = "\n".join(
            (
                'test -n "${MINIO_ROOT_USER:-}"',
                'test -n "${MINIO_ROOT_PASSWORD:-}"',
                'export MC_HOST_rollout="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"',
                f"/usr/bin/mc admin user info rollout {credential.access_key!r} --json",
                f"/usr/bin/mc admin policy info rollout {READONLY_MINIO_POLICY_NAME!r} --json",
            )
        )
        result = self._command(
            (*self._minio_admin_prefix(), script),
            timeout=60,
        )
        lines = tuple(line for line in result.stdout.splitlines() if line.strip())
        if len(lines) != 2:
            raise CredentialInstallError("readonly MinIO authority evidence is invalid")
        try:
            user = json.loads(lines[0])
            policy = json.loads(lines[1])
        except json.JSONDecodeError as exc:
            raise CredentialInstallError("readonly MinIO authority evidence is invalid") from exc
        if not isinstance(user, dict) or user.get("status") != "success":
            raise CredentialInstallError("readonly MinIO user authority drifted")
        policy_name = user.get("policyName", user.get("policy"))
        if policy_name != READONLY_MINIO_POLICY_NAME:
            raise CredentialInstallError("readonly MinIO user authority drifted")
        if not isinstance(policy, dict) or policy.get("status") != "success":
            raise CredentialInstallError("readonly MinIO policy authority drifted")
        policy_info = policy.get("policyInfo")
        if (
            policy.get("policy") != READONLY_MINIO_POLICY_NAME
            or not isinstance(policy_info, dict)
            or policy_info.get("PolicyName") != READONLY_MINIO_POLICY_NAME
        ):
            raise CredentialInstallError("readonly MinIO policy authority drifted")
        try:
            observed_policy = _canonical_minio_policy(policy_info.get("Policy"))
            expected_policy = _canonical_minio_policy(readonly_minio_policy())
        except ValueError as exc:
            raise CredentialInstallError("readonly MinIO policy authority drifted") from exc
        if observed_policy != expected_policy:
            raise CredentialInstallError("readonly MinIO policy authority drifted")

    def _minified_source_kubeconfig(self) -> bytes:
        self._read_root_kubeconfig(self.paths.root_kubeconfig)
        result = self._command(
            (
                "kubectl",
                "--kubeconfig",
                str(self.paths.root_kubeconfig),
                "config",
                "view",
                "--raw",
                "--minify",
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
                f"--audience={_TOKEN_AUDIENCE}",
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
        try:
            result = self.run(argv, input=input, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired):
            raise CredentialInstallError("preflight credential command failed") from None
        # stderr is a diagnostic stream, not a failure signal.  kubectl and mc
        # may emit bounded warnings there while still returning success.  The
        # subprocess exit status and the command-specific output validators
        # below are the fail-closed authority.
        if result.returncode != 0:
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

    def _read_root_kubeconfig(self, path: Path) -> bytes:
        try:
            metadata = os.lstat(path)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.root_uid
                or metadata.st_gid != self.root_gid
                or stat.S_IMODE(metadata.st_mode) not in {0o600, 0o644}
                or metadata.st_nlink != 1
                or metadata.st_size <= 0
                or metadata.st_size > 1 << 20
            ):
                raise CredentialInstallError("root kubeconfig authority is unsafe")
            return path.read_bytes()
        except OSError as exc:
            raise CredentialInstallError("root kubeconfig authority is unavailable") from exc

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
    parser.add_argument("command", choices=("install", "check", "refresh"))
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
        elif args.command == "check":
            if args.team_id is not None:
                raise CredentialInstallError("credential check rejects team override")
            result = installer.check()
        else:
            if args.team_id is not None:
                raise CredentialInstallError("credential refresh rejects team override")
            result = installer.refresh()
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
