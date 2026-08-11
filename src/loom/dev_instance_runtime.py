"""Production lifecycle executors for shared-fixture dev instances.

Secrets travel only through database connections, in-memory HTTP headers, or
``kubectl apply -f -`` stdin. They are never placed in argv, logs, rendered
runtime manifests, or API responses.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
import shlex
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import httpx
import psycopg
import yaml  # type: ignore[import-untyped]
from psycopg import sql

from loom.dev_instance import DevInstanceIdentity, RequestedPolicy
from loom.dev_instance_manifest import (
    DevInstanceManifestConfig,
    dev_instance_manifest_documents,
    personal_dev_preparation_manifest_documents,
)
from loom.dev_instance_provisioner import OwnerAccessSnapshot, dev_buckets
from loom.personal_dev_reconciler import PersonalDevReadinessObservation


class DevInstanceRuntimeError(RuntimeError):
    """Bounded runtime failure safe to persist and surface."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    stdout: str
    stderr: str


class AsyncCommandRunner:
    """Executes a fixed argv with optional stdin and bounded diagnostics."""

    async def run(
        self,
        argv: list[str],
        *,
        stdin: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> CommandResult:
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=(
                    asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin.encode() if stdin is not None else None),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            if "process" in locals():
                process.kill()
                await process.wait()
            raise DevInstanceRuntimeError("cluster command timed out") from None
        except asyncio.CancelledError:
            if "process" in locals() and process.returncode is None:
                process.kill()
                await process.wait()
            raise
        except OSError:
            raise DevInstanceRuntimeError("cluster command could not start") from None
        if process.returncode != 0:
            # kubectl stderr can contain server URLs and admission details. Keep
            # the public error bounded; protected process logs retain specifics.
            raise DevInstanceRuntimeError(
                f"cluster command failed with exit code {process.returncode}",
            )
        return CommandResult(
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
        )


@dataclass(slots=True)
class KubectlClient:
    executable: str
    context: str = ""
    runner: AsyncCommandRunner | Any = None

    def __post_init__(self) -> None:
        if not self.executable:
            raise ValueError("kubectl executable is required")
        if self.runner is None:
            self.runner = AsyncCommandRunner()

    def _argv(self, *parts: str) -> list[str]:
        argv = [self.executable]
        if self.context:
            argv.extend(["--context", self.context])
        argv.extend(parts)
        return argv

    async def apply(self, manifest: str, *, timeout_seconds: float = 120.0) -> None:
        await self.runner.run(
            self._argv("apply", "--server-side", "--field-manager", "loom-dev-instance", "-f", "-"),
            stdin=manifest,
            timeout_seconds=timeout_seconds,
        )

    async def wait_job(self, namespace: str, name: str) -> None:
        await self.runner.run(
            self._argv(
                "wait",
                "--namespace",
                namespace,
                "--for=condition=complete",
                "--timeout=600s",
                f"job/{name}",
            ),
            timeout_seconds=630,
        )

    async def wait_deployment(self, namespace: str, name: str) -> None:
        await self.runner.run(
            self._argv(
                "rollout",
                "status",
                "--namespace",
                namespace,
                "--timeout=300s",
                f"deployment/{name}",
            ),
            timeout_seconds=330,
        )

    async def delete_namespace(self, namespace: str) -> None:
        await self.runner.run(
            self._argv(
                "delete",
                "namespace",
                namespace,
                "--ignore-not-found=true",
                "--wait=true",
                "--timeout=300s",
            ),
            timeout_seconds=330,
        )

    async def read_secret(self, namespace: str, name: str) -> dict[str, bytes]:
        data = await self.read_secret_optional(namespace, name)
        if data is None:
            raise DevInstanceRuntimeError("cluster secret is unavailable")
        return data

    async def read_secret_optional(
        self,
        namespace: str,
        name: str,
    ) -> dict[str, bytes] | None:
        if not await self.namespace_exists(namespace):
            return None
        result = await self.runner.run(
            self._argv(
                "get",
                "secret",
                name,
                "--namespace",
                namespace,
                "--ignore-not-found=true",
                "-o",
                "json",
            ),
            timeout_seconds=30,
        )
        try:
            raw = json.loads(result.stdout)
            if raw == {}:
                return None
            data = raw["data"]
            if not isinstance(data, dict):
                raise TypeError
            return {
                str(key): base64.b64decode(str(value), validate=True) for key, value in data.items()
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise DevInstanceRuntimeError("cluster secret response was invalid") from None

    async def namespace_exists(self, namespace: str) -> bool:
        result = await self.runner.run(
            self._argv(
                "get",
                "namespace",
                namespace,
                "--ignore-not-found=true",
                "-o",
                "json",
            ),
            timeout_seconds=30,
        )
        try:
            value = json.loads(result.stdout)
            if value == {}:
                return False
            metadata = value["metadata"]
            if not isinstance(metadata, dict) or not isinstance(metadata.get("name"), str):
                raise TypeError
            return bool(metadata["name"] == namespace)
        except (KeyError, TypeError, json.JSONDecodeError):
            raise DevInstanceRuntimeError("cluster namespace response was invalid") from None

    async def read_resource_json(
        self,
        *,
        namespace: str,
        kind: str,
        name: str,
    ) -> dict[str, Any]:
        result = await self.runner.run(
            self._argv("get", kind, name, "--namespace", namespace, "-o", "json"),
            timeout_seconds=30,
        )
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise DevInstanceRuntimeError("cluster resource response was invalid") from None
        if not isinstance(value, dict):
            raise DevInstanceRuntimeError("cluster resource response was invalid")
        return value

    async def exec_stdin(
        self,
        *,
        namespace: str,
        pod: str,
        container: str,
        script: str,
        stdin: str | None = None,
    ) -> None:
        await self.runner.run(
            self._argv(
                "exec",
                "--namespace",
                namespace,
                "--container",
                container,
                "--stdin",
                pod,
                "--",
                "/bin/sh",
                "-eu",
                "-c",
                script,
            ),
            stdin=stdin,
            timeout_seconds=60,
        )


def instance_database_url(admin_url: str, identity: DevInstanceIdentity, password: str) -> str:
    """Derive the role-scoped instance DSN without altering endpoint/TLS query."""
    parsed = urlsplit(admin_url)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise ValueError("shared fixture database URL must use PostgreSQL")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("shared fixture database URL must include a host")
    host = f"[{hostname}]" if ":" in hostname else hostname
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    netloc = f"{quote(identity.db_role, safe='')}:{quote(password, safe='')}@{host}"
    scheme = "postgresql+psycopg" if parsed.scheme == "postgresql+psycopg" else "postgresql"
    return urlunsplit((scheme, netloc, f"/{identity.database}", parsed.query, ""))


def fixture_database_url(admin_url: str, database: str) -> str:
    """Retarget a protected fixture-admin URL to one validated database name."""
    if not database or any(
        char not in "abcdefghijklmnopqrstuvwxyz0123456789_" for char in database
    ):
        raise ValueError("fixture database name is invalid")
    parsed = urlsplit(admin_url)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise ValueError("shared fixture database URL must use PostgreSQL")
    return urlunsplit((parsed.scheme, parsed.netloc, f"/{database}", parsed.query, ""))


@dataclass(slots=True)
class PsycopgSharedFixtureSqlExecutor:
    admin_url: str

    @property
    def _connect_url(self) -> str:
        return self.admin_url.replace("postgresql+psycopg://", "postgresql://", 1)

    async def apply_role_and_database(
        self,
        identity: DevInstanceIdentity,
        *,
        role_sql: str,
        create_database_sql: str,
    ) -> None:
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url,
                autocommit=True,
            ) as connection:
                await connection.execute(role_sql)
                exists = await connection.execute(
                    "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                    (identity.database,),
                )
                if await exists.fetchone() is None:
                    await connection.execute(create_database_sql)
                await connection.execute(
                    sql.SQL("REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM PUBLIC").format(
                        sql.Identifier(identity.database),
                    ),
                )
                await connection.execute(
                    sql.SQL("GRANT CONNECT, TEMPORARY ON DATABASE {} TO {}").format(
                        sql.Identifier(identity.database),
                        sql.Identifier(identity.db_role),
                    ),
                )
        except Exception:
            raise DevInstanceRuntimeError("shared fixture database provisioning failed") from None

    async def drop_database_and_role(self, identity: DevInstanceIdentity) -> None:
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url,
                autocommit=True,
            ) as connection:
                await connection.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (identity.database,),
                )
                await connection.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(identity.database)),
                )
                await connection.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(identity.db_role)),
                )
        except Exception:
            raise DevInstanceRuntimeError("shared fixture database cleanup failed") from None


@dataclass(slots=True)
class PsycopgOwnerAccessBootstrap:
    """Seed only the requesting owner/team and current credential.

    It connects with the instance's dedicated database role after migrations
    complete. Values are always bound parameters: password hashes and
    credential hashes never enter SQL text, argv, manifests, or logs.
    """

    database_admin_url: str

    async def bootstrap(
        self,
        identity: DevInstanceIdentity,
        *,
        password: str,
        access: OwnerAccessSnapshot,
    ) -> None:
        connect_url = instance_database_url(
            self.database_admin_url,
            identity,
            password,
        ).replace("postgresql+psycopg://", "postgresql://", 1)
        try:
            async with await psycopg.AsyncConnection.connect(connect_url) as connection:
                # A keep-data teardown deliberately preserves this database. Revoke
                # every old worker/batch credential before the endpoint can become
                # ready again so a stale submit-host env file can never reactivate
                # workers across lifecycle or candidate generations.
                await connection.execute(
                    """
                    UPDATE tokens
                    SET revoked_at = %s
                    WHERE type = 'worker' AND revoked_at IS NULL
                    """,
                    (datetime.now(UTC),),
                )
                await connection.execute(
                    """
                    INSERT INTO teams (
                        id, name, created_at, public_registration_enabled
                    ) VALUES (%s, %s, %s, false)
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                    """,
                    (access.team_id, access.team_name, access.team_created_at),
                )
                await connection.execute(
                    """
                    INSERT INTO users (
                        id, email, username, username_normalized, display_name,
                        password_hash, password_set_at, status, disabled_at,
                        is_platform_admin, created_at, last_login_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        email = EXCLUDED.email,
                        username = EXCLUDED.username,
                        username_normalized = EXCLUDED.username_normalized,
                        display_name = EXCLUDED.display_name,
                        password_hash = EXCLUDED.password_hash,
                        password_set_at = EXCLUDED.password_set_at,
                        status = EXCLUDED.status,
                        disabled_at = EXCLUDED.disabled_at,
                        is_platform_admin = false,
                        last_login_at = EXCLUDED.last_login_at
                    """,
                    (
                        access.user_id,
                        access.email,
                        access.username,
                        access.username_normalized,
                        access.display_name,
                        access.password_hash,
                        access.password_set_at,
                        access.user_status,
                        access.user_disabled_at,
                        access.user_created_at,
                        access.user_last_login_at,
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO team_memberships (team_id, user_id, role, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (team_id, user_id) DO UPDATE
                    SET role = EXCLUDED.role
                    """,
                    (
                        access.team_id,
                        access.user_id,
                        access.membership_role,
                        access.membership_created_at,
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO team_quotas (
                        team_id, fair_share_weight, max_attempts_ceiling,
                        in_flight_count, license_allowlist, taskset_max_count,
                        taskset_max_storage_bytes, allow_private_endpoints
                    ) VALUES (%s, %s, %s, 0, %s, %s, %s, %s)
                    ON CONFLICT (team_id) DO UPDATE SET
                        fair_share_weight = EXCLUDED.fair_share_weight,
                        max_attempts_ceiling = EXCLUDED.max_attempts_ceiling,
                        license_allowlist = EXCLUDED.license_allowlist,
                        taskset_max_count = EXCLUDED.taskset_max_count,
                        taskset_max_storage_bytes = EXCLUDED.taskset_max_storage_bytes,
                        allow_private_endpoints = EXCLUDED.allow_private_endpoints
                    """,
                    (
                        access.team_id,
                        access.fair_share_weight,
                        access.max_attempts_ceiling,
                        list(access.license_allowlist),
                        access.taskset_max_count,
                        access.taskset_max_storage_bytes,
                        access.allow_private_endpoints,
                    ),
                )
                if access.bearer is not None:
                    bearer = access.bearer
                    await connection.execute(
                        """
                        INSERT INTO tokens (
                            token_hash, name, type, scopes, team_id,
                            created_by_user_id, created_by_actor, issued_at,
                            expires_at, revoked_at, last_used_at, last_seen_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            NULL, NULL, NULL
                        )
                        ON CONFLICT (token_hash) DO UPDATE SET
                            name = EXCLUDED.name,
                            type = EXCLUDED.type,
                            scopes = EXCLUDED.scopes,
                            team_id = EXCLUDED.team_id,
                            created_by_user_id = EXCLUDED.created_by_user_id,
                            created_by_actor = EXCLUDED.created_by_actor,
                            expires_at = EXCLUDED.expires_at,
                            revoked_at = NULL
                        """,
                        (
                            bearer.token_hash,
                            bearer.name,
                            bearer.type,
                            list(bearer.scopes),
                            access.team_id,
                            access.user_id,
                            bearer.created_by_actor,
                            bearer.issued_at,
                            bearer.expires_at,
                        ),
                    )
                if access.session is not None:
                    session = access.session
                    await connection.execute(
                        """
                        INSERT INTO user_sessions (
                            session_hash, user_id, current_team_id, csrf_hash,
                            issued_at, expires_at, revoked_at, last_seen_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s)
                        ON CONFLICT (session_hash) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            current_team_id = EXCLUDED.current_team_id,
                            csrf_hash = EXCLUDED.csrf_hash,
                            expires_at = EXCLUDED.expires_at,
                            revoked_at = NULL,
                            last_seen_at = EXCLUDED.last_seen_at
                        """,
                        (
                            session.session_hash,
                            access.user_id,
                            access.team_id,
                            session.csrf_hash,
                            session.issued_at,
                            session.expires_at,
                            session.last_seen_at,
                        ),
                    )
        except Exception:
            raise DevInstanceRuntimeError("instance owner access bootstrap failed") from None


@dataclass(slots=True)
class S3BucketEnsurer:
    client: Any
    region: str = "us-east-1"

    async def ensure_buckets(
        self,
        _identity: DevInstanceIdentity,
        buckets: list[str],
    ) -> None:
        for bucket in buckets:
            await asyncio.to_thread(self._ensure_bucket, bucket)

    def _ensure_bucket(self, bucket: str) -> None:
        try:
            self.client.head_bucket(Bucket=bucket)
            return
        except Exception:
            pass
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if self.region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
        try:
            self.client.create_bucket(**kwargs)
        except Exception:
            try:
                self.client.head_bucket(Bucket=bucket)
            except Exception:
                raise DevInstanceRuntimeError(
                    "shared object-store bucket creation failed"
                ) from None

    async def remove_buckets(
        self,
        _identity: DevInstanceIdentity,
        buckets: list[str],
    ) -> None:
        for bucket in buckets:
            await asyncio.to_thread(self._remove_bucket, bucket)

    def _remove_bucket(self, bucket: str) -> None:
        try:
            paginator = self.client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=bucket):
                objects = [
                    {"Key": item["Key"], "VersionId": item["VersionId"]}
                    for field in ("Versions", "DeleteMarkers")
                    for item in page.get(field, [])
                ]
                for offset in range(0, len(objects), 1000):
                    self.client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": objects[offset : offset + 1000], "Quiet": True},
                    )
            self.client.delete_bucket(Bucket=bucket)
        except Exception as exc:
            code = str(
                getattr(exc, "response", {}).get("Error", {}).get("Code", ""),
            )
            if code not in {"NoSuchBucket", "404"}:
                raise DevInstanceRuntimeError("shared object-store bucket cleanup failed") from None


@dataclass(slots=True)
class KubectlSecretVault:
    kubectl: KubectlClient
    database_admin_url: str
    manifest_config: DevInstanceManifestConfig | None = None
    _admin_tokens: dict[str, str] | None = None
    _object_credentials: dict[str, tuple[str, str]] | None = None
    _database_passwords: dict[str, str] | None = None

    def __post_init__(self) -> None:
        self._admin_tokens = {}
        self._object_credentials = {}
        self._database_passwords = {}

    @staticmethod
    def _password_from_secret(data: dict[str, bytes]) -> str:
        try:
            parsed = urlsplit(data["cp-db-url"].decode())
            password = unquote(parsed.password or "")
        except (KeyError, UnicodeDecodeError, ValueError):
            raise DevInstanceRuntimeError("instance database credential is invalid") from None
        if len(password) < 16 or any(character not in "0123456789abcdef" for character in password):
            raise DevInstanceRuntimeError("instance database credential is invalid")
        return password

    @staticmethod
    def _admin_token_from_secret(data: dict[str, bytes]) -> str:
        try:
            document = data["secrets.toml"].decode()
            token_line = next(line for line in document.splitlines() if line.startswith("token = "))
            return token_line.split('"', 2)[1]
        except (KeyError, StopIteration, UnicodeDecodeError, IndexError):
            raise DevInstanceRuntimeError("instance admin credential is invalid") from None

    async def database_password(self, identity: DevInstanceIdentity) -> str | None:
        assert self._database_passwords is not None
        cached = self._database_passwords.get(identity.name)
        if cached is not None:
            return cached
        data = await self.kubectl.read_secret_optional(identity.namespace, "loom-secrets")
        if data is None:
            return None
        password = self._password_from_secret(data)
        self._database_passwords[identity.name] = password
        return password

    async def store(self, identity: DevInstanceIdentity, password: str) -> str:
        existing_main = await self.kubectl.read_secret_optional(
            identity.namespace,
            "loom-secrets",
        )
        existing_admin = await self.kubectl.read_secret_optional(
            identity.namespace,
            "loom-admin-secret",
        )
        if (existing_main is None) != (existing_admin is None):
            raise DevInstanceRuntimeError("instance secret set is incomplete")
        if existing_main is not None and existing_admin is not None:
            existing_password = self._password_from_secret(existing_main)
            if existing_password != password:
                raise ValueError("instance database password binding changed")
            try:
                object_credentials = (
                    existing_main["minio-access-key"].decode(),
                    existing_main["minio-secret-key"].decode(),
                )
            except (KeyError, UnicodeDecodeError):
                raise DevInstanceRuntimeError("instance object credential is invalid") from None
            admin_token = self._admin_token_from_secret(existing_admin)
            assert self._admin_tokens is not None
            assert self._object_credentials is not None
            assert self._database_passwords is not None
            self._admin_tokens[identity.name] = admin_token
            self._object_credentials[identity.name] = object_credentials
            self._database_passwords[identity.name] = existing_password
            return f"k8s-secret://{identity.namespace}/loom-secrets"
        database_url = instance_database_url(self.database_admin_url, identity, password)
        admin_token = "loom_admin_" + secrets.token_urlsafe(32)
        object_access_key = f"loomdev-{identity.name}"
        object_secret_key = secrets.token_urlsafe(48)
        assert self._admin_tokens is not None
        assert self._object_credentials is not None
        assert self._database_passwords is not None
        self._admin_tokens[identity.name] = admin_token
        self._object_credentials[identity.name] = (object_access_key, object_secret_key)
        self._database_passwords[identity.name] = password
        if self.manifest_config is None:
            namespace = {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": identity.namespace,
                    "labels": {
                        "app.kubernetes.io/managed-by": "loom-dev-instance-controller",
                        "app.kubernetes.io/part-of": "loom",
                        "loom.dev/instance": identity.name,
                        "pod-security.kubernetes.io/enforce": "restricted",
                    },
                },
            }
        else:
            namespace = dev_instance_manifest_documents(identity, self.manifest_config)[0]
        await self.kubectl.apply(yaml.safe_dump(namespace, sort_keys=False))
        labels = {
            "app.kubernetes.io/managed-by": "loom-dev-instance-controller",
            "loom.dev/instance": identity.name,
        }
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "loom-secrets",
                "namespace": identity.namespace,
                "labels": labels,
            },
            "type": "Opaque",
            "stringData": {
                "cp-db-url": database_url,
                "svc-db-url": database_url,
                "gw-db-url": database_url,
                "step-jwt-signing-key": secrets.token_hex(64),
                "minio-access-key": object_access_key,
                "minio-secret-key": object_secret_key,
                "secret-store-master-key": base64.b64encode(secrets.token_bytes(32)).decode(),
            },
        }
        admin = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "loom-admin-secret",
                "namespace": identity.namespace,
                "labels": labels,
            },
            "type": "Opaque",
            "stringData": {"secrets.toml": f'[admin]\ntoken = "{admin_token}"\n'},
        }
        await self.kubectl.apply(
            yaml.safe_dump_all((secret, admin), sort_keys=False, explicit_start=True),
        )
        return f"k8s-secret://{identity.namespace}/loom-secrets"

    async def admin_token(self, identity: DevInstanceIdentity) -> str:
        assert self._admin_tokens is not None
        cached = self._admin_tokens.get(identity.name)
        if cached is not None:
            return cached
        data = await self.kubectl.read_secret(identity.namespace, "loom-admin-secret")
        token = self._admin_token_from_secret(data)
        self._admin_tokens[identity.name] = token
        return token

    async def object_credentials(self, identity: DevInstanceIdentity) -> tuple[str, str]:
        assert self._object_credentials is not None
        cached = self._object_credentials.get(identity.name)
        if cached is not None:
            return cached
        data = await self.kubectl.read_secret(identity.namespace, "loom-secrets")
        try:
            credentials = (
                data["minio-access-key"].decode(),
                data["minio-secret-key"].decode(),
            )
        except (KeyError, UnicodeDecodeError):
            raise DevInstanceRuntimeError("instance object credential is invalid") from None
        self._object_credentials[identity.name] = credentials
        return credentials

    async def delete(self, identity: DevInstanceIdentity) -> None:
        assert self._admin_tokens is not None
        assert self._object_credentials is not None
        assert self._database_passwords is not None
        self._admin_tokens.pop(identity.name, None)
        self._object_credentials.pop(identity.name, None)
        self._database_passwords.pop(identity.name, None)
        # Namespace deletion owns Kubernetes Secret cleanup. This remains an
        # explicit idempotent seam for non-Kubernetes vaults and future moves.


@dataclass(slots=True)
class KubectlMinioTenantProvisioner:
    """Manage one MinIO user/policy restricted to derived instance buckets."""

    kubectl: KubectlClient
    vault: KubectlSecretVault
    namespace: str = "loom-dev"
    pod: str = "loom-dev-minio-0"
    container: str = "admin"

    @staticmethod
    def _names(identity: DevInstanceIdentity) -> tuple[str, str]:
        return f"loomdev-{identity.name}", f"loom-dev-{identity.name}"

    @staticmethod
    def _policy(identity: DevInstanceIdentity) -> dict[str, Any]:
        buckets = dev_buckets(identity)
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}" for bucket in buckets],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:AbortMultipartUpload",
                        "s3:DeleteObject",
                        "s3:GetObject",
                        "s3:ListMultipartUploadParts",
                        "s3:PutObject",
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket}/*" for bucket in buckets],
                },
            ],
        }

    async def converge(self, identity: DevInstanceIdentity) -> None:
        expected_access, policy_name = self._names(identity)
        access_key, secret_key = await self.vault.object_credentials(identity)
        if access_key != expected_access:
            raise DevInstanceRuntimeError("instance object credential identity is invalid")
        policy_payload = base64.b64encode(
            json.dumps(self._policy(identity), sort_keys=True, separators=(",", ":")).encode(),
        ).decode()
        quoted_access = shlex.quote(access_key)
        quoted_policy = shlex.quote(policy_name)
        script = "\n".join(
            (
                "IFS= read -r access_key",
                "IFS= read -r secret_key",
                f'test "$access_key" = {quoted_access}',
                'test -n "${MINIO_ROOT_USER:-}"',
                'test -n "${MINIO_ROOT_PASSWORD:-}"',
                "umask 077",
                "policy_file=$(mktemp)",
                "trap 'rm -f -- \"$policy_file\"' EXIT HUP INT TERM",
                f'printf %s {shlex.quote(policy_payload)} | base64 -d >"$policy_file"',
                'export MC_HOST_fixture="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"',
                f'mc admin policy detach fixture {quoted_policy} --user "$access_key" >/dev/null 2>&1 || true',
                'mc admin user rm fixture "$access_key" >/dev/null 2>&1 || true',
                f"mc admin policy rm fixture {quoted_policy} >/dev/null 2>&1 || true",
                f'mc admin policy create fixture {quoted_policy} "$policy_file" >/dev/null',
                'printf "%s\\n%s\\n" "$access_key" "$secret_key" | mc admin user add fixture >/dev/null',
                f'mc admin policy attach fixture {quoted_policy} --user "$access_key" >/dev/null',
            ),
        )
        await self.kubectl.exec_stdin(
            namespace=self.namespace,
            pod=self.pod,
            container=self.container,
            script=script,
            stdin=f"{access_key}\n{secret_key}\n",
        )

    async def delete(self, identity: DevInstanceIdentity) -> None:
        access_key, policy_name = self._names(identity)
        quoted_access = shlex.quote(access_key)
        quoted_policy = shlex.quote(policy_name)
        script = "\n".join(
            (
                'test -n "${MINIO_ROOT_USER:-}"',
                'test -n "${MINIO_ROOT_PASSWORD:-}"',
                'export MC_HOST_fixture="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"',
                f"mc admin policy detach fixture {quoted_policy} --user {quoted_access} >/dev/null 2>&1 || true",
                f"mc admin user rm fixture {quoted_access} >/dev/null 2>&1 || true",
                f"mc admin policy rm fixture {quoted_policy} >/dev/null 2>&1 || true",
            ),
        )
        await self.kubectl.exec_stdin(
            namespace=self.namespace,
            pod=self.pod,
            container=self.container,
            script=script,
        )


@dataclass(slots=True)
class KubectlClusterProvisioner:
    kubectl: KubectlClient
    base_manifest_config: DevInstanceManifestConfig

    async def deploy(
        self,
        identity: DevInstanceIdentity,
        *,
        deployment_generation: int,
        candidate_sha: str,
    ) -> None:
        if (
            deployment_generation != self.base_manifest_config.deployment_generation
            or candidate_sha != self.base_manifest_config.candidate_sha
        ):
            raise DevInstanceRuntimeError("deployment binding changed during provisioning")
        docs = dev_instance_manifest_documents(identity, self.base_manifest_config)
        migration = docs[1]
        migration_name = str(migration["metadata"]["name"])
        await self.kubectl.apply(yaml.safe_dump(migration, sort_keys=False))
        await self.kubectl.wait_job(identity.namespace, migration_name)
        await self.kubectl.apply(
            yaml.safe_dump_all(docs[2:], sort_keys=False, explicit_start=True),
        )
        await asyncio.gather(
            *(
                self.kubectl.wait_deployment(identity.namespace, name)
                for name in (
                    "loom-control-plane",
                    "loom-llm-gateway",
                    "loom-service",
                    "loom-web",
                )
            ),
        )

    async def destroy(self, identity: DevInstanceIdentity, *, keep_data: bool) -> None:
        del keep_data
        await self.kubectl.delete_namespace(identity.namespace)


@dataclass(slots=True)
class KubectlCandidateGenerationProvisioner:
    """Prepare one digest-pinned generation without mutating stable routes."""

    kubectl: KubectlClient

    async def prepare(
        self,
        identity: DevInstanceIdentity,
        config: DevInstanceManifestConfig,
    ) -> PersonalDevReadinessObservation:
        documents = personal_dev_preparation_manifest_documents(identity, config)
        namespace, migration, *runtime = documents
        await self.kubectl.apply(yaml.safe_dump(namespace, sort_keys=False))
        await self.kubectl.apply(yaml.safe_dump(migration, sort_keys=False))
        migration_name = str(migration["metadata"]["name"])
        await self.kubectl.wait_job(identity.namespace, migration_name)
        await self.kubectl.apply(
            yaml.safe_dump_all(runtime, sort_keys=False, explicit_start=True),
        )
        names = {
            component: f"loom-{component}-g{config.deployment_generation}"
            for component in ("control-plane", "llm-gateway", "service", "web")
        }
        await asyncio.gather(
            *(self.kubectl.wait_deployment(identity.namespace, name) for name in names.values()),
        )
        evidence: list[dict[str, object]] = []
        deployed_images: dict[str, str] = {}
        for component, name in names.items():
            resource = await self.kubectl.read_resource_json(
                namespace=identity.namespace,
                kind="deployment",
                name=name,
            )
            try:
                metadata = resource["metadata"]
                spec = resource["spec"]
                status = resource["status"]
                generation = int(metadata["generation"])
                containers = spec["template"]["spec"]["containers"]
                replicas = int(spec.get("replicas", 1))
                image = str(containers[0]["image"])
                observed_generation = int(status["observedGeneration"])
                available = int(status.get("availableReplicas", 0))
                updated = int(status.get("updatedReplicas", 0))
                uid = str(metadata["uid"])
                resource_version = str(metadata["resourceVersion"])
            except (KeyError, IndexError, TypeError, ValueError):
                raise DevInstanceRuntimeError(
                    "candidate deployment readiness response was invalid",
                ) from None
            expected_image = config.image(component)
            if (
                len(containers) != 1
                or image != expected_image
                or observed_generation < generation
                or available != replicas
                or updated != replicas
                or not uid
                or not resource_version
            ):
                raise DevInstanceRuntimeError(
                    "candidate deployment did not converge to the exact generation",
                )
            deployed_images[component] = image
            evidence.append(
                {
                    "component": component,
                    "generation": generation,
                    "name": name,
                    "resource_version": resource_version,
                    "uid": uid,
                },
            )
        canonical = json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return PersonalDevReadinessObservation(
            deployed_images=deployed_images,
            resource_evidence_sha256=hashlib.sha256(canonical).hexdigest(),
        )


@dataclass(slots=True)
class HttpControlPlanePolicyRegistrar:
    vault: KubectlSecretVault
    control_plane_url_template: str
    actuator_config: dict[str, Any]
    drain_timeout_seconds: int = 900
    poll_interval_seconds: float = 5.0
    transport: httpx.AsyncBaseTransport | None = None

    def _base_url(self, identity: DevInstanceIdentity) -> str:
        try:
            value = self.control_plane_url_template.format(
                namespace=identity.namespace,
                environment=identity.runtime_environment,
                name=identity.name,
            )
        except (KeyError, ValueError):
            raise DevInstanceRuntimeError("control-plane URL template is invalid") from None
        if not value.startswith(("http://", "https://")):
            raise DevInstanceRuntimeError("control-plane URL template must render HTTP(S)")
        return value.rstrip("/")

    def _payload(
        self,
        identity: DevInstanceIdentity,
        requested: RequestedPolicy,
        *,
        enabled: bool,
        max_slots: int,
    ) -> dict[str, Any]:
        actuator_config: dict[str, Any] = {}
        for key, value in self.actuator_config.items():
            if isinstance(value, str):
                try:
                    value = value.format(
                        namespace=identity.namespace,
                        environment=identity.runtime_environment,
                        name=identity.name,
                        pool=identity.worker_pool,
                    )
                except (KeyError, ValueError):
                    raise DevInstanceRuntimeError(
                        "Slurm actuator configuration template is invalid",
                    ) from None
            actuator_config[key] = value
        return {
            "actuator": "slurm",
            "enabled": enabled,
            "min_slots": 0 if not enabled else requested.min_slots,
            "max_slots": max_slots,
            "scale_up_threshold_slots": 1,
            "scale_down_idle_seconds": 60,
            "scale_up_cooldown_seconds": 30,
            "scale_down_cooldown_seconds": 60,
            "drain_timeout_seconds": self.drain_timeout_seconds,
            "force": False,
            "disabled_reason": None,
            "actuator_config": {**actuator_config, "external_runner": True},
        }

    async def upsert_dev_policy(
        self,
        identity: DevInstanceIdentity,
        requested: RequestedPolicy,
    ) -> None:
        token = await self.vault.admin_token(identity)
        url = (
            f"{self._base_url(identity)}/admin/worker-pool-autoscaler-policies/"
            f"{identity.runtime_environment}/{identity.worker_pool}"
        )
        async with httpx.AsyncClient(timeout=15.0, transport=self.transport) as client:
            response = await client.put(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json=self._payload(
                    identity,
                    requested,
                    enabled=True,
                    max_slots=requested.max_slots,
                ),
            )
        if response.status_code // 100 != 2:
            raise DevInstanceRuntimeError("instance autoscaler policy registration failed")

    async def drop_dev_policy(self, identity: DevInstanceIdentity) -> None:
        token = await self.vault.admin_token(identity)
        base = self._base_url(identity)
        policy_url = (
            f"{base}/admin/worker-pool-autoscaler-policies/"
            f"{identity.runtime_environment}/{identity.worker_pool}"
        )
        headers = {"Authorization": f"Bearer {token}"}
        requested = RequestedPolicy(actuator="slurm", min_slots=0, max_slots=0)
        async with httpx.AsyncClient(timeout=15.0, transport=self.transport) as client:
            response = await client.put(
                policy_url,
                headers=headers,
                json=self._payload(identity, requested, enabled=True, max_slots=0),
            )
            if response.status_code == 404:
                return
            if response.status_code // 100 != 2:
                raise DevInstanceRuntimeError("instance autoscaler drain request failed")
            deadline = asyncio.get_running_loop().time() + self.drain_timeout_seconds
            while True:
                status = await client.get(
                    f"{base}/admin/worker-pool-autoscalers/status",
                    params={
                        "environment": identity.runtime_environment,
                        "pool_name": identity.worker_pool,
                    },
                    headers=headers,
                )
                if status.status_code // 100 != 2:
                    raise DevInstanceRuntimeError("instance autoscaler drain status failed")
                raw = status.json()
                policies = raw.get("policies", []) if isinstance(raw, dict) else []
                if not isinstance(policies, list):
                    raise DevInstanceRuntimeError("instance autoscaler status was invalid")
                typed_policies = [policy for policy in policies if isinstance(policy, dict)]
                if len(typed_policies) != len(policies):
                    raise DevInstanceRuntimeError("instance autoscaler status was invalid")
                if not typed_policies or all(
                    int(policy.get(field) or 0) == 0
                    for policy in typed_policies
                    for field in (
                        "last_actual_slots",
                        "last_pending_slots",
                        "last_draining_slots",
                    )
                ):
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    raise DevInstanceRuntimeError("instance autoscaler drain timed out")
                await asyncio.sleep(self.poll_interval_seconds)
            revoked = await client.delete(
                f"{base}/admin/worker-tokens",
                headers=headers,
            )
            if revoked.status_code // 100 != 2:
                raise DevInstanceRuntimeError("instance worker credential cleanup failed")
            deleted = await client.delete(policy_url, headers=headers)
            if deleted.status_code not in {200, 204, 404}:
                raise DevInstanceRuntimeError("instance autoscaler policy cleanup failed")


__all__ = [
    "AsyncCommandRunner",
    "CommandResult",
    "DevInstanceRuntimeError",
    "HttpControlPlanePolicyRegistrar",
    "KubectlCandidateGenerationProvisioner",
    "KubectlClient",
    "KubectlClusterProvisioner",
    "KubectlMinioTenantProvisioner",
    "KubectlSecretVault",
    "PsycopgOwnerAccessBootstrap",
    "PsycopgSharedFixtureSqlExecutor",
    "S3BucketEnsurer",
    "fixture_database_url",
    "instance_database_url",
]
