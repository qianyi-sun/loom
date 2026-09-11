"""Pinned, least-privileged management connection for native admission."""

from __future__ import annotations

import asyncio
import hmac
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Literal

from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom_capacity_agent.client import read_owner_only_bytes
from loom_capacity_executor.admission_client import _database_url_from_bytes
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes
from loom_service.config import LoomServiceSettings


class BuildAdmissionServiceConfigV1(StrictV1Model):
    mode: Literal["prepare-bind-only", "native-registration", "native-claims"]
    database_url_file: str
    database_url_sha256: Digest
    principals_file: str
    principals_sha256: Digest

    @field_validator("database_url_file", "principals_file")
    @classmethod
    def _canonical_path(cls, value: str) -> str:
        path = Path(value)
        if (
            not path.is_absolute()
            or value != str(path)
            or value == "/"
            or ".." in path.parts
            or "\0" in value
            or path.is_symlink()
        ):
            raise ValueError("build admission input path must be canonical and absolute")
        return value


@dataclass(frozen=True, slots=True)
class PersonalBuildAdmissionRuntime:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    verifier: CapacityPrincipalVerifier
    mode: Literal["prepare-bind-only", "native-registration", "native-claims"]

    async def aclose(self) -> None:
        await self.engine.dispose()


async def _assert_private_agent(connection: AsyncConnection, *, registration_enabled: bool = False,
    claims_enabled: bool = False,
) -> None:
    safe = await connection.scalar(
        text("""
        SELECT rolcanlogin AND NOT (rolinherit OR rolsuper OR rolcreatedb OR rolcreaterole
            OR rolreplication OR rolbypassrls)
            AND current_user=session_user
            AND NOT EXISTS (SELECT 1 FROM pg_auth_members WHERE member=r.oid)
        FROM pg_roles r WHERE rolname=current_user
    """)
    )
    if safe is not True:
        raise RuntimeError("build admission requires a private least-privileged agent")
    namespace = await connection.scalar(text("SELECT to_regnamespace('loom_capacity_build_guard')"))
    if namespace is None:
        raise RuntimeError("build admission private guard is absent")
    safe_owner = await connection.scalar(
        text("""
        SELECT NOT (r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb
            OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
            AND NOT EXISTS (SELECT 1 FROM pg_auth_members WHERE member=r.oid)
        FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner
        WHERE n.nspname='loom_capacity_build_guard'
    """)
    )
    if safe_owner is not True:
        raise RuntimeError("build admission private owner privilege changed")
    safe_schema = await connection.scalar(
        text("""
        SELECT has_schema_privilege(current_user,'loom_capacity_build_guard','USAGE')
            AND NOT has_schema_privilege(current_user,'loom_capacity_build_guard','CREATE')
            AND NOT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname IN ('public','loom_capacity_build_guard') AND (
                    (c.relkind IN ('r','p','v','m','f') AND (
                        has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
                        OR has_any_column_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,REFERENCES')))
                    OR (c.relkind='S' AND has_sequence_privilege(current_user,c.oid,'USAGE,SELECT,UPDATE'))))
    """)
    )
    if safe_schema is not True:
        raise RuntimeError("build admission agent has direct data privileges")
    signatures: tuple[str, ...] = (
        "prepare_worker(uuid,jsonb,bytea,text,text)",
        "bind_slurm_job(uuid,jsonb,bytea,text)",
        "observe_intent(uuid,jsonb,bytea,text)",
        "revoke_prepared_bootstrap(uuid,jsonb,bytea,text)",
        "withdraw_unregistered_worker(uuid,jsonb,bytea,text)",
    )
    if registration_enabled:
        signatures += ("register_worker(uuid,jsonb,bytea,text,text)", "begin_drain(uuid,jsonb,bytea,text)",
            "acknowledge_release(uuid,jsonb,bytea,text,text)")
    if claims_enabled:
        signatures += ("claim_platform(uuid,jsonb,bytea,text,text)", "record_outcome(uuid,jsonb,bytea,text,text)")
    for signature in signatures:
        callable_safe = await connection.scalar(
            text("""
            SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE p.oid=to_regprocedure(:signature) AND p.proowner=n.nspowner
                    AND p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
                    AND EXISTS (SELECT 1 FROM aclexplode(p.proacl) a
                        WHERE a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                            AND a.privilege_type='EXECUTE' AND NOT a.is_grantable)
                    AND NOT EXISTS (SELECT 1 FROM aclexplode(p.proacl) a
                        WHERE a.grantee NOT IN (p.proowner,
                            (SELECT oid FROM pg_roles WHERE rolname=current_user))))
        """),
            {"signature": f"loom_capacity_build_guard.{signature}"},
        )
        if callable_safe is not True:
            raise RuntimeError("build admission protected procedure privilege changed")


async def build_personal_build_admission_runtime(
    settings: LoomServiceSettings,
) -> PersonalBuildAdmissionRuntime | None:
    path = settings.personal_dev_build_admission_config_file
    expected = settings.personal_dev_build_admission_config_sha256
    if path is None and not expected:
        return None
    if path is None or not expected:
        raise RuntimeError("build admission configuration requires path and digest")
    wire = read_owner_only_bytes(path, max_bytes=64 * 1024)
    if not hmac.compare_digest(sha256(wire).hexdigest(), expected):
        raise RuntimeError("build admission configuration digest changed")
    config = BuildAdmissionServiceConfigV1.model_validate_json(wire)
    if canonical_bytes(config) != wire:
        raise RuntimeError("build admission configuration must be canonical")
    database_wire = read_owner_only_bytes(Path(config.database_url_file), max_bytes=16 * 1024)
    if not hmac.compare_digest(sha256(database_wire).hexdigest(), config.database_url_sha256):
        raise RuntimeError("build admission private database input changed")
    database_url = _database_url_from_bytes(database_wire)
    verifier = CapacityPrincipalVerifier.from_pool_executor_file(
        Path(config.principals_file), expected_sha256=config.principals_sha256
    )
    engine = create_async_engine(
        database_url,
        isolation_level="SERIALIZABLE",
        pool_size=2,
        max_overflow=0,
        pool_timeout=5,
        connect_args={"connect_timeout": 5},
        echo=False,
    )
    try:
        async with asyncio.timeout(30), engine.connect() as connection:
            await connection.execute(text("SET LOCAL statement_timeout='10000ms'"))
            await connection.execute(text("SET LOCAL lock_timeout='5000ms'"))
            await _assert_private_agent(connection, registration_enabled=config.mode != "prepare-bind-only",
                claims_enabled=config.mode == "native-claims")
    except BaseException:
        await engine.dispose()
        raise
    return PersonalBuildAdmissionRuntime(
        engine, async_sessionmaker(engine, expire_on_commit=False), verifier, config.mode
    )
