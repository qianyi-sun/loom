"""Protected build-owner onboarding; run only from independently pinned authority.

This command retains installed runtime facts and additive recovery configuration.
It never activates V4, enables build intake, mutates a live service, or supplies
feature source with owner/management credentials.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import os
import re
import ssl
import sys
import tempfile
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Annotated

import httpx
from pydantic import Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.personal_dev_build_runtime_installation import resolve_personal_build_runtime_installation
from loom.personal_dev_build_runtime_publication import load_personal_build_runtime_publication
from loom.personal_dev_typed_membership_client import (
    CapacityManagerPersonalDevTypedMembershipClient,
    PersonalDevTypedMembershipEnvelopeV1,
)
from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    canonical_manager_origin,
    read_owner_only_bearer_token,
    read_owner_only_bytes,
)
from loom_capacity_agent.contracts import ReporterConfigurationV1
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationStore,
    build_guard_installation_document,
)
from loom_capacity_build_guard.scope_registry import (
    CREDENTIAL_KINDS,
    BuildScopeRegistry,
    reporter_file_name,
    scope_snapshot_name,
)
from loom_capacity_executor.admission_client import _database_url_from_bytes
from loom_capacity_executor.launch_policy_set import PoolLaunchPolicyV3
from loom_capacity_executor.launch_renderer import OperatorLaunchProfileV2
from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1
from loom_capacity_manager.contracts import Digest, FleetManifestV1, StrictV1Model, canonical_bytes
from loom_capacity_manager.typed_membership_commands import (
    PersonalBuildCommandV2,
    derive_build_member,
)
from loom_service.personal_dev_build_management import (
    BuildManagementFileV1,
    BuildManagementScopeV1,
    BuildManagementServiceConfigV1,
)


@dataclass(frozen=True, slots=True)
class _LoadedScopeClient:
    token: str = field(repr=False)
    tls: ssl.SSLContext = field(repr=False)
    payloads: tuple[bytes, ...] = field(repr=False)


class BuildScopeClientFilesV1(StrictV1Model):
    bearer_token: BuildManagementFileV1
    ca: BuildManagementFileV1
    certificate: BuildManagementFileV1
    private_key: BuildManagementFileV1

    def load(self) -> _LoadedScopeClient:
        """Construct transport from verified bytes, never later path rereads."""
        values = (self.bearer_token.read(), self.ca.read(), self.certificate.read(), self.private_key.read())
        with tempfile.TemporaryDirectory(prefix="loom-build-installer-client-") as directory:
            paths = tuple(Path(directory) / name for name in ("token", "ca", "certificate", "key"))
            for path, value in zip(paths, values, strict=True):
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(value)
            token = read_owner_only_bearer_token(paths[0])
            if not token.isascii() or len(token) > 16 * 1024 or any(not 0x21 <= ord(char) <= 0x7E for char in token):
                raise ValueError("installer bearer credential is invalid")
            tls = build_reporter_tls_context(DemandReporterTLSFiles(ca_file=paths[1], certificate_file=paths[2], private_key_file=paths[3]))
        return _LoadedScopeClient(token, tls, values)


class BuildScopePoolProfilesV1(StrictV1Model):
    policy: PoolLaunchPolicyV3
    profiles: Annotated[tuple[OperatorLaunchProfileV2, ...], Field(min_length=1, max_length=64)]


class BuildScopeInstallConfigV1(StrictV1Model):
    envelope: PersonalDevTypedMembershipEnvelopeV1
    preparation: ExecutionPreparationV4
    fleet: FleetManifestV1
    pools: Annotated[tuple[BuildScopePoolProfilesV1, ...], Field(min_length=2, max_length=2)]
    trusted_release: BuildManagementFileV1
    release_evidence: BuildManagementFileV1
    manager_origin: str
    management_credentials: BuildScopeClientFilesV1
    reporter_credentials: BuildScopeClientFilesV1
    reporter: ReporterConfigurationV1
    database_url: BuildManagementFileV1
    owner_role: str
    registry_directory: str
    expected_registry_sha256: Digest | None

    @field_validator("owner_role")
    @classmethod
    def _owner(cls, value: str) -> str:
        if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value) is None:
            raise ValueError("installer owner role must be canonical")
        return value

    @field_validator("registry_directory")
    @classmethod
    def _directory(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or str(path) != value or ".." in path.parts or value == "/" or "\0" in value:
            raise ValueError("installer registry directory must be canonical and absolute")
        return value

    @field_validator("manager_origin")
    @classmethod
    def _origin(cls, value: str) -> str:
        return canonical_manager_origin(value)


@dataclass(frozen=True, slots=True)
class PreparedBuildScopeInstallation:
    config: BuildScopeInstallConfigV1
    scope: BuildManagementScopeV1
    management_token: str = field(repr=False)
    management_tls: ssl.SSLContext = field(repr=False)
    database_url: str = field(repr=False)
    reporter_payloads: tuple[bytes, ...] = field(repr=False)


def prepare_build_scope_installation(config: BuildScopeInstallConfigV1) -> PreparedBuildScopeInstallation:
    config = BuildScopeInstallConfigV1.model_validate_json(canonical_bytes(config))
    request = config.envelope.request
    if not isinstance(request.command, PersonalBuildCommandV2) or request.command.projection.operation_kind not in {"create", "update"}:
        raise ValueError("build scope installation requires a typed create or update")
    member = derive_build_member(request, config.preparation, config.fleet)
    # The publication loader independently checks the release hash on its read.
    # The preflight read additionally enforces owner-only file permissions.
    config.trusted_release.read()
    publication = load_personal_build_runtime_publication(Path(config.trusted_release.path),
        expected_release_sha256=config.trusted_release.sha256, evidence_payload=config.release_evidence.read())
    runtime = resolve_personal_build_runtime_installation(publication, preparation=config.preparation,
        pool_profiles=tuple((pool.policy, pool.profiles) for pool in config.pools))
    document = build_guard_installation_document(member=member, runtime=runtime)
    files = config.reporter_credentials
    targets = {kind: BuildManagementFileV1(path=str(Path(config.registry_directory) /
        reporter_file_name(kind, getattr(files, kind).sha256)), sha256=getattr(files, kind).sha256)
        for kind in CREDENTIAL_KINDS}
    scope = BuildManagementScopeV1(installation=document, reporter=config.reporter, manager_origin=config.manager_origin,
        bearer_token=targets["bearer_token"], ca=targets["ca"], certificate=targets["certificate"], private_key=targets["private_key"])
    if (scope.reporter.configuration_generation != member.configuration.configuration_generation
        or scope.reporter.authority_incarnation != request.execution.authority_incarnation):
        raise ValueError("installer reporter authority or configuration generation differs")
    management = config.management_credentials.load()
    reporter = files.load()
    if (hmac.compare_digest(management.token, reporter.token)
        or sha256(reporter.token.encode("ascii")).hexdigest() != request.command.projection.demand_reporter_token_sha256):
        raise ValueError("installer requires the exact separate build reporter credential")
    database_url = _database_url_from_bytes(config.database_url.read())
    return PreparedBuildScopeInstallation(config, scope, management.token, management.tls, database_url, reporter.payloads)


async def install_build_scope(prepared: PreparedBuildScopeInstallation, *, sessions: async_sessionmaker[AsyncSession],
    manager: CapacityManagerPersonalDevTypedMembershipClient,
) -> BuildManagementServiceConfigV1:
    """Commit private owner retention before exposing a merged recovery config."""
    config, expected = prepared.config, prepared.scope.installation
    with BuildScopeRegistry(Path(config.registry_directory)) as registry:
        result = registry.propose(prepared.scope, expected_sha256=config.expected_registry_sha256)
        # Verify role/immutable deployment before a remote mutation. Do not hold
        # database locks over HTTP; the later retention rechecks exact evidence.
        async with asyncio.timeout(30), sessions.begin() as session:
            await session.execute(text(f"SET LOCAL ROLE {config.owner_role}"))
            previous = await BuildGuardInstallationStore(session, expected_owner_role=config.owner_role).read(expected.id)
            if previous is not None and previous.document != expected:
                raise ValueError("installer cannot replace retained deployment facts")
        membership = await manager.mutate_membership(config.envelope, preparation=config.preparation, fleet=config.fleet)
        if not isinstance(membership.member, PersonalBuildMemberV1):
            raise ValueError("installer received non-build membership")
        async with asyncio.timeout(30), sessions.begin() as session:
            await session.execute(text(f"SET LOCAL ROLE {config.owner_role}"))
            retained = await BuildGuardInstallationStore(session, expected_owner_role=config.owner_role).retain(
                member=membership.member, runtime=expected.runtime)
            if retained.document != expected:
                raise ValueError("installed build scope differs from protected preflight")
        for kind, payload in zip(CREDENTIAL_KINDS, prepared.reporter_payloads, strict=True):
            if registry.retain_reporter_file(kind, payload) != getattr(prepared.scope, kind):
                raise ValueError("reporter materialization differs from verified bytes")
        registry.publish(result)
        return result


def load_build_scope_installation(path: Path, *, expected_sha256: str) -> PreparedBuildScopeInstallation:
    wire = read_owner_only_bytes(path, max_bytes=1024 * 1024)
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None or not hmac.compare_digest(sha256(wire).hexdigest(), expected_sha256):
        raise ValueError("installer configuration digest differs from protected authority")
    config = BuildScopeInstallConfigV1.model_validate_json(wire)
    if canonical_bytes(config) != wire:
        raise ValueError("installer configuration is not canonical")
    return prepare_build_scope_installation(config)


async def _run(prepared: PreparedBuildScopeInstallation) -> BuildManagementServiceConfigV1:
    engine = create_async_engine(prepared.database_url, isolation_level="SERIALIZABLE",
        pool_size=1, max_overflow=0, pool_timeout=10, connect_args={"connect_timeout": 10})
    try:
        async with httpx.AsyncClient(verify=prepared.management_tls, timeout=httpx.Timeout(10),
            follow_redirects=False, trust_env=False) as http:
            manager = CapacityManagerPersonalDevTypedMembershipClient(manager_origin=prepared.config.manager_origin,
                bearer_token=prepared.management_token, http_client=http)
            return await install_build_scope(prepared, sessions=async_sessionmaker(engine, expire_on_commit=False), manager=manager)
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install a protected owner build recovery scope; does not activate builds.")
    parser.add_argument("--config-file", required=True, type=Path)
    parser.add_argument("--config-sha256", required=True)
    arguments = parser.parse_args(argv)
    try:
        prepared = load_build_scope_installation(arguments.config_file, expected_sha256=arguments.config_sha256)
        result = asyncio.run(_run(prepared))
    except Exception:
        print("build scope installation unconfirmed; retain the exact request and inspect protected authority", file=sys.stderr)
        return 1
    print(json.dumps({"management_config_file": str(Path(prepared.config.registry_directory) / scope_snapshot_name(result)),
        "management_config_sha256": sha256(canonical_bytes(result)).hexdigest(), "mode": result.mode}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
