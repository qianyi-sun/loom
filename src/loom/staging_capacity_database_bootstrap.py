"""One-shot protected staging capacity database convergence."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import ValidationError
from sqlalchemy import URL

from loom.dev_instance import DevInstanceIdentity
from loom.personal_dev_capacity_runtime import (
    CapacityDatabaseCredentials,
    CapacityDatabaseInstallation,
    PsycopgPersonalDevCapacityDatabase,
)
from loom_capacity_agent.contracts import ReporterConfigurationV1

_SUBJECT_ID = uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")
_SUBJECT_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1")
_AUTHORITY_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-authority:v1")
_AGENT_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-agent:v1")
_SEED_FIELDS = frozenset(
    {
        "agent_database_password",
        "agent_incarnation",
        "authority_incarnation",
        "migrator_database_password",
        "observer_database_password",
        "reporter_incarnation",
        "reporter_token",
        "runtime_database_password",
        "schema_version",
        "subject_id",
        "subject_incarnation",
    }
)
_EXPECTED_CAPABILITIES = {
    ("oldlab-x86-none", "oldlab", "linux", "x86_64", "none", ("public",)),
    ("gb10-arm-none", "gb10", "linux", "arm64", "none", ("public",)),
}
_MAX_JSON_BYTES = 1024 * 1024
_MAX_CREDENTIAL_BYTES = 4096


class ProtectedStagingDatabase(Protocol):
    async def converge_protected(
        self,
        *,
        identity: DevInstanceIdentity,
        credentials: CapacityDatabaseCredentials,
        configuration: ReporterConfigurationV1,
    ) -> CapacityDatabaseInstallation: ...


@dataclass(frozen=True, slots=True)
class StagingCapacityDatabaseBootstrapSettings:
    credential_seed_path: Path = Path("/run/loom-staging-capacity-bootstrap/seed.json")
    reporter_configuration_path: Path = Path(
        "/run/loom-staging-capacity-bootstrap/reporter-configuration.json"
    )
    admin_username_path: Path = Path("/run/loom-postgres-admin/username")
    admin_password_path: Path = Path("/run/loom-postgres-admin/password")
    database_ca_path: Path = Path("/run/loom-postgres-ca/ca.crt")
    database_host: str = "loom-postgres-rw.loom-staging.svc.cluster.local"
    database_port: int = 5432
    database_name: str = "loom"

    def __post_init__(self) -> None:
        paths = (
            self.credential_seed_path,
            self.reporter_configuration_path,
            self.admin_username_path,
            self.admin_password_path,
            self.database_ca_path,
        )
        if (
            any(not path.is_absolute() or ".." in path.parts for path in paths)
            or self.database_host != "loom-postgres-rw.loom-staging.svc.cluster.local"
            or self.database_port != 5432
            or self.database_name != "loom"
        ):
            raise ValueError("staging capacity database bootstrap authority is invalid")


def _read_bounded(path: Path, *, max_bytes: int) -> bytes:
    payload = path.read_bytes()
    if not payload or len(payload) > max_bytes or b"\x00" in payload:
        raise ValueError("staging capacity bootstrap input is invalid")
    return payload


def _read_text_credential(path: Path) -> str:
    try:
        value = _read_bounded(path, max_bytes=_MAX_CREDENTIAL_BYTES).decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("staging capacity database credential is invalid") from None
    if value != value.strip() or any(character in value for character in ("\r", "\n")):
        raise ValueError("staging capacity database credential is invalid")
    return value


def _parse_seed(payload: bytes) -> CapacityDatabaseCredentials:
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("staging capacity credential seed is invalid") from None
    if not isinstance(raw, dict) or set(raw) != _SEED_FIELDS or raw["schema_version"] != 1:
        raise ValueError("staging capacity credential seed is invalid")
    try:
        subject_id = UUID(raw["subject_id"])
        subject_incarnation = UUID(raw["subject_incarnation"])
        authority_incarnation = UUID(raw["authority_incarnation"])
        agent_incarnation = UUID(raw["agent_incarnation"])
        reporter_incarnation = UUID(raw["reporter_incarnation"])
    except (AttributeError, TypeError, ValueError):
        raise ValueError("staging capacity credential seed identity is invalid") from None
    incarnations = {
        subject_incarnation,
        authority_incarnation,
        agent_incarnation,
        reporter_incarnation,
    }
    if (
        subject_id != _SUBJECT_ID
        or subject_incarnation != _SUBJECT_INCARNATION
        or authority_incarnation != _AUTHORITY_INCARNATION
        or agent_incarnation != _AGENT_INCARNATION
        or len(incarnations) != 4
    ):
        raise ValueError("staging capacity credential seed identity is invalid")

    values: dict[str, str] = {}
    for field in (
        "agent_database_password",
        "migrator_database_password",
        "observer_database_password",
        "reporter_token",
        "runtime_database_password",
    ):
        value = raw[field]
        try:
            encoded = value.encode("ascii")
        except (AttributeError, UnicodeEncodeError):
            raise ValueError("staging capacity credential seed is invalid") from None
        if not 32 <= len(encoded) <= 1024 or any(not 0x21 <= byte <= 0x7E for byte in encoded):
            raise ValueError("staging capacity credential seed is invalid")
        values[field] = value
    return CapacityDatabaseCredentials(
        reporter_incarnation=reporter_incarnation,
        reporter_token=values["reporter_token"],
        migrator_password=values["migrator_database_password"],
        agent_password=values["agent_database_password"],
        observer_password=values["observer_database_password"],
        runtime_password=values["runtime_database_password"],
    )


def _parse_configuration(
    payload: bytes,
    *,
    credentials: CapacityDatabaseCredentials,
) -> ReporterConfigurationV1:
    try:
        configuration = ReporterConfigurationV1.model_validate_json(payload)
    except ValidationError:
        raise ValueError("staging capacity reporter configuration is invalid") from None
    capabilities = {
        (
            item.capability_id,
            item.pool_id,
            item.operating_system,
            item.cpu_architecture,
            item.gpu_vendor,
            item.network_policies,
        )
        for item in configuration.pool_capabilities
    }
    if (
        configuration.environment_id != "staging"
        or configuration.subject_id != _SUBJECT_ID
        or configuration.subject_incarnation != _SUBJECT_INCARNATION
        or configuration.authority_incarnation != _AUTHORITY_INCARNATION
        or configuration.agent_incarnation != _AGENT_INCARNATION
        or configuration.reporter_incarnation != credentials.reporter_incarnation
        or configuration.authority_mode != "disabled"
        or configuration.allocation_epoch != 0
        or configuration.reporter_high_water != 0
        or configuration.protected_admission_sha256 is not None
        or configuration.deployment_generation != configuration.configuration_generation
        or capabilities != _EXPECTED_CAPABILITIES
    ):
        raise ValueError("staging capacity bootstrap identity mismatch")
    return configuration


def staging_capacity_identity() -> DevInstanceIdentity:
    return DevInstanceIdentity(
        name="staging",
        runtime_environment="staging",
        namespace="loom-staging",
        database="loom",
        db_role="loom",
        task_bucket="loom-staging-tasks",
        trajectories_bucket="loom-staging-trajectories",
        artifacts_bucket="loom-staging-artifacts",
        route_host="staging.yylx.world",
        worker_control_plane_host="cp.staging.yylx.world",
        worker_gateway_host="gw.staging.yylx.world",
        route_path="/staging",
        worker_pool="staging",
        provider_connection_namespace="staging",
    )


async def bootstrap_staging_capacity_database(
    settings: StagingCapacityDatabaseBootstrapSettings,
    *,
    database_factory: Callable[[str], ProtectedStagingDatabase] = (
        PsycopgPersonalDevCapacityDatabase
    ),
) -> CapacityDatabaseInstallation:
    credentials = _parse_seed(
        _read_bounded(settings.credential_seed_path, max_bytes=_MAX_JSON_BYTES)
    )
    configuration = _parse_configuration(
        _read_bounded(settings.reporter_configuration_path, max_bytes=_MAX_JSON_BYTES),
        credentials=credentials,
    )
    username = _read_text_credential(settings.admin_username_path)
    password = _read_text_credential(settings.admin_password_path)
    _read_bounded(settings.database_ca_path, max_bytes=_MAX_CREDENTIAL_BYTES)
    admin_url = URL.create(
        "postgresql+psycopg",
        username=username,
        password=password,
        host=settings.database_host,
        port=settings.database_port,
        database=settings.database_name,
        query={
            "sslmode": "verify-full",
            "sslrootcert": str(settings.database_ca_path),
        },
    ).render_as_string(hide_password=False)
    return await database_factory(admin_url).converge_protected(
        identity=staging_capacity_identity(),
        credentials=credentials,
        configuration=configuration,
    )


def main() -> None:
    asyncio.run(bootstrap_staging_capacity_database(StagingCapacityDatabaseBootstrapSettings()))


if __name__ == "__main__":
    main()


__all__ = [
    "StagingCapacityDatabaseBootstrapSettings",
    "bootstrap_staging_capacity_database",
    "staging_capacity_identity",
]
