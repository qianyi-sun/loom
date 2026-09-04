from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest
from sqlalchemy.engine import make_url

from loom.personal_dev_capacity_runtime import (
    CapacityDatabaseCredentials,
    CapacityDatabaseInstallation,
)
from loom.staging_capacity_database_bootstrap import (
    StagingCapacityDatabaseBootstrapSettings,
    bootstrap_staging_capacity_database,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    ReporterConfigurationV1,
)


def _seed() -> dict[str, object]:
    return {
        "agent_database_password": "a" * 48,
        "agent_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-agent:v1")),
        "authority_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-authority:v1")),
        "migrator_database_password": "m" * 48,
        "observer_database_password": "o" * 48,
        "reporter_incarnation": "0d598e5b-0acd-4d37-8d6f-227e1a4f7e32",
        "reporter_token": "t" * 48,
        "runtime_database_password": "r" * 48,
        "schema_version": 1,
        "subject_id": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")),
        "subject_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1")),
    }


def _configuration(seed: dict[str, object]) -> ReporterConfigurationV1:
    return ReporterConfigurationV1(
        environment_id="staging",
        subject_id=UUID(str(seed["subject_id"])),
        subject_incarnation=UUID(str(seed["subject_incarnation"])),
        authority_incarnation=UUID(str(seed["authority_incarnation"])),
        agent_incarnation=UUID(str(seed["agent_incarnation"])),
        reporter_incarnation=UUID(str(seed["reporter_incarnation"])),
        candidate_digest="1" * 64,
        candidate_identity_algorithm="git-sha1",
        candidate_identity="2" * 40,
        candidate_publication_sha256="3" * 64,
        deployment_generation=7,
        configuration_generation=7,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


def _write_inputs(root: Path) -> tuple[StagingCapacityDatabaseBootstrapSettings, dict[str, object]]:
    seed = _seed()
    configuration = _configuration(seed)
    values: dict[str, bytes] = {
        "seed.json": json.dumps(seed).encode("ascii"),
        "reporter-configuration.json": configuration.model_dump_json().encode("ascii"),
        "username": b"postgres",
        "password": b"admin-password",
        "ca.crt": b"test-ca",
    }
    for name, payload in values.items():
        (root / name).write_bytes(payload)
    return (
        StagingCapacityDatabaseBootstrapSettings(
            credential_seed_path=root / "seed.json",
            reporter_configuration_path=root / "reporter-configuration.json",
            admin_username_path=root / "username",
            admin_password_path=root / "password",
            database_ca_path=root / "ca.crt",
        ),
        seed,
    )


@pytest.mark.asyncio
async def test_bootstrap_uses_fixed_staging_identity_and_existing_database_installer(
    tmp_path: Path,
) -> None:
    settings, seed = _write_inputs(tmp_path)
    observed: dict[str, Any] = {}

    class Database:
        def __init__(self, admin_url: str) -> None:
            observed["admin_url"] = admin_url

        async def converge_protected(
            self,
            *,
            identity: object,
            credentials: CapacityDatabaseCredentials,
            configuration: ReporterConfigurationV1,
        ) -> CapacityDatabaseInstallation:
            observed.update(
                identity=identity,
                credentials=credentials,
                configuration=configuration,
            )
            return CapacityDatabaseInstallation(
                protected_admission_sha256="4" * 64,
                agent_database_url="redacted-agent-url",
                runtime_database_url="redacted-runtime-url",
            )

    installation = await bootstrap_staging_capacity_database(
        settings,
        database_factory=Database,
    )

    parsed_url = make_url(str(observed["admin_url"]))
    assert parsed_url.drivername == "postgresql+psycopg"
    assert parsed_url.username == "postgres"
    assert parsed_url.password == "admin-password"
    assert parsed_url.host == "loom-postgres-rw.loom-staging.svc.cluster.local"
    assert parsed_url.port == 5432
    assert parsed_url.database == "loom"
    assert parsed_url.query == {
        "sslmode": "verify-full",
        "sslrootcert": str(tmp_path / "ca.crt"),
    }
    identity = observed["identity"]
    assert identity.name == "staging"
    assert identity.runtime_environment == "staging"
    assert identity.namespace == "loom-staging"
    assert identity.database == "loom"
    assert identity.db_role == "loom"
    assert observed["credentials"] == CapacityDatabaseCredentials(
        reporter_incarnation=UUID(str(seed["reporter_incarnation"])),
        reporter_token="t" * 48,
        migrator_password="m" * 48,
        agent_password="a" * 48,
        observer_password="o" * 48,
        runtime_password="r" * 48,
    )
    assert observed["configuration"] == _configuration(seed)
    assert installation.protected_admission_sha256 == "4" * 64


@pytest.mark.asyncio
async def test_bootstrap_rejects_seed_and_configuration_identity_mismatch(
    tmp_path: Path,
) -> None:
    settings, seed = _write_inputs(tmp_path)
    seed["reporter_incarnation"] = "3a31e7ef-a8b0-4ed1-9135-03f58fd848ca"
    settings.credential_seed_path.write_text(json.dumps(seed), encoding="ascii")
    factory_called = False

    class Database:
        def __init__(self, _admin_url: str) -> None:
            nonlocal factory_called
            factory_called = True

    with pytest.raises(ValueError, match="staging capacity bootstrap identity mismatch"):
        await bootstrap_staging_capacity_database(settings, database_factory=Database)

    assert factory_called is False
