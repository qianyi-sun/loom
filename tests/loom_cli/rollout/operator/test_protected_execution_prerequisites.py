from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    canonical_execution_prerequisite_bytes,
    parse_execution_prerequisite_bytes,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact as _artifact,
)


def test_prerequisite_artifact_round_trips_and_realizes_only_prepared_profile() -> None:
    artifact = _artifact()

    payload = canonical_execution_prerequisite_bytes(artifact)
    parsed = parse_execution_prerequisite_bytes(payload)
    prepared = ExecutionContextV2(
        authority_incarnation=UUID(artifact.executor_profile_seed.authority_incarnation),
        writer_epoch=12,
        configuration_epoch=10,
        execution_epoch=3,
        execution_manifest_sha256="1" * 64,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256=(artifact.executor_profile_seed.trusted_fleet_release_sha256),
    )

    realized = parsed.executor_profile_seed.realize(prepared)

    assert parsed == artifact
    assert payload.endswith(b"\n")
    assert realized.writer_epoch == 12
    assert realized.configuration_epoch == 10
    assert realized.execution_epoch == 3
    assert realized.execution_manifest_sha256 == "1" * 64
    assert realized.executable_new_capacity_ceiling == 0
    assert tuple(pool.pool_id for pool in realized.pools) == ("gb10", "oldlab")


def test_executor_profile_seed_excludes_future_epochs_and_rejects_drift() -> None:
    seed = _artifact().executor_profile_seed
    prepared = ExecutionContextV2(
        authority_incarnation=UUID(seed.authority_incarnation),
        writer_epoch=12,
        configuration_epoch=10,
        execution_epoch=3,
        execution_manifest_sha256="1" * 64,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256=seed.trusted_fleet_release_sha256,
    )

    assert (
        not {
            "writer_epoch",
            "configuration_epoch",
            "execution_epoch",
            "execution_manifest_sha256",
        }
        & seed.to_dict().keys()
    )
    for drifted in (
        prepared.model_copy(update={"execution_state": "active"}),
        prepared.model_copy(update={"executable_new_capacity_rate_per_minute": 1}),
        prepared.model_copy(update={"trusted_fleet_release_sha256": "0" * 64}),
    ):
        with pytest.raises(ValueError, match="differs from executor profile seed"):
            seed.realize(drifted)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda artifact: replace(artifact, rollback_evidence_sha256="0" * 64),
            "rollback",
        ),
        (
            lambda artifact: replace(
                artifact,
                manager_client_cidrs={
                    **artifact.manager_client_cidrs,
                    "gb10": "192.168.60.0/24",
                },
            ),
            "route",
        ),
        (
            lambda artifact: replace(
                artifact,
                credential_metadata_sha256={
                    name: value
                    for name, value in artifact.credential_metadata_sha256.items()
                    if name != "manager-abort"
                },
            ),
            "credential",
        ),
    ],
)
def test_prerequisite_artifact_rejects_authority_drift(change, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        change(_artifact())


def test_prerequisite_routes_are_limited_to_renderer_rfc1918_networks() -> None:
    artifact = _artifact()

    with pytest.raises(ValueError, match="route"):
        replace(
            artifact,
            manager_client_cidrs={
                **artifact.manager_client_cidrs,
                "operator": "127.0.0.1/32",
            },
        )


def test_prerequisite_rejects_cross_pool_identity_overlap() -> None:
    artifact = _artifact()
    gb10, oldlab = artifact.executor_profile_seed.pools

    with pytest.raises(ValueError, match="profile seed is invalid"):
        replace(
            artifact.executor_profile_seed,
            pools=(
                gb10,
                oldlab.model_copy(update={"executor_incarnation": gb10.executor_incarnation}),
            ),
        )


@pytest.mark.parametrize(
    "change",
    [
        lambda artifact: replace(
            artifact,
            execution_policy=artifact.execution_policy.model_copy(
                update={"trusted_fleet_release_sha256": "0" * 64}
            ),
        ),
        lambda artifact: replace(
            artifact,
            legacy_writer_evidence_sha256={
                **artifact.legacy_writer_evidence_sha256,
                "global/development/allocation/global-dev-supervisor": "0" * 64,
            },
        ),
    ],
)
def test_prerequisite_rejects_policy_profile_or_fence_drift(change) -> None:
    with pytest.raises(ValueError, match="policy authority drifted"):
        change(_artifact())


def test_prerequisite_parser_rejects_duplicate_and_unknown_fields() -> None:
    payload = canonical_execution_prerequisite_bytes(_artifact())

    with pytest.raises(ValueError, match="duplicate"):
        parse_execution_prerequisite_bytes(
            payload.replace(
                b'{"backup_lease_sha256":', b'{"schema_version":1,"backup_lease_sha256":', 1
            )
        )
    with pytest.raises(ValueError, match="invalid"):
        parse_execution_prerequisite_bytes(payload[:-2] + b',"unexpected":"value"}\n')

    with pytest.raises(ValueError, match="invalid"):
        parse_execution_prerequisite_bytes(payload[:-2] + b',"bearer_token":"not-secret-safe"}\n')
