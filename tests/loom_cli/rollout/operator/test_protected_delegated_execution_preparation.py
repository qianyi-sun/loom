from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationPolicyV3,
    ExecutionPreparationV3,
    PersonalMembershipPolicyV1,
)
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    _ManagerExecutionStatus,
    _preparation_request,
)
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClient,
)
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    ProtectedExecutionPrerequisiteArtifact,
    canonical_execution_prerequisite_bytes,
    parse_execution_prerequisite_bytes,
)
from loom_cli.rollout.operator.protected_staging_capacity_manager_policy_component import (
    _validate_manager_status,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_protected_capacity_manager_client import (
    _credentials,
    _execution_preparation,
    _HTTPClient,
    _prepared_execution,
    _Response,
)


def _membership() -> PersonalMembershipPolicyV1:
    return PersonalMembershipPolicyV1(
        namespace_id=UUID(int=1871),
        management_principal_id="personal-membership-manager",
        development_template_sha256="d" * 64,
        max_subjects=16,
    )


def _artifact(version: int) -> ProtectedExecutionPrerequisiteArtifact:
    artifact = execution_prerequisite_artifact()
    if version == 2:
        return artifact
    policy = ExecutionPreparationPolicyV3(
        **artifact.execution_policy.model_dump(exclude={"schema_version"}),
        personal_membership=_membership(),
    )
    return replace(artifact, execution_policy=policy)


def _request(version: int) -> ExecutionPreparationV2:
    request = _execution_preparation()
    if version == 2:
        return request
    return ExecutionPreparationV3(
        **request.model_dump(exclude={"schema_version"}),
        personal_membership=_membership(),
    )


@pytest.mark.parametrize("version", [2, 3])
def test_prerequisite_round_trip_preserves_exact_versioned_policy(version: int) -> None:
    artifact = _artifact(version)
    payload = canonical_execution_prerequisite_bytes(artifact)
    parsed = parse_execution_prerequisite_bytes(payload)

    assert parsed == artifact
    assert type(parsed.execution_policy) is type(artifact.execution_policy)
    assert canonical_execution_prerequisite_bytes(parsed) == payload
    assert parsed.execution_policy_sha256 == canonical_executable_digest(artifact.execution_policy)


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize("surface", ["request", "readback"])
def test_preparation_and_prepared_readback_preserve_membership(version: int, surface: str) -> None:
    artifact = _artifact(version)
    expected = _request(version)
    status = _ManagerExecutionStatus(
        authority_incarnation=expected.authority_incarnation,
        writer_epoch=expected.expected_writer_epoch,
        configuration_epoch=expected.configuration_epoch,
        configuration_digest="c" * 64,
        execution_epoch=1,
        execution_state="prepared",
        execution_manifest_sha256=canonical_executable_digest(expected),
        executable_new_capacity_ceiling=0,
        increase_freeze=True,
    )

    if surface == "request":
        request = _preparation_request(status, artifact=artifact)
        assert request == expected
        assert type(request) is type(expected)
        return
    observed = {
        "schema_version": 1,
        "authority_incarnation": str(status.authority_incarnation),
        "observer_principal_id": "manager-read",
        "writer_epoch": status.writer_epoch,
        "configuration_epoch": status.configuration_epoch,
        "configuration_digest": status.configuration_digest,
        "execution_epoch": status.execution_epoch,
        "execution_state": status.execution_state,
        "execution_manifest_sha256": status.execution_manifest_sha256,
        "executable_new_capacity_ceiling": 0,
        "increase_freeze": True,
        "latest_shadow_epoch": None,
        "latest_shadow_input_digest": None,
        "report_freshness_counts": {},
        "account_slots": {},
        "tier_slots": {},
        "pool_slots": {},
        "blocker_counts": {},
    }
    _validate_manager_status(
        observed, authority_incarnation=status.authority_incarnation, prerequisite=artifact
    )
    if version == 3:
        changed = replace(
            artifact,
            execution_policy=artifact.execution_policy.model_copy(
                update={
                    "personal_membership": _membership().model_copy(update={"max_subjects": 17})
                }
            ),
        )
        assert changed.artifact_sha256 != artifact.artifact_sha256
        with pytest.raises(ValueError, match="manifest"):
            _validate_manager_status(
                observed, authority_incarnation=status.authority_incarnation, prerequisite=changed
            )
        with pytest.raises(ValueError, match="manifest"):
            _preparation_request(status, artifact=changed)


@pytest.mark.parametrize("version", [2, 3])
def test_client_sends_exact_versioned_preparation_to_matching_endpoint(
    tmp_path: Path, version: int
) -> None:
    request = _request(version)
    prepared = _prepared_execution().model_copy(
        update={"execution_manifest_sha256": canonical_executable_digest(request)}
    )
    http = _HTTPClient([_Response(prepared.model_dump_json().encode("ascii"))])
    client = ProtectedCapacityManagerClient(
        origin="https://127.0.0.1:43210",
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        client_factory=lambda _context: http,
    )
    key = UUID(int=1872)

    assert client.prepare_execution(request, key) == prepared
    assert len(http.calls) == 1
    call = http.calls[0]
    assert call["url"] == f"https://127.0.0.1:43210/v{version}/execution-preparations"
    assert json.loads(call["content"]) == request.model_dump(mode="json", exclude_none=False)
    assert call["headers"]["Idempotency-Key"] == str(key)


@pytest.mark.parametrize("version", [1, 2, 4, "3", 3.0, True])
def test_client_rejects_invalid_preparation_version_before_transport(
    tmp_path: Path, version: object
) -> None:
    request = _request(3).model_copy(update={"schema_version": version})
    http = _HTTPClient([])
    client = ProtectedCapacityManagerClient(
        origin="https://127.0.0.1:43210",
        credentials_root=tmp_path / "unavailable-credentials",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        client_factory=lambda _context: http,
    )

    with pytest.raises(
        (TypeError, ValueError), match=r"preparation|schema|tag|personal_membership"
    ):
        client.prepare_execution(request, UUID(int=1872))
    assert http.calls == []


@pytest.mark.parametrize("version", [1, 2, 4, "3", 3.0, True])
def test_artifact_rejects_unknown_or_downgraded_membership_policy(version: object) -> None:
    value = _artifact(3).to_dict()
    value["execution_policy"]["schema_version"] = version

    with pytest.raises(ValueError, match="artifact"):
        parse_execution_prerequisite_bytes(json.dumps(value).encode("ascii"))
