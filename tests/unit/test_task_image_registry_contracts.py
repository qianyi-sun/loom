from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_task_image_authority.contracts import (
    TaskImageRegistryCredentialV1,
)
from loom_task_image_authority.http_contracts import (
    TaskImagePublicationCandidateResponseV1,
)

NOW = datetime(2026, 9, 4, 15, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-4111-8111-111111111111")
SESSION_ID = UUID("22222222-2222-4222-8222-222222222222")
REQUEST_ID = UUID("33333333-3333-4333-8333-333333333333")
MATERIALIZATION_ID = UUID("44444444-4444-4444-8444-444444444444")
ATTEMPT_ID = UUID("55555555-5555-4555-8555-555555555555")
CREDENTIAL_ID = UUID("66666666-6666-4666-8666-666666666666")
HEARTBEAT_ID = UUID("77777777-7777-4777-8777-777777777777")
OPERATION_ID = UUID("88888888-8888-4888-8888-888888888888")
CANDIDATE_ID = UUID("99999999-9999-4999-8999-999999999999")
SESSION_TOKEN = "loom_tibs_" + "S" * 64
BEARER_TOKEN = "header.payload.signature"
REPOSITORY = f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/task"


def _credential(**changes: object) -> TaskImageRegistryCredentialV1:
    values: dict[str, object] = {
        "credential_id": CREDENTIAL_ID,
        "request_id": REQUEST_ID,
        "grant_id": GRANT_ID,
        "session_id": SESSION_ID,
        "session_generation": 2,
        "attestation_generation": 2,
        "attestation_sha256": "1" * 64,
        "materialization_id": MATERIALIZATION_ID,
        "attempt_id": ATTEMPT_ID,
        "attempt_number": 4,
        "lease_epoch": 3,
        "builder_id": f"rootless:{SESSION_ID.hex}",
        "purpose": "production",
        "shadow_campaign_id": None,
        "cpu_arch": "arm64",
        "platform": "linux/arm64",
        "component": "task",
        "generation": 2,
        "predecessor_credential_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "predecessor_generation": 1,
        "lease_heartbeat_operation_id": HEARTBEAT_ID,
        "registry_origin": "https://registry.example:5443",
        "registry_service": "registry.example",
        "registry_issuer": "loom-task-image-authority",
        "repository": REPOSITORY,
        "actions": ("pull", "push"),
        "registry_key_id": "K" * 43,
        "bearer_token": BEARER_TOKEN,
        "issued_at": NOW,
        "expires_at": NOW + timedelta(seconds=45),
    }
    values.update(changes)
    return TaskImageRegistryCredentialV1.model_validate(values)


def _candidate_response(**changes: object) -> TaskImagePublicationCandidateResponseV1:
    values: dict[str, object] = {
        "candidate_id": CANDIDATE_ID,
        "operation_id": OPERATION_ID,
        "credential_id": CREDENTIAL_ID,
        "credential_generation": 2,
        "grant_id": GRANT_ID,
        "session_id": SESSION_ID,
        "session_generation": 2,
        "materialization_id": MATERIALIZATION_ID,
        "attempt_id": ATTEMPT_ID,
        "attempt_number": 4,
        "lease_epoch": 3,
        "builder_id": f"rootless:{SESSION_ID.hex}",
        "component": "task",
        "repository": REPOSITORY,
        "manifest_digest": "sha256:" + "2" * 64,
        "manifest_size": 512,
        "oci_file_sha256": "3" * 64,
        "oci_file_size": 4096,
        "platform": "linux/arm64",
        "recorded_at": NOW + timedelta(seconds=1),
    }
    values.update(changes)
    return TaskImagePublicationCandidateResponseV1.model_validate(values)


@pytest.mark.parametrize(
    "changes",
    [
        {"credential_id": UUID(int=0)},
        {"request_id": UUID(int=0)},
        {"grant_id": UUID(int=0)},
        {"session_id": UUID(int=0)},
        {"session_generation": 0},
        {"attestation_generation": 0},
        {"attestation_sha256": "0" * 64},
        {"materialization_id": UUID(int=0)},
        {"attempt_id": UUID(int=0)},
        {"attempt_number": 0},
        {"lease_epoch": 0},
        {"builder_id": "rootless:wrong"},
        {"purpose": "shadow"},
        {"shadow_campaign_id": GRANT_ID},
        {"cpu_arch": "x86_64"},
        {"platform": "linux/amd64"},
        {"component": "sidecar:other"},
        {"generation": 0},
        {"generation": 513},
        {"predecessor_credential_id": None},
        {"predecessor_generation": None},
        {"predecessor_generation": 2},
        {"lease_heartbeat_operation_id": None},
        {"registry_origin": "http://registry.example"},
        {"registry_origin": "https://user@registry.example"},
        {"registry_service": "REGISTRY"},
        {"registry_issuer": "issuer with spaces"},
        {"repository": "library/alpine"},
        {"actions": ("push", "pull")},
        {"actions": ("pull",)},
        {"registry_key_id": "short"},
        {"bearer_token": "not a jwt"},
        {"issued_at": NOW.replace(tzinfo=None)},
        {"expires_at": NOW},
        {"expires_at": NOW + timedelta(seconds=46)},
        {"unknown": "catalog:*"},
    ],
)
def test_credential_rejects_mutated_binding_scope_or_secret(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        _credential(**changes)


def test_first_credential_response_has_no_renewal_evidence() -> None:
    credential = _credential(
        generation=1,
        predecessor_credential_id=None,
        predecessor_generation=None,
        lease_heartbeat_operation_id=None,
    )

    assert credential.generation == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"candidate_id": UUID(int=0)},
        {"operation_id": UUID(int=0)},
        {"credential_id": UUID(int=0)},
        {"credential_generation": 0},
        {"grant_id": UUID(int=0)},
        {"session_id": UUID(int=0)},
        {"session_generation": 0},
        {"materialization_id": UUID(int=0)},
        {"attempt_id": UUID(int=0)},
        {"attempt_number": 0},
        {"lease_epoch": 0},
        {"builder_id": "rootless:wrong"},
        {"component": "sidecar:"},
        {"repository": "library/alpine"},
        {"manifest_digest": "bad"},
        {"manifest_size": 0},
        {"oci_file_sha256": "bad"},
        {"oci_file_size": 0},
        {"platform": "linux/s390x"},
        {"recorded_at": NOW.replace(tzinfo=None)},
        {"unknown": True},
    ],
)
def test_candidate_response_rejects_mutated_binding(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        _candidate_response(**changes)
