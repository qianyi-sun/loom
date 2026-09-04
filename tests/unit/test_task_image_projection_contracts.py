from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_task_image_authority.contracts import (
    TaskImageAttachmentProofV1,
    TaskImageBootstrapExchangeV1,
    TaskImageBuildGrantAuthorityV1,
    TaskImageBuildSessionV1,
    TaskImageContainmentAttachmentV1,
    TaskImageContainmentAttestationV1,
    TaskImageGuardPrincipalV1,
    TaskImageProjectionChallengeV1,
    TaskImageProjectionReceiptV1,
    TaskImageProjectionRequestV1,
    TaskImageProjectionRevocationV1,
    canonical_authority_bytes,
    canonical_authority_sha256,
    new_bootstrap_token,
    new_session_token,
)

NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")
REQUEST_ID = UUID("22222222-2222-2222-2222-222222222222")
CHALLENGE_NONCE = UUID("33333333-3333-3333-3333-333333333333")
PROOF_ID = UUID("44444444-4444-4444-4444-444444444444")
EXCHANGE_ID = UUID("55555555-5555-5555-5555-555555555555")
SESSION_ID = UUID("66666666-6666-6666-6666-666666666666")
NODE_BOOT_ID = UUID("77777777-7777-7777-7777-777777777777")
ATTESTATION_ID = UUID("88888888-8888-8888-8888-888888888888")


def _authority(**changes: object) -> TaskImageBuildGrantAuthorityV1:
    values: dict[str, object] = {
        "purpose": "production",
        "shadow_campaign_id": None,
        "environment": "staging",
        "pool_id": "staging-gb10-task-image",
        "slurm_cluster_id": "gb10",
        "cpu_arch": "arm64",
        "slurm_request_sha256": "1" * 64,
        "builder_release_sha256": "2" * 64,
        "build_policy_sha256": "3" * 64,
        "containment_policy_sha256": "4" * 64,
        "resource_profile_sha256": "5" * 64,
        "issued_at": NOW,
        "expires_at": NOW + timedelta(hours=2),
    }
    values.update(changes)
    return TaskImageBuildGrantAuthorityV1.model_validate(values)


def _request(**changes: object) -> TaskImageProjectionRequestV1:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "grant_id": GRANT_ID,
        "observed_at": NOW + timedelta(seconds=1),
        "node_name": "trt-gb10-1",
        "node_boot_id": NODE_BOOT_ID,
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "supervisor_pid": 42100,
        "supervisor_uid": 993,
        "supervisor_gid": 980,
        "supervisor_executable_sha256": "6" * 64,
        "cgroup_path": "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch",
        "cgroup_inode": 987654,
        "submitting_identity": "loom-builder",
        "slurm_account": "loom-task-builder",
        "slurm_partition": "loom-task-builder",
        "slurm_qos": "loom-task-image-builder-rootless-gb10",
        "cpu_arch": "arm64",
        "slurm_request_sha256": "1" * 64,
    }
    values.update(changes)
    return TaskImageProjectionRequestV1.model_validate(values)


def _attachment(**changes: object) -> TaskImageContainmentAttachmentV1:
    cgroup = "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch"
    values: dict[str, object] = {
        "cgroup_inode": 987654,
        "containment_root": f"{cgroup}/loom-builder",
        "trusted_service_cgroup": f"{cgroup}/loom-builder/trusted-service",
        "build_egress_cgroup": f"{cgroup}/loom-builder/build-egress",
        "bpf_program_sha256": "7" * 64,
        "bpf_map_schema_sha256": "8" * 64,
        "containment_policy_sha256": "4" * 64,
        "resource_limits_sha256": "5" * 64,
        "probe_sha256": "9" * 64,
        "link_ids": (101, 102, 103),
        "program_ids": (201, 202, 203),
        "map_ids": (301, 302),
    }
    values.update(changes)
    return TaskImageContainmentAttachmentV1.model_validate(values)


def _challenge(**changes: object) -> TaskImageProjectionChallengeV1:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "grant_id": GRANT_ID,
        "request_sha256": "a" * 64,
        "challenge_nonce": CHALLENGE_NONCE,
        "containment_policy_sha256": "4" * 64,
        "resource_profile_sha256": "5" * 64,
        "issued_at": NOW + timedelta(seconds=2),
        "expires_at": NOW + timedelta(seconds=62),
    }
    values.update(changes)
    return TaskImageProjectionChallengeV1.model_validate(values)


def _proof(**changes: object) -> TaskImageAttachmentProofV1:
    request = _request()
    values: dict[str, object] = {
        "proof_id": PROOF_ID,
        "grant_id": GRANT_ID,
        "request_id": REQUEST_ID,
        "request_sha256": "a" * 64,
        "challenge_nonce": CHALLENGE_NONCE,
        "observed_at": NOW + timedelta(seconds=3),
        "node_name": request.node_name,
        "node_boot_id": request.node_boot_id,
        "slurm_cluster_id": request.slurm_cluster_id,
        "slurm_job_id": request.slurm_job_id,
        "cgroup_path": request.cgroup_path,
        "cgroup_inode": request.cgroup_inode,
        "attachment": _attachment(),
        "attestation_generation": 1,
        "attestation_expires_at": NOW + timedelta(seconds=33),
    }
    values.update(changes)
    return TaskImageAttachmentProofV1.model_validate(values)


def test_authority_uses_literal_rfc8785_bytes_and_digest() -> None:
    authority = _authority()
    expected = (
        b'{"build_policy_sha256":"3333333333333333333333333333333333333333333333333333333333333333",'
        b'"builder_release_sha256":"2222222222222222222222222222222222222222222222222222222222222222",'
        b'"containment_policy_sha256":"4444444444444444444444444444444444444444444444444444444444444444",'
        b'"cpu_arch":"arm64","environment":"staging","expires_at":"2026-09-02T16:00:00Z",'
        b'"issued_at":"2026-09-02T14:00:00Z","pool_id":"staging-gb10-task-image",'
        b'"purpose":"production",'
        b'"resource_profile_sha256":"5555555555555555555555555555555555555555555555555555555555555555",'
        b'"schema_version":1,"shadow_campaign_id":null,"slurm_cluster_id":"gb10",'
        b'"slurm_request_sha256":"1111111111111111111111111111111111111111111111111111111111111111"}'
    )

    assert canonical_authority_bytes(authority) == expected
    assert canonical_authority_sha256(authority) == (
        "4bef145cc35374e1cce1b888dba46c709d47f1bc3cf433ceab739638c0b784fd"
    )
    assert hashlib.sha256(expected).hexdigest() == canonical_authority_sha256(authority)


@pytest.mark.parametrize(
    "changes",
    [
        {"purpose": "shadow", "shadow_campaign_id": None},
        {"purpose": "production", "shadow_campaign_id": GRANT_ID},
        {"slurm_cluster_id": "oldlab", "cpu_arch": "arm64"},
        {"slurm_cluster_id": "gb10", "cpu_arch": "x86_64"},
        {"slurm_request_sha256": "0" * 64},
        {"slurm_request_sha256": "A" * 64},
        {"issued_at": NOW.replace(tzinfo=None)},
        {"expires_at": NOW},
        {"expires_at": NOW + timedelta(hours=4, seconds=1)},
        {"environment": "staging/other"},
        {"pool_id": "GB10 pool"},
    ],
)
def test_grant_authority_rejects_ambiguous_or_unbounded_identity(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        _authority(**changes)


def test_shadow_authority_requires_and_binds_one_campaign() -> None:
    authority = _authority(purpose="shadow", shadow_campaign_id=GRANT_ID)

    assert authority.purpose == "shadow"
    assert authority.shadow_campaign_id == GRANT_ID


def test_guard_principal_is_canonical_and_single_node_bound() -> None:
    principal = TaskImageGuardPrincipalV1(
        principal_id="gb10-trt-gb10-1",
        slurm_cluster_id="gb10",
        node_name="trt-gb10-1",
        scopes=("task-image:project", "task-image:attest"),
    )

    assert principal.scopes == ("task-image:attest", "task-image:project")
    with pytest.raises(ValidationError, match="duplicate"):
        TaskImageGuardPrincipalV1.model_validate(
            {**principal.model_dump(), "scopes": ("task-image:project",) * 2}
        )
    with pytest.raises(ValidationError):
        TaskImageGuardPrincipalV1.model_validate(
            {**principal.model_dump(), "node_name": "TRT GB10 1"}
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": UUID(int=0)},
        {"observed_at": NOW.replace(tzinfo=None)},
        {"slurm_job_id": "12345.batch"},
        {"supervisor_pid": 0},
        {"supervisor_uid": -1},
        {"supervisor_gid": True},
        {"cgroup_inode": 1 << 63},
        {"cgroup_path": "relative/job_12345"},
        {"cgroup_path": "/sys/fs/cgroup/job_12345/../other"},
        {"submitting_identity": "root"},
        {"slurm_account": "loom-staging"},
        {"slurm_partition": "gb10"},
        {"slurm_qos": "loom-task-image-builder"},
        {"slurm_cluster_id": "oldlab", "cpu_arch": "arm64"},
    ],
)
def test_projection_request_rejects_untrusted_or_noncanonical_job_facts(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        _request(**changes)


def test_projection_request_accepts_offset_time_but_canonicalizes_to_utc() -> None:
    observed = datetime(2026, 9, 2, 10, 0, 1, tzinfo=timezone(timedelta(hours=-4)))

    request = _request(observed_at=observed)

    assert request.observed_at == NOW + timedelta(seconds=1)
    assert request.model_dump(mode="json")["observed_at"] == "2026-09-02T14:00:01Z"


@pytest.mark.parametrize(
    "changes",
    [
        {"containment_root": "/sys/fs/cgroup/job_999/loom-builder"},
        {
            "trusted_service_cgroup": (
                "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch/other"
            )
        },
        {
            "build_egress_cgroup": (
                "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch/loom-builder"
                "/trusted-service/nested"
            )
        },
        {"link_ids": (101, 101)},
        {"program_ids": (203, 202)},
        {"map_ids": ()},
        {"resource_limits_sha256": "0" * 64},
    ],
)
def test_attachment_rejects_unsafe_paths_or_noncanonical_kernel_identity(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        _attachment(**changes)


def test_attachment_proof_binds_request_cgroup_and_initial_attestation() -> None:
    proof = _proof()

    assert proof.attestation_generation == 1
    assert proof.attachment.cgroup_inode == proof.cgroup_inode
    assert proof.attachment.containment_root.startswith(f"{proof.cgroup_path}/")

    with pytest.raises(ValidationError, match="cgroup inode"):
        _proof(cgroup_inode=987655)
    with pytest.raises(ValidationError, match="containment root"):
        alternate_root = "/sys/fs/cgroup/system.slice/other/loom-builder"
        _proof(
            attachment=_attachment(
                containment_root=alternate_root,
                trusted_service_cgroup=f"{alternate_root}/trusted-service",
                build_egress_cgroup=f"{alternate_root}/build-egress",
            )
        )
    with pytest.raises(ValidationError, match="generation"):
        _proof(attestation_generation=2)


def test_challenge_and_proof_require_forward_bounded_time() -> None:
    assert _challenge().expires_at - _challenge().issued_at == timedelta(seconds=60)
    with pytest.raises(ValidationError):
        _challenge(expires_at=NOW + timedelta(seconds=2))
    with pytest.raises(ValidationError):
        _challenge(expires_at=NOW + timedelta(seconds=63))
    with pytest.raises(ValidationError):
        _proof(attestation_expires_at=NOW + timedelta(seconds=3))


def test_attestation_requires_same_attachment_and_forward_generation() -> None:
    attestation = TaskImageContainmentAttestationV1(
        attestation_id=ATTESTATION_ID,
        grant_id=GRANT_ID,
        generation=2,
        node_name="trt-gb10-1",
        node_boot_id=NODE_BOOT_ID,
        slurm_cluster_id="gb10",
        slurm_job_id="12345",
        cgroup_path=_request().cgroup_path,
        cgroup_inode=987654,
        attachment=_attachment(),
        issued_at=NOW + timedelta(seconds=20),
        expires_at=NOW + timedelta(seconds=50),
    )

    assert attestation.generation == 2
    with pytest.raises(ValidationError, match="cgroup inode"):
        TaskImageContainmentAttestationV1.model_validate(
            {**attestation.model_dump(), "cgroup_inode": 987655}
        )


def test_projection_revocation_is_strict_bounded_and_canonical() -> None:
    revocation = TaskImageProjectionRevocationV1(
        grant_id=GRANT_ID,
        reason="guard_attestation_lost",
        observed_at=NOW + timedelta(seconds=9),
    )

    assert revocation.model_dump(mode="json") == {
        "schema_version": 1,
        "grant_id": str(GRANT_ID),
        "reason": "guard_attestation_lost",
        "observed_at": "2026-09-02T14:00:09Z",
    }
    assert canonical_authority_bytes(revocation) == (
        b'{"grant_id":"11111111-1111-1111-1111-111111111111",'
        b'"observed_at":"2026-09-02T14:00:09Z","reason":"guard_attestation_lost",'
        b'"schema_version":1}'
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"grant_id": UUID(int=0)},
        {"reason": ""},
        {"reason": "Operator_revoked"},
        {"reason": "operator-revoked"},
        {"reason": "a" + "b" * 64},
        {"observed_at": NOW.replace(tzinfo=None)},
        {"unknown": True},
    ],
)
def test_projection_revocation_rejects_ambiguous_fields(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "grant_id": GRANT_ID,
        "reason": "guard_attestation_lost",
        "observed_at": NOW + timedelta(seconds=9),
    }
    values.update(changes)

    with pytest.raises(ValidationError):
        TaskImageProjectionRevocationV1.model_validate(values)


def test_secret_responses_expose_only_hashed_public_bindings() -> None:
    receipt = TaskImageProjectionReceiptV1(
        grant_id=GRANT_ID,
        proof_id=PROOF_ID,
        proof_sha256="b" * 64,
        bootstrap_token="loom_tibp_" + "A" * 64,
        issued_at=NOW + timedelta(seconds=4),
        expires_at=NOW + timedelta(seconds=34),
    )
    exchange = TaskImageBootstrapExchangeV1(
        exchange_id=EXCHANGE_ID,
        grant_id=GRANT_ID,
        proof_sha256="b" * 64,
        bootstrap_token=receipt.bootstrap_token,
        observed_at=NOW + timedelta(seconds=5),
    )
    session = TaskImageBuildSessionV1(
        grant_id=GRANT_ID,
        session_id=SESSION_ID,
        purpose="production",
        shadow_campaign_id=None,
        pool_id="staging-gb10-task-image",
        cpu_arch="arm64",
        session_token="loom_tibs_" + "B" * 64,
        attestation_generation=1,
        attestation_sha256="c" * 64,
        issued_at=NOW + timedelta(seconds=6),
        expires_at=NOW + timedelta(seconds=30),
    )

    receipt_binding = receipt.public_binding()
    exchange_binding = exchange.public_binding()
    session_binding = session.public_binding()
    assert "bootstrap_token" not in receipt_binding
    assert "bootstrap_token" not in exchange_binding
    assert "session_token" not in session_binding
    assert receipt_binding["bootstrap_token_sha256"] == hashlib.sha256(
        receipt.bootstrap_token.encode("utf-8")
    ).hexdigest()
    assert exchange_binding["bootstrap_token_sha256"] == receipt_binding[
        "bootstrap_token_sha256"
    ]
    assert session_binding["session_token_sha256"] == hashlib.sha256(
        session.session_token.encode("utf-8")
    ).hexdigest()
    for model in (receipt, exchange, session):
        with pytest.raises(TypeError, match="secret-bearing"):
            canonical_authority_bytes(model)


def test_token_factories_are_typed_random_and_noninterchangeable() -> None:
    bootstrap_a = new_bootstrap_token()
    bootstrap_b = new_bootstrap_token()
    session_a = new_session_token()
    session_b = new_session_token()

    assert bootstrap_a.startswith("loom_tibp_")
    assert session_a.startswith("loom_tibs_")
    assert len({bootstrap_a, bootstrap_b, session_a, session_b}) == 4
    assert len(bootstrap_a) >= 74
    assert len(session_a) >= 74


def test_all_contracts_reject_unknown_fields() -> None:
    values = _authority().model_dump()
    values["registry_token"] = "forbidden"

    with pytest.raises(ValidationError, match="extra_forbidden"):
        TaskImageBuildGrantAuthorityV1.model_validate(values)
