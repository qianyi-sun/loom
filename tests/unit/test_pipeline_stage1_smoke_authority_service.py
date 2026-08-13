from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient

from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.public_api import PipelineRunSubmitRequestV1
from loom.pipeline.recipes import OfficialRecipeRegistry
from loom.pipeline.stage1_smoke import (
    Stage1SmokeAuthorizationV1,
    Stage1SmokeGpuDeviceV1,
    Stage1SmokePreflightV1,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.pipeline_api_service import PipelineApiError, create_public_run
from loom_service.pipeline_stage1_smoke_service import (
    Stage1SmokeSignatureVerifier,
    _validate_worker,
    capacity_preflight_signature_payload,
    execute_signature_payload,
)
from tests.unit.test_pipeline_stage1_smoke import _candidate


def test_stage1_signature_verifier_is_fresh_exact_and_key_scoped() -> None:
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes_raw()
    verifier = Stage1SmokeSignatureVerifier(keys={"stage1-test": public}, max_age_seconds=60)
    now = datetime.now(UTC)
    payload = canonical_document({"action": "execute", "candidate_sha256": "sha256:" + "1" * 64})
    signature = private.sign(payload).hex()

    assert verifier.verify(
        key_id="stage1-test",
        payload=payload,
        signature=signature,
        observed_at=now,
        now=now,
    ).startswith("sha256:")
    with pytest.raises(ValueError, match="signature is invalid"):
        verifier.verify(
            key_id="stage1-test",
            payload=payload + b"x",
            signature=signature,
            observed_at=now,
            now=now,
        )


def test_hidden_stage1_routes_are_absent_from_openapi_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
    }.items():
        monkeypatch.setenv(key, value)
    app = create_app(LoomServiceSettings(_env_file=None))
    assert not any("stage1-smoke" in path for path in app.openapi().get("paths", {}))
    assert not hasattr(app.state, "pipeline_stage1_smoke_verifier")
    assert not hasattr(app.state, "pipeline_stage1_execution_preflight_authority")
    assert not hasattr(app.state, "pipeline_stage1_evidence_authority")
    assert not hasattr(app.state, "pipeline_stage1_cleanup_authority")


@pytest.mark.asyncio
async def test_stage1_identity_is_404_on_the_public_submission_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PendingIdempotency:
        replay = False
        record = SimpleNamespace(response_json=None)

    async def claim_without_write(*_args: object, **_kwargs: object) -> PendingIdempotency:
        return PendingIdempotency()

    monkeypatch.setattr(
        "loom_service.pipeline_api_service.claim_idempotency",
        claim_without_write,
    )
    request = PipelineRunSubmitRequestV1.model_validate_json(
        canonical_document(
            {
                "budget": _candidate().run_budget.model_dump(mode="json"),
                "display_name": "must stay hidden",
                "inputs": {},
                "judge_profile_id": None,
                "parameters": {},
                "recipe": "behavior-stage1-smoke@1",
            }
        )
    )
    with pytest.raises(PipelineApiError) as raised:
        await create_public_run(
            SimpleNamespace(),  # type: ignore[arg-type]
            team_id=uuid4(),
            user_id=uuid4(),
            idempotency_key="hidden-stage1",
            request=request,
            registry=OfficialRecipeRegistry(),
        )
    assert raised.value.status_code == 404
    assert raised.value.reason_code == "not_found"


def test_execute_http_json_scalars_reach_the_fail_closed_preflight_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
    }.items():
        monkeypatch.setenv(key, value)
    app = create_app(LoomServiceSettings(_env_file=None))
    private = Ed25519PrivateKey.generate()
    app.state.pipeline_stage1_smoke_verifier = Stage1SmokeSignatureVerifier(
        keys={"stage1-test": private.public_key().public_bytes_raw()},
        max_age_seconds=300,
    )
    # The route checks this handle before the independently injected preflight
    # authority, but must never enter it when that authority is absent.
    app.state.session_factory = object()
    now = datetime.now(UTC)
    candidate = _candidate(
        start_by=now.replace(microsecond=0),
        cleanup_deadline=now.replace(microsecond=0) + timedelta(hours=1),
    )
    authorization = Stage1SmokeAuthorizationV1(
        schema_version="loom.behavior-stage1-smoke-authorization.v1",
        action="stage1",
        authorization_id=uuid4(),
        candidate_sha256=candidate.candidate_sha256,
        operator_user_id=candidate.operator_user_id,
        team_id=candidate.team_id,
        environment=candidate.environment,
        loom_commit_sha=candidate.loom_commit_sha,
        recipe_digest=candidate.recipe_digest,
        image_index_digest=candidate.image_index_digest,
        platform=candidate.platform,
        platform_child_digest=candidate.platform_child_digest,
        backend_variant_id=candidate.backend_variant_id,
        policy_id=candidate.policy_id,
        policy_config_sha256=candidate.policy_config_sha256,
        policy_activation_epoch=candidate.policy_activation_epoch,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        run_budget_sha256=canonical_digest(candidate.run_budget),
        start_by=candidate.start_by,
        cleanup_deadline=candidate.cleanup_deadline,
        live_mutation_authorized=True,
        authorized_at=now,
        expires_at=now + timedelta(minutes=2),
        nonce_sha256="sha256:" + "9" * 64,
    )
    preflight = Stage1SmokePreflightV1(
        schema_version="loom.behavior-stage1-smoke-preflight.v1",
        candidate_sha256=candidate.candidate_sha256,
        authorization_id=authorization.authorization_id,
        authorization_sha256=authorization.authorization_sha256,
        worker_id=uuid4(),
        worker_lease_epoch=1,
        worker_capability_snapshot_sha256="sha256:" + "8" * 64,
        slurm_allocation_id="oldlab:test",
        gpu_devices=[
            Stage1SmokeGpuDeviceV1(
                logical_index=0,
                device_uuid="GPU-0",
                model="NVIDIA GeForce RTX 5080",
                role="sim",
            ),
            Stage1SmokeGpuDeviceV1(
                logical_index=1,
                device_uuid="GPU-1",
                model="NVIDIA GeForce RTX 5080",
                role="vla",
            ),
        ],
        policy_activation_epoch=candidate.policy_activation_epoch,
        platform_child_digest=candidate.platform_child_digest,
        image_runtime_contract_sha256=candidate.image_runtime_contract_sha256,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        ancestry_ok=True,
        image_platform_ok=True,
        worker_capability_ok=True,
        slurm_config_ok=True,
        gpu_topology_ok=True,
        cas_capacity_ok=True,
        scratch_capacity_ok=True,
        input_markers_ok=True,
        existing_pipeline_runs=0,
        existing_attempts=0,
        existing_upload_sessions=0,
        existing_slurm_jobs=0,
        observed_at=now,
    )
    key = "stage1-route-json"
    capacity_signed = capacity_preflight_signature_payload(
        candidate=candidate,
        authorization=authorization,
        idempotency_key="stage1-capacity-route-json",
    )
    assert b'"action":"capacity_preflight_activate_one_slot"' in capacity_signed
    assert b'"activation_state":"active","desired_slots":1' in capacity_signed
    signature = private.sign(
        execute_signature_payload(
            candidate=candidate,
            authorization=authorization,
            preflight=preflight,
            idempotency_key=key,
        )
    ).hex()
    assert capacity_signed != execute_signature_payload(
        candidate=candidate,
        authorization=authorization,
        preflight=preflight,
        idempotency_key=key,
    )
    response = TestClient(app).post(
        "/api/v1/internal/pipeline-stage1-smoke/execute",
        json={
            "candidate": candidate.model_dump(mode="json"),
            "authorization": authorization.model_dump(mode="json"),
            "preflight": preflight.model_dump(mode="json"),
        },
        headers={
            "Idempotency-Key": key,
            "X-Loom-Stage1-Signature-Key-Id": "stage1-test",
            "X-Loom-Stage1-Signature": signature,
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "stage1_smoke_preflight_authority_unavailable"


def test_capacity_http_json_scalars_reach_the_fail_closed_authority_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
    }.items():
        monkeypatch.setenv(key, value)
    app = create_app(LoomServiceSettings(_env_file=None))
    private = Ed25519PrivateKey.generate()
    app.state.pipeline_stage1_smoke_verifier = Stage1SmokeSignatureVerifier(
        keys={"stage1-test": private.public_key().public_bytes_raw()},
        max_age_seconds=300,
    )
    app.state.session_factory = object()
    now = datetime.now(UTC)
    candidate = _candidate(
        start_by=now.replace(microsecond=0),
        cleanup_deadline=now.replace(microsecond=0) + timedelta(hours=1),
    )
    authorization = Stage1SmokeAuthorizationV1(
        schema_version="loom.behavior-stage1-smoke-authorization.v1",
        action="stage1",
        authorization_id=uuid4(),
        candidate_sha256=candidate.candidate_sha256,
        operator_user_id=candidate.operator_user_id,
        team_id=candidate.team_id,
        environment=candidate.environment,
        loom_commit_sha=candidate.loom_commit_sha,
        recipe_digest=candidate.recipe_digest,
        image_index_digest=candidate.image_index_digest,
        platform=candidate.platform,
        platform_child_digest=candidate.platform_child_digest,
        backend_variant_id=candidate.backend_variant_id,
        policy_id=candidate.policy_id,
        policy_config_sha256=candidate.policy_config_sha256,
        policy_activation_epoch=candidate.policy_activation_epoch,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        run_budget_sha256=canonical_digest(candidate.run_budget),
        start_by=candidate.start_by,
        cleanup_deadline=candidate.cleanup_deadline,
        live_mutation_authorized=True,
        authorized_at=now,
        expires_at=now + timedelta(minutes=2),
        nonce_sha256="sha256:" + "9" * 64,
    )
    key = "stage1-capacity-route-json"
    signature = private.sign(
        capacity_preflight_signature_payload(
            candidate=candidate,
            authorization=authorization,
            idempotency_key=key,
        )
    ).hex()
    response = TestClient(app).post(
        "/api/v1/internal/pipeline-stage1-smoke/capacity-preflight",
        json={
            "candidate": candidate.model_dump(mode="json"),
            "authorization": authorization.model_dump(mode="json"),
        },
        headers={
            "Idempotency-Key": key,
            "X-Loom-Stage1-Signature-Key-Id": "stage1-test",
            "X-Loom-Stage1-Signature": signature,
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == (
        "stage1_smoke_capacity_preflight_authority_unavailable"
    )


@pytest.mark.asyncio
async def test_worker_validation_preserves_preflight_cuda_order() -> None:
    candidate = _candidate()
    worker_id = uuid4()
    preflight = Stage1SmokePreflightV1(
        schema_version="loom.behavior-stage1-smoke-preflight.v1",
        candidate_sha256=candidate.candidate_sha256,
        authorization_id=uuid4(),
        authorization_sha256="sha256:" + "7" * 64,
        worker_id=worker_id,
        worker_lease_epoch=2,
        worker_capability_snapshot_sha256="sha256:" + "8" * 64,
        slurm_allocation_id="oldlab:123",
        gpu_devices=[
            Stage1SmokeGpuDeviceV1(
                logical_index=0,
                device_uuid="GPU-Z",
                model="NVIDIA GeForce RTX 5080",
                role="sim",
            ),
            Stage1SmokeGpuDeviceV1(
                logical_index=1,
                device_uuid="GPU-A",
                model="NVIDIA GeForce RTX 5080",
                role="vla",
            ),
        ],
        policy_activation_epoch=candidate.policy_activation_epoch,
        platform_child_digest=candidate.platform_child_digest,
        image_runtime_contract_sha256=candidate.image_runtime_contract_sha256,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        ancestry_ok=True,
        image_platform_ok=True,
        worker_capability_ok=True,
        slurm_config_ok=True,
        gpu_topology_ok=True,
        cas_capacity_ok=True,
        scratch_capacity_ok=True,
        input_markers_ok=True,
        existing_pipeline_runs=0,
        existing_attempts=0,
        existing_upload_sessions=0,
        existing_slurm_jobs=0,
        observed_at=datetime.now(UTC),
    )
    worker = SimpleNamespace(
        lease_epoch=2,
        capability_snapshot_digest=preflight.worker_capability_snapshot_sha256,
        pool_name=candidate.policy_id,
        capability_snapshot_json={
            "cpu_arch": "x86_64",
            "gpu_devices": [
                {"device_uuid": "GPU-A", "model": "NVIDIA GeForce RTX 5080"},
                {"device_uuid": "GPU-Z", "model": "NVIDIA GeForce RTX 5080"},
            ],
        },
        slurm_gpu_allocation_evidence_json={
            "slurm_cluster_id": "oldlab",
            "allocation_id": "oldlab:123",
            "variant_id": "oldlab-rtx5080-2gpu",
            "device_uuids": ["GPU-A", "GPU-Z"],
        },
    )

    class FakeSession:
        async def get(self, *_args: object, **_kwargs: object) -> object:
            return worker

    await _validate_worker(FakeSession(), candidate=candidate, preflight=preflight)  # type: ignore[arg-type]
