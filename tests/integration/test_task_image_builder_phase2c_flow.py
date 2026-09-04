from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from threading import Thread
from types import MethodType
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from loom.db.schema import (
    TaskImageBuildProjection,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
)
from loom_control_plane.task_image_build_environment import canonical_request_sha256
from loom_task_image_authority.api import create_app
from loom_task_image_authority.auth import TaskImagePrincipalVerifier
from loom_task_image_authority.bundle_capability import TaskImageBundleCapabilityProvider
from loom_task_image_authority.contracts import (
    TaskImageAttachmentProofV1,
    TaskImageContainmentAttestationV1,
    TaskImageProjectionRequestV1,
    canonical_authority_sha256,
)
from loom_task_image_builder_guard.authority import (
    AcceptedAttestation,
    BuildSession,
    LeaseAcknowledgement,
    ProjectionChallenge,
    ProjectionReceipt,
    SealedAuthorityPayload,
)
from loom_task_image_builder_guard.protocol import create_sealed_memfd
from tests.integration.test_task_image_authority_api import (
    _BEARER,
    NOW,
    _clear_authority_rows,
    _FakeBundleBackend,
    _principal_document,
    _seed_materialization,
    _seed_released_grant,
    _settings,
)
from tests.integration.test_task_image_projection_store import GRANT_ID, SUPERVISOR_SHA256, _policy
from tests.unit.test_task_image_builder_guard_service import (
    PROOF,
    REQUEST,
    RESPONSE,
    _service,
)


def _sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _phase2c_uuid_factory():
    seeded = iter((REQUEST, PROOF, RESPONSE))
    counter = 1
    while True:
        try:
            yield next(seeded)
        except StopIteration:
            yield UUID(int=counter)
            counter += 1


class _FastAPIAuthorityAdapter:
    def __init__(self, client: TestClient, events: list[str]) -> None:
        self.client = client
        self.events = events
        self.headers = {"Authorization": f"Bearer {_BEARER}"}

    @staticmethod
    def request_sha256(value: object) -> str:
        if isinstance(value, dict):
            if {
                "request_id",
                "supervisor_executable_sha256",
                "slurm_request_sha256",
            }.issubset(value):
                return canonical_authority_sha256(
                    TaskImageProjectionRequestV1.model_validate_json(json.dumps(value))
                )
            if {"proof_id", "attachment", "attestation_generation"}.issubset(value):
                return canonical_authority_sha256(
                    TaskImageAttachmentProofV1.model_validate_json(json.dumps(value))
                )
            if {"attestation_id", "attachment", "generation"}.issubset(value):
                return canonical_authority_sha256(
                    TaskImageContainmentAttestationV1.model_validate_json(json.dumps(value))
                )
        return _sha256(value)

    def _put(self, path: str, document: dict[str, Any], *, status: int = 200) -> bytes:
        response = self.client.put(path, json=document, headers=self.headers)
        redacted = dict(document)
        redacted.pop("bootstrap_token", None)
        redacted.pop("session_token", None)
        assert response.status_code == status, f"{path}: {response.text}; request={redacted!r}"
        return response.content

    def _post(self, path: str, document: dict[str, Any], *, status: tuple[int, ...] = (200, 204)) -> bytes:
        response = self.client.post(path, json=document, headers=self.headers)
        redacted = dict(document)
        redacted.pop("bootstrap_token", None)
        redacted.pop("session_token", None)
        assert response.status_code in status, f"{path}: {response.text}; request={redacted!r}"
        return response.content

    def challenge(self, grant_id: UUID, request: dict[str, object], **_: object) -> ProjectionChallenge:
        self.events.append("authority_api_challenge")
        raw = self._put(f"/v1/projections/{grant_id}/challenge", dict(request))
        value = json.loads(raw)
        return ProjectionChallenge(
            UUID(value["request_id"]),
            UUID(value["grant_id"]),
            value["request_sha256"],
            UUID(value["challenge_nonce"]),
            value["containment_policy_sha256"],
            value["resource_profile_sha256"],
            _dt(value["issued_at"]),
            _dt(value["expires_at"]),
        )

    def attach(self, grant_id: UUID, proof: dict[str, object]) -> ProjectionReceipt:
        self.events.append("authority_api_attach")
        TaskImageAttachmentProofV1.model_validate_json(json.dumps(proof))
        raw = self._put(f"/v1/projections/{grant_id}/attachment", dict(proof))
        value = json.loads(raw)
        return ProjectionReceipt(
            UUID(value["grant_id"]),
            UUID(value["proof_id"]),
            value["proof_sha256"],
            value["bootstrap_token"],
            _dt(value["issued_at"]),
            _dt(value["expires_at"]),
            raw,
        )

    def _session(self, raw: bytes) -> BuildSession:
        value = json.loads(raw)
        return BuildSession(
            UUID(value["grant_id"]),
            UUID(value["session_id"]),
            value["purpose"],
            None if value["shadow_campaign_id"] is None else UUID(value["shadow_campaign_id"]),
            value["pool_id"],
            value["cpu_arch"],
            value["session_token"],
            int(value["attestation_generation"]),
            value["attestation_sha256"],
            _dt(value["issued_at"]),
            _dt(value["expires_at"]),
            raw,
            int(value["generation"]),
        )

    def parse_session(self, payload: bytes) -> BuildSession:
        return self._session(payload)

    def exchange(self, grant_id: UUID, request: dict[str, object]) -> BuildSession:
        self.events.append("authority_api_exchange")
        return self._session(self._put(f"/v1/projections/{grant_id}/exchange", dict(request)))

    def renew(self, grant_id: UUID, generation: int, request: dict[str, object]) -> BuildSession:
        self.events.append("authority_api_renew")
        return self._session(
            self._put(f"/v1/projections/{grant_id}/sessions/{generation}/renew", dict(request))
        )

    def attest(self, grant_id: UUID, generation: int, attestation: dict[str, object]) -> AcceptedAttestation:
        self.events.append("authority_api_attest")
        raw = self._put(
            f"/v1/projections/{grant_id}/attestations/{generation}",
            dict(attestation),
        )
        value = json.loads(raw)
        return AcceptedAttestation(
            UUID(value["attestation_id"]),
            UUID(value["grant_id"]),
            int(value["generation"]),
            _dt(value["issued_at"]),
            _dt(value["expires_at"]),
            _sha256(value),
        )

    def claim(self, grant_id: UUID, request: dict[str, object]) -> SealedAuthorityPayload | None:
        self.events.append("authority_api_claim")
        raw = self._post(f"/v1/projections/{grant_id}/materializations/claim", dict(request))
        if not raw:
            return None
        fd = create_sealed_memfd("claim", raw, maximum=8 * 1024 * 1024)
        return SealedAuthorityPayload(fd, hashlib.sha256(raw).hexdigest())

    def bundle(self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]) -> SealedAuthorityPayload:
        self.events.append("authority_api_bundle")
        raw = self._put(
            f"/v1/projections/{grant_id}/materializations/{materialization_id}/bundle",
            dict(request),
        )
        fd = create_sealed_memfd("bundle", raw, maximum=8 * 1024 * 1024)
        return SealedAuthorityPayload(fd, hashlib.sha256(raw).hexdigest())

    def _lease(self, operation: str, grant_id: UUID, materialization_id: UUID, request: dict[str, object]) -> LeaseAcknowledgement:
        self.events.append(f"authority_api_{operation}")
        raw = self._put(
            f"/v1/projections/{grant_id}/materializations/{materialization_id}/{operation}",
            dict(request),
        )
        value = json.loads(raw)
        return LeaseAcknowledgement(
            value["operation"],
            UUID(value["operation_id"]),
            UUID(value["materialization_id"]),
            UUID(value["attempt_id"]),
            int(value["lease_epoch"]),
            value["state"],
            int(value["deterministic_failure_count"]),
            None if value["lease_expires_at"] is None else _dt(value["lease_expires_at"]),
        )

    def start(self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]) -> LeaseAcknowledgement:
        return self._lease("start", grant_id, materialization_id, request)

    def heartbeat(self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]) -> LeaseAcknowledgement:
        return self._lease("heartbeat", grant_id, materialization_id, request)

    def release(self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]) -> LeaseAcknowledgement:
        return self._lease("release", grant_id, materialization_id, request)

    def fail(self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]) -> LeaseAcknowledgement:
        return self._lease("fail", grant_id, materialization_id, request)

    def revoke(self, grant_id: UUID, request: dict[str, object]) -> None:
        self.events.append("authority_api_revoke")
        self._put(f"/v1/projections/{grant_id}/revocation", dict(request), status=204)


@pytest.mark.asyncio
async def test_real_authority_guard_socket_and_go_orchestrator_flow(
    tmp_path: Path,
    postgres_url: str,
) -> None:
    await _clear_authority_rows(postgres_url)
    await _seed_released_grant(postgres_url)
    materialization_id = await _seed_materialization(postgres_url)
    settings = _settings(
        tmp_path,
        postgres_url,
        principal_document=_principal_document(principal_id="gb10-trt-gb10-1"),
        bundle_public_https_origin="https://objects.example",
        bundle_expected_bucket="loom-bundles",
        bundle_url_expiry_seconds=600,
    )
    bundle_backend = _FakeBundleBackend()
    authority_events: list[str] = []
    provider = TaskImageBundleCapabilityProvider(
        backend=bundle_backend,
        public_https_origin=settings.bundle_public_https_origin or "",
        expected_bucket=settings.bundle_expected_bucket or "",
        maximum_objects=settings.bundle_maximum_objects,
        maximum_bytes=settings.bundle_maximum_bytes,
        url_expiry_seconds=settings.bundle_url_expiry_seconds,
    )

    def flow_now() -> datetime:
        if "authority_api_start" in authority_events:
            return NOW + timedelta(seconds=61)
        return NOW + timedelta(seconds=30)

    app = create_app(
        settings,
        verifier=TaskImagePrincipalVerifier.from_file(settings.principals_file),
        now_factory=flow_now,
        bundle_capability_provider=provider,
    )

    with TestClient(app) as client:
        service, ledger, _peer, _slurm, guard_events = _service(
            tmp_path,
            now_factory=flow_now,
        )
        service._uuid = _phase2c_uuid_factory().__next__  # type: ignore[method-assign]
        _peer.executable_sha256 = SUPERVISOR_SHA256
        service.config = replace(
            service.config,
            slurm=replace(
                service.config.slurm,
                request_sha256=canonical_request_sha256(_policy().request_identity()),
            ),
            containment=replace(
                service.config.containment,
                containment_policy_sha256="4" * 64,
                resource_profile_sha256="5" * 64,
                bpf_program_sha256="7" * 64,
                bpf_map_schema_sha256="8" * 64,
            ),
        )
        service.containment.containment_policy_sha256 = "4" * 64  # type: ignore[attr-defined]
        service.containment.resource_limits_sha256 = "5" * 64  # type: ignore[attr-defined]
        service.containment.bpf_program_sha256 = "7" * 64  # type: ignore[attr-defined]
        service.containment.bpf_map_schema_sha256 = "8" * 64  # type: ignore[attr-defined]
        service.containment.probe_sha256 = "9" * 64  # type: ignore[attr-defined]
        local_build_egress_cgroup = service.containment.build_egress_cgroup  # type: ignore[attr-defined]
        authority_build_egress_cgroup = (
            "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0"
            "/loom-builder/build-egress"
        )
        service.containment.build_egress_cgroup = Path(authority_build_egress_cgroup)  # type: ignore[attr-defined]
        open_directory_capability = service._open_directory_capability

        def mapped_open_directory_capability(self: object, path: str):
            del self
            if str(path) == authority_build_egress_cgroup:
                return open_directory_capability(local_build_egress_cgroup)
            return open_directory_capability(path)

        service._open_directory_capability = MethodType(  # type: ignore[method-assign]
            mapped_open_directory_capability,
            service,
        )
        service.authority = _FastAPIAuthorityAdapter(client, authority_events)  # type: ignore[assignment]

        failure: list[BaseException] = []

        def run_service() -> None:
            try:
                service.start()
            except BaseException as exc:  # pragma: no cover - reported below
                failure.append(exc)

        thread = Thread(target=run_service)
        thread.start()
        deadline = time.monotonic() + 5
        while not service.config.protocol.socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert service.config.protocol.socket_path.exists()

        repo = Path(__file__).resolve().parents[2]
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--pid=host",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-v",
                f"{repo}:/src:ro",
                "-v",
                f"{tmp_path}:{tmp_path}",
                "-e",
                "GOCACHE=/tmp/loom-go-cache",
                "-e",
                "GOMODCACHE=/tmp/loom-go-mod-cache",
                "-e",
                "LOOM_PHASE2C_SOCKET",
                "-e",
                "LOOM_PHASE2C_GRANT_ID",
                "-e",
                "LOOM_PHASE2C_MATERIALIZATION_ID",
                "-e",
                "LOOM_PHASE2C_GOARCH_OVERRIDE",
                "-w",
                "/src",
                "golang:1.23.4-bookworm",
                "go",
                "test",
                "./cmd/loom-task-image-builder-supervisor",
                "-run",
                "^TestSupervisorExternalGuardFlow$",
                "-count=1",
                "-timeout=30s",
                "-v",
            ],
            cwd=repo,
            env={
                **os.environ,
                "LOOM_PHASE2C_SOCKET": str(service.config.protocol.socket_path),
                "LOOM_PHASE2C_GRANT_ID": str(GRANT_ID),
                "LOOM_PHASE2C_MATERIALIZATION_ID": str(materialization_id),
                "LOOM_PHASE2C_GOARCH_OVERRIDE": "arm64",
            },
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

        service.stop()
        thread.join(timeout=5)
        service.close()
        ledger_entry = ledger.get(GRANT_ID)
        ledger_document = None if ledger_entry is None else ledger_entry.document()
        ledger.close()

    assert failure == []
    assert result.returncode == 0, (
        result.stdout
        + f"\nauthority_events={authority_events!r}"
        + f"\nguard_events={guard_events!r}"
        + f"\nledger={ledger_document!r}"
    )
    assert "PASS: TestSupervisorExternalGuardFlow" in result.stdout
    assert "sentinel" not in result.stdout
    assert "X-Amz-Signature" not in result.stdout
    assert {"authority_api_challenge", "authority_api_attach", "authority_api_exchange", "authority_api_renew", "authority_api_claim", "authority_api_bundle", "authority_api_start", "authority_api_release"}.issubset(set(authority_events))
    assert "storage_cleanup" not in guard_events

    engine = create_async_engine(postgres_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            projection = await session.scalar(
                select(TaskImageBuildProjection).where(
                    TaskImageBuildProjection.grant_id == GRANT_ID
                )
            )
            materialization = await session.get(TaskImageMaterialization, materialization_id)
            attempts = (
                await session.execute(
                    select(TaskImageMaterializationAttempt).where(
                        TaskImageMaterializationAttempt.materialization_id == materialization_id
                    )
                )
            ).scalars().all()
            events = (
                await session.execute(
                    select(TaskImageMaterializationOperationEvent).where(
                        TaskImageMaterializationOperationEvent.materialization_id == materialization_id
                    )
                )
            ).scalars().all()
    finally:
        await engine.dispose()

    assert projection is not None
    assert projection.state == "exchanged"
    assert materialization is not None
    assert attempts
    assert {event.operation_type for event in events} >= {"bundle", "start", "release"}
