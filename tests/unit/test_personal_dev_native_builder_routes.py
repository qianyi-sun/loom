from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.personal_dev_builder_runtime import PersonalDevBuildCapability
from loom.personal_dev_candidate import (
    CandidateRegistration,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateRecord,
)
from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_PLATFORM,
    NATIVE_BUILDER_PROVIDER,
    NativeBuilderAgentStatus,
    NativeBuilderCompletion,
    NativeBuilderHeartbeatRequest,
    NativeBuilderPollRequest,
    NativeBuilderRuntimeEvidence,
    PersonalDevNativeBuilderSigner,
    PersonalDevNativeBuilderVerifier,
)
from loom.personal_dev_native_builder_store import (
    NativeBuilderArtifactHead,
    NativeBuilderGrantFencedError,
    NativeBuilderPollResult,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.routes.personal_dev_native_builder import router

_PRIVATE_KEY = bytes.fromhex("11" * 32)
_KEY_ID = "gb10-native-builder-v1"
_AGENT_ID = UUID("10000000-0000-0000-0000-000000000001")
_AGENT_IMAGE = (
    "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64
)
_BUILDER_IMAGE = "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
_CONTRACT = '{"platform":"linux/arm64","schema_version":1}'
_CONTRACT_SHA256 = "6a70281ff4f91c00db4f7c1d0c0dadaead31b7a425fc1fb40dfb2c8a3b4bb714"


def _poll(now: datetime, *, nonce: UUID | None = None) -> NativeBuilderPollRequest:
    return NativeBuilderPollRequest(
        status=NativeBuilderAgentStatus(
            agent_instance_id=_AGENT_ID,
            agent_key_id=_KEY_ID,
            provider=NATIVE_BUILDER_PROVIDER,
            platform=NATIVE_BUILDER_PLATFORM,
            protocol_version=1,
            host_name="gx10-01c7",
            host_architecture="aarch64",
            host_boot_id=UUID("10000000-0000-0000-0000-000000000003"),
            agent_image=_AGENT_IMAGE,
            builder_image=_BUILDER_IMAGE,
            runtime_profile_sha256="c" * 64,
            max_concurrency=2,
            managed_grant_ids=(),
            active_grant_ids=(),
            available=True,
            unavailable_reason=None,
            readiness_evidence_sha256="d" * 64,
        ),
        requested_at=now,
        request_nonce=nonce or uuid4(),
    )


def _registration(now: datetime) -> CandidateRegistration:
    candidate_id = UUID("20000000-0000-0000-0000-000000000001")
    attempt_id = UUID("30000000-0000-0000-0000-000000000001")
    owner_user_id = UUID("40000000-0000-0000-0000-000000000001")
    owner_team_id = UUID("50000000-0000-0000-0000-000000000001")
    candidate_sha = "e" * 64
    return CandidateRegistration(
        candidate=PersonalDevCandidateRecord(
            id=candidate_id,
            owner_user_id=owner_user_id,
            owner_team_id=owner_team_id,
            candidate_sha=candidate_sha,
            source_sha256="f" * 64,
            archive_sha256="1" * 64,
            build_contract_sha256="2" * 64,
            source_commit="3" * 40,
            dirty=True,
            manifest_json={"attestation_scope": "personal-dev-only", "schema_version": 1},
            object_bucket="artifacts",
            object_key=(
                f"personal-dev/sources/{owner_team_id}/{owner_user_id}/{candidate_sha}/"
                f"{candidate_id}/{'1' * 64}.tar"
            ),
            source_generation_id=candidate_id,
            archive_size_bytes=4096,
            status="building",
            created_at=now,
            updated_at=now,
        ),
        build_attempt=PersonalDevCandidateBuildAttemptRecord(
            id=attempt_id,
            candidate_id=candidate_id,
            subject_id=UUID("60000000-0000-0000-0000-000000000001"),
            subject_incarnation=UUID("70000000-0000-0000-0000-000000000001"),
            operation_id=UUID("80000000-0000-0000-0000-000000000001"),
            operation_epoch=3,
            attempt_sequence=2,
            state="running",
            lease_epoch=7,
            claimed_by="personal-builder-test",
            lease_expires_at=now + timedelta(minutes=30),
            created_at=now,
            updated_at=now,
            started_at=now,
        ),
        created=False,
    )


def _grant(registration: CandidateRegistration) -> SimpleNamespace:
    attempt = registration.build_attempt
    assert attempt is not None
    return SimpleNamespace(
        id=UUID("90000000-0000-0000-0000-000000000001"),
        candidate_id=registration.candidate.id,
        attempt_id=attempt.id,
        attempt_lease_epoch=attempt.lease_epoch,
        platform=NATIVE_BUILDER_PLATFORM,
        provider=NATIVE_BUILDER_PROVIDER,
        required_agent_instance_id=_AGENT_ID,
        required_agent_key_id=_KEY_ID,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        contract_json=_CONTRACT,
        contract_sha256=_CONTRACT_SHA256,
        artifact_bucket="artifacts",
        artifact_object_key=(
            f"personal-dev/builds/{registration.candidate.owner_team_id}/"
            f"{registration.candidate.owner_user_id}/{registration.candidate.candidate_sha}/"
            f"{attempt.id}/l{attempt.lease_epoch:016x}/arm64/artifacts.tar"
        ),
        artifact_max_bytes=8 * 1024 * 1024 * 1024,
        active_deadline_seconds=3600,
        state="running",
    )


class _Store:
    def __init__(self) -> None:
        self.poll_result = NativeBuilderPollResult(grant=None, cancel_grant_ids=())
        self.registration: CandidateRegistration | None = None
        self.last_poll_at: datetime | None = None
        self.last_poll_nonce: UUID | None = None
        self.heartbeat_result = True
        self.last_heartbeat_at: datetime | None = None
        self.last_heartbeat_nonce: UUID | None = None
        self.grant: SimpleNamespace | None = None
        self.completed: list[
            tuple[NativeBuilderCompletion, NativeBuilderArtifactHead | None]
        ] = []
        self.fence_completion = False

    async def poll(self, request: NativeBuilderPollRequest, now: datetime):
        if request.request_nonce == self.last_poll_nonce or (
            self.last_poll_at is not None and request.requested_at <= self.last_poll_at
        ):
            raise NativeBuilderGrantFencedError("replayed")
        self.last_poll_at = request.requested_at
        self.last_poll_nonce = request.request_nonce
        return self.poll_result

    async def registration_for_grant(self, _grant: object):
        return self.registration

    async def heartbeat(self, request: NativeBuilderHeartbeatRequest, now: datetime):
        if request.request_nonce == self.last_heartbeat_nonce or (
            self.last_heartbeat_at is not None
            and request.requested_at <= self.last_heartbeat_at
        ):
            raise NativeBuilderGrantFencedError("replayed")
        self.last_heartbeat_at = request.requested_at
        self.last_heartbeat_nonce = request.request_nonce
        return self.heartbeat_result

    async def grant_for_completion(self, completion: NativeBuilderCompletion):
        if self.grant is None or (
            self.grant.id != completion.grant_id
            or self.grant.attempt_id != completion.attempt_id
            or self.grant.attempt_lease_epoch != completion.attempt_lease_epoch
            or self.grant.required_agent_instance_id != completion.agent_instance_id
            or self.grant.required_agent_key_id != completion.agent_key_id
        ):
            return None
        return self.grant

    async def complete(
        self,
        completion: NativeBuilderCompletion,
        now: datetime,
        artifact_head: NativeBuilderArtifactHead | None,
    ):
        if self.fence_completion:
            raise NativeBuilderGrantFencedError("parent lease changed")
        if completion.outcome == "succeeded":
            if self.grant is None or artifact_head is None:
                raise NativeBuilderGrantFencedError("artifact missing")
            expected_metadata = {
                "attestation-scope": "personal-dev-only",
                "build-attempt-id": str(self.grant.attempt_id),
                "build-lease-epoch": str(self.grant.attempt_lease_epoch),
                "candidate-sha256": "e" * 64,
                "platform": NATIVE_BUILDER_PLATFORM,
            }
            if (
                artifact_head.bucket != self.grant.artifact_bucket
                or artifact_head.object_key != self.grant.artifact_object_key
                or dict(artifact_head.metadata) != expected_metadata
            ):
                raise NativeBuilderGrantFencedError("artifact mismatch")
        elif artifact_head is not None:
            raise NativeBuilderGrantFencedError("failed completion has artifact")
        self.completed.append((completion, artifact_head))
        assert self.grant is not None
        self.grant.state = completion.outcome
        return self.grant


class _Capabilities:
    def __init__(self, now: datetime) -> None:
        self.now = now
        self.expiry_seconds = 3700

    async def issue(self, registration: CandidateRegistration, *, platform: str):
        assert registration.build_attempt is not None
        assert platform == NATIVE_BUILDER_PLATFORM
        return PersonalDevBuildCapability(
            source_get_url="https://objects.example.test/artifacts/source?X-Amz-Signature=source",
            artifact_upload_url="https://objects.example.test/artifacts",
            artifact_upload_fields={
                "Content-Type": "application/vnd.loom.personal-dev-build.v1+tar",
                "key": "bound-output",
            },
            artifact_max_bytes=8 * 1024 * 1024 * 1024,
            expires_at=self.now + timedelta(seconds=self.expiry_seconds),
        )


class _Minio:
    def __init__(self, grant: SimpleNamespace | None = None) -> None:
        self.grant = grant
        self.calls: list[dict[str, str]] = []
        self.metadata_override: dict[str, str] | None = None

    def head_object(self, **kwargs: str):
        self.calls.append(kwargs)
        assert self.grant is not None
        metadata = self.metadata_override or {
            "attestation-scope": "personal-dev-only",
            "build-attempt-id": str(self.grant.attempt_id),
            "build-lease-epoch": str(self.grant.attempt_lease_epoch),
            "candidate-sha256": "e" * 64,
            "platform": NATIVE_BUILDER_PLATFORM,
        }
        return {
            "ContentLength": 4096,
            "ContentType": "application/vnd.loom.personal-dev-build.v1+tar",
            "Metadata": metadata,
        }


def _app(store: _Store, now: datetime) -> tuple[FastAPI, SimpleNamespace]:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/internal")
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    app.state.personal_dev_native_builder_verifier = PersonalDevNativeBuilderVerifier(
        keys={_KEY_ID: signer.public_key_bytes(_KEY_ID)}
    )
    session_state = SimpleNamespace(entries=0)

    @asynccontextmanager
    async def sessions():
        session_state.entries += 1
        yield object()

    app.state.session_factory = sessions
    app.state.personal_dev_native_builder_store_factory = lambda _session: store
    app.state.personal_dev_native_builder_capabilities = _Capabilities(now)
    app.state.minio_client = _Minio(store.grant)
    return app, session_state


def _poll_body(request: NativeBuilderPollRequest) -> bytes:
    assert json.loads(request.canonical_bytes())["schema_version"] == 1
    return request.canonical_bytes()


def _heartbeat(now: datetime, *, nonce: UUID | None = None) -> NativeBuilderHeartbeatRequest:
    return NativeBuilderHeartbeatRequest(
        agent_instance_id=_AGENT_ID,
        agent_key_id=_KEY_ID,
        grant_id=UUID("90000000-0000-0000-0000-000000000001"),
        attempt_id=UUID("30000000-0000-0000-0000-000000000001"),
        attempt_lease_epoch=7,
        requested_at=now,
        request_nonce=nonce or uuid4(),
    )


def _signed_headers(signature: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-Loom-Native-Builder-Signature": signature,
    }


def _runtime_evidence(now: datetime) -> NativeBuilderRuntimeEvidence:
    return NativeBuilderRuntimeEvidence(
        agent_instance_id=_AGENT_ID,
        grant_id=UUID("90000000-0000-0000-0000-000000000001"),
        attempt_id=UUID("30000000-0000-0000-0000-000000000001"),
        attempt_lease_epoch=7,
        provider=NATIVE_BUILDER_PROVIDER,
        platform=NATIVE_BUILDER_PLATFORM,
        host_name="gx10-01c7",
        host_architecture="aarch64",
        host_boot_id=UUID("10000000-0000-0000-0000-000000000003"),
        agent_image=_AGENT_IMAGE,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        contract_sha256=_CONTRACT_SHA256,
        runtime_name="runsc-personal-dev-native",
        client_container_id="4" * 64,
        buildkit_container_id="5" * 64,
        network_id="6" * 64,
        client_inspect_sha256="7" * 64,
        buildkit_inspect_sha256="8" * 64,
        network_inspect_sha256="9" * 64,
        client_exit_code=0,
        client_oom_killed=False,
        client_restart_count=0,
        buildkit_restart_count=0,
        buildkit_running=True,
        observed_at=now,
    )


def _completion(
    now: datetime,
    *,
    outcome: str = "succeeded",
    nonce: UUID | None = None,
) -> NativeBuilderCompletion:
    return NativeBuilderCompletion(
        agent_instance_id=_AGENT_ID,
        agent_key_id=_KEY_ID,
        grant_id=UUID("90000000-0000-0000-0000-000000000001"),
        attempt_id=UUID("30000000-0000-0000-0000-000000000001"),
        attempt_lease_epoch=7,
        outcome=outcome,  # type: ignore[arg-type]
        failure_reason=None if outcome == "succeeded" else "client_failed",
        evidence=_runtime_evidence(now) if outcome == "succeeded" else None,
        requested_at=now,
        request_nonce=nonce or uuid4(),
    )


def test_signed_poll_without_work_is_bodyless_and_opens_one_session() -> None:
    now = datetime.now(UTC)
    poll = _poll(now)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    app, sessions = _app(_Store(), now)

    response = TestClient(app).post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=_poll_body(poll),
        headers={
            "Content-Type": "application/json",
            "X-Loom-Native-Builder-Signature": signer.sign_poll(poll),
        },
    )

    assert response.status_code == 204
    assert response.content == b""
    assert sessions.entries == 1


def test_signed_poll_returns_one_exact_public_capability_and_cancellations() -> None:
    now = datetime.now(UTC)
    poll = _poll(now)
    registration = _registration(now)
    grant = _grant(registration)
    cancellation = UUID("90000000-0000-0000-0000-000000000002")
    store = _Store()
    store.poll_result = NativeBuilderPollResult(
        grant=grant,  # type: ignore[arg-type]
        cancel_grant_ids=(cancellation,),
    )
    store.registration = registration
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    app, sessions = _app(store, now)

    response = TestClient(app).post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=_poll_body(poll),
        headers={
            "Content-Type": "application/json",
            "X-Loom-Native-Builder-Signature": signer.sign_poll(poll),
        },
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    assert response.json() == {
        "cancel_grant_ids": [str(cancellation)],
        "grant": {
            "active_deadline_seconds": 3600,
            "agent_instance_id": str(_AGENT_ID),
            "agent_key_id": _KEY_ID,
            "artifact_max_bytes": 8 * 1024 * 1024 * 1024,
            "artifact_upload_fields": {
                "Content-Type": "application/vnd.loom.personal-dev-build.v1+tar",
                "key": "bound-output",
            },
            "artifact_upload_url": "https://objects.example.test/artifacts",
            "attempt_id": str(registration.build_attempt.id),
            "attempt_lease_epoch": 7,
            "builder_image": _BUILDER_IMAGE,
            "candidate_id": str(registration.candidate.id),
            "candidate_sha": "e" * 64,
            "capability_expires_at": (
                now + timedelta(seconds=3700)
            ).isoformat().replace("+00:00", "Z"),
            "contract_json": _CONTRACT,
            "contract_sha256": _CONTRACT_SHA256,
            "grant_id": str(grant.id),
            "platform": NATIVE_BUILDER_PLATFORM,
            "provider": NATIVE_BUILDER_PROVIDER,
            "runtime_profile_sha256": "c" * 64,
            "source_get_url": (
                "https://objects.example.test/artifacts/source?X-Amz-Signature=source"
            ),
        },
    }
    assert sessions.entries == 1


def test_poll_rejects_invalid_stale_and_replayed_requests_without_secret_echo() -> None:
    now = datetime.now(UTC)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    store = _Store()
    app, sessions = _app(store, now)
    client = TestClient(app)
    valid = _poll(now)

    invalid = client.post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=_poll_body(valid),
        headers={
            "Content-Type": "application/json",
            "X-Loom-Native-Builder-Signature": "0" * 128,
        },
    )
    stale = _poll(now - timedelta(seconds=61))
    stale_response = client.post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=_poll_body(stale),
        headers={
            "Content-Type": "application/json",
            "X-Loom-Native-Builder-Signature": signer.sign_poll(stale),
        },
    )
    accepted = client.post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=_poll_body(valid),
        headers={
            "Content-Type": "application/json",
            "X-Loom-Native-Builder-Signature": signer.sign_poll(valid),
        },
    )
    replayed = client.post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=_poll_body(valid),
        headers={
            "Content-Type": "application/json",
            "X-Loom-Native-Builder-Signature": signer.sign_poll(valid),
        },
    )

    assert invalid.status_code == 403
    assert stale_response.status_code == 403
    assert accepted.status_code == 204
    assert replayed.status_code == 409
    assert sessions.entries == 2
    for response in (invalid, stale_response, replayed):
        body = response.text
        assert "X-Amz-Signature" not in body
        assert "0000000000000000" not in body
        assert "replayed" not in body


def test_poll_rejects_noncanonical_or_unknown_json_before_session() -> None:
    now = datetime.now(UTC)
    poll = _poll(now)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    app, sessions = _app(_Store(), now)
    client = TestClient(app)
    body = poll.canonical_bytes()

    noncanonical = client.post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=body.replace(b'"request_nonce":', b'"request_nonce": '),
        headers=_signed_headers(signer.sign_poll(poll)),
    )
    with_extra = json.loads(body)
    with_extra["capability"] = "must-not-be-accepted"
    unknown = client.post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        json=with_extra,
        headers={"X-Loom-Native-Builder-Signature": signer.sign_poll(poll)},
    )

    assert noncanonical.status_code == 403
    assert unknown.status_code == 422
    assert sessions.entries == 0


def test_poll_returns_cancellation_without_issuing_a_grant_capability() -> None:
    now = datetime.now(UTC)
    poll = _poll(now)
    cancellation = UUID("90000000-0000-0000-0000-000000000002")
    store = _Store()
    store.poll_result = NativeBuilderPollResult(
        grant=None,
        cancel_grant_ids=(cancellation,),
    )
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    app, sessions = _app(store, now)

    response = TestClient(app).post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=poll.canonical_bytes(),
        headers=_signed_headers(signer.sign_poll(poll)),
    )

    assert response.status_code == 200
    assert response.json() == {
        "cancel_grant_ids": [str(cancellation)],
        "grant": None,
    }
    assert sessions.entries == 1


def test_poll_rejects_capability_that_cannot_cover_active_deadline() -> None:
    now = datetime.now(UTC)
    poll = _poll(now)
    registration = _registration(now)
    grant = _grant(registration)
    store = _Store()
    store.poll_result = NativeBuilderPollResult(grant=grant, cancel_grant_ids=())  # type: ignore[arg-type]
    store.registration = registration
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    app, sessions = _app(store, now)
    capabilities = app.state.personal_dev_native_builder_capabilities
    capabilities.expiry_seconds = 3659

    response = TestClient(app).post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=poll.canonical_bytes(),
        headers=_signed_headers(signer.sign_poll(poll)),
    )

    assert response.status_code == 503
    assert sessions.entries == 1
    assert "X-Amz-Signature" not in response.text
    assert "objects.example.test" not in response.text


def test_signed_heartbeat_returns_continue_and_cancel_decisions() -> None:
    now = datetime.now(UTC)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    store = _Store()
    app, sessions = _app(store, now)
    client = TestClient(app)
    continuing = _heartbeat(now)

    continue_response = client.post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{continuing.grant_id}/heartbeat",
        content=continuing.canonical_bytes(),
        headers=_signed_headers(signer.sign_heartbeat(continuing)),
    )
    store.heartbeat_result = False
    cancelling = _heartbeat(now + timedelta(seconds=1))
    cancel_response = client.post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{cancelling.grant_id}/heartbeat",
        content=cancelling.canonical_bytes(),
        headers=_signed_headers(signer.sign_heartbeat(cancelling)),
    )

    assert continue_response.status_code == 200
    assert continue_response.json() == {"continue": True}
    assert cancel_response.status_code == 200
    assert cancel_response.json() == {"continue": False}
    assert sessions.entries == 2


def test_heartbeat_authenticates_exact_path_before_session_and_fences_replay() -> None:
    now = datetime.now(UTC)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    store = _Store()
    app, sessions = _app(store, now)
    client = TestClient(app)
    heartbeat = _heartbeat(now)
    signature = signer.sign_heartbeat(heartbeat)
    foreign_grant = UUID("90000000-0000-0000-0000-000000000099")

    wrong_path = client.post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{foreign_grant}/heartbeat",
        content=heartbeat.canonical_bytes(),
        headers=_signed_headers(signature),
    )
    invalid = client.post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{heartbeat.grant_id}/heartbeat",
        content=heartbeat.canonical_bytes(),
        headers=_signed_headers("0" * 128),
    )
    accepted = client.post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{heartbeat.grant_id}/heartbeat",
        content=heartbeat.canonical_bytes(),
        headers=_signed_headers(signature),
    )
    replayed = client.post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{heartbeat.grant_id}/heartbeat",
        content=heartbeat.canonical_bytes(),
        headers=_signed_headers(signature),
    )

    assert wrong_path.status_code == 403
    assert invalid.status_code == 403
    assert accepted.status_code == 200
    assert replayed.status_code == 409
    assert sessions.entries == 2
    assert "replayed" not in replayed.text


def test_success_completion_heads_exact_internal_object_before_acknowledgement() -> None:
    now = datetime.now(UTC)
    registration = _registration(now)
    grant = _grant(registration)
    store = _Store()
    store.grant = grant
    app, sessions = _app(store, now)
    minio = app.state.minio_client
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    completion = _completion(now)

    response = TestClient(app).post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{grant.id}/complete",
        content=completion.canonical_bytes(),
        headers=_signed_headers(signer.sign_completion(completion)),
    )

    assert response.status_code == 200
    assert response.json() == {"accepted": True, "state": "succeeded"}
    assert sessions.entries == 2
    assert minio.calls == [
        {"Bucket": grant.artifact_bucket, "Key": grant.artifact_object_key}
    ]
    assert len(store.completed) == 1
    _, artifact_head = store.completed[0]
    assert artifact_head is not None
    assert artifact_head.size_bytes == 4096
    assert artifact_head.content_type == "application/vnd.loom.personal-dev-build.v1+tar"


def test_failure_completion_never_heads_object_store() -> None:
    now = datetime.now(UTC)
    grant = _grant(_registration(now))
    store = _Store()
    store.grant = grant
    app, sessions = _app(store, now)
    minio = app.state.minio_client
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    completion = _completion(now, outcome="failed")

    response = TestClient(app).post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{grant.id}/complete",
        content=completion.canonical_bytes(),
        headers=_signed_headers(signer.sign_completion(completion)),
    )

    assert response.status_code == 200
    assert response.json() == {"accepted": True, "state": "failed"}
    assert sessions.entries == 1
    assert minio.calls == []
    assert store.completed == [(completion, None)]


def test_success_completion_rejects_object_metadata_drift_and_parent_fencing() -> None:
    now = datetime.now(UTC)
    grant = _grant(_registration(now))
    store = _Store()
    store.grant = grant
    app, sessions = _app(store, now)
    minio = app.state.minio_client
    minio.metadata_override = {
        "attestation-scope": "personal-dev-only",
        "build-attempt-id": str(grant.attempt_id),
        "build-lease-epoch": str(grant.attempt_lease_epoch),
        "candidate-sha256": "e" * 64,
        "platform": "linux/amd64",
    }
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    completion = _completion(now)

    drifted = TestClient(app).post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{grant.id}/complete",
        content=completion.canonical_bytes(),
        headers=_signed_headers(signer.sign_completion(completion)),
    )
    store.fence_completion = True
    fenced_completion = _completion(now + timedelta(seconds=1), outcome="failed")
    fenced = TestClient(app).post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{grant.id}/complete",
        content=fenced_completion.canonical_bytes(),
        headers=_signed_headers(signer.sign_completion(fenced_completion)),
    )

    assert drifted.status_code == 409
    assert fenced.status_code == 409
    assert sessions.entries == 3
    assert "artifact mismatch" not in drifted.text
    assert "parent lease changed" not in fenced.text


def test_completion_authenticates_exact_path_before_any_session() -> None:
    now = datetime.now(UTC)
    grant = _grant(_registration(now))
    store = _Store()
    store.grant = grant
    app, sessions = _app(store, now)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    completion = _completion(now, outcome="failed")
    signature = signer.sign_completion(completion)
    foreign_grant = UUID("90000000-0000-0000-0000-000000000099")

    wrong_path = TestClient(app).post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{foreign_grant}/complete",
        content=completion.canonical_bytes(),
        headers=_signed_headers(signature),
    )
    invalid = TestClient(app).post(
        f"/api/v1/internal/personal-dev/native-builder/grants/{grant.id}/complete",
        content=completion.canonical_bytes(),
        headers=_signed_headers("0" * 128),
    )

    assert wrong_path.status_code == 403
    assert invalid.status_code == 403
    assert sessions.entries == 0


def test_service_app_registers_native_routes_but_keeps_default_storage_inert() -> None:
    app = create_app(
        LoomServiceSettings(
            _env_file=None,
            db_url="postgresql+asyncpg://x@y/z",
            gateway_url="http://gw.test",
            control_plane_url="http://cp.test",
            minio_endpoint="http://minio.test",
            minio_access_key="x",
            minio_secret_key="x",
        )
    )
    client = TestClient(app)
    paths = {
        "/api/v1/internal/personal-dev/native-builder/poll",
        "/api/v1/internal/personal-dev/native-builder/grants/{grant_id}/heartbeat",
        "/api/v1/internal/personal-dev/native-builder/grants/{grant_id}/complete",
    }
    for path in paths:
        resolved = path.replace("{grant_id}", str(uuid4()))
        assert client.post(resolved).status_code != 404
    assert not hasattr(app.state, "personal_dev_native_builder_presign_client")
    assert not hasattr(app.state, "personal_dev_native_builder_capabilities")
