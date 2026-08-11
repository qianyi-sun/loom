from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import jwt
import pytest

from loom.integrations.behavior.contracts import BehaviorStage
from loom.integrations.behavior.provider import (
    OFFLINE_JUDGE_STEP_ID,
    PRIMITIVE_STEP_ID,
    PipelineProviderAuthError,
    RotatingPipelineStepJwtReader,
    build_pipeline_anthropic_client,
)
from loom.integrations.behavior.stage_credentials import REGISTERED_STAGE_PROVIDER_MODES
from loom_worker.control_plane_client import (
    ExecutionAttemptClaimHeaders,
    HttpControlPlaneClient,
)
from loom_worker.pipeline_runtime_secret import (
    PipelineStepJwtRotator,
    RuntimeSecretMount,
)

DIGEST = "sha256:" + "a" * 64


def _token(
    attempt_id: UUID,
    *,
    step_id: str = PRIMITIVE_STEP_ID,
    binding_sha256: str | None = DIGEST,
    expires_delta: int = 600,
) -> str:
    now = int(time.time())
    claims = {
        "iss": "loom-control-plane",
        "sub": "step-session",
        "team_id": str(uuid4()),
        "execution_attempt_id": str(attempt_id),
        "subject_kind": "execution_attempt",
        "step_id": step_id,
        "iat": now,
        "exp": now + expires_delta,
        "scopes": ["llm:call"],
    }
    if binding_sha256 is not None:
        claims["binding_sha256"] = binding_sha256
    body = jwt.encode(
        claims,
        "test-only-signing-key-at-least-32-bytes",
        algorithm="HS256",
    )
    return f"loom_step_{body}"


def _install(path: Path, token: str) -> None:
    replacement = path.with_name(f".{path.name}.next")
    replacement.write_text(token, encoding="utf-8")
    replacement.chmod(0o400)
    os.replace(replacement, path)


def test_reader_reopens_rotated_attempt_jwt_and_rejects_cross_node(tmp_path: Path) -> None:
    attempt_id = uuid4()
    token_path = tmp_path / "step-jwt"
    _install(token_path, _token(attempt_id))
    reader = RotatingPipelineStepJwtReader(
        token_path,
        attempt_id=attempt_id,
        step_id=PRIMITIVE_STEP_ID,
        binding_sha256=DIGEST,
    )
    first = reader.read_for_request()
    rotated = _token(attempt_id)
    _install(token_path, rotated)
    assert reader.read_for_request() == rotated
    assert first != ""

    _install(token_path, _token(attempt_id, step_id=OFFLINE_JUDGE_STEP_ID))
    with pytest.raises(PipelineProviderAuthError, match="node subject drift"):
        reader.read_for_request()


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("expired", "expired"),
        ("attempt", "Attempt subject drift"),
        ("binding", "binding drift"),
        ("missing_binding", "binding drift"),
        ("overlong_lifetime", "lifetime drift"),
        ("mode", "0400"),
    ],
)
def test_reader_rejects_invalid_file_before_dispatch(
    tmp_path: Path,
    mutation: str,
    match: str,
) -> None:
    attempt_id = uuid4()
    token_path = tmp_path / "step-jwt"
    token = _token(
        uuid4() if mutation == "attempt" else attempt_id,
        binding_sha256="sha256:" + "b" * 64 if mutation == "binding" else DIGEST,
        expires_delta=(
            -1 if mutation == "expired" else 30_001 if mutation == "overlong_lifetime" else 600
        ),
    )
    if mutation == "missing_binding":
        token = _token(attempt_id, binding_sha256=None)
    _install(token_path, token)
    if mutation == "mode":
        token_path.chmod(0o600)
    reader = RotatingPipelineStepJwtReader(
        token_path,
        attempt_id=attempt_id,
        step_id=PRIMITIVE_STEP_ID,
        binding_sha256=DIGEST,
    )
    with pytest.raises(PipelineProviderAuthError, match=match):
        reader.read_for_request()


def test_public_primitive_constructor_has_one_secret_path_and_route() -> None:
    with pytest.raises(PipelineProviderAuthError, match="/run/loom/step-jwt"):
        build_pipeline_anthropic_client(
            "/home/user/.anthropic/key",
            "https://gateway.internal",
            uuid4(),
            DIGEST,
        )


def test_registered_frame_mop_and_non_provider_stages_have_no_credential_mode() -> None:
    assert REGISTERED_STAGE_PROVIDER_MODES[BehaviorStage.FRAME_AUTHOR] == {"none"}
    assert REGISTERED_STAGE_PROVIDER_MODES[BehaviorStage.ROLLOUT] == {"none"}
    assert REGISTERED_STAGE_PROVIDER_MODES[BehaviorStage.AGGREGATE] == {"none"}
    assert REGISTERED_STAGE_PROVIDER_MODES[BehaviorStage.OFFLINE_JUDGE] == {"offline_judge"}
    assert REGISTERED_STAGE_PROVIDER_MODES[BehaviorStage.RECOVERY] == {
        "none",
        "primitive",
    }
    with pytest.raises(PipelineProviderAuthError, match="exact /v1/messages"):
        build_pipeline_anthropic_client(
            "/run/loom/step-jwt",
            "https://gateway.internal/arbitrary",
            uuid4(),
            DIGEST,
        )


async def test_pipeline_rotator_installs_mode_0400_attempt_token(tmp_path: Path) -> None:
    attempt_id = uuid4()
    uid = os.getuid() or 65_534
    gid = os.getgid() or 65_534
    mount = RuntimeSecretMount(tmp_path / "loom", container_uid=uid, container_gid=gid)
    mount.initialize()
    calls: list[tuple[UUID, str, int]] = []

    async def mint(subject: UUID, step_id: str, ttl: int) -> str:
        calls.append((subject, step_id, ttl))
        return _token(subject, step_id=step_id)

    rotator = PipelineStepJwtRotator(
        attempt_id=attempt_id,
        step_id=PRIMITIVE_STEP_ID,
        ttl_seconds=600,
        secret_mount=mount,
        mint=mint,
    )
    await rotator.start()
    await rotator.stop()
    assert calls == [(attempt_id, PRIMITIVE_STEP_ID, 600)]
    assert mount.verify().mode == 0o400
    assert mount.read_verified().startswith(b"loom_step_")
    mount.teardown()


async def test_attempt_token_mint_keeps_lease_and_worker_bearers_out_of_body() -> None:
    attempt_id = uuid4()
    claim = ExecutionAttemptClaimHeaders(
        claim_id=uuid4(),
        lease_epoch=3,
        lease_token="lease-secret-canary",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/admin/step-tokens"
        assert request.headers["authorization"] == "Bearer worker-secret-canary"
        assert request.headers["x-loom-lease-token"] == "lease-secret-canary"
        assert b"secret-canary" not in request.content
        assert request.content.count(str(attempt_id).encode()) == 1
        return httpx.Response(201, json={"token": "loom_step_returned"})

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://control-plane.internal",
    )
    client = HttpControlPlaneClient(
        base_url="https://control-plane.internal",
        token="worker-secret-canary",
        _client=http,
    )
    try:
        token = await client.mint_execution_attempt_step_token(
            team_id=uuid4(),
            execution_attempt_id=attempt_id,
            step_id=PRIMITIVE_STEP_ID,
            ttl_sec=600,
            claim=claim,
        )
    finally:
        await http.aclose()
    assert token == "loom_step_returned"
