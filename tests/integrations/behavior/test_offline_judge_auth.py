from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import jwt
import pytest

from loom.integrations.behavior.offline_runner import (
    OfflineJudgeAuthError,
    OfflineJudgeGatewayShim,
    assert_closed_judge_environments,
    build_pipeline_offline_judge_process_spec,
    terminate_process_group,
)
from loom.integrations.behavior.provider import (
    OFFLINE_JUDGE_STEP_ID,
    PRIMITIVE_STEP_ID,
    PipelineProviderAuthError,
    RotatingPipelineStepJwtReader,
    pipeline_step_jwt_ttl_seconds,
)

DIGEST = "sha256:" + "c" * 64


def _install_judge_token(
    path: Path,
    attempt_id: object,
    step_id: str,
    *,
    marker: str = "initial",
) -> None:
    now = int(time.time())
    token = jwt.encode(
        {
            "iss": "loom-control-plane",
            "sub": "step-session",
            "execution_attempt_id": str(attempt_id),
            "subject_kind": "execution_attempt",
            "step_id": step_id,
            "binding_sha256": DIGEST,
            "marker": marker,
            "iat": now,
            "exp": now + 3900,
            "scopes": ["llm:call"],
        },
        "test-only-signing-key-at-least-32-bytes",
        algorithm="HS256",
    )
    replacement = path.with_name(f".{path.name}.{marker}.next")
    replacement.write_text(f"loom_step_{token}", encoding="utf-8")
    replacement.chmod(0o400)
    os.replace(replacement, path)


@pytest.mark.parametrize("profile", ["codex", "synthetic"])
def test_registered_judge_profiles_have_closed_process_environments(profile: str) -> None:
    spec = build_pipeline_offline_judge_process_spec(
        profile=profile,
        gateway_responses_url="https://loom-gateway.internal/v1/responses",
        attempt_id=uuid4(),
        binding_sha256=DIGEST,
    )
    assert_closed_judge_environments(spec)
    assert spec.new_process_group is True
    assert spec.shim_env["LOOM_STEP_JWT_FILE"] == "/run/loom/step-jwt"
    assert spec.shim_env["LOOM_GATEWAY_RESPONSES_URL"].endswith("/v1/responses")
    assert not {
        "ANTHROPIC_API_KEY",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "XDG_CONFIG_HOME",
    }.intersection(spec.runner_env)


def test_unregistered_profile_and_non_gateway_route_fail_closed() -> None:
    assert pipeline_step_jwt_ttl_seconds(3600) == 3900
    assert pipeline_step_jwt_ttl_seconds(29_900) == 30_000
    with pytest.raises(OfflineJudgeAuthError, match="not registered"):
        build_pipeline_offline_judge_process_spec(
            profile="ambient-agent",
            gateway_responses_url="https://loom-gateway.internal/v1/responses",
            attempt_id=uuid4(),
            binding_sha256=DIGEST,
        )
    with pytest.raises(OfflineJudgeAuthError, match="/v1/responses"):
        build_pipeline_offline_judge_process_spec(
            profile="codex",
            gateway_responses_url="https://provider.example/v1/chat/completions",
            attempt_id=uuid4(),
            binding_sha256=DIGEST,
        )


def test_primitive_token_cannot_satisfy_judge_node(tmp_path: Path) -> None:
    attempt_id = uuid4()
    path = tmp_path / "step-jwt"
    _install_judge_token(path, attempt_id, PRIMITIVE_STEP_ID)
    reader = RotatingPipelineStepJwtReader(
        path,
        attempt_id=attempt_id,
        step_id=OFFLINE_JUDGE_STEP_ID,
        binding_sha256=DIGEST,
    )
    with pytest.raises(PipelineProviderAuthError, match="node subject drift"):
        reader.read_for_request()


def test_judge_shim_strips_client_auth_and_rereads_rotation(tmp_path: Path) -> None:
    attempt_id = uuid4()
    path = tmp_path / "step-jwt"
    _install_judge_token(path, attempt_id, OFFLINE_JUDGE_STEP_ID, marker="first")
    reader = RotatingPipelineStepJwtReader(
        path,
        attempt_id=attempt_id,
        step_id=OFFLINE_JUDGE_STEP_ID,
        binding_sha256=DIGEST,
    )
    shim = OfflineJudgeGatewayShim(
        gateway_responses_url="https://loom-gateway.internal/v1/responses",
        attempt_id=attempt_id,
        binding_sha256=DIGEST,
        token_reader=reader,
    )
    first = shim.headers_for_request(
        {
            "Authorization": "Bearer runner-secret-canary",
            "x-api-key": "runner-key-canary",
            "cookie": "session-canary",
            "accept": "text/event-stream",
        }
    )
    _install_judge_token(path, attempt_id, OFFLINE_JUDGE_STEP_ID, marker="second")
    second = shim.headers_for_request({"Authorization": "Bearer stale"})
    assert first["Authorization"].startswith("Bearer loom_step_")
    assert second["Authorization"].startswith("Bearer loom_step_")
    assert first["Authorization"] != second["Authorization"]
    assert "runner-secret-canary" not in repr(first)
    assert "runner-key-canary" not in repr(first)
    assert "session-canary" not in repr(first)
    assert first["accept"] == "text/event-stream"


@pytest.mark.skipif(not hasattr(os, "killpg"), reason="POSIX process groups required")
def test_process_group_cleanup_reaps_registered_runner() -> None:
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    terminate_process_group(process, grace_seconds=0.1)
    assert process.poll() is not None
