from __future__ import annotations

import os
from pathlib import Path

import pytest

from loom_worker.pipeline_codex import (
    OFFICIAL_CODEX_VERSION,
    PipelineCodexContractError,
    RotatingStepJwtReader,
    build_pipeline_codex_process_spec,
)


def test_official_codex_process_has_closed_per_process_environments() -> None:
    spec = build_pipeline_codex_process_spec(
        gateway_responses_url="https://loom-gateway.internal/v1/responses"
    )
    assert spec.codex_version == OFFICIAL_CODEX_VERSION == "0.146.0"
    assert spec.install_script is None
    assert spec.mcp_servers == ("video", "video_demo")
    assert "--strict-config" in spec.argv
    assert set(spec.codex_env) == {
        "HOME",
        "CODEX_HOME",
        "OPENAI_API_KEY",
        "NO_PROXY",
        "LANG",
        "LC_ALL",
        "PATH",
    }
    assert not any(key.startswith("LOOM_") for key in spec.codex_env)
    assert set(spec.shim_env) == {"LOOM_STEP_JWT_FILE", "LOOM_GATEWAY_RESPONSES_URL"}


@pytest.mark.parametrize(
    "url",
    [
        "http://gateway/v1/responses",
        "https://user@gateway/v1/responses",
        "https://gateway/v1/responses?token=x",
        "https://gateway/openai/v1/responses",
    ],
)
def test_gateway_url_fails_closed(url: str) -> None:
    with pytest.raises(PipelineCodexContractError):
        build_pipeline_codex_process_spec(gateway_responses_url=url)


def _install_token(path: Path, value: str) -> None:
    replacement = path.with_name(path.name + ".next")
    replacement.write_text(value, encoding="utf-8")
    replacement.chmod(0o400)
    os.replace(replacement, path)


def test_gateway_shim_rereads_rotated_token_for_every_request(tmp_path: Path) -> None:
    token_path = tmp_path / "step-jwt"
    _install_token(token_path, "loom_step_first")
    reader = RotatingStepJwtReader(
        token_path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    assert reader.read_for_request() == "loom_step_first"
    _install_token(token_path, "loom_step_second")
    assert reader.read_for_request() == "loom_step_second"


def test_gateway_shim_rejects_writable_token(tmp_path: Path) -> None:
    token_path = tmp_path / "step-jwt"
    token_path.write_text("loom_step_unsafe", encoding="utf-8")
    token_path.chmod(0o600)
    reader = RotatingStepJwtReader(
        token_path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    with pytest.raises(PipelineCodexContractError, match="0400"):
        reader.read_for_request()
