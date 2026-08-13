import pytest

from loom_worker.pipeline_codex import (
    OFFICIAL_CODEX_VERSION,
    PipelineCodexContractError,
    build_pipeline_codex_process_spec,
)


def test_codex_adapter_is_pinned_offline_and_server_routed() -> None:
    spec = build_pipeline_codex_process_spec(
        gateway_responses_url="https://gateway.example.com/v1/responses"
    )
    assert spec.codex_version == OFFICIAL_CODEX_VERSION
    assert spec.install_script is None
    assert spec.codex_env["OPENAI_API_KEY"] == "loom-loopback-dummy"
    assert spec.new_process_group is True
    with pytest.raises(PipelineCodexContractError):
        build_pipeline_codex_process_spec(
            gateway_responses_url="https://api.openai.com/v1/responses?key=x"
        )
