from dataclasses import dataclass
from uuid import UUID

from loom.agent.subprocess import _gateway_url_for_adapter


@dataclass(frozen=True)
class _Adapter:
    endpoint_dialect: str
    name: str = "stub"
    supports_os: frozenset[str] = frozenset({"linux"})
    api_key_env: str = "API_KEY"
    base_url_env: str = "BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = None

    def build_invocation(self, *, instruction, workdir, model, env):  # type: ignore[no-untyped-def]
        return ["true"]

    async def capture_events(self, *, exec_handle, step_id: str, trial_id: UUID):  # type: ignore[no-untyped-def]
        if False:
            yield None


def test_openai_subprocess_gateway_url_stays_on_openai_facade() -> None:
    assert (
        _gateway_url_for_adapter(
            "http://host.docker.internal:30443/openai/v1",
            _Adapter(endpoint_dialect="openai_chat"),  # type: ignore[arg-type]
        )
        == "http://host.docker.internal:30443/openai/v1"
    )


def test_openai_subprocess_gateway_url_adds_openai_facade_to_bare_router() -> None:
    assert (
        _gateway_url_for_adapter(
            "http://host.docker.internal:30443",
            _Adapter(endpoint_dialect="openai_responses"),  # type: ignore[arg-type]
        )
        == "http://host.docker.internal:30443/openai/v1"
    )


def test_anthropic_subprocess_gateway_url_uses_sibling_anthropic_facade() -> None:
    assert (
        _gateway_url_for_adapter(
            "http://host.docker.internal:30443/openai/v1",
            _Adapter(endpoint_dialect="anthropic"),  # type: ignore[arg-type]
        )
        == "http://host.docker.internal:30443/anthropic"
    )


def test_anthropic_subprocess_gateway_url_keeps_explicit_anthropic_facade() -> None:
    assert (
        _gateway_url_for_adapter(
            "http://host.docker.internal:30443/anthropic",
            _Adapter(endpoint_dialect="anthropic"),  # type: ignore[arg-type]
        )
        == "http://host.docker.internal:30443/anthropic"
    )


def test_gemini_subprocess_gateway_url_uses_google_facade_root() -> None:
    assert (
        _gateway_url_for_adapter(
            "http://host.docker.internal:30443/openai/v1",
            _Adapter(endpoint_dialect="gemini"),  # type: ignore[arg-type]
        )
        == "http://host.docker.internal:30443/google"
    )
