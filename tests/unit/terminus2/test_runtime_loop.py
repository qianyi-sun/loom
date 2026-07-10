"""Runtime helper unit tests (#744)."""

from __future__ import annotations

from loom.agent.terminus2.runtime import (
    _harbor_model_name,
    _openai_gateway_base,
)
from loom.models.types import ModelSpec


def test_openai_gateway_base_appends_v1() -> None:
    assert _openai_gateway_base("http://gateway/openai") == "http://gateway/openai/v1"
    assert (
        _openai_gateway_base("http://gateway/openai/v1")
        == "http://gateway/openai/v1"
    )


def test_harbor_model_name_prefixes_openai() -> None:
    assert _harbor_model_name(ModelSpec(provider="openai", name="gpt-4")) == (
        "openai/gpt-4"
    )
