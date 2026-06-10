"""`_parse_model` recognises `hf:` IDs and local weight paths.

Local paths are detected by leading filesystem markers (`/`, `~`, `./`,
`../`) — no `file:` prefix needed. This keeps the surface terse without
losing the disambiguation guarantee that `--model openai/gpt-4` and
`--model ./weights/` parse differently.
"""

from __future__ import annotations

import pytest

from loom_cli.run_cmd import _parse_model


def test_parse_hf_id_returns_hf_provider() -> None:
    spec = _parse_model("hf:meta-llama/Llama-3.1-8B-Instruct")
    assert spec.provider == "hf"
    assert spec.name == "meta-llama/Llama-3.1-8B-Instruct"


def test_parse_absolute_path_returns_file_provider() -> None:
    spec = _parse_model("/data/checkpoints/my-model/")
    assert spec.provider == "file"
    assert spec.name == "/data/checkpoints/my-model/"


def test_parse_home_path_returns_file_provider() -> None:
    spec = _parse_model("~/weights/llama-3-1-8b")
    assert spec.provider == "file"
    assert spec.name == "~/weights/llama-3-1-8b"


def test_parse_relative_dot_path_returns_file_provider() -> None:
    spec = _parse_model("./weights/")
    assert spec.provider == "file"
    assert spec.name == "./weights/"


def test_parse_relative_dotdot_path_returns_file_provider() -> None:
    spec = _parse_model("../weights/")
    assert spec.provider == "file"
    assert spec.name == "../weights/"


def test_parse_classic_provider_slash_name_still_works() -> None:
    spec = _parse_model("anthropic/claude-opus-4-7")
    assert spec.provider == "anthropic"
    assert spec.name == "claude-opus-4-7"


def test_parse_local_three_part_still_works() -> None:
    spec = _parse_model("local/vllm/Llama-3.1-8B")
    assert spec.provider == "local"
    assert spec.name == "vllm/Llama-3.1-8B"


def test_parse_no_slash_no_prefix_errors_with_examples() -> None:
    with pytest.raises(SystemExit, match="hf:<id>"):
        _parse_model("bare-name-no-slash")


def test_parse_hf_id_without_slash_rejected() -> None:
    with pytest.raises(SystemExit, match="must be `<org>/<name>`"):
        _parse_model("hf:no-slash")
