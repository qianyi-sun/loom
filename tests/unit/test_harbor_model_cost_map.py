from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "prepare_harbor_model_cost_map", ROOT / "scripts/ops/prepare_harbor_model_cost_map.py"
)
assert spec and spec.loader
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
MODEL_MAP = {
    "known-model": {"litellm_provider": "openai", "input_cost_per_token": 0.000001},
    "known-unpriced": {"litellm_provider": "openai"},
}


@pytest.fixture
def backup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = tmp_path / "litellm/model_prices_and_context_window_backup.json"
    target.parent.mkdir()
    monkeypatch.setattr(
        helper.importlib.metadata,
        "distribution",
        lambda name: SimpleNamespace(version="1.100.1", locate_file=lambda path: tmp_path / path),
    )
    return target


def test_valid_package_backup_never_downloads(
    backup: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = json.dumps(MODEL_MAP).encode()
    backup.write_bytes(payload)

    def unavailable(*args: object, **kwargs: object) -> None:
        pytest.fail("a valid bundled table must not access the network")

    monkeypatch.setattr(helper, "urlopen", unavailable)
    assert "using bundled" in helper.prepare_model_cost_map()
    assert backup.read_bytes() == payload


@pytest.mark.parametrize("old_payload", [None, b"broken json", b"{}", b"[]", b'{"x": 0}'])
def test_missing_or_invalid_backup_uses_exact_installed_release(
    backup: Path, monkeypatch: pytest.MonkeyPatch, old_payload: bytes | None
) -> None:
    if old_payload is not None:
        backup.write_bytes(old_payload)
    calls = []

    def download(url: str, timeout: int) -> io.BytesIO:
        calls.append((url, timeout))
        return io.BytesIO(json.dumps(MODEL_MAP).encode())

    monkeypatch.setattr(helper, "urlopen", download)
    assert "packaged offline" in helper.prepare_model_cost_map()
    assert calls == [
        (
            "https://raw.githubusercontent.com/BerriAI/litellm/v1.100.1/"
            "model_prices_and_context_window.json",
            30,
        )
    ]
    restored = json.loads(backup.read_bytes())
    assert restored == MODEL_MAP
    assert "unknown-model" not in restored
    assert "input_cost_per_token" not in restored["known-unpriced"]
    assert backup.stat().st_mode & 0o777 == 0o644


@pytest.mark.parametrize("failure", [b"not-json", b"{}", b'[{"x": 1}]', "404", "timeout"])
def test_unavailable_release_fails_build_without_empty_or_latest_fallback(
    backup: Path, monkeypatch: pytest.MonkeyPatch, failure: bytes | str
) -> None:
    def download(url: str, timeout: int) -> io.BytesIO:
        if failure == "404":
            raise HTTPError(url, 404, "Not Found", {}, None)
        if failure == "timeout":
            raise TimeoutError("timed out")
        assert isinstance(failure, bytes)
        return io.BytesIO(failure)

    monkeypatch.setattr(helper, "urlopen", download)
    with pytest.raises(RuntimeError, match="matching release snapshot is unavailable or invalid"):
        helper.prepare_model_cost_map()
    assert not backup.exists()
