from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

import loom_capacity_manager.global_execution_witness_publisher as publisher

_AUTHORITY = UUID("00000000-0000-4000-8000-000000000901")


def _settings(tmp_path: Path, **updates: object):
    values: dict[str, object] = {
        "db_url_file": tmp_path / "database-url",
        "expected_authority_incarnation": _AUTHORITY,
        "signing_key_file": tmp_path / "global-execution-signing-key",
        "signing_key_id": "global-capacity-manager-2026-08",
        "kubernetes_api_server": "https://192.168.50.103:6443",
        "kubernetes_token_file": tmp_path / "token",
        "kubernetes_ca_file": tmp_path / "ca.crt",
    }
    values.update(updates)
    return publisher.GlobalExecutionWitnessPublisherSettings.model_validate(values)


@pytest.mark.parametrize(
    "api_server",
    (
        "http://192.168.50.103:6443",
        "https://user@example.test:6443",
        "https://example.test:6443/path",
        "https://127.0.0.1:6443",
        "https://8.8.8.8:6443",
    ),
)
def test_settings_reject_unsafe_kubernetes_api_server(
    tmp_path: Path,
    api_server: str,
) -> None:
    with pytest.raises(ValidationError, match="Kubernetes API server"):
        _settings(tmp_path, kubernetes_api_server=api_server)


@pytest.mark.asyncio
async def test_one_publication_patches_both_pool_exports_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    settings.kubernetes_token_file.write_text("projected-token\n", encoding="utf-8")
    settings.kubernetes_ca_file.write_text("test-ca", encoding="utf-8")
    exports = {
        "gb10": b'{"schema_version":1,"witness":{"pool_id":"gb10"}}\n',
        "oldlab": b'{"schema_version":1,"witness":{"pool_id":"oldlab"}}\n',
    }
    captured: dict[str, Any] = {}

    async def build(_settings: object) -> dict[str, bytes]:
        return exports

    class Response:
        status = 200

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, limit: int) -> bytes:
            assert limit == 256 * 1024 + 1
            return b"{}"

    def open_request(request: object, **kwargs: object) -> Response:
        captured["request"] = request
        captured["kwargs"] = kwargs
        return Response()

    monkeypatch.setattr(publisher, "_build_current_exports", build)
    monkeypatch.setattr(
        publisher.ssl,
        "create_default_context",
        lambda **kwargs: SimpleNamespace(ca=kwargs),
    )
    monkeypatch.setattr(publisher.urllib.request, "urlopen", open_request)

    await publisher.publish_global_execution_witnesses_once(settings)

    request = captured["request"]
    assert request.full_url == (
        "https://192.168.50.103:6443/api/v1/namespaces/loom-dev/"
        "configmaps/loom-global-execution-witness-v1"
    )
    assert request.get_method() == "PATCH"
    assert request.headers == {
        "Authorization": "Bearer projected-token",
        "Content-type": "application/merge-patch+json",
        "Accept": "application/json",
    }
    assert json.loads(request.data) == {
        "data": {
            "gb10.json": exports["gb10"].decode("ascii"),
            "oldlab.json": exports["oldlab"].decode("ascii"),
        }
    }
    assert captured["kwargs"]["timeout"] == 5.0
    assert captured["kwargs"]["context"].ca == {"cadata": "test-ca"}


@pytest.mark.asyncio
async def test_publication_accepts_a_normal_config_map_response_larger_than_one_kib(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    settings.kubernetes_token_file.write_text("projected-token\n", encoding="ascii")
    settings.kubernetes_ca_file.write_text("test-ca", encoding="ascii")
    export = b'{"schema_version":1,"witness":"' + b"x" * 600 + b'"}\n'
    response_body = json.dumps(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "loom-global-execution-witness-v1",
                "namespace": "loom-dev",
                "resourceVersion": "12345",
                "uid": "00000000-0000-0000-0000-000000000123",
            },
            "data": {
                "gb10.json": export.decode("ascii"),
                "oldlab.json": export.decode("ascii"),
            },
        },
        separators=(",", ":"),
    ).encode("ascii")
    assert len(response_body) > 1024
    captured: dict[str, int] = {}

    async def build(_settings: object) -> dict[str, bytes]:
        return {"gb10": export, "oldlab": export}

    class Response:
        status = 200

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, limit: int) -> bytes:
            captured["limit"] = limit
            return response_body[:limit]

    monkeypatch.setattr(publisher, "_build_current_exports", build)
    monkeypatch.setattr(
        publisher.ssl,
        "create_default_context",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(publisher.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    await publisher.publish_global_execution_witnesses_once(settings)

    assert captured["limit"] == 256 * 1024 + 1


def test_patch_rejects_response_larger_than_256_kib(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    settings.kubernetes_token_file.write_text("projected-token\n", encoding="ascii")
    settings.kubernetes_ca_file.write_text("test-ca", encoding="ascii")
    response_body = b"x" * (256 * 1024 + 1)
    captured: dict[str, int] = {}

    class Response:
        status = 200

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, limit: int) -> bytes:
            captured["limit"] = limit
            return response_body[:limit]

    monkeypatch.setattr(
        publisher.ssl,
        "create_default_context",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(publisher.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    with pytest.raises(RuntimeError, match="publication was rejected"):
        publisher._patch_config_map(settings, {"gb10": b"{}\n", "oldlab": b"{}\n"})

    assert captured["limit"] == 256 * 1024 + 1


@pytest.mark.asyncio
async def test_publication_rejects_missing_or_oversized_pool_export(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)

    async def missing(_settings: object) -> dict[str, bytes]:
        return {"gb10": b"{}\n"}

    monkeypatch.setattr(publisher, "_build_current_exports", missing)
    with pytest.raises(ValueError, match="exports are invalid"):
        await publisher.publish_global_execution_witnesses_once(settings)

    async def oversized(_settings: object) -> dict[str, bytes]:
        return {"gb10": b"x" * 65_537, "oldlab": b"{}\n"}

    monkeypatch.setattr(publisher, "_build_current_exports", oversized)
    with pytest.raises(ValueError, match="export is invalid"):
        await publisher.publish_global_execution_witnesses_once(settings)


def test_patch_rejects_oversized_projected_ca_before_network_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    settings.kubernetes_token_file.write_text("projected-token\n", encoding="ascii")
    settings.kubernetes_ca_file.write_bytes(b"x" * 65_537)
    network_called = False

    def open_request(*_args: object, **_kwargs: object) -> None:
        nonlocal network_called
        network_called = True

    monkeypatch.setattr(publisher.urllib.request, "urlopen", open_request)

    with pytest.raises(ValueError, match="projected Kubernetes CA bundle is invalid"):
        publisher._patch_config_map(
            settings,
            {"gb10": b"{}\n", "oldlab": b"{}\n"},
        )

    assert network_called is False


@pytest.mark.asyncio
async def test_periodic_failure_log_never_contains_dependency_exception_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(tmp_path)
    secret = "projected-token-and-witness-payload"

    async def fail_publication(_settings: object) -> None:
        raise RuntimeError(secret)

    async def stop_after_failure(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(
        publisher,
        "publish_global_execution_witnesses_once",
        fail_publication,
    )
    monkeypatch.setattr(publisher.asyncio, "sleep", stop_after_failure)

    with caplog.at_level(logging.ERROR, logger=publisher.__name__):
        with pytest.raises(asyncio.CancelledError):
            await publisher.run_global_execution_witness_publisher(settings)

    assert "global execution witness publication failed safely" in caplog.text
    assert secret not in caplog.text
