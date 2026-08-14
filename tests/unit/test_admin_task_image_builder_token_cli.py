from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from loom_cli import admin_cmd


def test_mint_task_image_builder_token_uses_dedicated_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    class _Response:
        status_code = 201
        text = ""

        def json(self) -> dict[str, str]:
            return {
                "token": "loom_tib_secret",
                "token_hash_prefix": "ab12cd34",
            }

    def post(url: str, **kwargs: Any) -> _Response:
        captured["url"] = url
        captured.update(kwargs)
        return _Response()

    monkeypatch.setattr(admin_cmd.httpx, "post", post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")
    args = SimpleNamespace(
        cp_url="http://cp:8080/",
        admin_token="env:LOOM_ADMIN_TOKEN",
        expires_in_days=30,
        format="json",
        show_secret=False,
        kind="task-image-builder",
    )

    assert admin_cmd._mint_worker_token(args) == 0

    assert captured["url"] == "http://cp:8080/admin/task-image-builder-tokens"
    assert captured["json"] == {"expires_in_days": 30}
    assert json.loads(capsys.readouterr().out)["token"] == "loom_tib_secret"


def test_mint_task_image_registry_gc_token_uses_dedicated_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    class _Response:
        status_code = 201
        text = ""

        def json(self) -> dict[str, str]:
            return {
                "token": "loom_tigc_secret",
                "token_hash_prefix": "ab12cd34",
            }

    def post(url: str, **kwargs: Any) -> _Response:
        captured["url"] = url
        captured.update(kwargs)
        return _Response()

    monkeypatch.setattr(admin_cmd.httpx, "post", post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")
    args = SimpleNamespace(
        cp_url="http://cp:8080/",
        admin_token="env:LOOM_ADMIN_TOKEN",
        expires_in_days=30,
        format="json",
        show_secret=False,
        kind="task-image-registry-gc",
    )

    assert admin_cmd._mint_worker_token(args) == 0

    assert captured["url"] == "http://cp:8080/admin/task-image-registry-gc-tokens"
    assert captured["json"] == {"expires_in_days": 30}
    assert json.loads(capsys.readouterr().out)["token"] == "loom_tigc_secret"
