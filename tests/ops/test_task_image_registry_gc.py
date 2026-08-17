from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ops/task_image_registry_gc_once.py"


@pytest.fixture
def module():
    name = "task_image_registry_gc_once_test"
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    spec.loader.exec_module(loaded)
    try:
        yield loaded
    finally:
        sys.modules.pop(name, None)


class _Transport:
    def __init__(self, module: Any, responses: list[tuple[int, object]]) -> None:
        self.module = module
        self.responses = list(responses)
        self.requests: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        ca_file: Path | None,
    ):
        self.requests.append((method, url, headers, body))
        status, payload = self.responses.pop(0)
        raw = (
            payload
            if isinstance(payload, bytes)
            else self.module.json.dumps(payload).encode("utf-8")
        )
        return self.module.HttpResponse(status=status, body=raw)


def _config(module: Any, tmp_path: Path):
    return module.GcConfig(
        cp_url="http://127.0.0.1:18081",
        cp_token="gc-control-plane-token",
        registry_url="https://192.168.50.103:5443",
        registry_namespace="loom-trial-cache",
        registry_authorization="Basic Z2M6c2VjcmV0",
        ca_file=tmp_path / "ca.crt",
    )


def test_gc_deletes_every_claimed_manifest_before_completing(
    module: Any,
    tmp_path: Path,
) -> None:
    digest_a = "sha256:" + "a" * 64
    digest_b = "sha256:" + "b" * 64
    claim = {
        "id": "e1852682-a565-42a8-983f-80dc210dad5a",
        "lease_epoch": 7,
        "registry_images": {
            "task": f"192.168.50.103:5443/loom-trial-cache@{digest_a}",
            "sidecar:db": f"192.168.50.103:5443/loom-trial-cache@{digest_b}",
        },
    }
    transport = _Transport(
        module,
        [(200, claim), (202, b""), (404, b""), (200, {"state": "retired"})],
    )

    result = module.run_gc_once(
        _config(module, tmp_path),
        transport=transport,
        gc_id="registry-gc-test",
    )

    assert result == {"claimed": True, "deleted_manifests": 2}
    assert [request[:2] for request in transport.requests] == [
        (
            "POST",
            "http://127.0.0.1:18081/api/v1/internal/"
            "task-image-materializations/registry-gc/claim",
        ),
        (
            "DELETE",
            "https://192.168.50.103:5443/v2/loom-trial-cache/manifests/" + digest_a,
        ),
        (
            "DELETE",
            "https://192.168.50.103:5443/v2/loom-trial-cache/manifests/" + digest_b,
        ),
        (
            "POST",
            "http://127.0.0.1:18081/api/v1/internal/task-image-materializations/"
            "registry-gc/e1852682-a565-42a8-983f-80dc210dad5a/complete",
        ),
    ]
    assert transport.requests[0][2]["Authorization"] == "Bearer gc-control-plane-token"
    assert transport.requests[1][2]["Authorization"] == "Basic Z2M6c2VjcmV0"
    assert transport.requests[-1][2]["Authorization"] == "Bearer gc-control-plane-token"


def test_gc_no_claim_is_a_successful_noop(module: Any, tmp_path: Path) -> None:
    transport = _Transport(module, [(204, b"")])

    result = module.run_gc_once(
        _config(module, tmp_path),
        transport=transport,
        gc_id="registry-gc-empty",
    )

    assert result == {"claimed": False, "deleted_manifests": 0}
    assert len(transport.requests) == 1


def test_gc_rejects_foreign_registry_reference_before_delete_or_complete(
    module: Any,
    tmp_path: Path,
) -> None:
    claim = {
        "id": "e1852682-a565-42a8-983f-80dc210dad5a",
        "lease_epoch": 2,
        "registry_images": {
            "task": "foreign.example/loom-trial-cache@sha256:" + "c" * 64,
        },
    }
    transport = _Transport(module, [(200, claim)])

    with pytest.raises(module.RegistryGcError, match="outside the configured registry"):
        module.run_gc_once(
            _config(module, tmp_path),
            transport=transport,
            gc_id="registry-gc-foreign",
        )

    assert len(transport.requests) == 1


def test_gc_does_not_complete_when_registry_delete_fails(
    module: Any,
    tmp_path: Path,
) -> None:
    claim = {
        "id": "e1852682-a565-42a8-983f-80dc210dad5a",
        "lease_epoch": 3,
        "registry_images": {
            "task": "192.168.50.103:5443/loom-trial-cache@sha256:" + "d" * 64,
        },
    }
    transport = _Transport(module, [(200, claim), (503, {"errors": []})])

    with pytest.raises(module.RegistryGcError, match="manifest deletion failed"):
        module.run_gc_once(
            _config(module, tmp_path),
            transport=transport,
            gc_id="registry-gc-failure",
        )

    assert len(transport.requests) == 2
