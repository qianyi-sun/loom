from __future__ import annotations

import ssl
from pathlib import Path

import pytest

from loom_worker import sandbox_link_proxy as proxy

SHA = "a" * 40


def _env() -> dict[str, str]:
    return {
        "LOOM_WORKER_SANDBOX_IDENTITY": "qianyi",
        "LOOM_WORKER_CANDIDATE_SHA": SHA,
        "LOOM_SANDBOX_LINK_CP_UPSTREAM": "https://192.168.50.14:26080",
        "LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM": "https://192.168.50.14:26100",
        "LOOM_SANDBOX_LINK_MINIO_UPSTREAM": "https://192.168.50.14:26900",
    }


def test_proxy_inventory_is_closed_and_compose_private() -> None:
    links = proxy.load_links(_env())

    assert [(link.name, link.local_port, link.upstream_port) for link in links] == [
        ("control-plane", 8080, 26080),
        ("gateway", 9100, 26100),
        ("minio", 9000, 26900),
    ]


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("LOOM_WORKER_CANDIDATE_SHA", "b" * 39),
        ("LOOM_WORKER_SANDBOX_IDENTITY", "unknown"),
        ("LOOM_SANDBOX_LINK_CP_UPSTREAM", "http://192.168.50.14:26080"),
        ("LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM", "https://192.168.50.14:26101"),
        ("LOOM_SANDBOX_LINK_MINIO_UPSTREAM", "https://user@192.168.50.14:26900"),
    ),
)
def test_proxy_rejects_identity_or_upstream_drift(key: str, value: str) -> None:
    env = _env()
    env[key] = value

    with pytest.raises(proxy.SandboxLinkError):
        proxy.load_links(env)


def test_proxy_client_context_is_tls13_only(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeContext:
        minimum_version: ssl.TLSVersion | None = None
        maximum_version: ssl.TLSVersion | None = None

        def load_cert_chain(self, **_: object) -> None:
            return None

    context = FakeContext()
    monkeypatch.setattr(ssl, "create_default_context", lambda **_: context)
    monkeypatch.setattr(
        proxy,
        "_secure_file",
        lambda path, *, private: Path(path),
    )

    built = proxy.build_client_context()

    assert built.minimum_version == ssl.TLSVersion.TLSv1_3
    assert built.maximum_version == ssl.TLSVersion.TLSv1_3
