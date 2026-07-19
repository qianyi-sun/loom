from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

import pytest
import yaml

from loom_cli.rollout.preflight_kubeconfig_authority import (
    render_token_request_kubeconfig,
    validate_token_request,
    validate_token_request_kubeconfig,
)

NOW = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)


def _token(*, subject: str = "system:serviceaccount:loom-staging:loom-rollout-readonly", **claims) -> str:
    payload = {
        "aud": ["https://kubernetes.default.svc"],
        "exp": int(NOW.timestamp()) + 6 * 60 * 60,
        "iat": int(NOW.timestamp()),
        "sub": subject,
        **claims,
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True).encode()
    ).decode().rstrip("=")
    return f"header.{encoded}.signature"


def _source() -> bytes:
    certificate = base64.b64encode(b"-----BEGIN CERTIFICATE-----\nca\n").decode()
    return yaml.safe_dump(
        {
            "apiVersion": "v1",
            "clusters": [
                {
                    "cluster": {
                        "certificate-authority-data": certificate,
                        "server": "https://127.0.0.1:6443",
                    },
                    "name": "loom-staging",
                }
            ],
            "contexts": [
                {
                    "context": {"cluster": "loom-staging", "user": "root"},
                    "name": "loom-staging",
                }
            ],
            "current-context": "loom-staging",
            "kind": "Config",
            "users": [{"name": "root", "user": {"client-key-data": "secret"}}],
        }
    ).encode()


def test_renders_only_the_bounded_tokenrequest_identity() -> None:
    raw = _token()
    rendered = render_token_request_kubeconfig(
        _source(),
        raw,
        namespace="loom-staging",
        service_account="loom-rollout-readonly",
        now=NOW,
    )
    body = yaml.safe_load(rendered.payload)

    assert body["users"] == [
        {
            "name": "loom-staging-loom-rollout-readonly",
            "user": {"token": raw},
        }
    ]
    assert "client-key-data" not in rendered.payload.decode()
    assert body["contexts"][0]["context"]["namespace"] == "loom-staging"
    assert rendered.evidence.subject.endswith(":loom-rollout-readonly")
    assert len(rendered.evidence.metadata_digest) == 64
    assert raw not in repr(rendered)


@pytest.mark.parametrize(
    "claims,match",
    [
        ({"exp": int(NOW.timestamp()) + 60}, "freshness"),
        ({"exp": int(NOW.timestamp()) + 25 * 60 * 60}, "freshness"),
        ({"aud": []}, "authority"),
        ({"iat": int(NOW.timestamp()) + 120}, "authority"),
    ],
)
def test_rejects_expiry_audience_and_issue_time_drift(
    claims: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        validate_token_request(
            _token(**claims),
            namespace="loom-staging",
            service_account="loom-rollout-readonly",
            now=NOW,
        )


def test_rejects_cross_service_account_token_and_non_minified_source() -> None:
    with pytest.raises(ValueError, match="authority"):
        validate_token_request(
            _token(subject="system:serviceaccount:loom-staging:other"),
            namespace="loom-staging",
            service_account="loom-rollout-readonly",
            now=NOW,
        )
    source = yaml.safe_load(_source())
    source["clusters"].append(source["clusters"][0])
    with pytest.raises(ValueError, match="minified"):
        render_token_request_kubeconfig(
            yaml.safe_dump(source).encode(),
            _token(),
            namespace="loom-staging",
            service_account="loom-rollout-readonly",
            now=NOW,
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body["users"][0]["user"].update({"client-key-data": "root-key"}),
        lambda body: body["clusters"][0]["cluster"].update(
            {"insecure-skip-tls-verify": True}
        ),
        lambda body: body["contexts"][0]["context"].update(
            {"namespace": "loom-staging" + "-other"}
        ),
        lambda body: body.update({"extension": "unexpected"}),
    ],
)
def test_installed_validator_rejects_inherited_or_extended_authority(mutate) -> None:
    rendered = render_token_request_kubeconfig(
        _source(),
        _token(),
        namespace="loom-staging",
        service_account="loom-rollout-readonly",
        now=NOW,
    )
    body = yaml.safe_load(rendered.payload)
    mutate(body)

    with pytest.raises(ValueError, match=r"authority|invalid"):
        validate_token_request_kubeconfig(
            yaml.safe_dump(body).encode(),
            namespace="loom-staging",
            service_account="loom-rollout-readonly",
            now=NOW,
        )
