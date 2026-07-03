from __future__ import annotations

from loom.security.redaction import (
    RedactionDecision,
    contains_secret_like_content,
    redact_mapping,
    redact_text,
)


def test_redact_text_covers_staging_secret_shapes() -> None:
    raw = (
        "Authorization: Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345 "
        "Cookie: loom_session=loom_session_secret123; loom_csrf=loom_csrf_abc "
        "invite=loom_invite_invitationsecret "
        "provider=sk-live-super-secret "
        "hf=hf_abcdefghijklmnopqrstuvwxyz1234567890 "
        "signed=https://minio.internal:9000/artifacts/team/trial/out.txt?"
        "X-Amz-Signature=abcdef&X-Amz-Credential=minio%2F20260622 "
        "ref=loom://provider-connection/123e4567-e89b-12d3-a456-426614174000 "
        "internal=http://loom-control-plane:8080/trials"
    )

    redacted = redact_text(raw)

    for leaked in (
        "loom_api_abcdefghijklmnopqrstuvwxyz012345",
        "loom_session_secret123",
        "loom_csrf_abc",
        "loom_invite_invitationsecret",
        "sk-live-super-secret",
        "hf_abcdefghijklmnopqrstuvwxyz1234567890",
        "X-Amz-Signature=abcdef",
        "X-Amz-Credential=minio%2F20260622",
        "loom://provider-connection/123e4567-e89b-12d3-a456-426614174000",
        "http://loom-control-plane:8080/trials",
    ):
        assert leaked not in redacted
    assert "[REDACTED:" in redacted


def test_redact_mapping_preserves_shape_but_redacts_sensitive_values() -> None:
    payload = {
        "status": "failed",
        "headers": {
            "authorization": "Bearer loom_admin_supersecret",
            "x-loom-csrf": "loom_csrf_supersecret",
        },
        "provider": {
            "api_key": "sk-provider-secret",
            "base_url": "https://api.openai.com/v1",
        },
        "errors": [
            {
                "detail": (
                    "upstream 403 from http://loom-llm-gateway:9100/chat "
                    "for key sk-provider-secret"
                ),
            },
        ],
    }

    redacted = redact_mapping(payload)

    assert redacted["status"] == "failed"
    assert redacted["provider"]["base_url"] == "https://api.openai.com/v1"
    assert redacted["headers"]["authorization"] == "[REDACTED:authorization]"
    assert redacted["headers"]["x-loom-csrf"] == "[REDACTED:x-loom-csrf]"
    assert redacted["provider"]["api_key"] == "[REDACTED:api_key]"
    assert "sk-provider-secret" not in str(redacted)
    assert "loom-llm-gateway" not in str(redacted)


def test_contains_secret_like_content_blocks_shared_artifacts() -> None:
    safe = b"reward: 0.82\nsummary: solved with public benchmark data\n"
    unsafe = (
        b"OPENAI_API_KEY=sk-artifact-secret\n"
        b"curl -H 'Authorization: Bearer loom_api_artifactsecret'\n"
    )

    assert contains_secret_like_content(safe) == RedactionDecision(
        status="shared",
        reason=None,
    )
    assert contains_secret_like_content(unsafe) == RedactionDecision(
        status="blocked",
        reason="secret-like content detected",
    )
