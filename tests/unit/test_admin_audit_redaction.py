from __future__ import annotations

import pytest

from loom_service.admin_audit import write_admin_audit_event


class _FakeSession:
    def add(self, _row: object) -> None:
        raise AssertionError("unsafe audit metadata should be rejected before add")


@pytest.mark.parametrize(
    "metadata",
    [
        {
            "artifact_url": (
                "https://minio.internal:9000/artifacts/a/b?"
                "X-Amz-Signature=abcdef"
            ),
        },
        {"provider_ref": "loom://provider-connection/123e4567-e89b-12d3-a456-426614174000"},
        {"internal": {"url": "http://loom-control-plane:8080/trials"}},
    ],
)
async def test_admin_audit_rejects_public_beta_secret_shapes(
    metadata: dict[str, object],
) -> None:
    with pytest.raises(
        ValueError,
        match="admin audit metadata contains secret-looking value",
    ):
        await write_admin_audit_event(
            _FakeSession(),  # type: ignore[arg-type]
            actor="ops",
            action="unsafe",
            target_type="test",
            target_id="target",
            metadata=metadata,
        )
