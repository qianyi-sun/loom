import pytest
from pydantic import ValidationError

from loom.pipeline.renderers.core_fixture_aggregate import render, request_digest

D0 = "sha256:" + "0" * 64
D1 = "sha256:" + "1" * 64


def _request() -> dict[str, object]:
    return {
        "schema_version": "loom.pipeline-core-aggregate-request.v1",
        "transforms": [
            {
                "shard_key": "item-000",
                "artifact_id": "artifact-0",
                "content_sha256": D0,
                "manifest_sha256": D1,
            },
            {
                "shard_key": "item-001",
                "artifact_id": "artifact-1",
                "content_sha256": D1,
                "manifest_sha256": D0,
            },
        ],
    }


def test_terminal_renderer_is_canonical_and_order_bound() -> None:
    value = _request()
    rendered = render(value)
    assert rendered.endswith(b"\n")
    assert request_digest(value).startswith("sha256:")
    reversed_value = {**value, "transforms": list(reversed(value["transforms"]))}
    with pytest.raises(ValidationError, match="canonical shard order"):
        render(reversed_value)
