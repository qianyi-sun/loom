from __future__ import annotations

from datetime import UTC, datetime

import pytest

from loom_cli.rollout.operator.rollout_checkpoint import (
    ImmutableObjectInventory,
    ImmutableObjectReference,
    build_immutable_inventory,
)

NOW = datetime(2026, 7, 19, 22, tzinfo=UTC)


def _reference(key: str, *, data_class: str = "benchmark") -> ImmutableObjectReference:
    return ImmutableObjectReference(
        bucket="loom-artifacts",
        object_key=key,
        version_id=f"version-{key}",
        content_sha256="a" * 64,
        size_bytes=123,
        data_class=data_class,
        authoritative_source="catalog:sha256:" + "b" * 64,
    )


def test_inventory_is_sorted_unique_and_content_addressed() -> None:
    inventory = build_immutable_inventory(
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=9,
        schema_revision="0066",
        created_at=NOW,
        objects=[_reference("z"), _reference("a", data_class="catalog")],
    )

    assert [item.object_key for item in inventory.objects] == ["a", "z"]
    assert len(inventory.inventory_root) == 64
    assert ImmutableObjectInventory.from_dict(inventory.to_dict()) == inventory


@pytest.mark.parametrize("data_class", ["run", "trial", "event", "artifact"])
def test_ephemeral_execution_payloads_cannot_enter_rollout_checkpoint(
    data_class: str,
) -> None:
    with pytest.raises(ValueError, match="immutable object"):
        _reference("run-output", data_class=data_class)


def test_inventory_rejects_duplicate_or_cross_environment_authority() -> None:
    duplicate = _reference("same")
    with pytest.raises(ValueError, match="sorted and unique"):
        ImmutableObjectInventory(
            environment="staging",
            namespace="loom-staging",
            mutation_epoch=1,
            schema_revision="0066",
            created_at=NOW,
            objects=(duplicate, duplicate),
        )
    with pytest.raises(ValueError, match="inventory identity"):
        build_immutable_inventory(
            environment="production",
            namespace="loom-staging",
            mutation_epoch=1,
            schema_revision="0066",
            created_at=NOW,
            objects=[],
        )
