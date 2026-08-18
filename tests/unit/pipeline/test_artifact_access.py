from __future__ import annotations

from uuid import uuid4

import pytest

from loom.pipeline.artifact_access import (
    artifact_read_allowed,
    pipeline_output_access_class,
)


@pytest.mark.parametrize(
    ("artifact_type", "recipe_name", "node_key", "artifact_name", "expected"),
    [
        (
            "terminalgen_final_audit.v1",
            "terminalgen-authoring",
            "global_finalize",
            "final_audit",
            "sanitized_audit",
        ),
        (
            "terminalgen_corpus.v1",
            "terminalgen-authoring",
            "package_runtime",
            "corpus",
            "team_runtime",
        ),
        (
            "terminalgen_corpus.v1",
            "terminalgen-authoring",
            "package_authoring",
            "corpus",
            "authoring_restricted",
        ),
        (
            "terminalgen_task_bundle.v1",
            "terminalgen-authoring",
            "generate_card_00",
            "task_bundle",
            "authoring_restricted",
        ),
        ("behavior_rollout_bundle.v1", None, None, None, "team_runtime"),
    ],
)
def test_pipeline_output_access_class_is_frozen_by_recipe_output_identity(
    artifact_type: str,
    recipe_name: str | None,
    node_key: str | None,
    artifact_name: str | None,
    expected: str,
) -> None:
    assert (
        pipeline_output_access_class(
            artifact_type,
            recipe_name=recipe_name,
            node_key=node_key,
            artifact_name=artifact_name,
        )
        == expected
    )


def test_restricted_read_is_closed_to_creator_owner_and_platform_admin() -> None:
    creator = uuid4()
    assert artifact_read_allowed(
        "authoring_restricted",
        run_created_by_user_id=creator,
        requesting_user_id=creator,
        requesting_role="member",
        platform_admin=False,
    )
    assert artifact_read_allowed(
        "authoring_restricted",
        run_created_by_user_id=creator,
        requesting_user_id=uuid4(),
        requesting_role="owner",
        platform_admin=False,
    )
    assert artifact_read_allowed(
        "authoring_restricted",
        run_created_by_user_id=creator,
        requesting_user_id=uuid4(),
        requesting_role="platform_admin",
        platform_admin=True,
    )
    assert not artifact_read_allowed(
        "authoring_restricted",
        run_created_by_user_id=creator,
        requesting_user_id=uuid4(),
        requesting_role="member",
        platform_admin=False,
    )
    assert not artifact_read_allowed(
        "future_class",
        run_created_by_user_id=creator,
        requesting_user_id=creator,
        requesting_role="owner",
        platform_admin=True,
    )
