"""Closed access classification for Pipeline-produced Artifacts."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

ArtifactAccessClass = Literal[
    "team_runtime",
    "authoring_restricted",
    "sanitized_audit",
]


def pipeline_output_access_class(
    artifact_type: str,
    *,
    recipe_name: str | None = None,
    node_key: str | None = None,
    artifact_name: str | None = None,
) -> ArtifactAccessClass:
    """Classify from frozen Pipeline identity, never container metadata.

    The authoring and runtime corpus intentionally share one document schema.
    Their access classes therefore come from the official Recipe node/output
    identity instead of the container-authored document field.
    """

    if artifact_type == "terminalgen_final_audit.v1":
        return "sanitized_audit"
    if (
        artifact_type == "terminalgen_corpus.v1"
        and recipe_name == "terminalgen-authoring"
        and node_key == "package_runtime"
        and artifact_name == "corpus"
    ):
        return "team_runtime"
    if artifact_type.startswith("terminalgen"):
        return "authoring_restricted"
    return "team_runtime"


def artifact_read_allowed(
    access_class: str | None,
    *,
    run_created_by_user_id: UUID | None,
    requesting_user_id: UUID | None,
    requesting_role: str | None,
    platform_admin: bool,
) -> bool:
    """Authorize after the caller has already proved the same-team boundary."""

    normalized = access_class or "team_runtime"
    if normalized in {"team_runtime", "sanitized_audit"}:
        return True
    if normalized != "authoring_restricted":
        return False
    return bool(
        platform_admin
        or requesting_role == "owner"
        or (
            requesting_user_id is not None
            and run_created_by_user_id is not None
            and requesting_user_id == run_created_by_user_id
        )
    )


__all__ = [
    "ArtifactAccessClass",
    "artifact_read_allowed",
    "pipeline_output_access_class",
]
