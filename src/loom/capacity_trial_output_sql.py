"""Column-scoped output writes for the non-login protected guard owner."""

from __future__ import annotations

from psycopg import sql


def protected_trial_output_owner_grants(owner: str) -> tuple[sql.Composed, ...]:
    artifact_columns = (
        "lifecycle_authority_id, artifact_type, artifact_schema_version, name, team_id, "
        "batch_id, trial_id, created_by, content_hash, storage, visibility, share_status, "
        "redaction_state, safety_state, blocked_reason, retention, provenance, metadata"
    )
    statements = (
        "GRANT SELECT (trajectory_index, visibility, share_status, source_provenance) ON public.trials TO {}",
        "GRANT UPDATE (trajectory_index) ON public.trials TO {}",
        "GRANT SELECT (visibility, source_provenance) ON public.batches TO {}",
        "GRANT SELECT ON public.artifacts, public.data_lifecycle_authorities, "
        "public.data_lifecycle_objects, public.artifact_lineage_edges TO {}",
        "GRANT UPDATE (id) ON public.data_lifecycle_authorities, public.data_lifecycle_objects TO {}",
        f"GRANT INSERT (id, created_at, {artifact_columns}) ON public.artifacts TO {{}}",
        f"GRANT UPDATE ({artifact_columns}) ON public.artifacts TO {{}}",
        "GRANT INSERT (authority_id, environment, namespace, bucket, object_key, version_id, "
        "content_sha256, size_bytes, created_at, state) ON public.data_lifecycle_objects TO {}",
        "GRANT INSERT (child_artifact_id, parent_artifact_id, relation, metadata), DELETE "
        "ON public.artifact_lineage_edges TO {}",
    )
    return tuple(sql.SQL(statement).format(sql.Identifier(owner)) for statement in statements)
