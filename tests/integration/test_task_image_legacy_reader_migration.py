"""The installed protected claim function must keep both V1 readers closed."""

from sqlalchemy import create_engine, text

FUNCTION = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"


def test_installed_protected_reader_fences_both_native_selection_boundaries(capacity_guard_database):
    engine = create_engine(capacity_guard_database["admin_url"])
    try:
        with engine.connect() as connection:
            function = connection.execute(text(
                "SELECT pg_get_functiondef(oid) AS definition, "
                "pg_get_userbyid(proowner) AS owner, prosecdef, proconfig, "
                "has_column_privilege(proowner, 'public.task_image_materializations', "
                "'ready_publication_operation_id', 'SELECT') AS can_read_native_identity "
                "FROM pg_proc WHERE oid = CAST(:function AS regprocedure)"
            ), {"function": FUNCTION}).mappings().one()
            assert function["definition"].count(
                "materialization.ready_publication_operation_id IS NULL"
            ) == 2, "candidate and locked V1 snapshot must each exclude native publication"
            assert function["can_read_native_identity"] is True
            assert function["owner"] == capacity_guard_database["owner_role"]
            assert function["prosecdef"] is True
            assert function["proconfig"] == ["search_path=pg_catalog"]
    finally:
        engine.dispose()
