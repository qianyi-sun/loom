"""Pin personal storage layouts to immutable owner/incarnation lifecycle history.

Revision ID: 0141
Revises: 0140
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0141"
down_revision = "0140"
branch_labels = None
depends_on = None

_TABLES = ("dev_instances", "dev_lifecycle_operations")
_IDENTITY = (
    "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
    "OR (candidate_id IS NOT NULL AND capacity_namespace IS NOT NULL "
    "AND capacity_database IS NOT NULL AND capacity_namespace = 'loom-dev-' || name "
    "AND capacity_database = {database})"
)
_LEGACY_DATABASE = "'loom_dev_' || replace(name, '-', '_')"
_DATABASE = (
    "CASE WHEN storage_binding IS NULL THEN " + _LEGACY_DATABASE + " ELSE "
    "'ld_' || replace(name, '-', '_') || '_' || replace(subject_incarnation::text, '-', '') END"
)


def upgrade() -> None:
    for table in _TABLES:
        name = "name" if table == "dev_instances" else "environment_name"
        op.add_column(table, sa.Column("storage_binding", postgresql.JSONB(), nullable=True))
        op.add_column(table, sa.Column("storage_binding_sha256", sa.String(64), nullable=True))
        op.create_check_constraint(f"{table}_storage_binding_check", table, (
            "(storage_binding IS NULL AND storage_binding_sha256 IS NULL) OR (("
            "storage_binding IS NOT NULL AND storage_binding_sha256 ~ '^[0-9a-f]{64}$' "
            "AND storage_binding_sha256 <> repeat('0', 64) "
            "AND storage_binding->>'schema_version' = '1' "
            "AND subject_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND subject_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            "AND owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid "
            f"AND {name} NOT IN ('dev', 'development', 'staging', 'production', "
            "'prod', 'local', 'loom', 'shared', 'default') "
            "AND storage_binding = jsonb_build_object('schema_version', 1, "
            f"'layout', 'incarnation-v1', 'environment_name', {name}, "
            "'subject_id', subject_id::text, 'subject_incarnation', subject_incarnation::text, "
            "'owner_user_id', owner_user_id::text, 'owner_team_id', owner_team_id::text)) IS TRUE)"
        ))
    op.drop_constraint("dev_instances_personal_capacity_identity_check", "dev_instances", type_="check")
    op.create_check_constraint("dev_instances_personal_capacity_identity_check", "dev_instances",
                               _IDENTITY.format(database=_DATABASE))
    op.execute("""
        CREATE FUNCTION loom_guard_personal_storage_binding() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE calculated text; current_storage jsonb;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.storage_binding IS NOT NULL THEN
                    RAISE EXCEPTION 'personal storage history cannot be deleted';
                END IF;
                IF TG_TABLE_NAME = 'dev_lifecycle_operations' THEN
                    -- A locking read follows a concurrent recreation's updated
                    -- row rather than accepting the statement's old NULL image.
                    SELECT environment.storage_binding INTO current_storage
                    FROM dev_instances environment WHERE environment.name = OLD.environment_name
                    FOR UPDATE;
                    IF current_storage IS NOT NULL
                       OR EXISTS (SELECT 1 FROM dev_lifecycle_operations history
                                  WHERE history.environment_name = OLD.environment_name
                                    AND history.storage_binding IS NOT NULL) THEN
                        RAISE EXCEPTION 'personal storage legacy predecessor history cannot be deleted';
                    END IF;
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF (TG_TABLE_NAME = 'dev_lifecycle_operations'
                    OR NEW.subject_incarnation = OLD.subject_incarnation)
                   AND (NEW.storage_binding IS DISTINCT FROM OLD.storage_binding
                        OR NEW.storage_binding_sha256 IS DISTINCT FROM OLD.storage_binding_sha256) THEN
                    RAISE EXCEPTION 'personal storage binding is immutable within an incarnation';
                END IF;
                IF OLD.storage_binding IS NOT NULL AND NEW.storage_binding IS NULL THEN
                    RAISE EXCEPTION 'personal storage layout cannot downgrade to legacy';
                END IF;
                IF TG_TABLE_NAME = 'dev_instances'
                   AND NEW.subject_incarnation IS DISTINCT FROM OLD.subject_incarnation
                   AND (OLD.storage_binding IS NOT NULL OR NEW.storage_binding IS NOT NULL) THEN
                    IF OLD.status <> 'deleted' OR NEW.status <> 'provisioning'
                       OR NEW.operation_epoch <> OLD.operation_epoch + 1
                       OR NEW.operation_id IS NOT DISTINCT FROM OLD.operation_id
                       OR ROW(NEW.name, NEW.subject_id, NEW.owner_user_id, NEW.owner_team_id)
                          IS DISTINCT FROM ROW(OLD.name, OLD.subject_id, OLD.owner_user_id, OLD.owner_team_id)
                       OR NOT EXISTS (
                           SELECT 1 FROM dev_lifecycle_operations retired
                           WHERE retired.id = OLD.operation_id AND retired.kind = 'destroy'
                             AND retired.state = 'succeeded'
                             AND retired.checkpoint IN ('complete', 'pre_activation_abandoned')
                             AND retired.checkpoint = OLD.operation_step
                             AND retired.environment_name = OLD.name
                             AND retired.subject_id = OLD.subject_id
                             AND retired.owner_user_id = OLD.owner_user_id
                             AND retired.owner_team_id = OLD.owner_team_id
                             AND retired.keep_data = OLD.keep_data
                             AND retired.subject_incarnation = OLD.subject_incarnation
                             AND retired.operation_epoch = OLD.operation_epoch
                             AND retired.storage_binding IS NOT DISTINCT FROM OLD.storage_binding
                             AND retired.storage_binding_sha256 IS NOT DISTINCT FROM OLD.storage_binding_sha256
                       )
                       OR EXISTS (
                           SELECT 1 FROM dev_lifecycle_operations history
                           WHERE history.environment_name = NEW.name
                             AND history.subject_incarnation = NEW.subject_incarnation
                             AND history.id IS DISTINCT FROM NEW.operation_id
                       ) THEN
                        RAISE EXCEPTION 'personal storage incarnation requires fresh release-gated recreation';
                    END IF;
                END IF;
            END IF;
            IF NEW.storage_binding IS NOT NULL THEN
                -- Every accepted value is an ASCII scalar under the exact shape
                -- constraint. Explicit lexical ordering matches canonical JSON.
                SELECT encode(sha256(convert_to('{' || string_agg(
                    to_json(key)::text || ':' || value::text, ',' ORDER BY key COLLATE "C"
                ) || '}', 'UTF8')), 'hex') INTO calculated
                FROM jsonb_each(NEW.storage_binding);
                IF calculated IS DISTINCT FROM NEW.storage_binding_sha256 THEN
                    RAISE EXCEPTION 'personal storage binding digest is invalid';
                END IF;
            END IF;
            IF TG_TABLE_NAME = 'dev_lifecycle_operations' THEN
               IF NEW.membership_accepted_operation_id IS NOT NULL
               AND EXISTS (SELECT 1 FROM dev_lifecycle_operations accepted
                           WHERE accepted.id = NEW.membership_accepted_operation_id
                             AND (accepted.storage_binding IS DISTINCT FROM NEW.storage_binding
                               OR accepted.storage_binding_sha256 IS DISTINCT FROM NEW.storage_binding_sha256)) THEN
                RAISE EXCEPTION 'membership successor differs from accepted storage binding';
               END IF;
               IF NEW.membership_predecessor_operation_id IS NOT NULL
               AND EXISTS (SELECT 1 FROM dev_lifecycle_operations previous
                           WHERE previous.id = NEW.membership_predecessor_operation_id
                             AND (previous.storage_binding IS DISTINCT FROM NEW.storage_binding
                               OR previous.storage_binding_sha256 IS DISTINCT FROM NEW.storage_binding_sha256)) THEN
                RAISE EXCEPTION 'membership successor changed retained storage binding';
               END IF;
            END IF;
            RETURN NEW;
        END $$
    """)
    for table in _TABLES:
        op.execute(f"CREATE TRIGGER {table}_storage_binding_guard BEFORE INSERT OR UPDATE OR DELETE ON {table} "
                   "FOR EACH ROW EXECUTE FUNCTION loom_guard_personal_storage_binding()")
    op.execute("""
        CREATE FUNCTION loom_check_personal_storage_handoff() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE environment dev_instances%ROWTYPE; current_operation dev_lifecycle_operations%ROWTYPE;
        BEGIN
            IF TG_TABLE_NAME = 'dev_instances' THEN
                SELECT * INTO environment FROM dev_instances WHERE name = NEW.name;
            ELSE
                SELECT * INTO environment FROM dev_instances WHERE name = NEW.environment_name;
            END IF;
            SELECT * INTO current_operation FROM dev_lifecycle_operations WHERE id = environment.operation_id;
            IF environment.storage_binding IS DISTINCT FROM current_operation.storage_binding
               OR environment.storage_binding_sha256 IS DISTINCT FROM current_operation.storage_binding_sha256 THEN
                RAISE EXCEPTION 'personal storage handoff differs from current environment';
            END IF;
            RETURN NULL;
        END $$
    """)
    for table in _TABLES:
        op.execute(f"CREATE CONSTRAINT TRIGGER {table}_storage_handoff AFTER INSERT OR UPDATE ON {table} "
                   "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION loom_check_personal_storage_handoff()")


def downgrade() -> None:
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM dev_instances WHERE storage_binding IS NOT NULL)
               OR EXISTS (SELECT 1 FROM dev_lifecycle_operations WHERE storage_binding IS NOT NULL) THEN
                RAISE EXCEPTION 'cannot downgrade 0141 with incarnation storage history';
            END IF;
        END $$
    """)
    for table in _TABLES:
        op.execute(f"DROP TRIGGER {table}_storage_handoff ON {table}")
        op.execute(f"DROP TRIGGER {table}_storage_binding_guard ON {table}")
        op.drop_constraint(f"{table}_storage_binding_check", table, type_="check")
    op.execute("DROP FUNCTION loom_check_personal_storage_handoff()")
    op.execute("DROP FUNCTION loom_guard_personal_storage_binding()")
    op.drop_constraint("dev_instances_personal_capacity_identity_check", "dev_instances", type_="check")
    op.create_check_constraint("dev_instances_personal_capacity_identity_check", "dev_instances",
                               _IDENTITY.format(database=_LEGACY_DATABASE))
    for table in _TABLES:
        op.drop_column(table, "storage_binding_sha256")
        op.drop_column(table, "storage_binding")
