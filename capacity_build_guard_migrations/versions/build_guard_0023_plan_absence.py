"""Distinguish absent plan from revoked or unavailable publication authority.

Revision ID: build_guard_0023
Revises: build_guard_0022
"""

from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0023"
down_revision = "build_guard_0022"
branch_labels = None
depends_on = None
OLD = "IF NOT FOUND THEN RAISE EXCEPTION 'build publication plan is absent'; END IF;"
NEW = "IF NOT FOUND THEN RAISE EXCEPTION 'build publication plan is absent' USING ERRCODE='P0002'; END IF;"


def upgrade():
    _replace_clause("authorize_publication(uuid,uuid)", OLD, NEW)


def downgrade():
    _replace_clause("authorize_publication(uuid,uuid)", NEW, OLD)
