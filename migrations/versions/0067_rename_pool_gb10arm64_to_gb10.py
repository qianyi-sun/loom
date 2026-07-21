"""Rename the internal worker-pool identity 'gb10-arm64' to 'gb10'.

The external Slurm world (partition, node names, unit names) is already
'gb10'; only the Loom-internal pool identity string lags behind. This
migration renames the ``pool_name`` value on every table that carries it so
control-plane rows line up with the code after the source rename.

Each UPDATE is guarded so it can never collide with a pre-existing target-name
row on the table's unique/partial index. In practice no 'gb10' rows exist
before this migration (and no 'gb10-arm64' rows after), so the guards are
belt-and-suspenders. See #883.
"""

from __future__ import annotations

from alembic import op

revision = "0067"
down_revision = "0066"
branch_labels = None
depends_on = None


# workers has no (environment, pool_name) unique constraint: a plain rename is
# safe.
_WORKERS_SQL = """
UPDATE workers
   SET pool_name = '{new}'
 WHERE pool_name = '{old}'
"""

# gb10_worker_pool_desired_states + worker_pool_autoscaler_policies are unique
# on (environment, pool_name).
_ENV_POOL_SQL = """
UPDATE {table} p
   SET pool_name = '{new}'
 WHERE p.pool_name = '{old}'
   AND NOT EXISTS (
        SELECT 1 FROM {table} q
         WHERE q.pool_name = '{new}'
           AND q.environment = p.environment
   )
"""

# gb10_worker_node_statuses is unique on (environment, pool_name, hostname).
_ENV_POOL_HOST_SQL = """
UPDATE gb10_worker_node_statuses p
   SET pool_name = '{new}'
 WHERE p.pool_name = '{old}'
   AND NOT EXISTS (
        SELECT 1 FROM gb10_worker_node_statuses q
         WHERE q.pool_name = '{new}'
           AND q.environment = p.environment
           AND q.hostname = p.hostname
   )
"""

# slurm_worker_jobs has a partial unique index over
# (environment, pool_name, nodelist, coalesce(requested_cpus, -1),
#  coalesce(requested_memory_mib, -1), requested_concurrency)
# WHERE state IN ('pending', 'running'). Match every column so the rename
# cannot violate the active-capacity uniqueness.
_SLURM_SQL = """
UPDATE slurm_worker_jobs p
   SET pool_name = '{new}'
 WHERE p.pool_name = '{old}'
   AND NOT EXISTS (
        SELECT 1 FROM slurm_worker_jobs q
         WHERE q.pool_name = '{new}'
           AND q.environment = p.environment
           AND q.nodelist = p.nodelist
           AND coalesce(q.requested_cpus, -1) = coalesce(p.requested_cpus, -1)
           AND coalesce(q.requested_memory_mib, -1)
                 = coalesce(p.requested_memory_mib, -1)
           AND q.requested_concurrency = p.requested_concurrency
           AND q.state IN ('pending', 'running')
           AND p.state IN ('pending', 'running')
   )
"""

_OLD = "gb10-arm64"
_NEW = "gb10"


def _rename(old: str, new: str) -> None:
    op.execute(_WORKERS_SQL.format(old=old, new=new))
    for table in (
        "gb10_worker_pool_desired_states",
        "worker_pool_autoscaler_policies",
    ):
        op.execute(_ENV_POOL_SQL.format(table=table, old=old, new=new))
    op.execute(_ENV_POOL_HOST_SQL.format(old=old, new=new))
    op.execute(_SLURM_SQL.format(old=old, new=new))


def upgrade() -> None:
    _rename(_OLD, _NEW)


def downgrade() -> None:
    _rename(_NEW, _OLD)
