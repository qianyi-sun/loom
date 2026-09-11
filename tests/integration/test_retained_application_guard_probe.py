"""Actual guard loop reads the epoch after PostgreSQL rejects every fresh client."""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from loom_cli.rollout.operator.protected_application_guard_retention import _read_pending_retention
from loom_cli.rollout.operator.protected_apply_journal import ComponentObservation, ComponentState
from loom_cli.rollout.operator.readonly_database_client import _query_callback
from loom_cli.rollout.operator.staging_mutation_guard import (
    MutationGuardManager,
    guard_evidence_path,
    hold_request_guard,
    read_mutation_guard_evidence,
)
from tests.integration.test_application_ownership_transfer import (
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _setup
from tests.loom_cli.rollout.operator.test_staging_mutation_guard import _Cluster, _config


def _until(probe):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail("original guard did not publish expected state")


@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
def test_same_guard_reads_closed_database_and_survives_bounded_select_failure(
    tmp_path,
    transfer_postgres_url,  # noqa: F811
):
    url = transfer_postgres_url.replace("postgresql+psycopg://", "postgresql://")
    database, role, password = "probe_" + uuid4().hex, "reader_" + uuid4().hex, uuid4().hex
    plan, journal = _setup(tmp_path)
    config = _config(tmp_path)
    cluster = _Cluster()
    owner_alive, stop = threading.Event(), threading.Event()
    owner_alive.set()
    connections = []
    complete = False

    with psycopg.connect(url, dbname="postgres", autocommit=True) as maintenance:
        maintenance.execute(
            sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
                sql.Identifier(role),
                sql.Literal(password),
            )
        )
        maintenance.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            with psycopg.connect(url, dbname=database, autocommit=True) as admin:
                admin.execute(
                    "CREATE TABLE staging_mutation_epochs(environment text, namespace text, epoch bigint)"
                )
                admin.execute(
                    "INSERT INTO staging_mutation_epochs VALUES ('staging','loom-staging',7)"
                )
                admin.execute(
                    sql.SQL("GRANT SELECT ON staging_mutation_epochs TO {}").format(
                        sql.Identifier(role)
                    )
                )

                @contextmanager
                def query_context(**_):
                    with psycopg.connect(
                        url,
                        dbname=database,
                        user=role,
                        password=password,
                        autocommit=True,
                        options="-c default_transaction_read_only=on -c statement_timeout=300",
                        row_factory=dict_row,
                    ) as connection:
                        connections.append(connection.info.backend_pid)
                        yield _query_callback(connection)

                def hold():
                    return hold_request_guard(
                        config=config,
                        request_id=plan.request_id,
                        generation="1" * 32,
                        service_uid=os.getuid(),
                        run=cluster,
                        query_context=query_context,
                        resolve_candidate=lambda _: (plan.candidate_sha, plan.candidate_tree),
                        stop_requested=stop.is_set,
                        owner_running=lambda _: owner_alive.is_set(),
                    )

                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(hold)
                    guard = None
                    component = None
                    try:
                        path = guard_evidence_path(config, plan.request_id)
                        _until(lambda: path.exists() or future.done())
                        if future.done():
                            future.result()
                        guard = read_mutation_guard_evidence(path, service_uid=os.getuid())
                        assert guard.database_backend_pid == connections[0]

                        def interrupted(_):
                            journal.retain_application_guard(plan, guard=guard)
                            raise RuntimeError("worker interrupted")

                        component = replace(
                            _component(interrupted),
                            classify=lambda _: ComponentObservation(
                                ComponentState.EXACT if complete else ComponentState.READY,
                                "3" * 64,
                                8,
                            ),
                        )
                        with pytest.raises(RuntimeError, match="worker interrupted"):
                            journal.execute(plan, [component])
                        _until(
                            lambda: (
                                _read_pending_retention(
                                    config.state_root,
                                    request_id=plan.request_id,
                                    service_uid=os.getuid(),
                                ).acknowledged
                            )
                        )
                        owner_alive.clear()
                        admin.execute("UPDATE staging_mutation_epochs SET epoch=8")
                        maintenance.execute(
                            sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                                sql.Identifier(database)
                            )
                        )
                        with pytest.raises(
                            psycopg.OperationalError, match="not currently accepting connections"
                        ):
                            psycopg.connect(url, dbname=database, connect_timeout=2).close()
                        manager = MutationGuardManager(
                            config=config,
                            service_uid=os.getuid(),
                            systemd=SimpleNamespace(
                                show_mutation_guard=lambda _: SimpleNamespace(
                                    is_running=not future.done(),
                                    main_pid=os.getpid(),
                                )
                            ),
                            resolve_candidate=lambda _: (plan.candidate_sha, plan.candidate_tree),
                        )
                        assert manager.observe_retained_epoch(guard) == 8
                        # A SELECT timeout must refuse the probe, preserving the
                        # original healthy backend/lock so a later probe can work.
                        with admin.transaction():
                            admin.execute(
                                "LOCK TABLE staging_mutation_epochs IN ACCESS EXCLUSIVE MODE"
                            )
                            with pytest.raises(RuntimeError, match="probe refused"):
                                manager.observe_retained_epoch(guard)
                        assert manager.observe_retained_epoch(guard) == 8
                        assert connections == [guard.database_backend_pid] and not future.done()
                        assert maintenance.execute(
                            "SELECT datallowconn FROM pg_database WHERE datname=%s",
                            (database,),
                        ).fetchone() == (False,)
                        assert "restore" not in cluster.events and "unlock" not in cluster.events
                    finally:
                        # This test component only controls the test guard's
                        # lifetime; it does not claim full handoff completion.
                        complete = True
                        if component is not None:
                            journal.execute(plan, [component])
                        stop.set()
                    released = future.result(timeout=10)
                    assert released.state == "released"
                    assert released.database_backend_pid == guard.database_backend_pid
        finally:
            maintenance.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
            )
            maintenance.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
