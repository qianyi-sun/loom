"""Create the saved staging owner through the real component journal and SQL."""

import os
from contextlib import contextmanager

import pytest
from psycopg import sql

from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)
@pytest.mark.parametrize("interruption", [None, "receipt", "commit", "foreign-role"])
async def test_staging_owner_creation_recovers_only_its_saved_oid(
    transfer_database, tmp_path, monkeypatch, interruption,  # noqa: F811
):
    from loom_cli.rollout.operator.protected_application_owner_preparation import (
        APPLICATION_OWNER_ROLE,
        prepare_application_owner,
    )

    plan, journal = _setup(tmp_path)
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
                   candidate_tree=plan.candidate_tree, generation=evidence.generation)
    with _closed(transfer_database, request=request) as (peer, maintenance, db_guard, _arguments):
        maintenance.execute("ALTER DATABASE loom ALLOW_CONNECTIONS true")
        evidence = MutationGuardEvidence.build(**{
            k: v for k, v in evidence.to_dict().items()
            if k not in {"schema_version", "evidence_digest", "database_backend_pid"}
        }, database_backend_pid=db_guard.info.backend_pid)
        armed = [True]
        seen = []
        original = journal.record_application_owner_oid

        def record(*args, **kwargs):
            original(*args, **kwargs)
            seen.append(kwargs["role_oid"])
            if interruption == "receipt" and armed[0]:
                armed[0] = False
                raise RuntimeError("receipt persisted before rollback")
        monkeypatch.setattr(journal, "record_application_owner_oid", record)

        class CommitLoss:
            info = peer.info
            def execute(self, statement):
                return peer.execute(statement)
            @contextmanager
            def transaction(self):
                with peer.transaction():
                    yield
                if interruption == "commit" and armed[0] and seen:
                    armed[0] = False
                    raise RuntimeError("owner commit acknowledgement lost")

        outcome = []
        def apply(_):
            journal.retain_application_guard(plan, guard=evidence)
            assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
                                                service_uid=os.getuid(), guard=evidence, acknowledge=True)
            outcome.append(prepare_application_owner(plan, journal=journal, connection=CommitLoss(), guard=evidence))
            raise RuntimeError("owner creation section verified")

        role = sql.Identifier(APPLICATION_OWNER_ROLE)
        try:
            if interruption == "foreign-role":
                peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(role))
                with pytest.raises(RuntimeError, match="unrecorded"):
                    journal.execute(plan, [_component(apply)])
                assert seen == [] and outcome == []
                return
            if interruption:
                with pytest.raises(RuntimeError, match=r"rollback|acknowledgement"):
                    journal.execute(plan, [_component(apply)])
            for _ in range(2):
                with pytest.raises(RuntimeError, match="section verified"):
                    journal.execute(plan, [_component(apply)])
            assert outcome[0] == outcome[1] == seen[-1]
            assert len(seen) == (2 if interruption == "receipt" else 1)
            if interruption == "receipt":
                assert seen[0] != seen[1]
            row = peer.execute(sql.SQL("SELECT oid::bigint,NOT (rolcanlogin OR rolinherit OR rolsuper "
                "OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) AND rolpassword IS NULL "
                "FROM pg_authid WHERE rolname={}").format(sql.Literal(APPLICATION_OWNER_ROLE))).fetchone()
            assert row == (outcome[0], True)
            assert not list(journal.root.rglob("terminal.json"))
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(role))
