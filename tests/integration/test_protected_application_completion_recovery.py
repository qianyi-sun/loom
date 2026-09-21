"""Real peer loss and SQL recovery through the installed journal-bound composer.

Only disposable PostgreSQL is mutated. Kubernetes credential/configuration and
manager-executable receipts are fixtures, not CNPG/workload retirement evidence.
"""

import os
from dataclasses import replace

import psycopg
import pytest

from loom.application_handoff_completion import complete_application_handoff_database
from loom.application_schema_reference import BUNDLED_APPLICATION_SCHEMA_REVISION
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner
from loom_cli.rollout.operator.staging_mutation_guard import _HEALTH_SQL, MutationGuardEvidence
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_protected_peer_database_connection import (  # noqa: F401
    _peer,
    peer_postgres,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_credential_recovery import _Runner, _sources
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _manager
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


@pytest.fixture(scope="module")
def transfer_postgres(peer_postgres):  # noqa: F811
    return peer_postgres


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)
@pytest.mark.parametrize("interruption", ["closed", "restored", "peer-publication", "login-ack", "live-old-peer", "guard-loss", "quiescence"])
async def test_recovery_completes_without_reclosing_a_restored_database(
    transfer_database, transfer_postgres, tmp_path, monkeypatch, interruption,  # noqa: F811
):
    recover = SubprocessProtectedApplyCommandRunner.recover_and_complete_staging_application_database
    retries = []
    if interruption == "quiescence":
        from loom import application_handoff_completion as completion

        transfer = completion.transfer_application_ownership

        def refuse_once(connection, **kwargs):
            transfer(connection, **kwargs)
            retries.append(1)
            if len(retries) == 1:
                connection.execute("DO $$ BEGIN RAISE EXCEPTION 'private diagnostic' USING ERRCODE='55L01'; END $$")

        monkeypatch.setattr(completion, "transfer_application_ownership", refuse_once)
    password = "ab" * 16
    plan, live = _sources(tmp_path, password=password, schema_revision=BUNDLED_APPLICATION_SCHEMA_REVISION)
    journal = _journal(tmp_path)
    for path in (tmp_path / "state", tmp_path / "state/requests", journal.attempt_root.parent.parent, journal.attempt_root.parent):
        path.chmod(0o700)
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
                   candidate_tree=plan.candidate_tree, generation=evidence.generation)
    with _closed(transfer_database, request=request) as (original_peer, maintenance, database_guard, arguments):
        arguments["password"] = password
        arguments["schema_acl_profile"] = "cnpg-staging"
        values = evidence.to_dict()
        values.pop("schema_version")
        values.pop("evidence_digest")
        values["database_backend_pid"] = database_guard.info.backend_pid
        evidence = MutationGuardEvidence.build(**values)
        if interruption == "restored":
            complete_application_handoff_database(original_peer, maintenance=maintenance, **arguments)
        # The installed recovery owns its maintenance transport. A fixture
        # inspection connection must not survive its cluster-wide retirement.
        maintenance.close()
        if interruption != "live-old-peer":
            original_peer.close()
        if interruption == "guard-loss":
            assert database_guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
        class Runner(_Runner):
            fail_login_ack = interruption == "login-ack"
            def open_staging_peer_maintenance_database(self):
                return _peer(transfer_postgres, "postgres")
            def open_staging_peer_database(self):
                return _peer(transfer_postgres, "loom")
            def complete_staging_application_database(self, *args, **kwargs):
                outcome = SubprocessProtectedApplyCommandRunner.complete_staging_application_database(self, *args, **kwargs)
                if self.fail_login_ack:
                    self.fail_login_ack = False
                    raise RuntimeError("lost final completion acknowledgement")
                return outcome
        runner = Runner(live)
        if interruption == "peer-publication":
            publish = journal.record_application_handoff_replacement
            failed = False
            def interrupt_publish(**kwargs):
                nonlocal failed
                receipt = publish(**kwargs)
                if not failed:
                    failed = True
                    raise RuntimeError("lost peer publication acknowledgement")
                return receipt
            monkeypatch.setattr(journal, "record_application_handoff_replacement", interrupt_publish)
        outcomes = []
        def apply(_):
            journal.retain_application_guard(plan, guard=evidence)
            application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
                                          service_uid=os.getuid(), guard=evidence, acknowledge=True)
            journal.record_application_admission_recovery(
                target=arguments["target"], handoff_backend=arguments["handoff_backend"],
                coordination_guard=arguments["coordination_guard"],
            )
            journal.prepare_application_manager_replacement(identity=_manager())
            journal.begin_application_manager_replacement()
            journal.record_application_manager_replacement(identity=replace(_manager(), executable_inode=101))
            outcomes.append(recover(runner, plan, journal=journal, guard=evidence))
            raise RuntimeError("workloads and CNPG fence remain pending")
        if interruption in {"live-old-peer", "guard-loss"}:
            with pytest.raises(RuntimeError, match=r"surviving or unknown peers|coordination guard"):
                journal.execute(plan, [_component(apply)])
            assert outcomes == []
            assert not list(journal.root.rglob("application-handoff-*-peer.json"))
            with psycopg.connect(transfer_database[0], dbname="postgres", autocommit=True) as inspection:
                assert inspection.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (False,)
            return
        if interruption in {"peer-publication", "login-ack"}:
            with pytest.raises(RuntimeError):
                journal.execute(plan, [_component(apply)])
            assert outcomes == []
        with pytest.raises(RuntimeError, match="workloads and CNPG"):
            journal.execute(plan, [_component(apply)])
        assert len(outcomes) == 1
        if interruption == "quiescence":
            assert len(retries) == 2
        assert outcomes[0].target == arguments["target"]
        assert database_guard.execute(_HEALTH_SQL).fetchone() == (evidence.database_backend_pid, True)
        assert not list(journal.root.rglob("terminal.json"))
        assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id, service_uid=os.getuid())
        with psycopg.connect(transfer_database[0], user="loom", password=password, autocommit=True) as runtime:
            assert runtime.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                runtime.execute("ALTER TABLE public.trials DISABLE TRIGGER ALL")
