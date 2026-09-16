"""Real peer loss and SQL recovery through the installed journal-bound composer.

Only disposable PostgreSQL is mutated. Kubernetes credential/configuration and
manager-executable receipts are fixtures, not CNPG/workload retirement evidence.
"""

import os
from dataclasses import replace

import psycopg
import pytest

from loom.application_handoff_completion import complete_application_handoff_database
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
    plan, live = _sources(tmp_path, password=password, schema_revision="0148/guard_0036")
