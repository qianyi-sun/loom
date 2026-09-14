"""Capacity runtime connects exact resources to real PostgreSQL owner retirement.

Configuration observation is injected here; installed configuration/SQL profile
admission and running the actual bootstrap image need separate composition proof.
"""


import psycopg
import pytest
from psycopg import sql

from loom.application_completed_authority import ApplicationGuardOwner, ApplicationOwnerSuccessor
from loom.application_handoff_completion import complete_application_handoff_database
from loom_cli.rollout.operator.protected_application_migration_journal import ApplicationMigrationEvent
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
    _DatabaseState,
)
from tests.integration.test_application_database_admission import _maintenance
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_application_migration_resources import Runner as ResourceRunner
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import _database_component

pytestmark = [pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True),
    pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)]


@pytest.mark.asyncio
async def test_capacity_runtime_retires_original_owner_sessions_and_preserves_runtime(transfer_database, tmp_path, monkeypatch):  # noqa: F811
    from loom_cli.rollout.operator.protected_capacity_bootstrap_runtime import ProtectedCapacityBootstrapRuntime

    plan, source, _ = _database_component(tmp_path, database_state="needs-convergence")
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree, generation=evidence.generation)
    url, previous_owner, bindings = transfer_database
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(sql.SQL("ALTER ROLE {} RENAME TO loom_app_staging_owner").format(sql.Identifier(previous_owner)))
        try:
            with _closed((url, "loom_app_staging_owner", bindings), request=request) as (peer, maintenance, db_guard, args):
                args["schema_acl_profile"] = "cnpg-staging"
                complete_application_handoff_database(peer, maintenance=maintenance, **args)
                oids = {name: peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (name,)).fetchone()[0]
                    for name in bindings if name.startswith("loom_cap_staging_")}
                runtime_oids = {name: oid for name, oid in oids.items() if not name.endswith(("owner", "migrator"))}
                for name in runtime_oids:
                    peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(name)))
                identity = ApplicationOwnerSuccessor("loom_cap_staging_migrator", oids["loom_cap_staging_migrator"],
                    ApplicationGuardOwner("loom_cap_staging_owner", oids["loom_cap_staging_owner"]))
                evidence = type(evidence).build(**{k: v for k, v in evidence.to_dict().items()
                    if k not in {"schema_version", "evidence_digest", "database_backend_pid"}}, database_backend_pid=db_guard.info.backend_pid)
                configured = [False]

                class Runner(ResourceRunner):
                    def open_staging_peer_database(self):
                        return psycopg.connect(url, autocommit=True)
                    def open_staging_peer_maintenance_database(self):
                        return _maintenance(peer)

                runner = Runner()
                base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example.test/loom", lambda: source.seed,
                    application_owner_role="loom_app_staging_owner")

                def state(self, candidate, seed, *, durable_runtime_credentials=True):
                    assert candidate == plan and seed == source.seed
                    assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (True,)
                    if not configured[0]:
                        return _DatabaseState.NEEDS_CONVERGENCE
                    durable = peer.execute("SELECT bool_and(rolvaliduntil='infinity'::timestamptz) FROM pg_authid WHERE rolname=ANY(%s)",
                        ([n for n in runtime_oids if not n.endswith("executor")],)).fetchone()[0]
                    return _DatabaseState.EXACT if durable == durable_runtime_credentials else _DatabaseState.NEEDS_CONVERGENCE
                monkeypatch.setattr(KubernetesProtectedStagingCapacityDatabaseComponent, "_database_state", state)
                with ProtectedCapacityBootstrapRuntime(plan=plan, guard=evidence, target=args["target"],
                        coordination_guard=args["coordination_guard"], runner=runner, template=base._manifest(plan, source.seed),
                        ca_certificate=b"disposable-public-ca" * 8, runtime_password=args["password"],
                        container_registry=base.container_registry, assert_guard=lambda: evidence, assert_inputs=lambda: None,
                        intent_digest="1" * 64, base=base, seed=source.seed, identity=identity, runtime_role_oids=runtime_oids) as runtime:
                    assert runtime.read_revision() == "pending"
                    generation = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=runtime.prepare_generation(1),
                        intent_digest="1" * 64, guard_digest=evidence.evidence_digest, previous_digest="2" * 64)
                    recorded = []
                    runtime.create(generation, recorded.append)
                    assert recorded == [identity.role_oid]
                    runtime.arm(generation, identity.role_oid)
                    resources = runtime.resources(generation)
                    secret = resources.ensure_secret(creation_dispatched=True)
                    job = resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
                    with psycopg.connect(url, user=identity.role_name, password=generation.payload["password"], autocommit=True) as session:
                        session.execute("SET ROLE loom_app_staging_owner")
                        configured[0] = True
                        runtime.release_creator()
                        runtime.begin_retirement(generation, [])
                        runtime.seal(generation, identity.role_oid)
                        resources.delete_job(expected_uid=job.uid)
                        runtime.close(generation, identity.role_oid)
                        runtime.retire(generation, identity.role_oid)
                        with pytest.raises(psycopg.OperationalError):
                            session.execute("SELECT 1")
                    runtime.require_role_retired(generation, identity.role_oid)
                    resources.delete_secret(expected_uid=secret.uid)
                    runtime.reopen(generation, identity.role_oid)
                    assert runtime.read_revision() == "exact"
                    assert runtime.role_exists(generation, identity.role_oid)
                    assert db_guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            admin.execute(sql.SQL("ALTER ROLE loom_app_staging_owner RENAME TO {}").format(sql.Identifier(previous_owner)))
