"""Capacity runtime connects exact resources to real PostgreSQL owner retirement.

The real-bootstrap cases run the production entrypoint and configuration SQL.
The initial empty-fixture admission and Kubernetes transport are injected; the
installed authority chain and actual container image need separate proof.
"""


import base64
import json

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.application_completed_authority import ApplicationGuardOwner, ApplicationOwnerSuccessor
from loom.application_handoff_completion import complete_application_handoff_database
from loom_cli.rollout.operator.protected_application_migration_journal import (
    ApplicationMigrationEvent,
)
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
from tests.loom_cli.rollout.operator.test_application_migration_resources import (
    Runner as ResourceRunner,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _database_component,
)

pytestmark = [pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True),
    pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)]


@pytest.mark.asyncio
@pytest.mark.parametrize("bootstrap,durable_agent", [(False, False), (True, False), (True, True)])
async def test_capacity_runtime_retires_original_owner_sessions_and_preserves_runtime(transfer_database, tmp_path, monkeypatch, bootstrap, durable_agent):  # noqa: F811
    from loom_cli.rollout.operator.protected_capacity_bootstrap_runtime import (
        ProtectedCapacityBootstrapRuntime,
    )

    plan, source, _ = _database_component(tmp_path, database_state="needs-convergence")
    evidence = _guard(plan)
    request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree, generation=evidence.generation)
    url, previous_owner, bindings = transfer_database
    with psycopg.connect(url, dbname="postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("ALTER ROLE {} RENAME TO loom_app_staging_owner").format(sql.Identifier(previous_owner)))
        admin.close()
        try:
            with _closed((url, "loom_app_staging_owner", bindings), request=request) as (peer, maintenance, db_guard, args):
                args["schema_acl_profile"] = "cnpg-staging"
                complete_application_handoff_database(peer, maintenance=maintenance, **args)
                oids = {name: peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (name,)).fetchone()[0]
                    for name in bindings if name.startswith("loom_cap_staging_")}
                runtime_oids = {name: oid for name, oid in oids.items() if not name.endswith(("owner", "migrator"))}
                for name in runtime_oids:
                    peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(name)))
                if durable_agent:
                    peer.execute(sql.SQL("ALTER ROLE loom_cap_staging_agent LOGIN PASSWORD {} VALID UNTIL 'infinity'").format(
                        sql.Literal(source.seed["agent_database_password"])))
                identity = ApplicationOwnerSuccessor("loom_cap_staging_migrator", oids["loom_cap_staging_migrator"],
                    ApplicationGuardOwner("loom_cap_staging_owner", oids["loom_cap_staging_owner"]))
                evidence = type(evidence).build(**{k: v for k, v in evidence.to_dict().items()
                    if k not in {"schema_version", "evidence_digest", "database_backend_pid"}}, database_backend_pid=db_guard.info.backend_pid)
                configured = [False]

                class Runner(ResourceRunner):
                    def capture_stdout(self, argv, **kwargs):
                        if argv[0] == "kubectl" and "exec" in argv:
                            with psycopg.connect(url) as observer:
                                cursor = observer.execute(argv[-1])
                                while cursor.description is None:
                                    assert cursor.nextset()
                                row = cursor.fetchone()
                                assert row is not None and len(row) == 1
                                value = row[0]
                                return (json.dumps(value) if isinstance(value, (dict, list)) else str(value)).encode()
                        return super().capture_stdout(argv, **kwargs)
                    def open_staging_peer_database(self):
                        return psycopg.connect(url, autocommit=True)
                    def open_staging_peer_maintenance_database(self):
                        return _maintenance(peer)

                runner = Runner()
                base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example.test/loom", lambda: source.seed,
                    application_owner_role="loom_app_staging_owner")

                real_state = KubernetesProtectedStagingCapacityDatabaseComponent._database_state
                def state(self, candidate, seed, *, durable_runtime_credentials=True):
                    assert candidate == plan and seed == source.seed
                    assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (True,)
                    if not configured[0]:
                        return _DatabaseState.NEEDS_CONVERGENCE
                    if bootstrap:
                        return real_state(self, candidate, seed, durable_runtime_credentials=durable_runtime_credentials)
                    durable = peer.execute("SELECT bool_and(rolvaliduntil='infinity'::timestamptz) FROM pg_authid WHERE rolname=ANY(%s)",
                        ([n for n in runtime_oids if not n.endswith("executor")],)).fetchone()[0]
                    return _DatabaseState.EXACT if durable == durable_runtime_credentials else _DatabaseState.NEEDS_CONVERGENCE
                monkeypatch.setattr(KubernetesProtectedStagingCapacityDatabaseComponent, "_database_state", state)
                with ProtectedCapacityBootstrapRuntime(plan=plan, guard=evidence, target=args["target"],
                        coordination_guard=args["coordination_guard"], runner=runner, template=base._manifest(plan, source.seed),
                        ca_certificate=b"disposable-public-ca" * 8, runtime_password=args["password"],
                        container_registry=base.container_registry, assert_guard=lambda: evidence, assert_inputs=lambda: None,
                        intent_digest="1" * 64, provisioner_role=peer.info.user, base=base, seed=source.seed, identity=identity, runtime_role_oids=runtime_oids) as runtime:
                    assert runtime.read_revision() == "pending"
                    generation = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=runtime.prepare_generation(1),
                        intent_digest="1" * 64, guard_digest=evidence.evidence_digest, previous_digest="2" * 64)
                    recorded = []
                    runtime.create(generation, recorded.append)
                    assert recorded == [identity.role_oid]
                    runtime.arm(generation, identity.role_oid)
                    if durable_agent:
                        assert peer.execute("SELECT rolvaliduntil='infinity'::timestamptz FROM pg_authid WHERE rolname='loom_cap_staging_agent'").fetchone() == (True,)
                    resources = runtime.resources(generation)
                    secret = resources.ensure_secret(creation_dispatched=True)
                    job = resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
                    if bootstrap:
                        from loom.personal_dev_capacity_runtime import (
                            PsycopgPersonalDevCapacityDatabase,
                        )
                        from loom.staging_capacity_database_bootstrap import (
                            StagingCapacityDatabaseBootstrapSettings,
                            bootstrap_staging_capacity_database,
                        )
                        data = resources.secret["data"]
                        paths = {}
                        for key in ("seed.json", "reporter-configuration.json", "admin-username", "admin-password", "ca.crt"):
                            path = tmp_path / key
                            path.write_bytes(base64.b64decode(data[key]))
                            paths[key] = path
                        settings = StagingCapacityDatabaseBootstrapSettings(credential_seed_path=paths["seed.json"],
                            reporter_configuration_path=paths["reporter-configuration.json"], admin_username_path=paths["admin-username"],
                            admin_password_path=paths["admin-password"], database_ca_path=paths["ca.crt"])
                        def database_factory(admin_url, **kwargs):
                            local = make_url(url)
                            connection = make_url(admin_url).set(host=local.host, port=local.port, query={})
                            return PsycopgPersonalDevCapacityDatabase(connection.render_as_string(hide_password=False), **kwargs)
                        await bootstrap_staging_capacity_database(settings, database_factory=database_factory)
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
                    if not bootstrap:
                        peer.execute("ALTER ROLE loom_cap_staging_executor RENAME TO saved_capacity_executor")
                        try:
                            peer.execute("CREATE ROLE loom_cap_staging_executor NOLOGIN NOINHERIT")
                            with pytest.raises(RuntimeError, match="saved runtime role identity"):
                                runtime.read_revision()
                        finally:
                            peer.execute("DROP ROLE loom_cap_staging_executor")
                            peer.execute("ALTER ROLE saved_capacity_executor RENAME TO loom_cap_staging_executor")
        finally:
            with psycopg.connect(url, dbname="postgres", autocommit=True) as cleanup:
                cleanup.execute(sql.SQL("ALTER ROLE loom_app_staging_owner RENAME TO {}").format(sql.Identifier(previous_owner)))
