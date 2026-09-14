"""Capacity runtime connects exact resources to real PostgreSQL owner retirement.

The real-bootstrap cases run the production entrypoint and configuration SQL.
The initial empty-fixture admission and Kubernetes transport are injected; the
installed authority chain and actual container image need separate proof.
"""


import base64
import hashlib
import json
from contextlib import ExitStack
from dataclasses import replace

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
@pytest.mark.parametrize("bootstrap,durable_agent,rebind,interruption", [
    (bootstrap, durable, rebind, interruption)
    for bootstrap, durable, rebind in [(False, False, False), (True, False, False), (True, True, False), (True, True, True)]
    for interruption in (None, "close", "reopen")
] + [(True, True, True, "rebind")])
async def test_capacity_runtime_retires_original_owner_sessions_and_preserves_runtime(transfer_database, tmp_path, monkeypatch, bootstrap, durable_agent, rebind, interruption):  # noqa: F811
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
                try:
                    complete_application_handoff_database(peer, maintenance=maintenance, **args)
                except (psycopg.errors.ObjectNotInPrerequisiteState, RuntimeError) as exc:
                    activity = maintenance.execute("SELECT pid,backend_type,usename,state,wait_event_type "
                        "FROM pg_stat_activity WHERE datname='loom' ORDER BY pid").fetchall()
                    raise AssertionError(f"handoff quiescence refusal; backend inventory={activity!r}") from exc
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
                configured = [rebind]
                provisioner = peer.info.user
                provisioner_password = make_url(url).password
                if rebind:
                    from uuid import UUID, uuid4

                    from loom.personal_dev_capacity_runtime import (
                        ApplicationOwnerBinding,
                        PsycopgPersonalDevCapacityDatabase,
                    )
                    from loom.staging_capacity_database_bootstrap import (
                        _parse_seed,
                        staging_capacity_identity,
                    )
                    from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
                        build_staging_reporter_configuration,
                    )
                    configuration = build_staging_reporter_configuration(plan, source.seed).model_copy(
                        update={"authority_incarnation": UUID("558afea6-2a37-55a1-9f7c-3399695da966")})
                    effective_seed = {**source.seed, "reporter_incarnation": str(configuration.reporter_incarnation)}
                    # Seed only this disposable fixture with the exact unused
                    # legacy authority that the production migration certifies.
                    fixture_database = PsycopgPersonalDevCapacityDatabase(url, application_owner_binding=ApplicationOwnerBinding(
                        database="loom", runtime_role="loom", owner_role="loom_app_staging_owner"))
                    initial = configuration.model_copy(update={"configuration_generation": configuration.configuration_generation - 1,
                        "deployment_generation": configuration.deployment_generation - 1, "candidate_digest": "d" * 64,
                        "candidate_identity": "f" * 40, "candidate_publication_sha256": "d" * 64,
                        "reporter_incarnation": uuid4()})
                    for legacy_configuration in (initial, configuration):
                        effective_seed["reporter_incarnation"] = str(legacy_configuration.reporter_incarnation)
                        await fixture_database.converge_protected(identity=staging_capacity_identity(),
                            credentials=_parse_seed(json.dumps(effective_seed).encode()).credentials,
                            configuration=legacy_configuration)
                    for role in oids:
                        peer.execute(sql.SQL("ALTER ROLE {} VALID UNTIL 'infinity'").format(sql.Identifier(role)))
                    provisioner = "postgres"
                    peer.execute(sql.SQL("CREATE ROLE postgres LOGIN SUPERUSER PASSWORD {}").format(sql.Literal(provisioner_password)))

                lost_rebind = [False]
                class CreationPeer:
                    def __init__(self, connection):
                        self.connection = connection
                    def __getattr__(self, name):
                        return getattr(self.connection, name)
                    def __enter__(self):
                        return self
                    def __exit__(self, *args):
                        self.connection.close()
                    def execute(self, statement, *args):
                        result = self.connection.execute(statement, *args)
                        rendered = statement if isinstance(statement, str) else statement.as_string()
                        if (interruption == "rebind" and not lost_rebind[0]
                                and "ALTER TABLE loom_capacity_guard.authority_state" in rendered):
                            lost_rebind[0] = True
                            raise RuntimeError("lost committed rebind reply")
                        return result
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
                        return CreationPeer(psycopg.connect(url, user=provisioner, password=provisioner_password, autocommit=True))
                    def open_staging_peer_maintenance_database(self):
                        return psycopg.connect(url, user=provisioner, password=provisioner_password, dbname="postgres", autocommit=True)

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
                with ExitStack() as recovery_stack, ProtectedCapacityBootstrapRuntime(plan=plan, guard=evidence, target=args["target"],
                        coordination_guard=args["coordination_guard"], runner=runner, template=base._manifest(plan, source.seed),
                        ca_certificate=b"disposable-public-ca" * 8, runtime_password=args["password"],
                        container_registry=base.container_registry, assert_guard=lambda: evidence, assert_inputs=lambda: None,
                        intent_digest="1" * 64, provisioner_role=provisioner, base=base, seed=source.seed, identity=identity, runtime_role_oids=runtime_oids,
                        initial_database_state=_DatabaseState.AUTHORITY_REBIND_REQUIRED if rebind else _DatabaseState.NEEDS_CONVERGENCE,
                        rebind_sha256=hashlib.sha256(base._legacy_authority_rebind_payload(plan, source.seed)).hexdigest() if rebind else None) as runtime:
                    assert runtime.read_revision() == "pending"
                    generation = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=runtime.prepare_generation(1),
                        intent_digest="1" * 64, guard_digest=evidence.evidence_digest, previous_digest="2" * 64)
                    recorded = []
                    runtime.create(generation, recorded.append)
                    assert recorded == [identity.role_oid]
                    if interruption == "rebind":
                        with pytest.raises(RuntimeError, match="lost committed rebind reply"):
                            runtime.arm(generation, identity.role_oid)
                        previous = runtime
                        previous.__exit__(None, None, None)
                        runtime = recovery_stack.enter_context(replace(previous))
                        runtime.begin_retirement(generation, [])
                        runtime.resources(generation).require_retired()
                        runtime.seal(generation, identity.role_oid)
                        runtime.close(generation, identity.role_oid)
                        runtime.retire(generation, identity.role_oid)
                        runtime.require_role_retired(generation, identity.role_oid)
                        runtime.reopen(generation, identity.role_oid)
                        assert runtime.read_revision() == "exact"
                        assert lost_rebind == [True] and not runner.objects
                        return
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
                        retirement_backend = runtime.begin_retirement(generation, [])
                        runtime.seal(generation, identity.role_oid)
                        resources.delete_job(expected_uid=job.uid)
                        runtime.close(generation, identity.role_oid)
                        if interruption == "close":
                            # Lose all privileged runtime peers after closure,
                            # retaining only the independently supervised guard.
                            previous = runtime
                            previous.__exit__(None, None, None)
                            runtime = recovery_stack.enter_context(replace(previous))
                            retirement_backend = runtime.begin_retirement(generation, [retirement_backend])
                            runtime.seal(generation, identity.role_oid)
                            runtime.close(generation, identity.role_oid)
                        runtime.retire(generation, identity.role_oid)
                        with pytest.raises(psycopg.OperationalError):
                            session.execute("SELECT 1")
                    runtime.require_role_retired(generation, identity.role_oid)
                    resources.delete_secret(expected_uid=secret.uid)
                    runtime.reopen(generation, identity.role_oid)
                    if interruption == "reopen":
                        previous = runtime
                        previous.__exit__(None, None, None)
                        runtime = recovery_stack.enter_context(replace(previous))
                        runtime.begin_retirement(generation, [retirement_backend])
                        runtime.require_role_retired(generation, identity.role_oid)
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
                if rebind:
                    cleanup.execute("DROP ROLE IF EXISTS postgres")
                cleanup.execute(sql.SQL("ALTER ROLE loom_app_staging_owner RENAME TO {}").format(sql.Identifier(previous_owner)))
