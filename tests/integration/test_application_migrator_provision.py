"""Bounded application DDL credentials on disposable PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_handoff_completion import complete_application_handoff_database
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.mark.asyncio
async def test_application_migrator_journals_oid_before_login_and_retires_surviving_owner_session(transfer_database):  # noqa: F811
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        name, password = "app_migrator_" + uuid4().hex, uuid4().hex
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        recorded = []
        def persist(identity):
            assert peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (identity.role_oid,)).fetchone() == (False, None)
            recorded.append(identity)
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=persist)
        assert recorded == [identity]
        expires = datetime.now(UTC) + timedelta(minutes=45)
        try:
            for _ in range(2):
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expires)
            with psycopg.connect(transfer_database[0], user=name, password=password, autocommit=True) as active:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    active.execute("CREATE TABLE public.app_migrator_probe(id int)")
                active.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(target.successor_role)))
                active.execute("CREATE TABLE public.app_migrator_probe(id int)")
                for _ in range(2):
                    seal_application_migrator(peer, **authority, identity=identity)
                assert active.execute("SELECT current_user").fetchone() == (target.successor_role,)
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
                retire_application_migrator(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)
                with pytest.raises(psycopg.OperationalError):
                    active.execute("SELECT 1")
            assert peer.execute("SELECT relowner::bigint FROM pg_class WHERE relname='app_migrator_probe'").fetchone() == (target.successor_oid,)
            assert guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["closed", "expired", "long-lease", "oid", "credential", "expiry", "membership", "role-setting"])
async def test_migrator_arming_refuses_changed_admission_or_saved_authority(transfer_database, drift):  # noqa: F811
    from dataclasses import replace

    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        name, password = "app_migrator_" + uuid4().hex, uuid4().hex
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=lambda _: None)
        expiry = datetime.now(UTC) + timedelta(minutes=45)
        try:
            if drift in {"credential", "expiry"}:
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expiry)
            if drift == "closed":
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
            elif drift == "expired":
                expiry = datetime.now(UTC) - timedelta(seconds=1)
            elif drift == "long-lease":
                expiry = datetime.now(UTC) + timedelta(hours=2)
            elif drift == "oid":
                identity = replace(identity, role_oid=identity.role_oid + 1)
            elif drift == "credential":
                password += "changed"
            elif drift == "expiry":
                expiry += timedelta(seconds=1)
            elif drift == "membership":
                peer.execute(sql.SQL("GRANT pg_read_all_data TO {}").format(sql.Identifier(name)))
            elif drift == "role-setting":
                peer.execute(sql.SQL("ALTER ROLE {} SET role TO {}").format(sql.Identifier(name), sql.Identifier(target.successor_role)))
            with pytest.raises(RuntimeError):
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expiry)
            assert peer.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (name,)).fetchone() == (drift in {"credential", "expiry"},)
        finally:
            peer.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM {}; DROP ROLE {}").format(
                sql.Identifier(target.database), sql.Identifier(name), sql.Identifier(name)))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["journal", "guard", "collision"])
async def test_migrator_creation_never_adopts_ambient_role_or_outlives_failed_journal(transfer_database, failure):  # noqa: F811
    from loom.application_migrator_provision import create_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        name = "app_migrator_" + uuid4().hex
        if failure == "collision":
            peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(name)))
        recorded = []
        def persist(identity):
            recorded.append(identity)
            if failure == "journal":
                raise RuntimeError("journal publication failed")
            if failure == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
        try:
            with pytest.raises(RuntimeError):
                create_application_migrator(peer, target=args["target"], coordination_guard=args["coordination_guard"],
                    provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
                    migrator_role=name, persist_identity=persist)
            assert bool(peer.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)).fetchone()) == (failure == "collision")
            assert bool(recorded) == (failure != "collision")
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("target_revision", ["0142", "0146", "0147", "0148", "0149", "0150", "0151"])
async def test_actual_baseline_upgrade_preserves_separated_runtime_authority(transfer_database, monkeypatch, target_revision):  # noqa: F811
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.engine import make_url

    from loom.application_completed_authority import observe_completed_application_authority
    from loom.application_migrator_admission import (
        close_application_migrator_admission,
        reopen_application_migrator_admission,
    )
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        team, trial, task = uuid4(), uuid4(), "handoff-reward-" + uuid4().hex
        peer.execute("INSERT INTO teams(id,name) VALUES (%s,'handoff reward')", (team,))
        peer.execute("INSERT INTO tasks(id,config,checksum) VALUES (%s,'{}',%s)", (task, "a" * 64))
        peer.execute("""INSERT INTO trials(id,team_id,task_id,config,requires_caps,state,result)
            VALUES (%s,%s,%s,'{}','{}','succeeded',
            '{"schema_version":"loom.service-execution-trial-result.v1","reward":{"passed":1.0},
              "runtime_result":{"verifier_rewards":{"passed":1.0},"status":"succeeded"}}')""", (trial, team, task))
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        if target_revision in {"0147", "0148", "0149", "0150", "0151"}:
            from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility
            guard_owner = next(name for name, binding in args["role_bindings"].items() if binding == "guard-owner")
            metadata_query = """SELECT p.oid,p.proowner,p.proacl,p.prosecdef,p.proconfig,
                r.rolcanlogin,r.rolinherit,r.rolsuper,r.rolcreatedb,r.rolcreaterole,
                r.rolreplication,r.rolbypassrls,r.rolpassword
                FROM pg_proc p JOIN pg_authid r ON r.oid=p.proowner
                WHERE p.oid='loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)'::regprocedure"""
            original_metadata = peer.execute(metadata_query).fetchone()
            retained = []
            ensure_guard_claim_compatibility(peer, **authority, guard_owner=guard_owner,
                retained=None, persist=lambda value: retained.append(dict(value)))
            ensure_guard_claim_compatibility(peer, **authority, guard_owner=guard_owner,
                retained=retained[0], persist=lambda _: pytest.fail("replay must retain original inputs"))
            assert peer.execute(metadata_query).fetchone() == original_metadata
            assert peer.execute("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version").fetchone() == ("guard_0030",)
        identity = create_application_migrator(peer, **authority, migrator_role="app_migrator_" + uuid4().hex,
            persist_identity=lambda _: None)
        password = uuid4().hex
        arm_application_migrator(peer, **authority, identity=identity, password=password,
            expires_at=datetime.now(UTC) + timedelta(minutes=45))
        root = Path(__file__).resolve().parents[2]
        config = Config(str(root / "migrations/alembic.ini"))
        config.set_main_option("script_location", str(root / "migrations"))
        url = make_url(transfer_database[0]).set(drivername="postgresql+psycopg", username=identity.role_name, password=password)
        config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
        monkeypatch.setenv("LOOM_DB_OWNER_ROLE", target.successor_role)
        try:
            command.upgrade(config, target_revision)
            assert peer.execute("SELECT version_num FROM public.alembic_version").fetchone() == (target_revision,)
            assert peer.execute("SELECT result->>'aggregate_reward' FROM trials WHERE id=%s", (trial,)).fetchone() == (
                "1.0" if target_revision in {"0146", "0147", "0148", "0149", "0150", "0151"} else None,)
            observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=identity)
        finally:
            seal_application_migrator(maintenance, **authority, identity=identity)
            close_application_migrator_admission(maintenance, **authority, identity=identity)
            retire_application_migrator(maintenance, **authority, migrator_role=identity.role_name, migrator_oid=identity.role_oid)
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password=args["password"])
        observe_completed_application_authority(peer, target=target, runtime_password=args["password"])


@pytest.mark.asyncio
async def test_migration_recovery_waits_for_exact_prior_peer_and_never_adopts_unrecorded_role(transfer_database):  # noqa: F811
    from loom.application_migrator_provision import create_application_migrator
    from loom.application_migrator_recovery import (
        observe_application_migration_backend,
        observe_application_migrator_role,
        require_application_migration_peer_retired,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        with psycopg.connect(transfer_database[0], autocommit=True) as creator:
            backend = observe_application_migration_backend(creator, **authority, maintenance=False)
            with pytest.raises(RuntimeError, match="previous peer"):
                require_application_migration_peer_retired(maintenance, **authority, backend=backend)
        require_application_migration_peer_retired(maintenance, **authority, backend=backend)
        name = "app_migrator_" + uuid4().hex
        assert not observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=None)
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=lambda _: None)
        try:
            with pytest.raises(RuntimeError, match="unrecorded"):
                observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=None)
            assert observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)
        finally:
            peer.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))
        assert not observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)


@pytest.mark.asyncio
async def test_protected_migration_runtime_connects_actual_sql_lifecycle(transfer_database, tmp_path):  # noqa: F811
    from loom_cli.rollout.operator.protected_application_migration_journal import (
        ApplicationMigrationEvent,
    )
    from loom_cli.rollout.operator.protected_application_migration_runtime import (
        ProtectedApplicationMigrationRuntime,
    )
    from tests.integration.test_application_database_admission import _maintenance
    from tests.loom_cli.rollout.operator.test_application_migration_documents import _inputs

    plan, template, evidence, _ = _inputs(tmp_path)
    request = dict(request_id=evidence.request_id, candidate_sha=evidence.candidate_sha,
        candidate_tree=evidence.candidate_tree, generation=evidence.generation)
    with _closed(transfer_database, request=request) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        class Runner:
            @property
            def environment(self):
                return {"KUBECONFIG": "/disposable-only"}
            def open_staging_peer_database(self):
                return psycopg.connect(transfer_database[0], autocommit=True)
            def open_staging_peer_maintenance_database(self):
                return _maintenance(peer)
        fields = {key: value for key, value in evidence.to_dict().items() if key not in {"schema_version", "evidence_digest"}}
        evidence = type(evidence).build(**{**fields, "database_backend_pid": args["coordination_guard"].backend.pid})
        observed = []
        with ProtectedApplicationMigrationRuntime(plan=plan, guard=evidence, target=args["target"],
                coordination_guard=args["coordination_guard"], runner=Runner(), template=template,
                ca_certificate=b"public-ca-fixture" * 8, runtime_password=args["password"],
                container_registry="registry.example", assert_guard=lambda: evidence,
                assert_inputs=lambda: observed.append("inputs"), intent_digest="1" * 64,
                provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner")) as runtime:
            generation = ApplicationMigrationEvent.build(sequence=1, phase="generation",
                payload=runtime.prepare_generation(1), intent_digest="1" * 64,
                guard_digest=evidence.evidence_digest, previous_digest="2" * 64)
            recorded = []
            runtime.create(generation, recorded.append)
            oid = recorded[0]
            runtime.arm(generation, oid)
            runtime.release_creator()
            runtime.begin_retirement(generation, [])
            assert runtime.role_exists(generation, oid)
            runtime.seal(generation, oid)
            runtime.close(generation, oid)
            runtime.retire(generation, oid)
            runtime.reopen(generation, oid)
            assert not runtime.role_exists(generation, oid)
        assert len(observed) >= 10
        assert peer.execute("SELECT datallowconn FROM pg_database WHERE oid=%s", (args["target"].database_oid,)).fetchone() == (True,)


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("failure", ["journal", "readback", "guard"])
async def test_claim_compatibility_failure_keeps_replay_bound_to_original_function(transfer_database, failure):  # noqa: F811
    from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
            guard_owner=next(n for n, a in args["role_bindings"].items() if a == "guard-owner"))
        query = "SELECT pg_get_functiondef('loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)'::regprocedure)"
        before = peer.execute(query).fetchone()
        retained = []
        def persist(binding):
            retained.append(dict(binding))
            assert peer.execute(query).fetchone() == before
            if failure == "journal":
                raise RuntimeError("durable journal failed")
            if failure == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
        class LostReadback:
            def __getattr__(self, name):
                return getattr(peer, name)
            def execute(self, statement, *args, **kwargs):
                rendered = statement.as_string(peer) if isinstance(statement, sql.Composable) else statement
                if rendered.startswith("SELECT pg_catalog.pg_get_functiondef(oid),"):
                    raise RuntimeError("rewrite readback lost")
                return peer.execute(statement, *args, **kwargs)
        with pytest.raises(RuntimeError):
            ensure_guard_claim_compatibility(LostReadback() if failure == "readback" else peer,
                **authority, retained=None, persist=persist)
        assert peer.execute(query).fetchone() == before
        assert len(retained) == 1
        if failure != "guard":
            for _ in range(2):
                ensure_guard_claim_compatibility(peer, **authority, retained=retained[0],
                    persist=lambda _: pytest.fail("must not replace retained binding"))
            assert "guard_0033: refundable admission compatibility" in peer.execute(query).fetchone()[0]
        assert peer.execute("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version").fetchone() == ("guard_0030",)


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("drift", ["owner", "security", "search-path", "identity", "digest", "malformed-digest"])
async def test_claim_compatibility_refuses_changed_private_authority(transfer_database, drift):  # noqa: F811
    from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
            guard_owner=next(n for n, a in args["role_bindings"].items() if a == "guard-owner"))
        retained = []
        ensure_guard_claim_compatibility(peer, **authority, retained=None, persist=lambda value: retained.append(dict(value)))
        function = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"
        if drift == "owner":
            authority["guard_owner"] = "unrelated_guard_owner"
        elif drift == "security":
            peer.execute(sql.SQL("ALTER FUNCTION " + function + " SECURITY INVOKER"))
        elif drift == "search-path":
            peer.execute(sql.SQL("ALTER FUNCTION " + function + " SET search_path=public"))
        elif drift == "identity":
            retained[0]["function_oid"] += 1
        elif drift == "digest":
            retained[0]["after_sha256"] = "0" * 64
        elif drift == "malformed-digest":
            retained[0]["before_sha256"] = []
        with pytest.raises(RuntimeError, match="guard compatibility"):
            ensure_guard_claim_compatibility(peer, **authority, retained=retained[0],
                persist=lambda _: pytest.fail("refusal must not rewrite saved authority"))
