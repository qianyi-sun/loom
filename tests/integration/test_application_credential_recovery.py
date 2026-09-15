"""Original backup credential through real peer ownership/login restoration.

Kubernetes responses and backup payloads are isolated fixtures. This proves the
database/credential composition, not live CNPG reconciliation or fleet cutover.
"""

from uuid import uuid4

import psycopg
import pytest
from sqlalchemy.engine import make_url

from loom.application_database_admission import (
    ApplicationDatabaseHandoffBackend,
    capture_application_database_admission,
    close_application_database_admission,
    reopen_application_database_admission,
    require_application_database_drained,
)
from loom.application_login_sealing import seal_application_login
from loom.application_ownership_transfer import transfer_application_ownership
from loom.application_password import application_scram_verifier, matches_application_scram
from loom.application_runtime_login import restore_application_runtime_login
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_protected_peer_database_connection import _peer
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_credential_recovery import (
    _read,
    _Runner,
    _sources,
)
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
@pytest.mark.parametrize("kind", ["md5", "scram", "leading-dollar-scram"])
def test_cnpg_raw_password_refresh_changes_verifier_literal_semantics(
    transfer_postgres_url, tmp_path, kind,  # noqa: F811
):
    password = "md5" + "a" * 32
    if kind != "md5":
        password = application_scram_verifier("synthetic-underlying-password")
        if kind == "leading-dollar-scram":
            password = "$" + password
    role = "cnpg_literal_" + uuid4().hex
    url = transfer_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(url, autocommit=True) as admin:
        # The generic restore primitive deliberately treats verifier-shaped
        # originals as literal passwords. CNPG's raw PASSWORD SQL does not.
        admin.execute(psycopg.sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            psycopg.sql.Identifier(role),
            psycopg.sql.Literal(application_scram_verifier(password)),
        ))
        try:
            with psycopg.connect(url, user=role, password=password, connect_timeout=2) as client:
                assert client.execute("SELECT current_user").fetchone() == (role,)
            admin.execute("SET password_encryption='scram-sha-256'")
            assert admin.execute("SHOW password_encryption").fetchone() == ("scram-sha-256",)
            admin.execute(psycopg.sql.SQL("ALTER ROLE {} WITH PASSWORD {}").format(
                psycopg.sql.Identifier(role), psycopg.sql.Literal(password),
            ))
            state = admin.execute(
                "SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname=%s", (role,)
            ).fetchone()
            assert state == (True, password)  # Stored verbatim even under SCRAM encryption.
            assert not matches_application_scram(password, state[1])
            with pytest.raises(psycopg.OperationalError, match="password authentication failed"):
                psycopg.connect(url, user=role, password=password, connect_timeout=2).close()

            plan, live = _sources(tmp_path, password=password)
            journal, runner = _journal(tmp_path), _Runner(live)

            def apply(_):
                _read(plan, journal, runner)
                pytest.fail("CNPG-incompatible original credential reached the handoff")

            with pytest.raises(ValueError, match="credential"):
                journal.execute(plan, [_component(apply)])
            assert runner.calls == []
            assert not list(journal.root.rglob("application-credentials.json"))
            assert admin.execute(
                "SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname=%s", (role,)
            ).fetchone() == state
        finally:
            admin.execute(psycopg.sql.SQL("DROP ROLE {}").format(psycopg.sql.Identifier(role)))


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["staging-credential"], indirect=True)
async def test_same_original_password_refresh_cannot_reopen_the_handoff(
    transfer_database,  # noqa: F811
    transfer_postgres,  # noqa: F811
):
    url, owner, bindings = transfer_database
    password = "ab" * 16
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    with (
        psycopg.connect(url, dbname="postgres", autocommit=True) as cnpg,
        _peer(transfer_postgres, "loom") as connection,
        _peer(transfer_postgres, "postgres") as maintenance,
    ):

        def refresh():
            # Exact v1.25.1 SetUserPassword statement shape, synthetic credential
            # in a disposable DB. Each refresh independently re-salts SCRAM.
            cnpg.execute(
                psycopg.sql.SQL("ALTER ROLE loom WITH PASSWORD {}").format(
                    psycopg.sql.Literal(password)
                )
            )
            assert cnpg.execute(
                "SELECT rolcanlogin FROM pg_authid WHERE rolname='loom'"
            ).fetchone() == (False,)

        seal_application_login(
            connection, database="loom", role="loom", provisioner_role=provisioner
        )
        refresh()
        identity = connection.backend_identity
        handoff = ApplicationDatabaseHandoffBackend(
            identity.backend_pid,
            identity.backend_started_at,
            identity.system_identifier,
            identity.server_started_at,
            identity.database_oid,
        )
        with pytest.raises(RuntimeError, match="sealed role"):
            capture_application_database_admission(
                maintenance,
                database="loom",
                owner_role="loom",
                successor_role=owner,
                provisioner_role=provisioner,
                handoff_backend=handoff,
            )
        target = capture_application_database_admission(
            maintenance,
            database="loom",
            owner_role="loom",
            successor_role=owner,
            provisioner_role=provisioner,
            handoff_backend=handoff,
            runtime_password=password,
        )
        refresh()
        close_application_database_admission(
            maintenance,
            target=target,
            provisioner_role=provisioner,
            runtime_password=password,
        )
        try:
            refresh()
            require_application_database_drained(
                maintenance,
                target=target,
                provisioner_role=provisioner,
                handoff_backend=handoff,
                runtime_password=password,
            )
            for _ in range(2):
                refresh()

                class RefreshDuringTransfer:
                    refreshed = False

                    @property
                    def info(self):
                        return connection.info

                    def transaction(self):
                        return connection.transaction()

                    def execute(self, query):
                        statement = query if isinstance(query, str) else query.as_string()
                        assert password not in statement
                        result = connection.execute(query)
                        if not self.refreshed and "pg_stat_clear_snapshot()" in statement:
                            # A second backend commits a new salt while the real
                            # handoff transaction is open, after role admission.
                            refresh()
                            self.refreshed = True
                        return result

                refreshing = RefreshDuringTransfer()
                with connection.transaction():
                    transfer_application_ownership(
                        refreshing,
                        owner_role=owner,
                        role_bindings=bindings,
                        runtime_password=password,
                    )
                assert refreshing.refreshed
            assert cnpg.execute(
                "SELECT datallowconn FROM pg_database WHERE datname='loom'"
            ).fetchone() == (False,)
            assert cnpg.execute(
                "SELECT rolcanlogin,rolpassword IS NULL FROM pg_authid WHERE rolname=%s", (owner,)
            ).fetchone() == (False, True)
        finally:
            reopen_application_database_admission(
                maintenance,
                target=target,
                provisioner_role=provisioner,
                runtime_password=password,
            )
        refresh()
        for _ in range(2):
            restore_application_runtime_login(
                connection,
                owner_role=owner,
                role_bindings=bindings,
                target=target,
                password=password,
            )
            cnpg.execute(
                psycopg.sql.SQL("ALTER ROLE loom WITH PASSWORD {}").format(
                    psycopg.sql.Literal(password)
                )
            )
        runtime_url = (
            make_url(url)
            .set(username="loom", password=password)
            .render_as_string(hide_password=False)
        )
        with psycopg.connect(runtime_url, autocommit=True) as runtime:
            assert runtime.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                runtime.execute("ALTER TABLE public.trials DISABLE TRIGGER ALL")


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["staging-credential"], indirect=True)
@pytest.mark.parametrize("drift", ["changed_password", "runtime_login", "owner_password"])
async def test_password_refresh_mode_refuses_changed_credentials_or_authority(
    transfer_database,  # noqa: F811
    transfer_postgres,  # noqa: F811
    drift,
):
    url, owner, bindings = transfer_database
    password = "ab" * 16
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    with (
        psycopg.connect(url, dbname="postgres", autocommit=True) as cnpg,
        _peer(transfer_postgres, "loom") as connection,
        _peer(transfer_postgres, "postgres") as maintenance,
    ):
        identity = connection.backend_identity
        handoff = ApplicationDatabaseHandoffBackend(
            identity.backend_pid,
            identity.backend_started_at,
            identity.system_identifier,
            identity.server_started_at,
            identity.database_oid,
        )
        arguments = dict(
            database="loom",
            owner_role="loom",
            successor_role=owner,
            provisioner_role=provisioner,
            handoff_backend=handoff,
            runtime_password=password,
        )
        target = capture_application_database_admission(maintenance, **arguments)

        def change():
            if drift == "runtime_login":
                cnpg.execute("ALTER ROLE loom LOGIN")
            else:
                cnpg.execute(
                    psycopg.sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                        psycopg.sql.Identifier(owner if drift == "owner_password" else "loom"),
                        psycopg.sql.Literal("unexpected-original-password"),
                    )
                )

        def reset():
            cnpg.execute("ALTER ROLE loom NOLOGIN PASSWORD NULL")
            cnpg.execute(
                psycopg.sql.SQL("ALTER ROLE {} PASSWORD NULL").format(psycopg.sql.Identifier(owner))
            )

        change()
        with pytest.raises(RuntimeError):
            capture_application_database_admission(maintenance, **arguments)
        reset()
        close_application_database_admission(
            maintenance, target=target, provisioner_role=provisioner
        )
        try:
            change()
            for operation in (
                close_application_database_admission,
                reopen_application_database_admission,
            ):
                with pytest.raises(RuntimeError):
                    operation(
                        maintenance,
                        target=target,
                        provisioner_role=provisioner,
                        runtime_password=password,
                    )
            with pytest.raises(RuntimeError):
                require_application_database_drained(
                    maintenance,
                    target=target,
                    provisioner_role=provisioner,
                    handoff_backend=handoff,
                    runtime_password=password,
                )
            with connection.transaction(), pytest.raises(RuntimeError):
                transfer_application_ownership(
                    connection,
                    owner_role=owner,
                    role_bindings=bindings,
                    runtime_password=password,
                )
            assert cnpg.execute(
                "SELECT datallowconn,pg_get_userbyid(datdba) FROM pg_database WHERE datname='loom'"
            ).fetchone() == (False, "loom")
        finally:
            reset()
            reopen_application_database_admission(
                maintenance, target=target, provisioner_role=provisioner
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["staging-credential"], indirect=True)
async def test_backup_credential_restores_over_real_peer_and_retries(
    transfer_database,  # noqa: F811
    transfer_postgres,  # noqa: F811
    tmp_path,
):
    url, owner, bindings = transfer_database
    assert make_url(url).database == "loom"
    assert bindings["loom"] == "application-owner"
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    # Same original credential as this fixture's ordinary provisioning phase.
    plan, live = _sources(tmp_path, password="ab" * 16)
    journal, runner = _journal(tmp_path), _Runner(live)
    transfers = []

    def apply(_):
        credential = _read(plan, journal, runner)
        with _peer(transfer_postgres, "loom") as connection:
            saved = journal.read_application_admission_recovery()
            if saved is None:
                seal_application_login(
                    connection, database="loom", role="loom", provisioner_role=provisioner
                )
                identity = connection.backend_identity
                handoff = ApplicationDatabaseHandoffBackend(
                    identity.backend_pid,
                    identity.backend_started_at,
                    identity.system_identifier,
                    identity.server_started_at,
                    identity.database_oid,
                )
                with _peer(transfer_postgres, "postgres") as maintenance:
                    target = capture_application_database_admission(
                        maintenance,
                        database="loom",
                        owner_role="loom",
                        successor_role=owner,
                        provisioner_role=provisioner,
                        handoff_backend=handoff,
                    )
                    saved = journal.record_application_admission_recovery(
                        target=target, handoff_backend=handoff
                    )
                    close_application_database_admission(
                        maintenance, target=target, provisioner_role=provisioner
                    )
                    try:
                        require_application_database_drained(
                            maintenance,
                            target=target,
                            provisioner_role=provisioner,
                            handoff_backend=handoff,
                        )
                        with connection.transaction():
                            transfer_application_ownership(
                                connection, owner_role=owner, role_bindings=bindings
                            )
                        transfers.append(target)
                    finally:
                        reopen_application_database_admission(
                            maintenance, target=target, provisioner_role=provisioner
                        )
            # This injected retry starts after restoration; a full deployed
            # component still needs every other intermediate-state branch.
            restore_application_runtime_login(
                connection,
                owner_role=owner,
                role_bindings=bindings,
                target=saved.target,
                password=credential.password,
            )
            assert credential.username == saved.target.owner_role
            assert credential.password == "ab" * 16
            client_url = (
                make_url(url)
                .set(username=credential.username, password=credential.password)
                .render_as_string(hide_password=False)
            )
            wrong_url = (
                make_url(client_url).set(password="wrong").render_as_string(hide_password=False)
            )
            with pytest.raises(psycopg.OperationalError, match="password authentication failed"):
                psycopg.connect(wrong_url)
            with psycopg.connect(client_url, autocommit=True) as runtime:
                assert runtime.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    runtime.execute("ALTER TABLE public.trials DISABLE TRIGGER ALL")
        raise RuntimeError("injected interruption after restored login")

    for _ in range(2):
        with pytest.raises(RuntimeError, match="injected interruption"):
            journal.execute(plan, [_component(apply)])
        journal = type(journal)(
            tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number
        )
    assert len(transfers) == 1
