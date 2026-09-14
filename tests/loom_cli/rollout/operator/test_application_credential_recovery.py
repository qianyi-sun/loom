"""Original credentials are returned only after exact, durable source binding."""

import base64
import hashlib
import json
import os
import subprocess
from pathlib import Path
from urllib.parse import quote

import pytest

from loom.application_password import application_scram_verifier
from loom_cli.cluster_backup_guard import backup_manifest_sha256, write_backup_manifest
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournalError
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal
from tests.loom_cli.rollout.test_rehearsal_secret_restore import _checkpoint

_PASSWORD = "original+literal%3A:credential"
_NAMES = ("loom-secrets", "loom-postgres-cnpg-credentials")


def _json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sources(tmp_path, *, password=_PASSWORD, pools=True, change=None, schema=2, schema_revision="0142/guard_0033"):
    manifest_path = _checkpoint(tmp_path, optional_protected_present=True, inventory_schema=schema)
    root = manifest_path.parent / "secrets"
    app_path = root / "loom-secrets.yaml"
    app = json.loads(app_path.read_bytes())
    values = {"postgres-user": "loom", "postgres-password": password}
    for key in ("cp-db-url", "gw-db-url", "svc-db-url"):
        credentials = "loom:" + quote(password, safe="+")
        values[key] = f"postgresql+psycopg://{credentials}@loom-postgres:5432/loom?sslmode=prefer"
        if pools:
            values[key + "-pool"] = (
                f"postgresql+psycopg://{credentials}@loom-pgbouncer:6432/loom?sslmode=prefer"
            )
    if change:
        change(values)
    app["data"] = {key: base64.b64encode(value.encode()).decode() for key, value in values.items()}
    app_path.write_bytes(_json_bytes(app))
    paths = {"loom-secrets": app_path}
    if schema == 2:
        cnpg_path = root / "protected-loom-staging-loom-postgres-cnpg-credentials.json"
        cnpg = json.loads(cnpg_path.read_bytes())
        cnpg["data"] = {
            "username": base64.b64encode(b"loom").decode(),
            "password": base64.b64encode(password.encode()).decode(),
        }
        cnpg_path.write_bytes(_json_bytes(cnpg))
        paths[_NAMES[1]] = cnpg_path
        inventory_path = root / "protected-capacity-secret-inventory.json"
        inventory = json.loads(inventory_path.read_bytes())
        for record in inventory["secrets"]:
            if record["name"] == _NAMES[1]:
                record["sha256"] = hashlib.sha256(cnpg_path.read_bytes()).hexdigest()
        inventory_path.write_bytes(_json_bytes(inventory))
    manifest = json.loads(manifest_path.read_bytes())
    from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
    authority_path = Path(manifest["components"]["database_authority"]["path"])
    authority_data = json.loads(authority_path.read_bytes())
    public_revision, guard_revision = schema_revision.split("/")
    authority_data.update(public_schema_revision=public_revision, capacity_guard_schema_revision=guard_revision)
    authority = DatabaseAuthorityEvidence.from_dict(authority_data)
    authority_path.write_bytes(authority.payload)
    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components={name: Path(value["path"]) for name, value in manifest["components"].items()},
        schema_version=3,
    )
    payload = _plan(tmp_path).to_dict()
    payload.update(schema_revision=public_revision, public_schema_revision=public_revision,
                   capacity_guard_schema_revision=guard_revision, database_authority_digest=authority.digest)
    payload["backup_manifest_path"] = str(manifest_path)
    payload["backup_manifest_sha256"] = backup_manifest_sha256(
        manifest_path, expected_owner_uid=os.geteuid()
    )
    payload["checkpoint_component_sha256"] = {
        name: value["sha256"] for name, value in manifest["components"].items()
    }
    payload["db_snapshot_identity"] = (
        "pgdump-sha256:" + manifest["components"]["postgres"]["sha256"]
    )
    from loom_cli.rollout.operator.backup_lease import component_set_digest

    payload["backup_component_set_digest"] = component_set_digest(
        payload["checkpoint_component_sha256"]
    )
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in payload.items() if key != "plan_digest"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    live = {}
    for name, path in paths.items():
        secret = json.loads(path.read_bytes())
        secret["metadata"].update(uid="11111111-1111-4111-8111-111111111111", resourceVersion="42")
        live[name] = secret
    return FinalGatePlan.from_dict(payload), live


class _Runner:
    def __init__(self, live):
        self.environment = {"KUBECONFIG": "/fixture/protected-config"}
        self.live = live
        self.calls = []
        self.configuration_calls = []
        self.after_read = lambda: None

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 30
        if argv[4] != "secret":
            from tests.loom_cli.rollout.operator.test_cnpg_writer_configuration import (
                _configuration,
            )
            assert tuple(argv[:4]) == ("kubectl", "--namespace", "loom-staging", "get")
            kind = argv[4]
            if kind.startswith("--raw="):
                resource = kind.rsplit("/", 1)[-1]
                assert resource in {"databases", "poolers", "publications", "subscriptions"}
                assert kind == "--raw=/apis/postgresql.cnpg.io/v1/namespaces/loom-staging/" + resource
                assert tuple(argv[5:]) == ("--request-timeout=30s",)
                kind = resource + ".postgresql.cnpg.io"
            else:
                assert kind in {"cluster.postgresql.cnpg.io", "configmap"}
                assert tuple(argv[-2:]) == ("--output=json", "--request-timeout=30s")
            self.configuration_calls.append(kind)
            return _json_bytes(_configuration()[kind])
        assert tuple(argv[:5]) == ("kubectl", "--namespace", "loom-staging", "get", "secret")
        assert tuple(argv[6:]) == ("--output=json", "--request-timeout=30s")
        name = argv[5]
        assert name in _NAMES
        self.calls.append(name)
        result = _json_bytes(self.live[name])
        self.after_read()
        return result


def _read(plan, journal, runner):
    from loom_cli.rollout.operator.protected_application_credential_recovery import (
        recover_application_runtime_credential,
    )

    return recover_application_runtime_credential(plan, journal=journal, runner=runner)


@pytest.mark.parametrize("pools", [False, True])
def test_original_credential_is_journal_bound_and_recoverable(tmp_path, pools):
    plan, live = _sources(tmp_path, pools=pools)
    journal, runner = _journal(tmp_path), _Runner(live)
    with pytest.raises(ProtectedApplyJournalError, match="active component"):
        _read(plan, journal, runner)
    assert runner.calls == []

    def apply(_):
        credential = _read(plan, journal, runner)
        assert credential.username == "loom"
        assert credential.password == _PASSWORD  # Literal plus and %3A are preserved.
        assert _PASSWORD not in repr(credential)
        raise RuntimeError("interrupted after credential binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    record = journal.root / "00-application-ownership-handoff/application-credentials.json"
    saved = record.read_bytes()
    assert _PASSWORD.encode() not in saved
    assert base64.b64encode(_PASSWORD.encode()) not in saved
    assert record.stat().st_mode & 0o777 == 0o600
    assert runner.calls == [*_NAMES, *_NAMES]
    journal = type(journal)(
        tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number
    )
    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    assert record.read_bytes() == saved
    assert not (record.parent / "terminal.json").exists()


@pytest.mark.parametrize(
    "change",
    [
        lambda values: values.update(
            {"svc-db-url": values["svc-db-url"].replace("loom:original", "other:original")}
        ),
        lambda values: values.update({"cp-db-url-pool": values["cp-db-url"]}),
        lambda values: values.update({"cp-db-url": values["cp-db-url"] + "&host=foreign"}),
        lambda values: values.update({"gw-db-url": values["gw-db-url"] + "#fragment"}),
        lambda values: values.pop("gw-db-url"),
    ],
)
def test_inconsistent_or_redirected_database_credentials_refuse(tmp_path, change):
    plan, live = _sources(tmp_path, change=change)
    journal, runner = _journal(tmp_path), _Runner(live)
    with pytest.raises(ValueError, match="credential"):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert runner.calls == []
    assert not list(journal.root.rglob("application-credentials.json"))


@pytest.mark.parametrize("schema,password", [(1, _PASSWORD), (2, "password with space")])
def test_unrecoverable_source_refuses_before_live_reads(tmp_path, schema, password):
    plan, live = _sources(tmp_path, schema=schema, password=password)
    journal, runner = _journal(tmp_path), _Runner(live)
    with pytest.raises(ValueError, match="credential"):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert runner.calls == []


@pytest.mark.parametrize(
    "password",
    [
        "md5" + "a" * 32,
        "md5" + "A" * 32,
        application_scram_verifier("synthetic-underlying-password"),
        "$" + application_scram_verifier("synthetic-underlying-password"),
        "$$" + application_scram_verifier("synthetic-underlying-password"),
        # Reserve this syntax even when our bounded verifier reader would refuse
        # it. PostgreSQL's stored-secret parser has different bounds/grammar.
        "SCRAM-SHA-256$1:YQ==$" + "YQ==:YQ==",
    ],
    ids=["md5", "uppercase-md5", "scram", "leading-dollar", "leading-dollars", "reserved-scram"],
)
def test_cnpg_verifier_shaped_password_refuses_before_binding_or_handoff(tmp_path, password):
    plan, live = _sources(tmp_path, password=password)
    journal, runner = _journal(tmp_path), _Runner(live)
    reached_handoff = []

    def apply(_):
        credential = _read(plan, journal, runner)
        reached_handoff.append(credential.username)
        pytest.fail("CNPG-incompatible original credential reached the handoff")

    with pytest.raises(ValueError, match="credential") as caught:
        journal.execute(plan, [_component(apply)])
    assert password not in str(caught.value)
    assert runner.calls == []
    assert reached_handoff == []
    assert not list(journal.root.rglob("application-credentials.json"))


@pytest.mark.parametrize("password", ["md5-ordinary", "$ordinary", "SCRAM-ordinary", "a'\\b+$:c"])
def test_cnpg_ordinary_literal_password_is_preserved(tmp_path, password):
    plan, live = _sources(tmp_path, password=password)
    journal, runner = _journal(tmp_path), _Runner(live)

    def apply(_):
        credential = _read(plan, journal, runner)
        assert credential.password == password
        raise RuntimeError("interrupted after credential binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    assert runner.calls == [*_NAMES, *_NAMES]


@pytest.mark.parametrize("field", ["uid", "resourceVersion", "password"])
def test_live_secret_drift_across_retry_is_not_adopted(tmp_path, field):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)

    def apply(_):
        _read(plan, journal, runner)
        raise RuntimeError("interrupted after binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    path = journal.root / "00-application-ownership-handoff/application-credentials.json"
    saved = path.read_bytes()
    if field == "password":
        live[_NAMES[1]]["data"][field] = base64.b64encode(b"changed").decode()
    else:
        live[_NAMES[1]]["metadata"][field] = (
            "22222222-2222-4222-8222-222222222222" if field == "uid" else "43"
        )
    with pytest.raises((ValueError, ProtectedApplyJournalError)):
        journal.execute(plan, [_component(apply)])
    assert path.read_bytes() == saved


def test_live_secret_change_between_observations_refuses(tmp_path):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)
    runner.after_read = lambda: live[_NAMES[0]]["metadata"].update(resourceVersion="43")
    with pytest.raises(ValueError, match="credential"):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert not list(journal.root.rglob("application-credentials.json"))


def test_recovery_rejects_record_schema_boolean(tmp_path):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)

    def apply(_):
        _read(plan, journal, runner)
        raise RuntimeError("interrupted after binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    path = journal.root / "00-application-ownership-handoff/application-credentials.json"
    payload = json.loads(path.read_bytes())
    payload["schema_version"] = True
    path.write_bytes(_json_bytes(payload))
    with pytest.raises(ProtectedApplyJournalError, match="readback"):
        journal.execute(plan, [_component(apply)])


def test_recovery_validates_other_backup_components_before_live_read(tmp_path):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)
    dump = Path(plan.backup_manifest_path).parent / "postgres/loom.dump"
    dump.write_bytes(b"drift-dump")
    with pytest.raises(ValueError, match="credential"):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert runner.calls == []


def test_recovery_binds_the_actual_private_bytes_consumed(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import protected_application_credential_recovery as module

    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)
    validate = module.validate_backup_manifest

    def changed_after_validation(*args, **kwargs):
        result = validate(*args, **kwargs)
        source = Path(plan.backup_manifest_path).parent / "secrets/loom-secrets.yaml"
        payload = json.loads(source.read_bytes())
        payload["data"]["postgres-password"] = base64.b64encode(b"changed").decode()
        source.write_bytes(_json_bytes(payload))
        return result

    monkeypatch.setattr(module, "validate_backup_manifest", changed_after_validation)
    with pytest.raises(ValueError, match="credential"):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert runner.calls == []


@pytest.mark.parametrize("retry", [False, True])
def test_credential_not_returned_before_durable_binding(tmp_path, monkeypatch, retry):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)
    path = journal.root / "00-application-ownership-handoff/application-credentials.json"
    original_fsync = os.fsync
    fail = False
    returned = []

    def fsync(fd):
        if fail and path.exists() and os.fstat(fd).st_ino == path.stat().st_ino:
            raise OSError("credential record sync failure")
        original_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)

    def apply(_):
        nonlocal fail
        if retry:
            _read(plan, journal, runner)
        fail = True
        returned.append(_read(plan, journal, runner))

    with pytest.raises(OSError, match="sync failure"):
        journal.execute(plan, [_component(apply)])
    assert not returned
    assert not (path.parent / "terminal.json").exists()


def test_raw_password_delimiter_cannot_disagree_with_sqlalchemy(tmp_path):
    from sqlalchemy.engine import make_url

    def raw_delimiter(values):
        values["cp-db-url"] = values["cp-db-url"].replace("%40", "@")
        assert make_url(values["cp-db-url"]).password != values["postgres-password"]

    plan, live = _sources(tmp_path, password="p@segment", change=raw_delimiter)
    journal, runner = _journal(tmp_path), _Runner(live)
    with pytest.raises(ValueError, match="credential"):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert runner.calls == []


def test_subprocess_timeout_with_partial_secret_is_sanitized(tmp_path, monkeypatch):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)
    sensitive = b"partial-secret-value"

    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 30, output=sensitive, stderr=sensitive)

    monkeypatch.setattr(runner, "capture_stdout", timeout)
    with pytest.raises(ValueError, match="credential") as caught:
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert sensitive.decode() not in str(caught.value)
    assert sensitive.decode() not in repr(caught.value)
    assert caught.value.__suppress_context__
    assert not list(journal.root.rglob("application-credentials.json"))


def test_classification_observes_original_credentials_without_publishing(tmp_path, monkeypatch):
    from loom_cli.rollout.operator.protected_application_credential_recovery import (
        observe_application_runtime_credential,
    )
    from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal

    plan, live = _sources(tmp_path)
    runner = _Runner(live)
    before = {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    def forbid(*args, **kwargs):
        pytest.fail('credential observation cannot publish or fsync')
    monkeypatch.setattr(os, 'fsync', forbid)
    monkeypatch.setattr(ProtectedApplyJournal, '_publish_or_match', forbid)
    observed = observe_application_runtime_credential(plan, runner=runner, service_uid=os.getuid())
    assert observed.credential.password == _PASSWORD
    assert _PASSWORD not in repr(observed)
    assert observed.binding.manifest_sha256 == plan.backup_manifest_sha256
    assert observed.configuration.cluster_uid == '22222222-2222-4222-8222-222222222222'
    assert {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before


@pytest.mark.parametrize('corruption', [None, 'credentials-version', 'credentials-source', 'config-generation', 'config-unknown'])
def test_saved_credential_and_writer_bindings_are_available_without_apply_authority(tmp_path, monkeypatch, corruption):
    from dataclasses import asdict

    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), _Runner(live)
    def apply(_):
        _read(plan, journal, runner)
        raise RuntimeError('saved original credentials')
    component = _component(apply)
    with pytest.raises(RuntimeError, match='saved original'):
        journal.execute(plan, [component])
    if corruption:
        path = journal.root / '00-application-ownership-handoff' / (
            'application-credentials.json' if corruption.startswith('credentials') else 'application-cnpg-configuration.json')
        value = json.loads(path.read_text())
        if corruption == 'credentials-version':
            value['schema_version'] = True
        elif corruption == 'credentials-source':
            value['binding']['component_sha256'] = '0' * 64
        elif corruption == 'config-generation':
            value['binding']['cluster_generation'] = True
        else:
            value['binding']['unknown'] = 'field'
        path.write_text(json.dumps(value))
    def forbid(*args, **kwargs):
        pytest.fail('classification cannot publish or sync')
    monkeypatch.setattr(journal, '_sync_application_recovery', forbid)
    monkeypatch.setattr(journal, '_publish_or_match', forbid)
    if corruption:
        with pytest.raises((ValueError, RuntimeError), match=r'application recovery|configuration binding'):
            journal.read_application_recovery_view(plan, component, ordinal=0)
        return
    view = journal.read_application_recovery_view(plan, component, ordinal=0)
    assert view.credential_binding.manifest_sha256 == plan.backup_manifest_sha256
    assert view.cnpg_configuration.cluster_uid == '22222222-2222-4222-8222-222222222222'
    assert _PASSWORD not in json.dumps(asdict(view))
