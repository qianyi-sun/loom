"""Declared CNPG writers must be bounded before original credential recovery."""

import copy
import json
from pathlib import Path

import pytest
import yaml

from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_credential_recovery import (
    _Runner,
    _sources,
)
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


def _configuration():
    metadata = {
        "namespace": "loom-staging", "uid": "22222222-2222-4222-8222-222222222222",
        "resourceVersion": "101", "generation": 1,
    }
    monitoring = yaml.safe_load((Path(__file__).resolve().parents[3] /
                                "fixtures/cnpg/default-monitoring-1.25.1.yaml").read_text())
    monitoring["metadata"] = {**metadata, "name": "cnpg-default-monitoring"}
    cluster = {
        "apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster",
        "metadata": {**metadata, "name": "loom-postgres"},
        "spec": {
            "imageName": "ghcr.io/cloudnative-pg/postgresql:17.4",
            "enableSuperuserAccess": False,
            "bootstrap": {"initdb": {"database": "loom", "owner": "loom",
                                     "secret": {"name": "loom-postgres-cnpg-credentials"}}},
            "superuserSecret": {"name": "loom-postgres-cnpg-credentials"},
            "postgresql": {"parameters": {"shared_preload_libraries": ""}},
            "monitoring": {"disableDefaultQueries": False, "enablePodMonitor": True,
                           "customQueriesConfigMap": [{"name": "cnpg-default-monitoring",
                                                       "key": "queries"}]},
        },
        "status": {"poolerIntegrations": {"pgBouncerIntegration": {}}},
    }
    empty = {"apiVersion": "postgresql.cnpg.io/v1", "metadata": {"resourceVersion": "101"},
             "items": []}
    return {"cluster.postgresql.cnpg.io": cluster,
            "databases.postgresql.cnpg.io": {**copy.deepcopy(empty), "kind": "DatabaseList"},
            "poolers.postgresql.cnpg.io": {**copy.deepcopy(empty), "kind": "PoolerList"},
            "publications.postgresql.cnpg.io": {**copy.deepcopy(empty), "kind": "PublicationList"},
            "subscriptions.postgresql.cnpg.io": {**copy.deepcopy(empty), "kind": "SubscriptionList"},
            "configmap": monitoring}


class ConfigurationRunner(_Runner):
    def __init__(self, live):
        super().__init__(live)
        self.configuration = _configuration()
        self.configuration_calls = []
        self.after_configuration_read = lambda: None

    def capture_stdout(self, argv, *, env, timeout_seconds):
        if argv[4] == "secret":
            return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)
        assert tuple(argv[:4]) == ("kubectl", "--namespace", "loom-staging", "get")
        assert env == self.environment and timeout_seconds == 30
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
        payload = json.dumps(self.configuration[kind]).encode()
        self.after_configuration_read()
        return payload


def _capture(plan, journal, runner):
    from loom_cli.rollout.operator.protected_cnpg_writer_configuration import (
        capture_cnpg_writer_configuration,
    )
    return capture_cnpg_writer_configuration(plan, journal=journal, runner=runner)


def test_configuration_requires_active_component_and_binds_recovery(tmp_path):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    with pytest.raises(RuntimeError, match="active component"):
        _capture(plan, journal, runner)
    assert runner.configuration_calls == []

    def apply(_):
        binding = _capture(plan, journal, runner)
        assert binding.cluster_uid == runner.configuration["cluster.postgresql.cnpg.io"]["metadata"]["uid"]
        raise RuntimeError("interrupted after configuration binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-configuration.json"
    saved = path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    assert path.read_bytes() == saved
    assert runner.calls == []


@pytest.mark.parametrize("drift", ["managed-role", "database", "pooler", "publication", "subscription", "pooler-status",
                                  "preload", "sql-bootstrap", "monitoring-sql", "monitoring-secret",
                                  "superuser", "foreign-secret", "unknown-setting", "plugin"])
def test_extra_writer_configuration_refuses_before_credentials(tmp_path, drift):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    cluster = runner.configuration["cluster.postgresql.cnpg.io"]
    spec = cluster["spec"]
    if drift == "managed-role":
        spec["managed"] = {"roles": [{"name": "loom", "login": True}]}
    elif drift in {"database", "pooler", "publication", "subscription"}:
        kind = drift + "s"
        runner.configuration[kind + ".postgresql.cnpg.io"]["items"] = [
            {"spec": {"cluster": {"name": "loom-postgres"}}}]
    elif drift == "pooler-status":
        cluster["status"]["poolerIntegrations"]["pgBouncerIntegration"]["secrets"] = ["pool-password"]
    elif drift == "preload":
        spec["postgresql"]["parameters"]["shared_preload_libraries"] = "pgaudit"
    elif drift == "sql-bootstrap":
        spec["bootstrap"]["initdb"]["postInitApplicationSQL"] = ["ALTER ROLE loom LOGIN"]
    elif drift == "monitoring-sql":
        runner.configuration["configmap"]["data"]["queries"] += "\nforeign: {query: 'ALTER ROLE loom LOGIN'}\n"
    elif drift == "monitoring-secret":
        spec["monitoring"]["customQueriesSecret"] = [{"name": "foreign", "key": "queries"}]
    elif drift == "superuser":
        spec["enableSuperuserAccess"] = True
    elif drift == "foreign-secret":
        spec["bootstrap"]["initdb"]["secret"]["name"] = "foreign"
    elif drift == "unknown-setting":
        spec["postgresql"]["parameters"]["session_preload_libraries"] = "foreign"
    else:
        spec["plugins"] = [{"name": "foreign"}]

    with pytest.raises(ValueError, match="CNPG"):
        journal.execute(plan, [_component(lambda _: _capture(plan, journal, runner))])
    assert runner.calls == []
    assert not list(journal.root.rglob("application-cnpg-configuration.json"))


@pytest.mark.parametrize("field", ["uid", "generation", "parameters", "monitoring-rv"])
def test_configuration_drift_between_reads_or_recovery_is_not_adopted(tmp_path, field):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)

    def apply(_):
        _capture(plan, journal, runner)
        raise RuntimeError("interrupted after configuration binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-configuration.json"
    saved = path.read_bytes()
    cluster = runner.configuration["cluster.postgresql.cnpg.io"]
    if field == "uid":
        cluster["metadata"]["uid"] = "33333333-3333-4333-8333-333333333333"
    elif field == "generation":
        cluster["metadata"]["generation"] = 2
    elif field == "parameters":
        cluster["spec"]["postgresql"]["parameters"]["max_connections"] = "200"
    else:
        runner.configuration["configmap"]["metadata"]["resourceVersion"] = "102"
    with pytest.raises((ValueError, RuntimeError)):
        journal.execute(plan, [_component(apply)])
    assert path.read_bytes() == saved


def test_configuration_change_during_capture_does_not_publish(tmp_path):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    runner.after_configuration_read = lambda: runner.configuration["cluster.postgresql.cnpg.io"]["metadata"].update(generation=2)
    with pytest.raises(ValueError, match="CNPG"):
        journal.execute(plan, [_component(lambda _: _capture(plan, journal, runner))])
    assert not list(journal.root.rglob("application-cnpg-configuration.json"))


def test_narrower_saved_profile_is_not_reused_for_expanded_writer_admission(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import protected_cnpg_writer_configuration as configuration

    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)

    def apply(_):
        _capture(plan, journal, runner)
        raise RuntimeError("interrupted after configuration binding")

    with monkeypatch.context() as previous:
        previous.setattr(configuration, "_PROFILE", "cnpg-1.25.1-staging-declared-sql-writers-v1")
        with pytest.raises(RuntimeError, match="interrupted after"):
            journal.execute(plan, [_component(apply)])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-configuration.json"
    saved = path.read_bytes()
    with pytest.raises(RuntimeError, match="record cannot be replaced"):
        journal.execute(plan, [_component(apply)])
    assert path.read_bytes() == saved
    assert runner.calls == []


def test_credential_recovery_requires_configuration_before_live_secret_reads(tmp_path):
    from tests.loom_cli.rollout.operator.test_application_credential_recovery import _read
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    runner.configuration["cluster.postgresql.cnpg.io"]["spec"]["managed"] = {
        "roles": [{"name": "loom", "login": True}]
    }
    with pytest.raises(ValueError):
        journal.execute(plan, [_component(lambda _: _read(plan, journal, runner))])
    assert runner.calls == []
    assert not list(journal.root.rglob("application-credentials.json"))


@pytest.mark.parametrize("pagination", ["continue", "remainingItemCount"])
@pytest.mark.parametrize("resource", ["databases", "poolers", "publications", "subscriptions"])
def test_partial_writer_inventory_is_not_treated_as_empty(tmp_path, pagination, resource):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    runner.configuration[resource + ".postgresql.cnpg.io"]["metadata"][pagination] = (
        "next-page" if pagination == "continue" else 1
    )
    with pytest.raises(ValueError, match="CNPG"):
        journal.execute(plan, [_component(lambda _: _capture(plan, journal, runner))])
    assert not list(journal.root.rglob("application-cnpg-configuration.json"))


@pytest.mark.parametrize("resource", ["databases", "poolers", "publications", "subscriptions"])
def test_foreign_writer_is_preserved_and_status_only_updates_do_not_drift_binding(tmp_path, resource):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    foreign = {"spec": {"cluster": {"name": "unrelated-database"}}}
    runner.configuration[resource + ".postgresql.cnpg.io"]["items"] = [foreign]
    before = copy.deepcopy(runner.configuration)

    def apply(_):
        _capture(plan, journal, runner)
        raise RuntimeError("interrupted after configuration binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    assert runner.configuration == before
    runner.configuration["cluster.postgresql.cnpg.io"]["metadata"]["resourceVersion"] = "105"
    runner.configuration["cluster.postgresql.cnpg.io"]["status"]["readyInstances"] = 3
    runner.configuration[resource + ".postgresql.cnpg.io"]["metadata"]["resourceVersion"] = "105"
    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    assert runner.configuration[resource + ".postgresql.cnpg.io"]["items"] == [foreign]


def test_configuration_binding_must_be_durable_before_return(tmp_path, monkeypatch):
    import os
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)
    path = journal.root / "00-application-ownership-handoff/application-cnpg-configuration.json"
    fsync = os.fsync
    returned = []

    def fail_sync(fd):
        if path.exists() and os.fstat(fd).st_ino == path.stat().st_ino:
            raise OSError("configuration fsync failure")
        fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_sync)
    with pytest.raises(OSError, match="fsync"):
        journal.execute(plan, [_component(lambda _: returned.append(_capture(plan, journal, runner)))])
    assert returned == []
    assert not (path.parent / "terminal.json").exists()


def test_saved_boolean_generation_is_not_equivalent_to_integer(tmp_path):
    plan, live = _sources(tmp_path)
    journal, runner = _journal(tmp_path), ConfigurationRunner(live)

    def apply(_):
        _capture(plan, journal, runner)
        raise RuntimeError("interrupted after configuration binding")

    with pytest.raises(RuntimeError, match="interrupted after"):
        journal.execute(plan, [_component(apply)])
    path = journal.root / "00-application-ownership-handoff/application-cnpg-configuration.json"
    value = json.loads(path.read_bytes())
    value["binding"]["cluster_generation"] = True
    path.write_text(json.dumps(value))
    with pytest.raises(RuntimeError, match=r"CNPG.*readback"):
        journal.execute(plan, [_component(apply)])
