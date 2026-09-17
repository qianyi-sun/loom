"""Bind declared staging CNPG SQL-writer inputs to the existing handoff intent.

This is NOT controller quiescence or complete CNPG admission. The outer protected
operation must serialize configuration/Secret/admin writers, admit installed
operator and instance processes/images/volumes, and retire previously queued
controller work. It must independently inspect actual extensions, role settings,
private definitions and sessions. Matching Kubernetes observations prove none of
those conditions. No Kubernetes or database mutations occur here.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .final_gate_plan import FinalGatePlan

if TYPE_CHECKING:
    from .protected_application_credential_recovery import CredentialRecoveryRunner
    from .protected_apply_journal import ProtectedApplyJournal

_NAMESPACE = "loom-staging"
_CLUSTER = "loom-postgres"
_SECRET = "loom-postgres-cnpg-credentials"
_MONITORING = "cnpg-default-monitoring"
_UID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RV = re.compile(r"[1-9][0-9]{0,31}\Z")
# Independent upstream v1.25.1 config/manager/default-monitoring.yaml, data.queries.
# Never replace this with an observed live query digest. The fixture preserves
# upstream bytes so tests check this reference without network or live SQL.
_MONITORING_SHA256 = "9d568e8924bb890d9e15bcff67314f98b927142680461a8a949d5f81ed029592"
_PROFILE = "cnpg-1.25.1-staging-declared-sql-writers-v2"
_KNOWN_SPEC_FIELDS = frozenset({
    "affinity", "bootstrap", "enablePDB", "enableSuperuserAccess", "failoverDelay",
    "imageName", "instances", "logLevel", "managed", "maxSyncReplicas", "minSyncReplicas",
    "monitoring", "postgresGID", "postgresUID", "postgresql", "primaryUpdateMethod",
    "primaryUpdateStrategy", "replicationSlots", "resources", "smartShutdownTimeout",
    "startDelay", "stopDelay", "storage", "superuserSecret", "switchoverDelay",
})
# Review permits ordinary CNPG logging/replication knobs, not arbitrary extension
# GUCs, preload hooks, archive commands, search paths, or connection settings.
_FIXED_PARAMETERS = {
    "archive_mode": "on", "archive_timeout": "5min", "dynamic_shared_memory_type": "posix",
    "full_page_writes": "on", "log_destination": "csvlog", "log_directory": "/controller/log",
    "log_filename": "postgres", "log_rotation_age": "0", "log_rotation_size": "0",
    "log_truncate_on_rotation": "false", "logging_collector": "on", "shared_buffers": "256MB",
    "shared_memory_type": "mmap", "shared_preload_libraries": "", "ssl_max_protocol_version": "TLSv1.3",
    "ssl_min_protocol_version": "TLSv1.3", "wal_keep_size": "512MB", "wal_level": "logical",
    "wal_log_hints": "on", "wal_receiver_timeout": "5s", "wal_sender_timeout": "5s",
    "password_encryption": "scram-sha-256", "scram_iterations": "4096",
}
_INTEGER_PARAMETERS = frozenset({
    "max_connections", "max_parallel_workers", "max_replication_slots", "max_worker_processes",
})


@dataclass(frozen=True, slots=True)
class CNPGWriterConfigurationBinding:
    cluster_uid: str
    cluster_generation: int
    monitoring_uid: str
    monitoring_resource_version: str
    configuration_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.cluster_uid, str) or _UID.fullmatch(self.cluster_uid) is None
            or type(self.cluster_generation) is not int or self.cluster_generation < 1
            or not isinstance(self.monitoring_uid, str) or _UID.fullmatch(self.monitoring_uid) is None
            or not isinstance(self.monitoring_resource_version, str)
            or _RV.fullmatch(self.monitoring_resource_version) is None
            or not isinstance(self.configuration_sha256, str)
            or _SHA.fullmatch(self.configuration_sha256) is None
        ):
            raise ValueError("CNPG writer configuration binding is invalid")


    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CNPGWriterConfigurationBinding:
        if (set(value) != set(cls.__dataclass_fields__) or type(value['cluster_generation']) is not int
                or any(not isinstance(value[key], str) for key in value if key != 'cluster_generation')):
            raise ValueError('CNPG writer configuration binding fields are invalid')
        return cls(str(value['cluster_uid']), int(str(value['cluster_generation'])),
                   str(value['monitoring_uid']), str(value['monitoring_resource_version']),
                   str(value['configuration_sha256']))


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("CNPG writer configuration object is invalid")
    return value


def _json(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > 4 * 1024 * 1024:
        raise ValueError("CNPG writer configuration size is invalid")

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("CNPG writer configuration repeats fields")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError("CNPG writer configuration has nonfinite numbers")

    return _mapping(json.loads(payload, object_pairs_hook=unique, parse_constant=reject_constant))


def _metadata(value: Mapping[str, object], *, kind: str, name: str) -> dict[str, object]:
    metadata = _mapping(value.get("metadata"))
    if (
        value.get("kind") != kind or metadata.get("name") != name
        or metadata.get("namespace") != _NAMESPACE
        or metadata.get("deletionTimestamp") is not None
    ):
        raise ValueError("CNPG writer configuration identity changed")
    for key, pattern in (("uid", _UID), ("resourceVersion", _RV)):
        item = metadata.get(key)
        if not isinstance(item, str) or pattern.fullmatch(item) is None:
            raise ValueError("CNPG writer configuration identity is invalid")
    return metadata


def _validate_cluster(cluster: Mapping[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    metadata = _metadata(cluster, kind="Cluster", name=_CLUSTER)
    spec = _mapping(cluster.get("spec"))
    if cluster.get("apiVersion") != "postgresql.cnpg.io/v1" or set(spec) - _KNOWN_SPEC_FIELDS:
        raise ValueError("CNPG writer configuration profile is unsupported")
    if (
        spec.get("imageName") not in {
            "ghcr.io/cloudnative-pg/postgresql:17.4",
            "ghcr.io/cloudnative-pg/postgresql@sha256:3c0ba08ea353c9705a755c113e4ae395be76553e0ed68076e5410cb09b9d17d9",
        }
        or spec.get("enableSuperuserAccess") is not False
        or spec.get("superuserSecret") != {"name": _SECRET}
        or _mapping(spec.get("managed", {})) not in ({}, {"roles": []})
    ):
        raise ValueError("CNPG role or credential writer configuration is unsupported")
    bootstrap = _mapping(spec.get("bootstrap"))
    if set(bootstrap) != {"initdb"}:
        raise ValueError("CNPG bootstrap writer configuration is unsupported")
    initdb = _mapping(bootstrap["initdb"])
    if (
        set(initdb) - {"database", "owner", "secret", "encoding", "localeCType", "localeCollate"}
        or initdb.get("database") != "loom" or initdb.get("owner") != "loom"
        or initdb.get("secret") != {"name": _SECRET}
        or initdb.get("encoding", "UTF8") != "UTF8"
        or initdb.get("localeCType", "C") != "C" or initdb.get("localeCollate", "C") != "C"
    ):
        raise ValueError("CNPG application bootstrap writer configuration is unsupported")
    postgres = _mapping(spec.get("postgresql"))
    if set(postgres) - {"parameters", "syncReplicaElectionConstraint"}:
        raise ValueError("CNPG PostgreSQL writer configuration is unsupported")
    for name, setting in _mapping(postgres.get("parameters")).items():
        if name in _INTEGER_PARAMETERS:
            if not isinstance(setting, str) or re.fullmatch(r"[1-9][0-9]{0,3}", setting) is None:
                raise ValueError("CNPG PostgreSQL parameter is unsupported")
        elif name not in _FIXED_PARAMETERS or setting != _FIXED_PARAMETERS[name]:
            raise ValueError("CNPG PostgreSQL writer parameter is unsupported")
    monitoring = _mapping(spec.get("monitoring"))
    if (
        set(monitoring) - {"disableDefaultQueries", "enablePodMonitor", "customQueriesConfigMap"}
        or monitoring.get("disableDefaultQueries") is not False
        or type(monitoring.get("enablePodMonitor", False)) is not bool
        or monitoring.get("customQueriesConfigMap") != [{"name": _MONITORING, "key": "queries"}]
    ):
        raise ValueError("CNPG monitoring writer configuration is unsupported")
    status = _mapping(cluster.get("status", {}))
    integrations = _mapping(status.get("poolerIntegrations", {}))
    if (
        integrations not in ({}, {"pgBouncerIntegration": {}}, {"pgBouncerIntegration": {"secrets": []}})
        or _mapping(status.get("managedRolesStatus", {})) != {}
    ):
        raise ValueError("CNPG controller writer status is not empty")
    return metadata, {"spec": spec, "annotations": metadata.get("annotations", {}),
                      "poolerIntegrations": integrations, "profile": _PROFILE}


def _require_no_target_resources(value: Mapping[str, object], *, kind: str) -> None:
    items = value.get("items")
    metadata = _mapping(value.get("metadata"))
    version = metadata.get("resourceVersion")
    if (
        value.get("apiVersion") != "postgresql.cnpg.io/v1" or value.get("kind") != kind
        or not isinstance(items, list)
        or not isinstance(version, str) or _RV.fullmatch(version) is None
        or metadata.get("continue", "") != ""
        or ("remainingItemCount" in metadata and
            (type(metadata["remainingItemCount"]) is not int or metadata["remainingItemCount"] != 0))
    ):
        raise ValueError("CNPG writer resource inventory is incomplete")
    for item in items:
        name = _mapping(_mapping(_mapping(item).get("spec")).get("cluster")).get("name")
        if not isinstance(name, str) or not name or name == _CLUSTER:
            raise ValueError("CNPG target has an additional declared SQL writer")


def _observe(runner: CredentialRecoveryRunner) -> CNPGWriterConfigurationBinding:
    def read(resource: str, name: str | None = None) -> dict[str, object]:
        argv = ["kubectl", "--namespace", _NAMESPACE, "get", resource]
        if name is not None:
            argv.append(name)
        argv.extend(("--output=json", "--request-timeout=30s"))
        return _json(runner.capture_stdout(argv, env=runner.environment, timeout_seconds=30))

    cluster = read("cluster.postgresql.cnpg.io", _CLUSTER)
    metadata, inputs = _validate_cluster(cluster)
    for resource, kind in (("databases", "DatabaseList"), ("poolers", "PoolerList"),
                           ("publications", "PublicationList"), ("subscriptions", "SubscriptionList")):
        # Ordinary kubectl get flattens lists and drops their resourceVersion.
        # Raw fixed endpoints preserve the API completeness/pagination evidence.
        payload = runner.capture_stdout(
            ["kubectl", "--namespace", _NAMESPACE, "get",
             f"--raw=/apis/postgresql.cnpg.io/v1/namespaces/{_NAMESPACE}/{resource}",
             "--request-timeout=30s"],
            env=runner.environment, timeout_seconds=30,
        )
        _require_no_target_resources(_json(payload), kind=kind)
    monitoring = read("configmap", _MONITORING)
    monitor_metadata = _metadata(monitoring, kind="ConfigMap", name=_MONITORING)
    data = _mapping(monitoring.get("data"))
    queries = data.get("queries")
    if (
        monitoring.get("apiVersion") != "v1" or set(data) != {"queries"}
        or monitoring.get("binaryData", {}) != {} or not isinstance(queries, str)
        or hashlib.sha256(queries.encode()).hexdigest() != _MONITORING_SHA256
    ):
        raise ValueError("CNPG monitoring SQL differs from the independent upstream reference")
    inputs["monitoring_sha256"] = _MONITORING_SHA256
    uid, generation = metadata["uid"], metadata.get("generation")
    monitor_uid, monitor_rv = monitor_metadata["uid"], monitor_metadata["resourceVersion"]
    if not isinstance(uid, str) or type(generation) is not int:
        raise ValueError("CNPG cluster identity is invalid")
    assert isinstance(monitor_uid, str) and isinstance(monitor_rv, str)
    return CNPGWriterConfigurationBinding(
        cluster_uid=uid, cluster_generation=generation,
        monitoring_uid=monitor_uid, monitoring_resource_version=monitor_rv,
        configuration_sha256=hashlib.sha256(json.dumps(inputs, sort_keys=True, separators=(",", ":"),
                                                     allow_nan=False).encode()).hexdigest(),
    )


def capture_cnpg_writer_configuration(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, runner: CredentialRecoveryRunner
) -> CNPGWriterConfigurationBinding:
    """Require an active protected intent, validate twice, then bind immutably.

    Ignores changing Cluster status RV/health counters, not spec generation or
    writer inputs. Foreign declared-writer objects are not modified or adopted.
    This does not freeze Kubernetes inputs or drain cached controller actions.
    """
    journal.require_application_credential_context(plan)
    first = observe_cnpg_writer_configuration(runner)
    journal.record_application_cnpg_configuration(plan, binding=first)
    return first


def observe_cnpg_writer_configuration(runner: CredentialRecoveryRunner) -> CNPGWriterConfigurationBinding:
    """Read twice without publishing authority or certifying writer exclusion."""
    try:
        first, second = _observe(runner), _observe(runner)
        if first != second:
            raise ValueError("CNPG writer configuration changed during capture")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError):
        raise ValueError("CNPG writer configuration validation failed") from None
    return first
