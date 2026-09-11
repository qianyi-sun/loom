"""Journaled Kubernetes shutdown and recovery for the fixed staging writers.

Called only within the original admitted application handoff, after SQL/profile
and external writer exclusion. It preserves the guard-owned CronJob suspension.
It neither retires CNPG work nor releases an input fence or the database guard.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol

from loom.application_database_admission import _require_coordination_guard
from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_handoff_completion import ApplicationHandoffDatabaseOutcome
from loom.staging_mutation_coordination import rollout_guard_application_name

from .final_gate_plan import FinalGatePlan
from .protected_application_readiness import ApplicationDeployment, deployment_is_ready
from .protected_application_workloads import (
    APPLICATION_CRONJOB,
    APPLICATION_DEPLOYMENTS,
    ApplicationWorkload,
    _mapping,
)
from .protected_apply_journal import ProtectedApplyJournal
from .staging_mutation_guard import MutationGuardEvidence

_NAMESPACE = "loom-staging"
_SECONDS = 180.0
_LISTS = {
    "Deployment": "/apis/apps/v1/namespaces/loom-staging/deployments",
    "CronJob": "/apis/batch/v1/namespaces/loom-staging/cronjobs",
    "Job": "/apis/batch/v1/namespaces/loom-staging/jobs",
    "ReplicaSet": "/apis/apps/v1/namespaces/loom-staging/replicasets",
    "Pod": "/api/v1/namespaces/loom-staging/pods",
    "HorizontalPodAutoscaler": "/apis/autoscaling/v2/namespaces/loom-staging/horizontalpodautoscalers",
}
_API_VERSIONS = {"Deployment": "apps/v1", "ReplicaSet": "apps/v1", "CronJob": "batch/v1",
                 "Job": "batch/v1", "Pod": "v1", "HorizontalPodAutoscaler": "autoscaling/v2"}


class ApplicationWorkloadRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float) -> bytes: ...

    def capture_stdout_with_input(self, argv: Sequence[str], *, env: Mapping[str, str],
                                  input_payload: bytes, timeout_seconds: float) -> bytes: ...

    def open_staging_peer_maintenance_database(self) -> AbstractContextManager[ApplicationDatabaseConnection]: ...

    def recover_and_complete_staging_application_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, guard: MutationGuardEvidence,
    ) -> ApplicationHandoffDatabaseOutcome: ...


def _decode(payload: bytes) -> dict[str, object]:
    if not isinstance(payload, bytes) or not payload or len(payload) > 4 * 1024 * 1024:
        raise ValueError("application workload response is empty or unbounded")
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("application workload response repeats fields")
            value[key] = item
        return value
    def nonfinite(_: str) -> object:
        raise ValueError("application workload response contains a non-finite number")
    return _mapping(json.loads(payload, object_pairs_hook=unique, parse_constant=nonfinite))


def _live_guard(plan: FinalGatePlan, journal: ProtectedApplyJournal,
                runner: ApplicationWorkloadRunner, guard: MutationGuardEvidence) -> None:
    journal.require_application_guard_retained(plan, guard=guard)
    original = journal.read_application_admission_recovery()
    if (original is None or original.coordination_guard is None or original.target.database != "loom"
            or original.target.owner_role != "loom"
            or original.coordination_guard.backend.pid != guard.database_backend_pid
            or original.coordination_guard.application_name != rollout_guard_application_name(
                request_id=guard.request_id, candidate_sha=guard.candidate_sha,
                candidate_tree=guard.candidate_tree, generation=guard.generation)):
        raise RuntimeError("application workloads require the original admitted database guard")
    with runner.open_staging_peer_maintenance_database() as connection, connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        _require_coordination_guard(connection, original.target, original.coordination_guard)
    journal.require_application_guard_retained(plan, guard=guard)


def _list(runner: ApplicationWorkloadRunner, kind: str) -> list[dict[str, object]]:
    value = _decode(runner.capture_stdout(
        ("kubectl", "get", f"--raw={_LISTS[kind]}", "--request-timeout=30s"),
        env=runner.environment, timeout_seconds=30,
    ))
    metadata, items = _mapping(value.get("metadata")), value.get("items")
    if (value.get("kind") != kind + "List" or value.get("apiVersion") != _API_VERSIONS[kind]
            or not isinstance(items, list) or len(items) > 1024
            or not isinstance(metadata.get("resourceVersion"), str) or not metadata["resourceVersion"]
            or metadata.get("continue", "") != "" or metadata.get("remainingItemCount", 0) != 0):
        raise ValueError("application workload list is incomplete")
    documents = []
    for item in items:
        document = _mapping(item)
        # Typed Kubernetes lists omit TypeMeta on members. Bind it to this
        # validated fixed endpoint/envelope; never accept conflicting metadata.
        if (document.get("kind", kind) != kind
                or document.get("apiVersion", _API_VERSIONS[kind]) != _API_VERSIONS[kind]):
            raise ValueError("application workload list member type changed")
        documents.append({**document, "kind": kind, "apiVersion": _API_VERSIONS[kind]})
    return documents


def _owner(document: Mapping[str, object]) -> tuple[str, str, str] | None:
    refs = _mapping(document.get("metadata")).get("ownerReferences", [])
    if not isinstance(refs, list):
        raise ValueError("application workload owner list is invalid")
    controllers = [_mapping(item) for item in refs if _mapping(item).get("controller") is True]
    if len(controllers) > 1:
        raise ValueError("application workload has ambiguous controller ownership")
    if not controllers:
        return None
    ref = controllers[0]
    if any(not isinstance(ref.get(key), str) or not ref[key] for key in ("kind", "name", "uid")):
        raise ValueError("application workload controller identity is invalid")
    return str(ref["kind"]), str(ref["name"]), str(ref["uid"])


def _runtime_secret(value: object) -> bool:
    if isinstance(value, list):
        return any(_runtime_secret(item) for item in value)
    if not isinstance(value, dict):
        return False
    ref = value.get("secretKeyRef")
    if isinstance(ref, dict) and ref.get("name") == "loom-secrets" and ref.get("key") in {
        "cp-db-url", "cp-db-url-pool", "gw-db-url", "gw-db-url-pool", "svc-db-url", "svc-db-url-pool",
    }:
        return True
    ref = value.get("secretRef")
    if isinstance(ref, dict) and ref.get("name") == "loom-secrets":
        return True
    # A whole Secret mounted as files can supply the same credentials.
    ref = value.get("secret")
    if isinstance(ref, dict) and (ref.get("secretName") == "loom-secrets" or ref.get("name") == "loom-secrets"):
        return True
    return any(_runtime_secret(item) for item in value.values())


def _terminal_job(document: Mapping[str, object]) -> bool:
    status = _mapping(document.get("status", {}))
    conditions = status.get("conditions", [])
    if not isinstance(conditions, list):
        raise ValueError("application workload Job conditions are invalid")
    return any(_mapping(item).get("type") in {"Complete", "Failed"}
               and _mapping(item).get("status") == "True" for item in conditions)


def _active_pod(document: Mapping[str, object]) -> bool:
    status = _mapping(document.get("status", {}))
    if status.get("phase") not in {"Succeeded", "Failed"}:
        return True
    for key in ("containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses"):
        entries = status.get(key, [])
        if not isinstance(entries, list):
            raise ValueError("application workload Pod status is invalid")
        if any("running" in _mapping(_mapping(item).get("state", {})) for item in entries):
            return True
    return False


def _observe(plan: FinalGatePlan, runner: ApplicationWorkloadRunner, guard: MutationGuardEvidence,
             saved: tuple[ApplicationWorkload, ...]) -> tuple[dict[tuple[str, str], dict[str, object]], bool]:
    lists = {kind: _list(runner, kind) for kind in _LISTS}
    objects: dict[tuple[str, str], dict[str, object]] = {}
    roots: dict[str, tuple[str, str]] = {}
    for kind, names in (("Deployment", APPLICATION_DEPLOYMENTS), ("CronJob", {APPLICATION_CRONJOB})):
        for document in lists[kind]:
            metadata = _mapping(document.get("metadata"))
            name = metadata.get("name")
            if name not in names:
                if _runtime_secret(document.get("spec")):
                    raise RuntimeError("application workload inventory contains an unknown credential consumer")
                continue
            snapshot = ApplicationWorkload.capture(document)
            if _owner(document) is not None or (kind, snapshot.name) in objects:
                raise RuntimeError("application workload controller identity is not exclusive")
            objects[(kind, snapshot.name)] = document
            roots[snapshot.uid] = (kind, snapshot.name)
    if set(objects) != ({("Deployment", name) for name in APPLICATION_DEPLOYMENTS} | {("CronJob", APPLICATION_CRONJOB)}):
        raise RuntimeError("application workload fixed writer inventory is incomplete")
    cron = objects[("CronJob", APPLICATION_CRONJOB)]
    if (_mapping(cron["metadata"])["uid"] != guard.cronjob_uid
            or _mapping(cron["spec"]).get("suspend") is not True):
        raise RuntimeError("application workload guard-owned CronJob suspension changed")
    for hpa in lists["HorizontalPodAutoscaler"]:
        target = _mapping(_mapping(hpa.get("spec")).get("scaleTargetRef"))
        if (target.get("kind"), target.get("name")) in objects:
            raise RuntimeError("application workload has an active external scale controller")
    for document in lists["Job"]:
        metadata = _mapping(document.get("metadata"))
        name = metadata.get("name")
        owner = _owner(document)
        controlled = owner == ("CronJob", APPLICATION_CRONJOB, guard.cronjob_uid)
        exact_migration = (name == plan.migration_job_name and owner is None
                           and _mapping(metadata.get("labels", {})).get("app") == "loom-migration")
        if not controlled and not exact_migration:
            if not _terminal_job(document) and _runtime_secret(document.get("spec")):
                raise RuntimeError("application workload has an unowned active migration Job")
            continue
        uid = metadata.get("uid")
        if not isinstance(uid, str) or not isinstance(name, str):
            raise ValueError("application workload Job identity is invalid")
        roots[uid] = ("Job", name)
        if not _terminal_job(document) or any(item.uid == uid for item in saved):
            ApplicationWorkload.capture(document)
            objects[("Job", name)] = document
    for document in lists["ReplicaSet"]:
        owner = _owner(document)
        if owner is not None and roots.get(owner[2]) == owner[:2] and owner[0] == "Deployment":
            metadata = _mapping(document.get("metadata"))
            uid, name = metadata.get("uid"), metadata.get("name")
            if not isinstance(uid, str) or not isinstance(name, str):
                raise ValueError("application workload ReplicaSet identity is invalid")
            roots[uid] = ("ReplicaSet", name)
    roots.update({item.uid: (item.kind, item.name) for item in saved})
    missing_jobs = {item.uid for item in saved if item.kind == "Job" and (item.kind, item.name) not in objects}
    active = False
    for document in lists["Pod"]:
        if not _active_pod(document):
            continue
        owner = _owner(document)
        if owner is not None and roots.get(owner[2]) == owner[:2]:
            if owner[0] == "Job" and owner[2] in missing_jobs:
                raise RuntimeError("application workload deleted Job still has an active Pod")
            active = True
        elif _runtime_secret(document.get("spec")):
            raise RuntimeError("application workload has an unowned live database client Pod")
    return objects, active


def _require_saved_guard_cron(saved: tuple[ApplicationWorkload, ...], guard: MutationGuardEvidence) -> None:
    cron = [item for item in saved if item.kind == "CronJob" and item.name == APPLICATION_CRONJOB]
    if (len(cron) != 1 or cron[0].uid != guard.cronjob_uid
            or cron[0].original_value is not True or not cron[0].original_present):
        raise RuntimeError("application workload baseline would change the guard-owned CronJob suspension")


def _require_recovered_endpoints(saved: tuple[ApplicationWorkload, ...],
                                 objects: Mapping[tuple[str, str], dict[str, object]]) -> None:
    if set(objects) - {(item.kind, item.name) for item in saved}:
        raise RuntimeError("application workload inventory changed during recovery")
    for item in saved:
        current = objects.get((item.kind, item.name))
        if current is None and item.kind == "Job":
            continue
        if current is None or item.patch(current, recovering=True) is not None:
            raise RuntimeError("application workload original endpoint changed after readiness")


def _patch(plan: FinalGatePlan, journal: ProtectedApplyJournal, runner: ApplicationWorkloadRunner,
           guard: MutationGuardEvidence, saved: ApplicationWorkload, current: Mapping[str, object],
           *, recovering: bool) -> bool:
    patch = saved.patch(current, recovering=recovering)
    if patch is None:
        return False
    _live_guard(plan, journal, runner, guard)
    observed = _decode(runner.capture_stdout_with_input(
        ("kubectl", "--namespace", _NAMESPACE, "patch", saved.resource, saved.name,
         "--type=json", "--patch-file=/dev/stdin", "--output=json", "--request-timeout=30s"),
        env=runner.environment, input_payload=json.dumps(patch, separators=(",", ":")).encode(), timeout_seconds=30,
    ))
    if saved.patch(observed, recovering=recovering) is not None:
        raise RuntimeError("application workload patch did not reach its exact endpoint")
    _live_guard(plan, journal, runner, guard)
    return True


def pause_application_workloads(plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
                               runner: ApplicationWorkloadRunner, guard: MutationGuardEvidence) -> None:
    """Keep originals durable, catch late CronJob children, and observe Pod drain."""
    if journal.application_workloads_restoring(plan):
        raise RuntimeError("application workloads are already restoring; cannot pause again")
    deadline = time.monotonic() + _SECONDS
    while True:
        _live_guard(plan, journal, runner, guard)
        saved = journal.read_application_workloads(plan)
        if saved:
            _require_saved_guard_cron(saved, guard)
        objects, active = _observe(plan, runner, guard, saved)
        if not saved:
            journal.record_application_workloads(plan, workloads=tuple(
                ApplicationWorkload.capture(document) for document in objects.values()))
        else:
            known = {(item.kind, item.name) for item in saved}
            for key, document in objects.items():
                if key not in known:
                    journal.record_application_workload_job(plan, workload=ApplicationWorkload.capture(document))
        saved = journal.read_application_workloads(plan)
        _require_saved_guard_cron(saved, guard)
        changed = False
        for item in sorted(saved, key=lambda value: ({"CronJob": 0, "Job": 1, "Deployment": 2}[value.kind], value.name)):
            current = objects.get((item.kind, item.name))
            if current is None and item.kind == "Job":
                continue  # Never recreate a Job removed by its terminal TTL controller.
            if current is None:
                raise RuntimeError("application workload original controller disappeared")
            changed = _patch(plan, journal, runner, guard, item, current, recovering=False) or changed
        if not changed and not active:
            _live_guard(plan, journal, runner, guard)
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("application workload shutdown is still draining")
        time.sleep(0.25)


def restore_application_workloads(plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
                                 runner: ApplicationWorkloadRunner, guard: MutationGuardEvidence) -> None:
    """Recheck actual database completion, then recover only original workloads."""
    _live_guard(plan, journal, runner, guard)
    saved = journal.read_application_workloads(plan)
    if not saved:
        raise RuntimeError("application workload recovery lacks its original inventory")
    _require_saved_guard_cron(saved, guard)
    outcome = runner.recover_and_complete_staging_application_database(plan, journal=journal, guard=guard)
    original = journal.read_application_admission_recovery()
    if (original is None or type(outcome) is not ApplicationHandoffDatabaseOutcome
            or outcome.target != original.target or outcome.coordination_guard != original.coordination_guard):
        raise RuntimeError("application workload database completion identity changed")
    journal.begin_application_workload_restoration(plan)
    deadline = time.monotonic() + _SECONDS
    while True:
        _live_guard(plan, journal, runner, guard)
        objects, _ = _observe(plan, runner, guard, saved)
        if set(objects) - {(item.kind, item.name) for item in saved}:
            raise RuntimeError("application workload inventory changed during recovery")
        changed, ready = False, True
        for item in sorted(saved, key=lambda value: ({"Deployment": 0, "Job": 1, "CronJob": 2}[value.kind], value.name)):
            current = objects.get((item.kind, item.name))
            if current is None and item.kind == "Job":
                continue
            if current is None:
                raise RuntimeError("application workload original controller disappeared")
            changed = _patch(plan, journal, runner, guard, item, current, recovering=True) or changed
            if item.kind == "Deployment" and not changed:
                desired = ApplicationDeployment(item.name, _NAMESPACE, int(item.original_value),
                                                _mapping(_mapping(current["spec"])["template"]))
                ready = deployment_is_ready(desired, runner=runner, environment=runner.environment, timeout_seconds=30) and ready
        if not changed and ready:
            verified, _ = _observe(plan, runner, guard, saved)
            _require_recovered_endpoints(saved, verified)
            _live_guard(plan, journal, runner, guard)
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("application workload recovery is awaiting its original serving generation")
        time.sleep(0.25)
