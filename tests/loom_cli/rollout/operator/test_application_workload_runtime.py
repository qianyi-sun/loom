"""Run workload pause/recovery through the real protected component journal."""

import copy
import json
import os
from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import pytest

from loom.application_handoff_completion import ApplicationHandoffDatabaseOutcome
from loom.staging_mutation_coordination import rollout_guard_application_name
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _component,
    _handoff,
    _target,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _guard as _database_guard,
)
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
from tests.loom_cli.rollout.operator.test_application_workloads import _patched, _workload


class Runner:
    def __init__(self, guard, database_guard):
        self.environment = {"KUBECONFIG": "/fixture"}
        self.guard = database_guard
        self.objects = [_workload(name=name) for name in (
            "loom-capacity-agent", "loom-control-plane", "loom-family-orchestrator",
            "loom-llm-gateway", "loom-pgbouncer", "loom-pipeline-orchestrator", "loom-service",
        )]
        cron = _workload("CronJob", "loom-staging-data-lifecycle", True)
        cron["metadata"]["uid"] = guard.cronjob_uid
        self.objects.append(cron)
        self.patch_calls = []
        self.fail_after = None
        self.guard_alive = True
        self.late_job = False
        self.injected = False
        self._ready()

    def _ready(self):
        for obj in self.objects:
            if obj["kind"] == "Deployment":
                n = obj["spec"]["replicas"]
                obj["status"] = {"observedGeneration": obj["metadata"]["generation"],
                                 "replicas": n, "updatedReplicas": n, "readyReplicas": n, "availableReplicas": n}

    @contextmanager
    def open_staging_peer_maintenance_database(self):
        runner = self
        class Peer:
            @contextmanager
            def transaction(self):
                yield
            def execute(self, statement):
                rendered = statement if isinstance(statement, str) else statement.as_string()
                row = (True,)
                if rendered.startswith("SELECT a.pid"):
                    b = runner.guard.backend
                    row = (b.pid, b.started_at, b.system_identifier, b.server_started_at, b.database_oid, runner.guard.role_oid) if runner.guard_alive else None
                class Rows:
                    def fetchone(self):
                        return row
                return Rows()
        yield Peer()

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        raw = next((arg for arg in argv if arg.startswith("--raw=")), None)
        if raw:
            resource = raw.rsplit("/", 1)[1]
            kind = {"deployments": "Deployment", "cronjobs": "CronJob", "jobs": "Job",
                    "replicasets": "ReplicaSet", "pods": "Pod", "horizontalpodautoscalers": "HorizontalPodAutoscaler"}[resource]
            version = {"Deployment": "apps/v1", "ReplicaSet": "apps/v1", "CronJob": "batch/v1",
                       "Job": "batch/v1", "Pod": "v1", "HorizontalPodAutoscaler": "autoscaling/v2"}[kind]
            return json.dumps({"apiVersion": version, "kind": kind + "List", "metadata": {"resourceVersion": "101"},
                               "items": [obj for obj in self.objects if obj["kind"] == kind]}).encode()
        name = argv[argv.index("get") + 2]
        return json.dumps(next(obj for obj in self.objects if obj["metadata"]["name"] == name)).encode()

    def capture_stdout_with_input(self, argv, *, env, input_payload, timeout_seconds):
        assert "--patch-file=/dev/stdin" in argv
        assert env == self.environment
        name = argv[argv.index("patch") + 2]
        index = next(i for i, obj in enumerate(self.objects) if obj["metadata"]["name"] == name)
        self.objects[index] = _patched(self.objects[index], json.loads(input_payload))
        self.patch_calls.append(name)
        self._ready()
        if self.late_job and not self.injected:
            self.injected = True
            job = _workload("Job", "loom-staging-data-lifecycle-456", False)
            job["metadata"]["ownerReferences"] = [{"apiVersion": "batch/v1", "kind": "CronJob", "controller": True,
                                                    "name": "loom-staging-data-lifecycle", "uid": self.guard_cron_uid}]
            self.objects.append(job)
        if self.fail_after == len(self.patch_calls):
            raise RuntimeError("lost workload patch acknowledgement")
        return json.dumps(self.objects[index]).encode()

    def recover_and_complete_staging_application_database(self, plan, *, journal, guard):
        return ApplicationHandoffDatabaseOutcome(_target(), self.guard)

    @property
    def guard_cron_uid(self):
        return next(obj["metadata"]["uid"] for obj in self.objects if obj["kind"] == "CronJob")


def _context(tmp_path):
    plan, journal = _setup(tmp_path)
    evidence = _guard(plan)
    saved = replace(_database_guard(), backend=replace(_database_guard().backend, pid=evidence.database_backend_pid),
                    application_name=rollout_guard_application_name(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
                        candidate_tree=plan.candidate_tree, generation=evidence.generation))
    runner = Runner(evidence, saved)
    def admit():
        journal.retain_application_guard(plan, guard=evidence)
        assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id, service_uid=os.getuid(), guard=evidence, acknowledge=True)
        journal.record_application_admission_recovery(target=_target(), handoff_backend=_handoff(), coordination_guard=saved)
    return plan, journal, evidence, saved, runner, admit


@pytest.mark.parametrize("change", ["typed-list", "member-kind", "member-version", "list-version"])
def test_fixed_workload_list_binds_omitted_member_types_and_refuses_conflicts(tmp_path, change):
    from loom_cli.rollout.operator.protected_application_workload_runtime import _list

    *_, runner, _admit = _context(tmp_path)
    capture = runner.capture_stdout

    def response(*args, **kwargs):
        value = json.loads(capture(*args, **kwargs))
        for item in value["items"]:
            item.pop("kind")
            item.pop("apiVersion")
        if change == "member-kind":
            value["items"][0]["kind"] = "Job"
        elif change == "member-version":
            value["items"][0]["apiVersion"] = "v1"
        elif change == "list-version":
            value["apiVersion"] = "v1"
        return json.dumps(value).encode()
    runner.capture_stdout = response
    if change == "typed-list":
        observed = _list(runner, "Deployment")
        assert len(observed) == 7
        assert all(item["kind"] == "Deployment" and item["apiVersion"] == "apps/v1" for item in observed)
    else:
        with pytest.raises(ValueError, match="list"):
            _list(runner, "Deployment")


@pytest.mark.parametrize("interruption", [None, "pause", "restore"])
def test_workload_runtime_recovers_partial_patches_and_keeps_guard_cron_suspended(tmp_path, interruption):
    from loom_cli.rollout.operator.protected_application_workload_runtime import (
        pause_application_workloads,
        restore_application_workloads,
    )

    plan, journal, evidence, _saved, runner, admit = _context(tmp_path)
    original = copy.deepcopy(runner.objects)
    if interruption == "pause":
        runner.fail_after = 2
    mode = ["pause"]
    def apply(_):
        admit()
        if mode[0] == "pause":
            pause_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
            assert all(obj["spec"]["replicas"] == 0 for obj in runner.objects if obj["kind"] == "Deployment")
            mode[0] = "restore"
            if interruption == "restore":
                runner.fail_after = len(runner.patch_calls) + 2
        restore_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        raise RuntimeError("workload recovery observed")
    component = _component(apply)
    if interruption:
        with pytest.raises(RuntimeError, match="acknowledgement"):
            journal.execute(plan, [component])
        runner.fail_after = None
    with pytest.raises(RuntimeError, match="recovery observed"):
        journal.execute(plan, [component])
    for before, after in zip(original, runner.objects, strict=True):
        assert after["metadata"]["uid"] == before["metadata"]["uid"]
        assert after["spec"] == before["spec"]
    assert runner.objects[-1]["spec"]["suspend"] is True
    assert not list(journal.root.rglob("terminal.json"))


def test_late_cronjob_child_is_journaled_and_paused_before_drain_succeeds(tmp_path):
    from loom_cli.rollout.operator.protected_application_workload_runtime import (
        pause_application_workloads,
    )

    plan, journal, evidence, _, runner, admit = _context(tmp_path)
    runner.late_job = True
    def apply(_):
        admit()
        pause_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        job = next(obj for obj in runner.objects if obj["kind"] == "Job")
        assert job["spec"]["suspend"] is True
        assert any(item.uid == job["metadata"]["uid"] and item.original_value is False for item in journal.read_application_workloads(plan))
        raise RuntimeError("late child paused")
    with pytest.raises(RuntimeError, match="late child paused"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("blocker", ["hpa", "foreign-pod", "projected-secret", "orphan-writer", "guard", "cron-uid"])
def test_workload_runtime_refuses_unknown_writers_before_any_patch(tmp_path, blocker):
    from loom_cli.rollout.operator.protected_application_workload_runtime import (
        pause_application_workloads,
    )

    plan, journal, evidence, _, runner, admit = _context(tmp_path)
    if blocker == "guard":
        runner.guard_alive = False
    elif blocker == "cron-uid":
        runner.objects[-1]["metadata"]["uid"] = str(uuid4())
    elif blocker == "hpa":
        runner.objects.append({"kind": "HorizontalPodAutoscaler", "metadata": {"name": "other-scaler"},
                               "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "loom-control-plane"}}})
    else:
        runner.objects.append({"kind": "Pod", "metadata": {"name": "foreign-writer", "uid": str(uuid4())},
            "spec": {"containers": [{"name": "foreign", "env": [{"name": "DB_URL", "valueFrom": {"secretKeyRef": {"name": "loom-secrets", "key": "cp-db-url"}}}]}]},
            "status": {"phase": "Running"}})
        if blocker == "projected-secret":
            runner.objects[-1]["spec"] = {"containers": [{"name": "foreign"}], "volumes": [
                {"name": "credentials", "projected": {"sources": [{"secret": {"name": "loom-secrets"}}]}}]}
        elif blocker == "orphan-writer":
            runner.objects[-1]["metadata"]["labels"] = {"app": "loom-control-plane"}
            runner.objects[-1]["spec"] = {"containers": [{"name": "writer"}]}
    def apply(_):
        admit()
        with pytest.raises((ValueError, RuntimeError)):
            pause_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        assert runner.patch_calls == []
        raise RuntimeError("writer refused")
    with pytest.raises(RuntimeError, match="writer refused"):
        journal.execute(plan, [_component(apply)])


def test_recovery_refuses_a_deleted_saved_job_with_surviving_active_pods(tmp_path):
    from loom_cli.rollout.operator.protected_application_workload_runtime import (
        pause_application_workloads,
        restore_application_workloads,
    )

    plan, journal, evidence, _, runner, admit = _context(tmp_path)
    job = _workload("Job", "loom-staging-data-lifecycle-456", False)
    job["metadata"]["ownerReferences"] = [{"apiVersion": "batch/v1", "kind": "CronJob", "controller": True,
                                            "name": "loom-staging-data-lifecycle", "uid": runner.guard_cron_uid}]
    runner.objects.append(job)

    def apply(_):
        admit()
        pause_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        runner.objects.remove(next(obj for obj in runner.objects if obj["kind"] == "Job"))
        runner.objects.append({"kind": "Pod", "metadata": {"name": "orphan-job-pod", "uid": str(uuid4()),
            "ownerReferences": [{"kind": "Job", "name": job["metadata"]["name"], "uid": job["metadata"]["uid"], "controller": True}]},
            "spec": {"containers": [{"name": "still-running"}]}, "status": {"phase": "Running"}})
        with pytest.raises(RuntimeError, match="deleted Job"):
            restore_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        raise RuntimeError("orphan refused")
    with pytest.raises(RuntimeError, match="orphan refused"):
        journal.execute(plan, [_component(apply)])


def test_recovery_rejects_a_baseline_that_would_release_the_guard_cronjob(tmp_path):
    from loom_cli.rollout.operator.protected_application_workload_runtime import (
        restore_application_workloads,
    )
    from loom_cli.rollout.operator.protected_application_workloads import ApplicationWorkload

    plan, journal, evidence, _, runner, admit = _context(tmp_path)
    def apply(_):
        admit()
        saved = tuple(ApplicationWorkload.capture(obj) for obj in runner.objects)
        saved = tuple(replace(obj, original_value=False) if obj.kind == "CronJob" else obj for obj in saved)
        journal.record_application_workloads(plan, workloads=saved)
        with pytest.raises((ValueError, RuntimeError)):
            restore_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        assert runner.patch_calls == []
        assert runner.objects[-1]["spec"]["suspend"] is True
        raise RuntimeError("unsafe baseline refused before mutation")
    with pytest.raises(RuntimeError, match="unsafe baseline refused"):
        journal.execute(plan, [_component(apply)])


def test_recovery_rechecks_saved_uid_after_serving_generation_read(tmp_path):
    from loom_cli.rollout.operator.protected_application_workload_runtime import (
        pause_application_workloads,
        restore_application_workloads,
    )

    plan, journal, evidence, _, runner, admit = _context(tmp_path)
    capture = runner.capture_stdout
    def replaced_at_readiness(argv, **kwargs):
        if "deployment" in argv and "loom-control-plane" in argv:
            current = next(obj for obj in runner.objects if obj["metadata"]["name"] == "loom-control-plane")
            current["metadata"]["uid"] = str(uuid4())
        return capture(argv, **kwargs)
    def apply(_):
        admit()
        pause_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        runner.capture_stdout = replaced_at_readiness
        with pytest.raises((ValueError, RuntimeError)):
            restore_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
        raise RuntimeError("replacement cannot satisfy recovery")
    with pytest.raises(RuntimeError, match="replacement cannot"):
        journal.execute(plan, [_component(apply)])
