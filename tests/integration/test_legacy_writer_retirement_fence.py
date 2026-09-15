"""Permanent old-writer admission refusal in a disposable Kubernetes API."""

import copy
import os
import time

import pytest

from loom_cli.rollout.operator.protected_legacy_writer_fence import render_legacy_writer_fence
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

_CP_IMAGE = "registry.example.test/loom-control-plane@sha256:" + "a" * 64


def _deployment(name, *, image=_CP_IMAGE, cutover=False, replicas=1):
    return {
        "apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name},
        "spec": {"replicas": replicas, "selector": {"matchLabels": {"app": name}},
                 "template": {"metadata": {"labels": {"app": name}}, "spec": {"containers": [
                     {"name": "control-plane", "image": image, "env": [
                         {"name": "LOOM_CP_PROTECTED_TRIAL_CUTOVER_ENABLED", "value": str(cutover).lower()},
                         {"name": "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE", "value": "/run/loom/protected-worker-runtime/files/database-url"},
                     ]}
                 ]}}},
    }


@pytest.mark.timeout(240)
def test_old_writers_cannot_resume_but_exact_successor_can_run(tmp_path):
    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    original = client.Configuration.get_default_copy()
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        admission = client.AdmissionregistrationV1Api()
        apps = client.AppsV1Api()
        batch = client.BatchV1Api()
        for namespace in ("loom-staging", "foreign"):
            core.create_namespace({"metadata": {"name": namespace}})
        apps.create_namespaced_deployment("loom-staging", _deployment("loom-service", replicas=0))
        documents = render_legacy_writer_fence(intent_digest="b" * 64, control_plane_image=_CP_IMAGE)
        from loom_cli.rollout.operator.protected_apply_executor import (
            SubprocessProtectedApplyCommandRunner,
        )
        from loom_cli.rollout.operator.protected_legacy_writer_fence_installation import (
            LegacyWriterFenceInstallation,
            LegacyWriterFenceJournal,
        )
        result = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert result.exit_code == 0
        kubeconfig = tmp_path / "disposable-kubeconfig"
        kubeconfig.write_text(result.output.decode().replace("https://127.0.0.1:6443",
            f"https://127.0.0.1:{container.get_exposed_port(6443)}"))
        kubeconfig.chmod(0o600)
        class DisposableRunner(SubprocessProtectedApplyCommandRunner):
            @property
            def environment(self):
                return {**super().environment, "KUBECONFIG": str(kubeconfig)}
        state = tmp_path / "fence-state"
        state.mkdir(mode=0o700)
        installation = LegacyWriterFenceInstallation(
            LegacyWriterFenceJournal(state, "disposable-fence", 1, os.geteuid()),
            DisposableRunner(), "b" * 64, _CP_IMAGE, lambda: None)
        retained = installation.install()
        assert len(retained) == 12 and installation.install() == retained
        assert installation.observe() == retained
        deadline = time.monotonic() + 30
        while True:
            try:
                apps.patch_namespaced_deployment_scale("loom-service", "loom-staging", {"spec": {"replicas": 1}}, dry_run="All")
            except ApiException as exc:
                assert exc.status == 403 and "loom-legacy-writer-retirement" in exc.body
                break
            assert time.monotonic() < deadline, "retirement policy never enforced"
            time.sleep(0.1)
        for document in documents:
            if document["kind"] == "ValidatingAdmissionPolicy":
                while True:
                    policy = admission.read_validating_admission_policy(document["metadata"]["name"])
                    if policy.status is not None and policy.status.type_checking is not None:
                        assert not policy.status.type_checking.expression_warnings
                        break
                    assert time.monotonic() < deadline, "policy type checking did not finish"
                    time.sleep(0.1)
        for name in ("loom-service", "loom-family-orchestrator", "loom-pipeline-orchestrator"):
            with pytest.raises(ApiException) as refused:
                apps.create_namespaced_deployment("loom-staging", _deployment(name), dry_run="All")
            assert refused.value.status == 403
        for image, cutover in ((_CP_IMAGE, False), ("registry.example.test/old:latest", True)):
            with pytest.raises(ApiException) as refused:
                apps.create_namespaced_deployment("loom-staging", _deployment("loom-control-plane", image=image, cutover=cutover), dry_run="All")
            assert refused.value.status == 403
        successor = _deployment("loom-control-plane", cutover=True)
        apps.create_namespaced_deployment("loom-staging", successor, dry_run="All")
        for change in ("command", "missing-credential", "duplicate-cutover"):
            invalid = copy.deepcopy(successor)
            worker = invalid["spec"]["template"]["spec"]["containers"][0]
            if change == "command":
                worker["command"] = ["python", "-m", "loom_service"]
            elif change == "missing-credential":
                worker["env"].pop()
            else:
                worker["env"].append({"name": "LOOM_CP_PROTECTED_TRIAL_CUTOVER_ENABLED", "value": "false"})
            with pytest.raises(ApiException) as refused:
                apps.create_namespaced_deployment("loom-staging", invalid, dry_run="All")
            assert refused.value.status == 403
        for cutover in (False, True):
            replica = _deployment("loom-control-plane", cutover=cutover)
            replica["kind"] = "ReplicaSet"
            replica["metadata"].update({"name": "loom-control-plane-abcdef", "labels": {"app": "loom-control-plane"}})
            if cutover:
                apps.create_namespaced_replica_set("loom-staging", replica, dry_run="All")
            else:
                with pytest.raises(ApiException) as refused:
                    apps.create_namespaced_replica_set("loom-staging", replica, dry_run="All")
                assert refused.value.status == 403
        apps.create_namespaced_deployment("foreign", _deployment("loom-control-plane"), dry_run="All")
        apps.patch_namespaced_deployment_scale("loom-service", "loom-staging", {"spec": {"replicas": 0}}, dry_run="All")
        # A prior ReplicaSet or direct old Pod cannot evade Deployment admission.
        old = _deployment("loom-control-plane")
        for template in (old, successor):
            pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "old-writer", "labels": template["spec"]["template"]["metadata"]["labels"]},
                   "spec": copy.deepcopy(template["spec"]["template"]["spec"])}
            if template is old:
                with pytest.raises(ApiException) as refused:
                    core.create_namespaced_pod("loom-staging", pod, dry_run="All")
                assert refused.value.status == 403
            else:
                core.create_namespaced_pod("loom-staging", pod, dry_run="All")
        job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "loom-staging-data-lifecycle-123"},
               "spec": {"suspend": False, "template": {"spec": {"restartPolicy": "Never", "containers": [{"name": "lifecycle", "image": _CP_IMAGE}]}}}}
        with pytest.raises(ApiException) as refused:
            batch.create_namespaced_job("loom-staging", job, dry_run="All")
        assert refused.value.status == 403
        job["spec"]["suspend"] = True
        batch.create_namespaced_job("loom-staging", job, dry_run="All")
        cronjob = {"apiVersion": "batch/v1", "kind": "CronJob",
                   "metadata": {"name": "loom-staging-data-lifecycle"},
                   "spec": {"schedule": "*/5 * * * *", "suspend": False,
                            "jobTemplate": {"spec": copy.deepcopy(job["spec"])}}}
        with pytest.raises(ApiException) as refused:
            batch.create_namespaced_cron_job("loom-staging", cronjob, dry_run="All")
        assert refused.value.status == 403
        cronjob["spec"]["suspend"] = True
        batch.create_namespaced_cron_job("loom-staging", cronjob, dry_run="All")
    finally:
        client.Configuration.set_default(original)
        container.stop()
