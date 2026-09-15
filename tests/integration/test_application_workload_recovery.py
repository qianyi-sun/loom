"""Real Kubernetes patches and SQL completion; not a full live handoff test."""

import copy
import os
import shutil
import subprocess
import time
from contextlib import contextmanager

import psycopg
import pytest
import yaml

from loom.application_handoff_completion import complete_application_handoff_database
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_application_workload_runtime import (
    pause_application_workloads,
    restore_application_workloads,
)
from loom_cli.rollout.operator.protected_application_workloads import APPLICATION_DEPLOYMENTS
from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner
from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup

_IMAGE = "docker.io/library/busybox@sha256:dc2d74b28e4cf8984fa52af1f39bc7c3d9c73760b41a74d629f5d11b1ab28616"
_NAMESPACE = "loom-staging"


def _wait(predicate, diagnostic):
    deadline = time.monotonic() + 120
    while not predicate():
        assert time.monotonic() < deadline, diagnostic()
        time.sleep(0.25)


@pytest.mark.asyncio
@pytest.mark.timeout(420)
@pytest.mark.parametrize("transfer_postgres", [17], indirect=True)
@pytest.mark.parametrize("transfer_database", ["protected-staging"], indirect=True)
async def test_real_workload_pause_and_sql_recovery_survive_lost_patch_ack(
    transfer_database, tmp_path, monkeypatch,  # noqa: F811
):
    from kubernetes import client

    original_configuration = client.Configuration.get_default_copy()
    # Nested kubelet sees the host's multi-terabyte filesystem. A proportional
    # 10% reserve can evict these tiny Pods with hundreds of GiB still free.
    # Preserve memory/inode safeguards and require an explicit 2 GiB disk floor.
    container = _start_k3s(ephemeral_storage_floor="2Gi")
    try:
        _, core, batch = _load_client(container)
        apps = client.AppsV1Api()
        core.create_namespace({"metadata": {"name": _NAMESPACE}})
        pod = {"terminationGracePeriodSeconds": 1, "containers": [{"name": "writer",
               "image": _IMAGE, "command": ["sh", "-c", "exec sleep 3600"]}]}
        for name in sorted(APPLICATION_DEPLOYMENTS):
            apps.create_namespaced_deployment(_NAMESPACE, {
                "apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name},
                "spec": {"replicas": 1, "selector": {"matchLabels": {"app": name}},
                         "template": {"metadata": {"labels": {"app": name}}, "spec": copy.deepcopy(pod)}},
            })
        job_template = {"spec": {"template": {"spec": {**copy.deepcopy(pod), "restartPolicy": "Never"}}}}
        cron = batch.create_namespaced_cron_job(_NAMESPACE, {
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": "loom-staging-data-lifecycle"},
            "spec": {"schedule": "0 0 * * *", "suspend": True, "jobTemplate": job_template},
        })
        job_name = "loom-staging-data-lifecycle-123"
        batch.create_namespaced_job(_NAMESPACE, {
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name, "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob",
                "name": cron.metadata.name, "uid": cron.metadata.uid, "controller": True}]},
            **copy.deepcopy(job_template),
        })
        def pods():
            return core.list_namespaced_pod(_NAMESPACE).items
        _wait(lambda: len(pods()) == 8 and all(p.status.phase == "Running" for p in pods()),
              lambda: [(p.metadata.name, p.status.phase, [(c.name, c.state.to_dict()) for c in (p.status.container_statuses or [])]) for p in pods()])
        original_pod_uids = {p.metadata.uid for p in pods()}

        kubectl = shutil.which("kubectl")
        assert kubectl is not None
        result = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert result.exit_code == 0
        kubeconfig = yaml.safe_load(result.output.decode())
        kubeconfig["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{container.get_exposed_port(6443)}"
        config_path = tmp_path / "disposable-kubeconfig"
        config_path.write_text(yaml.safe_dump(kubeconfig))
        config_path.chmod(0o600)
        subprocess_run = subprocess.run
        plan, journal = _setup(tmp_path)
        evidence = _guard(plan)
        request = dict(request_id=plan.request_id, candidate_sha=plan.candidate_sha,
                       candidate_tree=plan.candidate_tree, generation=evidence.generation)
        with _closed(transfer_database, request=request) as (peer, maintenance, db_guard, arguments):
            arguments["password"] = "ab" * 16
            # The protected-staging fixture uses CNPG's template0/C database.
            # Match the installed completion adapter's independently pinned ACL profile.
            arguments["schema_acl_profile"] = "cnpg-staging"
            evidence = MutationGuardEvidence.build(**{
                k: v for k, v in evidence.to_dict().items()
                if k not in {"schema_version", "evidence_digest", "database_backend_pid", "cronjob_uid", "suspended_resource_version"}
            }, database_backend_pid=db_guard.info.backend_pid, cronjob_uid=cron.metadata.uid,
                suspended_resource_version=cron.metadata.resource_version)
            original_server = db_guard.execute("SELECT pg_postmaster_start_time()").fetchone()
            completions = []
            lost_completion_ack = [True]

            class Runner(SubprocessProtectedApplyCommandRunner):
                @contextmanager
                def open_staging_peer_maintenance_database(self):
                    with psycopg.connect(transfer_database[0], dbname=maintenance.info.dbname, autocommit=True) as connection:
                        yield connection

                def recover_and_complete_staging_application_database(self, plan, *, journal, guard):
                    assert journal.application_workloads_restoring(plan)
                    outcome = complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
                    completions.append(outcome)
                    if lost_completion_ack[0]:
                        lost_completion_ack[0] = False
                        raise subprocess.TimeoutExpired("actual SQL completion acknowledgement lost", 30)
                    return outcome

            runner = Runner()
            lost_ack = [True]
            patched = []

            def isolated_run(argv, **kwargs):
                assert argv[0] == "kubectl" and kwargs["env"] == runner.environment
                assert "get" in argv or "patch" in argv
                kwargs["env"] = {**kwargs["env"], "KUBECONFIG": str(config_path)}
                reply = subprocess_run((kubectl, *argv[1:]), **kwargs)
                if "patch" in argv and reply.returncode == 0:
                    patched.append(argv[argv.index("patch") + 2])
                    if lost_ack[0]:
                        lost_ack[0] = False
                        raise subprocess.TimeoutExpired("actual workload patch acknowledgement lost", 30)
                return reply

            mode = ["pause"]
            original_inventory = []

            def apply(_):
                journal.retain_application_guard(plan, guard=evidence)
                assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
                    service_uid=os.getuid(), guard=evidence, acknowledge=True)
                journal.record_application_admission_recovery(target=arguments["target"],
                    handoff_backend=arguments["handoff_backend"], coordination_guard=arguments["coordination_guard"])
                if mode[0] == "pause":
                    pause_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
                    remaining = pods()
                    assert all(p.status.phase in {"Failed", "Succeeded"} and all(
                        c.state.terminated is not None for c in (
                            (p.status.container_statuses or []) + (p.status.init_container_statuses or [])
                            + (p.status.ephemeral_container_statuses or [])
                        )
                    ) for p in remaining), [(p.metadata.name, p.status.to_dict()) for p in remaining]
                    assert peer.execute("SELECT datallowconn FROM pg_database WHERE datname='loom'").fetchone() == (False,)
                    original_inventory.append(journal.read_application_workloads(plan))
                    mode[0] = "restore"
                    lost_ack[0] = True
                restore_application_workloads(plan, journal=journal, runner=runner, guard=evidence)
                assert original_inventory == [journal.read_application_workloads(plan)]
                raise RuntimeError("workload section verified; no handoff terminal")

            with monkeypatch.context() as transport:
                transport.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", isolated_run)
                # Lose ACKs after real pause, SQL completion, and workload restore.
                for _ in range(3):
                    with pytest.raises(subprocess.TimeoutExpired, match="acknowledgement lost"):
                        journal.execute(plan, [_component(apply)])
                with pytest.raises(RuntimeError, match="workload section verified"):
                    journal.execute(plan, [_component(apply)])
            assert len(completions) == 3 and len(patched) == 16
            assert all(item.original_value == 1 for item in original_inventory[0] if item.kind == "Deployment")
            assert batch.read_namespaced_cron_job(cron.metadata.name, _NAMESPACE).spec.suspend is True
            def running_pods():
                return [p for p in pods() if p.status.phase == "Running"]
            _wait(lambda: len(running_pods()) == 8,
                  lambda: [(p.metadata.name, p.status.phase, [(c.name, c.state.to_dict()) for c in (p.status.container_statuses or [])]) for p in pods()])
            assert not original_pod_uids.intersection(p.metadata.uid for p in running_pods())
            assert db_guard.execute("SELECT pg_postmaster_start_time()").fetchone() == original_server
            from loom_cli.rollout.operator.staging_mutation_guard import _HEALTH_SQL
            assert db_guard.execute(_HEALTH_SQL).fetchone() == (evidence.database_backend_pid, True)
            with psycopg.connect(transfer_database[0], user="loom", password=arguments["password"], autocommit=True) as runtime:
                assert runtime.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    runtime.execute("ALTER TABLE public.trials DISABLE TRIGGER ALL")
            assert not list(journal.root.rglob("terminal.json"))
    finally:
        client.Configuration.set_default(original_configuration)
        container.stop()
