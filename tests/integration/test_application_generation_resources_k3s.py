"""Real API defaulting and lost-reply retirement for protected generations."""

import shutil
import subprocess
from dataclasses import replace

import pytest
import yaml

from loom_cli.rollout.operator.protected_application_migration_journal import (
    ApplicationMigrationEvent,
)
from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner
from loom_cli.rollout.operator.protected_capacity_bootstrap_resources import (
    capacity_bootstrap_resources,
)
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_application_migration_journal import _generation
from tests.loom_cli.rollout.operator.test_application_migration_resources import _resources
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _database_component,
)


@pytest.mark.timeout(240)
@pytest.mark.parametrize("capacity", [False, True])
def test_generation_lost_creation_reply_and_exact_retirement(tmp_path, monkeypatch, capacity):
    from kubernetes import client

    original_configuration = client.Configuration.get_default_copy()
    kubectl = shutil.which("kubectl")
    assert kubectl is not None
    container = _start_k3s()
    try:
        client, core, _batch = _load_client(container)
        core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name="loom-staging")))
        config_result = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        assert config_result.exit_code == 0
        kubeconfig = yaml.safe_load(config_result.output.decode())
        kubeconfig["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{container.get_exposed_port(6443)}"
        config_path = tmp_path / "disposable-kubeconfig"
        config_path.write_text(yaml.safe_dump(kubeconfig))
        config_path.chmod(0o600)
        runner = SubprocessProtectedApplyCommandRunner()
        if capacity:
            plan, source, _ = _database_component(tmp_path, database_state="needs-convergence")
            guard = _guard(plan)
            base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example.test/loom", lambda: source.seed)
            event = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=_generation(),
                intent_digest="1" * 64, guard_digest=guard.evidence_digest, previous_digest="2" * 64)
            resources = capacity_bootstrap_resources(base=base, plan=plan, seed=source.seed,
                generation=event, guard=guard, assert_guard=lambda: guard)
        else:
            resources = replace(_resources(tmp_path)[0], runner=runner)
        original_run = subprocess.run
        creations = []

        def isolated_run(argv, **kwargs):
            assert argv[0] == "kubectl" and kwargs["env"] == runner.environment
            assert not any(arg.startswith(("--kubeconfig", "--context", "--server")) for arg in argv)
            kwargs["env"] = {**kwargs["env"], "KUBECONFIG": str(config_path)}
            result = original_run((kubectl, *argv[1:]), **kwargs)
            if "create" in argv and result.returncode == 0:
                creations.append(1)
                raise subprocess.TimeoutExpired("disposable creation reply lost", 35)
            return result

        with monkeypatch.context() as transport:
            transport.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", isolated_run)
            with pytest.raises((RuntimeError, subprocess.TimeoutExpired)):
                resources.ensure_secret(creation_dispatched=True)
            secret = resources.observe_secret()
            assert secret is not None
            assert resources.ensure_secret(creation_dispatched=True, expected_uid=secret.uid).uid == secret.uid
            with pytest.raises((RuntimeError, subprocess.TimeoutExpired)):
                resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
            job = resources.observe_job()
            assert job is not None
            assert resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid,
                expected_uid=job.uid).uid == job.uid
            resources.delete_job(expected_uid=job.uid)
            resources.delete_secret(expected_uid=secret.uid)
            resources.require_retired()
            assert len(creations) == 2
        assert core.list_namespaced_secret("loom-staging").items == []
    finally:
        client.Configuration.set_default(original_configuration)
        container.stop()
