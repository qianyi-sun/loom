"""Capacity credentials and public CA stay bound to one recorded generation."""

import base64
import copy
import json

import pytest

from loom_cli.rollout.operator.protected_application_migration_journal import ApplicationMigrationEvent
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
)
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_application_migration_journal import _generation
from tests.loom_cli.rollout.operator.test_application_migration_resources import Runner
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import _database_component


@pytest.mark.parametrize("interruption", [None, "secret", "job"])
def test_capacity_bootstrap_resources_retire_lost_deliveries_without_recreation(tmp_path, interruption):
    from loom_cli.rollout.operator.protected_capacity_bootstrap_resources import (
        capacity_bootstrap_resources,
    )

    plan, source, _ = _database_component(tmp_path, database_state="needs-convergence")
    runner, guard = Runner(), _guard(plan)
    base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example.test/loom", lambda: source.seed)
    event = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=_generation(),
        intent_digest="1" * 64, guard_digest=guard.evidence_digest, previous_digest="2" * 64)
    original_seed = copy.deepcopy(source.seed)
    resources = capacity_bootstrap_resources(base=base, plan=plan, seed=source.seed, generation=event,
        guard=guard, assert_guard=lambda: guard)
    data = resources.secret["data"]
    assert base64.b64decode(data["admin-password"]).decode() == event.payload["password"]
    assert json.loads(base64.b64decode(data["seed.json"]))["migrator_database_password"] == event.payload["password"]
    assert data["ca.crt"] == event.payload["ca_certificate"]
    assert source.seed == original_seed
    pod = resources.job["spec"]["template"]["spec"]
    assert {v["secret"]["secretName"] for v in pod["volumes"]} == {resources.secret["metadata"]["name"]}
    assert pod["containers"][0]["command"][-1] == "loom.staging_capacity_database_bootstrap"
    assert resources.job["metadata"]["name"] != "loom-staging-capacity-database-bootstrap"
    for kind in ("secret", "job"):
        runner.lose_create = kind == interruption
        if kind == "secret":
            create = lambda: resources.ensure_secret(creation_dispatched=True)
        else:
            create = lambda: resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
        if kind == interruption:
            with pytest.raises(RuntimeError, match="reply lost"):
                create()
        else:
            create()
        observed = resources.observe_secret() if kind == "secret" else resources.observe_job()
        if kind == "secret":
            secret = observed
        else:
            job = observed
    runner.objects["Job"]["spec"].update(parallelism=1, completions=1)
    resources.delete_job(expected_uid=job.uid)
    resources.delete_secret(expected_uid=secret.uid)
    resources.require_retired()
    assert runner.events == [("create", "Secret"), ("create", "Job"), ("delete", "Job"), ("delete", "Secret")]
    next_event = ApplicationMigrationEvent.build(sequence=17, phase="generation", payload=_generation(2),
        intent_digest="1" * 64, guard_digest=guard.evidence_digest, previous_digest="3" * 64)
    other = capacity_bootstrap_resources(base=base, plan=plan, seed=source.seed, generation=next_event,
        guard=guard, assert_guard=lambda: guard)
    assert other.job["metadata"]["name"] != resources.job["metadata"]["name"]
    assert other.secret["metadata"]["name"] != resources.secret["metadata"]["name"]
