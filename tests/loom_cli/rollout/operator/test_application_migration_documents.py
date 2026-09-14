"""Generation credentials are fixed to the attested staging Job and SQL target."""

import base64
import hashlib
from dataclasses import replace

import pytest
import yaml
from sqlalchemy.engine import make_url

from loom_cli.cluster_migration import render_migration_manifest
from loom_cli.rollout.operator.protected_application_migration_journal import (
    ApplicationMigrationEvent,
)
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
from tests.loom_cli.rollout.operator.test_application_migration_journal import _generation


def _inputs(tmp_path):
    plan, _ = _setup(tmp_path)
    payload = render_migration_manifest(image_tag="staging-aaaaaaa", namespace="loom-staging", job_suffix="test",
        application_owner_role="loom_app_staging_owner", container_registry="registry.example",
        registry_digest="sha256:" + "a" * 64).encode()
    plan = replace(plan, migration_manifest_sha256=hashlib.sha256(payload).hexdigest(),
        migration_job_name=yaml.safe_load(payload)["metadata"]["name"])
    guard = _guard(plan)
    generation = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=_generation(),
        intent_digest="1" * 64, guard_digest=guard.evidence_digest, previous_digest="2" * 64)
    return plan, payload, guard, generation


def test_generation_documents_deliver_only_bounded_credential_and_original_ca(tmp_path):
    from loom_cli.rollout.operator.protected_application_migration_documents import (
        application_migration_documents,
    )

    plan, payload, guard, generation = _inputs(tmp_path)
    job, secret = application_migration_documents(plan, template=payload, generation=generation, guard=guard,
        container_registry="registry.example")
    url = make_url(base64.b64decode(secret["data"]["db-url"]).decode())
    assert url.username == "loom_app_migrate_" + generation.payload["nonce"]
    assert url.password == generation.payload["password"]
    assert url.host == "loom-postgres-rw.loom-staging.svc.cluster.local" and url.database == "loom"
    assert url.query == {"sslmode": "verify-full", "sslrootcert": "/run/loom-application-migration/ca.crt"}
    assert secret["data"]["ca.crt"] == generation.payload["ca_certificate"]
    assert job["metadata"]["name"] != plan.migration_job_name
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"]["name"] == secret["metadata"]["name"]
    assert pod["volumes"][0]["secret"]["secretName"] == secret["metadata"]["name"]
    assert application_migration_documents(plan, template=payload, generation=generation, guard=guard,
        container_registry="registry.example") == (job, secret)
    another = ApplicationMigrationEvent.build(sequence=17, phase="generation", payload=_generation(2),
        intent_digest=generation.intent_digest, guard_digest=guard.evidence_digest, previous_digest="3" * 64)
    next_job, next_secret = application_migration_documents(plan, template=payload, generation=another, guard=guard,
        container_registry="registry.example")
    assert next_job["metadata"]["name"] != job["metadata"]["name"]
    assert next_secret["metadata"]["name"] != secret["metadata"]["name"]


@pytest.mark.parametrize("drift", ["artifact", "guard", "candidate", "owner"])
def test_generation_documents_refuse_changed_attested_inputs(tmp_path, drift):
    from loom_cli.rollout.operator.protected_application_migration_documents import (
        application_migration_documents,
    )

    plan, payload, guard, generation = _inputs(tmp_path)
    if drift == "artifact":
        payload += b"# changed\n"
    elif drift == "guard":
        fields = {key: value for key, value in guard.to_dict().items() if key not in {"schema_version", "evidence_digest"}}
        guard = type(guard).build(**{**fields, "generation": "e" * 32})
    elif drift == "candidate":
        fields = {key: value for key, value in guard.to_dict().items() if key not in {"schema_version", "evidence_digest"}}
        guard = type(guard).build(**{**fields, "candidate_sha": "d" * 40})
    else:
        document = yaml.safe_load(payload)
        document["spec"]["template"]["spec"]["containers"][0]["env"][1]["value"] = "loom"
        payload = yaml.safe_dump(document).encode()
        plan = replace(plan, migration_manifest_sha256=hashlib.sha256(payload).hexdigest())
    with pytest.raises(ValueError):
        application_migration_documents(plan, template=payload, generation=generation, guard=guard,
            container_registry="registry.example")
