"""Controller SQL delivery is a fixed staging binding, not an arbitrary file map."""

import base64
import hashlib
from dataclasses import replace
from pathlib import PurePosixPath
from uuid import NAMESPACE_URL, uuid5

import pytest

from loom_capacity_executor.runtime import AdmissionBindingEntryV2
from tests.loom_cli.rollout.operator.test_application_migration_ca import _ca
from tests.loom_cli.rollout.operator.test_protected_active_controller import _request


def _bundle(document):
    from loom_cli.rollout.operator.protected_controller_admission import ControllerAdmissionBundle

    url = (
        b"postgresql+psycopg://loom_cap_staging_executor:"
        + b"p" * 64
        + b"@loom-postgres-rw.loom-staging.svc.cluster.local:31432/loom?sslmode=verify-full&hostaddr=192.168.50.103"
    )
    entry = AdmissionBindingEntryV2(
        subject_id=uuid5(NAMESPACE_URL, "loom:staging:capacity-subject"),
        subject_incarnation=uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1"),
        configuration_generation=document.execution.configuration_epoch,
        deployment_generation=1,
        candidate_generation=1,
        protected_admission_sha256="a" * 64,
        environment_name="staging",
        database_url_file=str(
            PurePosixPath(document.state_directory) / "admission-credentials" / "staging.url"
        ),
        database_url_sha256=hashlib.sha256(url).hexdigest(),
    )
    return ControllerAdmissionBundle(entry, url, _ca(), "b" * 64)


def test_controller_admission_binds_actual_directory_and_private_url(tmp_path):
    request = _request(tmp_path)
    bundle = _bundle(request.document)
    document = request.document.model_copy(
        update={"admission_directory_sha256": bundle.directory_sha256}
    )
    bound = replace(request, document=document, admission=bundle)
    assert type(bound).from_bytes(bound.to_bytes()) == bound
    assert base64.b64encode(bundle.database_url) in bound.to_bytes() and "p" * 64 not in repr(bound)
    assert len(bundle.files(document)) == 2
    assert (
        b"PGSSLROOTCERT=/opt/loom-capacity-executor-releases/.active-trust/postgres-ca.pem\n"
        in bound.files["/etc/loom-capacity-executor/active-service.env"]
    )
    with pytest.raises(ValueError, match="admission"):
        replace(request, admission=bundle)


@pytest.mark.parametrize(
    "drift", ["role", "route", "tls", "path", "subject", "certificate", "generation"]
)
def test_controller_admission_refuses_foreign_delivery(tmp_path, drift):
    request = _request(tmp_path)
    bundle = _bundle(request.document)
    document = request.document.model_copy(
        update={"admission_directory_sha256": bundle.directory_sha256}
    )
    with pytest.raises(ValueError, match="admission"):
        if drift in {"role", "route", "tls"}:
            old, new = {
                "role": (b"loom_cap_staging_executor", b"postgres"),
                "route": (b"192.168.50.103", b"192.168.50.14"),
                "tls": (b"verify-full", b"require"),
            }[drift]
            url = bundle.database_url.replace(old, new)
            bundle = replace(
                bundle,
                database_url=url,
                entry=bundle.entry.model_copy(
                    update={"database_url_sha256": hashlib.sha256(url).hexdigest()}
                ),
            )
        elif drift == "path":
            bundle = replace(
                bundle,
                entry=bundle.entry.model_copy(update={"database_url_file": "/etc/unrelated"}),
            )
        elif drift == "subject":
            bundle = replace(
                bundle, entry=bundle.entry.model_copy(update={"environment_name": "production"})
            )
        elif drift == "certificate":
            bundle = replace(bundle, ca_certificate=b"not a CA")
        else:
            bundle = replace(
                bundle,
                entry=bundle.entry.model_copy(
                    update={"configuration_generation": bundle.entry.configuration_generation + 1}
                ),
            )
        replace(request, document=document, admission=bundle)


def test_bundle_builder_uses_retained_password_and_exact_subject_generations(tmp_path):
    from sqlalchemy.engine import make_url

    from loom_cli.rollout.operator.protected_controller_admission import (
        build_controller_admission_bundle,
    )
    from tests.capacity_fixtures import subject_configuration
    from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
    from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
    from tests.loom_cli.rollout.operator.test_executor_admission_journal import _record

    request = _request(tmp_path)
    plan, _ = _setup(tmp_path)
    record = _record(plan, _component(lambda _: None), _guard(plan))
    subject = subject_configuration().model_copy(update={
        "subject_id": uuid5(NAMESPACE_URL, "loom:staging:capacity-subject"),
        "subject_incarnation": uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1"),
        "tier_id": "staging", "configuration_generation": request.document.execution.configuration_epoch,
        "candidate_generation": 17, "deployment_generation": 19})
    ca = _ca()
    first = build_controller_admission_bundle(record, subject=subject,
        state_directory=request.document.state_directory, protected_admission_sha256="a" * 64, ca_certificate=ca)
    assert first == build_controller_admission_bundle(record, subject=subject,
        state_directory=request.document.state_directory, protected_admission_sha256="a" * 64, ca_certificate=ca)
    assert make_url(first.database_url.decode()).password == record.password
    assert first.issuance_digest == record.digest
    assert first.entry.candidate_generation == 17 and first.entry.deployment_generation == 19
    document = request.document.model_copy(update={"admission_directory_sha256": first.directory_sha256})
    assert replace(request, document=document, admission=first).admission == first
    with pytest.raises(ValueError, match="admission"):
        build_controller_admission_bundle(record, subject=subject.model_copy(update={"tier_id": "production"}),
            state_directory=request.document.state_directory, protected_admission_sha256="a" * 64, ca_certificate=ca)
