"""Management drives native guard protocols without application DB authority."""

from importlib import import_module
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from loom_capacity_agent.client import DemandReporterClient
from loom_capacity_manager.contracts import DemandSnapshotV1, canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAcknowledgementV2,
    ExecutableBootstrapAcknowledgementV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_bootstrap import bootstrap
from tests.integration.test_personal_dev_build_guard_demand_publication import (
    reporter_configuration,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def runtime(factory, installation, manager):
    return import_module("loom_capacity_build_guard.management_runtime").BuildManagementRuntime(
        session_factory=factory, installation=installation, manager=manager, configuration_generation=1)


@pytest.mark.parametrize("boundary", ["enabled", "disabled", "demand-failure"])
async def test_runtime_publishes_demand_and_converges_native_work(prepared_input, boundary):
    factory, engine, installation, plan, *_ = prepared_input
    proposal = bootstrap(plan)
    calls = []

    async def handle(outgoing):
        path = outgoing.url.path
        calls.append(path)
        if "/reports/demand/" in path:
            snapshot = DemandSnapshotV1.model_validate_json(outgoing.content)
            if boundary == "demand-failure":
                return httpx.Response(503)
            return httpx.Response(200, json={"snapshot_id": str(uuid4()), "digest": canonical_digest(snapshot),
                "sequence": snapshot.sequence, "replayed": False})
        if path.endswith("/bootstrap-work"):
            return httpx.Response(200, content=canonical_executable_bytes(proposal))
        if path.endswith("/admission-work"):
            return httpx.Response(200, content=canonical_executable_bytes(plan))
        if "/bootstraps/" in path:
            ack = ExecutableBootstrapAcknowledgementV2.model_validate_json(outgoing.content)
            return httpx.Response(200, json={"intent_id": str(ack.binding.intent_id), "bootstrap_registration_epoch": 1,
                "receipt_digest": canonical_executable_digest(ack), "replayed": False, "executable": True})
        if "/admission-plans/" in path:
            ack = ExecutableAdmissionAcknowledgementV2.model_validate_json(outgoing.content)
            return httpx.Response(200, json={"proposal_id": str(ack.proposal_id), "prepared_plan_digest": ack.prepared_plan_digest,
                "receipt_digest": canonical_executable_digest(ack), "replayed": False, "executable": True})
        pytest.fail(f"unexpected manager operation {path}")

    configuration = reporter_configuration(installation).model_copy(update={
        "candidate_identity_algorithm": installation.document.runtime.candidate.algorithm,
        "candidate_identity": installation.document.runtime.candidate.identity,
        "candidate_publication_sha256": installation.document.runtime.candidate.publication_sha256,
        "protected_admission_sha256": installation.document.protected_admission_sha256})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = DemandReporterClient(configuration, manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        result = await runtime(factory, installation, manager).run_once(admission_enabled=boundary != "disabled")
    assert result.failed_stages == (("demand",) if boundary == "demand-failure" else ())
    assert any("/reports/demand/" in path for path in calls)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == int(boundary == "enabled")
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.bootstraps")) == int(boundary == "enabled")
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0
