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


def configuration_for(installation):
    return reporter_configuration(installation).model_copy(update={
        "candidate_identity_algorithm": installation.document.runtime.candidate.algorithm,
        "candidate_identity": installation.document.runtime.candidate.identity,
        "candidate_publication_sha256": installation.document.runtime.candidate.publication_sha256,
        "protected_admission_sha256": installation.document.protected_admission_sha256})


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
        if path.endswith("/bootstrap-acknowledgements"):
            ack = ExecutableBootstrapAcknowledgementV2.model_validate_json(outgoing.content)
            return httpx.Response(200, json={"intent_id": str(ack.binding.intent_id), "bootstrap_registration_epoch": 1,
                "receipt_digest": canonical_executable_digest(ack), "replayed": False, "executable": True})
        if "/admission-acknowledgements/" in path:
            ack = ExecutableAdmissionAcknowledgementV2.model_validate_json(outgoing.content)
            return httpx.Response(200, json={"proposal_id": str(ack.proposal_id), "prepared_plan_digest": ack.prepared_plan_digest,
                "receipt_digest": canonical_executable_digest(ack), "replayed": False, "executable": True})
        pytest.fail(f"unexpected manager operation {path}")

    configuration = configuration_for(installation)
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


@pytest.mark.parametrize("boundary", ["disabled", "demand-failure", "terminal-transient", "release-transient", "final-transient"])
async def test_management_recovery_continues_without_intake_and_retries_transport(prepared_input, monkeypatch, boundary):
    from loom_capacity_manager.executable_contracts import ExecutableProtectedReleaseV2
    from tests.integration.test_personal_dev_build_guard_hold_retirement import release_witness
    from tests.integration.test_personal_dev_build_guard_registered_release import (
        registered_release_input,
    )
    from tests.integration.test_personal_dev_build_guard_release_outbox import outbox

    factory, engine, installation, *_ = prepared_input
    _request, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="live", drain=False)
    failures = []
    witnesses = []

    async def handle(outgoing):
        path = outgoing.url.path
        stage = ("terminal" if path.endswith("/terminal-inventory-evidence") else "release" if "/reports/protected-releases/" in path
            else "final" if path.endswith("/final-release-witness") else "demand" if "/reports/demand/" in path else "other")
        if boundary == f"{stage}-transient" and not failures:
            failures.append(stage)
            return httpx.Response(503)
        if stage == "terminal":
            return httpx.Response(200, content=canonical_executable_bytes(terminal))
        if stage == "release":
            release = ExecutableProtectedReleaseV2.model_validate_json(outgoing.content)
            async with factory.begin() as session:
                publication = await outbox(session, installation).read_next()
            assert publication.release == release
            # Actual private guard + HTTP client; manager execution remains
            # simulated here, not a live/global-ledger acceptance claim.
            witnesses.append(release_witness(publication, terminal))
            return httpx.Response(200, json={"intent_id": str(release.binding.intent_id),
                "protected_release_sha256": release.protected_release_sha256,
                "receipt_digest": canonical_executable_digest(release), "replayed": False, "executable": True})
        if stage == "final":
            return httpx.Response(200, content=canonical_executable_bytes(witnesses[-1]))
        if stage == "demand":
            if boundary == "demand-failure":
                return httpx.Response(503)
            snapshot = DemandSnapshotV1.model_validate_json(outgoing.content)
            return httpx.Response(200, json={"snapshot_id": str(uuid4()), "digest": canonical_digest(snapshot),
                "sequence": snapshot.sequence, "replayed": False})
        assert path.endswith("/admission-work")
        return httpx.Response(200, content=b"null")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = DemandReporterClient(configuration_for(installation), manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        managed = runtime(factory, installation, manager)
        result = await managed.run_once(admission_enabled=False)
        if boundary.endswith("transient"):
            assert result.failed_stages
            result = await managed.run_once(admission_enabled=False)
        assert result.failed_stages == (("demand",) if boundary == "demand-failure" else ())
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


async def test_manager_closure_runs_when_demand_and_new_admission_are_unavailable(prepared_input):
    from loom_capacity_manager.executable_contracts import (
        ExecutableAdmissionPlanClosureAcknowledgementV2,
        ExecutableAdmissionPlanClosureV2,
    )

    factory, engine, installation, plan, *_ = prepared_input
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=plan, close_reason="manager-closed")
    acknowledgements = []

    async def handle(outgoing):
        path = outgoing.url.path
        if "/reports/demand/" in path:
            return httpx.Response(503)
        if path.endswith("/admission-work"):
            return httpx.Response(200, content=canonical_executable_bytes(closure))
        assert "/admission-closures/" in path
        ack = ExecutableAdmissionPlanClosureAcknowledgementV2.model_validate_json(outgoing.content)
        acknowledgements.append(ack)
        return httpx.Response(200, json={"closure_id": str(ack.closure_id), "disposition_kind": ack.disposition_kind,
            "disposition_digest": ack.disposition_digest, "receipt_digest": canonical_executable_digest(ack),
            "replayed": False, "executable": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = DemandReporterClient(configuration_for(installation), manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        result = await runtime(factory, installation, manager).run_once(admission_enabled=False)
    assert result.failed_stages == ("demand",)
    assert len(acknowledgements) == 1 and acknowledgements[0].closure_id == closure.closure_id
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='closure'")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0


async def test_management_cancellation_propagates_without_running_following_stages(prepared_input):
    import asyncio

    factory, _engine, installation, *_ = prepared_input
    paths = []

    async def handle(outgoing):
        paths.append(outgoing.url.path)
        raise asyncio.CancelledError

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = DemandReporterClient(configuration_for(installation), manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        with pytest.raises(asyncio.CancelledError):
            await runtime(factory, installation, manager).run_once(admission_enabled=True)
    assert len(paths) == 1 and "/reports/demand/" in paths[0]


@pytest.mark.parametrize("boundary", ["lost-reply", "invalid-receipt"])
async def test_restart_recovers_accepted_plan_missing_from_manager_queue(prepared_input, boundary):
    factory, engine, installation, plan, *_ = prepared_input
    accepted = []

    async def handle(outgoing):
        path = outgoing.url.path
        if "/reports/demand/" in path:
            snapshot = DemandSnapshotV1.model_validate_json(outgoing.content)
            return httpx.Response(200, json={"snapshot_id": str(uuid4()), "digest": canonical_digest(snapshot),
                "sequence": snapshot.sequence, "replayed": False})
        if path.endswith("/bootstrap-work"):
            return httpx.Response(200, content=b"null")
        if path.endswith("/admission-work"):
            # Like the manager's actual queue, acknowledged proposals disappear.
            return httpx.Response(200, content=b"null" if accepted else canonical_executable_bytes(plan))
        assert "/admission-acknowledgements/" in path
        ack = ExecutableAdmissionAcknowledgementV2.model_validate_json(outgoing.content)
        accepted.append((outgoing.content, outgoing.headers["Idempotency-Key"]))
        if len(accepted) == 1:
            if boundary == "lost-reply":
                raise httpx.ReadError("reply lost after commit", request=outgoing)
            return httpx.Response(200, json={})
        return httpx.Response(200, json={"proposal_id": str(ack.proposal_id), "prepared_plan_digest": ack.prepared_plan_digest,
            "receipt_digest": canonical_executable_digest(ack), "replayed": True, "executable": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = DemandReporterClient(configuration_for(installation), manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        first = await runtime(factory, installation, manager).run_once(admission_enabled=True)
        assert first.failed_stages == ("admission",)
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='publication'")) == 0
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        # New runtime instance: no in-memory proposal survives the failed pass.
        second = await runtime(factory, installation, manager).run_once(admission_enabled=True)
        assert second.failed_stages == ()
        assert len(accepted) == 2 and accepted[0] == accepted[1]
        third = await runtime(factory, installation, manager).run_once(admission_enabled=True)
        assert third.failed_stages == () and len(accepted) == 2
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='publication'")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 1
