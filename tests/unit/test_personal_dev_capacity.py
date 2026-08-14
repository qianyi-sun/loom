from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from loom.personal_dev_capacity import (
    CapacityManagerPersonalDevProjector,
    PersonalDevCapacityManagerCheckpoint,
    PersonalDevCapacityProjectionConflictError,
    PersonalDevCapacityProjectionError,
)
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    SubjectConfigurationV1,
    canonical_bytes,
)
from tests.capacity_fixtures import development_projection, fleet_with_development_template


def _projection_response(request, *, configuration_epoch: int | None = None):
    fleet = fleet_with_development_template()
    template = fleet.development_subject_template
    assert template is not None
    account = AccountPolicyV1(
        account_id=f"dev-owner-{request.owner_id.hex}",
        kind="owner",
        owner_id=request.owner_id,
        min_reservation_slots=4,
        max_slots=8,
        max_surge_slots=1,
        max_pending_slots=8,
        max_pending_jobs=8,
        max_live_subjects=2,
    )
    subject = SubjectConfigurationV1(
        subject_id=request.subject_id,
        subject_incarnation=request.subject_incarnation,
        display_name=f"dev-{request.environment_name}",
        account_id=account.account_id,
        tier_id="development",
        min_slots=0 if request.operation_kind == "destroy" else request.min_slots,
        max_slots=0 if request.operation_kind == "destroy" else request.max_slots,
        rollout_surge_slots=template.rollout_surge_slots,
        max_pending_slots=template.max_pending_slots_per_subject,
        max_pending_jobs=template.max_pending_jobs_per_subject,
        lifecycle_state="disabled" if request.operation_kind == "destroy" else "active",
        candidate_generation=request.candidate_generation,
        deployment_generation=request.deployment_generation,
        configuration_generation=request.configuration_generation,
        demand_reporter_incarnation=request.demand_reporter_incarnation,
        profiles=template.profiles,
    )
    return {
        "configuration_epoch": configuration_epoch
        if configuration_epoch is not None
        else request.expected_configuration_epoch + 1,
        "configuration_digest": "a" * 64,
        "subject": subject.model_dump(mode="json"),
        "account": account.model_dump(mode="json"),
        "replayed": False,
    }


async def test_capacity_projector_reads_complete_shadow_checkpoint() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("https://capacity.example/v1/status")
        assert request.headers["Authorization"] == "Bearer lifecycle-token"
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "configuration_epoch": 7,
                "execution_state": "shadow",
                "execution_epoch": 0,
                "executable_new_capacity_ceiling": 0,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        projector = CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example",
            bearer_token="lifecycle-token",
            http_client=http,
        )
        assert await projector.current_manager_checkpoint() == (
            PersonalDevCapacityManagerCheckpoint(
                configuration_epoch=7,
                execution_state="shadow",
                execution_epoch=0,
                executable_new_capacity_ceiling=0,
            )
        )


async def test_capacity_projector_publishes_canonical_exact_request() -> None:
    projection = development_projection(expected_configuration_epoch=7)
    key = uuid4()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL(
            f"https://capacity.example/v1/development-projections/{projection.subject_id}"
        )
        assert request.headers["Authorization"] == "Bearer lifecycle-token"
        assert request.headers["Idempotency-Key"] == str(key)
        assert request.headers["Content-Type"] == "application/json"
        assert request.content == canonical_bytes(projection)
        return httpx.Response(
            200,
            headers={"content-type": "application/json; charset=utf-8"},
            json=_projection_response(projection),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example/",
            bearer_token="lifecycle-token",
            http_client=http,
        ).project(projection, idempotency_key=key)

    assert result.configuration_epoch == 8
    assert result.subject_id == projection.subject_id
    assert result.reporter_incarnation == projection.demand_reporter_incarnation


async def test_capacity_projector_accepts_only_a_disabled_zero_slot_retirement() -> None:
    projection = development_projection().model_copy(update={"operation_kind": "destroy"})

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json=_projection_response(projection),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example",
            bearer_token="lifecycle-token",
            http_client=http,
        ).project(projection, idempotency_key=uuid4())
    assert result.configuration_generation == projection.configuration_generation


async def test_capacity_projector_distinguishes_epoch_conflict() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            headers={"content-type": "application/json"},
            json={"detail": "capacity state conflict"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        projector = CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example",
            bearer_token="lifecycle-token",
            http_client=http,
        )
        with pytest.raises(PersonalDevCapacityProjectionConflictError):
            await projector.project(development_projection(), idempotency_key=uuid4())


async def test_capacity_projector_rejects_mismatched_or_executable_acknowledgement() -> None:
    projection = development_projection()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path == "/v1/status":
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={
                    "configuration_epoch": 1,
                    "execution_state": "active",
                    "execution_epoch": 7,
                    "executable_new_capacity_ceiling": 2,
                },
            )
        payload = _projection_response(projection, configuration_epoch=99)
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json=payload,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        projector = CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example",
            bearer_token="lifecycle-token",
            http_client=http,
        )
        assert await projector.current_manager_checkpoint() == (
            PersonalDevCapacityManagerCheckpoint(
                configuration_epoch=1,
                execution_state="active",
                execution_epoch=7,
                executable_new_capacity_ceiling=2,
            )
        )
        with pytest.raises(PersonalDevCapacityProjectionError, match="differs"):
            await projector.project(projection, idempotency_key=uuid4())
    assert calls == 2


@pytest.mark.parametrize(
    "payload",
    [
        {
            "configuration_epoch": 1,
            "execution_state": "shadow",
            "execution_epoch": 1,
            "executable_new_capacity_ceiling": 0,
        },
        {
            "configuration_epoch": 1,
            "execution_state": "prepared",
            "execution_epoch": 7,
            "executable_new_capacity_ceiling": 1,
        },
        {
            "configuration_epoch": 1,
            "execution_state": "active",
            "execution_epoch": 0,
            "executable_new_capacity_ceiling": 1,
        },
        {
            "configuration_epoch": 1,
            "execution_state": "drain-only",
            "execution_epoch": 7,
            "executable_new_capacity_ceiling": 1,
        },
    ],
)
async def test_capacity_projector_rejects_incoherent_manager_checkpoint(payload) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-type": "application/json"}, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        projector = CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example",
            bearer_token="lifecycle-token",
            http_client=http,
        )
        with pytest.raises(PersonalDevCapacityProjectionError, match="checkpoint"):
            await projector.current_manager_checkpoint()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payload: payload["subject"].update(lifecycle_state="disabled"), "differs"),
        (lambda payload: payload.update(configuration_digest="not-a-digest"), "invalid"),
    ],
)
async def test_capacity_projector_rejects_non_active_or_invalid_acknowledgement(
    mutation,
    message: str,
) -> None:
    projection = development_projection()

    async def handler(_request: httpx.Request) -> httpx.Response:
        payload = _projection_response(projection)
        mutation(payload)
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json=payload,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        projector = CapacityManagerPersonalDevProjector(
            manager_origin="https://capacity.example",
            bearer_token="lifecycle-token",
            http_client=http,
        )
        with pytest.raises(PersonalDevCapacityProjectionError, match=message):
            await projector.project(projection, idempotency_key=uuid4())
