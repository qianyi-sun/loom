from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError

from loom.pipeline.keys import canonical_document
from loom.pipeline.public_api import (
    AcceptanceRecipeSubmissionV1,
    FailClosedAcceptanceRecipeAuthorityV1,
    FailClosedOfficialRecipeSubmissionAuthorityV1,
    IdempotencyKey,
    OfficialRecipeSubmissionRequestV1,
    PipelineIdempotencyEndpoint,
    PipelineRunCancelRequestV1,
    PipelineRunEventsQueryV1,
    PipelineRunListQueryV1,
    PipelineRunRetryRequestV1,
    PipelineRunSubmitRequestV1,
    pipeline_request_digest,
)

TEAM_ID = UUID("11111111-1111-4111-8111-111111111111")
AUTHORITY_ID = UUID("22222222-2222-4222-8222-222222222222")
ARTIFACT_ID = UUID("33333333-3333-4333-8333-333333333333")
DIGEST = "sha256:" + "a" * 64


def _budget(*, wall: int = 60) -> dict[str, object]:
    return {
        "max_artifact_bytes": 1024,
        "max_attempts_total": 2,
        "max_gpu_seconds": 0,
        "max_provider_cost_usd": "0.000000",
        "max_stage_runs": 2,
        "max_wall_seconds": wall,
    }


def _submit(**overrides: object) -> PipelineRunSubmitRequestV1:
    body: dict[str, object] = {
        "budget": _budget(),
        "inputs": {"dataset": ARTIFACT_ID},
        "parameters": {},
        "recipe": "behavior-recovery@1",
    }
    body.update(overrides)
    return PipelineRunSubmitRequestV1.model_validate(body)


def test_omitted_and_explicit_submit_defaults_have_one_digest() -> None:
    omitted = _submit()
    explicit = _submit(display_name=None, judge_profile_id=None)

    assert pipeline_request_digest(
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
        team_id=TEAM_ID,
        request=omitted,
    ) == pipeline_request_digest(
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
        team_id=TEAM_ID,
        request=explicit,
    )


def test_request_digest_is_exact_jcs_document_preimage() -> None:
    request = _submit()
    preimage = {
        "endpoint": "pipeline_run_submit",
        "team_id": TEAM_ID,
        "request": request.model_dump(mode="json", exclude_none=False),
    }

    assert (
        pipeline_request_digest(
            endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
            team_id=TEAM_ID,
            request=request,
        )
        == "sha256:" + hashlib.sha256(canonical_document(preimage)).hexdigest()
    )


def test_reordered_json_has_one_digest_and_semantic_change_does_not() -> None:
    first = _submit(parameters={"z": 2, "a": 1})
    reordered = _submit(parameters={"a": 1, "z": 2})
    changed = _submit(parameters={"a": 1, "z": 3})

    def digest(request: PipelineRunSubmitRequestV1) -> str:
        return pipeline_request_digest(
            endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
            team_id=TEAM_ID,
            request=request,
        )

    assert digest(first) == digest(reordered)
    assert digest(first) != digest(changed)


def test_endpoint_and_authenticated_team_are_digest_bound() -> None:
    request = _submit()
    submit = pipeline_request_digest(
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
        team_id=TEAM_ID,
        request=request,
    )
    other_team = pipeline_request_digest(
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
        team_id=UUID("44444444-4444-4444-8444-444444444444"),
        request=request,
    )

    assert submit != other_team
    with pytest.raises(TypeError, match="route registration"):
        pipeline_request_digest(
            endpoint="pipeline_run_submit",  # type: ignore[arg-type]
            team_id=TEAM_ID,
            request=request,
        )


@pytest.mark.parametrize(
    "value",
    ["", " leading", "trailing ", "tab\tinside", "line\nbreak", "é", "x" * 129],
)
def test_idempotency_key_rejects_noncanonical_values(value: str) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(IdempotencyKey).validate_python(value)


def test_idempotency_key_preserves_printable_ascii_verbatim() -> None:
    key = "run:behavior/S02!#$%&'*+-=?^_`{|}~"
    assert TypeAdapter(IdempotencyKey).validate_python(key) == key


@pytest.mark.parametrize(
    ("model", "body"),
    [
        (
            PipelineRunSubmitRequestV1,
            {
                "budget": _budget(),
                "inputs": {"dataset": ARTIFACT_ID},
                "parameters": {},
                "recipe": "behavior-recovery@1",
                "graph": {},
            },
        ),
        (PipelineRunRetryRequestV1, {"budget": _budget(), "checkpoint_id": ARTIFACT_ID}),
        (PipelineRunCancelRequestV1, {"reason": "stop", "signal": "KILL"}),
        (PipelineRunEventsQueryV1, {"after_seq": 0, "limit": 200, "watch": True}),
    ],
)
def test_public_dtos_are_strict_and_forbid_extra_fields(
    model: type[object], body: dict[str, object]
) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model.model_validate(body)  # type: ignore[attr-defined]


def test_public_dto_limits_and_nfc_normalization() -> None:
    request = _submit(display_name="Cafe\u0301")
    assert request.display_name == "Café"

    with pytest.raises(ValidationError):
        PipelineRunEventsQueryV1(after_seq=0, limit=501)
    with pytest.raises(ValidationError):
        PipelineRunCancelRequestV1(reason="x" * 501)
    with pytest.raises(ValidationError):
        PipelineRunListQueryV1(
            created_after=datetime(2026, 1, 2, tzinfo=UTC),
            created_before=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_internal_submission_requests_are_exact_and_strict() -> None:
    acceptance = AcceptanceRecipeSubmissionV1(
        schema_version="loom.acceptance-recipe-submission.v1",
        authorization_id=AUTHORITY_ID,
        candidate_sha256=DIGEST,
        recipe="behavior-recovery-acceptance-preflight@1",
    )
    official = OfficialRecipeSubmissionRequestV1(
        schema_version="loom.official-recipe-submission.v1",
        official_submission_kind="profile_calibration",
        authority_id=AUTHORITY_ID,
        request_identity_digest=DIGEST,
    )

    assert set(acceptance.model_dump()) == {
        "schema_version",
        "authorization_id",
        "candidate_sha256",
        "recipe",
    }
    assert set(official.model_dump()) == {
        "schema_version",
        "official_submission_kind",
        "authority_id",
        "request_identity_digest",
    }
    with pytest.raises(ValidationError):
        AcceptanceRecipeSubmissionV1.model_validate({**acceptance.model_dump(), "team_id": TEAM_ID})


@pytest.mark.asyncio
async def test_internal_authority_fakes_fail_closed() -> None:
    acceptance = AcceptanceRecipeSubmissionV1(
        schema_version="loom.acceptance-recipe-submission.v1",
        authorization_id=AUTHORITY_ID,
        candidate_sha256=DIGEST,
        recipe="behavior-recovery-acceptance-preflight@1",
    )
    official = OfficialRecipeSubmissionRequestV1(
        schema_version="loom.official-recipe-submission.v1",
        official_submission_kind="profile_calibration",
        authority_id=AUTHORITY_ID,
        request_identity_digest=DIGEST,
    )

    with pytest.raises(PermissionError, match="not configured"):
        await FailClosedAcceptanceRecipeAuthorityV1().load_and_lock(acceptance)
    with pytest.raises(PermissionError, match="not configured"):
        await FailClosedOfficialRecipeSubmissionAuthorityV1().load_and_lock(official)
