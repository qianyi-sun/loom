"""POST /admin/step-tokens — mint a step-scoped JWT (Plan 9 Task 4).

Worker calls this once per step to obtain a bearer token the agent
presents at the Gateway. The Gateway verifies the JWT and extracts
`(team_id, trial_id, step_id)` for cost attribution.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select

from loom.auth import mint_step_jwt, verify_bearer_token
from loom.db.schema import (
    PipelineRun,
    PipelineStageRun,
    ProviderConnection,
    ProviderConnectionShare,
)
from loom.db.schema import (
    Trial as TrialRow,
)
from loom_control_plane.execution_attempt_fencing import (
    AttemptFenceError,
    verify_attempt_claim,
)

router = APIRouter(prefix="/admin")
_MAX_STEP_TOKEN_TTL_SEC = 30_000
OptionalClaimIdHeader = Annotated[UUID | None, Header(alias="X-Loom-Claim-Id")]
OptionalLeaseEpochHeader = Annotated[int | None, Header(alias="X-Loom-Lease-Epoch")]
OptionalLeaseTokenHeader = Annotated[str | None, Header(alias="X-Loom-Lease-Token")]


class _IssueStepTokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    team_id: UUID
    trial_id: UUID | None = None
    execution_attempt_id: UUID | None = None
    step_id: str = Field(min_length=1, max_length=64)
    ttl_sec: int = Field(gt=0, le=_MAX_STEP_TOKEN_TTL_SEC)
    # Only the dedicated family-orchestrator credential may set this field.
    # Presence is significant: explicit null means the evolver must use the
    # platform route even when the completed trial used a BYO connection.
    provider_connection_id: UUID | None = None

    @model_validator(mode="after")
    def exactly_one_subject(self) -> _IssueStepTokenRequest:
        if (self.trial_id is None) == (self.execution_attempt_id is None):
            raise ValueError("exactly one step-token subject is required")
        return self


@router.post("/step-tokens", status_code=201)
async def issue_step_token(
    request: Request,
    payload: _IssueStepTokenRequest,
    authorization: str | None = Header(default=None),
    claim_id: OptionalClaimIdHeader = None,
    lease_epoch: OptionalLeaseEpochHeader = None,
    lease_token: OptionalLeaseTokenHeader = None,
) -> dict[str, str]:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            signing_key=signing_key,
            allow_family_orchestrator=True,
        )
    explicit_provider = "provider_connection_id" in payload.model_fields_set
    family_evolver_request = payload.step_id == "family_evolver"
    if ctx is None:
        raise HTTPException(
            status_code=403,
            detail="missing step-token scope",
        )
    if family_evolver_request:
        if (
            ctx.type != "family_orchestrator"
            or ctx.team_id is not None
            or ctx.user_id is not None
            or ctx.scopes != ["family:evolve"]
            or not explicit_provider
            or payload.execution_attempt_id is not None
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "family_evolver step tokens require the dedicated principal and "
                    "an explicit provider_connection_id field"
                ),
            )
    elif explicit_provider or "worker:report" not in ctx.scopes:
        raise HTTPException(
            status_code=403,
            detail="provider override requires the family:evolve scope",
        )

    # Plan 9 audit fix: verify the trial exists and matches the team
    # the worker claims to be acting on behalf of. Without this, a
    # buggy/compromised worker could mint step tokens for fictional
    # trial_ids and the Gateway would silently attribute orphan
    # llm_calls rows.
    #
    # Pull the agent provider from the trial row by default. Only a dedicated
    # family:evolve credential may override this value (explicit null selects
    # the platform route); the CP still authorizes a configured override
    # against the authoritative trial team before minting the JWT.
    subject_provider_connection_id: UUID | None = None
    attempt_step_jwt_id: UUID | None = None
    attempt_lease_epoch: int | None = None
    attempt_execution_spec_digest: str | None = None
    attempt_control_binding_digest: str | None = None
    attempt_execution_authorization_digest: str | None = None
    if payload.trial_id is not None:
        async with request.app.state.session_factory() as session:
            trial_row = (
                await session.execute(
                    select(TrialRow.team_id, TrialRow.provider_connection_id).where(
                        TrialRow.id == payload.trial_id
                    ),
                )
            ).one_or_none()
        if trial_row is None:
            raise HTTPException(status_code=404, detail="trial not found")
        subject_team, subject_provider_connection_id = trial_row
    else:
        assert payload.execution_attempt_id is not None
        async with request.app.state.session_factory() as session:
            if claim_id is None or lease_epoch is None or lease_token is None:
                raise HTTPException(status_code=409, detail="claim_fenced")
            try:
                attempt = await verify_attempt_claim(
                    session,
                    attempt_id=payload.execution_attempt_id,
                    auth=ctx,
                    claim_id=claim_id,
                    lease_epoch=lease_epoch,
                    lease_token=lease_token,
                    require_live_lease=True,
                    lock=True,
                )
            except AttemptFenceError as exc:
                raise HTTPException(status_code=409, detail="claim_fenced") from exc
            attempt_row = (
                await session.execute(
                    select(
                        PipelineRun.team_id,
                        PipelineRun.state,
                        PipelineStageRun.resolved_execution_spec_json,
                        PipelineStageRun.execution_spec_digest,
                        PipelineStageRun.provider_connection_ref,
                        PipelineStageRun.node_key,
                        PipelineStageRun.state,
                    )
                    .join(PipelineStageRun, PipelineStageRun.pipeline_run_id == PipelineRun.id)
                    .where(PipelineStageRun.id == attempt.stage_run_id)
                )
            ).one_or_none()
            if attempt_row is None:
                raise HTTPException(status_code=404, detail="execution attempt not found")
            (
                subject_team,
                run_state,
                execution_spec,
                execution_spec_digest,
                subject_provider_connection_id,
                node_key,
                stage_state,
            ) = attempt_row
            if run_state != "running" or stage_state not in {"claimed", "running"}:
                raise HTTPException(status_code=409, detail="execution_attempt_not_dispatchable")
            if payload.step_id != node_key:
                raise HTTPException(status_code=409, detail="execution_attempt_step_mismatch")
            if not isinstance(execution_spec, dict) or not isinstance(
                execution_spec_digest, str
            ):
                raise HTTPException(status_code=409, detail="execution_attempt_spec_unavailable")
            container_node = execution_spec.get("container_node")
            if not isinstance(container_node, dict) or container_node.get(
                "network_profile"
            ) != "gateway":
                raise HTTPException(
                    status_code=409, detail="gateway token forbidden for network none"
                )
            timeout_seconds = container_node.get("timeout_seconds")
            if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool):
                raise HTTPException(status_code=409, detail="execution_attempt_spec_unavailable")
            expected_ttl = min(timeout_seconds + 300, _MAX_STEP_TOKEN_TTL_SEC)
            if payload.ttl_sec != expected_ttl:
                raise HTTPException(status_code=409, detail="execution_attempt_ttl_mismatch")
            control_refs = execution_spec.get("control_binding_snapshots")
            if not isinstance(control_refs, list) or len(control_refs) > 1:
                raise HTTPException(
                    status_code=409, detail="execution_attempt_control_binding_unavailable"
                )
            control_binding_snapshot_digest: str | None = None
            if control_refs:
                control_ref = control_refs[0]
                if not isinstance(control_ref, dict) or not isinstance(
                    control_ref.get("snapshot_sha256"), str
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="execution_attempt_control_binding_unavailable",
                    )
                control_binding_snapshot_digest = control_ref["snapshot_sha256"]
            if subject_team != payload.team_id:
                raise HTTPException(status_code=404, detail="step-token subject not found")
            token_id = uuid4()
            attempt.step_jwt_id = token_id
            attempt.version += 1
            attempt_step_jwt_id = token_id
            attempt_lease_epoch = attempt.lease_epoch
            attempt_execution_spec_digest = execution_spec_digest
            attempt_control_binding_digest = control_binding_snapshot_digest
            attempt_execution_authorization_digest = attempt.execution_authorization_digest
            await session.commit()

    if subject_team != payload.team_id:
        if payload.trial_id is not None:
            raise HTTPException(
                status_code=400,
                detail=f"team_id {payload.team_id} does not own trial {payload.trial_id}",
            )
        raise HTTPException(status_code=404, detail="step-token subject not found")

    effective_provider_connection_id = subject_provider_connection_id
    if family_evolver_request:
        effective_provider_connection_id = payload.provider_connection_id
        if effective_provider_connection_id is not None:
            async with request.app.state.session_factory() as provider_session:
                connection_team = (
                    await provider_session.execute(
                        select(ProviderConnection.team_id).where(
                            ProviderConnection.id == effective_provider_connection_id,
                            ProviderConnection.deleted_at.is_(None),
                        ),
                    )
                ).scalar_one_or_none()
                if connection_team is None:
                    raise HTTPException(
                        status_code=404,
                        detail="provider_connection not found",
                    )
                if connection_team != subject_team:
                    shared = (
                        await provider_session.execute(
                            select(ProviderConnectionShare.provider_connection_id).where(
                                ProviderConnectionShare.provider_connection_id
                                == effective_provider_connection_id,
                                ProviderConnectionShare.target_team_id == subject_team,
                            ),
                        )
                    ).scalar_one_or_none()
                    if shared is None:
                        raise HTTPException(
                            status_code=404,
                            detail="provider_connection not found",
                        )

    token = mint_step_jwt(
        team_id=payload.team_id,
        trial_id=payload.trial_id,
        execution_attempt_id=payload.execution_attempt_id,
        step_id=payload.step_id,
        ttl_sec=payload.ttl_sec,
        signing_key=signing_key,
        provider_connection_id=effective_provider_connection_id,
        provider_connection_id_bound=(
            family_evolver_request or payload.execution_attempt_id is not None
        ),
        step_jwt_id=attempt_step_jwt_id,
        execution_attempt_lease_epoch=attempt_lease_epoch,
        execution_spec_digest=attempt_execution_spec_digest,
        control_binding_snapshot_digest=attempt_control_binding_digest,
        execution_authorization_digest=attempt_execution_authorization_digest,
    )
    expires_at = datetime.now(UTC) + timedelta(seconds=payload.ttl_sec)
    return {
        "token": token,
        "expires_at": expires_at.isoformat(),
    }
