"""Transactional service layer for the public official-Recipe Pipeline API."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ApiIdempotencyRecord,
    Artifact,
    ArtifactUploadSession,
    PipelineBudgetLedger,
    PipelineEvent,
    PipelineInputImport,
    PipelineRun,
    PipelineRunControlBinding,
    PipelineRunGpuBackendSelection,
    PipelineStageRun,
)
from loom.pipeline.budget import TerminalCause
from loom.pipeline.gpu_backend import select_ordinary_gpu_backend
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.public_api import (
    PipelineIdempotencyEndpoint,
    PipelineRunRetryRequestV1,
    PipelineRunSubmitRequestV1,
    pipeline_request_digest,
)
from loom.pipeline.recipes import OfficialRecipeRegistry
from loom.pipeline.resource_profiles import ResourceProfileRegistry, ResourceProfileRegistryError
from loom.pipeline.spec import RunGraphSpecV1
from loom_control_plane.metrics import PIPELINE_LIVE_PREVIEW_PURGES_TOTAL


class PipelineApiError(RuntimeError):
    def __init__(self, status_code: int, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reason_code = reason_code
        self.message = message


async def _behavior_unknown_input_is_admissible(
    session: AsyncSession,
    *,
    artifact: Artifact,
    team_id: UUID,
    recipe_name: str,
    recipe_version: int,
    recipe_digest: str,
    input_name: str,
) -> bool:
    """The sole ordinary-use exception for an unknown-safety Artifact."""

    expected_types = {
        "dataset": "behavior_dataset_snapshot.v1",
        "policy": "behavior_policy_checkpoint.v1",
        "mop_bank": "behavior_mop_bank.v1",
    }
    if (
        (recipe_name, recipe_version) != ("behavior-recovery", 1)
        or expected_types.get(input_name) != artifact.artifact_type
        or artifact.team_id != team_id
        or artifact.safety_state != "unknown"
        or artifact.producer_kind != "input_import"
        or artifact.pipeline_input_import_id is None
        or artifact.artifact_upload_session_id is None
        or artifact.manifest_sha256 is None
    ):
        return False
    imported = await session.get(PipelineInputImport, artifact.pipeline_input_import_id)
    upload = await session.get(ArtifactUploadSession, artifact.artifact_upload_session_id)
    return bool(
        imported is not None
        and upload is not None
        and imported.team_id == team_id
        and imported.kind == input_name
        and imported.target_artifact_type == artifact.artifact_type
        and imported.trust_class == "internal_trusted"
        and imported.state == "committed"
        and imported.recipe_name == recipe_name
        and imported.recipe_version == recipe_version
        and imported.recipe_digest == recipe_digest
        and imported.committed_artifact_id == artifact.id
        and imported.artifact_upload_session_id == upload.id
        and upload.state == "committed"
        and upload.manifest_sha256 == artifact.provenance.get("root_manifest_sha256")
        and upload.committed_marker_sha256 == artifact.provenance.get("marker_sha256")
    )


@dataclass(frozen=True, slots=True)
class IdempotencyOutcome:
    record: ApiIdempotencyRecord
    replay: bool


def split_recipe(value: str) -> tuple[str, int]:
    name, raw_version = value.rsplit("@", 1)
    return name, int(raw_version)


def _logical_control_slots(graph: RunGraphSpecV1) -> tuple[tuple[str, str], ...]:
    aliases = {
        "offline_judge": "behavior_offline_judge",
        "recovery_primitive": "behavior_recovery_primitive",
    }
    return tuple(
        sorted(
            (
                (aliases.get(node.node_key, node.node_key), node.node_key)
                for node in graph.nodes
                if getattr(node, "network_profile", None) == "gateway"
            ),
            key=lambda item: item[0].encode(),
        )
    )


def _graph_has_gpu_nodes(graph: RunGraphSpecV1) -> bool:
    if graph.budget.max_gpu_seconds == 0:
        return False
    registry = ResourceProfileRegistry.load()
    try:
        return any(
            any(
                variant.gpu_count_exact > 0
                for variant in registry.get(node.resource_profile).profile.execution_variants
            )
            for node in graph.nodes
            if hasattr(node, "resource_profile")
        )
    except ResourceProfileRegistryError as exc:
        raise PipelineApiError(
            422, "stage_request_invalid", "Resource profile is unavailable"
        ) from exc


def _add_ordinary_gpu_selection(
    session: AsyncSession, run: PipelineRun, graph: RunGraphSpecV1
) -> None:
    if not _graph_has_gpu_nodes(graph):
        return
    selection = select_ordinary_gpu_backend(
        recipe_digest=run.recipe_digest,
        pipeline_run_id=run.id,
        selected_at=run.created_at,
    )
    value = selection.model_dump(mode="json")
    session.add(
        PipelineRunGpuBackendSelection(
            pipeline_run_id=run.id,
            scope=selection.scope,
            variant_id=selection.variant_id,
            policy_id=selection.policy_id,
            selection_source=selection.selection_source,
            selected_at=selection.selected_at,
            selection_json=value,
            selection_bytes=canonical_document(value),
            gpu_backend_selection_sha256=selection.gpu_backend_selection_sha256,
        )
    )


def encode_pipeline_cursor(
    *, created_at: datetime, item_id: UUID, filter_digest: str, signing_key: bytes
) -> str:
    import base64

    payload = json.dumps(
        {"f": filter_digest, "i": str(item_id), "t": created_at.isoformat(), "v": 1},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    signature = hmac.new(signing_key, payload, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(payload + signature).decode().rstrip("=")


def _decode_canonical_urlsafe_base64(value: str) -> bytes:
    import base64

    raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    canonical = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    if not hmac.compare_digest(value, canonical):
        raise ValueError("non-canonical base64url")
    return raw


def decode_pipeline_cursor(
    value: str, *, filter_digest: str, signing_key: bytes
) -> tuple[datetime, UUID]:
    try:
        raw = _decode_canonical_urlsafe_base64(value)
        payload, signature = raw[:-32], raw[-32:]
        if not hmac.compare_digest(
            signature, hmac.new(signing_key, payload, hashlib.sha256).digest()
        ):
            raise ValueError("signature")
        item = json.loads(payload)
        if item != {"f": item.get("f"), "i": item.get("i"), "t": item.get("t"), "v": 1}:
            raise ValueError("shape")
        if item["f"] != filter_digest:
            raise ValueError("filter")
        created_at = datetime.fromisoformat(item["t"])
        if created_at.tzinfo is None:
            raise ValueError("timezone")
        return created_at, UUID(item["i"])
    except Exception as exc:
        raise PipelineApiError(422, "invalid_cursor", "Pipeline cursor is invalid") from exc


def encode_pipeline_stage_cursor(
    *, node_key: str, shard_key: str, filter_digest: str, signing_key: bytes
) -> str:
    import base64

    payload = json.dumps(
        {"f": filter_digest, "n": node_key, "s": shard_key, "v": 1},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    signature = hmac.new(signing_key, payload, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(payload + signature).decode().rstrip("=")


def decode_pipeline_stage_cursor(
    value: str, *, filter_digest: str, signing_key: bytes
) -> tuple[str, str]:
    try:
        raw = _decode_canonical_urlsafe_base64(value)
        payload, signature = raw[:-32], raw[-32:]
        if not hmac.compare_digest(
            signature, hmac.new(signing_key, payload, hashlib.sha256).digest()
        ):
            raise ValueError("signature")
        item = json.loads(payload)
        if item != {"f": item.get("f"), "n": item.get("n"), "s": item.get("s"), "v": 1}:
            raise ValueError("shape")
        if item["f"] != filter_digest:
            raise ValueError("filter")
        node_key = item["n"]
        shard_key = item["s"]
        if not isinstance(node_key, str) or not isinstance(shard_key, str):
            raise ValueError("keys")
        return node_key, shard_key
    except Exception as exc:
        raise PipelineApiError(422, "invalid_cursor", "Pipeline cursor is invalid") from exc


async def claim_idempotency(
    session: AsyncSession,
    *,
    team_id: UUID,
    endpoint: PipelineIdempotencyEndpoint,
    key: str,
    request_digest: str,
) -> IdempotencyOutcome:
    now = datetime.now(UTC)
    inserted_id = (
        await session.execute(
            text("""
                INSERT INTO api_idempotency_records (
                    team_id, endpoint, idempotency_key, request_digest, expires_at
                ) VALUES (:team_id, :endpoint, :key, :digest, :expires_at)
                ON CONFLICT (team_id, endpoint, idempotency_key)
                    WHERE team_id IS NOT NULL DO NOTHING
                RETURNING id
            """),
            {
                "team_id": team_id,
                "endpoint": endpoint.value,
                "key": key,
                "digest": request_digest,
                "expires_at": now + timedelta(days=3650),
            },
        )
    ).scalar_one_or_none()
    row = (
        await session.execute(
            select(ApiIdempotencyRecord)
            .where(
                ApiIdempotencyRecord.team_id == team_id,
                ApiIdempotencyRecord.endpoint == endpoint.value,
                ApiIdempotencyRecord.idempotency_key == key,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise RuntimeError("idempotency insert was not readable in its transaction")
    if inserted_id is None:
        if not hmac.compare_digest(row.request_digest, request_digest):
            raise PipelineApiError(409, "idempotency_conflict", "Idempotency key body differs")
        if row.state != "completed" or row.response_json is None:
            raise PipelineApiError(
                409, "idempotency_in_progress", "Identical request is in progress"
            )
        return IdempotencyOutcome(record=row, replay=True)
    return IdempotencyOutcome(record=row, replay=False)


def complete_idempotency(
    record: ApiIdempotencyRecord,
    *,
    resource_type: str,
    resource_id: UUID,
    response_status: int,
    response_json: dict[str, Any],
) -> None:
    record.state = "completed"
    record.resource_type = resource_type
    record.resource_id = resource_id
    record.response_status = response_status
    record.response_json = response_json
    record.completed_at = datetime.now(UTC)


def run_projection(run: PipelineRun) -> dict[str, Any]:
    return {
        "id": str(run.id),
        "display_name": run.display_name,
        "recipe": {
            "name": run.recipe_name,
            "version": run.recipe_version,
            "digest": run.recipe_digest,
        },
        "graph_digest": run.graph_spec_digest,
        "control_binding_snapshots_digest": run.control_binding_snapshots_digest,
        "parameters_digest": run.parameters_digest,
        "request_digest": run.request_digest,
        "state": run.state,
        "result": run.result,
        "reason": run.result_reason,
        "created_by_user_id": str(run.created_by_user_id)
        if getattr(run, "created_by_user_id", None)
        else None,
        "retry_of_pipeline_run_id": str(run.retry_of_pipeline_run_id)
        if run.retry_of_pipeline_run_id
        else None,
        "retry_from_stage_run_id": str(run.retry_from_stage_run_id)
        if run.retry_from_stage_run_id
        else None,
        "created_at": run.created_at.isoformat(),
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "cancellation_requested_at": (
            run.cancellation_requested_at.isoformat() if run.cancellation_requested_at else None
        ),
        "source_budget": getattr(
            run,
            "budget_json",
            {
                "max_provider_cost_usd": "0",
                "max_gpu_seconds": 0,
                "max_wall_seconds": 1,
                "max_artifact_bytes": 1,
                "max_stage_runs": 1,
                "max_attempts_total": 1,
            },
        ),
    }


async def create_public_run(
    session: AsyncSession,
    *,
    team_id: UUID,
    user_id: UUID,
    idempotency_key: str,
    request: PipelineRunSubmitRequestV1,
    registry: OfficialRecipeRegistry,
    binding_resolver: Any | None = None,
) -> tuple[dict[str, Any], bool]:
    digest = pipeline_request_digest(
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
        team_id=team_id,
        request=request,
    )
    outcome = await claim_idempotency(
        session,
        team_id=team_id,
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT,
        key=idempotency_key,
        request_digest=digest,
    )
    if outcome.replay:
        return dict(outcome.record.response_json or {}), True
    name, version = split_recipe(request.recipe)
    try:
        registration = registry.get(name, version)
        if registration.submission_policy != "ordinary":
            raise PermissionError
        graph = registry.resolve_ordinary(name, version, request.parameters)
    except KeyError as exc:
        # Unknown identities are indistinguishable from controller-only Recipes.
        # This keeps the hidden Stage 1 smoke surface 404 for every public role.
        raise PipelineApiError(404, "not_found", "Official Recipe was not found") from exc
    except (PermissionError, ValueError) as exc:
        raise PipelineApiError(
            422, "recipe_invalid", "Official Recipe is unavailable or invalid"
        ) from exc
    if graph.budget != request.budget:
        value = graph.model_dump(mode="python")
        value["budget"] = request.budget.model_dump(mode="python")
        graph = RunGraphSpecV1.model_validate(value)
    logical_slots = _logical_control_slots(graph)
    binding_items: list[dict[str, Any]] = []
    if logical_slots:
        if binding_resolver is None:
            raise PipelineApiError(
                422, "binding_unavailable", "Recipe control binding resolver is unavailable"
            )
        binding_result = await binding_resolver.resolve(
            team_id,
            registration.identity,
            request.judge_profile_id,
            tuple(item[0] for item in logical_slots),
            session=session,
        )
        binding_items = [item.model_dump(mode="json") for item in binding_result.items]
        if (
            tuple((item["logical_name"], item["node_key"]) for item in binding_items)
            != logical_slots
        ):
            raise PipelineApiError(
                422, "binding_drift", "Recipe control bindings do not match logical slots"
            )
    expected = {item.name: item.artifact_type for item in graph.inputs}
    if set(request.inputs) != set(expected):
        raise PipelineApiError(
            422, "input_contract_mismatch", "Pipeline inputs do not match Recipe"
        )
    artifacts = list(
        (
            await session.execute(
                select(Artifact).where(
                    Artifact.id.in_(request.inputs.values()), Artifact.team_id == team_id
                )
            )
        ).scalars()
    )
    by_id = {item.id: item for item in artifacts}
    resolved: list[dict[str, Any]] = []
    for input_name in sorted(request.inputs, key=lambda item: item.encode()):
        artifact = by_id.get(request.inputs[input_name])
        if (
            artifact is None
            or artifact.artifact_type != expected[input_name]
            or artifact.manifest_sha256 is None
        ):
            raise PipelineApiError(422, "input_not_reusable", "Pipeline input is not reusable")
        if artifact.safety_state not in {"verified_internal", "verified"} and not (
            await _behavior_unknown_input_is_admissible(
                session,
                artifact=artifact,
                team_id=team_id,
                recipe_name=name,
                recipe_version=version,
                recipe_digest=registration.digest,
                input_name=input_name,
            )
        ):
            raise PipelineApiError(422, "input_not_reusable", "Pipeline input is not reusable")
        resolved.append(
            {
                "input_name": input_name,
                "artifact_id": str(artifact.id),
                "artifact_type": artifact.artifact_type,
                "content_sha256": artifact.content_hash,
                "manifest_sha256": artifact.manifest_sha256,
            }
        )
    run = PipelineRun(
        id=uuid4(),
        created_at=datetime.now(UTC),
        team_id=team_id,
        created_by_user_id=user_id,
        display_name=request.display_name,
        submission_policy="ordinary",
        recipe_name=name,
        recipe_version=version,
        recipe_digest=registration.digest,
        graph_spec_json=graph.model_dump(mode="json", exclude_none=False),
        graph_spec_digest=canonical_digest(graph),
        parameters_json=request.parameters,
        parameters_digest=canonical_digest(request.parameters),
        resolved_inputs_json=resolved,
        control_binding_snapshots_json=binding_items,
        control_binding_snapshots_digest=canonical_digest(binding_items),
        budget_json=request.budget.model_dump(mode="json"),
        request_digest=digest,
        idempotency_key=f"{PipelineIdempotencyEndpoint.PIPELINE_RUN_SUBMIT.value}:{idempotency_key}",
        state="submitted",
    )
    session.add(run)
    if binding_items:
        assert binding_resolver is not None
        await binding_resolver.persist_run_bindings(
            session, pipeline_run_id=run.id, items=binding_result
        )
    _add_ordinary_gpu_selection(session, run, graph)
    await session.flush()
    response = run_projection(run)
    complete_idempotency(
        outcome.record,
        resource_type="pipeline_run",
        resource_id=run.id,
        response_status=201,
        response_json=response,
    )
    return response, False


async def create_retry_run(
    session: AsyncSession,
    *,
    team_id: UUID,
    user_id: UUID,
    stage: PipelineStageRun,
    idempotency_key: str,
    request: PipelineRunRetryRequestV1,
    registry: OfficialRecipeRegistry,
    binding_resolver: Any | None = None,
) -> tuple[dict[str, Any], bool]:
    digest = pipeline_request_digest(
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_STAGE_RETRY,
        team_id=team_id,
        request=request,
    )
    outcome = await claim_idempotency(
        session,
        team_id=team_id,
        endpoint=PipelineIdempotencyEndpoint.PIPELINE_STAGE_RETRY,
        key=idempotency_key,
        request_digest=digest,
    )
    if outcome.replay:
        return dict(outcome.record.response_json or {}), True
    source = await session.get(PipelineRun, stage.pipeline_run_id, with_for_update=True)
    if source is None or source.team_id != team_id:
        raise PipelineApiError(404, "not_found", "Pipeline stage was not found")
    if source.submission_policy != "ordinary" or source.official_submission_kind is not None:
        raise PipelineApiError(
            409, "controller_owned_run", "Controller-owned run cannot be retried"
        )
    if source.state != "finished" or source.result not in {"failed", "partial_failed"}:
        raise PipelineApiError(409, "run_not_retryable", "Pipeline run is not retryable")
    if stage.state != "failed":
        raise PipelineApiError(409, "stage_not_failed", "Selected stage did not fail")
    if stage.reason_code in {
        "user_cancel",
        "provider_budget",
        "gpu_budget",
        "artifact_budget",
        "stage_run_budget",
        "attempt_budget",
        "wall_budget",
        "accounting_violation",
    }:
        raise PipelineApiError(
            409, "run_not_retryable", "Cancelled or exhausted runs require a new submission"
        )
    try:
        registration = registry.get(source.recipe_name, source.recipe_version)
    except KeyError as exc:
        raise PipelineApiError(
            409, "recipe_snapshot_unavailable", "Recipe snapshot is unavailable"
        ) from exc
    if registration.submission_policy != "ordinary" or not hmac.compare_digest(
        registration.digest, source.recipe_digest
    ):
        raise PipelineApiError(409, "recipe_snapshot_unavailable", "Recipe snapshot is unavailable")
    graph = RunGraphSpecV1.model_validate(source.graph_spec_json)
    artifact_ids = [UUID(item["artifact_id"]) for item in source.resolved_inputs_json]
    current_artifacts = list(
        (
            await session.execute(
                select(Artifact).where(Artifact.id.in_(artifact_ids), Artifact.team_id == team_id)
            )
        ).scalars()
    )
    current_by_id = {item.id: item for item in current_artifacts}
    for frozen in source.resolved_inputs_json:
        artifact = current_by_id.get(UUID(frozen["artifact_id"]))
        if artifact is None:
            raise PipelineApiError(409, "input_not_reusable", "Original input is unavailable")
        if (
            artifact.artifact_type != frozen["artifact_type"]
            or artifact.content_hash != frozen["content_sha256"]
            or artifact.manifest_sha256 != frozen["manifest_sha256"]
        ):
            raise PipelineApiError(409, "input_drift", "Original input snapshot drifted")
        if artifact.safety_state not in {"verified_internal", "verified"} and not (
            await _behavior_unknown_input_is_admissible(
                session,
                artifact=artifact,
                team_id=team_id,
                recipe_name=source.recipe_name,
                recipe_version=source.recipe_version,
                recipe_digest=source.recipe_digest,
                input_name=frozen["input_name"],
            )
        ):
            raise PipelineApiError(409, "input_not_reusable", "Original input is not reusable")
    logical_slots = _logical_control_slots(graph)
    if logical_slots:
        if binding_resolver is None:
            raise PipelineApiError(409, "binding_drift", "Control binding resolver is unavailable")
        # A full replay freezes the source Run's immutable rows.  Admin updates
        # must not substitute a newer current version into a retry.
        source_rows = list(
            (
                await session.execute(
                    select(PipelineRunControlBinding).where(
                        PipelineRunControlBinding.pipeline_run_id == source.id
                    )
                )
            ).scalars()
        )
        if len(source_rows) != len(logical_slots):
            raise PipelineApiError(409, "binding_drift", "Control binding rows are incomplete")
    candidate_graph = graph.model_dump(mode="python")
    candidate_graph["budget"] = request.budget.model_dump(mode="python")
    try:
        RunGraphSpecV1.model_validate(candidate_graph)
    except ValueError as exc:
        raise PipelineApiError(409, "budget_invalid", "Retry budget is invalid") from exc
    new_run = PipelineRun(
        id=uuid4(),
        created_at=datetime.now(UTC),
        team_id=team_id,
        created_by_user_id=user_id,
        display_name=request.display_name,
        submission_policy="ordinary",
        recipe_name=source.recipe_name,
        recipe_version=source.recipe_version,
        recipe_digest=source.recipe_digest,
        graph_spec_json=source.graph_spec_json,
        graph_spec_digest=source.graph_spec_digest,
        parameters_json=source.parameters_json,
        parameters_digest=source.parameters_digest,
        resolved_inputs_json=source.resolved_inputs_json,
        control_binding_snapshots_json=source.control_binding_snapshots_json,
        control_binding_snapshots_digest=source.control_binding_snapshots_digest,
        budget_json=request.budget.model_dump(mode="json"),
        request_digest=digest,
        idempotency_key=f"{PipelineIdempotencyEndpoint.PIPELINE_STAGE_RETRY.value}:{idempotency_key}",
        state="submitted",
        retry_of_pipeline_run_id=source.id,
        retry_from_stage_run_id=stage.id,
    )
    session.add(new_run)
    if logical_slots:
        for frozen_binding in source_rows:
            session.add(
                PipelineRunControlBinding(
                    pipeline_run_id=new_run.id,
                    logical_name=frozen_binding.logical_name,
                    kind=frozen_binding.kind,
                    node_key=frozen_binding.node_key,
                    source_object_id=frozen_binding.source_object_id,
                    source_version=frozen_binding.source_version,
                    snapshot_json=frozen_binding.snapshot_json,
                    snapshot_bytes=frozen_binding.snapshot_bytes,
                    snapshot_sha256=frozen_binding.snapshot_sha256,
                    provider_connection_id=frozen_binding.provider_connection_id,
                    provider_request_limit=frozen_binding.provider_request_limit,
                    provider_cost_limit_microusd=frozen_binding.provider_cost_limit_microusd,
                    per_call_timeout_seconds=frozen_binding.per_call_timeout_seconds,
                )
            )
    _add_ordinary_gpu_selection(session, new_run, graph)
    await session.flush()
    response = run_projection(new_run)
    complete_idempotency(
        outcome.record,
        resource_type="pipeline_run",
        resource_id=new_run.id,
        response_status=201,
        response_json=response,
    )
    return response, False


async def request_user_cancellation(
    session: AsyncSession, *, run: PipelineRun, actor_id: UUID, reason: str
) -> tuple[dict[str, Any], bool]:
    if (
        run.submission_policy != "ordinary"
        or run.official_submission_kind is not None
        or run.official_submission_authority_id is not None
        or run.official_submission_authority_snapshot_digest is not None
        or run.official_submission_identity_digest is not None
    ):
        raise PipelineApiError(
            409, "controller_owned_run", "Controller-owned run cannot be cancelled"
        )
    ledger = await session.get(PipelineBudgetLedger, run.id, with_for_update=True)
    if run.state == "finished" or (ledger is not None and ledger.terminal_cause is not None):
        body = run_projection(run)
        body["terminal_cause"] = ledger.terminal_cause if ledger is not None else None
        body["terminal_cause_at"] = (
            ledger.terminal_cause_at.isoformat()
            if ledger is not None and ledger.terminal_cause_at is not None
            else None
        )
        return body, True
    if ledger is None:
        graph = RunGraphSpecV1.model_validate(run.graph_spec_json)
        budget = graph.budget
        ledger = PipelineBudgetLedger(
            pipeline_run_id=run.id,
            provider_limit_microusd=int(Decimal(budget.max_provider_cost_usd) * 1_000_000),
            gpu_limit_seconds=budget.max_gpu_seconds,
            artifact_limit_bytes=budget.max_artifact_bytes,
            stage_run_limit=budget.max_stage_runs,
            attempt_limit=budget.max_attempts_total,
            wall_deadline_at=run.created_at + timedelta(seconds=budget.max_wall_seconds),
        )
        session.add(ledger)
        await session.flush()
    ledger.terminal_cause = TerminalCause.USER_CANCEL.value
    ledger.terminal_cause_at = datetime.now(UTC)
    ledger.version += 1
    run.state = "cancelling"
    run.cancellation_requested_at = datetime.now(UTC)
    run.version += 1
    seq = run.next_event_seq
    run.next_event_seq += 1
    session.add(
        PipelineEvent(
            pipeline_run_id=run.id,
            seq=seq,
            event_type="terminal_cause_latched",
            actor_kind="user",
            actor_id=str(actor_id),
            payload_json={"terminal_cause": "user_cancel", "reason": reason},
        )
    )
    released = (
        (
            await session.execute(
                text("""
            SELECT b.kind, b.reserved_amount
              FROM pipeline_budget_reservations b
              JOIN execution_attempts a ON a.id=b.execution_attempt_id
              JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
             WHERE s.pipeline_run_id=:run_id AND b.state='active'
               AND a.state IN ('fault_pending','queued')
             FOR UPDATE OF b, a, s
        """),
                {"run_id": run.id},
            )
        )
        .mappings()
        .all()
    )
    amounts = {"provider": 0, "gpu": 0, "artifact": 0}
    for item in released:
        amounts[item["kind"]] += item["reserved_amount"]
    await session.execute(
        text("""
        UPDATE pipeline_budget_reservations b
           SET state='released', settled_at=clock_timestamp()
          FROM execution_attempts a, pipeline_stage_runs s
         WHERE b.execution_attempt_id=a.id AND s.id=a.stage_run_id
           AND s.pipeline_run_id=:run_id AND b.state='active'
           AND a.state IN ('fault_pending','queued')
    """),
        {"run_id": run.id},
    )
    if released:
        ledger.provider_reserved_microusd -= amounts["provider"]
        ledger.gpu_reserved_seconds -= amounts["gpu"]
        ledger.artifact_reserved_bytes -= amounts["artifact"]
    await session.execute(
        text("""
        UPDATE execution_attempts a
           SET state='cancelled', cancellation_requested_at=clock_timestamp(),
               cancellation_observed_at=clock_timestamp(), cancellation_outcome='not_started',
               finished_at=clock_timestamp(), retry_class='cancelled', reason_code='user_cancel',
               cleanup_acknowledged_at=clock_timestamp(),
               cleanup_proof_json='{"not_started":true}'::jsonb,
               cleanup_proof_digest=:cleanup_digest, version=a.version+1
          FROM pipeline_stage_runs s
         WHERE s.id=a.stage_run_id AND s.pipeline_run_id=:run_id
           AND a.state IN ('fault_pending','queued')
    """),
        {"run_id": run.id, "cleanup_digest": canonical_digest({"not_started": True})},
    )
    attempts = (
        (
            await session.execute(
                text("""
            SELECT a.id FROM execution_attempts a
              JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
             WHERE s.pipeline_run_id=:run_id AND a.state IN ('claimed','running')
             FOR UPDATE OF a
        """),
                {"run_id": run.id},
            )
        )
        .scalars()
        .all()
    )
    for attempt_id in attempts:
        cancel_request = {
            "execution_attempt_id": str(attempt_id),
            "pipeline_run_id": str(run.id),
            "terminal_cause": TerminalCause.USER_CANCEL.value,
        }
        await session.execute(
            text("""
            INSERT INTO pipeline_cancellation_outbox (
                pipeline_run_id, execution_attempt_id, terminal_cause,
                idempotency_key, request_json, request_digest
            ) VALUES (:run_id, :attempt_id, 'user_cancel', :key,
                      CAST(:request AS jsonb), :digest)
            ON CONFLICT (execution_attempt_id) DO NOTHING
        """),
            {
                "run_id": run.id,
                "attempt_id": attempt_id,
                "key": f"pipeline-cancel:{attempt_id}",
                "request": json.dumps(cancel_request, separators=(",", ":"), sort_keys=True),
                "digest": canonical_digest(cancel_request),
            },
        )
        await session.execute(
            text("""
            UPDATE execution_attempts
               SET cancellation_requested_at=COALESCE(
                       cancellation_requested_at, clock_timestamp()), version=version+1
             WHERE id=:attempt_id AND state IN ('claimed','running')
        """),
            {"attempt_id": attempt_id},
        )
        deleted_preview = await session.execute(
            text("""
            DELETE FROM pipeline_live_preview_frames
             WHERE execution_attempt_id=:attempt_id
             RETURNING sequence
        """),
            {"attempt_id": attempt_id},
        )
        ended_preview = await session.execute(
            text("""
            UPDATE pipeline_live_preview_generations
               SET state='ended', latest_sequence=NULL, latest_step_idx=NULL,
                   received_at=NULL, frame_count=0, total_bytes=0,
                   expires_at=clock_timestamp(), purge_reason='cancelled',
                   purged_at=clock_timestamp(), updated_at=clock_timestamp()
             WHERE execution_attempt_id=:attempt_id AND purged_at IS NULL
         RETURNING execution_attempt_id
        """),
            {"attempt_id": attempt_id},
        )
        if ended_preview.first() is not None or deleted_preview.first() is not None:
            PIPELINE_LIVE_PREVIEW_PURGES_TOTAL.labels(reason="cancelled").inc()
        await session.execute(
            text("""
            INSERT INTO execution_attempt_control_commands (
                execution_attempt_id, seq, command
            ) VALUES (
                :attempt_id,
                COALESCE((SELECT max(seq)+1 FROM execution_attempt_control_commands
                          WHERE execution_attempt_id=:attempt_id), 1),
                'cancel_requested'
            ) ON CONFLICT DO NOTHING
        """),
            {"attempt_id": attempt_id},
        )
    await session.execute(
        text("""
        UPDATE pipeline_stage_runs SET state='cancelled', finished_at=clock_timestamp(),
               reason_code='user_cancel', version=version+1
         WHERE pipeline_run_id=:run_id AND state IN ('blocked','ready','queued','retry_wait')
    """),
        {"run_id": run.id},
    )
    body = run_projection(run)
    body["terminal_cause"] = ledger.terminal_cause
    body["terminal_cause_at"] = (
        ledger.terminal_cause_at.isoformat() if ledger.terminal_cause_at else None
    )
    return body, False


__all__ = [
    "PipelineApiError",
    "claim_idempotency",
    "complete_idempotency",
    "create_public_run",
    "create_retry_run",
    "decode_pipeline_cursor",
    "encode_pipeline_cursor",
    "request_user_cancellation",
    "run_projection",
    "split_recipe",
]
