#!/usr/bin/env python3
"""Local, one-shot HTTP fault provider for issue #1748 deadline evidence.

This tool deliberately stops at the Gateway transport boundary.  It does not
create Loom batches, configure provider connections, select a worker pool, or
claim that the production-equivalent Case A/B acceptance has passed.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import math
import os
import re
import secrets
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_JWT_RE = re.compile(
    r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}"
)
_FORBIDDEN_KEY_PARTS = ("authorization", "bearer", "api_key", "secret", "token", "header", "body")
_MISSING_ACCEPTANCE_LAYERS = [
    "control_plane_persistence",
    "worker_supervision_and_retry",
    "canonical_trajectory",
    "atif_artifact",
    "deployed_image_and_route_readback",
    "post_run_worker_pool_readback",
]
_EVIDENCE_KEYS = {
    "schema_version",
    "scope",
    "full_canary_passed",
    "case",
    "candidate_sha",
    "candidate_tree",
    "harness_sha256",
    "trial_id",
    "step_id",
    "gateway_route",
    "provider_observation",
    "gateway_outcomes",
    "transport_assertions",
    "sensitive_material_recorded",
    "missing_acceptance_layers",
}
_PROVIDER_KEYS = {
    "schema_version",
    "scope",
    "full_canary_passed",
    "case",
    "candidate_sha",
    "candidate_tree",
    "trial_id",
    "step_id",
    "nonce_sha256",
    "nonce_length",
    "deadline_budget_sec",
    "hold_sec",
    "requests",
    "rejected_request_count",
    "unarmed_request_count",
}
_PROVIDER_REQUEST_KEYS = {
    "request_ordinal",
    "request_id",
    "started_at",
    "finished_at",
    "outcome",
}
_GATEWAY_OUTCOME_KEYS = {
    "phase",
    "case_attempt_ordinal",
    "http_status",
    "detail_code",
    "detail_reason",
    "request_started_at",
    "response_received_at",
    "signed_deadline_wall_clock",
    "grant_expires_at",
    "provider_request_count_before",
    "provider_request_count_after",
}
_TRANSPORT_ASSERTION_KEYS = {
    "provider_request_count",
    "post_deadline_attempt_1_dispatch_count",
    "fresh_deadline_grant_completed",
}
_REQUEST_BODY_TIMEOUT_SEC = 2.0
_GRACEFUL_SHUTDOWN_TIMEOUT_SEC = 2


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


class FaultProviderConfig(BaseModel):
    """Immutable binding for one local Case A or Case B provider."""

    case: Literal["A", "B"]
    candidate_sha: str
    candidate_tree: str
    trial_id: UUID
    step_id: str = Field(min_length=1, max_length=256)
    nonce: str = Field(min_length=16, max_length=512)
    deadline_budget_sec: float = Field(gt=0, le=30_000)
    hold_sec: float = Field(gt=0, le=60)

    @field_validator("hold_sec")
    @classmethod
    def _validate_hold_exceeds_budget(cls, value: float, info: Any) -> float:
        budget = info.data.get("deadline_budget_sec")
        if isinstance(budget, int | float) and value <= budget:
            raise ValueError("hold_sec must exceed deadline_budget_sec")
        return value

    @field_validator("candidate_sha", "candidate_tree")
    @classmethod
    def _validate_git_object_id(cls, value: str) -> str:
        if not _SHA1_RE.fullmatch(value):
            raise ValueError("candidate bindings must be full lowercase SHA-1 object ids")
        return value


class FaultProviderState:
    """Thread-safe request ledger which never retains request content."""

    def __init__(self, config: FaultProviderConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._requests: list[dict[str, Any]] = []
        self._rejected_request_count = 0
        self._unarmed_request_count = 0

    @property
    def config(self) -> FaultProviderConfig:
        return self._config

    def rebind(self, config: FaultProviderConfig) -> None:
        """Replace a pre-request test binding; a used server cannot be rebound."""

        with self._lock:
            if self._requests:
                raise RuntimeError("cannot rebind a fault provider after its first request")
            self._config = config

    def claim(self) -> tuple[int, str] | None:
        maximum = 1 if self._config.case == "A" else 2
        with self._lock:
            if len(self._requests) >= maximum:
                self._rejected_request_count += 1
                return None
            ordinal = len(self._requests) + 1
            request_id = str(uuid4())
            self._requests.append(
                {
                    "request_ordinal": ordinal,
                    "request_id": request_id,
                    "started_at": _utc_now(),
                    "finished_at": None,
                    "outcome": "in_progress",
                }
            )
            return ordinal, request_id

    def finish(self, request_id: str, *, outcome: Literal["held", "completed"]) -> None:
        with self._lock:
            for item in self._requests:
                if item["request_id"] == request_id:
                    item["finished_at"] = _utc_now()
                    item["outcome"] = outcome
                    return
        raise RuntimeError("unknown fault-provider request id")

    def record_unarmed(self) -> None:
        with self._lock:
            self._unarmed_request_count += 1

    def record_for_test(self, *, outcome: Literal["held", "completed"]) -> None:
        claimed = self.claim()
        if claimed is None:
            raise RuntimeError("fault-provider request limit reached")
        _, request_id = claimed
        self.finish(request_id, outcome=outcome)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            requests = [dict(item) for item in self._requests]
            config = self._config
            rejected_request_count = self._rejected_request_count
            unarmed_request_count = self._unarmed_request_count
        return {
            "schema_version": "loom.issue-1748.fault-provider-observation.v1",
            "scope": "local_fault_provider_only",
            "full_canary_passed": False,
            "case": config.case,
            "candidate_sha": config.candidate_sha,
            "candidate_tree": config.candidate_tree,
            "trial_id": str(config.trial_id),
            "step_id": config.step_id,
            "nonce_sha256": hashlib.sha256(config.nonce.encode()).hexdigest(),
            "nonce_length": len(config.nonce),
            "deadline_budget_sec": config.deadline_budget_sec,
            "hold_sec": config.hold_sec,
            "requests": requests,
            "rejected_request_count": rejected_request_count,
            "unarmed_request_count": unarmed_request_count,
        }


async def _discard_request_content(
    request: Request,
    *,
    limit_bytes: int = 2 * 1024 * 1024,
    timeout_sec: float = _REQUEST_BODY_TIMEOUT_SEC,
) -> None:
    """Drain a provider request without retaining or interpreting its content."""

    observed = 0
    try:
        async with asyncio.timeout(timeout_sec):
            async for chunk in request.stream():
                observed += len(chunk)
                if observed > limit_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail={"code": "fault_provider_request_too_large"},
                    )
    except TimeoutError as exc:
        raise HTTPException(
            status_code=408,
            detail={"code": "fault_provider_request_body_timeout"},
        ) from exc


def create_fault_provider_app(
    state: FaultProviderState,
    *,
    request_body_timeout_sec: float = _REQUEST_BODY_TIMEOUT_SEC,
) -> FastAPI:
    """Create the local-only provider app used by the real-HTTP regression."""

    if not math.isfinite(request_body_timeout_sec) or request_body_timeout_sec <= 0:
        raise ValueError("request body timeout must be positive and finite")

    app = FastAPI(
        title="Loom issue 1748 local fault provider",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.post("/{nonce}/v1/responses")
    async def responses(nonce: str, request: Request) -> dict[str, Any]:
        if not secrets.compare_digest(nonce, state.config.nonce):
            state.record_unarmed()
            raise HTTPException(status_code=404, detail={"code": "fault_provider_not_armed"})
        # Drain under a wall-time bound before consuming this one-shot provider
        # capability. A slow/incomplete body must not occupy an attempt slot.
        await _discard_request_content(request, timeout_sec=request_body_timeout_sec)
        claimed = state.claim()
        if claimed is None:
            raise HTTPException(
                status_code=409,
                detail={"code": "fault_provider_request_limit_reached"},
            )
        ordinal, request_id = claimed
        if ordinal == 1:
            await asyncio.sleep(state.config.hold_sec)
            state.finish(request_id, outcome="held")
        else:
            state.finish(request_id, outcome="completed")

        return {
            "id": "issue-1748-canary",
            "object": "response",
            "model": "canary-model",
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "ok"}],
                }
            ],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
        }

    return app


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_secret_safe(value: Any, *, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key).lower()
            if any(part in key_text for part in _FORBIDDEN_KEY_PARTS):
                raise ValueError(f"evidence is not secret-safe: forbidden key at {path}.{key}")
            _assert_secret_safe(child, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_secret_safe(child, path=f"{path}[{index}]")
        return
    if isinstance(value, str) and (
        re.search(r"(?i)\bbearer\s+", value)
        or re.search(r"(?i)\bsk-[A-Za-z0-9]", value)
        or _JWT_RE.search(value)
    ):
        raise ValueError(f"evidence is not secret-safe: forbidden value at {path}")


def _assert_no_known_capability(
    value: Any,
    *,
    nonce_sha256: str,
    nonce_length: int,
    path: str = "$",
) -> None:
    """Reject the bound raw capability wherever it appears in a fixed schema."""

    if isinstance(value, dict):
        for key, child in value.items():
            _assert_no_known_capability(
                child,
                nonce_sha256=nonce_sha256,
                nonce_length=nonce_length,
                path=f"{path}.{key}",
            )
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_known_capability(
                child,
                nonce_sha256=nonce_sha256,
                nonce_length=nonce_length,
                path=f"{path}[{index}]",
            )
        return
    if isinstance(value, str):
        if len(value) > 1024:
            raise ValueError(f"evidence string exceeds its safety bound at {path}")
        for start in range(len(value) - nonce_length + 1):
            candidate = value[start : start + nonce_length]
            if secrets.compare_digest(
                hashlib.sha256(candidate.encode()).hexdigest(),
                nonce_sha256,
            ):
                raise ValueError(f"evidence contains the bound raw capability at {path}")


def _require_exact_keys(document: dict[str, Any], expected: set[str], *, name: str) -> None:
    if set(document) != expected:
        raise ValueError(f"{name} has missing or unexpected fields")


def _timestamp(value: object, *, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an RFC3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an RFC3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include an offset")
    return parsed.astimezone(UTC)


def _plain_int(value: object, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _positive_finite_number(value: object, *, name: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"{name} must be a number")
    converted = float(value)
    if not math.isfinite(converted) or converted <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return converted


def _derive_transport_assertions(
    *,
    case: object,
    provider_requests: list[Any],
    gateway_outcomes: list[Any],
) -> dict[str, Any]:
    by_phase = {
        item.get("phase"): item
        for item in gateway_outcomes
        if isinstance(item, dict) and isinstance(item.get("phase"), str)
    }
    replay = by_phase.get("expired_deadline_replay", {})
    before = replay.get("provider_request_count_before")
    after = replay.get("provider_request_count_after")
    replay_delta = (
        after - before
        if isinstance(before, int)
        and not isinstance(before, bool)
        and isinstance(after, int)
        and not isinstance(after, bool)
        else None
    )
    fresh = by_phase.get("fresh_deadline_grant", {})
    initial = by_phase.get("initial_deadline", {})
    fresh_before = fresh.get("provider_request_count_before")
    fresh_after = fresh.get("provider_request_count_after")
    fresh_completed = (
        case == "B"
        and fresh.get("http_status") == 200
        and isinstance(fresh_before, int)
        and not isinstance(fresh_before, bool)
        and isinstance(fresh_after, int)
        and not isinstance(fresh_after, bool)
        and fresh_after == fresh_before + 1
        and fresh.get("signed_deadline_wall_clock") != initial.get("signed_deadline_wall_clock")
    )
    return {
        "provider_request_count": len(provider_requests),
        "post_deadline_attempt_1_dispatch_count": replay_delta,
        "fresh_deadline_grant_completed": fresh_completed,
    }


def build_local_transport_evidence(
    *,
    config: FaultProviderConfig,
    provider_snapshot: dict[str, Any],
    gateway_outcomes: list[dict[str, object]],
    harness_path: Path,
    gateway_route: str,
) -> dict[str, Any]:
    """Build deliberately partial evidence for the local transport seam."""

    if gateway_route != "/v1/responses":
        raise ValueError("local evidence must use the fixed Gateway route")
    requests = provider_snapshot.get("requests")
    if not isinstance(requests, list):
        raise ValueError("provider snapshot requests must be a list")
    if config.nonce in json.dumps(provider_snapshot, sort_keys=True):
        raise ValueError("provider observation contains the raw capability")
    assertions = _derive_transport_assertions(
        case=config.case,
        provider_requests=requests,
        gateway_outcomes=gateway_outcomes,
    )
    evidence = {
        "schema_version": "loom.issue-1748.local-transport-evidence.v1",
        "scope": "local_real_http_transport_only",
        "full_canary_passed": False,
        "case": config.case,
        "candidate_sha": config.candidate_sha,
        "candidate_tree": config.candidate_tree,
        "harness_sha256": _file_sha256(harness_path),
        "trial_id": str(config.trial_id),
        "step_id": config.step_id,
        "gateway_route": gateway_route,
        "provider_observation": provider_snapshot,
        "gateway_outcomes": gateway_outcomes,
        "transport_assertions": assertions,
        "sensitive_material_recorded": False,
        "missing_acceptance_layers": list(_MISSING_ACCEPTANCE_LAYERS),
    }
    if config.nonce in json.dumps(evidence, sort_keys=True):
        raise ValueError("local evidence contains the raw capability")
    validate_local_transport_evidence(evidence)
    return evidence


def validate_local_transport_evidence(evidence: dict[str, Any]) -> None:
    """Fail closed unless evidence matches the bounded local Case A/B contract."""

    _require_exact_keys(evidence, _EVIDENCE_KEYS, name="local transport evidence")
    if evidence.get("schema_version") != "loom.issue-1748.local-transport-evidence.v1":
        raise ValueError("unexpected local transport evidence schema")
    if evidence.get("scope") != "local_real_http_transport_only":
        raise ValueError("local evidence has an invalid scope")
    if evidence.get("full_canary_passed") is not False:
        raise ValueError("local evidence cannot claim full canary acceptance")
    if evidence.get("gateway_route") != "/v1/responses":
        raise ValueError("local evidence must use the fixed Gateway route")
    if evidence.get("sensitive_material_recorded") is not False:
        raise ValueError("local evidence cannot record sensitive material")
    if evidence.get("missing_acceptance_layers") != _MISSING_ACCEPTANCE_LAYERS:
        raise ValueError("local evidence must enumerate every untested acceptance layer")

    provider = evidence.get("provider_observation")
    if not isinstance(provider, dict):
        raise ValueError("provider observation is required")
    _require_exact_keys(provider, _PROVIDER_KEYS, name="provider observation")
    if provider.get("schema_version") != "loom.issue-1748.fault-provider-observation.v1":
        raise ValueError("unexpected provider observation schema")
    if provider.get("scope") != "local_fault_provider_only":
        raise ValueError("unexpected provider observation scope")
    if provider.get("full_canary_passed") is not False:
        raise ValueError("provider observation cannot claim full canary acceptance")
    nonce_sha256 = provider.get("nonce_sha256")
    if not isinstance(nonce_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", nonce_sha256):
        raise ValueError("provider capability digest is missing or malformed")
    nonce_length = _plain_int(provider.get("nonce_length"), name="provider capability length")
    if not 16 <= nonce_length <= 512:
        raise ValueError("provider capability length is outside the bounded contract")
    _assert_secret_safe(evidence)
    _assert_no_known_capability(
        evidence,
        nonce_sha256=nonce_sha256,
        nonce_length=nonce_length,
    )
    binding_fields = ("case", "candidate_sha", "candidate_tree", "trial_id", "step_id")
    if any(evidence.get(field) != provider.get(field) for field in binding_fields):
        raise ValueError("candidate binding or request correlation does not match")
    if not _SHA1_RE.fullmatch(str(evidence.get("candidate_sha", ""))) or not _SHA1_RE.fullmatch(
        str(evidence.get("candidate_tree", ""))
    ):
        raise ValueError("candidate binding or request correlation does not match")
    if not re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("harness_sha256", ""))):
        raise ValueError("harness digest is missing or malformed")

    case = evidence.get("case")
    if case not in {"A", "B"}:
        raise ValueError("local evidence case must be A or B")
    try:
        UUID(str(evidence.get("trial_id")))
    except ValueError as exc:
        raise ValueError("request correlation trial_id is malformed") from exc
    if not isinstance(evidence.get("step_id"), str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}",
        evidence["step_id"],
    ):
        raise ValueError("request correlation step_id is malformed")
    deadline_budget = _positive_finite_number(
        provider.get("deadline_budget_sec"), name="provider deadline budget"
    )
    hold_sec = _positive_finite_number(provider.get("hold_sec"), name="provider hold duration")
    if deadline_budget > 30_000 or hold_sec > 60 or hold_sec <= deadline_budget:
        raise ValueError("provider fault timing is outside the bounded contract")
    rejected_count = _plain_int(
        provider.get("rejected_request_count"), name="provider rejected request count"
    )
    unarmed_count = _plain_int(
        provider.get("unarmed_request_count"), name="provider unarmed request count"
    )
    requests = provider.get("requests")
    outcomes = evidence.get("gateway_outcomes")
    if not isinstance(requests, list) or not isinstance(outcomes, list):
        raise ValueError("provider requests and gateway outcomes are required")
    expected_request_count = 1 if case == "A" else 2
    if len(requests) != expected_request_count:
        raise ValueError("fault provider request count does not match the selected case")
    for expected_ordinal, item in enumerate(requests, start=1):
        if not isinstance(item, dict):
            raise ValueError("provider request observation must be an object")
        _require_exact_keys(item, _PROVIDER_REQUEST_KEYS, name="provider request observation")
        if item.get("request_ordinal") != expected_ordinal:
            raise ValueError("provider request ordinals are malformed")
        try:
            UUID(str(item.get("request_id")))
        except ValueError as exc:
            raise ValueError("provider request id is malformed") from exc
        provider_started = _timestamp(item.get("started_at"), name="provider request started_at")
        provider_finished = _timestamp(item.get("finished_at"), name="provider request finished_at")
        if provider_finished < provider_started:
            raise ValueError("provider request timestamps are inconsistent")
    for item in outcomes:
        if not isinstance(item, dict):
            raise ValueError("Gateway outcome must be an object")
        _require_exact_keys(item, _GATEWAY_OUTCOME_KEYS, name="Gateway outcome")
    expected_provider_outcomes = ["held"] if case == "A" else ["held", "completed"]
    if [
        item.get("outcome") for item in requests if isinstance(item, dict)
    ] != expected_provider_outcomes:
        raise ValueError("fault provider observations do not match the selected case")
    expected_phases = (
        ["initial_deadline", "expired_deadline_replay"]
        if case == "A"
        else ["initial_deadline", "expired_deadline_replay", "fresh_deadline_grant"]
    )
    if [item.get("phase") for item in outcomes] != expected_phases:
        raise ValueError("Gateway outcome phases do not match the selected case")
    expected_http = [504, 504] if case == "A" else [504, 504, 200]
    if [item.get("http_status") for item in outcomes if isinstance(item, dict)] != expected_http:
        raise ValueError("gateway outcomes do not match the selected case")
    for item in outcomes[:2]:
        if item.get("case_attempt_ordinal") != 1:
            raise ValueError("deadline and replay must remain bound to attempt 1")
        if item.get("detail_code") != "agent_timeout" or item.get("detail_reason") != (
            "attempt_deadline_reached"
        ):
            raise ValueError("attempt 1 did not expose the stable deadline outcome")
    first, replay = outcomes[0], outcomes[1]
    if (
        _plain_int(first.get("provider_request_count_before"), name="initial provider count") != 0
        or _plain_int(first.get("provider_request_count_after"), name="initial provider count") != 1
    ):
        raise ValueError("initial request count is inconsistent")
    first_started = _timestamp(first.get("request_started_at"), name="initial request start")
    first_received = _timestamp(first.get("response_received_at"), name="initial response")
    first_deadline = _timestamp(
        first.get("signed_deadline_wall_clock"), name="initial signed deadline"
    )
    first_expiry = _timestamp(first.get("grant_expires_at"), name="initial grant expiry")
    replay_started = _timestamp(replay.get("request_started_at"), name="replay request start")
    replay_received = _timestamp(replay.get("response_received_at"), name="replay response")
    if not (first_started < first_deadline <= first_received <= replay_started <= replay_received):
        raise ValueError("deadline and replay timestamps are inconsistent")
    if replay.get("signed_deadline_wall_clock") != first.get("signed_deadline_wall_clock"):
        raise ValueError("expired replay is not bound to the original deadline")
    if replay.get("grant_expires_at") != first.get("grant_expires_at"):
        raise ValueError("expired replay is not bound to the original grant")
    if first_expiry < first_deadline + timedelta(seconds=300):
        raise ValueError("initial grant does not cover deadline cleanup")
    if (
        replay.get("provider_request_count_before"),
        replay.get("provider_request_count_after"),
    ) != (1, 1):
        raise ValueError("expired replay changed the provider request count")
    if case == "B":
        fresh = outcomes[2]
        if fresh.get("case_attempt_ordinal") != 2 or fresh.get("detail_code") is not None:
            raise ValueError("fresh deadline grant outcome is malformed")
        if fresh.get("detail_reason") is not None:
            raise ValueError("fresh deadline grant outcome is malformed")
        fresh_started = _timestamp(fresh.get("request_started_at"), name="fresh request start")
        fresh_received = _timestamp(fresh.get("response_received_at"), name="fresh response")
        fresh_deadline = _timestamp(
            fresh.get("signed_deadline_wall_clock"), name="fresh signed deadline"
        )
        fresh_expiry = _timestamp(fresh.get("grant_expires_at"), name="fresh grant expiry")
        if not (replay_received <= fresh_started <= fresh_received < fresh_deadline):
            raise ValueError("fresh deadline grant timestamps are inconsistent")
        if fresh.get("signed_deadline_wall_clock") == first.get("signed_deadline_wall_clock"):
            raise ValueError("fresh request reused the expired deadline")
        if fresh_expiry < fresh_deadline + timedelta(seconds=300):
            raise ValueError("fresh grant does not cover deadline cleanup")
        if (
            fresh.get("provider_request_count_before"),
            fresh.get("provider_request_count_after"),
        ) != (1, 2):
            raise ValueError("fresh request count is inconsistent")
    assertions = evidence.get("transport_assertions")
    if not isinstance(assertions, dict):
        raise ValueError("transport assertions are required")
    _require_exact_keys(assertions, _TRANSPORT_ASSERTION_KEYS, name="transport assertions")
    derived_assertions = _derive_transport_assertions(
        case=case,
        provider_requests=requests,
        gateway_outcomes=outcomes,
    )
    if assertions != derived_assertions:
        raise ValueError("transport assertions do not match observed evidence")
    if assertions.get("provider_request_count") != expected_request_count:
        raise ValueError("transport provider count does not match the selected case")
    if rejected_count != 0 or unarmed_count != 0:
        raise ValueError("fault provider observed an unexpected request")
    if assertions.get("post_deadline_attempt_1_dispatch_count") != 0:
        raise ValueError("attempt 1 dispatched after its deadline fence")
    if assertions.get("fresh_deadline_grant_completed") is not (case == "B"):
        raise ValueError("fresh-deadline result does not match the selected case")


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _loopback_host(value: str) -> str:
    if value == "localhost":
        return value
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("bind host must be an explicit loopback address") from exc
    if not address.is_loopback:
        raise argparse.ArgumentTypeError("this local harness refuses non-loopback binds")
    return value


def assert_local_candidate_binding(
    *,
    repo_root: Path,
    candidate_sha: str,
    candidate_tree: str,
) -> None:
    """Bind CLI evidence to an exact, clean local checkout."""

    def git(*args: str) -> str:
        completed = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    observed_sha = git("rev-parse", "HEAD")
    observed_tree = git("rev-parse", "HEAD^{tree}")
    if (candidate_sha, candidate_tree) != (observed_sha, observed_tree):
        raise ValueError("candidate binding does not match the local checkout")
    if git("status", "--porcelain", "--untracked-files=all"):
        raise ValueError("candidate-bound local evidence requires a clean checkout")


def _serve(args: argparse.Namespace) -> int:
    nonce = os.environ.get(args.nonce_env)
    if nonce is None:
        raise ValueError("the capability environment variable is missing")
    repo_root = Path(__file__).resolve().parents[2]
    assert_local_candidate_binding(
        repo_root=repo_root,
        candidate_sha=args.candidate_sha,
        candidate_tree=args.candidate_tree,
    )
    config = FaultProviderConfig(
        case=args.case,
        candidate_sha=args.candidate_sha,
        candidate_tree=args.candidate_tree,
        trial_id=UUID(args.trial_id),
        step_id=args.step_id,
        nonce=nonce,
        deadline_budget_sec=args.deadline_budget_sec,
        hold_sec=args.hold_sec,
    )
    state = FaultProviderState(config)
    app = create_fault_provider_app(state)
    try:
        uvicorn.run(
            app,
            host=args.bind_host,
            port=args.port,
            access_log=False,
            log_level="warning",
            timeout_graceful_shutdown=_GRACEFUL_SHUTDOWN_TIMEOUT_SEC,
        )
    finally:
        snapshot = state.snapshot()
        _assert_secret_safe(snapshot)
        _assert_no_known_capability(
            snapshot,
            nonce_sha256=hashlib.sha256(config.nonce.encode()).hexdigest(),
            nonce_length=len(config.nonce),
        )
        _write_json(args.output, snapshot)
    return 0


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError("local evidence JSON contains duplicate keys")
        document[key] = value
    return document


def _reject_nonfinite_json_constant(value: str) -> None:
    raise ValueError(f"local evidence JSON contains unsupported constant {value}")


def _validate(args: argparse.Namespace) -> int:
    raw_document = args.input.read_text(encoding="utf-8")
    if len(raw_document.encode()) > 1024 * 1024:
        raise ValueError("local transport evidence exceeds the bounded input size")
    document = json.loads(
        raw_document,
        object_pairs_hook=_reject_duplicate_json_keys,
        parse_constant=_reject_nonfinite_json_constant,
    )
    if not isinstance(document, dict):
        raise ValueError("local transport evidence must be a JSON object")
    validate_local_transport_evidence(document)
    print("issue-1748 local transport evidence valid (not full canary acceptance)")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="serve the local-only one-shot provider")
    serve.add_argument("--case", choices=("A", "B"), required=True)
    serve.add_argument("--candidate-sha", required=True)
    serve.add_argument("--candidate-tree", required=True)
    serve.add_argument("--trial-id", required=True)
    serve.add_argument("--step-id", required=True)
    serve.add_argument("--deadline-budget-sec", type=float, default=10.0)
    serve.add_argument("--hold-sec", type=float, default=15.0)
    serve.add_argument("--bind-host", type=_loopback_host, default="127.0.0.1")
    serve.add_argument("--port", type=int, default=9011)
    serve.add_argument("--nonce-env", default="LOOM_1748_CANARY_NONCE")
    serve.add_argument("--output", type=Path, required=True)
    serve.set_defaults(handler=_serve)
    validate = subparsers.add_parser(
        "validate",
        help="validate a combined local transport evidence document",
    )
    validate.add_argument("--input", type=Path, required=True)
    validate.set_defaults(handler=_validate)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        return int(args.handler(args))
    except KeyboardInterrupt:
        raise
    except SystemExit as exc:
        if exc.code == 0:
            return 0
        print("issue-1748 local canary failed safely; inspect configuration", file=sys.stderr)
        return 2
    except Exception:
        print("issue-1748 local canary failed safely; inspect configuration", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
