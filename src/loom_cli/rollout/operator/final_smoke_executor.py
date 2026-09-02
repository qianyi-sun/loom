"""Exact installed live admin-on-behalf smoke using the shared predicates."""

from __future__ import annotations

import hashlib
import json
import time
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.admin_smoke_contract import (
    AdminSmokeAuthority,
    AdminSmokeContract,
    decode_json_object,
)
from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.preflight_contract import CheckOperation

from .final_gate_plan import FinalGatePlan

_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_TOKEN_BYTES = 64 * 1024


class SmokeTransport(Protocol):
    @property
    def base_url(self) -> str: ...

    def __call__(
        self,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, object] | None,
        headers: Mapping[str, str] | None,
    ) -> tuple[int, bytes]: ...


class FinalSmokeError(RuntimeError):
    """One normalized, non-secret live-smoke failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


CapacityRefresh = Callable[[FinalGatePlan], str]


@dataclass(frozen=True, slots=True)
class FinalSmokeExecutor:
    """Submit or recover one exact bounded batch and require terminal success."""

    service_uid: int
    token_path: Path
    expected_token_fingerprint: str
    authority: AdminSmokeAuthority
    request: SmokeTransport
    refresh_capacity: CapacityRefresh
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    terminal_timeout_seconds: float = 900.0
    poll_interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        if (
            self.service_uid < 0
            or not self.token_path.is_absolute()
            or ".." in self.token_path.parts
            or any(character in str(self.token_path) for character in (",", "\n", "\r", "\x00"))
            or not self.expected_token_fingerprint.startswith("sha256:")
            or not callable(self.request)
            or not callable(self.refresh_capacity)
            or not callable(self.monotonic)
            or not callable(self.sleep)
            or not 0 < self.terminal_timeout_seconds <= 3600
            or not 0 < self.poll_interval_seconds <= 60
        ):
            raise ValueError("final smoke executor authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        if check_id != "final.smoke" or operation is not CheckOperation.APPLY:
            raise ValueError("final smoke executor operation is invalid")
        if (
            plan.environment != "staging"
            or plan.namespace != "loom-staging"
            or self.request.base_url != plan.route
        ):
            return self._result(plan, blocker="smoke-plan-drift", mutated=False)
        token = self._token(plan)
        if token is None:
            return self._result(plan, blocker="smoke-token-binding-drift", mutated=False)

        evidence: dict[str, str] = {}
        contract = AdminSmokeContract(self.authority)
        mutated = False
        try:
            self._expect(
                evidence,
                "health",
                token,
                "GET",
                "/api/v1/health",
                accepted=frozenset({200}),
            )
            whoami = self._expect_json(
                evidence,
                "whoami",
                token,
                "GET",
                "/api/v1/auth/whoami",
            )
            if contract.validate_admin_identity(whoami) is not None:
                raise FinalSmokeError("smoke-admin-identity-invalid")
            catalog = self._expect_json(
                evidence,
                "catalog",
                token,
                "GET",
                "/api/v1/benchmarks",
            )
            if contract.validate_benchmark_catalog(catalog) is not None:
                raise FinalSmokeError("smoke-catalog-invalid")
            task_path = "/api/v1/tasks/" + urllib.parse.quote(self.authority.task_id, safe="/")
            self._expect(
                evidence,
                "task",
                token,
                "GET",
                task_path,
                accepted=frozenset({200}),
            )

            batch_id, batch_name = self._existing_batch(
                evidence,
                token,
                contract,
                plan,
            )
            if batch_id is not None:
                # The exact batch is protected work already owned by this
                # request's single claimed epoch; recovery does not claim a
                # second epoch or submit replacement demand.
                mutated = True
            if batch_id is None:
                # Admission evidence expires after five minutes while the
                # protected rollout guard intentionally suspends the combined
                # capacity/GC CronJob.  Refresh only the capacity publication
                # immediately before the one request that consumes it.
                mutated = True
                try:
                    capacity_digest = self.refresh_capacity(plan)
                except (OSError, RuntimeError, ValueError) as exc:
                    raise FinalSmokeError("smoke-capacity-refresh-failed") from exc
                if (
                    not isinstance(capacity_digest, str)
                    or len(capacity_digest) != 64
                    or any(character not in "0123456789abcdef" for character in capacity_digest)
                ):
                    raise FinalSmokeError("smoke-capacity-refresh-failed")
                evidence["capacity"] = capacity_digest
                submitted = self._expect_json(
                    evidence,
                    "submit",
                    token,
                    "POST",
                    "/api/v1/admin/batches/on-behalf",
                    payload=contract.submission_payload(batch_name=batch_name),
                    headers={"X-Loom-Admin-Actor": self.authority.admin_actor},
                    accepted=frozenset({200, 201}),
                )
                value = submitted.get("id") or submitted.get("batch_id")
                if not isinstance(value, str) or not value:
                    raise FinalSmokeError("smoke-submit-identity-invalid")
                batch_id = value

            deadline = self.monotonic() + self.terminal_timeout_seconds
            terminal: Mapping[str, object] | None = None
            while self.monotonic() < deadline:
                batch = self._expect_json(
                    evidence,
                    "poll",
                    token,
                    "GET",
                    f"/api/v1/batches/{urllib.parse.quote(batch_id, safe='')}",
                )
                nonrecoverable = contract.nonrecoverable_failure(batch)
                if nonrecoverable is not None:
                    raise FinalSmokeError("smoke-batch-nonrecoverable")
                state = batch.get("state")
                if state not in {"failed", "cancelled"} and (
                    contract.validate_admitted_batch(
                        batch,
                        batch_id=batch_id,
                        batch_name=batch_name,
                    )
                    is not None
                ):
                    raise FinalSmokeError("smoke-batch-identity-invalid")
                if state in {"finished", "failed", "cancelled"}:
                    terminal = batch
                    break
                self.sleep(self.poll_interval_seconds)
            if terminal is None:
                raise FinalSmokeError("smoke-terminal-timeout")
            if contract.validate_terminal_batch(terminal) is not None:
                raise FinalSmokeError("smoke-terminal-invalid")
        except (FinalSmokeError, OSError, RuntimeError, ValueError) as exc:
            code = exc.code if isinstance(exc, FinalSmokeError) else "smoke-transport-failed"
            return self._result(plan, blocker=code, evidence=evidence, mutated=mutated)
        return self._result(
            plan,
            evidence=evidence,
            batch_id=batch_id,
            mutated=mutated,
        )

    def _token(self, plan: FinalGatePlan) -> str | None:
        try:
            trusted = read_trusted_file(
                self.token_path,
                service_uid=self.service_uid,
                private=True,
                allow_qianyi_owner=True,
                max_bytes=_MAX_TOKEN_BYTES,
                require_nonempty=True,
            )
            payload = trusted.payload.strip()
            token = payload.decode("ascii")
        except (OSError, UnicodeDecodeError, ValueError):
            return None
        if (
            safe_content_fingerprint(payload) != self.expected_token_fingerprint
            or plan.secret_metadata_fingerprints.get("admin")
            != f"sha256:{trusted.metadata_fingerprint}"
        ):
            return None
        return token

    def _expect_json(
        self,
        evidence: dict[str, str],
        label: str,
        token: str,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        accepted: frozenset[int] = frozenset({200}),
    ) -> Mapping[str, object]:
        body = self._expect(
            evidence,
            label,
            token,
            method,
            path,
            payload=payload,
            headers=headers,
            accepted=accepted,
        )
        value = decode_json_object(body)
        if value is None:
            raise FinalSmokeError(f"smoke-{label}-response-invalid")
        return value

    def _expect(
        self,
        evidence: dict[str, str],
        label: str,
        token: str,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        accepted: frozenset[int],
    ) -> bytes:
        if method not in {"GET", "POST"} or not path.startswith("/api/v1/"):
            raise FinalSmokeError("smoke-request-authority-invalid")
        status, body = self.request(method, path, token, payload, headers)
        if len(body) > _MAX_RESPONSE_BYTES:
            raise FinalSmokeError("smoke-response-too-large")
        evidence[label] = hashlib.sha256(str(status).encode() + b"\0" + body).hexdigest()
        if status not in accepted:
            raise FinalSmokeError(f"smoke-{label}-http-failed")
        return body

    @staticmethod
    def _batch_name(plan: FinalGatePlan) -> str:
        return f"rollout-{plan.request_id.removeprefix('req-')}-{plan.attempt_number}"

    def _existing_batch(
        self,
        evidence: dict[str, str],
        token: str,
        contract: AdminSmokeContract,
        plan: FinalGatePlan,
    ) -> tuple[str | None, str]:
        current_name = self._batch_name(plan)
        # A validated resume changes only the attempt suffix.  Reconcile the
        # newest exact batch before admitting any new Slurm-backed demand.
        for attempt_number in range(plan.attempt_number, 0, -1):
            batch_name = (
                current_name
                if attempt_number == plan.attempt_number
                else f"rollout-{plan.request_id.removeprefix('req-')}-{attempt_number}"
            )
            query = urllib.parse.urlencode(
                {
                    "team_id": self.authority.team_id,
                    "q": batch_name,
                    "limit": "20",
                }
            )
            existing = self._expect_json(
                evidence,
                "existing" if attempt_number == plan.attempt_number else f"prior-{attempt_number}",
                token,
                "GET",
                f"/api/v1/batches?{query}",
            )
            batch_id = contract.existing_batch_id(existing, batch_name=batch_name)
            if batch_id is not None:
                return batch_id, batch_name
        return None, current_name

    @staticmethod
    def _result(
        plan: FinalGatePlan,
        *,
        blocker: str | None = None,
        evidence: Mapping[str, str] | None = None,
        batch_id: str | None = None,
        mutated: bool,
    ) -> FinalGateResult:
        payload = {
            "batch_id": batch_id,
            "blocker": blocker,
            "evidence": dict(sorted((evidence or {}).items())),
        }
        return FinalGateResult(
            check_id="final.smoke",
            operation=CheckOperation.APPLY,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            observed_epoch=plan.starting_mutation_epoch + (1 if mutated else 0),
            evidence_digest=hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            protected_mutation=mutated,
            blockers=({"smoke": blocker} if blocker is not None else {}),
        )


__all__ = ["CapacityRefresh", "FinalSmokeExecutor", "SmokeTransport"]
