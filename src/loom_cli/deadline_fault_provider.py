"""Isolated, bounded OpenAI fault fixture; never grants live acceptance itself.

The provider sees only an opaque receipt ID. A separately authenticated operator
joins it to the durable Gateway audit before approving any execution request.
No database credential, step token, prompt or completion is kept in this ledger.
Run exactly one process; an interrupted fixture must be replaced, never resumed.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from loom.deadline_canary import CanaryBinding, ReceiptApproval

MODEL = "loom-deadline-canary"
_COMPLETION = json.dumps(
    {
        "analysis": "Canary complete.",
        "plan": "No further commands.",
        "commands": [],
        "task_complete": True,
    }
)


class _Arm(BaseModel):
    model_config = ConfigDict(extra="forbid")
    trial_id: UUID
    step_id: str = Field(min_length=1, max_length=256)


class FaultLedger:
    """Single-event-loop ledger. All mutations finish before yielding control."""

    def __init__(self, binding: CanaryBinding) -> None:
        self.binding = binding
        self.trial_id: UUID | None = None
        self.step_id: str | None = None
        self.requests: dict[UUID, dict[str, Any]] = {}
        self.approvals: list[ReceiptApproval] = []
        self.rejected = 0
        self.discovery = 0
        self.closed = False

    def arm(self, *, trial_id: UUID, step_id: str) -> None:
        if self.closed or self.trial_id is not None:
            raise ValueError("fixture cannot be rebound")
        self.trial_id, self.step_id = trial_id, step_id

    def reject(self, message: str) -> NoReturn:
        self.rejected += 1
        raise ValueError(message)

    def queue(self, receipt_id: UUID) -> None:
        if self.closed or receipt_id in self.requests or len(self.requests) >= 4:
            self.reject("request rejected")
        self.requests[receipt_id] = {
            "receipt_id": str(receipt_id),
            "received_at": datetime.now(UTC).isoformat(),
            "action": None,
            "outcome": "waiting_for_join",
        }

    def approve(self, receipt: ReceiptApproval, *, now: datetime | None = None) -> str:
        now = now or datetime.now(UTC)
        item = self.requests.get(receipt.receipt_id)
        if self.closed or item is None or item["outcome"] != "waiting_for_join":
            self.reject("request no longer awaiting approval")
        if (
            receipt.team_id != self.binding.team_id
            or receipt.provider_connection_id != self.binding.provider_connection_id
            or receipt.trial_id != self.trial_id
            or receipt.step_id != self.step_id
        ):
            self.reject("receipt binding mismatch")
        remaining = (receipt.deadline - now).total_seconds()
        if not 0 < remaining <= 10:
            self.reject("receipt deadline rejected")
        count = len(self.approvals)
        if count == 0:
            action = "hold"
        else:
            first = self.approvals[0]
            if (
                self.binding.case != "B"
                or count >= 3
                or receipt.agent_attempt_id == first.agent_attempt_id
                or receipt.step_jwt_id == first.step_jwt_id
                or now < first.deadline
                or not receipt.previous_attempt_stopped
            ):
                self.reject("retry attempt rejected")
            if count == 2:
                second = self.approvals[1]
                if (
                    receipt.agent_attempt_id != second.agent_attempt_id
                    or receipt.deadline != second.deadline
                ):
                    self.reject("retry attempt changed")
                prior = self.requests[second.receipt_id]
                if prior["outcome"] != "completed":
                    self.reject("previous completion not finished")
            action = "complete"
        self.approvals.append(receipt)
        item.update(action=action, outcome="approved", approval=receipt.model_dump(mode="json"))
        return action

    def finish(self, receipt_id: UUID, outcome: str) -> None:
        self.requests[receipt_id]["outcome"] = outcome
        self.requests[receipt_id]["finished_at"] = datetime.now(UTC).isoformat()

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "scope": "isolated_fault_provider",
            "full_canary_passed": False,
            "binding": self.binding.model_dump(mode="json"),
            "trial_id": str(self.trial_id) if self.trial_id else None,
            "step_id": self.step_id,
            "closed": self.closed,
            "rejected_request_count": self.rejected,
            "discovery_request_count": self.discovery,
            # Return a detached value; callers cannot mutate ledger authority.
            "requests": json.loads(json.dumps(list(self.requests.values()))),
        }


def create_fault_app(
    binding: CanaryBinding,
    *,
    provider_key: SecretStr,
    operator_key: SecretStr,
    lifetime_seconds: float = 180,
) -> FastAPI:
    if (
        not 30 <= lifetime_seconds <= 300
        or min(len(provider_key.get_secret_value()), len(operator_key.get_secret_value())) < 32
        or provider_key == operator_key
    ):
        raise ValueError("invalid isolated fixture credentials or lifetime")
    ledger = FaultLedger(binding)
    expires = time.monotonic() + lifetime_seconds
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.state.ledger = ledger

    def authenticate(request: Request, secret: SecretStr) -> None:
        supplied = request.headers.get("authorization", "")
        if not hmac.compare_digest(
            supplied.encode(), ("Bearer " + secret.get_secret_value()).encode()
        ):
            ledger.rejected += 1
            raise HTTPException(401, "fixture authentication failed")
        if time.monotonic() >= expires or ledger.closed:
            raise HTTPException(410, "fixture closed")

    async def payload(request: Request) -> Any:
        data = bytearray()
        try:
            async with asyncio.timeout(1):
                async for chunk in request.stream():
                    data.extend(chunk)
                    if len(data) > 65536:
                        raise ValueError
            return json.loads(data)
        except (ValueError, TimeoutError):
            ledger.rejected += 1
            raise HTTPException(400, "invalid fixture request") from None

    @app.get("/healthz")
    async def health() -> dict[str, bool]:
        if ledger.closed or time.monotonic() >= expires:
            raise HTTPException(503, "fixture closed")
        return {"ready": True}

    @app.get("/v1/models")
    async def models(request: Request) -> dict[str, Any]:
        authenticate(request, provider_key)
        ledger.discovery += 1
        return {"object": "list", "data": [{"id": MODEL, "object": "model"}]}

    @app.get("/operator/evidence")
    async def evidence(request: Request) -> dict[str, Any]:
        # Evidence remains available after close/expiry with valid capability.
        if not hmac.compare_digest(
            request.headers.get("authorization", "").encode(),
            ("Bearer " + operator_key.get_secret_value()).encode(),
        ):
            raise HTTPException(401, "fixture authentication failed")
        return ledger.snapshot()

    @app.post("/operator/arm")
    async def arm(request: Request) -> dict[str, bool]:
        authenticate(request, operator_key)
        try:
            config = _Arm.model_validate(await payload(request))
            ledger.arm(trial_id=config.trial_id, step_id=config.step_id)
        except ValueError:
            raise HTTPException(409, "fixture arm rejected") from None
        return {"armed": True}

    @app.post("/operator/approve")
    async def approve(request: Request) -> dict[str, str]:
        authenticate(request, operator_key)
        try:
            receipt = ReceiptApproval.model_validate(await payload(request))
            action = ledger.approve(receipt)
        except ValueError:
            raise HTTPException(409, "fixture approval rejected") from None
        return {"action": action}

    @app.post("/operator/close")
    async def close(request: Request) -> dict[str, bool]:
        authenticate(request, operator_key)
        ledger.closed = True
        return {"closed": True}

    def completion(receipt_id: UUID | None) -> dict[str, Any]:
        return {
            "id": "chatcmpl-" + (str(receipt_id) if receipt_id else "discovery"),
            "object": "chat.completion",
            "created": int(time.time()),
            "model": MODEL,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": _COMPLETION},
                    "finish_reason": "stop",
                }
            ],
            # Synthetic fixture accounting, explicitly not real provider billing.
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    @app.post("/v1/chat/completions")
    async def chat(request: Request) -> dict[str, Any]:
        authenticate(request, provider_key)
        body = await payload(request)
        if (
            not isinstance(body, dict)
            or body.get("model") != MODEL
            or not isinstance(body.get("messages"), list)
            or body.get("stream")
        ):
            ledger.rejected += 1
            raise HTTPException(400, "unsupported fixture protocol")
        raw_id = request.headers.get("x-request-id")
        if raw_id is None and ledger.trial_id is None:
            ledger.discovery += 1
            return completion(None)
        try:
            receipt_id = UUID(raw_id or "")
        except ValueError:
            ledger.rejected += 1
            raise HTTPException(409, "fixture request rejected") from None
        try:
            ledger.queue(receipt_id)
        except ValueError:
            raise HTTPException(409, "fixture request rejected") from None
        try:
            # Operator may still be reading the single-trial submission response.
            async with asyncio.timeout(3):
                while ledger.requests[receipt_id]["action"] is None:
                    if ledger.closed or time.monotonic() >= expires:
                        raise TimeoutError
                    await asyncio.sleep(0.01)
            record = ledger.requests[receipt_id]
            approval = ReceiptApproval.model_validate(record["approval"])
            if record["action"] == "hold":
                hold = max(0, (approval.deadline - datetime.now(UTC)).total_seconds()) + 2
                async with asyncio.timeout(13):
                    until = time.monotonic() + hold
                    while time.monotonic() < until:
                        if ledger.closed:
                            raise TimeoutError
                        await asyncio.sleep(0.01)
                ledger.finish(receipt_id, "held")
                raise HTTPException(503, "intentional fixture hold")
            if datetime.now(UTC) >= approval.deadline or ledger.closed:
                raise TimeoutError
            ledger.finish(receipt_id, "completed")
            return completion(receipt_id)
        except TimeoutError:
            ledger.finish(receipt_id, "closed" if ledger.closed else "expired_or_unapproved")
            if not ledger.closed:
                ledger.rejected += 1
            raise HTTPException(409, "fixture request expired") from None
        except asyncio.CancelledError:
            ledger.finish(receipt_id, "cancelled")
            raise

    return app


async def _serve_bounded(app: FastAPI, *, host: str, port: int, lifetime: float) -> None:
    import uvicorn

    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            access_log=False,
            log_level="critical",
            timeout_graceful_shutdown=1,
        )
    )
    task = asyncio.create_task(server.serve())
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=lifetime)
    except TimeoutError:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=3)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def main() -> int:
    from loom_cli.secret_source import resolve_secret_source

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", required=True, type=Path)
    parser.add_argument("--provider-key-source", required=True)
    parser.add_argument("--operator-key-source", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--lifetime-seconds", type=float, default=180)
    args = parser.parse_args()
    try:
        binding = CanaryBinding.model_validate_json(args.binding.read_bytes())
        app = create_fault_app(
            binding,
            provider_key=SecretStr(
                resolve_secret_source(args.provider_key_source, flag_name="--provider-key-source")
            ),
            operator_key=SecretStr(
                resolve_secret_source(args.operator_key_source, flag_name="--operator-key-source")
            ),
            lifetime_seconds=args.lifetime_seconds,
        )
        asyncio.run(
            _serve_bounded(app, host=args.host, port=args.port, lifetime=args.lifetime_seconds)
        )
    except Exception:
        print('{"error":"isolated_fixture_failed"}')
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
