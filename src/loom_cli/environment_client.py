"""Typed management requests using the existing origin-bound CLI login."""

from __future__ import annotations

import time
from types import TracebackType
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from loom.nebius_environment_contract import (
    EnvironmentCreateRequestV1,
    EnvironmentOperationRequestV1,
    EnvironmentOperationV1,
    EnvironmentRegistrationV1,
    EnvironmentStatusV1,
)
from loom_cli.server_client import assert_2xx, authed_client, require_logged_in


class EnvironmentClient:
    def __init__(self) -> None:
        self.http = authed_client(require_logged_in())

    def __enter__(self) -> EnvironmentClient:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 traceback: TracebackType | None) -> None:
        self.http.close()

    def create(self, request: EnvironmentCreateRequestV1, *, idempotency_key: str) -> EnvironmentOperationV1:
        response = self.http.post("/api/v1/environments", json=request.model_dump(mode="json"),
                                  headers={"Idempotency-Key": idempotency_key})
        return EnvironmentOperationV1.model_validate(assert_2xx(response, action="create personal environment"))

    def list(self) -> list[EnvironmentRegistrationV1]:
        response = self.http.get("/api/v1/environments")
        return TypeAdapter(list[EnvironmentRegistrationV1]).validate_python(
            assert_2xx(response, action="list personal environments")["items"],
        )

    def destroy(self, environment_id: UUID, *, expected_generation: int, idempotency_key: str) -> EnvironmentOperationV1:
        request = EnvironmentOperationRequestV1(action="destroy_retained", expected_generation=expected_generation)
        response = self.http.post(f"/api/v1/environments/{environment_id}/operations", json=request.model_dump(mode="json"),
                                  headers={"Idempotency-Key": idempotency_key})
        return EnvironmentOperationV1.model_validate(assert_2xx(response, action="retain data and destroy personal environment"))

    def status(self, environment_id: UUID) -> EnvironmentStatusV1:
        return EnvironmentStatusV1.model_validate(assert_2xx(
            self.http.get(f"/api/v1/environments/{environment_id}"), action="read personal environment",
        ))

    def login(self, environment_id: UUID) -> dict[str, Any]:
        value = assert_2xx(self.http.post(f"/api/v1/environments/{environment_id}/login"), action="request personal login")
        if not isinstance(value, dict):
            raise ValueError("invalid personal login response")
        return value

    def operation(self, operation_id: UUID, *, timeout: float = 30) -> EnvironmentOperationV1:
        return EnvironmentOperationV1.model_validate(assert_2xx(
            self.http.get(f"/api/v1/environment-operations/{operation_id}", timeout=timeout),
            action="read environment operation",
        ))

    def retry(self, operation_id: UUID) -> EnvironmentOperationV1:
        return EnvironmentOperationV1.model_validate(assert_2xx(
            self.http.post(f"/api/v1/environment-operations/{operation_id}/retry"), action="retry frozen environment operation",
        ))

    def wait(self, operation_id: UUID, *, timeout: float) -> tuple[EnvironmentOperationV1, bool]:
        if not 0 <= timeout <= 86400:
            raise ValueError("wait timeout must be between zero and one day")
        deadline = time.monotonic() + timeout
        while True:
            operation = self.operation(operation_id, timeout=max(0.1, min(30, deadline - time.monotonic())))
            if operation.phase in {"completed", "blocked"}:
                return operation, True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return operation, False
            time.sleep(min(2, remaining))
