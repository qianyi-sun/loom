"""Bounded, origin-pinned child control; never forward management credentials."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError


class ChildEnvironmentClient:
    def __init__(self, http: httpx.AsyncClient):
        self.http = http

    @staticmethod
    def _expected(row: EnvironmentRegistrationV1) -> dict[str, str]:
        return {"environment_id": str(row.environment_id), "incarnation": str(row.incarnation),
                "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
                "origin": "https://" + row.public_host}

    async def _request(
        self, row: EnvironmentRegistrationV1, *, method: str, action: str, admin_token: str,
    ) -> dict[str, Any]:
        if row.scope != "personal" or row.owner_user_id is None or row.desired_state != "active":
            raise ProviderBlockedError("child_control_identity_invalid")
        identity = {"environment_id": str(row.environment_id), "incarnation": str(row.incarnation)}
        options: dict[str, Any] = {"params": identity} if method == "GET" else {"json": identity}
        try:
            async with asyncio.timeout(30), self.http.stream(
                method, "https://" + row.public_host + "/api/v1/admin/managed-environment/" + action,
                # Explicitly suppress any cookie jar on the injected client.
                headers={"Authorization": "Bearer " + admin_token, "Cookie": "", "Accept": "application/json"},
                follow_redirects=False, timeout=30, **options,
            ) as response:
                if response.status_code in (429, 502, 503, 504):
                    raise ProviderRetryError("child_service_unavailable")
                if response.status_code != 200:
                    raise ProviderBlockedError("child_control_request_rejected")
                body = bytearray()
                async for part in response.aiter_bytes():
                    body.extend(part)
                    if len(body) > 16384:
                        raise ProviderBlockedError("child_control_response_invalid")
        except (httpx.TransportError, TimeoutError):
            raise ProviderRetryError("child_service_unavailable") from None
        try:
            value = json.loads(body)
            if not isinstance(value, dict):
                raise ValueError
            return value
        except ValueError:
            raise ProviderBlockedError("child_control_response_invalid") from None

    def _verify(self, row: EnvironmentRegistrationV1, response: dict[str, Any], *, login: bool = False) -> dict[str, Any]:
        expected = self._expected(row)
        if (set(response) != set(expected) | ({"login_token", "expires_in"} if login else set())
                or any(response.get(key) != value for key, value in expected.items())):
            raise ProviderBlockedError("child_control_identity_mismatch")
        if login:
            import re

            token = response.get("login_token")
            if (not isinstance(token, str) or re.fullmatch(r"loom_env_login_[A-Za-z0-9_-]{43}", token) is None
                    or type(response.get("expires_in")) is not int or response["expires_in"] != 90):
                raise ProviderBlockedError("child_login_proof_invalid")
        return response

    async def login(self, row: EnvironmentRegistrationV1, *, admin_token: str) -> dict[str, Any]:
        return self._verify(row, await self._request(row, method="POST", action="login", admin_token=admin_token), login=True)

    async def enroll(self, row: EnvironmentRegistrationV1, *, admin_token: str) -> dict[str, Any]:
        return self._verify(row, await self._request(row, method="POST", action="owner", admin_token=admin_token))

    async def owner(self, row: EnvironmentRegistrationV1, *, admin_token: str) -> dict[str, Any]:
        return self._verify(row, await self._request(row, method="GET", action="owner", admin_token=admin_token))
