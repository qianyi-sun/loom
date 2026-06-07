"""HTTP client for the Control Plane.

Used by the worker to register, claim, heartbeat, PATCH state, and update the
trajectory index. All operations that need crash-safe ownership semantics
are fenced by `worker_id` in the Control Plane's UPDATE WHERE clause
(state PATCH, trajectory index) — a stale worker_id returns 409 and we
surface that as `False` from the corresponding methods so callers can log
+ stop reporting against a trial we no longer own.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import httpx


@dataclass
class HttpControlPlaneClient:
    """One-per-worker, long-lived. Inject `_client` in tests for ASGITransport."""

    base_url: str
    token: str
    timeout_sec: float = 30.0
    _client: httpx.AsyncClient | None = None

    def _http(self) -> tuple[httpx.AsyncClient, bool]:
        """Return (client, owned). owned=True means caller must close it."""
        if self._client is not None:
            return self._client, False
        return (
            httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout_sec,
            ),
            True,
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def register(
        self,
        *,
        hostname: str,
        version: str,
        capabilities: list[dict[str, Any]],
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                "/workers/register", headers=self._headers,
                json={
                    "hostname": hostname,
                    "version": version,
                    "capabilities": capabilities,
                },
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def claim(
        self,
        *,
        worker_id: UUID,
        caps: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        client, owned = self._http()
        try:
            r = await client.post(
                "/trials/claim", headers=self._headers,
                json={"worker_id": str(worker_id), "caps": caps},
            )
            if r.status_code == 204:
                return None
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def heartbeat(self, worker_id: UUID) -> None:
        client, owned = self._http()
        try:
            r = await client.post(
                f"/workers/{worker_id}/heartbeat", headers=self._headers,
            )
            r.raise_for_status()
        finally:
            if owned:
                await client.aclose()

    async def patch_state(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        state: str,
        failure_reason: str | None = None,
    ) -> bool:
        """True on accepted, False if the trial is no longer ours (409)."""
        client, owned = self._http()
        try:
            payload: dict[str, Any] = {
                "worker_id": str(worker_id),
                "state": state,
            }
            if failure_reason is not None:
                payload["failure_reason"] = failure_reason
            r = await client.patch(
                f"/trials/{trial_id}/state", headers=self._headers,
                json=payload,
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def get_task_bundle(self, task_id: str) -> dict[str, Any]:
        """Fetch full TaskConfig + checksum + source by `task_id`."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/tasks/{task_id}/bundle", headers=self._headers,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def patch_trajectory_index(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        **fields: Any,
    ) -> bool:
        client, owned = self._http()
        try:
            r = await client.patch(
                f"/trials/{trial_id}/trajectory_index", headers=self._headers,
                json={"worker_id": str(worker_id), **fields},
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()
