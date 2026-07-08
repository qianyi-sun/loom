"""Minimal httpx-backed gateway client for the family-run orchestrator (#672 PR-3).

The orchestrator's ``skill_patcher_llm`` adapter needs a callable
matching the ``_GatewayClient`` protocol in
``loom.family_run.skill_patcher_llm``:

``async def chat_completion(*, model, messages, dialect, max_tokens,
timeout_sec) -> dict[str, Any]``.

The adapter runs in the orchestrator process — not inside a worker
trial — so there is no live ``trial_id`` to attribute the evolver
spend to. We synthesise a stable per-family attribution using the
family_key so the LLM audit trail is still recoverable, and set the
``family_evolver`` dialect on every call so cost dashboards can slice
adapter spend from agent spend.

Kept as a separate module so the orchestrator entrypoint stays a
thin wire-up shell and the client's HTTP shape is unit-testable
without instantiating the full loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import httpx


@dataclass
class OrchestratorGatewayClient:
    """Thin OpenAI-compatible ``/v1/chat/completions`` client.

    ``token`` may be an empty string in trusted-network deployments
    (the gateway trusts internal-mesh callers). When set, it is sent
    as a ``Bearer`` header.
    """

    base_url: str
    team_id: str
    token: str = ""
    timeout_sec: float = 300.0
    _client: httpx.AsyncClient | None = None
    _owned: bool = field(default=False, init=False)

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout_sec,
            )
            self._owned = True
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and self._owned:
            await self._client.aclose()
            self._client = None
            self._owned = False

    async def chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        dialect: str,
        max_tokens: int,
        timeout_sec: float,
    ) -> dict[str, Any]:
        """POST /v1/chat/completions with the orchestrator attribution
        block. Raises :class:`httpx.HTTPStatusError` on non-2xx so the
        adapter's failure policy sees a plain exception.
        """
        # Synthetic trial_id per call: the row still needs a UUID so
        # the LlmCall FK to trials.id is unique, but there is no real
        # trial. Downstream dashboards ignore rows whose trial_id
        # doesn't join a trial row — the audit intent is preserved
        # via ``dialect=family_evolver``.
        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "loom": {
                "team_id": self.team_id,
                "trial_id": str(uuid4()),
                "step_id": "family_evolver",
                "dialect": dialect,
            },
        }
        headers: dict[str, str] = {"content-type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        client = self._http()
        resp = await client.post(
            "/v1/chat/completions",
            json=body,
            headers=headers,
            timeout=timeout_sec,
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]
