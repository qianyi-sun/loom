"""Protected operator serialization reaches the real migrated V3 manager store.

Only the HTTP transport is bridged to the in-process ASGI application. This is
not TLS, live credentials, executor activation or all-environment cutover proof.
"""

import asyncio
import os
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.models import CapacityExecutionEpoch
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClient,
    ProtectedCapacityManagerClientError,
)
from tests.integration.test_capacity_membership_api import (
    PREPARE_TOKEN,
)
from tests.integration.test_capacity_membership_api import (
    membership_api as membership_api,
)
from tests.loom_cli.rollout.operator.test_protected_capacity_manager_client import _credentials


@pytest.mark.parametrize("membership_api", ("shadow",), indirect=True)
async def test_operator_v3_preparation_preserves_database_manifest_and_exact_replay(
    membership_api,
    tmp_path,
):
    api, active, sessions, request = membership_api
    assert active is None and request.schema_version == 3
    credentials = _credentials(tmp_path)
    (credentials / "manager-prepare" / "bearer-token").write_text(PREPARE_TOKEN)
    loop = asyncio.get_running_loop()
    calls = []

    def forward(outbound: httpx.Request) -> httpx.Response:
        calls.append(outbound.url.path)
        pending = asyncio.run_coroutine_threadsafe(
            api.request(
                outbound.method,
                outbound.url.path,
                headers=outbound.headers,
                content=outbound.content,
            ),
            loop,
        )
        try:
            inbound = pending.result(timeout=10)
            # Bridge the completed ASGI response bytes to a synchronous stream;
            # do not pass AsyncClient's bound async stream into the real client.
            return httpx.Response(
                inbound.status_code,
                headers=inbound.headers,
                content=inbound.content,
            )
        finally:
            if not pending.done():
                pending.cancel()

    operator = ProtectedCapacityManagerClient(
        origin="https://127.0.0.1:43210",
        credentials_root=credentials,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        client_factory=lambda _context: httpx.Client(transport=httpx.MockTransport(forward)),
    )
    key = UUID(int=22106)
    prepared = await asyncio.to_thread(operator.prepare_execution, request, key)
    assert prepared.execution_state == "prepared"
    assert prepared.executable_new_capacity_ceiling == 0
    assert prepared.execution_manifest_sha256 == canonical_executable_digest(request)
    assert await asyncio.to_thread(operator.prepare_execution, request, key) == prepared

    changed = request.model_copy(
        update={
            "personal_membership": request.personal_membership.model_copy(
                update={"max_subjects": 3}
            ),
        }
    )
    with pytest.raises(ProtectedCapacityManagerClientError):
        await asyncio.to_thread(operator.prepare_execution, changed, key)
    assert calls == ["/v3/execution-preparations"] * 3
    async with sessions() as session:
        rows = (await session.scalars(select(CapacityExecutionEpoch))).all()
        assert len(rows) == 1
        assert rows[0].manifest_payload == request.model_dump(mode="json", exclude_none=False)
        assert rows[0].idempotency_key == key
        assert rows[0].state == "prepared"
        assert rows[0].actor == "preparation-operator"
