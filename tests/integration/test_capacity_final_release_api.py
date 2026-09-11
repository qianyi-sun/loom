"""Only the historical subject reporter may request retained release authority."""

from unittest.mock import AsyncMock
from uuid import UUID

from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.capacity_final_release_fixtures import final_release_witness
from tests.capacity_fixtures import SUBJECT_ID, SUBJECT_INCARNATION, subject_configuration
from tests.integration.test_capacity_manager_api import (
    DEMAND_TOKEN,
    OLDLAB_V2_EXECUTOR_TOKEN,
    _v2_intent_binding,
)
from tests.integration.test_capacity_manager_api import (
    api_context_v2_executor_generation as api_context_v2_executor_generation,
)


def test_final_release_api_is_exact_reporter_scoped(api_context_v2_executor_generation, monkeypatch):
    client, app, *_ = api_context_v2_executor_generation
    reporter = subject_configuration().demand_reporter_incarnation
    witness = final_release_witness(_v2_intent_binding(), reporter)
    read = AsyncMock(return_value=witness)
    monkeypatch.setattr(app.state.execution_store, "subject_final_release_witness", read)
    path = f"/v2/subjects/{SUBJECT_ID}/intents/{witness.release.binding.intent_id}/final-release-witness"
    headers = {"Authorization": f"Bearer {DEMAND_TOKEN}"}
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"Authorization": f"Bearer {OLDLAB_V2_EXECUTOR_TOKEN}"}).status_code == 403
    assert client.get(path.replace(str(SUBJECT_ID), str(UUID(int=9999))), headers=headers).status_code == 403
    read.assert_not_awaited()
    response = client.get(path, headers=headers)
    assert response.status_code == 200, response.text
    assert response.content == canonical_executable_bytes(witness)
    assert read.await_args.kwargs == dict(subject_id=SUBJECT_ID, subject_incarnation=SUBJECT_INCARNATION,
        reporter_incarnation=reporter, intent_id=witness.release.binding.intent_id)
    read.return_value = None
    assert client.get(path, headers=headers).content == b"null"
