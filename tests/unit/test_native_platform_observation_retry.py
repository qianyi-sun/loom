"""Heartbeat conflicts retry whole read transactions, never permission failures."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.artifact_resolver import BuildAcceptedArtifactResolver
from loom_capacity_build_guard.installation_store import (
    RetainedBuildInstallation,
    build_guard_installation_document,
)
from loom_capacity_manager.contracts import canonical_bytes
from tests.integration.test_personal_dev_native_attempt_executor import receipt_for
from tests.unit.test_personal_dev_build_admission import admission_input
from tests.unit.test_personal_dev_builder import _registration


@pytest.mark.parametrize("operation", ["observe", "resolve"])
@pytest.mark.parametrize("sqlstate,failures,expected_calls", [("40001", 2, 3), ("40P01", 1, 2), ("40001", 4, 3), ("08006", 1, 1), ("P0001", 1, 1)])
async def test_native_platform_reader_retries_only_definite_transaction_aborts(tmp_path, operation, sqlstate, failures, expected_calls):
    values = admission_input(tmp_path)
    document = build_guard_installation_document(member=values["member"], runtime=values["runtime"])
    installation = RetainedBuildInstallation(document=document, wire_payload=canonical_bytes(document))
    registration = _registration()
    receipt = receipt_for(registration, values["member"], "linux/arm64")
    calls = []

    class DatabaseFailureError(Exception):
        pass

    failure = DatabaseFailureError("definite or ambiguous database error")
    failure.sqlstate = sqlstate

    class Session:
        async def scalar(self, statement, arguments):
            assert arguments["installation"] == installation.id
            assert arguments["request"] == receipt.request.claim.request_id
            return canonical_bytes(receipt).decode("ascii")

    class Sessions:
        @asynccontextmanager
        async def begin(self):
            session = Session()
            calls.append(session)
            yield session
            if len(calls) <= failures:
                raise DBAPIError("COMMIT", {}, failure)

    resolver = BuildAcceptedArtifactResolver(session_factory=Sessions(), installation=installation)
    if sqlstate in {"40001", "40P01"} and failures < 3:
        assert await getattr(resolver, operation)(registration, platform="linux/arm64") == receipt
    else:
        with pytest.raises(DBAPIError):
            await getattr(resolver, operation)(registration, platform="linux/arm64")
    assert len(calls) == len({id(session) for session in calls}) == expected_calls
