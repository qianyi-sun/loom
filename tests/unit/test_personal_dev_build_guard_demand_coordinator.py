"""Retry only definite PostgreSQL transaction aborts, never ambiguous outcomes."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.exc import DBAPIError

import loom_capacity_build_guard.demand_store as module


@pytest.mark.parametrize("sqlstate,failures,expected_calls", [
    ("40001",2,3), ("40P01",2,3), ("40001",4,3), ("08006",1,1), ("P0001",1,1),
])
async def test_whole_transaction_retry_policy(monkeypatch, sqlstate, failures, expected_calls):
    calls = []
    value = object()

    class DatabaseFailureError(Exception):
        pass

    failure = DatabaseFailureError("database failure")
    failure.sqlstate = sqlstate

    class Sessions:
        @asynccontextmanager
        async def begin(self):
            session = object()
            calls.append(session)
            yield session
            if len(calls) <= failures:
                raise DBAPIError("COMMIT", {}, failure)

    class Store:
        def __init__(self, session, *, installation):
            self.session = session

        async def capture(self, **kwargs):
            assert self.session is calls[-1]
            return value

    monkeypatch.setattr(module, "BuildGuardDemandStore", Store)
    coordinator = module.BuildDemandCoordinator(Sessions(), installation=object())
    if sqlstate in {"40001","40P01"} and failures<3:
        assert await coordinator.capture(configuration_generation=1, sources={}) is value
    else:
        with pytest.raises(DBAPIError):
            await coordinator.capture(configuration_generation=1, sources={})
    assert len(calls) == expected_calls
    assert len({id(session) for session in calls}) == expected_calls
