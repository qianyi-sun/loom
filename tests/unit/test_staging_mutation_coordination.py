from __future__ import annotations

from contextlib import AbstractContextManager
from typing import cast

import pytest
from sqlalchemy import Engine

from loom.staging_mutation_coordination import hold_staging_mutation_guard

_TRY_SQL = "SELECT pg_try_advisory_lock(5498691230183247727)"
_UNLOCK_SQL = "SELECT pg_advisory_unlock(5498691230183247727)"


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar_one(self) -> object:
        return self._value


class _Connection:
    def __init__(
        self,
        results: list[object],
        *,
        failures: dict[int, BaseException] | None = None,
    ) -> None:
        self._results = iter(results)
        self._failures = failures or {}
        self.statements: list[str] = []
        self.closed = False

    def execute(self, statement: object) -> _ScalarResult:
        index = len(self.statements)
        self.statements.append(str(statement))
        if index in self._failures:
            raise self._failures[index]
        return _ScalarResult(next(self._results))

    def close(self) -> None:
        self.closed = True


class _Engine:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.connect_calls = 0

    def connect(self) -> _Connection:
        self.connect_calls += 1
        return self.connection


def _guard(engine: _Engine) -> AbstractContextManager[bool]:
    return hold_staging_mutation_guard(cast(Engine, engine))


def test_guard_holds_one_connection_until_explicit_unlock() -> None:
    connection = _Connection([True, True])
    engine = _Engine(connection)

    with _guard(engine) as acquired:
        assert acquired is True
        assert connection.statements == [_TRY_SQL]
        assert connection.closed is False

    assert engine.connect_calls == 1
    assert connection.statements == [_TRY_SQL, _UNLOCK_SQL]
    assert connection.closed is True


def test_guard_denial_closes_without_unlocking() -> None:
    connection = _Connection([False])

    with _guard(_Engine(connection)) as acquired:
        assert acquired is False

    assert connection.statements == [_TRY_SQL]
    assert connection.closed is True


@pytest.mark.parametrize("value", [None, 0, 1, "t"])
def test_guard_rejects_non_boolean_acquisition_result(value: object) -> None:
    connection = _Connection([value])

    with pytest.raises(RuntimeError, match="acquisition result is invalid"):
        with _guard(_Engine(connection)):
            pass

    assert connection.statements == [_TRY_SQL]
    assert connection.closed is True


def test_guard_unlocks_when_protected_work_raises() -> None:
    connection = _Connection([True, True])

    with pytest.raises(LookupError, match="protected work failed"):
        with _guard(_Engine(connection)) as acquired:
            assert acquired is True
            raise LookupError("protected work failed")

    assert connection.statements == [_TRY_SQL, _UNLOCK_SQL]
    assert connection.closed is True


def test_guard_fails_closed_when_unlock_reports_not_held() -> None:
    connection = _Connection([True, False])

    with pytest.raises(RuntimeError, match="unlock result is invalid"):
        with _guard(_Engine(connection)) as acquired:
            assert acquired is True

    assert connection.statements == [_TRY_SQL, _UNLOCK_SQL]
    assert connection.closed is True


def test_guard_closes_connection_when_explicit_unlock_raises() -> None:
    connection = _Connection(
        [True],
        failures={1: RuntimeError("database unavailable during unlock")},
    )

    with pytest.raises(RuntimeError, match="database unavailable during unlock"):
        with _guard(_Engine(connection)) as acquired:
            assert acquired is True

    assert connection.statements == [_TRY_SQL, _UNLOCK_SQL]
    assert connection.closed is True
