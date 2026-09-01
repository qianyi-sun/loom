from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import cast

import pytest
from sqlalchemy import Engine

from loom.staging_mutation_coordination import (
    RolloutCapacityAuthority,
    hold_rollout_capacity_authority,
    hold_staging_mutation_guard,
)

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


class _MappingsResult:
    def __init__(self, row: Mapping[str, object] | None) -> None:
        self._row = row

    def mappings(self) -> _MappingsResult:
        return self

    def one_or_none(self) -> Mapping[str, object] | None:
        return self._row


class _AuthorityConnection:
    def __init__(self, rows: list[Mapping[str, object] | None]) -> None:
        self._rows = iter(rows)
        self.calls: list[tuple[str, Mapping[str, object]]] = []
        self.closed = False

    def execute(
        self,
        statement: object,
        parameters: Mapping[str, object],
    ) -> _MappingsResult:
        self.calls.append((str(statement), parameters))
        return _MappingsResult(next(self._rows))

    def close(self) -> None:
        self.closed = True


class _AuthorityEngine:
    def __init__(self, connection: _AuthorityConnection) -> None:
        self.connection = connection

    def connect(self) -> _AuthorityConnection:
        return self.connection


def _rollout_authority() -> RolloutCapacityAuthority:
    return RolloutCapacityAuthority(
        request_id="req-1111111111111111",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        generation="1" * 32,
        guard_backend_pid=4321,
        mutation_epoch=9,
        plan_digest="f" * 64,
    )


def test_rollout_capacity_authority_requires_exact_guard_and_epoch_before_and_after() -> None:
    connection = _AuthorityConnection(
        [
            {"guard_owned": True, "epoch_owned": True},
            {"guard_owned": True, "epoch_owned": True},
        ]
    )

    with hold_rollout_capacity_authority(
        cast(Engine, _AuthorityEngine(connection)), _rollout_authority()
    ):
        assert len(connection.calls) == 1
        assert connection.closed is False

    assert len(connection.calls) == 2
    _, parameters = connection.calls[0]
    assert parameters == {
        "guard_application_name": (
            "loom-rollout-guard-c6933d4f4410aceb546586acdd9a1843641553f7"
        ),
        "guard_backend_pid": 4321,
        "mutation_epoch": 9,
        "plan_digest": "f" * 64,
        "request_id": "req-1111111111111111",
    }
    assert connection.calls[1][1] == parameters
    assert connection.closed is True


@pytest.mark.parametrize(
    "row",
    [
        None,
        {"guard_owned": False, "epoch_owned": True},
        {"guard_owned": True, "epoch_owned": False},
        {"guard_owned": 1, "epoch_owned": True},
        {"guard_owned": True, "epoch_owned": True, "unexpected": True},
    ],
)
def test_rollout_capacity_authority_denies_wrong_guard_request_plan_or_epoch(
    row: Mapping[str, object] | None,
) -> None:
    connection = _AuthorityConnection([row])
    entered = False

    with pytest.raises(RuntimeError, match="rollout capacity authority is unavailable"):
        with hold_rollout_capacity_authority(
            cast(Engine, _AuthorityEngine(connection)), _rollout_authority()
        ):
            entered = True

    assert entered is False
    assert len(connection.calls) == 1
    assert connection.closed is True


def test_rollout_capacity_authority_must_remain_live_after_publication() -> None:
    connection = _AuthorityConnection(
        [
            {"guard_owned": True, "epoch_owned": True},
            {"guard_owned": False, "epoch_owned": True},
        ]
    )
    published = False

    with pytest.raises(RuntimeError, match="rollout capacity authority is unavailable"):
        with hold_rollout_capacity_authority(
            cast(Engine, _AuthorityEngine(connection)), _rollout_authority()
        ):
            published = True

    assert published is True
    assert len(connection.calls) == 2
    assert connection.closed is True


def test_rollout_capacity_authority_is_rechecked_when_publication_body_raises() -> None:
    connection = _AuthorityConnection(
        [
            {"guard_owned": True, "epoch_owned": True},
            {"guard_owned": False, "epoch_owned": True},
        ]
    )

    with pytest.raises(RuntimeError, match="rollout capacity authority is unavailable"):
        with hold_rollout_capacity_authority(
            cast(Engine, _AuthorityEngine(connection)), _rollout_authority()
        ):
            raise LookupError("publication output failed after commit")

    assert len(connection.calls) == 2
    assert connection.closed is True
