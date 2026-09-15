"""Installed protected adapter for the single actual legacy trial mutation ledger.

The enclosing cutover owns the admitted database opener and durable identities.
This adapter neither creates credentials nor invents per-route mutation counters.
Each operation commits before returning evidence; a refused NOWAIT operation is
rolled back before the caller can retry with the same retained identity.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from uuid import UUID

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom_capacity_agent.contracts import AgentRegistrationV1


@dataclass(frozen=True, slots=True)
class TrialWriterControlBinding:
    registration: AgentRegistrationV1
    writer_incarnation: UUID
    freeze_operation_id: UUID

    def __post_init__(self) -> None:
        if (
            not isinstance(self.registration, AgentRegistrationV1)
            or any(not isinstance(value, UUID) or value.int == 0 for value in (
                self.writer_incarnation, self.freeze_operation_id,
            ))
            or self.writer_incarnation == self.freeze_operation_id
        ):
            raise ValueError("trial writer control binding is invalid")


@dataclass(frozen=True, slots=True)
class TrialWriterObservation:
    writer_incarnation: UUID
    writer_epoch: int
    high_water: int
    frozen: bool
    freeze_operation_id: UUID | None
    evidence_sha256: str


def _object(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("trial writer returned invalid evidence")
    return value


def _one(connection: ApplicationDatabaseConnection, statement: str | sql.Composed) -> Mapping[str, object]:
    rows = connection.execute(statement).fetchall()
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError("trial writer returned incomplete evidence")
    return _object(rows[0][0])


@dataclass(frozen=True, slots=True)
class ProtectedTrialWriterControl:
    open_database: Callable[[], AbstractContextManager[ApplicationDatabaseConnection]]
    binding: TrialWriterControlBinding

    def __post_init__(self) -> None:
        if not callable(self.open_database) or not isinstance(self.binding, TrialWriterControlBinding):
            raise ValueError("trial writer control authority is invalid")

    def initialize(self) -> TrialWriterObservation:
        return self._run(application_sql(
            "SELECT loom_capacity_guard.initialize_trial_writer_fence({}::uuid, {}::uuid)",
            str(self.binding.registration.agent_incarnation), str(self.binding.writer_incarnation),
        ))

    def freeze(self) -> TrialWriterObservation:
        return self._run(application_sql(
            "SELECT loom_capacity_guard.freeze_trial_writer({}::uuid, {}::uuid)",
            str(self.binding.writer_incarnation), str(self.binding.freeze_operation_id),
        ))

    def capture(self) -> TrialWriterObservation:
        return self._run(None)

    def _run(self, mutation: sql.Composed | None) -> TrialWriterObservation:
        with self.open_database() as connection:
            if connection.info.transaction_status != TransactionStatus.IDLE:
                raise ValueError("trial writer requires an independent control transaction")
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                connection.execute("SET LOCAL statement_timeout = '15s'")
                live = _one(connection, application_sql(
                    "SELECT loom_capacity_guard.lock_trial_writer_registration({}::uuid)",
                    str(self.binding.registration.agent_incarnation),
                ))
                registration = _object(live.get("registration"))
                expected = self.binding.registration.model_dump(mode="json", exclude={"reporter_high_water"})
                if (
                    any(registration.get(key) != value for key, value in expected.items())
                    or registration.get("registration_state") != "registered"
                    or registration.get("singleton_id") != 1
                ):
                    raise ValueError("trial writer registration drifted")
                returned = _one(connection, mutation) if mutation is not None else None
                captured = _one(connection, """
                    SELECT pg_catalog.jsonb_build_object(
                      'fence', pg_catalog.to_jsonb(f),
                      'committed_count', (SELECT count(*)
                        FROM loom_capacity_guard.trial_writer_mutations m
                        WHERE m.writer_incarnation = f.writer_incarnation))
                    FROM loom_capacity_guard.trial_writer_fence f
                    WHERE f.singleton_id = 1 FOR SHARE OF f NOWAIT
                """)
                record = _object(captured.get("fence"))
                count = captured.get("committed_count")
                if returned is not None and returned != record:
                    raise ValueError("trial writer mutation readback drifted")
                observation = self._observation(record, live=live, count=count)
            # This must be outside the transaction: evidence never precedes commit.
            return observation

    def _observation(self, record: Mapping[str, object], *,
                     live: Mapping[str, object], count: object) -> TrialWriterObservation:
        frozen, high_water = record.get("frozen"), record.get("high_water")
        epoch, operation = record.get("writer_epoch"), record.get("freeze_operation_id")
        if (
            set(record) != {"singleton_id", "writer_incarnation", "writer_epoch", "subject_id",
                            "registration", "authority_binding", "high_water", "frozen", "freeze_operation_id"}
            or record.get("singleton_id") != 1
            or record.get("writer_incarnation") != str(self.binding.writer_incarnation)
            or record.get("subject_id") != str(self.binding.registration.subject_id)
            or record.get("registration") != live.get("registration")
            or record.get("authority_binding") != live.get("authority")
            or type(count) is not int or count < 0
            or type(epoch) is not int or epoch < 1
            or type(high_water) is not int or high_water < 0
            or type(frozen) is not bool
            or (frozen and (operation != str(self.binding.freeze_operation_id) or high_water != count))
            or (not frozen and (operation is not None or high_water != 0))
        ):
            raise ValueError("trial writer ledger authority drifted")
        payload = json.dumps(
            {"fence": dict(record), "committed_count": count},
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
        ).encode("ascii")
        return TrialWriterObservation(
            writer_incarnation=self.binding.writer_incarnation, writer_epoch=epoch,
            high_water=count, frozen=frozen,
            freeze_operation_id=self.binding.freeze_operation_id if frozen else None,
            evidence_sha256=hashlib.sha256(payload).hexdigest(),
        )
