"""SQLAlchemy-backed lifecycle authority for shared-fleet dev instances."""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import DevInstance
from loom.dev_instance_provisioner import (
    DevInstanceConflictError,
    DevInstanceOperationFencedError,
    DevInstanceRecord,
    DevInstanceStatus,
    InstanceReservation,
)


def _record(row: DevInstance) -> DevInstanceRecord:
    return DevInstanceRecord(
        name=row.name,
        owner_user_id=row.owner_user_id,
        owner_team_id=row.owner_team_id,
        min_slots=row.min_slots,
        max_slots=row.max_slots,
        status=cast(DevInstanceStatus, row.status),
        deployment_generation=row.deployment_generation,
        candidate_sha=row.candidate_sha,
        operation_epoch=row.operation_epoch,
        operation_id=row.operation_id,
        operation_step=row.operation_step,
        created_at=row.created_at,
        updated_at=row.updated_at,
        secret_ref=row.secret_ref,
        keep_data=row.keep_data,
        failure_reason=row.failure_reason,
        ready_at=row.ready_at,
        deleted_at=row.deleted_at,
    )


class SqlAlchemyDevInstanceStore:
    """Fenced registry operations bound to one request-scoped session."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, name: str) -> DevInstanceRecord | None:
        row = await self.session.get(DevInstance, name)
        return _record(row) if row is not None else None

    async def list_active(self) -> list[DevInstanceRecord]:
        rows = (
            (
                await self.session.execute(
                    select(DevInstance)
                    .where(DevInstance.status != "deleted")
                    .order_by(DevInstance.name),
                )
            )
            .scalars()
            .all()
        )
        return [_record(row) for row in rows]

    async def list_visible(
        self,
        *,
        owner_user_id: UUID | None,
        include_deleted: bool = False,
    ) -> list[DevInstanceRecord]:
        statement = select(DevInstance)
        if owner_user_id is not None:
            statement = statement.where(DevInstance.owner_user_id == owner_user_id)
        if not include_deleted:
            statement = statement.where(DevInstance.status != "deleted")
        rows = (
            (
                await self.session.execute(
                    statement.order_by(DevInstance.created_at, DevInstance.name),
                )
            )
            .scalars()
            .all()
        )
        return [_record(row) for row in rows]

    async def claim_create(self, requested: DevInstanceRecord) -> InstanceReservation:
        inserted = (
            await self.session.execute(
                pg_insert(DevInstance)
                .values(
                    name=requested.name,
                    owner_user_id=requested.owner_user_id,
                    owner_team_id=requested.owner_team_id,
                    min_slots=requested.min_slots,
                    max_slots=requested.max_slots,
                    status="provisioning",
                    deployment_generation=requested.deployment_generation,
                    candidate_sha=requested.candidate_sha,
                    operation_epoch=1,
                    operation_id=requested.operation_id,
                    operation_step="claimed",
                    secret_ref=requested.secret_ref,
                    keep_data=False,
                    failure_reason=None,
                    created_at=requested.created_at,
                    updated_at=requested.updated_at,
                )
                .on_conflict_do_nothing(index_elements=[DevInstance.name])
                .returning(DevInstance.name),
            )
        ).scalar_one_or_none()
        if inserted is not None:
            row = await self._locked(requested.name)
            return InstanceReservation(record=_record(row), acquired=True)

        row = await self._locked(requested.name)
        if row.owner_user_id != requested.owner_user_id:
            raise DevInstanceConflictError("dev instance name is already owned")
        if row.owner_team_id != requested.owner_team_id:
            raise DevInstanceConflictError("dev instance owner team changed; destroy it first")
        exact_shape = (
            row.min_slots == requested.min_slots
            and row.max_slots == requested.max_slots
            and row.deployment_generation == requested.deployment_generation
            and row.candidate_sha == requested.candidate_sha
        )
        if row.status == "ready" and exact_shape:
            return InstanceReservation(record=_record(row), acquired=False)
        if row.status == "ready":
            raise DevInstanceConflictError(
                "ready dev instance is bound to a different shape or candidate; "
                "destroy it first (optionally with keep_data)",
            )
        if row.status in {"provisioning", "deleting"}:
            return InstanceReservation(record=_record(row), acquired=False)

        row.min_slots = requested.min_slots
        row.max_slots = requested.max_slots
        row.status = "provisioning"
        row.deployment_generation = requested.deployment_generation
        row.candidate_sha = requested.candidate_sha
        row.operation_epoch += 1
        row.operation_id = requested.operation_id
        row.operation_step = "claimed"
        row.keep_data = False
        row.failure_reason = None
        row.updated_at = requested.updated_at
        row.deleted_at = None
        await self.session.flush()
        return InstanceReservation(record=_record(row), acquired=True)

    async def claim_destroy(
        self,
        name: str,
        *,
        operation_id: UUID,
        keep_data: bool,
        now: datetime,
    ) -> InstanceReservation | None:
        row = (
            await self.session.execute(
                select(DevInstance).where(DevInstance.name == name).with_for_update(),
            )
        ).scalar_one_or_none()
        if row is None or row.status == "deleted":
            return None
        if row.status == "provisioning":
            raise DevInstanceConflictError(
                "dev instance is still provisioning; retry create or wait for readiness",
            )
        if row.status == "deleting":
            return InstanceReservation(record=_record(row), acquired=False)
        resuming_deletion = row.failure_reason == "deletion_failed"
        if resuming_deletion and row.keep_data != keep_data:
            raise DevInstanceConflictError(
                "keep_data cannot change while resuming a failed deletion",
            )
        row.status = "deleting"
        row.operation_epoch += 1
        row.operation_id = operation_id
        if not resuming_deletion:
            row.operation_step = "claimed"
        row.keep_data = keep_data
        row.failure_reason = None
        row.updated_at = now
        await self.session.flush()
        return InstanceReservation(record=_record(row), acquired=True)

    async def assert_operation(self, name: str, operation_id: UUID) -> None:
        observed = (
            await self.session.execute(
                select(DevInstance.operation_id).where(DevInstance.name == name),
            )
        ).scalar_one_or_none()
        if observed != operation_id:
            raise DevInstanceOperationFencedError(
                "dev instance lifecycle operation was superseded",
            )

    async def set_secret_ref(self, name: str, operation_id: UUID, secret_ref: str) -> None:
        result = await self.session.execute(
            update(DevInstance)
            .where(
                DevInstance.name == name,
                DevInstance.operation_id == operation_id,
            )
            .values(secret_ref=secret_ref)
            .returning(DevInstance.name),
        )
        if result.scalar_one_or_none() is None:
            raise DevInstanceOperationFencedError(
                "dev instance lifecycle operation was superseded",
            )

    async def set_operation_step(self, name: str, operation_id: UUID, step: str) -> None:
        result = await self.session.execute(
            update(DevInstance)
            .where(
                DevInstance.name == name,
                DevInstance.operation_id == operation_id,
            )
            .values(operation_step=step)
            .returning(DevInstance.name),
        )
        if result.scalar_one_or_none() is None:
            raise DevInstanceOperationFencedError(
                "dev instance lifecycle operation was superseded",
            )

    async def complete_operation(
        self,
        name: str,
        operation_id: UUID,
        *,
        status: DevInstanceStatus,
        now: datetime,
        failure_reason: str | None = None,
    ) -> DevInstanceRecord:
        values: dict[str, object] = {
            "status": status,
            "failure_reason": failure_reason,
            "updated_at": now,
        }
        if status == "ready":
            values["ready_at"] = now
            values["operation_step"] = "complete"
        if status == "deleted":
            values["deleted_at"] = now
            values["operation_step"] = "complete"
        result = await self.session.execute(
            update(DevInstance)
            .where(
                DevInstance.name == name,
                DevInstance.operation_id == operation_id,
            )
            .values(**values)
            .returning(DevInstance),
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise DevInstanceOperationFencedError(
                "dev instance lifecycle operation was superseded",
            )
        return _record(row)

    async def checkpoint(self) -> None:
        await self.session.commit()

    async def _locked(self, name: str) -> DevInstance:
        return (
            await self.session.execute(
                select(DevInstance).where(DevInstance.name == name).with_for_update(),
            )
        ).scalar_one()


__all__ = ["SqlAlchemyDevInstanceStore"]
