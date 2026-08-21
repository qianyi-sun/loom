"""Bounded atomic worker outbox for resource accounting reports (#1503)."""

from __future__ import annotations

import asyncio
import os
import stat
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from loom.models.resource_usage import TrialResourceUsageReport
from loom_worker.metrics import (
    RESOURCE_ACCOUNTING_EVENTS_TOTAL,
    RESOURCE_ACCOUNTING_OUTBOX_BACKLOG,
)

Deliver = Callable[[TrialResourceUsageReport], Awaitable[bool]]
_MAX_REPORT_BYTES = 64 * 1024


class ResourceUsageOutbox:
    def __init__(self, root: Path, *, max_entries: int = 10_000) -> None:
        self.root = root
        self.max_entries = max_entries

    def _ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.root.is_symlink() or not self.root.is_dir():
            raise RuntimeError("resource usage outbox root is not a private directory")
        root_stat = self.root.stat()
        if root_stat.st_uid != os.getuid():
            raise RuntimeError("resource usage outbox root has an unexpected owner")
        os.chmod(self.root, 0o700)

    def _path(self, report: TrialResourceUsageReport) -> Path:
        return self.root / f"{report.execution_key}.json"

    def _files(self) -> list[Path]:
        if not self.root.exists():
            return []
        return sorted(
            (path for path in self.root.iterdir() if path.name.endswith(".json")),
            key=lambda path: path.name,
        )

    async def stage(self, report: TrialResourceUsageReport) -> None:
        await asyncio.to_thread(self._stage_sync, report)

    def _stage_sync(self, report: TrialResourceUsageReport) -> None:
        self._ensure_root()
        target = self._path(report)
        if not target.exists() and len(self._files()) >= self.max_entries:
            RESOURCE_ACCOUNTING_EVENTS_TOTAL.labels(
                result="failed",
                reason="outbox_full",
            ).inc()
            raise RuntimeError("resource usage outbox entry limit reached")
        body = report.model_dump_json(exclude_none=False).encode("utf-8") + b"\n"
        if len(body) > _MAX_REPORT_BYTES:
            raise RuntimeError("resource usage report exceeds bounded outbox record size")
        temporary = self.root / f".{report.execution_key}.{uuid4().hex}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            dir_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if temporary.exists():
                temporary.unlink()
        RESOURCE_ACCOUNTING_OUTBOX_BACKLOG.set(len(self._files()))

    async def stage_and_deliver(
        self,
        report: TrialResourceUsageReport,
        deliver: Deliver,
    ) -> bool:
        await self.stage(report)
        if not report.finalized_at:
            return False
        return await self._deliver_staged(report, deliver)

    async def replay(self, deliver: Deliver) -> tuple[int, int]:
        self._ensure_root()
        delivered = 0
        failed = 0
        for path in self._files():
            try:
                body = await asyncio.to_thread(self._read_report, path)
                report = TrialResourceUsageReport.model_validate_json(body)
                if report.finalized_at is None:
                    report = report.model_copy(
                        update={
                            "finalized_at": max(datetime.now(UTC), report.last_observed_at),
                            "completeness": "partial",
                            "terminal_reason": "worker_restart",
                            "diagnostic_code": "worker_restart_before_finalize",
                        },
                    )
                    await self.stage(report)
                if await self._deliver_staged(report, deliver):
                    delivered += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
                RESOURCE_ACCOUNTING_EVENTS_TOTAL.labels(
                    result="failed",
                    reason="replay_invalid",
                ).inc()
        RESOURCE_ACCOUNTING_OUTBOX_BACKLOG.set(len(self._files()))
        return delivered, failed

    @staticmethod
    def _read_report(path: Path) -> bytes:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077:
            raise RuntimeError("resource usage outbox entry is not a private regular file")
        if metadata.st_size > _MAX_REPORT_BYTES:
            raise RuntimeError("resource usage outbox entry exceeds the size limit")
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            handle = os.fdopen(descriptor, "rb", closefd=True)
        except BaseException:
            os.close(descriptor)
            raise
        with handle:
            body = handle.read(_MAX_REPORT_BYTES + 1)
        if len(body) > _MAX_REPORT_BYTES:
            raise RuntimeError("resource usage outbox entry exceeds the size limit")
        return body

    async def _deliver_staged(
        self,
        report: TrialResourceUsageReport,
        deliver: Deliver,
    ) -> bool:
        try:
            accepted = await deliver(report)
        except Exception:
            RESOURCE_ACCOUNTING_EVENTS_TOTAL.labels(
                result="failed",
                reason="control_plane",
            ).inc()
            return False
        if not accepted:
            RESOURCE_ACCOUNTING_EVENTS_TOTAL.labels(
                result="failed",
                reason="rejected",
            ).inc()
            return False
        path = self._path(report)
        try:
            await asyncio.to_thread(path.unlink, missing_ok=True)
        except OSError:
            RESOURCE_ACCOUNTING_EVENTS_TOTAL.labels(
                result="failed",
                reason="ack_cleanup",
            ).inc()
            return False
        RESOURCE_ACCOUNTING_EVENTS_TOTAL.labels(result="delivered", reason="ok").inc()
        RESOURCE_ACCOUNTING_OUTBOX_BACKLOG.set(len(self._files()))
        return True
