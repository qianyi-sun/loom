"""Durable worker-local CAS metadata, leases, GC, and acceptance eviction."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID

from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.work_protocol import (
    AcceptanceEvictionEntryV1,
    AcceptanceEvictionGrantV1,
    AcceptanceEvictionResultV1,
)

HIGH_WATERMARK_NUMERATOR = 85
LOW_WATERMARK_NUMERATOR = 70
WATERMARK_DENOMINATOR = 100
GC_BATCH_SIZE = 100
PARTIAL_RETENTION = timedelta(hours=24)
QUARANTINE_RETENTION = timedelta(days=7)


class ArtifactInputJournalError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class AcceptanceEvictionAuthorityV1(Protocol):
    async def authorize(
        self,
        *,
        authorization_id: UUID,
        candidate_sha256: str,
        worker_id: UUID,
        ordered_manifest_sha256s: tuple[str, str, str, str, str],
    ) -> AcceptanceEvictionGrantV1: ...


@dataclass(frozen=True)
class CacheEntry:
    manifest_sha256: str
    state: str
    unpacked_size_bytes: int
    file_count: int
    ready_path: Path | None
    refcount: int
    ready_sha256: str | None


@dataclass(frozen=True)
class CacheCapacitySnapshot:
    input_cache_capacity_bytes: int
    input_cache_reserved_bytes: int
    input_cache_ready_bytes: int
    input_cache_in_progress_bytes: int

    def registration_fields(self) -> dict[str, int]:
        if not (
            0
            <= self.input_cache_reserved_bytes
            <= self.input_cache_ready_bytes + self.input_cache_in_progress_bytes
            <= self.input_cache_capacity_bytes
        ):
            raise ArtifactInputJournalError("input_cache_capacity_drift")
        return {
            "input_cache_capacity_bytes": self.input_cache_capacity_bytes,
            "input_cache_reserved_bytes": self.input_cache_reserved_bytes,
            "input_cache_ready_bytes": self.input_cache_ready_bytes,
        }


def allocatable_capacity(raw_filesystem_bytes: int) -> int:
    if isinstance(raw_filesystem_bytes, bool) or raw_filesystem_bytes < 0:
        raise ValueError("raw filesystem bytes must be non-negative")
    return raw_filesystem_bytes * HIGH_WATERMARK_NUMERATOR // WATERMARK_DENOMINATOR


def validate_registration_capacity(
    *, capacity_bytes: int, reserved_bytes: int, ready_bytes: int, in_progress_bytes: int = 0
) -> CacheCapacitySnapshot:
    snapshot = CacheCapacitySnapshot(
        input_cache_capacity_bytes=capacity_bytes,
        input_cache_reserved_bytes=reserved_bytes,
        input_cache_ready_bytes=ready_bytes,
        input_cache_in_progress_bytes=in_progress_bytes,
    )
    snapshot.registration_fields()
    return snapshot


class ArtifactInputJournal:
    """SQLite authority for cache entries and Attempt-scoped leases."""

    def __init__(self, *, database_path: Path, cas_root: Path, capacity_bytes: int) -> None:
        if capacity_bytes < 0:
            raise ValueError("capacity_bytes must be non-negative")
        self.database_path = database_path.resolve()
        self.cas_root = cas_root.resolve()
        self.capacity_bytes = capacity_bytes
        self._lock = threading.RLock()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.cas_root.mkdir(parents=True, exist_ok=True)
        (self.cas_root / ".acceptance-eviction").mkdir(exist_ok=True)
        (self.cas_root / ".partial").mkdir(exist_ok=True)
        (self.cas_root / ".quarantine").mkdir(exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS artifact_input_cache_entries (
                    manifest_sha256 TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK(state IN ('materializing','ready','quarantined','deleting')),
                    unpacked_size_bytes INTEGER NOT NULL CHECK(unpacked_size_bytes >= 0),
                    file_count INTEGER NOT NULL CHECK(file_count >= 0),
                    ready_path TEXT,
                    ready_sha256 TEXT,
                    owner_attempt_id TEXT,
                    refcount INTEGER NOT NULL DEFAULT 0 CHECK(refcount >= 0),
                    last_accessed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    CHECK((state='ready') = (ready_path IS NOT NULL AND ready_sha256 IS NOT NULL))
                );
                CREATE TABLE IF NOT EXISTS artifact_input_leases (
                    execution_attempt_id TEXT NOT NULL,
                    binding_name TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    manifest_sha256 TEXT NOT NULL REFERENCES artifact_input_cache_entries(manifest_sha256),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(execution_attempt_id,binding_name,item_key)
                );
                CREATE INDEX IF NOT EXISTS artifact_input_cache_lru_idx
                    ON artifact_input_cache_entries(state,refcount,last_accessed_at,manifest_sha256);
                CREATE TABLE IF NOT EXISTS acceptance_eviction_operations (
                    authorization_id TEXT NOT NULL,
                    candidate_sha256 TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    command_id TEXT NOT NULL UNIQUE,
                    ordered_manifest_sha256s_json BLOB NOT NULL,
                    entries_json BLOB NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('deleting','complete')),
                    result_json BLOB,
                    result_sha256 TEXT,
                    created_at TEXT NOT NULL,
                    finished_at TEXT,
                    PRIMARY KEY(authorization_id,candidate_sha256,worker_id),
                    CHECK((state='complete') = (result_json IS NOT NULL AND result_sha256 IS NOT NULL))
                );
                CREATE TABLE IF NOT EXISTS acceptance_eviction_audit (
                    authorization_id TEXT NOT NULL,
                    candidate_sha256 TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    result_sha256 TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    PRIMARY KEY(authorization_id,candidate_sha256,worker_id)
                );
                """
            )

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    def get_entry(self, manifest_sha256: str) -> CacheEntry | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM artifact_input_cache_entries WHERE manifest_sha256=?",
                (manifest_sha256,),
            ).fetchone()
        return self._entry(row) if row is not None else None

    @staticmethod
    def _entry(row: sqlite3.Row) -> CacheEntry:
        return CacheEntry(
            manifest_sha256=str(row["manifest_sha256"]),
            state=str(row["state"]),
            unpacked_size_bytes=int(row["unpacked_size_bytes"]),
            file_count=int(row["file_count"]),
            ready_path=Path(row["ready_path"]) if row["ready_path"] is not None else None,
            refcount=int(row["refcount"]),
            ready_sha256=str(row["ready_sha256"]) if row["ready_sha256"] else None,
        )

    def capacity_snapshot(self) -> CacheCapacitySnapshot:
        with self._connect() as db:
            row = db.execute(
                """SELECT
                   COALESCE(SUM(CASE WHEN state='ready' THEN unpacked_size_bytes ELSE 0 END),0) ready,
                   COALESCE(SUM(CASE WHEN state='materializing' THEN unpacked_size_bytes ELSE 0 END),0) progress,
                   COALESCE(SUM(CASE WHEN state='ready' AND refcount>0 THEN unpacked_size_bytes ELSE 0 END),0) leased
                   FROM artifact_input_cache_entries"""
            ).fetchone()
        assert row is not None
        return validate_registration_capacity(
            capacity_bytes=self.capacity_bytes,
            reserved_bytes=int(row["progress"]) + int(row["leased"]),
            ready_bytes=int(row["ready"]),
            in_progress_bytes=int(row["progress"]),
        )

    def reserve(
        self,
        *,
        manifest_sha256: str,
        unpacked_size_bytes: int,
        file_count: int,
        owner_attempt_id: UUID,
    ) -> CacheEntry | None:
        """Reserve a unique miss. Return an existing ready entry for a hit."""

        with self._lock:
            with self._connect() as probe:
                exists = probe.execute(
                    "SELECT 1 FROM artifact_input_cache_entries WHERE manifest_sha256=?",
                    (manifest_sha256,),
                ).fetchone()
                used = probe.execute(
                    "SELECT COALESCE(SUM(unpacked_size_bytes),0) FROM artifact_input_cache_entries "
                    "WHERE state IN ('materializing','ready')"
                ).fetchone()
            assert used is not None
            if exists is None and int(used[0]) + unpacked_size_bytes > self.capacity_bytes:
                self.gc_zero_ref(
                    target_bytes=max(0, self.capacity_bytes - unpacked_size_bytes)
                )
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM artifact_input_cache_entries WHERE manifest_sha256=?",
                (manifest_sha256,),
            ).fetchone()
            if row is not None:
                entry = self._entry(row)
                if entry.state == "ready":
                    if (
                        entry.unpacked_size_bytes != unpacked_size_bytes
                        or entry.file_count != file_count
                    ):
                        db.rollback()
                        raise ArtifactInputJournalError("input_descriptor_drift")
                    db.execute(
                        "UPDATE artifact_input_cache_entries SET last_accessed_at=?,updated_at=? WHERE manifest_sha256=?",
                        (self._now(), self._now(), manifest_sha256),
                    )
                    db.commit()
                    return entry
                db.rollback()
                raise ArtifactInputJournalError("input_cache_entry_busy")
            totals = db.execute(
                "SELECT COALESCE(SUM(unpacked_size_bytes),0) AS used FROM artifact_input_cache_entries "
                "WHERE state IN ('materializing','ready')"
            ).fetchone()
            assert totals is not None
            if int(totals["used"]) + unpacked_size_bytes > self.capacity_bytes:
                db.rollback()
                raise ArtifactInputJournalError("input_cache_capacity")
            now = self._now()
            db.execute(
                """INSERT INTO artifact_input_cache_entries
                   (manifest_sha256,state,unpacked_size_bytes,file_count,owner_attempt_id,
                    refcount,last_accessed_at,created_at,updated_at)
                   VALUES(?,?,?,?,?,0,?,?,?)""",
                (
                    manifest_sha256,
                    "materializing",
                    unpacked_size_bytes,
                    file_count,
                    str(owner_attempt_id),
                    now,
                    now,
                    now,
                ),
            )
            db.commit()
            return None

    def mark_ready(
        self, *, manifest_sha256: str, ready_path: Path, ready_sha256: str
    ) -> CacheEntry:
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                """UPDATE artifact_input_cache_entries
                   SET state='ready',ready_path=?,ready_sha256=?,owner_attempt_id=NULL,updated_at=?
                   WHERE manifest_sha256=? AND state='materializing'""",
                (str(ready_path.resolve()), ready_sha256, self._now(), manifest_sha256),
            ).rowcount
            if changed != 1:
                db.rollback()
                raise ArtifactInputJournalError("input_cache_state_conflict")
            row = db.execute(
                "SELECT * FROM artifact_input_cache_entries WHERE manifest_sha256=?",
                (manifest_sha256,),
            ).fetchone()
            db.commit()
        assert row is not None
        return self._entry(row)

    def quarantine(self, *, manifest_sha256: str) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                """UPDATE artifact_input_cache_entries SET state='quarantined',owner_attempt_id=NULL,
                   ready_path=NULL,ready_sha256=NULL,updated_at=? WHERE manifest_sha256=?""",
                (self._now(), manifest_sha256),
            )

    def acquire_lease(
        self,
        *,
        execution_attempt_id: UUID,
        binding_name: str,
        item_key: str,
        manifest_sha256: str,
    ) -> None:
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT state FROM artifact_input_cache_entries WHERE manifest_sha256=?",
                (manifest_sha256,),
            ).fetchone()
            if row is None or row["state"] != "ready":
                db.rollback()
                raise ArtifactInputJournalError("input_cache_not_ready")
            existing = db.execute(
                """SELECT manifest_sha256 FROM artifact_input_leases
                   WHERE execution_attempt_id=? AND binding_name=? AND item_key=?""",
                (str(execution_attempt_id), binding_name, item_key),
            ).fetchone()
            if existing is not None:
                if existing["manifest_sha256"] != manifest_sha256:
                    db.rollback()
                    raise ArtifactInputJournalError("input_lease_conflict")
                db.commit()
                return
            now = self._now()
            db.execute(
                "INSERT INTO artifact_input_leases VALUES(?,?,?,?,?)",
                (str(execution_attempt_id), binding_name, item_key, manifest_sha256, now),
            )
            db.execute(
                "UPDATE artifact_input_cache_entries SET refcount=refcount+1,last_accessed_at=?,updated_at=? WHERE manifest_sha256=?",
                (now, now, manifest_sha256),
            )
            db.commit()

    def release_attempt(self, execution_attempt_id: UUID) -> int:
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                "SELECT manifest_sha256,COUNT(*) count FROM artifact_input_leases "
                "WHERE execution_attempt_id=? GROUP BY manifest_sha256",
                (str(execution_attempt_id),),
            ).fetchall()
            db.execute(
                "DELETE FROM artifact_input_leases WHERE execution_attempt_id=?",
                (str(execution_attempt_id),),
            )
            for row in rows:
                db.execute(
                    "UPDATE artifact_input_cache_entries SET refcount=refcount-?,updated_at=? "
                    "WHERE manifest_sha256=? AND refcount>=?",
                    (row["count"], self._now(), row["manifest_sha256"], row["count"]),
                )
            db.commit()
            return sum(int(row["count"]) for row in rows)

    def abandon_materialization(self, *, manifest_sha256: str, owner_attempt_id: UUID) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "DELETE FROM artifact_input_cache_entries WHERE manifest_sha256=? "
                "AND state='materializing' AND owner_attempt_id=?",
                (manifest_sha256, str(owner_attempt_id)),
            )

    def gc_zero_ref(self, *, target_bytes: int | None = None) -> int:
        target = self.capacity_bytes * LOW_WATERMARK_NUMERATOR // WATERMARK_DENOMINATOR
        if target_bytes is not None:
            target = min(target, target_bytes)
        freed = 0
        with self._lock:
            while True:
                snapshot = self.capacity_snapshot()
                if snapshot.input_cache_ready_bytes <= target:
                    return freed
                with self._connect() as db:
                    rows = db.execute(
                        """SELECT * FROM artifact_input_cache_entries
                           WHERE state='ready' AND refcount=0
                           ORDER BY last_accessed_at,manifest_sha256 LIMIT ?""",
                        (GC_BATCH_SIZE,),
                    ).fetchall()
                if not rows:
                    return freed
                for row in rows:
                    entry = self._entry(row)
                    if entry.ready_path is None:
                        raise ArtifactInputJournalError("input_cache_journal_drift")
                    self._delete_entry(entry)
                    freed += entry.unpacked_size_bytes

    def _delete_entry(self, entry: CacheEntry) -> None:
        assert entry.ready_path is not None
        tombstone = self.cas_root / ".acceptance-eviction" / entry.manifest_sha256.removeprefix("sha256:")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                "UPDATE artifact_input_cache_entries SET state='deleting',ready_path=NULL,ready_sha256=NULL,updated_at=? "
                "WHERE manifest_sha256=? AND state='ready' AND refcount=0",
                (self._now(), entry.manifest_sha256),
            ).rowcount
            if changed != 1:
                db.rollback()
                return
            db.commit()
        os.replace(entry.ready_path, tombstone)
        _fsync_dir(entry.ready_path.parent)
        _fsync_dir(tombstone.parent)
        _remove_tree_no_links(tombstone)
        with self._connect() as db:
            db.execute(
                "DELETE FROM artifact_input_cache_entries WHERE manifest_sha256=? AND state='deleting'",
                (entry.manifest_sha256,),
            )

    def reconcile(self) -> None:
        """Remove abandoned reservations and resume durable deleting operations."""

        cutoff = (datetime.now(UTC) - PARTIAL_RETENTION).isoformat()
        quarantine_cutoff = (datetime.now(UTC) - QUARANTINE_RETENTION).isoformat()
        with self._lock, self._connect() as db:
            db.execute(
                "DELETE FROM artifact_input_cache_entries WHERE state='materializing' AND updated_at<?",
                (cutoff,),
            )
            db.execute(
                "DELETE FROM artifact_input_cache_entries WHERE state='quarantined' AND updated_at<?",
                (quarantine_cutoff,),
            )
            deleting = db.execute(
                "SELECT manifest_sha256 FROM artifact_input_cache_entries WHERE state='deleting'"
            ).fetchall()
        for row in deleting:
            digest = str(row["manifest_sha256"])
            tombstone = self.cas_root / ".acceptance-eviction" / digest.removeprefix("sha256:")
            ready = self.ready_path(digest)
            if ready.exists() and not tombstone.exists():
                os.replace(ready, tombstone)
                _fsync_dir(ready.parent)
                _fsync_dir(tombstone.parent)
            elif ready.exists() and tombstone.exists():
                raise ArtifactInputJournalError("input_cache_duplicate_deleting_tree")
            if tombstone.exists():
                _remove_tree_no_links(tombstone)
            with self._connect() as db:
                db.execute(
                    "DELETE FROM artifact_input_cache_entries WHERE manifest_sha256=? AND state='deleting'",
                    (digest,),
                )
        with self._connect() as db:
            operations = db.execute(
                "SELECT * FROM acceptance_eviction_operations WHERE state='deleting'"
            ).fetchall()
        for operation in operations:
            self._finish_eviction_operation(operation)
        sha_root = self.cas_root / "sha256"
        if sha_root.exists():
            for candidate in sha_root.glob("[0-9a-f][0-9a-f]/*"):
                if candidate.is_symlink() or not candidate.is_dir():
                    raise ArtifactInputJournalError("input_cache_tree_is_not_directory")
                digest = f"sha256:{candidate.name}"
                entry = self.get_entry(digest)
                if entry is None or entry.state != "ready":
                    _remove_tree_no_links(candidate)
        self._reap_filesystem_namespace(
            self.cas_root / ".partial", older_than=PARTIAL_RETENTION
        )
        self._reap_filesystem_namespace(
            self.cas_root / ".quarantine", older_than=QUARANTINE_RETENTION
        )

    def _reap_filesystem_namespace(self, root: Path, *, older_than: timedelta) -> None:
        cutoff = datetime.now(UTC).timestamp() - older_than.total_seconds()
        for candidate in root.iterdir():
            info = candidate.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise ArtifactInputJournalError("input_cache_tree_is_not_directory")
            if info.st_mtime < cutoff:
                _remove_tree_no_links(candidate)

    def _execute_acceptance_eviction(
        self, grant: AcceptanceEvictionGrantV1
    ) -> AcceptanceEvictionResultV1:
        key = (str(grant.authorization_id), grant.candidate_sha256, str(grant.worker_id))
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            replay = db.execute(
                "SELECT * FROM acceptance_eviction_operations WHERE authorization_id=? AND candidate_sha256=? AND worker_id=?",
                key,
            ).fetchone()
            request_bytes = canonical_document(grant.ordered_manifest_sha256s)
            if replay is not None:
                if bytes(replay["ordered_manifest_sha256s_json"]) != request_bytes:
                    db.rollback()
                    raise ArtifactInputJournalError("idempotency_conflict")
                if replay["state"] != "complete":
                    db.commit()
                    return self._finish_eviction_operation(replay)
                result = AcceptanceEvictionResultV1.model_validate_json(replay["result_json"])
                db.commit()
                return result
            entries: list[AcceptanceEvictionEntryV1] = []
            ready_entries: list[CacheEntry] = []
            for manifest in grant.ordered_manifest_sha256s:
                row = db.execute(
                    "SELECT * FROM artifact_input_cache_entries WHERE manifest_sha256=?",
                    (manifest,),
                ).fetchone()
                if row is None:
                    entries.append(
                        AcceptanceEvictionEntryV1(
                            manifest_sha256=manifest, pre_state="absent", freed_bytes=0
                        )
                    )
                    continue
                entry = self._entry(row)
                if entry.state != "ready" or entry.refcount != 0 or entry.ready_path is None:
                    db.rollback()
                    raise ArtifactInputJournalError("acceptance_eviction_precondition")
                ready_entries.append(entry)
                entries.append(
                    AcceptanceEvictionEntryV1(
                        manifest_sha256=manifest,
                        pre_state="ready",
                        freed_bytes=entry.unpacked_size_bytes,
                    )
                )
            entries.sort(key=lambda item: item.manifest_sha256.encode())
            now = self._now()
            db.execute(
                """INSERT INTO acceptance_eviction_operations
                   (authorization_id,candidate_sha256,worker_id,command_id,
                    ordered_manifest_sha256s_json,entries_json,state,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (*key, str(grant.command_id), request_bytes, canonical_document(entries), "deleting", now),
            )
            for entry in ready_entries:
                db.execute(
                    "UPDATE artifact_input_cache_entries SET state='deleting',ready_path=NULL,ready_sha256=NULL,updated_at=? "
                    "WHERE manifest_sha256=? AND state='ready' AND refcount=0",
                    (now, entry.manifest_sha256),
                )
            db.commit()
        for entry in ready_entries:
            assert entry.ready_path is not None
            tombstone = self.cas_root / ".acceptance-eviction" / entry.manifest_sha256.removeprefix("sha256:")
            os.replace(entry.ready_path, tombstone)
            _fsync_dir(entry.ready_path.parent)
            _fsync_dir(tombstone.parent)
        with self._connect() as db:
            operation = db.execute(
                "SELECT * FROM acceptance_eviction_operations WHERE authorization_id=? AND candidate_sha256=? AND worker_id=?",
                key,
            ).fetchone()
        assert operation is not None
        return self._finish_eviction_operation(operation)

    def _finish_eviction_operation(
        self, operation: sqlite3.Row
    ) -> AcceptanceEvictionResultV1:
        ordered = tuple(json.loads(bytes(operation["ordered_manifest_sha256s_json"])))
        entries = [
            AcceptanceEvictionEntryV1.model_validate(value)
            for value in json.loads(bytes(operation["entries_json"]))
        ]
        for entry in entries:
            if entry.pre_state != "ready":
                continue
            ready = self.ready_path(entry.manifest_sha256)
            tombstone = self.cas_root / ".acceptance-eviction" / entry.manifest_sha256.removeprefix("sha256:")
            if ready.exists() and not tombstone.exists():
                os.replace(ready, tombstone)
                _fsync_dir(ready.parent)
                _fsync_dir(tombstone.parent)
            elif ready.exists() and tombstone.exists():
                raise ArtifactInputJournalError("input_cache_duplicate_deleting_tree")
            if tombstone.exists():
                _remove_tree_no_links(tombstone)
        finished_at = datetime.now(UTC)
        evicted_count = sum(entry.pre_state == "ready" for entry in entries)
        result = AcceptanceEvictionResultV1(
            schema_version="loom.acceptance-eviction-result.v1",
            authorization_id=UUID(str(operation["authorization_id"])),
            candidate_sha256=str(operation["candidate_sha256"]),
            worker_id=UUID(str(operation["worker_id"])),
            ordered_manifest_sha256s=list(ordered),
            entries=entries,
            evicted_count=evicted_count,
            status="already_absent" if evicted_count == 0 else "evicted",
            absence_verified=True,
            finished_at=finished_at,
        )
        result_bytes = canonical_document(result.model_dump(mode="json"))
        result_sha256 = digest_bytes(result_bytes)
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for manifest in ordered:
                db.execute(
                    "DELETE FROM artifact_input_cache_entries WHERE manifest_sha256=? AND state='deleting'",
                    (manifest,),
                )
                if self.ready_path(manifest).exists():
                    db.rollback()
                    raise ArtifactInputJournalError("acceptance_eviction_absence_drift")
            db.execute(
                """UPDATE acceptance_eviction_operations SET state='complete',result_json=?,
                   result_sha256=?,finished_at=? WHERE authorization_id=? AND candidate_sha256=? AND worker_id=?""",
                (
                    result_bytes,
                    result_sha256,
                    finished_at.isoformat(),
                    operation["authorization_id"],
                    operation["candidate_sha256"],
                    operation["worker_id"],
                ),
            )
            db.execute(
                "INSERT OR IGNORE INTO acceptance_eviction_audit VALUES(?,?,?,?,?)",
                (
                    operation["authorization_id"],
                    operation["candidate_sha256"],
                    operation["worker_id"],
                    result_sha256,
                    finished_at.isoformat(),
                ),
            )
            db.commit()
        return result

    def ready_path(self, manifest_sha256: str) -> Path:
        digest = manifest_sha256.removeprefix("sha256:")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ArtifactInputJournalError("invalid_manifest_digest")
        return self.cas_root / "sha256" / digest[:2] / digest


@dataclass(frozen=True)
class AcceptanceEvictionCommandHandler:
    """The sole private fixed-candidate worker eviction command surface."""

    journal: ArtifactInputJournal
    authority: AcceptanceEvictionAuthorityV1

    async def evict_acceptance_entries(
        self,
        *,
        authorization_id: UUID,
        candidate_sha256: str,
        worker_id: UUID,
        ordered_manifest_sha256s: tuple[str, str, str, str, str],
    ) -> AcceptanceEvictionResultV1:
        grant = await self.authority.authorize(
            authorization_id=authorization_id,
            candidate_sha256=candidate_sha256,
            worker_id=worker_id,
            ordered_manifest_sha256s=ordered_manifest_sha256s,
        )
        if (
            grant.authorization_id != authorization_id
            or grant.candidate_sha256 != candidate_sha256
            or grant.worker_id != worker_id
            or tuple(grant.ordered_manifest_sha256s) != ordered_manifest_sha256s
        ):
            raise ArtifactInputJournalError("acceptance_eviction_grant_drift")
        return self.journal._execute_acceptance_eviction(grant)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _remove_tree_no_links(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink() or not path.is_dir():
        raise ArtifactInputJournalError("input_cache_tree_is_not_directory")
    for root, directories, files in os.walk(path, topdown=False, followlinks=False):
        root_path = Path(root)
        os.chmod(root_path, 0o700, follow_symlinks=False)
        for name in files:
            child = root_path / name
            if child.is_symlink():
                raise ArtifactInputJournalError("input_cache_tree_contains_link")
            child.unlink()
        for name in directories:
            child = root_path / name
            if child.is_symlink():
                raise ArtifactInputJournalError("input_cache_tree_contains_link")
            child.rmdir()
        _fsync_dir(root_path)
    path.rmdir()
    _fsync_dir(path.parent)


__all__ = [
    "AcceptanceEvictionAuthorityV1",
    "AcceptanceEvictionCommandHandler",
    "ArtifactInputJournal",
    "ArtifactInputJournalError",
    "CacheCapacitySnapshot",
    "CacheEntry",
    "allocatable_capacity",
    "validate_registration_capacity",
]
