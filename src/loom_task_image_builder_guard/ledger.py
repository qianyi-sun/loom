"""Crash-persistent, nonsecret state for guarded builder allocations."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import cast
from uuid import UUID, uuid4

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.safeio import read_stable_file

LEDGER_SCHEMA = "loom.task-image-builder-node-guard-ledger/v1"
_DEFAULT_MAXIMUM_ENTRY_BYTES = 256 * 1024
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_JOB = re.compile(r"^[1-9][0-9]{0,31}$")
_REASON = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_STAGING = re.compile(
    r"^staging-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-([a-z0-9]{1,32})$"
)
_STATES = frozenset(
    {
        "intent",
        "challenge_pending",
        "challenged",
        "containment_pending",
        "attached",
        "projected",
        "exchanged",
        "terminal",
        "quarantined",
    }
)
_FIELDS = frozenset(
    {
        "schema",
        "grant_id",
        "request_id",
        "peer_pid",
        "job_id",
        "peer_executable_sha256",
        "batch_cgroup_relative",
        "state",
        "projection_request",
        "projection_request_sha256",
        "challenge",
        "challenge_sha256",
        "proof",
        "proof_sha256",
        "pin_path",
        "link_ids",
        "program_ids",
        "map_ids",
        "receipt_public_binding_sha256",
        "bootstrap_token_sha256",
        "exchange_id",
        "exchange_public_binding_sha256",
        "session_id",
        "session_public_binding_sha256",
        "session_token_sha256",
        "session_expires_at",
        "attestation_generation",
        "attestation_sha256",
        "attestation_expires_at",
        "pending_attestation",
        "pending_attestation_sha256",
        "terminal_reason",
        "quarantine_reason",
    }
)


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise GuardError("ledger_document_invalid") from None


def _uuid(value: object, *, code: str = "ledger_document_invalid") -> UUID:
    try:
        if not isinstance(value, str):
            raise ValueError
        parsed = UUID(value)
    except (ValueError, AttributeError):
        raise GuardError(code) from None
    if parsed.int == 0 or str(parsed) != value:
        raise GuardError(code)
    return parsed


def _digest(value: object, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None or value == "0" * 64:
        raise GuardError("ledger_document_invalid")
    return value


def _safe_path(value: object, *, absolute: bool) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "//" in value:
        raise GuardError("ledger_document_invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute() != absolute
        or (absolute and path == PurePosixPath("/"))
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in value.split("/")[1:])
    ):
        raise GuardError("ledger_document_invalid")
    return value


def _relative_cgroup(value: object) -> str:
    result = _safe_path(value, absolute=True)
    if not result.endswith("/step_batch/user/task_0"):
        raise GuardError("ledger_document_invalid")
    return result


def _ids(value: object) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise GuardError("ledger_document_invalid")
    items = tuple(value)
    if (
        len(items) > 64
        or any(type(item) is not int or not 1 <= item <= (1 << 32) - 1 for item in items)
        or items != tuple(sorted(set(items)))
    ):
        raise GuardError("ledger_document_invalid")
    return cast(tuple[int, ...], items)


def _timestamp(value: object, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str) or not value.endswith("Z") or not 20 <= len(value) <= 40:
        raise GuardError("ledger_document_invalid")
    return value


def _secret_free(value: object) -> None:
    if isinstance(value, str):
        if "loom_tibp_" in value or "loom_tibs_" in value:
            raise GuardError("ledger_secret_forbidden")
        return
    if isinstance(value, list):
        for child in value:
            _secret_free(child)
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise GuardError("ledger_document_invalid")
            if key in {"bootstrap_token", "session_token", "authorization"}:
                raise GuardError("ledger_secret_forbidden")
            _secret_free(child)
        return
    if value is not None and type(value) is not int:
        raise GuardError("ledger_document_invalid")


def _directory_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
    )


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    grant_id: UUID
    request_id: UUID
    state: str
    peer_pid: int
    job_id: str
    raw: bytes = field(repr=False)

    def document(self) -> dict[str, object]:
        try:
            value = json.loads(self.raw, object_pairs_hook=_pairs)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            raise GuardError("ledger_document_invalid") from None
        if not isinstance(value, dict):
            raise GuardError("ledger_document_invalid")
        return cast(dict[str, object], value)


class GuardLedger:
    """Atomic ledger confined to one pre-created root-owned directory."""

    def __init__(
        self,
        root: Path,
        maximum_entries: int,
        *,
        maximum_entry_bytes: int = _DEFAULT_MAXIMUM_ENTRY_BYTES,
        trusted_uid: int = 0,
        trusted_gid: int = 0,
    ) -> None:
        if (
            not isinstance(root, Path)
            or not root.is_absolute()
            or type(maximum_entries) is not int
            or not 1 <= maximum_entries <= 4096
            or type(maximum_entry_bytes) is not int
            or not 1024 <= maximum_entry_bytes <= 1024 * 1024
        ):
            raise GuardError("ledger_arguments_invalid")
        descriptor: int | None = None
        try:
            lexical = os.lstat(root)
            if root.resolve(strict=True) != root:
                raise GuardError("ledger_root_invalid")
            descriptor = os.open(
                root,
                os.O_RDONLY
                | os.O_CLOEXEC
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(descriptor)
            if (
                _directory_identity(lexical) != _directory_identity(opened)
                or not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != trusted_uid
                or opened.st_gid != trusted_gid
                or stat.S_IMODE(opened.st_mode) != 0o700
            ):
                raise GuardError("ledger_root_invalid")
        except GuardError:
            if descriptor is not None:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise GuardError("ledger_root_invalid") from exc
        self.root = root
        self.maximum_entries = maximum_entries
        self.maximum_entry_bytes = maximum_entry_bytes
        self.trusted_uid = trusted_uid
        self.trusted_gid = trusted_gid
        self._descriptor = descriptor
        self._root_identity = _directory_identity(opened)
        self._closed = False

    @staticmethod
    def document_sha256(value: object) -> str:
        _secret_free(value)
        return hashlib.sha256(_canonical(value)).hexdigest()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        os.close(self._descriptor)

    def _assert_root(self) -> None:
        if self._closed:
            raise GuardError("ledger_closed")
        try:
            current = os.fstat(self._descriptor)
        except OSError as exc:
            raise GuardError("ledger_root_changed") from exc
        if _directory_identity(current) != self._root_identity:
            raise GuardError("ledger_root_changed")

    @staticmethod
    def _filename(grant_id: UUID) -> str:
        if not isinstance(grant_id, UUID) or grant_id.int == 0:
            raise GuardError("ledger_grant_invalid")
        return f"{grant_id}.json"

    def _names(self) -> tuple[str, ...]:
        self._assert_root()
        try:
            names = tuple(sorted(os.listdir(self._descriptor)))
        except OSError as exc:
            raise GuardError("ledger_inventory_ambiguous") from exc
        canonical: list[str] = []
        for name in names:
            if _STAGING.fullmatch(name) is not None:
                raise GuardError("ledger_inventory_ambiguous")
            if not name.endswith(".json"):
                raise GuardError("ledger_inventory_ambiguous")
            try:
                parsed = UUID(name[:-5])
            except ValueError:
                raise GuardError("ledger_inventory_ambiguous") from None
            if parsed.int == 0 or f"{parsed}.json" != name:
                raise GuardError("ledger_inventory_ambiguous")
            canonical.append(name)
        if len(canonical) > self.maximum_entries:
            raise GuardError("ledger_capacity_exhausted")
        return tuple(canonical)

    def _parse(self, raw: bytes, *, expected_grant: UUID) -> LedgerEntry:
        if not raw or len(raw) > self.maximum_entry_bytes or not raw.endswith(b"\n"):
            raise GuardError("ledger_file_invalid")
        if b"loom_tibp_" in raw or b"loom_tibs_" in raw:
            raise GuardError("ledger_secret_forbidden")
        try:
            value = json.loads(raw, object_pairs_hook=_pairs)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            raise GuardError("ledger_file_invalid") from None
        if not isinstance(value, dict) or set(value) != _FIELDS:
            raise GuardError("ledger_document_invalid")
        document = cast(dict[str, object], value)
        _secret_free(document)
        if _canonical(document) + b"\n" != raw or document["schema"] != LEDGER_SCHEMA:
            raise GuardError("ledger_document_invalid")
        grant = _uuid(document["grant_id"])
        request = _uuid(document["request_id"])
        if grant != expected_grant:
            raise GuardError("ledger_document_invalid")
        peer_pid = document["peer_pid"]
        job_id = document["job_id"]
        state = document["state"]
        if (
            type(peer_pid) is not int
            or not 1 <= peer_pid <= (1 << 63) - 1
            or not isinstance(job_id, str)
            or _JOB.fullmatch(job_id) is None
            or state not in _STATES
        ):
            raise GuardError("ledger_document_invalid")
        _digest(document["peer_executable_sha256"])
        _relative_cgroup(document["batch_cgroup_relative"])
        for name in (
            "projection_request_sha256",
            "challenge_sha256",
            "proof_sha256",
            "receipt_public_binding_sha256",
            "bootstrap_token_sha256",
            "exchange_public_binding_sha256",
            "session_public_binding_sha256",
            "session_token_sha256",
            "attestation_sha256",
        ):
            _digest(document[name], optional=True)
        for value_name, digest_name in (
            ("projection_request", "projection_request_sha256"),
            ("challenge", "challenge_sha256"),
            ("proof", "proof_sha256"),
            ("pending_attestation", "pending_attestation_sha256"),
        ):
            value = document[value_name]
            recorded_digest = document[digest_name]
            if (value is None) != (recorded_digest is None) or (
                value is not None
                and (
                    not isinstance(value, dict)
                    or hashlib.sha256(_canonical(value)).hexdigest() != recorded_digest
                )
            ):
                raise GuardError("ledger_document_invalid")
        for name in ("link_ids", "program_ids", "map_ids"):
            _ids(document[name])
        projection = document["projection_request"]
        challenge = document["challenge"]
        proof = document["proof"]
        if projection is not None and (
            not isinstance(projection, dict)
            or _uuid(projection.get("grant_id")) != grant
            or _uuid(projection.get("request_id")) != request
            or projection.get("slurm_job_id") != job_id
            or projection.get("supervisor_pid") != peer_pid
            or projection.get("supervisor_executable_sha256")
            != document["peer_executable_sha256"]
        ):
            raise GuardError("ledger_document_invalid")
        if challenge is not None and (
            not isinstance(challenge, dict)
            or _uuid(challenge.get("grant_id")) != grant
            or _uuid(challenge.get("request_id")) != request
            or challenge.get("request_sha256") != document["projection_request_sha256"]
        ):
            raise GuardError("ledger_document_invalid")
        if proof is not None:
            attachment = proof.get("attachment") if isinstance(proof, dict) else None
            if (
                not isinstance(proof, dict)
                or _uuid(proof.get("grant_id")) != grant
                or _uuid(proof.get("request_id")) != request
                or proof.get("request_sha256") != document["projection_request_sha256"]
                or not isinstance(challenge, dict)
                or proof.get("challenge_nonce") != challenge.get("challenge_nonce")
                or not isinstance(attachment, dict)
                or any(
                    attachment.get(name) != document[name]
                    for name in ("link_ids", "program_ids", "map_ids")
                )
            ):
                raise GuardError("ledger_document_invalid")
        if document["pin_path"] is not None:
            _safe_path(document["pin_path"], absolute=True)
        for name in ("exchange_id", "session_id"):
            if document[name] is not None:
                _uuid(document[name])
        for name in ("session_expires_at", "attestation_expires_at"):
            _timestamp(document[name], optional=True)
        generation = document["attestation_generation"]
        if generation is not None and (
            type(generation) is not int or not 1 <= generation <= (1 << 63) - 1
        ):
            raise GuardError("ledger_document_invalid")
        pending = document["pending_attestation"]
        if pending is not None:
            pending_keys = {
                "schema_version",
                "attestation_id",
                "grant_id",
                "generation",
                "node_name",
                "node_boot_id",
                "slurm_cluster_id",
                "slurm_job_id",
                "cgroup_path",
                "cgroup_inode",
                "attachment",
                "issued_at",
                "expires_at",
            }
            if (
                state not in {"projected", "exchanged"}
                or not isinstance(pending, dict)
                or set(pending) != pending_keys
                or pending.get("schema_version") != 1
                or _uuid(pending.get("attestation_id")) .int == 0
                or _uuid(pending.get("grant_id")) != grant
                or type(generation) is not int
                or pending.get("generation") != generation + 1
                or not isinstance(proof, dict)
                or any(
                    pending.get(name) != proof.get(name)
                    for name in (
                        "node_name",
                        "node_boot_id",
                        "slurm_cluster_id",
                        "slurm_job_id",
                        "cgroup_path",
                        "cgroup_inode",
                        "attachment",
                    )
                )
            ):
                raise GuardError("ledger_document_invalid")
            _timestamp(pending.get("issued_at"))
            _timestamp(pending.get("expires_at"))
        for name in ("terminal_reason", "quarantine_reason"):
            reason = document[name]
            if reason is not None and (
                not isinstance(reason, str) or _REASON.fullmatch(reason) is None
            ):
                raise GuardError("ledger_document_invalid")
        self._validate_state(document)
        return LedgerEntry(grant, request, state, peer_pid, job_id, raw)

    @staticmethod
    def _validate_state(document: dict[str, object]) -> None:
        state = document["state"]
        if state == "intent" and any(
            document[name] is not None
            for name in ("projection_request", "challenge", "proof", "pin_path")
        ):
            raise GuardError("ledger_document_invalid")
        if state == "challenge_pending" and (
            document["projection_request"] is None
            or document["projection_request_sha256"] is None
            or document["challenge"] is not None
            or document["challenge_sha256"] is not None
            or document["proof"] is not None
            or document["pin_path"] is not None
        ):
            raise GuardError("ledger_document_invalid")
        if state in {
            "challenged",
            "containment_pending",
            "attached",
            "projected",
            "exchanged",
        } and (
            document["projection_request"] is None or document["challenge"] is None
        ):
            raise GuardError("ledger_document_invalid")
        if state in {"attached", "projected", "exchanged"} and (
            document["proof"] is None
            or document["pin_path"] is None
            or not document["link_ids"]
            or not document["program_ids"]
            or not document["map_ids"]
        ):
            raise GuardError("ledger_document_invalid")
        if state in {"projected", "exchanged"} and (
            document["receipt_public_binding_sha256"] is None
            or document["bootstrap_token_sha256"] is None
        ):
            raise GuardError("ledger_document_invalid")
        if state == "exchanged" and (
            document["exchange_id"] is None or document["session_id"] is None
        ):
            raise GuardError("ledger_document_invalid")
        if state == "terminal" and document["terminal_reason"] is None:
            raise GuardError("ledger_document_invalid")
        if state == "quarantined" and document["quarantine_reason"] is None:
            raise GuardError("ledger_document_invalid")

    def _read_name(self, name: str) -> LedgerEntry:
        grant = UUID(name[:-5])
        try:
            raw = read_stable_file(
                self.root / name,
                uid=self.trusted_uid,
                gid=self.trusted_gid,
                mode=0o600,
                maximum=self.maximum_entry_bytes,
            )
            return self._parse(raw, expected_grant=grant)
        except GuardError as exc:
            if exc.code in {"ledger_secret_forbidden", "ledger_document_invalid"}:
                raise
            raise GuardError("ledger_file_invalid") from None

    def load_all(self) -> tuple[LedgerEntry, ...]:
        return tuple(self._read_name(name) for name in self._names())

    def get(self, grant_id: UUID) -> LedgerEntry | None:
        filename = self._filename(grant_id)
        names = self._names()
        if filename not in names:
            return None
        return self._read_name(filename)

    def _write(self, document: dict[str, object], *, prior: LedgerEntry | None) -> LedgerEntry:
        _secret_free(document)
        raw = _canonical(document) + b"\n"
        if len(raw) > self.maximum_entry_bytes:
            raise GuardError("ledger_entry_too_large")
        grant = _uuid(document["grant_id"])
        self._parse(raw, expected_grant=grant)
        if prior is not None and prior.raw == raw:
            return prior
        filename = self._filename(grant)
        suffix = uuid4().hex
        staging = f"staging-{grant}-{suffix}"
        descriptor: int | None = None
        try:
            self._assert_root()
            descriptor = os.open(
                staging,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=self._descriptor,
            )
            os.fchmod(descriptor, 0o600)
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise GuardError("ledger_write_failed")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            if prior is not None:
                current = self._read_name(filename)
                if current.raw != prior.raw:
                    raise GuardError("ledger_replay_conflict")
            os.replace(
                staging,
                filename,
                src_dir_fd=self._descriptor,
                dst_dir_fd=self._descriptor,
            )
            os.fsync(self._descriptor)
            persisted = self._read_name(filename)
            if persisted.raw != raw:
                raise GuardError("ledger_write_failed")
            return persisted
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("ledger_write_failed") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    @staticmethod
    def _base_document(
        *,
        grant_id: UUID,
        request_id: UUID,
        peer_pid: int,
        job_id: str,
        peer_executable_sha256: str,
        batch_cgroup_relative: str,
    ) -> dict[str, object]:
        if (
            not isinstance(grant_id, UUID)
            or grant_id.int == 0
            or not isinstance(request_id, UUID)
            or request_id.int == 0
            or type(peer_pid) is not int
            or peer_pid <= 0
            or not isinstance(job_id, str)
            or _JOB.fullmatch(job_id) is None
        ):
            raise GuardError("ledger_intent_invalid")
        _digest(peer_executable_sha256)
        _relative_cgroup(batch_cgroup_relative)
        return {
            "schema": LEDGER_SCHEMA,
            "grant_id": str(grant_id),
            "request_id": str(request_id),
            "peer_pid": peer_pid,
            "job_id": job_id,
            "peer_executable_sha256": peer_executable_sha256,
            "batch_cgroup_relative": batch_cgroup_relative,
            "state": "intent",
            "projection_request": None,
            "projection_request_sha256": None,
            "challenge": None,
            "challenge_sha256": None,
            "proof": None,
            "proof_sha256": None,
            "pin_path": None,
            "link_ids": [],
            "program_ids": [],
            "map_ids": [],
            "receipt_public_binding_sha256": None,
            "bootstrap_token_sha256": None,
            "exchange_id": None,
            "exchange_public_binding_sha256": None,
            "session_id": None,
            "session_public_binding_sha256": None,
            "session_token_sha256": None,
            "session_expires_at": None,
            "attestation_generation": None,
            "attestation_sha256": None,
            "attestation_expires_at": None,
            "pending_attestation": None,
            "pending_attestation_sha256": None,
            "terminal_reason": None,
            "quarantine_reason": None,
        }

    def create_intent(
        self,
        *,
        grant_id: UUID,
        request_id: UUID,
        peer_pid: int,
        job_id: str,
        peer_executable_sha256: str,
        batch_cgroup_relative: str,
    ) -> LedgerEntry:
        document = self._base_document(
            grant_id=grant_id,
            request_id=request_id,
            peer_pid=peer_pid,
            job_id=job_id,
            peer_executable_sha256=peer_executable_sha256,
            batch_cgroup_relative=batch_cgroup_relative,
        )
        prior = self.get(grant_id)
        if prior is not None:
            existing = prior.document()
            if all(
                existing[name] == document[name]
                for name in (
                    "grant_id",
                    "peer_pid",
                    "job_id",
                    "peer_executable_sha256",
                    "batch_cgroup_relative",
                )
            ):
                return prior
            raise GuardError("ledger_replay_conflict")
        if len(self._names()) >= self.maximum_entries:
            raise GuardError("ledger_capacity_exhausted")
        return self._write(document, prior=None)

    @staticmethod
    def _update(prior: LedgerEntry) -> dict[str, object]:
        return prior.document()

    def record_projection_request(
        self,
        grant_id: UUID,
        *,
        projection_request: dict[str, object],
        projection_request_sha256: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None:
            raise GuardError("ledger_intent_missing")
        if self.document_sha256(projection_request) != projection_request_sha256:
            raise GuardError("ledger_digest_invalid")
        if (
            _uuid(projection_request.get("grant_id")) != grant_id
            or _uuid(projection_request.get("request_id")) != prior.request_id
        ):
            raise GuardError("ledger_binding_invalid")
        document = self._update(prior)
        expected = {
            "projection_request": projection_request,
            "projection_request_sha256": projection_request_sha256,
        }
        if prior.state != "intent":
            if all(document[name] == value for name, value in expected.items()):
                return prior
            raise GuardError("ledger_replay_conflict")
        document.update(expected)
        document["state"] = "challenge_pending"
        return self._write(document, prior=prior)

    def record_challenge(
        self,
        grant_id: UUID,
        *,
        projection_request: dict[str, object],
        projection_request_sha256: str,
        challenge: dict[str, object],
        challenge_sha256: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None:
            raise GuardError("ledger_intent_missing")
        if self.document_sha256(projection_request) != projection_request_sha256 or (
            self.document_sha256(challenge) != challenge_sha256
        ):
            raise GuardError("ledger_digest_invalid")
        if (
            _uuid(projection_request.get("grant_id")) != grant_id
            or _uuid(projection_request.get("request_id")) != prior.request_id
            or _uuid(challenge.get("grant_id")) != grant_id
            or _uuid(challenge.get("request_id")) != prior.request_id
            or challenge.get("request_sha256") != projection_request_sha256
        ):
            raise GuardError("ledger_binding_invalid")
        document = self._update(prior)
        expected = {
            "projection_request": projection_request,
            "projection_request_sha256": projection_request_sha256,
            "challenge": challenge,
            "challenge_sha256": challenge_sha256,
        }
        if prior.state not in {"intent", "challenge_pending"}:
            if all(document[name] == value for name, value in expected.items()):
                return prior
            raise GuardError("ledger_replay_conflict")
        document.update(expected)
        document["state"] = "challenged"
        return self._write(document, prior=prior)

    def record_containment_pending(self, grant_id: UUID) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None:
            raise GuardError("ledger_intent_missing")
        if prior.state == "containment_pending":
            return prior
        if prior.state != "challenged":
            raise GuardError("ledger_replay_conflict")
        document = self._update(prior)
        document["state"] = "containment_pending"
        return self._write(document, prior=prior)

    def record_attachment(
        self,
        grant_id: UUID,
        *,
        proof: dict[str, object],
        proof_sha256: str,
        pin_path: str,
        link_ids: tuple[int, ...],
        program_ids: tuple[int, ...],
        map_ids: tuple[int, ...],
        attestation_generation: int,
        attestation_sha256: str,
        attestation_expires_at: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None:
            raise GuardError("ledger_intent_missing")
        if self.document_sha256(proof) != proof_sha256:
            raise GuardError("ledger_digest_invalid")
        document = self._update(prior)
        challenge = document["challenge"]
        attachment = proof.get("attachment")
        if (
            _uuid(proof.get("grant_id")) != grant_id
            or _uuid(proof.get("request_id")) != prior.request_id
            or _uuid(proof.get("proof_id")) .int == 0
            or not isinstance(challenge, dict)
            or proof.get("request_sha256") != document["projection_request_sha256"]
            or proof.get("challenge_nonce") != challenge.get("challenge_nonce")
            or not isinstance(attachment, dict)
            or attachment.get("link_ids") != list(link_ids)
            or attachment.get("program_ids") != list(program_ids)
            or attachment.get("map_ids") != list(map_ids)
        ):
            raise GuardError("ledger_binding_invalid")
        safe_pin = _safe_path(pin_path, absolute=True)
        if PurePosixPath(safe_pin).name != str(grant_id):
            raise GuardError("ledger_binding_invalid")
        for digest in (attestation_sha256,):
            _digest(digest)
        if type(attestation_generation) is not int or attestation_generation != 1:
            raise GuardError("ledger_binding_invalid")
        _timestamp(attestation_expires_at)
        expected: dict[str, object] = {
            "proof": proof,
            "proof_sha256": proof_sha256,
            "pin_path": safe_pin,
            "link_ids": list(link_ids),
            "program_ids": list(program_ids),
            "map_ids": list(map_ids),
            "attestation_generation": attestation_generation,
            "attestation_sha256": attestation_sha256,
            "attestation_expires_at": attestation_expires_at,
        }
        for name in ("link_ids", "program_ids", "map_ids"):
            if not _ids(expected[name]):
                raise GuardError("ledger_binding_invalid")
        if prior.state not in {"challenged", "containment_pending"}:
            if all(document[name] == value for name, value in expected.items()):
                return prior
            raise GuardError("ledger_replay_conflict")
        document.update(expected)
        document["state"] = "attached"
        return self._write(document, prior=prior)

    def record_projection(
        self,
        grant_id: UUID,
        *,
        receipt_public_binding_sha256: str,
        bootstrap_token_sha256: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None:
            raise GuardError("ledger_intent_missing")
        _digest(receipt_public_binding_sha256)
        _digest(bootstrap_token_sha256)
        expected = {
            "receipt_public_binding_sha256": receipt_public_binding_sha256,
            "bootstrap_token_sha256": bootstrap_token_sha256,
        }
        document = self._update(prior)
        if prior.state != "attached":
            if all(document[name] == value for name, value in expected.items()):
                return prior
            raise GuardError("ledger_replay_conflict")
        document.update(expected)
        document["state"] = "projected"
        return self._write(document, prior=prior)

    def record_exchange(
        self,
        grant_id: UUID,
        *,
        exchange_id: UUID,
        exchange_public_binding_sha256: str,
        session_id: UUID,
        session_public_binding_sha256: str,
        session_token_sha256: str,
        session_expires_at: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None:
            raise GuardError("ledger_intent_missing")
        if any(not isinstance(item, UUID) or item.int == 0 for item in (exchange_id, session_id)):
            raise GuardError("ledger_binding_invalid")
        for digest in (
            exchange_public_binding_sha256,
            session_public_binding_sha256,
            session_token_sha256,
        ):
            _digest(digest)
        _timestamp(session_expires_at)
        expected: dict[str, object] = {
            "exchange_id": str(exchange_id),
            "exchange_public_binding_sha256": exchange_public_binding_sha256,
            "session_id": str(session_id),
            "session_public_binding_sha256": session_public_binding_sha256,
            "session_token_sha256": session_token_sha256,
            "session_expires_at": session_expires_at,
        }
        document = self._update(prior)
        if prior.state != "projected":
            if all(document[name] == value for name, value in expected.items()):
                return prior
            raise GuardError("ledger_replay_conflict")
        document.update(expected)
        document["state"] = "exchanged"
        return self._write(document, prior=prior)

    def record_attestation(
        self,
        grant_id: UUID,
        *,
        generation: int,
        attestation_sha256: str,
        expires_at: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None or prior.state not in {"projected", "exchanged"}:
            raise GuardError("ledger_attestation_invalid")
        _digest(attestation_sha256)
        _timestamp(expires_at)
        document = self._update(prior)
        current = document["attestation_generation"]
        if generation == current:
            if (
                document["attestation_sha256"] == attestation_sha256
                and document["attestation_expires_at"] == expires_at
            ):
                return prior
            raise GuardError("ledger_replay_conflict")
        if type(current) is not int or type(generation) is not int or generation != current + 1:
            raise GuardError("ledger_attestation_invalid")
        pending = document["pending_attestation"]
        if pending is not None and (
            not isinstance(pending, dict)
            or pending.get("generation") != generation
            or document["pending_attestation_sha256"] != attestation_sha256
            or pending.get("expires_at") != expires_at
        ):
            raise GuardError("ledger_replay_conflict")
        document["attestation_generation"] = generation
        document["attestation_sha256"] = attestation_sha256
        document["attestation_expires_at"] = expires_at
        document["pending_attestation"] = None
        document["pending_attestation_sha256"] = None
        return self._write(document, prior=prior)

    def record_pending_attestation(
        self,
        grant_id: UUID,
        *,
        generation: int,
        attestation: dict[str, object],
        attestation_sha256: str,
    ) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None or prior.state not in {"projected", "exchanged"}:
            raise GuardError("ledger_attestation_invalid")
        if (
            self.document_sha256(attestation) != attestation_sha256
            or _uuid(attestation.get("grant_id")) != grant_id
            or attestation.get("generation") != generation
        ):
            raise GuardError("ledger_binding_invalid")
        document = self._update(prior)
        current = document["attestation_generation"]
        if type(current) is not int or type(generation) is not int or generation != current + 1:
            raise GuardError("ledger_attestation_invalid")
        expected = {
            "pending_attestation": attestation,
            "pending_attestation_sha256": attestation_sha256,
        }
        if document["pending_attestation"] is not None:
            if all(document[name] == value for name, value in expected.items()):
                return prior
            raise GuardError("ledger_replay_conflict")
        document.update(expected)
        return self._write(document, prior=prior)

    def quarantine(self, grant_id: UUID, *, reason: str) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None or not isinstance(reason, str) or _REASON.fullmatch(reason) is None:
            raise GuardError("ledger_quarantine_invalid")
        document = self._update(prior)
        if prior.state == "quarantined":
            if document["quarantine_reason"] == reason:
                return prior
            raise GuardError("ledger_replay_conflict")
        document["state"] = "quarantined"
        document["quarantine_reason"] = reason
        document["pending_attestation"] = None
        document["pending_attestation_sha256"] = None
        return self._write(document, prior=prior)

    def mark_terminal(self, grant_id: UUID, *, reason: str) -> LedgerEntry:
        prior = self.get(grant_id)
        if prior is None or not isinstance(reason, str) or _REASON.fullmatch(reason) is None:
            raise GuardError("ledger_terminal_invalid")
        if prior.state == "quarantined":
            raise GuardError("ledger_quarantined")
        document = self._update(prior)
        if prior.state == "terminal":
            if document["terminal_reason"] == reason:
                return prior
            raise GuardError("ledger_replay_conflict")
        document["state"] = "terminal"
        document["terminal_reason"] = reason
        document["pending_attestation"] = None
        document["pending_attestation_sha256"] = None
        return self._write(document, prior=prior)

    def removable_pin_paths(self, grant_id: UUID) -> tuple[Path, ...]:
        entry = self.get(grant_id)
        if entry is None or entry.state != "terminal":
            return ()
        pin_path = entry.document()["pin_path"]
        return () if pin_path is None else (Path(cast(str, pin_path)),)

    def remove_terminal(self, grant_id: UUID, *, allocation_empty: bool) -> None:
        entry = self.get(grant_id)
        if entry is None or entry.state != "terminal":
            raise GuardError("ledger_not_terminal")
        if allocation_empty is not True:
            raise GuardError("ledger_allocation_not_empty")
        filename = self._filename(grant_id)
        try:
            os.unlink(filename, dir_fd=self._descriptor)
            os.fsync(self._descriptor)
        except OSError as exc:
            raise GuardError("ledger_write_failed") from exc


__all__ = ["LEDGER_SCHEMA", "GuardLedger", "LedgerEntry"]
