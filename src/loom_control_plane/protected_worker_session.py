"""Least-privileged bridge for protected staging worker sessions."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request
from pydantic import JsonValue, ValidationError
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError, DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.auth import AuthContext, verify_bearer_token
from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    AtomicTrialSubmissionReceiptV1,
    AtomicTrialSubmissionV1,
    ProtectedRuntimeTrialReadinessReceiptV1,
)
from loom_capacity_agent.submission_store import (
    CapacityTrialSubmissionError,
    CapacityTrialSubmissionStore,
)


class ProtectedWorkerSessionRejected(RuntimeError):  # noqa: N818
    """The protected capacity guard rejected a worker registration or session."""


class ProtectedWorkerSessionAuthenticationRejected(ProtectedWorkerSessionRejected):
    """The protected capacity guard rejected worker claim authentication."""


class ProtectedWorkerRuntimeConfigurationError(RuntimeError):
    """The protected runtime-role database material is unavailable or unsafe."""


class ProtectedTrialSubmissionError(RuntimeError):
    """The protected capacity guard rejected a trial submission."""


class ProtectedTrialSubmissionConflictError(ProtectedTrialSubmissionError):
    """An idempotency key is already bound to a different submission."""


MAX_RUNTIME_DATABASE_URL_BYTES = 4096
EXECUTOR_WORKER_CREDENTIAL_HEADER = "X-Loom-Executor-Worker-Credential"


def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        stat.S_IFMT(metadata.st_mode),
        stat.S_IMODE(metadata.st_mode),
        metadata.st_uid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _safe_runtime_url_file(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_nlink == 1
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and 0 < metadata.st_size <= MAX_RUNTIME_DATABASE_URL_BYTES
    )


def load_protected_worker_runtime_db_url(path: Path) -> URL:
    """Load a stable current-UID 0600 PostgreSQL URL without following links."""

    error = ProtectedWorkerRuntimeConfigurationError(
        "protected worker runtime database URL is unavailable"
    )
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise error
    try:
        before = path.lstat()
        parent_fd = os.open(
            path.parent,
            os.O_RDONLY
            | os.O_DIRECTORY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise error from exc
    try:
        if not _safe_runtime_url_file(before):
            raise error
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY
                | os.O_NONBLOCK
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise error from exc
        try:
            opened = os.fstat(descriptor)
            if not _safe_runtime_url_file(opened) or _file_identity(opened) != _file_identity(
                before
            ):
                raise error
            payload = bytearray()
            while len(payload) <= MAX_RUNTIME_DATABASE_URL_BYTES:
                chunk = os.read(
                    descriptor,
                    min(4096, MAX_RUNTIME_DATABASE_URL_BYTES + 1 - len(payload)),
                )
                if not chunk:
                    break
                payload.extend(chunk)
            after = os.fstat(descriptor)
            try:
                current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise error from exc
            if (
                len(payload) != opened.st_size
                or _file_identity(after) != _file_identity(opened)
                or _file_identity(current) != _file_identity(opened)
            ):
                raise error
        finally:
            os.close(descriptor)
    finally:
        os.close(parent_fd)

    raw = bytes(payload)
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if not raw or b"\n" in raw or b"\r" in raw or b"\x00" in raw:
        raise error
    try:
        value = raw.decode("ascii")
        url = make_url(value)
    except (UnicodeDecodeError, ArgumentError) as exc:
        raise error from exc
    if (
        url.drivername not in {"postgresql+psycopg", "postgresql+asyncpg"}
        or not url.username
        or url.password is None
        or not url.host
        or not url.database
    ):
        raise error
    return url


@dataclass(frozen=True)
class ProtectedWorkerRegistration:
    worker_id: UUID
    worker_incarnation: UUID
    intent_id: UUID
    capability_snapshot_digest: str | None
    supported_work_kinds: tuple[str, ...]
    input_cache_capacity_bytes: int
    input_cache_reserved_bytes: int
    input_cache_ready_bytes: int
    slurm_gpu_allocation_evidence_digest: str | None
    heartbeat_interval_sec: int
    claim_poll_interval_sec: float
    drain_timeout_sec: int


@dataclass(frozen=True)
class ProtectedWorkerSession:
    worker_id: UUID
    worker_incarnation: UUID
    intent_id: UUID
    pool_name: str
    hostname: str
    candidate_sha: str
    slurm_job_id: str
    credential_sha256: str

    @property
    def credential_digest(self) -> bytes:
        return bytes.fromhex(self.credential_sha256)


_REGISTER = text(
    "SELECT loom_capacity_guard.register_staging_public_worker("
    ":credential, CAST(:projection AS jsonb))"
)
_ASSERT_SESSION = text(
    "SELECT loom_capacity_guard.assert_staging_worker_session(:worker_id, :credential)"
)
_CLAIM_ASSIGNED_TRIAL = text(
    "SELECT loom_capacity_guard.claim_staging_assigned_trial("
    ":worker_id, :credential, CAST(:claim_request AS jsonb))"
)
_RETRY_CLAIMED_TRIAL = text(
    "SELECT loom_capacity_guard.retry_staging_claimed_trial("
    ":worker_id, :credential, CAST(:retry_request AS jsonb))"
)
_CURRENT_REGISTRATION = text("SELECT loom_capacity_guard.current_protected_runtime_registration()")


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError("protected worker guard returned a malformed response")
    return value


def _registration(value: object) -> ProtectedWorkerRegistration:
    payload = _mapping(value)
    try:
        return ProtectedWorkerRegistration(
            worker_id=UUID(str(payload["worker_id"])),
            worker_incarnation=UUID(str(payload["worker_incarnation"])),
            intent_id=UUID(str(payload["intent_id"])),
            capability_snapshot_digest=payload["capability_snapshot_digest"],
            supported_work_kinds=tuple(payload["supported_work_kinds"]),
            input_cache_capacity_bytes=int(payload["input_cache_capacity_bytes"]),
            input_cache_reserved_bytes=int(payload["input_cache_reserved_bytes"]),
            input_cache_ready_bytes=int(payload["input_cache_ready_bytes"]),
            slurm_gpu_allocation_evidence_digest=(payload["slurm_gpu_allocation_evidence_digest"]),
            heartbeat_interval_sec=int(payload["heartbeat_interval_sec"]),
            claim_poll_interval_sec=float(payload["claim_poll_interval_sec"]),
            drain_timeout_sec=int(payload["drain_timeout_sec"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("protected worker guard returned a malformed registration") from exc


def _session(value: object) -> ProtectedWorkerSession:
    payload = _mapping(value)
    try:
        credential_sha256 = str(payload["credential_sha256"])
        if len(credential_sha256) != 64:
            raise ValueError("credential digest length")
        bytes.fromhex(credential_sha256)
        return ProtectedWorkerSession(
            worker_id=UUID(str(payload["worker_id"])),
            worker_incarnation=UUID(str(payload["worker_incarnation"])),
            intent_id=UUID(str(payload["intent_id"])),
            pool_name=str(payload["pool_name"]),
            hostname=str(payload["hostname"]),
            candidate_sha=str(payload["candidate_sha"]),
            slurm_job_id=str(payload["slurm_job_id"]),
            credential_sha256=credential_sha256,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("protected worker guard returned a malformed session") from exc


class ProtectedWorkerSessionStore:
    """Call protected guard functions through the dedicated runtime role."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def current_registration(self) -> AgentRegistrationV1:
        """Load the exact guard-owned registration used to seal new demand."""

        try:
            async with self._session_factory() as session, session.begin():
                value = (await session.execute(_CURRENT_REGISTRATION)).scalar_one()
            if not isinstance(value, Mapping):
                raise ValueError("registration is not an object")
            return AgentRegistrationV1.model_validate_json(
                json.dumps(
                    dict(value),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                )
            )
        except (DBAPIError, ValidationError, ValueError) as exc:
            raise ProtectedTrialSubmissionError(
                "protected trial registration is unavailable"
            ) from exc

    async def submit_trial(
        self,
        *,
        registration: AgentRegistrationV1,
        submission: AtomicTrialSubmissionV1,
        public_requires_caps: Mapping[str, JsonValue],
    ) -> AtomicTrialSubmissionReceiptV1:
        """Create one inert public/protected projection through the runtime role."""

        try:
            async with self._session_factory() as session, session.begin():
                return await CapacityTrialSubmissionStore(
                    session,
                    registration=registration,
                ).create_runtime_initial_submission(
                    submission,
                    public_requires_caps=public_requires_caps,
                )
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "23505":
                raise ProtectedTrialSubmissionConflictError(
                    "protected trial idempotency conflict"
                ) from exc
            raise ProtectedTrialSubmissionError("protected trial submission rejected") from exc
        except CapacityTrialSubmissionError as exc:
            raise ProtectedTrialSubmissionError("protected trial submission rejected") from exc

    async def publish_trial_readiness(
        self,
        *,
        trial_id: UUID,
        protected_attempt_id: UUID,
    ) -> ProtectedRuntimeTrialReadinessReceiptV1:
        """Publish a runtime submission after guard-owned prerequisite checks."""

        try:
            registration = await self.current_registration()
            async with self._session_factory() as session, session.begin():
                return await CapacityTrialSubmissionStore(
                    session,
                    registration=registration,
                ).publish_runtime_submission_readiness(
                    trial_id=trial_id,
                    protected_attempt_id=protected_attempt_id,
                )
        except (DBAPIError, CapacityTrialSubmissionError) as exc:
            raise ProtectedTrialSubmissionError("protected trial readiness rejected") from exc

    async def register(
        self,
        *,
        worker_credential: str,
        projection: Mapping[str, object],
    ) -> ProtectedWorkerRegistration:
        try:
            async with self._session_factory() as session, session.begin():
                value = (
                    await session.execute(
                        _REGISTER,
                        {
                            "credential": worker_credential,
                            "projection": json.dumps(
                                dict(projection),
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        },
                    )
                ).scalar_one()
        except DBAPIError as exc:
            raise ProtectedWorkerSessionRejected("protected worker registration rejected") from exc
        return _registration(value)

    async def claim_assigned_trial(
        self,
        *,
        worker_id: UUID,
        worker_credential: str,
        claim_request: Mapping[str, object],
    ) -> Mapping[str, Any] | None:
        """Atomically consume one exact manager assignment and claim its public trial."""

        try:
            async with self._session_factory() as session, session.begin():
                value = (
                    await session.execute(
                        _CLAIM_ASSIGNED_TRIAL,
                        {
                            "worker_id": worker_id,
                            "credential": worker_credential,
                            "claim_request": json.dumps(
                                dict(claim_request),
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        },
                    )
                ).scalar_one()
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "42501":
                raise ProtectedWorkerSessionAuthenticationRejected(
                    "protected worker claim authentication rejected"
                ) from exc
            raise ProtectedWorkerSessionRejected("protected worker claim rejected") from exc
        if value is None:
            return None
        return _mapping(value)

    async def retry_claimed_trial(
        self,
        *,
        worker_id: UUID,
        worker_credential: str,
        retry_request: Mapping[str, object],
    ) -> Mapping[str, Any] | None:
        """Close one pre-start claim and create its next inert attempt."""

        try:
            async with self._session_factory() as session, session.begin():
                value = (
                    await session.execute(
                        _RETRY_CLAIMED_TRIAL,
                        {
                            "worker_id": worker_id,
                            "credential": worker_credential,
                            "retry_request": json.dumps(
                                dict(retry_request),
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        },
                    )
                ).scalar_one()
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "42501":
                raise ProtectedWorkerSessionAuthenticationRejected(
                    "protected worker retry authentication rejected"
                ) from exc
            raise ProtectedWorkerSessionRejected("protected worker retry rejected") from exc
        if value is None:
            return None
        return _mapping(value)

    async def authenticate_session(
        self,
        *,
        worker_id: UUID,
        worker_credential: str,
    ) -> ProtectedWorkerSession:
        """Authenticate one request without retaining guard row locks."""

        try:
            async with self._session_factory() as session, session.begin():
                value = (
                    await session.execute(
                        _ASSERT_SESSION,
                        {
                            "worker_id": worker_id,
                            "credential": worker_credential,
                        },
                    )
                ).scalar_one()
        except DBAPIError as exc:
            raise ProtectedWorkerSessionRejected("protected worker session rejected") from exc
        return _session(value)

    @asynccontextmanager
    async def assert_session(
        self,
        *,
        worker_id: UUID,
        worker_credential: str,
    ) -> AsyncIterator[ProtectedWorkerSession]:
        async with self._session_factory() as session, session.begin():
            try:
                value = (
                    await session.execute(
                        _ASSERT_SESSION,
                        {
                            "worker_id": worker_id,
                            "credential": worker_credential,
                        },
                    )
                ).scalar_one()
            except DBAPIError as exc:
                raise ProtectedWorkerSessionRejected("protected worker session rejected") from exc
            yield _session(value)


@dataclass(frozen=True)
class ProtectedWorkerClaimContext:
    """Validated protected-claim inputs without an open database transaction."""

    worker_id: UUID
    worker_credential: str
    store: ProtectedWorkerSessionStore


def bind_protected_worker_auth(
    auth: AuthContext,
    protected_session: ProtectedWorkerSession | None,
) -> AuthContext:
    """Use the guard-bound digest for public worker ownership checks."""

    if protected_session is None:
        return auth
    return replace(auth, token_hash=protected_session.credential_digest)


async def _guard_request_worker(
    request: Request,
    *,
    worker_id: UUID,
    worker_credential: str | None,
) -> AsyncIterator[ProtectedWorkerSession | None]:
    store: ProtectedWorkerSessionStore | None = getattr(
        request.app.state,
        "protected_worker_session_store",
        None,
    )
    if store is None:
        if worker_credential is not None:
            raise HTTPException(
                status_code=503,
                detail="protected worker runtime unavailable",
            )
        yield None
        return
    if worker_credential is None:
        raise HTTPException(
            status_code=401,
            detail="protected worker session rejected",
        )
    try:
        async with store.assert_session(
            worker_id=worker_id,
            worker_credential=worker_credential,
        ) as protected_session:
            yield protected_session
    except ProtectedWorkerSessionRejected as exc:
        raise HTTPException(
            status_code=401,
            detail="protected worker session rejected",
        ) from exc


async def protected_body_worker_session(
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Guard a request whose JSON object contains ``worker_id``."""

    store = getattr(request.app.state, "protected_worker_session_store", None)
    if store is None and worker_credential is None:
        yield None
        return
    if store is None:
        raise HTTPException(status_code=503, detail="protected worker runtime unavailable")
    if worker_credential is None:
        raise HTTPException(status_code=401, detail="protected worker session rejected")
    try:
        payload = await request.json()
        worker_id = UUID(str(payload["worker_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="worker_id required") from exc
    async for protected_session in _guard_request_worker(
        request,
        worker_id=worker_id,
        worker_credential=worker_credential,
    ):
        request.state.protected_worker_session = protected_session
        yield protected_session


async def protected_body_worker_claim(
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> ProtectedWorkerClaimContext | None:
    """Validate protected claim inputs without opening a runtime transaction."""

    store: ProtectedWorkerSessionStore | None = getattr(
        request.app.state,
        "protected_worker_session_store",
        None,
    )
    if store is None and worker_credential is None:
        return None
    if store is None:
        raise HTTPException(status_code=503, detail="protected worker runtime unavailable")
    if worker_credential is None:
        raise HTTPException(status_code=401, detail="protected worker session rejected")
    try:
        payload = await request.json()
        worker_id = UUID(str(payload["worker_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="worker_id required") from exc
    return ProtectedWorkerClaimContext(
        worker_id=worker_id,
        worker_credential=worker_credential,
        store=store,
    )


async def protected_body_worker_state_session(
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Authenticate terminal reports without retaining a conflicting guard lock."""

    store: ProtectedWorkerSessionStore | None = getattr(
        request.app.state,
        "protected_worker_session_store",
        None,
    )
    if store is None and worker_credential is None:
        yield None
        return
    if store is None:
        raise HTTPException(status_code=503, detail="protected worker runtime unavailable")
    if worker_credential is None:
        raise HTTPException(status_code=401, detail="protected worker session rejected")
    try:
        payload = await request.json()
        worker_id = UUID(str(payload["worker_id"]))
        target_state = str(payload["state"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="state + worker_id required") from exc

    if target_state in {"succeeded", "failed", "cancelled"}:
        try:
            authenticated_session = await store.authenticate_session(
                worker_id=worker_id,
                worker_credential=worker_credential,
            )
        except ProtectedWorkerSessionRejected as exc:
            raise HTTPException(
                status_code=401,
                detail="protected worker session rejected",
            ) from exc
        request.state.protected_worker_session = authenticated_session
        yield authenticated_session
        return

    async for protected_session in _guard_request_worker(
        request,
        worker_id=worker_id,
        worker_credential=worker_credential,
    ):
        request.state.protected_worker_session = protected_session
        yield protected_session


async def protected_path_worker_session(
    worker_id: UUID,
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Guard a request whose path contains ``worker_id``."""

    async for protected_session in _guard_request_worker(
        request,
        worker_id=worker_id,
        worker_credential=worker_credential,
    ):
        request.state.protected_worker_session = protected_session
        yield protected_session


async def _lookup_worker_id(
    request: Request,
    *,
    table: str,
    identity_column: str,
    identity: UUID,
) -> UUID:
    if table not in {"trials", "execution_attempts"} or identity_column not in {
        "id",
    }:
        raise RuntimeError("protected worker lookup is not allowlisted")
    async with request.app.state.session_factory() as session:
        value = (
            await session.execute(
                text(f"SELECT worker_id FROM public.{table} WHERE {identity_column} = :identity"),
                {"identity": identity},
            )
        ).scalar_one_or_none()
    if value is None:
        raise HTTPException(status_code=401, detail="protected worker session rejected")
    return UUID(str(value))


async def protected_attempt_worker_session(
    attempt_id: UUID,
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Guard a claim-bound request using its assigned execution worker."""

    store = getattr(request.app.state, "protected_worker_session_store", None)
    if store is None and worker_credential is None:
        yield None
        return
    if store is None:
        raise HTTPException(status_code=503, detail="protected worker runtime unavailable")
    if worker_credential is None:
        raise HTTPException(status_code=401, detail="protected worker session rejected")
    worker_id = await _lookup_worker_id(
        request,
        table="execution_attempts",
        identity_column="id",
        identity=attempt_id,
    )
    async for protected_session in _guard_request_worker(
        request,
        worker_id=worker_id,
        worker_credential=worker_credential,
    ):
        request.state.protected_worker_session = protected_session
        yield protected_session


async def protected_trial_worker_session(
    trial_id: UUID,
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Guard a worker-only request using its currently assigned trial worker."""

    store = getattr(request.app.state, "protected_worker_session_store", None)
    if store is None and worker_credential is None:
        yield None
        return
    if store is None:
        raise HTTPException(status_code=503, detail="protected worker runtime unavailable")
    if worker_credential is None:
        raise HTTPException(status_code=401, detail="protected worker session rejected")
    worker_id = await _lookup_worker_id(
        request,
        table="trials",
        identity_column="id",
        identity=trial_id,
    )
    async for protected_session in _guard_request_worker(
        request,
        worker_id=worker_id,
        worker_credential=worker_credential,
    ):
        request.state.protected_worker_session = protected_session
        yield protected_session


async def protected_query_worker_session(
    worker_id: UUID,
    request: Request,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Guard a request whose query string contains ``worker_id``."""

    async for protected_session in _guard_request_worker(
        request,
        worker_id=worker_id,
        worker_credential=worker_credential,
    ):
        request.state.protected_worker_session = protected_session
        yield protected_session


def bind_request_protected_worker_auth(request: Request, auth: AuthContext) -> AuthContext:
    return bind_protected_worker_auth(
        auth,
        getattr(request.state, "protected_worker_session", None),
    )


async def protected_worker_principal(
    request: Request,
    authorization: str | None = Header(default=None),
) -> AuthContext | None:
    """Authenticate a caller before conditionally applying a worker session fence."""

    async with request.app.state.session_factory() as session:
        return await verify_bearer_token(session, authorization)


ProtectedWorkerPrincipal = Annotated[
    AuthContext | None,
    Depends(protected_worker_principal),
]


async def protected_principal_trial_session(
    trial_id: UUID,
    request: Request,
    principal: ProtectedWorkerPrincipal,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Fence a path-bound trial only when its authenticated caller is a worker."""

    if principal is None or principal.type != "worker":
        yield None
        return
    async for protected_session in protected_trial_worker_session(
        trial_id,
        request,
        worker_credential,
    ):
        yield protected_session


async def protected_principal_body_trial_session(
    request: Request,
    principal: ProtectedWorkerPrincipal,
    worker_credential: str | None = Header(
        default=None,
        alias=EXECUTOR_WORKER_CREDENTIAL_HEADER,
    ),
) -> AsyncIterator[ProtectedWorkerSession | None]:
    """Fence a body-bound trial only when its authenticated caller is a worker."""

    if principal is None or principal.type != "worker":
        yield None
        return
    try:
        payload = await request.json()
        trial_id = UUID(str(payload["trial_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="trial_id required") from exc
    async for protected_session in protected_trial_worker_session(
        trial_id,
        request,
        worker_credential,
    ):
        yield protected_session


ProtectedPrincipalTrialSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_principal_trial_session),
]
ProtectedPrincipalBodyTrialSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_principal_body_trial_session),
]


ProtectedBodyWorkerSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_body_worker_session),
]
ProtectedBodyWorkerStateSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_body_worker_state_session),
]
ProtectedBodyWorkerClaim = Annotated[
    ProtectedWorkerClaimContext | None,
    Depends(protected_body_worker_claim),
]
ProtectedPathWorkerSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_path_worker_session),
]
ProtectedAttemptWorkerSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_attempt_worker_session),
]
ProtectedTrialWorkerSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_trial_worker_session),
]
ProtectedQueryWorkerSession = Annotated[
    ProtectedWorkerSession | None,
    Depends(protected_query_worker_session),
]
