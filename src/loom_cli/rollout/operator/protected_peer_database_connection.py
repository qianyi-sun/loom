"""Bounded psql channel for installed protected code using existing peer authority.

This is a JSON-row transport, not a general DB-API driver or a release admission
boundary. The protected caller owns the exact process/target and durable operation.
No credential is created, accepted or exported. Forced process termination is
not proof of remote backend retirement; recovery must reconcile that identity.
"""

from __future__ import annotations

import base64
import json
import os
import re
import selectors
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from types import TracebackType
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_schema_inventory import (
    ApplicationSchemaInventoryError,
    require_application_event_trigger_policy,
)

_MAX_QUERY_BYTES = 1024 * 1024
_MAX_OUTPUT_BYTES = 16 * 1024 * 1024
_STOP_SECONDS = 5
_SQL_WHITESPACE = " \t\n\r\f\v"
_TRANSACTION_COMMANDS = frozenset(
    {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE", "PREPARE"}
)


class PeerDatabaseTransportError(RuntimeError):
    """Refused SQL or uncertain transport; never expose SQL or child diagnostics."""


def _identifier_continuation(char: str) -> bool:
    # PostgreSQL accepts every high-bit character in unquoted identifiers.
    return (char.isascii() and (char.isalnum() or char in "_$")) or ord(char) >= 128


def _single_statement(statement: str, *, standard_strings: bool) -> str:
    """Recognize boundaries only, not SQL grammar; reject psql meta commands.

    The backend's current string mode is observed on this same session before
    scanning. Dollar bodies, escaped/doubled quotes and nested comments do not
    end a statement. Unsupported/unclosed lexical forms are refused before I/O.
    """
    index = 0
    start: int | None = None
    end: int | None = None
    while index < len(statement):
        char = statement[index]
        if char in _SQL_WHITESPACE:
            index += 1
            continue
        if statement.startswith("--", index):
            newline = re.search(r"[\r\n]", statement[index + 2 :])
            index = len(statement) if newline is None else index + 3 + newline.start()
            continue
        if statement.startswith("/*", index):
            depth = 1
            index += 2
            while depth and index < len(statement):
                if statement.startswith("/*", index):
                    depth += 1
                    index += 2
                elif statement.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise PeerDatabaseTransportError("peer database requires a single statement")
            continue
        if end is not None or char == "\\":
            raise PeerDatabaseTransportError("peer database requires a single statement")
        if char == ";":
            end = index
            index += 1
            continue
        if start is None:
            start = index
        if char in {"'", '"'}:
            escaped = char == "'" and (
                not standard_strings
                or (
                    index > 0
                    and statement[index - 1] in "eE"
                    and (index < 2 or not _identifier_continuation(statement[index - 2]))
                )
            )
            quote = char
            index += 1
            while index < len(statement):
                if escaped and statement[index] == "\\":
                    index += 2
                elif statement[index] == quote:
                    index += 1
                    if index < len(statement) and statement[index] == quote:
                        index += 1
                        continue
                    break
                else:
                    index += 1
            else:
                raise PeerDatabaseTransportError("peer database requires a single statement")
            continue
        if char == "$" and (index == 0 or not _identifier_continuation(statement[index - 1])):
            match = re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", statement[index:])
            if match is None:
                raise PeerDatabaseTransportError("peer database requires a single statement")
            tag = match.group()
            close = statement.find(tag, index + len(tag))
            if close < 0:
                raise PeerDatabaseTransportError("peer database requires a single statement")
            index = close + len(tag)
            continue
        index += 1
    if start is None:
        raise PeerDatabaseTransportError("peer database requires a single statement")
    return statement[start:end].rstrip(_SQL_WHITESPACE)


def _row_query(statement: str) -> str:
    return (
        "SELECT pg_catalog.replace(pg_catalog.encode(pg_catalog.convert_to("
        "pg_catalog.row_to_json(peer_row)::pg_catalog.text,'UTF8'),'base64'),E'\\n','') "
        "FROM (\n" + statement + "\n) AS peer_row"
    )


@dataclass(frozen=True, slots=True)
class PeerBackendIdentity:
    system_identifier: str
    server_started_at: str
    backend_pid: int
    backend_started_at: str
    database_oid: int
    database: str
    session_user: str


class _Pairs(list[tuple[str, object]]):
    pass


def _json_value(value: object) -> object:
    if isinstance(value, _Pairs):
        return {key: _json_value(item) for key, item in value}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _decode_row(line: bytes) -> tuple[object, ...]:
    try:
        row = json.loads(base64.b64decode(line, validate=True), object_pairs_hook=_Pairs)
        if not isinstance(row, _Pairs):
            raise ValueError("row shape")
        return tuple(_json_value(value) for _, value in row)
    except (ValueError, UnicodeError, RecursionError):
        raise PeerDatabaseTransportError("peer database row framing failed") from None


@dataclass
class _Info:
    server_version: int = 0
    transaction_status: TransactionStatus = TransactionStatus.IDLE


class _Rows:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = iter(rows)

    def fetchone(self) -> tuple[object, ...] | None:
        return next(self._rows, None)

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class PeerDatabaseConnection:
    """Keep JSON-safe queries, SQL changes and savepoints on one psql backend.

    ``process`` must run the caller's admitted fixed peer psql command with
    -qAtX, stdin/stdout/stderr pipes and no inherited psqlrc. Each exchange
    explicitly disables ON_ERROR_STOP and ON_ERROR_ROLLBACK before its SQL.
    Caller-selected URLs, credentials and SQL parameter interpolation are absent.
    All value substitution uses already-composed public psycopg.sql objects.

    The caller must serialize administrator/event-trigger DDL. On PostgreSQL 17
    its admitted command MUST set event_triggers=off in libpq startup options:
    LOGIN triggers precede the first query, so observing this setting afterward
    cannot retroactively prove startup safety for an arbitrary process. We refuse
    every existing event trigger (including disabled policies), then restore
    ordinary DDL event handling before returning a usable connection.
    """

    def __init__(
        self, process: subprocess.Popen[bytes], *, query_timeout_seconds: float = 30
    ) -> None:
        self._process = process
        self._closed = False
        self._poisoned = False
        self._depth = 0
        self._info = _Info()
        self._query_timeout = query_timeout_seconds
        self._session_deadline = time.monotonic() + 300
        try:
            if not 0 < query_timeout_seconds <= 60:
                raise ValueError("peer database query bound is invalid")
            if process.stdin is None or process.stdout is None or process.stderr is None:
                raise ValueError("peer database process pipes are missing")
            for stream in (process.stdin, process.stdout, process.stderr):
                os.set_blocking(stream.fileno(), False)
            for setting in (
                "SET statement_timeout='30s'",
                "SET idle_in_transaction_session_timeout='30s'",
                "SET search_path=pg_catalog,pg_temp",
                "SET standard_conforming_strings=on",
                "SET DateStyle='ISO, YMD'",
            ):
                self._exchange(setting, returns_rows=False)
            version = self.execute(
                "SELECT pg_catalog.current_setting('server_version_num')::integer"
            ).fetchone()
            if version is None or type(version[0]) is not int or version[0] // 10000 not in {16, 17}:
                raise PeerDatabaseTransportError("peer database requires PostgreSQL 16 or 17")
            self._info.server_version = version[0]
            if version[0] // 10000 == 17 and self.execute(
                "SELECT setting,source FROM pg_catalog.pg_settings WHERE name='event_triggers'"
            ).fetchone() != ("off", "client"):
                raise PeerDatabaseTransportError("peer database startup event-trigger policy is invalid")
            try:
                require_application_event_trigger_policy(self)
            except ApplicationSchemaInventoryError:
                raise PeerDatabaseTransportError("peer database event trigger policy is not admitted") from None
            if version[0] // 10000 == 17:
                self._exchange("SET event_triggers=on", returns_rows=False)
                if self.execute(
                    "SELECT pg_catalog.current_setting('event_triggers')"
                ).fetchone() != ("on",):
                    raise PeerDatabaseTransportError("peer database event trigger handling is unavailable")
            identity = self.execute(
                "SELECT s.system_identifier::pg_catalog.text, pg_catalog.pg_postmaster_start_time()::pg_catalog.text, "
                "a.pid, a.backend_start::pg_catalog.text, a.datid::pg_catalog.int8, a.datname, a.usename "
                "FROM pg_catalog.pg_stat_activity a CROSS JOIN pg_catalog.pg_control_system() s "
                "WHERE a.pid=pg_catalog.pg_backend_pid()"
            ).fetchone()
            if (
                identity is None
                or len(identity) != 7
                or any(
                    type(item) is not (int if index in {2, 4} else str)
                    for index, item in enumerate(identity)
                )
            ):
                raise PeerDatabaseTransportError("peer database backend identity is unavailable")
            self._backend_identity = PeerBackendIdentity(
                system_identifier=str(identity[0]),
                server_started_at=str(identity[1]),
                backend_pid=int(str(identity[2])),
                backend_started_at=str(identity[3]),
                database_oid=int(str(identity[4])),
                database=str(identity[5]),
                session_user=str(identity[6]),
            )
        except BaseException:
            self._poisoned = True
            self._abort_process()
            raise

    @property
    def info(self) -> _Info:
        return self._info

    @property
    def backend_identity(self) -> PeerBackendIdentity:
        return self._backend_identity

    def execute(self, query: str | sql.SQL | sql.Composed) -> _Rows:
        try:
            statement = query if isinstance(query, str) else query.as_string()
        except (UnicodeError, psycopg.Error):
            raise PeerDatabaseTransportError("peer database query rendering failed") from None
        mode = self._exchange(
            _row_query("SELECT pg_catalog.current_setting('standard_conforming_strings')"),
            returns_rows=True,
        )
        if mode not in [[("on",)], [("off",)]]:
            self._poisoned = True
            self._info.transaction_status = TransactionStatus.UNKNOWN
            self._abort_process()
            raise PeerDatabaseTransportError("peer database string mode is unavailable")
        statement = _single_statement(statement, standard_strings=mode == [("on",)])
        keyword = re.match(r"[A-Za-z]+", statement)
        if keyword is not None and keyword.group().upper() in _TRANSACTION_COMMANDS:
            raise PeerDatabaseTransportError(
                "peer database transaction control requires the context manager"
            )
        returns_rows = re.match(r"(?:SELECT|WITH)\b", statement, re.IGNORECASE) is not None
        if returns_rows:
            # row_to_json preserves positional order and duplicate column names.
            # Base64 plus a non-base64 control prefix prevents row/marker confusion.
            statement = _row_query(statement)
        elif self._depth == 0:
            raise PeerDatabaseTransportError(
                "peer database changes require an explicit transaction"
            )
        return _Rows(self._exchange(statement, returns_rows=returns_rows))

    def _exchange(self, statement: str, *, returns_rows: bool) -> list[tuple[object, ...]]:
        if self._closed or self._poisoned:
            raise PeerDatabaseTransportError("peer database channel is unavailable")
        marker = "loom-peer-" + uuid4().hex
        frame = (
            "\\set ON_ERROR_STOP off\n\\set ON_ERROR_ROLLBACK off\n"
            + statement
            + "\n;\n\\if :ERROR\n\\echo "
            + marker
            + " error :SQLSTATE\n"
            "\\else\n\\echo " + marker + " ok\n\\endif\n"
        )
        try:
            payload = frame.encode("utf-8")
        except UnicodeError:
            raise PeerDatabaseTransportError("peer database query encoding failed") from None
        if b"\x00" in payload or len(payload) > _MAX_QUERY_BYTES:
            raise PeerDatabaseTransportError("peer database query bound exceeded")
        deadline = min(time.monotonic() + self._query_timeout, self._session_deadline)
        rows: list[tuple[object, ...]] = []
        buffer = bytearray()
        output_bytes = 0
        offset = 0
        process = self._process
        assert (
            process.stdin is not None and process.stdout is not None and process.stderr is not None
        )
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdin, selectors.EVENT_WRITE, "input")
                selector.register(process.stdout, selectors.EVENT_READ, "output")
                selector.register(process.stderr, selectors.EVENT_READ, "diagnostic")
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PeerDatabaseTransportError("peer database query timed out")
                    for key, _ in selector.select(timeout=min(remaining, 1)):
                        if key.data == "input":
                            offset += os.write(key.fd, payload[offset : offset + 65536])
                            if offset == len(payload):
                                selector.unregister(key.fileobj)
                            continue
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            if key.data == "output":
                                raise PeerDatabaseTransportError(
                                    "peer database output closed early"
                                )
                            continue
                        output_bytes += len(chunk)
                        if output_bytes > _MAX_OUTPUT_BYTES:
                            raise PeerDatabaseTransportError("peer database output bound exceeded")
                        if key.data == "diagnostic":
                            continue  # Never retain SQL/error diagnostics, including secret literals.
                        buffer.extend(chunk)
                        while b"\n" in buffer:
                            line, _, tail = buffer.partition(b"\n")
                            buffer = bytearray(tail)
                            if line == (marker + " ok").encode():
                                if buffer or offset != len(payload):
                                    raise PeerDatabaseTransportError(
                                        "peer database response is not exact"
                                    )
                                return rows
                            if line.startswith((marker + " error ").encode()):
                                code_bytes = line[len(marker) + 7 :]
                                if (
                                    buffer
                                    or offset != len(payload)
                                    or re.fullmatch(rb"[0-9A-Z]{5}", code_bytes) is None
                                    or code_bytes == b"00000"
                                ):
                                    raise PeerDatabaseTransportError(
                                        "peer database error framing failed"
                                    )
                                code = code_bytes.decode("ascii")
                                if self._info.transaction_status == TransactionStatus.INTRANS:
                                    self._info.transaction_status = TransactionStatus.INERROR
                                try:
                                    error_type = psycopg.errors.lookup(code)
                                except KeyError:
                                    error_type = psycopg.DatabaseError
                                raise error_type(
                                    "peer database statement failed (SQLSTATE " + code + ")"
                                )
                            if not returns_rows:
                                raise PeerDatabaseTransportError(
                                    "peer database response is unexpected"
                                )
                            rows.append(_decode_row(bytes(line)))
                    if process.poll() is not None:
                        raise PeerDatabaseTransportError("peer database process exited early")
        except psycopg.Error:
            raise
        except BaseException as exc:
            self._poisoned = True
            self._info.transaction_status = TransactionStatus.UNKNOWN
            self._abort_process()
            if isinstance(exc, Exception) and not isinstance(exc, PeerDatabaseTransportError):
                raise PeerDatabaseTransportError("peer database transport failed safely") from None
            raise

    @contextmanager
    def transaction(self) -> Iterator[None]:
        outer = self._depth == 0
        name = "peer_savepoint_" + uuid4().hex
        self._exchange("BEGIN" if outer else "SAVEPOINT " + name, returns_rows=False)
        self._info.transaction_status = TransactionStatus.INTRANS
        self._depth += 1
        try:
            yield
            if self._info.transaction_status == TransactionStatus.INERROR:
                raise psycopg.errors.InFailedSqlTransaction("peer database transaction failed")
        except BaseException:
            if not self._closed and not self._poisoned:
                self._exchange(
                    "ROLLBACK" if outer else "ROLLBACK TO SAVEPOINT " + name,
                    returns_rows=False,
                )
                if not outer:
                    self._exchange("RELEASE SAVEPOINT " + name, returns_rows=False)
            raise
        else:
            self._exchange("COMMIT" if outer else "RELEASE SAVEPOINT " + name, returns_rows=False)
        finally:
            self._depth -= 1
            if not self._closed and not self._poisoned:
                self._info.transaction_status = (
                    TransactionStatus.IDLE if outer else TransactionStatus.INTRANS
                )

    def _abort_process(self) -> None:
        self._closed = True
        process = self._process
        try:
            if process.stdin is not None:
                with suppress(OSError):
                    process.stdin.close()
            if process.poll() is None:
                with suppress(ProcessLookupError):
                    process.terminate()
            try:
                process.wait(timeout=_STOP_SECONDS)
            except subprocess.TimeoutExpired:
                with suppress(ProcessLookupError):
                    process.kill()
                process.wait(timeout=_STOP_SECONDS)
        except (OSError, subprocess.SubprocessError):
            raise PeerDatabaseTransportError(
                "peer database process retirement is unconfirmed"
            ) from None
        finally:
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    with suppress(OSError):
                        stream.close()

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._depth:
                self._exchange("ROLLBACK", returns_rows=False)
            assert self._process.stdin is not None
            os.write(self._process.stdin.fileno(), b"\\q\n")
            self._process.stdin.close()
            if self._process.wait(timeout=_STOP_SECONDS) != 0:
                raise PeerDatabaseTransportError("peer database shutdown is unconfirmed")
        except BaseException as exc:
            self._poisoned = True
            self._abort_process()
            if isinstance(exc, Exception) and not isinstance(exc, PeerDatabaseTransportError):
                raise PeerDatabaseTransportError("peer database shutdown failed safely") from None
            raise
        finally:
            self._closed = True
            self._info.transaction_status = TransactionStatus.UNKNOWN
            for stream in (self._process.stdout, self._process.stderr):
                if stream is not None:
                    with suppress(OSError):
                        stream.close()

    def __enter__(self) -> PeerDatabaseConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
