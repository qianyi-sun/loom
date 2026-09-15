"""Bounded peer channel failures; fake child never opens a database."""

import subprocess
import sys

import pytest
from psycopg.pq import TransactionStatus

import loom_cli.rollout.operator.protected_peer_database_connection as peer

_CHILD = r"""
import base64, json, os, re, signal, sys, time
mode = sys.argv[1]
parts = []
identity_seen = False
for line in sys.stdin:
    if line.strip() == r"\q":
        if mode == "close-hang":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(100)
        sys.exit(0)
    parts.append(line)
    if line.strip() != r"\endif":
        continue
    request = "".join(parts)
    parts = []
    marker = re.search(r"\\echo (loom-peer-[0-9a-f]+) ok", request).group(1)
    row = None
    if "current_setting('standard_conforming_strings')" in request:
        row = {"value": "invalid" if mode == "invalid-mode" and identity_seen else "on"}
    elif "current_setting('server_version_num')" in request:
        row = {"value": 170004 if mode.startswith("pg17") else 160000}
    elif "name='event_triggers'" in request:
        row = {"setting": "on" if mode == "pg17-default" else "off",
               "source": "session" if mode == "pg17-session-off" else "client"}
    elif "current_setting('event_triggers')" in request:
        row = {"value": "on"}
    elif "pg_catalog.pg_event_trigger" in request:
        row = {"value": False}
    elif "pg_control_system()" in request:
        row = dict(zip("abcdefg", ["system", "server-start", 123, "backend-start", 456, "loom", "postgres"]))
        identity_seen = True
    elif "probe" in request or (mode == "commit-loss" and re.search(r"\bCOMMIT\s*;", request)):
        if mode == "early-exit" or mode == "commit-loss":
            sys.exit(0)
        if mode == "hang":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(100)
        if mode == "output-limit":
            print("private-diagnostic" * 400, flush=True)
        elif mode == "bad-error":
            print(marker + " error private-diagnostic-\u00ff", flush=True)
        else:
            print("private-diagnostic-malformed-row", flush=True)
        continue
    if row is not None:
        print(base64.b64encode(json.dumps(row).encode()).decode(), flush=True)
    print(marker + " ok", flush=True)
"""


@pytest.mark.parametrize("mode", ["pg17-default", "pg17-session-off"])
def test_pg17_requires_admitted_startup_settings_before_identity(mode):
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, mode],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
    )
    with pytest.raises(peer.PeerDatabaseTransportError, match="startup"):
        peer.PeerDatabaseConnection(process)
    assert process.poll() is not None
    assert process.stdin.closed and process.stdout.closed and process.stderr.closed


@pytest.mark.parametrize(
    "mode", ["early-exit", "hang", "output-limit", "bad-row", "bad-error", "invalid-mode"]
)
@pytest.mark.parametrize("startup_delay", [0, 0.5])
def test_peer_protocol_failure_is_sanitized_poisoned_and_reaped(monkeypatch, mode, startup_delay):
    monkeypatch.setattr(peer, "_STOP_SECONDS", 0.1)
    # Deliberate child-startup fault injection, not a wait for readiness. The
    # protocol failure assertions below must run after successful admission.
    child = _CHILD.replace("mode = sys.argv[1]", f"time.sleep({startup_delay})\nmode = sys.argv[1]")
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", child, mode],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    connection = peer.PeerDatabaseConnection(process, query_timeout_seconds=0.25)
    monkeypatch.setattr(peer, "_MAX_OUTPUT_BYTES", 4096)
    try:
        with pytest.raises(peer.PeerDatabaseTransportError) as caught:
            connection.execute("SELECT 'probe'")
        assert "private-diagnostic" not in str(caught.value)
        assert "private-diagnostic" not in repr(caught.value)
        assert connection.info.transaction_status == TransactionStatus.UNKNOWN
        assert process.poll() is not None
        with pytest.raises(peer.PeerDatabaseTransportError, match="unavailable"):
            connection.execute("SELECT 1")
    finally:
        connection.close()
    assert process.stdin.closed and process.stdout.closed and process.stderr.closed


def test_peer_lost_commit_acknowledgement_is_unknown_not_rollback():
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, "commit-loss"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    with peer.PeerDatabaseConnection(process) as connection:
        with pytest.raises(peer.PeerDatabaseTransportError):
            with connection.transaction():
                pass
        assert connection.info.transaction_status == TransactionStatus.UNKNOWN
        assert process.poll() is not None


def test_peer_closed_transaction_finalizer_does_not_restore_usable_state():
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, "normal"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    with peer.PeerDatabaseConnection(process) as connection:
        with pytest.raises(RuntimeError, match="stop transaction"):
            with connection.transaction():
                connection.close()
                raise RuntimeError("stop transaction")
        assert connection.info.transaction_status == TransactionStatus.UNKNOWN
        assert process.poll() is not None


def test_peer_shutdown_timeout_is_sanitized_and_reaped(monkeypatch):
    monkeypatch.setattr(peer, "_STOP_SECONDS", 0.1)
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, "close-hang"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    connection = peer.PeerDatabaseConnection(process)
    with pytest.raises(peer.PeerDatabaseTransportError) as caught:
        connection.close()
    assert "private-diagnostic" not in repr(caught.value)
    assert connection.info.transaction_status == TransactionStatus.UNKNOWN
    assert process.poll() is not None
    connection.close()


def test_peer_invalid_encoding_is_sanitized_before_statement_io():
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", _CHILD, "normal"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    with peer.PeerDatabaseConnection(process) as connection:
        with pytest.raises(peer.PeerDatabaseTransportError) as caught:
            connection.execute("SELECT 'private-diagnostic-\udcff'")
        assert "private-diagnostic" not in repr(caught.value)
        assert connection.info.transaction_status == TransactionStatus.IDLE
        assert process.poll() is None
