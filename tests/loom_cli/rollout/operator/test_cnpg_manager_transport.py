"""The fixed replacement transport never repeats uncertain dispatch."""

import hashlib
import io
import json
import socket
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _admit, _manager
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


def test_manager_transport_requires_active_intent_before_any_io(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unadmitted replacement reached a process")

    monkeypatch.setattr("subprocess.Popen", forbidden)
    runner, journal = SubprocessProtectedApplyCommandRunner(), _journal(tmp_path)
    with pytest.raises(RuntimeError, match="active component"):
        runner.issue_staging_manager_replacement(journal=journal)


@pytest.mark.parametrize("upload_error", [None, TimeoutError("private diagnostic"), OSError("private diagnostic")])
def test_manager_transport_dispatches_once_and_keeps_ambiguous_result(tmp_path, monkeypatch, upload_error):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    runner = SubprocessProtectedApplyCommandRunner()
    calls = []

    def issue():
        assert journal.read_application_manager_replacement()[1] is True
        calls.append("upload")
        if upload_error:
            raise upload_error

    @contextmanager
    def channel(*args, **kwargs):
        calls.append("open")
        try:
            yield SimpleNamespace(issue=issue)
        finally:
            calls.append("close")

    monkeypatch.setattr(transport, "_prepared_update", channel)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        assert runner.issue_staging_manager_replacement(journal=journal) is True
        assert runner.issue_staging_manager_replacement(journal=journal) is False
        assert journal.read_application_manager_replacement()[2] is None
        raise RuntimeError("reconciliation still required")

    with pytest.raises(RuntimeError, match="reconciliation still required"):
        journal.execute(plan, [_component(apply)])
    assert calls == ["open", "upload", "close"]


def test_preparation_failure_does_not_consume_dispatch(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    plan, journal = _plan(tmp_path), _journal(tmp_path)

    @contextmanager
    def unavailable(*args, **kwargs):
        raise RuntimeError("CNPG manager channel unavailable")
        yield  # pragma: no cover

    monkeypatch.setattr(transport, "_prepared_update", unavailable)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        with pytest.raises(RuntimeError, match="channel unavailable"):
            SubprocessProtectedApplyCommandRunner().issue_staging_manager_replacement(journal=journal)
        assert journal.read_application_manager_replacement()[1:] == (False, None)
        raise RuntimeError("still undispatched")

    with pytest.raises(RuntimeError, match="still undispatched"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("change", [
    {"pod_uid": "33333333-3333-4333-8333-333333333333"},
    {"container_id": "containerd://" + "b" * 64},
    {"process_started_ticks": 12346}, {"executable_inode": 101},
])
def test_transport_refuses_changed_identity_before_binary_or_tunnel(tmp_path, monkeypatch, change):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    def forbidden(*args, **kwargs):
        pytest.fail("changed manager reached binary or tunnel subprocess")

    monkeypatch.setattr(transport, "_observe_identity", lambda *a, **k: replace(_manager(), **change))
    monkeypatch.setattr("subprocess.Popen", forbidden)
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        with pytest.raises(RuntimeError, match="identity changed"):
            SubprocessProtectedApplyCommandRunner().issue_staging_manager_replacement(journal=journal)
        assert journal.read_application_manager_replacement()[1:] == (False, None)
        raise RuntimeError("identity refused")

    with pytest.raises(RuntimeError, match="identity refused"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("case", ["exact", "short", "oversized", "wrong-hash", "stderr", "nonzero", "timeout"])
def test_binary_capture_is_streamed_bounded_and_reaps_child(monkeypatch, case):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    binary = b"pinned-manager" * 10000
    monkeypatch.setattr(transport, "CNPG_MANAGER_SIZE", len(binary))
    monkeypatch.setattr(transport, "CNPG_MANAGER_SHA256", hashlib.sha256(binary).hexdigest())
    command = "import sys; sys.stdout.buffer.write(b'pinned-manager' * 10000); sys.stdout.flush()"
    if case == "short":
        command = command.replace("10000", "9999")
    elif case == "oversized":
        command = command.replace("10000", "10001")
    elif case == "wrong-hash":
        command = command.replace("pinned-manager", "pinned-manageR")
    elif case == "stderr":
        command += "; sys.stderr.buffer.write(b'e' * 100000)"
    elif case == "nonzero":
        command += "; sys.exit(3)"
    elif case == "timeout":
        monkeypatch.setattr(transport, "_SECONDS", 0.1)
        command = "import threading; threading.Event().wait()"
    output = io.BytesIO()
    with transport._child((sys.executable, "-c", command), {}) as process:
        if case == "exact":
            transport._capture_binary(process, output)
            assert output.read() == binary
        else:
            with pytest.raises(RuntimeError, match="CNPG manager binary"):
                transport._capture_binary(process, output)
    assert process.poll() is not None
    assert process.stdout.closed and process.stderr.closed
    assert len(output.getvalue()) <= len(binary)


def _pod(identity):
    return {
        "kind": "Pod", "metadata": {"name": identity.pod_name, "namespace": "loom-staging",
                                     "uid": identity.pod_uid},
        "spec": {"nodeName": identity.node_name},
        "status": {"containerStatuses": [{"name": "postgres", "containerID": identity.container_id,
                                           "restartCount": identity.restart_count, "state": {"running": {}}}]},
    }


def _process(identity, transport):
    stat = b"1 (manager) " + b" ".join([b"S"] + [b"0"] * 18 + [str(identity.process_started_ticks).encode()])
    inode = f"{identity.executable_device} {identity.executable_inode}".encode()
    digest = transport.CNPG_MANAGER_SHA256.encode()
    return b"\n".join((transport._COMMAND, stat, inode, inode, digest + b"  /proc/1/exe",
                        digest + b"  /controller/manager")) + b"\n"


@pytest.mark.parametrize("changed", [None, "stored-inode", "stored-hash", "command", "restart", "node"])
def test_process_observation_binds_exact_pod_and_both_executables(changed):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    identity, calls = _manager(), []

    def capture(argv, **kwargs):
        calls.append(argv)
        assert argv[:3] == ("kubectl", "--namespace", "loom-staging")
        assert f"pod/{identity.pod_name}" in argv
        if argv[3] == "get":
            pod = _pod(identity)
            if changed == "node":
                pod["spec"]["nodeName"] = "trt-eai-oldlab-2"
            if changed == "restart" and len(calls) == 3:
                pod["status"]["containerStatuses"][0]["restartCount"] += 1
            return json.dumps(pod).encode()
        assert argv == transport._exec(identity, "sh", "-ceu", transport._PROCESS)
        lines = _process(identity, transport).splitlines()
        if changed == "stored-inode":
            lines[3] = b"24 101"
        elif changed == "stored-hash":
            lines[5] = b"0" * 64 + b"  /controller/manager"
        elif changed == "command":
            lines[0] += b"--other\x00"
        return b"\n".join(lines) + b"\n"

    runner = SimpleNamespace(capture_stdout=capture, environment={})
    if changed is None:
        assert transport._observe_identity(runner, identity) == identity
        assert len(calls) == 3
    else:
        with pytest.raises((ValueError, RuntimeError), match="CNPG manager"):
            transport._observe_identity(runner, identity)
        if changed == "node":
            assert len(calls) == 1  # Forbidden node rejected before exec.


@pytest.mark.parametrize("message", [
    "Forwarding from 127.0.0.1:4567 -> 8000\n",
    "Forwarding from 0.0.0.0:4567 -> 8000\n",
    "Forwarding from 127.0.0.1:4567 -> 5432\n",
    "Forwarding from 127.0.0.1:99999 -> 8000\n",
    "x" * 5000,
])
def test_port_forward_reads_exact_loopback_readiness_and_cleans_up(message):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    # Nonmatching readiness exits; the reader must refuse, not guess a port.
    command = f"import sys; sys.stdout.write({message!r}); sys.stdout.flush()"
    exact = message == "Forwarding from 127.0.0.1:4567 -> 8000\n"
    if exact:
        command += "; import threading; threading.Event().wait()"
    with transport._child((sys.executable, "-c", command), {}) as process:
        if exact:
            assert transport._forward_port(process) == 4567
        else:
            with pytest.raises(RuntimeError, match="CNPG manager Pod tunnel"):
                transport._forward_port(process)
    assert process.poll() is not None and process.stdout.closed and process.stderr.closed


def _tls_certificate(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "loom-postgres-rw")])
    now = datetime.now(UTC)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
                   .sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))
    cert_path, key_path = tmp_path / "server.crt", tmp_path / "server.key"
    cert_path.write_bytes(certificate)
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                           serialization.NoEncryption()))
    key_path.chmod(0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(cert_path, key_path)
    return certificate, context


@pytest.mark.parametrize("case", ["exact", "slow-check", "slow-journal", "wrong-certificate"])
def test_real_tls_upload_obeys_header_deadline_and_exact_certificate(tmp_path, monkeypatch, case):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    certificate, tls_server = _tls_certificate(tmp_path)
    binary = b"pinned-manager" * 10000
    monkeypatch.setattr(transport, "CNPG_MANAGER_SIZE", len(binary))

    @contextmanager
    def child(*args, **kwargs):
        yield object()

    monkeypatch.setattr(transport, "_child", child)
    monkeypatch.setattr(transport, "_capture_binary", lambda _process, output: (output.write(binary), output.seek(0)))
    monkeypatch.setattr(transport, "_observe_identity", lambda *a, **k: _manager())

    @contextmanager
    def maintenance():
        yield object()

    checks = []

    def drained(_connection, **kwargs):
        checks.append(kwargs)
        if case == "slow-check":
            # Deliberately exceed the test server's 0.2s header budget, just as
            # real kubectl/SQL work can exceed CNPG's pinned 3s header budget.
            time.sleep(0.4)

    monkeypatch.setattr(transport, "require_application_database_drained", drained)
    begin = journal.begin_application_manager_replacement

    def slow_begin():
        issued = begin()
        if case == "slow-journal":
            time.sleep(0.4)  # Models slow fsync after marker publication.
        return issued

    monkeypatch.setattr(journal, "begin_application_manager_replacement", slow_begin)
    if case == "wrong-certificate":
        other = tmp_path / "other"
        other.mkdir()
        certificate, _ = _tls_certificate(other)
    runner = SimpleNamespace(environment={}, capture_stdout=lambda *a, **k: certificate,
                             open_staging_peer_maintenance_database=maintenance)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(5)
        port = listener.getsockname()[1]
        monkeypatch.setattr(transport, "_forward_port", lambda _process: port)

        def server():
            raw, _address = listener.accept()
            with raw:
                raw.settimeout(5)
                with tls_server.wrap_socket(raw, server_side=True) as secure:
                    # Real deadline enforcement; not a fake successful channel.
                    secure.settimeout(0.2)
                    headers = b""
                    try:
                        while not headers.endswith(b"\r\n\r\n"):
                            chunk = secure.recv(1)
                            if not chunk:
                                return headers, b""
                            headers += chunk
                    except OSError:
                        return headers, b""
                    secure.settimeout(5)
                    body = b""
                    while len(body) < len(binary):
                        chunk = secure.recv(min(65536, len(binary) - len(body)))
                        if not chunk:
                            break
                        body += chunk
                    return headers, body

        with ThreadPoolExecutor(max_workers=1) as executor:
            received = executor.submit(server)

            def apply(_):
                _admit(journal)
                journal.prepare_application_manager_replacement(identity=_manager())
                if case == "wrong-certificate":
                    with pytest.raises(RuntimeError, match="certificate changed"):
                        transport.issue_staging_manager_replacement(runner, journal=journal)
                else:
                    assert transport.issue_staging_manager_replacement(runner, journal=journal)
                raise RuntimeError("no completion claimed")

            with pytest.raises(RuntimeError, match="no completion claimed"):
                journal.execute(plan, [_component(apply)])
            headers, body = received.result(timeout=6)
    if case == "wrong-certificate":
        assert not headers and not body
    else:
        assert headers.startswith(b"PUT /update HTTP/1.1\r\n")
        assert headers.count(b"Content-Length:") == 1
        assert f"Content-Length: {len(binary)}\r\n".encode() in headers
        assert body == binary
        assert checks[0]["coordination_guard"] == journal.read_application_recovery_view(
            plan, _component(lambda _: None), ordinal=0,
        ).admission.coordination_guard


@pytest.mark.parametrize("blocker", ["lost-guard", "pending-peer", "dispatch-fsync"])
def test_transport_never_opens_final_socket_before_authority_and_durability(tmp_path, monkeypatch, blocker):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    certificate, _ = _tls_certificate(tmp_path)
    monkeypatch.setattr(transport, "_observe_identity", lambda *a, **k: _manager())
    monkeypatch.setattr(transport, "_forward_port", lambda _process: 4567)
    monkeypatch.setattr(transport, "_capture_binary", lambda *a: None)

    @contextmanager
    def harmless(*args, **kwargs):
        yield object()

    def forbidden(*args, **kwargs):
        pytest.fail("final manager socket opened without ready durable authority")

    def drain(*args, **kwargs):
        if blocker == "lost-guard":
            raise RuntimeError("original guard lost")

    monkeypatch.setattr(transport, "_child", harmless)
    monkeypatch.setattr(transport.socket, "create_connection", forbidden)
    monkeypatch.setattr(transport, "require_application_database_drained", drain)
    runner = SimpleNamespace(environment={}, capture_stdout=lambda *a, **k: certificate,
                             open_staging_peer_maintenance_database=harmless)
    sync = journal._sync_application_recovery

    def fail_dispatch_sync(root, filename):
        if filename == "application-manager-dispatch.json":
            raise RuntimeError("dispatch fsync failed")
        sync(root, filename)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        if blocker == "pending-peer":
            journal.prepare_application_handoff_recovery(ordinal=1)
        elif blocker == "dispatch-fsync":
            monkeypatch.setattr(journal, "_sync_application_recovery", fail_dispatch_sync)
        with pytest.raises(RuntimeError, match=r"guard lost|pending peer recovery|fsync failed"):
            transport.issue_staging_manager_replacement(runner, journal=journal)
        monkeypatch.setattr(journal, "_sync_application_recovery", sync)
        assert journal.read_application_manager_replacement()[1] == (blocker == "dispatch-fsync")
        if blocker == "dispatch-fsync":
            assert transport.issue_staging_manager_replacement(runner, journal=journal) is False
        raise RuntimeError("no upload")

    with pytest.raises(RuntimeError, match="no upload"):
        journal.execute(plan, [_component(apply)])
