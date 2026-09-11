"""Actual CNPG manager exec and queued-role behavior in disposable Kubernetes.

This is mechanism evidence, not live process admission or complete SQL/API
retirement. The original PostgreSQL backend and lock must survive unchanged.
"""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import platform
import select
import socket
import ssl
import subprocess
import tarfile
import tempfile
import time
from contextlib import contextmanager

import pytest
import requests
import yaml

from tests.integration.test_execution_actuator_k3s import _start_k3s

_SOURCE = "c56e00d462c3899ab305540953ec541dfe0f762a"
_MANIFEST_SHA256 = "ece141801fef6507451a3032b1eda29e9fa15944b741fa7301701c781160ce41"
_OPERATOR = "ghcr.io/cloudnative-pg/cloudnative-pg@sha256:b5210df46c05bed3c5dbb67d316dece0ed67f4d148acac169416079dc10e4a91"
_POSTGRES = "ghcr.io/cloudnative-pg/postgresql:17.4@sha256:3c0ba08ea353c9705a755c113e4ae395be76553e0ed68076e5410cb09b9d17d9"


@pytest.mark.parametrize("changed_path", ["/proc/1/exe", "/controller/manager"])
def test_cnpg_replacement_refuses_binary_outside_pinned_image(changed_path):
    calls = []

    def kube(*args):
        calls.append(args)
        if args[-3:] == ("-Lc", "%d:%i", "/proc/1/exe"):
            return b"1:2"
        if args[-2:] in (("cat", "/controller/manager"), ("cat", "/proc/1/exe")):
            return b"unadmitted-executable" if args[-1] == changed_path else b"pinned-executable"
        pytest.fail("unadmitted manager reached replacement transport")

    with pytest.raises(AssertionError, match="pinned operator image"):
        _replace_manager(None, kube, "disposable", expected_manager=b"pinned-executable")
    assert calls and all(call[0] == "exec" for call in calls)


def _eventually(observe, expected, message, *, seconds=60):
    deadline = time.monotonic() + seconds
    while True:
        value = observe()
        if expected(value):
            return value
        assert time.monotonic() < deadline, message
        time.sleep(0.2)


@contextmanager
def _child(argv):
    process = subprocess.Popen(
        argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


def _query(peer, statement):
    peer.stdin.write((statement + ";\n").encode())
    peer.stdin.flush()
    assert select.select([peer.stdout], [], [], 20)[0], "disposable peer query timed out"
    result = peer.stdout.readline().decode().strip()
    assert result, "disposable peer connection lost"
    return result


@pytest.fixture(scope="module")
def pinned_manager():
    """Extract the reference without starting code or using mutable Cluster status."""
    import docker

    client = docker.from_env(timeout=120)
    container = None
    try:
        pinned = client.images.pull(_OPERATOR)
        architecture = pinned.attrs["Architecture"]
        assert architecture in ("amd64", "arm64"), "unsupported disposable image architecture"
        # /manager is a symlink; read its reviewed regular-file destination.
        name = f"manager_{architecture}"
        container = client.containers.create(pinned.id, network_disabled=True)
        stream, _ = container.get_archive(f"/operator/{name}")
        payload = io.BytesIO()
        for chunk in stream:
            assert payload.tell() + len(chunk) <= 256 * 1024 * 1024
            payload.write(chunk)
        payload.seek(0)
        with tarfile.open(fileobj=payload, mode="r:") as archive:
            members = archive.getmembers()
            assert len(members) == 1 and members[0].name == name
            assert members[0].isfile() and 0 < members[0].size <= 256 * 1024 * 1024
            source = archive.extractfile(members[0])
            assert source is not None
            with source:
                binary = source.read()
        assert binary.startswith(b"\x7fELF"), "pinned manager is not an ELF executable"
        return binary
    finally:
        if container is not None:
            container.remove(v=True)
        client.close()


@pytest.fixture
def cnpg_probe(tmp_path, pinned_manager):
    response = requests.get(
        f"https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/{_SOURCE}/releases/cnpg-1.25.1.yaml",
        timeout=30,
    )
    response.raise_for_status()
    assert hashlib.sha256(response.content).hexdigest() == _MANIFEST_SHA256
    # Both the operator Deployment image AND OPERATOR_IMAGE_NAME must be pinned:
    # the latter selects the bootstrap init image that copies /controller/manager.
    original_image = "ghcr.io/cloudnative-pg/cloudnative-pg:1.25.1"
    assert response.text.count(original_image) == 2
    manifest = response.text.replace(original_image, _OPERATOR)
    container = _start_k3s()
    try:
        result = _eventually(
            lambda: container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"]),
            lambda value: value.exit_code == 0, "disposable kubeconfig unavailable", seconds=90,
        )
        config = yaml.safe_load(result.output)
        config["clusters"][0]["cluster"]["server"] = (
            f"https://127.0.0.1:{container.get_exposed_port(6443)}"
        )
        config_path = tmp_path / "disposable-kubeconfig"
        config_path.write_text(yaml.safe_dump(config))
        config_path.chmod(0o600)

        def argv(*args):
            # Every command names this disposable API; never inherit host context.
            return ["kubectl", "--kubeconfig", str(config_path), *args]

        def kube(*args, data=None, timeout=60):
            result = subprocess.run(
                argv(*args), input=data, capture_output=True, timeout=timeout,
            )
            assert result.returncode == 0, result.stderr.decode(errors="replace")
            return result.stdout

        _eventually(
            lambda: subprocess.run(argv("get", "--raw=/readyz"), capture_output=True, timeout=10),
            lambda result: result.returncode == 0, "disposable API unready", seconds=90,
        )
        kube("apply", "--server-side", "-f", "-", data=manifest.encode(), timeout=120)
        kube("-n", "cnpg-system", "rollout", "status", "deployment/cnpg-controller-manager",
             "--timeout=300s", timeout=310)
        kube("apply", "-f", "-", data=yaml.safe_dump({
            "apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster",
            "metadata": {"name": "probe", "namespace": "default"},
            "spec": {"instances": 1, "imageName": _POSTGRES, "storage": {"size": "1Gi"},
                     "bootstrap": {"initdb": {"database": "loom", "owner": "loom"}}},
        }).encode())
        kube("wait", "--for=condition=Ready", "cluster/probe", "--timeout=300s", timeout=310)
        pods = json.loads(kube("get", "pods", "-l", "cnpg.io/cluster=probe", "-o", "json"))["items"]
        assert len(pods) == 1
        bootstrap = [entry for entry in pods[0]["spec"]["initContainers"]
                     if entry["name"] == "bootstrap-controller"]
        assert len(bootstrap) == 1 and bootstrap[0]["image"] == _OPERATOR
        yield argv, kube, pods[0]["metadata"]["name"], pinned_manager
    finally:
        container.stop()


def _replace_manager(argv, kube, pod, *, expected_manager):
    def execute(*args):
        return kube("exec", pod, "-c", "postgres", "--", *args)

    old_inode = execute("stat", "-Lc", "%d:%i", "/proc/1/exe").strip()
    assert execute("cat", "/proc/1/exe") == expected_manager, "running manager differs from pinned operator image"
    assert execute("cat", "/controller/manager") == expected_manager, "stored manager differs from pinned operator image"
    binary = expected_manager
    binary_sha = hashlib.sha256(binary).hexdigest()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    with _child(argv("port-forward", f"pod/{pod}", f"{port}:8000", "--address=127.0.0.1")) as forward:
        assert select.select([forward.stdout], [], [], 20)[0], "disposable tunnel unready"
        assert b"Forwarding from 127.0.0.1:" in forward.stdout.readline()
        # Only the exact disposable Pod's loopback tunnel, not a shared endpoint.
        with pytest.warns(requests.packages.urllib3.exceptions.InsecureRequestWarning):
            try:
                result = requests.put(
                    f"https://127.0.0.1:{port}/update", data=binary, timeout=30, verify=False,
                )
            except requests.RequestException:
                pass  # EOF/timeout is ambiguous, never sufficient success evidence.
            else:
                pytest.fail(f"manager unexpectedly returned HTTP {result.status_code}")
        _eventually(
            lambda: execute("stat", "-Lc", "%d:%i", "/proc/1/exe").strip(),
            lambda value: value != old_inode, "manager executable was not replaced",
        )
        assert hashlib.sha256(execute("cat", "/proc/1/exe")).hexdigest() == binary_sha


@pytest.mark.timeout(900)
@pytest.mark.skipif(platform.machine() not in {"x86_64", "amd64"}, reason="protected primary profile is amd64")
def test_production_stream_and_tls_transport_replaces_actual_cnpg_preserving_guard(cnpg_probe):
    """Actual pinned manager + real TLS/header/body limits, not full handoff admission."""
    from loom_cli.rollout.operator.protected_apply_executor import (
        SubprocessProtectedApplyCommandRunner,
    )
    from loom_cli.rollout.operator.protected_cnpg_manager_replacement import (
        CNPG_MANAGER_SHA256,
        CNPG_MANAGER_SIZE,
    )
    from loom_cli.rollout.operator.protected_cnpg_manager_transport import (
        _capture_binary,
        _forward_port,
        _UpdateChannel,
    )
    from loom_cli.rollout.operator.protected_cnpg_manager_transport import (
        _child as transport_child,
    )

    argv, kube, pod, expected_manager = cnpg_probe
    assert len(expected_manager) == CNPG_MANAGER_SIZE
    assert hashlib.sha256(expected_manager).hexdigest() == CNPG_MANAGER_SHA256
    # Every process is additionally pinned by argv to the disposable kubeconfig.
    command = argv("version", "--client")
    assert command[:2] == ["kubectl", "--kubeconfig"]
    environment = {**SubprocessProtectedApplyCommandRunner().environment, "KUBECONFIG": command[2]}
    def execute(*args):
        return kube("exec", pod, "-c", "postgres", "--", *args)

    original_inode = execute("stat", "-Lc", "%d:%i", "/proc/1/exe").strip()
    with _child(argv("exec", "-i", pod, "-c", "postgres", "--", "psql",
                     "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom")) as guard:
        query = "SELECT pg_backend_pid() || '|' || pg_postmaster_start_time() || '|' || pg_try_advisory_lock(5498691230183247727)"
        before = _query(guard, query)
        assert before.endswith("|true")
        with tempfile.TemporaryFile(mode="w+b") as binary:
            with transport_child(argv("exec", pod, "-c", "postgres", "--", "cat", "/proc/1/exe"), environment) as source:
                _capture_binary(source, binary)
            certificate = execute("cat", "/controller/certificates/server.crt").decode("ascii")
            with transport_child(argv("port-forward", f"pod/{pod}", ":8000", "--address=127.0.0.1"), environment) as forward:
                port = _forward_port(forward)
                try:
                    _UpdateChannel(binary, port, ssl.PEM_cert_to_DER_cert(certificate)).issue()
                except (OSError, http.client.HTTPException):
                    pass  # Ambiguous transport result must be reconciled below.
        _eventually(lambda: execute("stat", "-Lc", "%d:%i", "/proc/1/exe").strip(),
                    lambda value: value != original_inode, "production stream did not replace executable")
        assert execute("cat", "/proc/1/exe") == expected_manager
        assert _query(guard, query) == before


@pytest.mark.timeout(900)
@pytest.mark.parametrize("replace_manager", [False, True], ids=["queued-control", "exact-exec"])
def test_cnpg_queued_role_retirement_preserves_original_guard(cnpg_probe, replace_manager):
    argv, kube, pod, expected_manager = cnpg_probe

    def peer(database):
        return _child(argv("exec", "-i", pod, "-c", "postgres", "--", "psql",
                           "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database))

    with peer("loom") as guard, peer("postgres") as blocker:
        before = _query(guard, "SELECT pg_backend_pid() || '|' || pg_postmaster_start_time() || '|' || pg_try_advisory_lock(5498691230183247727)")
        assert before.endswith("|true")
        assert _query(blocker, "CREATE ROLE queued_probe NOLOGIN; BEGIN; LOCK TABLE pg_authid IN SHARE MODE; SELECT 'locked'") == "locked"
        kube("patch", "cluster/probe", "--type=merge", "-p", json.dumps({
            "spec": {"managed": {"roles": [{"name": "queued_probe", "login": True}]}},
        }))
        pending = _eventually(
            lambda: _query(guard, "SELECT COALESCE(string_agg(pid::text, ','), 'none') FROM pg_stat_activity WHERE datname='postgres' AND wait_event_type='Lock' AND query LIKE 'ALTER ROLE \"queued_probe\"%'"),
            lambda value: value != "none", "real CNPG role mutation did not queue",
        )
        old_pid = int(pending)
        kube("patch", "cluster/probe", "--type=merge", "-p", '{"spec":{"managed":{"roles":[]}}}')
        assert json.loads(kube("get", "cluster/probe", "-o", "json"))["spec"]["managed"]["roles"] == []
        assert _query(guard, f"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid={old_pid} AND wait_event_type='Lock')") == "t"
        if replace_manager:
            _replace_manager(argv, kube, pod, expected_manager=expected_manager)
            _eventually(
                lambda: _query(guard, f"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid={old_pid})"),
                lambda value: value == "f", "old queued SQL backend survived exec", seconds=30,
            )
        assert _query(blocker, "ROLLBACK; SELECT 'unlocked'") == "unlocked"
        expected_login = "f" if replace_manager else "t"
        _eventually(
            lambda: _query(guard, "SELECT rolcanlogin FROM pg_roles WHERE rolname='queued_probe'"),
            lambda value: value == expected_login, "unexpected queued role outcome", seconds=30,
        )
        after = _query(guard, "SELECT pg_backend_pid() || '|' || pg_postmaster_start_time() || '|' || EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted)")
        assert after == before


@pytest.mark.timeout(900)
@pytest.mark.parametrize("close_admission", [False, True], ids=["open-database", "closed-database"])
def test_cnpg_exec_reconciles_credentials_without_reopening_application_login(cnpg_probe, close_admission):
    """Same-binary exec is not SQL-writer silence, even with no managed roles."""
    argv, kube, pod, expected_manager = cnpg_probe
    with _child(argv("exec", "-i", pod, "-c", "postgres", "--", "psql",
                     "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom")) as guard:
        before = _query(guard, "SELECT pg_backend_pid() || '|' || pg_postmaster_start_time() || '|' || pg_try_advisory_lock(5498691230183247727)")
        assert before.endswith("|true")
        # Wait for initial credential reconciliation, not just Pod readiness.
        _eventually(
            lambda: _query(guard, "SELECT rolpassword IS NOT NULL FROM pg_authid WHERE rolname='loom'"),
            lambda value: value == "t", "initial application password was not configured",
        )
        assert _query(guard, "ALTER ROLE loom NOLOGIN PASSWORD NULL; ALTER ROLE streaming_replica NOLOGIN NOREPLICATION; SELECT 'sealed'") == "sealed"
        assert _query(guard, "SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_authid WHERE rolname='loom'") == "t"
        assert _query(guard, "SELECT NOT rolcanlogin AND NOT rolreplication FROM pg_roles WHERE rolname='streaming_replica'") == "t"
        if close_admission:
            # PostgreSQL refuses to close admission from the target database.
            # Use the separate maintenance database, just like the protected API.
            kube("exec", pod, "-c", "postgres", "--", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1",
                 "-U", "postgres", "-d", "postgres", "-c", "ALTER DATABASE loom ALLOW_CONNECTIONS false")
            assert _query(guard, "SELECT datallowconn FROM pg_database WHERE datname='loom'") == "f"

        _replace_manager(argv, kube, pod, expected_manager=expected_manager)

        # A new manager loses its in-memory Secret-version cache and reapplies
        # the same declared password. Never print the disposable password/hash.
        _eventually(
            lambda: _query(guard, "SELECT rolpassword IS NOT NULL FROM pg_authid WHERE rolname='loom'"),
            lambda value: value == "t", "replacement manager did not reconcile credentials",
        )
        _eventually(
            lambda: _query(guard, "SELECT rolcanlogin AND rolreplication FROM pg_roles WHERE rolname='streaming_replica'"),
            lambda value: value == "t", "replacement manager did not restore replication permissions",
        )
        assert _query(guard, "SELECT rolcanlogin FROM pg_roles WHERE rolname='loom'") == "f"
        after = _query(guard, "SELECT pg_backend_pid() || '|' || pg_postmaster_start_time() || '|' || EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted)")
        assert after == before
        if close_admission:
            assert _query(guard, "SELECT datallowconn FROM pg_database WHERE datname='loom'") == "f"
            # Refuse even a fresh privileged peer, not just password logins. The
            # previously opened guard is deliberately the only surviving client.
            rejected = subprocess.run(
                argv("exec", pod, "-c", "postgres", "--", "psql", "-XAtq", "-U", "postgres",
                     "-d", "loom", "-c", "SELECT 1"),
                capture_output=True, timeout=30,
            )
            assert rejected.returncode != 0
            assert b'database "loom" is not currently accepting connections' in rejected.stderr
            # No later success marker may hide failed setup/mutation SQL. Run
            # this only after the final original-guard continuity observation.
            with pytest.raises(AssertionError, match="peer connection lost"):
                _query(guard, "SELECT 1 / 0; SELECT 'masked-sql-error'")
            assert guard.wait(timeout=10) != 0
