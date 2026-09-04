from __future__ import annotations

import hashlib
import os
import signal
import socket
import struct
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import UUID

import pytest

from loom_task_image_builder_guard import identity as identity_module
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.identity import (
    PeerHandle,
    PeerInspector,
    derive_batch_cgroup,
    projection_request,
)
from loom_task_image_builder_guard.models import IdentityConfig
from loom_task_image_builder_guard.protocol import PeerCredentials

PID = 4242
UID = 993
GID = 980
GRANT = UUID("11111111-1111-4111-8111-111111111111")
REQUEST = UUID("22222222-2222-4222-8222-222222222222")
BOOT = UUID("33333333-3333-4333-8333-333333333333")
CGROUP = "/system.slice/slurmstepd.scope/job_123/step_batch/user/task_0"
CREDENTIALS = PeerCredentials(PID, UID, GID)


class _Connection:
    family = socket.AF_UNIX

    def getsockopt(self, level: int, option: int, length: int) -> bytes:
        assert (level, option, length) == (socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        return struct.pack("3i", PID, UID, GID)

    def getsockname(self) -> str:
        return "/run/loom-task-image-builder-guard/guard.sock"


def _fixture(tmp_path: Path) -> tuple[PeerInspector, Path, dict[str, bool]]:
    proc_root = tmp_path / "proc"
    process = proc_root / str(PID)
    process.mkdir(parents=True)
    executable = tmp_path / "usr/local/libexec/loom-task-builder-supervisor"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"trusted-supervisor-v1")
    executable.chmod(0o555)
    (process / "exe").symlink_to(executable)
    (process / "status").write_text(
        "Name:\tloom-builder\n"
        "State:\tS (sleeping)\n"
        f"Pid:\t{PID}\n"
        f"Uid:\t{UID}\t{UID}\t{UID}\t{UID}\n"
        f"Gid:\t{GID}\t{GID}\t{GID}\t{GID}\n"
        "Groups:\t44 980\n"
        "Threads:\t1\n",
        encoding="ascii",
    )
    (process / "cgroup").write_text(f"0::{CGROUP}\n", encoding="ascii")
    (process / "fd").mkdir()
    (process / "fd/0").symlink_to("/dev/null")
    (process / "fd/1").symlink_to("socket:[1234]")
    (process / "task" / str(PID)).mkdir(parents=True)
    (process / "net").mkdir()
    (process / "net/unix").write_text(
        "Num RefCount Protocol Flags Type St Inode Path\n"
        "0000000000000000: 00000003 00000000 00000000 0005 03 1234\n",
        encoding="ascii",
    )
    alive = {"value": True}
    inspector = PeerInspector(
        IdentityConfig(
            uid=UID,
            gid=GID,
            forbidden_supplementary_gids=(0, 27, 128),
            supervisor_path=Path("/usr/local/libexec/loom-task-builder-supervisor"),
            supervisor_sha256=hashlib.sha256(b"trusted-supervisor-v1").hexdigest(),
        ),
        proc_root=proc_root,
        host_root=tmp_path,
        trusted_file_uid=os.geteuid(),
        pidfd_open=lambda pid: os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC),
        pidfd_alive=lambda descriptor: alive["value"],
        socket_peer_inode=lambda _connection: 1234,
        socket_identity=lambda _pidfd, _fd: (
            socket.AF_UNIX,
            socket.SOCK_SEQPACKET,
            "",
            "/run/loom-task-image-builder-guard/guard.sock",
            1234,
        ),
    )
    return inspector, executable, alive


def _real_peer(
    child: subprocess.Popen[bytes],
    *,
    control_socket_path: str = "/run/loom-task-image-builder-guard/guard.sock",
    control_peer_inode: int | None = None,
) -> PeerHandle:
    pidfd = identity_module._pidfd_open(child.pid)
    executable_fd = os.open(
        Path("/proc") / str(child.pid) / "exe",
        os.O_RDONLY | os.O_CLOEXEC,
    )
    metadata = os.fstat(executable_fd)
    inspector = PeerInspector(
        IdentityConfig(
            uid=os.geteuid(),
            gid=os.getegid(),
            forbidden_supplementary_gids=(),
            supervisor_path=Path(sys.executable),
            supervisor_sha256="a" * 64,
        ),
        fd_table_shared=lambda _peer_pid, _other_pid: False,
        visible_processes=lambda: (child.pid,),
    )
    return PeerHandle(
        pid=child.pid,
        uid=os.geteuid(),
        gid=os.getegid(),
        groups=(),
        pidfd=pidfd,
        executable_fd=executable_fd,
        executable_path=Path(sys.executable),
        executable_device=metadata.st_dev,
        executable_inode=metadata.st_ino,
        executable_sha256="a" * 64,
        batch_cgroup_relative=PurePosixPath(CGROUP),
        cgroup_relative=PurePosixPath(CGROUP),
        job_id="123",
        control_socket_path=control_socket_path,
        control_peer_inode=control_peer_inode,
        _inspector=inspector,
    )


def test_capture_derives_peer_identity_executable_and_job_from_kernel_files(
    tmp_path: Path,
) -> None:
    inspector, executable, _alive = _fixture(tmp_path)

    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    try:
        assert (peer.pid, peer.uid, peer.gid, peer.job_id) == (PID, UID, GID, "123")
        assert peer.cgroup_relative.as_posix() == CGROUP
        assert peer.executable_path == executable
        assert peer.executable_sha256 == hashlib.sha256(b"trusted-supervisor-v1").hexdigest()
        peer.assert_unchanged()
    finally:
        peer.close()


def test_capture_rejects_message_credentials_from_a_delegated_socket(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)

    with pytest.raises(GuardError) as caught:
        inspector.capture(
            _Connection(),  # type: ignore[arg-type]
            PeerCredentials(PID + 1, UID, GID),
        )

    assert caught.value.code == "peer_credentials_invalid"


def test_capture_owns_pidfd_before_socket_diagnostics_and_closes_it_on_failure(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    events: list[str] = []
    opened: list[int] = []

    class OrderedConnection(_Connection):
        def getsockname(self) -> str:
            events.append("socket_name")
            return super().getsockname()

    def open_pidfd(pid: int) -> int:
        assert pid == PID
        events.append("pidfd_open")
        descriptor = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
        opened.append(descriptor)
        return descriptor

    def fail_socket_diagnostic(_connection: socket.socket) -> int:
        events.append("socket_diagnostic")
        raise GuardError("peer_socket_invalid")

    inspector._pidfd_open = open_pidfd
    inspector._socket_peer_inode = fail_socket_diagnostic

    with pytest.raises(GuardError) as caught:
        inspector.capture(OrderedConnection(), CREDENTIALS)  # type: ignore[arg-type]

    assert caught.value.code == "peer_socket_invalid"
    assert events.index("pidfd_open") < events.index("socket_name")
    assert events.index("pidfd_open") < events.index("socket_diagnostic")
    assert len(opened) == 1
    with pytest.raises(OSError) as closed:
        os.fstat(opened[0])
    assert closed.value.errno == 9


@pytest.mark.parametrize(
    ("line", "code"),
    [
        (f"Uid:\t{UID}\t{UID}\t0\t{UID}\n", "peer_identity_invalid"),
        (f"Gid:\t{GID}\t{GID}\t0\t{GID}\n", "peer_identity_invalid"),
        ("Groups:\t0 44 980\n", "peer_groups_forbidden"),
        ("Groups:\t44 44 980\n", "peer_status_invalid"),
    ],
)
def test_capture_rejects_elevated_or_malformed_process_identity(
    tmp_path: Path,
    line: str,
    code: str,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    status = inspector.proc_root / str(PID) / "status"
    rows = status.read_text(encoding="ascii").splitlines(keepends=True)
    prefix = line.split(":", 1)[0] + ":"
    status.write_text(
        "".join(line if row.startswith(prefix) else row for row in rows),
        encoding="ascii",
    )

    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]

    assert caught.value.code == code


def test_capture_rejects_wrong_executable_path_or_digest(tmp_path: Path) -> None:
    inspector, executable, _alive = _fixture(tmp_path)
    other = tmp_path / "other-supervisor"
    other.write_bytes(executable.read_bytes())
    other.chmod(0o555)
    link = inspector.proc_root / str(PID) / "exe"
    link.unlink()
    link.symlink_to(other)

    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    assert caught.value.code == "peer_executable_invalid"

    link.unlink()
    link.symlink_to(executable)
    executable.chmod(0o755)
    executable.write_bytes(b"changed")
    executable.chmod(0o555)
    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    assert caught.value.code == "peer_executable_invalid"


def test_rejected_executable_digest_does_not_leak_open_descriptors(tmp_path: Path) -> None:
    inspector, executable, _alive = _fixture(tmp_path)
    executable.chmod(0o755)
    executable.write_bytes(b"wrong-digest")
    executable.chmod(0o555)
    before = len(tuple(Path("/proc/self/fd").iterdir()))

    for _ in range(8):
        with pytest.raises(GuardError):
            inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]

    assert len(tuple(Path("/proc/self/fd").iterdir())) == before


def test_peer_close_attempts_both_descriptors_when_one_close_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    executable_fd = peer.executable_fd
    pidfd = peer.pidfd
    attempted: list[int] = []
    close = os.close

    def fail_executable_close(descriptor: int) -> None:
        attempted.append(descriptor)
        if descriptor == executable_fd:
            raise OSError("injected close failure")
        close(descriptor)

    monkeypatch.setattr(identity_module.os, "close", fail_executable_close)
    try:
        peer.close()
        peer.close()
        assert attempted == [executable_fd, pidfd]
        with pytest.raises(OSError):
            os.fstat(pidfd)
    finally:
        close(executable_fd)


def test_global_descriptor_audit_reports_bounded_main_loop_progress(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    other = inspector.proc_root / "4999/fd"
    other.mkdir(parents=True)
    (other / "0").symlink_to("/dev/null")
    inspector._visible_processes_source = lambda: (PID, 4999)
    inspector._fd_table_shared = lambda _peer_pid, _other_pid: False
    progress: list[str] = []
    inspector._progress = lambda: progress.append("progress")
    try:
        inspector._assert_unshared_fd_table(peer)
    finally:
        peer.close()

    assert len(progress) >= 4


def test_containment_hold_rejects_and_resumes_peer_with_preopened_inet_socket() -> None:
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import socket,sys; "
                "held=socket.socket(socket.AF_INET, socket.SOCK_STREAM); "
                "sys.stdout.buffer.write(b'R'); sys.stdout.buffer.flush(); "
                "sys.stdin.buffer.read(1)"
            ),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    peer: PeerHandle | None = None
    try:
        assert child.stdout is not None
        assert child.stdout.read(1) == b"R"
        peer = _real_peer(child)

        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                pass

        assert caught.value.code == "peer_network_socket_present"
        assert child.poll() is None
        stdout, stderr = child.communicate(input=b"X", timeout=5)
        assert stdout == b""
        assert stderr == b""
        assert child.returncode == 0
    finally:
        if peer is not None:
            peer.close()
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)


def test_containment_hold_rejects_noncontrol_unix_socket() -> None:
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import socket,sys; "
                "held=socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM); "
                "sys.stdout.buffer.write(b'R'); sys.stdout.buffer.flush(); "
                "sys.stdin.buffer.read(1)"
            ),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    peer: PeerHandle | None = None
    try:
        assert child.stdout is not None
        assert child.stdout.read(1) == b"R"
        peer = _real_peer(child)

        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                pass

        assert caught.value.code == "peer_unexpected_unix_socket"
        assert child.poll() is None
        stdout, stderr = child.communicate(input=b"X", timeout=5)
        assert stdout == b""
        assert stderr == b""
        assert child.returncode == 0
    finally:
        if peer is not None:
            peer.close()
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)


def test_containment_hold_allows_one_connected_seqpacket_control_socket(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "guard.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    listener.bind(str(socket_path))
    listener.listen(1)
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import socket,sys; "
                "control=socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET); "
                "control.connect(sys.argv[1]); "
                "sys.stdout.buffer.write(b'R'); sys.stdout.buffer.flush(); "
                "sys.stdin.buffer.read(1)"
            ),
            str(socket_path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    peer: PeerHandle | None = None
    connection: socket.socket | None = None
    try:
        connection, _address = listener.accept()
        assert child.stdout is not None
        assert child.stdout.read(1) == b"R"
        peer = _real_peer(
            child,
            control_socket_path=str(socket_path),
            control_peer_inode=identity_module._unix_peer_inode(connection),
        )

        with peer.containment_hold():
            status = Path("/proc") / str(child.pid) / "status"
            assert "State:\tT (stopped)" in status.read_text(encoding="ascii")

        assert child.poll() is None
        stdout, stderr = child.communicate(input=b"X", timeout=5)
        assert stdout == b""
        assert stderr == b""
        assert child.returncode == 0
    finally:
        if peer is not None:
            peer.close()
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
        if connection is not None:
            connection.close()
        listener.close()


def test_socket_peer_inode_distinguishes_reused_guard_path(tmp_path: Path) -> None:
    socket_path = tmp_path / "guard.sock"
    old_listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    old_listener.bind(str(socket_path))
    old_listener.listen(1)
    stale_client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    stale_client.connect(str(socket_path))
    old_connection, _address = old_listener.accept()
    old_connection.close()
    old_listener.close()
    socket_path.unlink()

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    listener.bind(str(socket_path))
    listener.listen(1)
    current_client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    current_client.connect(str(socket_path))
    connection, _address = listener.accept()
    try:
        assert identity_module._unix_peer_inode(connection) == os.fstat(
            current_client.fileno()
        ).st_ino
        assert identity_module._unix_peer_inode(connection) != os.fstat(
            stale_client.fileno()
        ).st_ino
    finally:
        connection.close()
        current_client.close()
        listener.close()
        stale_client.close()


def test_containment_hold_rejects_same_path_socket_from_another_connection(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    status = inspector.proc_root / str(PID) / "status"

    def send_signal(_pidfd: int, signum: int) -> None:
        state = "T (stopped)" if signum == signal.SIGSTOP else "S (sleeping)"
        rows = status.read_text(encoding="ascii").splitlines(keepends=True)
        status.write_text(
            "".join(
                f"State:\t{state}\n" if row.startswith("State:") else row
                for row in rows
            ),
            encoding="ascii",
        )

    inspector._pidfd_send_signal = send_signal
    inspector._fd_table_shared = lambda _peer_pid, _other_pid: False
    inspector._socket_identity = lambda _pidfd, _fd: (
        socket.AF_UNIX,
        socket.SOCK_SEQPACKET,
        "",
        "/run/loom-task-image-builder-guard/guard.sock",
        5678,
    )
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    try:
        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                pass

        assert caught.value.code == "peer_unexpected_unix_socket"
        assert "State:\tS (sleeping)" in status.read_text(encoding="ascii")
    finally:
        peer.close()


def test_containment_hold_rejects_seqpacket_connected_to_another_unix_service(
    tmp_path: Path,
) -> None:
    decoy_path = tmp_path / "decoy.sock"
    expected_path = tmp_path / "guard.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    listener.bind(str(decoy_path))
    listener.listen(1)
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import socket,sys; "
                "control=socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET); "
                "control.connect(sys.argv[1]); "
                "sys.stdout.buffer.write(b'R'); sys.stdout.buffer.flush(); "
                "sys.stdin.buffer.read(1)"
            ),
            str(decoy_path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    peer: PeerHandle | None = None
    connection: socket.socket | None = None
    try:
        connection, _address = listener.accept()
        assert child.stdout is not None
        assert child.stdout.read(1) == b"R"
        peer = _real_peer(child, control_socket_path=str(expected_path))

        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                pass

        assert caught.value.code == "peer_unexpected_unix_socket"
        assert child.poll() is None
        child.communicate(input=b"X", timeout=5)
        assert child.returncode == 0
    finally:
        if peer is not None:
            peer.close()
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
        if connection is not None:
            connection.close()
        listener.close()


def test_containment_hold_rejects_shared_fd_table_and_resumes_peer(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    status = inspector.proc_root / str(PID) / "status"
    (inspector.proc_root / "4999").mkdir()
    signals: list[int] = []

    def send_signal(_pidfd: int, signum: int) -> None:
        signals.append(signum)
        state = "T (stopped)" if signum == signal.SIGSTOP else "S (sleeping)"
        rows = status.read_text(encoding="ascii").splitlines(keepends=True)
        status.write_text(
            "".join(f"State:\t{state}\n" if row.startswith("State:") else row for row in rows),
            encoding="ascii",
        )

    inspector._pidfd_send_signal = send_signal
    inspector._fd_table_shared = lambda peer_pid, other_pid: (
        peer_pid == PID and other_pid == 4999
    )
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    try:
        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                pass

        assert caught.value.code == "peer_fd_table_shared"
        assert signals == [signal.SIGSTOP, signal.SIGCONT]
        assert "State:\tS (sleeping)" in status.read_text(encoding="ascii")
    finally:
        peer.close()


def test_containment_hold_rejects_control_socket_duplicated_into_another_process(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    status = inspector.proc_root / str(PID) / "status"
    foreign_fd_root = inspector.proc_root / "4999" / "fd"
    foreign_fd_root.mkdir(parents=True)
    (foreign_fd_root / "7").symlink_to("socket:[1234]")
    signals: list[int] = []

    def send_signal(_pidfd: int, signum: int) -> None:
        signals.append(signum)
        state = "T (stopped)" if signum == signal.SIGSTOP else "S (sleeping)"
        rows = status.read_text(encoding="ascii").splitlines(keepends=True)
        status.write_text(
            "".join(
                f"State:\t{state}\n" if row.startswith("State:") else row
                for row in rows
            ),
            encoding="ascii",
        )

    inspector._pidfd_send_signal = send_signal
    inspector._fd_table_shared = lambda _peer_pid, _other_pid: False
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    try:
        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                pass

        assert caught.value.code == "peer_socket_shared"
        assert signals == [signal.SIGSTOP, signal.SIGCONT]
        assert "State:\tS (sleeping)" in status.read_text(encoding="ascii")
    finally:
        peer.close()


def test_containment_hold_revalidates_fd_inventory_before_resuming_peer(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    status = inspector.proc_root / str(PID) / "status"
    signals: list[int] = []

    def send_signal(_pidfd: int, signum: int) -> None:
        signals.append(signum)
        state = "T (stopped)" if signum == signal.SIGSTOP else "S (sleeping)"
        rows = status.read_text(encoding="ascii").splitlines(keepends=True)
        status.write_text(
            "".join(f"State:\t{state}\n" if row.startswith("State:") else row for row in rows),
            encoding="ascii",
        )

    inspector._pidfd_send_signal = send_signal
    inspector._fd_table_shared = lambda _peer_pid, _other_pid: False
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    descriptor = inspector.proc_root / str(PID) / "fd/0"
    try:
        with pytest.raises(GuardError) as caught:
            with peer.containment_hold():
                descriptor.unlink()
                descriptor.symlink_to("/dev/zero")

        assert caught.value.code == "peer_fd_table_changed"
        assert signals == [signal.SIGSTOP, signal.SIGCONT]
        assert "State:\tS (sleeping)" in status.read_text(encoding="ascii")
    finally:
        peer.close()


def test_peer_revalidation_detects_death_reexec_and_cgroup_drift(tmp_path: Path) -> None:
    inspector, executable, alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    try:
        alive["value"] = False
        with pytest.raises(GuardError) as caught:
            peer.assert_unchanged()
        assert caught.value.code == "peer_dead"

        alive["value"] = True
        replacement = tmp_path / "replacement"
        replacement.write_bytes(executable.read_bytes())
        replacement.chmod(0o555)
        link = inspector.proc_root / str(PID) / "exe"
        link.unlink()
        link.symlink_to(replacement)
        with pytest.raises(GuardError) as caught:
            peer.assert_unchanged()
        assert caught.value.code == "peer_executable_changed"

        link.unlink()
        link.symlink_to(executable)
        (inspector.proc_root / str(PID) / "cgroup").write_text(
            "0::/system.slice/slurmstepd.scope/job_999/step_batch/user/task_0\n",
            encoding="ascii",
        )
        with pytest.raises(GuardError) as caught:
            peer.assert_unchanged()
        assert caught.value.code == "peer_cgroup_changed"
    finally:
        peer.close()


def test_peer_adopts_only_exact_trusted_service_cgroup_once(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    trusted = PurePosixPath(f"{CGROUP}/loom-builder/trusted-service")
    cgroup_file = inspector.proc_root / str(PID) / "cgroup"
    try:
        cgroup_file.write_text(f"0::{trusted}\n", encoding="ascii")
        peer.adopt_trusted_service_cgroup()
        assert peer.cgroup_relative == trusted
        peer.assert_unchanged()

        cgroup_file.write_text(
            f"0::{CGROUP}/loom-builder/build-egress\n", encoding="ascii"
        )
        with pytest.raises(GuardError) as caught:
            peer.adopt_trusted_service_cgroup()
        assert caught.value.code == "peer_cgroup_transition_invalid"
    finally:
        peer.close()


def test_capture_after_guard_restart_recovers_exact_trusted_service_membership(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    trusted = PurePosixPath(f"{CGROUP}/loom-builder/trusted-service")
    (inspector.proc_root / str(PID) / "cgroup").write_text(
        f"0::{trusted}\n", encoding="ascii"
    )

    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    try:
        assert peer.batch_cgroup_relative == PurePosixPath(CGROUP)
        assert peer.cgroup_relative == trusted
        assert peer.job_id == "123"
        peer.assert_unchanged()
        cgroup_root = tmp_path / "cgroup"
        batch_path = cgroup_root / CGROUP.removeprefix("/")
        trusted_path = batch_path / "loom-builder" / "trusted-service"
        trusted_path.mkdir(parents=True)
        (batch_path / "cgroup.type").write_text("domain\n", encoding="ascii")
        (batch_path / "cgroup.procs").write_text("", encoding="ascii")
        (trusted_path / "cgroup.type").write_text("domain\n", encoding="ascii")
        (trusted_path / "cgroup.procs").write_text(f"{PID}\n", encoding="ascii")
        batch = derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        assert batch.path == batch_path
        with pytest.raises(GuardError) as caught:
            peer.adopt_trusted_service_cgroup()
        assert caught.value.code == "peer_cgroup_transition_invalid"
    finally:
        peer.close()


def test_recapture_pid_uses_persisted_uid_gid_boundary_without_a_socket(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)

    peer = inspector.capture_pid(PID, expected_uid=UID, expected_gid=GID)
    try:
        assert (peer.pid, peer.uid, peer.gid, peer.job_id) == (PID, UID, GID, "123")
        peer.assert_unchanged()
    finally:
        peer.close()

    with pytest.raises(GuardError) as caught:
        inspector.capture_pid(PID, expected_uid=0, expected_gid=GID)
    assert caught.value.code == "peer_credentials_invalid"


def test_derive_batch_cgroup_requires_exact_single_process_task(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    cgroup_root = tmp_path / "cgroup"
    task = cgroup_root / CGROUP.removeprefix("/")
    task.mkdir(parents=True)
    (task / "cgroup.type").write_text("domain\n", encoding="ascii")
    (task / "cgroup.procs").write_text(f"{PID}\n", encoding="ascii")
    try:
        batch = derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        assert batch.path == task
        assert batch.inode == task.stat().st_ino

        (task / "cgroup.procs").write_text(f"{PID}\n9999\n", encoding="ascii")
        with pytest.raises(GuardError) as caught:
            derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        assert caught.value.code == "batch_cgroup_processes_invalid"
    finally:
        peer.close()


def test_derive_batch_cgroup_rejects_nonbatch_or_symlink_path(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    cgroup_file = inspector.proc_root / str(PID) / "cgroup"
    cgroup_file.write_text(
        "0::/system.slice/slurmstepd.scope/job_123/step_extern/user/task_0\n",
        encoding="ascii",
    )
    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    assert caught.value.code == "peer_cgroup_invalid"


def test_projection_request_is_exact_phase2b1_wire_contract(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection(), CREDENTIALS)  # type: ignore[arg-type]
    cgroup_root = tmp_path / "cgroup"
    task = cgroup_root / CGROUP.removeprefix("/")
    task.mkdir(parents=True)
    (task / "cgroup.type").write_text("domain\n", encoding="ascii")
    (task / "cgroup.procs").write_text(f"{PID}\n", encoding="ascii")
    try:
        batch = derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        request = projection_request(
            grant_id=GRANT,
            request_id=REQUEST,
            observed_at=datetime(2026, 9, 2, 12, 30, tzinfo=UTC),
            node_name="trt-eai-oldlab-3",
            node_boot_id=BOOT,
            cluster_id="oldlab",
            cpu_arch="x86_64",
            slurm_request_sha256="a" * 64,
            slurm_qos="loom-task-image-builder-rootless-oldlab",
            peer=peer,
            batch=batch,
        )
        assert request == {
            "schema_version": 1,
            "request_id": str(REQUEST),
            "grant_id": str(GRANT),
            "observed_at": "2026-09-02T12:30:00Z",
            "node_name": "trt-eai-oldlab-3",
            "node_boot_id": str(BOOT),
            "slurm_cluster_id": "oldlab",
            "slurm_job_id": "123",
            "supervisor_pid": PID,
            "supervisor_uid": UID,
            "supervisor_gid": GID,
            "supervisor_executable_sha256": peer.executable_sha256,
            "cgroup_path": f"/sys/fs/cgroup{CGROUP}",
            "cgroup_inode": batch.inode,
            "submitting_identity": "loom-builder",
            "slurm_account": "loom-task-builder",
            "slurm_partition": "loom-task-builder",
            "slurm_qos": "loom-task-image-builder-rootless-oldlab",
            "cpu_arch": "x86_64",
            "slurm_request_sha256": "a" * 64,
        }
    finally:
        peer.close()
