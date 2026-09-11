"""Real RootlessKit activation-FD and active-parent-death fixture only."""

import ctypes
import fcntl
import importlib.util
import json
import os
import select
import signal
import socket
import subprocess
import sys
from contextlib import suppress
from pathlib import Path


def main():
    mode = sys.argv[1]
    if mode == "io-parent":
        with socket.socket(fileno=int(sys.argv[2])) as report:
            child = subprocess.Popen([sys.executable, __file__, "launch", str(os.getpid()),
                sys.argv[3], sys.argv[4], sys.argv[5]],
                pass_fds=(int(sys.argv[3]), int(sys.argv[4])))
            report.send(str(child.pid).encode())
            report.recv(1)  # No deadline can stand in for the tested death chain.
    elif mode == "launch":
        # Builder fixture lacks the executor package's HTTP dependencies. Load
        # only this dependency-free production primitive from the trusted mount.
        spec = importlib.util.spec_from_file_location("native_parent_death", "/trusted-src/loom_capacity_executor/native_parent_death.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if sys.argv[5] != "io-kill-unbound":
            module.bind_native_parent_death(int(sys.argv[2]))
        copied = [fcntl.fcntl(int(value), fcntl.F_DUPFD_CLOEXEC, 10) for value in sys.argv[3:5]]
        for source, target in zip(copied, (3, 4), strict=True):
            os.dup2(source, target, inheritable=True)
        for descriptor in set(copied + [int(value) for value in sys.argv[3:5]]) - {3, 4}:
            os.close(descriptor)
        args = ["/usr/bin/rootlesskit", "--net=none", "--state-dir=/tmp/rootless-probe",
            sys.executable, __file__, "mapped-unbound" if sys.argv[5] == "kill-unbound" else "mapped"]
        os.execve(args[0], args, {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "PYTHONPATH": "/trusted-src",
            "LANG": "C.UTF-8", "LISTEN_FDS": "2", "LISTEN_PID": str(os.getpid())})
    elif mode in {"mapped", "mapped-unbound"}:
        assert os.getuid() == 0 and os.environ["LISTEN_PID"] == str(os.getpid())
        if mode == "mapped-unbound":
            # Negative control only: prove the fixture detects a missing link.
            assert ctypes.CDLL(None, use_errno=True).prctl(1, 0, 0, 0, 0) == 0
        with socket.socket(fileno=3) as control, socket.socket(fileno=4) as artifact:
            assert control.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) == socket.SOCK_SEQPACKET
            assert artifact.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) == socket.SOCK_STREAM
            # Parent-side pidfd observation and exact final cleanup bound the
            # fixture. No competing child timeout may make death falsely pass.
            control.settimeout(None)
            artifact.settimeout(5)
            control.set_inheritable(False)
            artifact.set_inheritable(False)
            subprocess.run([sys.executable, "/test-support/rootless_ownership.py", "mapped"],
                check=True, timeout=10)
            control.send(json.dumps({"pid": os.getpid(), "uid": os.getuid()}).encode())
            if control.recv(64) == b"export":
                artifact.sendall(Path("/tmp/native-ownership-output/build/artifact").read_bytes())
    elif mode in {"export", "kill", "kill-unbound", "io-kill", "io-kill-unbound"}:
        assert os.getuid() == 1000
        control, child_control = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        artifact, child_artifact = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        child = None
        pidfd = None
        rootless_pidfd = None
        report, child_report = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        try:
            control.settimeout(5)
            artifact.settimeout(5)
            if mode.startswith("io-kill"):
                report.settimeout(5)
                child = subprocess.Popen([sys.executable, __file__, "io-parent", str(child_report.fileno()),
                    str(child_control.fileno()), str(child_artifact.fileno()), mode],
                    pass_fds=(child_report.fileno(), child_control.fileno(), child_artifact.fileno()))
                child_report.close()
                rootless_pidfd = os.pidfd_open(int(report.recv(64)))
            else:
                child = subprocess.Popen([sys.executable, __file__, "launch", str(os.getpid()),
                    str(child_control.fileno()), str(child_artifact.fileno()), mode],
                    pass_fds=(child_control.fileno(), child_artifact.fileno()))
            child_control.close()
            child_artifact.close()
            ready = json.loads(control.recv(1024))
            assert ready["uid"] == 0 and ready["pid"] != child.pid
            pidfd = os.pidfd_open(ready["pid"])
            poller = select.poll()
            poller.register(pidfd, select.POLLIN)
            assert child.poll() is None and not poller.poll(0), "mapped child was not live at the tested boundary"
            if mode == "export":
                control.send(b"export")
                chunks = bytearray()
                while data := artifact.recv(64):
                    chunks.extend(data)
                    assert len(chunks) <= 64
                assert chunks == b"fixture-artifact"
                assert child.wait(timeout=5) == 0
                print("rootless-private-artifact-transfer-ok", flush=True)
            else:
                child.kill()
                child.wait(timeout=5)
                if mode in {"kill-unbound", "io-kill-unbound"}:
                    assert not poller.poll(300), "negative control did not leave a live child"
                    print("rootless-unbound-child-survival-detected", flush=True)
                else:
                    assert poller.poll(5000), "mapped child survived RootlessKit parent death"
                    assert artifact.recv(64) == b""
                    print("rootless-parent-death-stopped-mapped-child", flush=True)
                if rootless_pidfd is not None:
                    rootless_poller = select.poll()
                    rootless_poller.register(rootless_pidfd, select.POLLIN)
                    if mode == "io-kill-unbound":
                        assert not rootless_poller.poll(0), "unbound RootlessKit parent unexpectedly stopped"
                    else:
                        assert rootless_poller.poll(5000), "RootlessKit survived outer IO death"
                        print("outer-io-death-stopped-rootless-chain", flush=True)
        finally:
            if child is not None:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=5)
            for descriptor in (rootless_pidfd, pidfd):
                if descriptor is not None:
                    with suppress(ProcessLookupError):
                        signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                    os.close(descriptor)
            for peer in (control, child_control, artifact, child_artifact, report, child_report):
                peer.close()
    else:
        raise AssertionError("unknown fixture mode")


if __name__ == "__main__":
    main()
