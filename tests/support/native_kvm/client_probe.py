"""Trusted negative probes in a copy of the production client OCI boundary."""

import ctypes
import errno
import os
import socket
from pathlib import Path

from loom.personal_dev_sandbox_builder import _verify_client_identity

_verify_client_identity()
for path in ("/", "/input", "/var/run/loom-buildkit"):
    assert os.statvfs(path).f_flag & os.ST_RDONLY, path
try:
    Path("/var/run/loom-buildkit/client-write").write_bytes(b"forbidden")
except OSError as error:
    assert error.errno == errno.EROFS, error
else:
    raise AssertionError("client wrote to the shared socket mount")
assert not Path("/tmp/sidecar-private").exists()
Path("/output/probe").write_text("client-owned output")
libc = ctypes.CDLL(None, use_errno=True)
assert libc.unshare(0x10000000) == -1  # CLONE_NEWUSER
assert ctypes.get_errno() == errno.EPERM
# getpriority needs no capability and is implemented by gVisor. Its denial
# therefore checks deny-default filtering, not only lack of privilege.
ctypes.set_errno(0)
assert libc.getpriority(0, 0) == -1
assert ctypes.get_errno() == errno.EPERM
try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
        raise AssertionError("client created a forbidden network socket")
except OSError as error:
    assert error.errno == errno.EPERM, error
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
    connection.connect("/var/run/loom-buildkit/buildkitd.sock")
print("native-client-isolation-probes-ok", flush=True)
