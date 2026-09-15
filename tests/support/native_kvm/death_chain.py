"""Disposable multi-process kernel death chain; not an installed supervisor."""

import os
import subprocess
import sys

from execute import native_runtime_command
from native_parent_death import bind_native_parent_death

role, expected_parent, sandbox_id = sys.argv[1:]
bind_native_parent_death(int(expected_parent))
if role == "root":
    command = [*native_runtime_command(), "run", "--bundle=/fixtures/pause", sandbox_id]
    os.execv(command[0], command)
else:
    assert role in {"supervisor", "broker"}
    child = subprocess.Popen([sys.executable, __file__, "broker" if role == "supervisor" else "root",
        str(os.getpid()), sandbox_id], close_fds=True)
    raise SystemExit(child.wait())
