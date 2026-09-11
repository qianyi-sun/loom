"""Fixed commands for preverified native material; not installation authority.

This small exec helper must remain independent of network/database dependencies.
It runs inside a broker-owned child, never in the live deadline monitor. The
installed caller must verify immutable paths, allocation and one-shot ownership.
"""

from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from loom_capacity_executor.native_parent_death import bind_native_parent_death


@dataclass(frozen=True, slots=True)
class NativeRunscLayout:
    runsc: Path
    state_root: Path
    bundle_root: Path
    claim_digest: str

    def __post_init__(self) -> None:
        for path in (self.runsc, self.state_root, self.bundle_root):
            if (not isinstance(path, Path) or not path.is_absolute() or path == Path("/")
                or ".." in path.parts or any(character in str(path) for character in ("\x00", "\n", "\r"))):
                raise ValueError("native runtime paths must be absolute and non-root")
        paths = (self.runsc, self.state_root, self.bundle_root)
        if any(left == right or left in right.parents or right in left.parents
            for index, left in enumerate(paths) for right in paths[index + 1:]):
            raise ValueError("native runtime paths must be disjoint")
        if not isinstance(self.claim_digest, str) or re.fullmatch(r"[0-9a-f]{64}", self.claim_digest) is None:
            raise ValueError("native runtime claim digest is invalid")

    def identity(self, role: str) -> str:
        if role not in {"pause", "buildkit", "client"}:
            raise ValueError("native runtime role is invalid")
        return f"loom-native-{role}-{self.claim_digest}"

    def command(self, operation: str, role: str) -> tuple[str, ...]:
        identity = self.identity(role)
        prefix = (str(self.runsc), f"--root={self.state_root}", "--platform=kvm",
            "--network=none", "--ignore-cgroups=true", "--gvisor-marker-file=true",
            "--host-settings=check", "--sidecar-release-enforcement-policy=ALWAYS",
            "--host-uds=none", "--host-fifo=none", "--directfs=false",
            "--allow-suid=false", "--oci-seccomp=true")
        if operation == "start":
            return (*prefix, "run", f"--bundle={self.bundle_root / role}", identity)
        if operation == "state":
            return (*prefix, "state", identity)
        if operation == "delete":
            return (*prefix, "delete", "--force", identity)
        if operation == "ready" and role == "buildkit":
            return (*prefix, "exec", identity, "/usr/bin/test", "-S", "/var/run/loom-buildkit/buildkitd.sock")
        raise ValueError("native runtime operation is invalid")

    def arguments(self) -> tuple[str, ...]:
        """Trusted prevalidated configuration only, never supervisor IPC input."""
        return ("--runsc", str(self.runsc), "--state-root", str(self.state_root),
            "--bundle-root", str(self.bundle_root), "--claim-digest", self.claim_digest)


def exec_native_runsc(layout: NativeRunscLayout, *, operation: str, role: str,
    expected_parent_pid: int, deadline_boottime_ns: int,
) -> NoReturn:
    bind_native_parent_death(expected_parent_pid)
    layout.__post_init__()
    command = layout.command(operation, role)
    if operation == "start":
        if type(deadline_boottime_ns) is not int or deadline_boottime_ns <= 0:
            raise ValueError("native start requires a deadline")
        try:
            now = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
        except (AttributeError, OSError, ValueError):
            raise RuntimeError("native start clock is unavailable") from None
        if type(now) is not int or not 0 <= now < deadline_boottime_ns:
            raise RuntimeError("native start deadline has expired")
    elif type(deadline_boottime_ns) is not int or deadline_boottime_ns != 0:
        raise ValueError("native control command must not carry start permission")
    os.execve(command[0], command, {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"})


def layout_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runsc", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--bundle-root", type=Path, required=True)
    parser.add_argument("--claim-digest", required=True)
    parser.add_argument("--expected-parent", type=int, required=True)
    return parser


def main() -> NoReturn:
    parser = layout_parser()
    parser.add_argument("--operation", choices=("start", "state", "ready", "delete"), required=True)
    parser.add_argument("--role", choices=("pause", "buildkit", "client"), required=True)
    parser.add_argument("--deadline", type=int, required=True)
    args = parser.parse_args()
    exec_native_runsc(NativeRunscLayout(args.runsc, args.state_root, args.bundle_root, args.claim_digest),
        operation=args.operation, role=args.role, expected_parent_pid=args.expected_parent,
        deadline_boottime_ns=args.deadline)


if __name__ == "__main__":
    main()
