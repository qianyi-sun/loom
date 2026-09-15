"""Fixed privileged recovery composition; installed SSH/sudo provenance required.

Not a general CLI: the protected installer supplies a no-argument wrapper with
an immutable policy path/digest and isolated pinned Python. Only the dedicated
management principal may sudo that wrapper. The installer must attest that all
admitted release profiles preserve cgroup nondelegation and confine untrusted
writers/descriptor transfer throughout the job. This code does not install that
authority, enable intake or release capacity.
"""

from __future__ import annotations

import os
import pwd
import select
import stat
import sys
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from loom_capacity_agent.native_recovery import NativeRecoveryPreparationV1
from loom_capacity_build_guard.native_recovery_sender import (
    NativeNodeRecoveryRequestV1,
    NativeNodeRecoveryResultV1,
)
from loom_capacity_executor.native_cgroup_authority import require_native_cgroup_authority
from loom_capacity_executor.native_installed_release import _Observation
from loom_capacity_executor.native_mapped_scratch import _mount_id
from loom_capacity_executor.native_node_recovery_policy import (
    BoundNativeNodeRecovery,
    bind_native_node_recovery,
    read_native_node_recovery_policy,
)
from loom_capacity_executor.native_oci_material import _open_directory
from loom_capacity_executor.native_quarantine_journal import NativeQuarantineJournal
from loom_capacity_executor.native_quarantine_prune import _require_initial_root
from loom_capacity_executor.native_recovery_observation import (
    _read_kernel_text,
    _require_cgroup_mount,
    _require_host_cgroup_namespace,
    read_native_recovery_boot_id,
)
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest

_MAX_REQUEST = 128 * 1024


def _read_request() -> NativeNodeRecoveryRequestV1:
    deadline, wire = time.monotonic() + 10, bytearray()
    descriptor = sys.stdin.fileno()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([descriptor], [], [], remaining)[0]:
            raise ValueError("node recovery request deadline expired")
        part = os.read(descriptor, min(65536, _MAX_REQUEST + 1 - len(wire)))
        if not part:
            break
        wire.extend(part)
        if len(wire) > _MAX_REQUEST:
            raise ValueError("node recovery request exceeds bound")
    request = NativeNodeRecoveryRequestV1.model_validate_json(bytes(wire))
    if canonical_bytes(request) != wire:
        raise ValueError("node recovery request must be canonical")
    return request


def _require_unpopulated(wire: bytes) -> None:
    observed = {}
    for line in wire.decode("ascii").splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0] in observed or parts[1] not in {"0", "1"}:
            raise ValueError("node recovery cgroup events are ambiguous")
        observed[parts[0]] = parts[1]
    if observed.get("populated") != "0":
        raise ValueError("node recovery job subtree is not empty")


def _events(descriptor: int) -> bytes:
    fd = os.open("cgroup.events", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=descriptor)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode) or _mount_id(fd) != _mount_id(descriptor):
            raise ValueError("node recovery events identity changed")
        wire = os.read(fd, 4097)
        if len(wire) > 4096:
            raise ValueError("node recovery events exceed bound")
        return wire
    finally:
        os.close(fd)


@contextmanager
def _quiescent_scope(bound: BoundNativeNodeRecovery, prepared: NativeRecoveryPreparationV1) -> Iterator[None]:
    """Exact extant job only; missing/older cgroups remain explicitly retained.

    Stable emptiness also relies on the protected installed lifetime constraints
    above, not a snapshot or permission change performed during recovery.
    """
    with ExitStack() as stack:
        scope = bound.scope
        _require_host_cgroup_namespace(scope.host)
        if read_native_recovery_boot_id() != scope.host.boot_id:
            raise ValueError("node recovery boot differs from retained scope")
        quarantine = _Observation().directory(Path(scope.quarantine_root), stack)
        if (os.fstat(quarantine).st_dev, _mount_id(quarantine)) != (scope.scratch_device, scope.scratch_mount_id):
            raise ValueError("node recovery scratch mount differs from installed scope")
        rows = [row.split() for row in _read_kernel_text(Path("/proc/self/mountinfo"), 1024**2).splitlines()]
        mounts = [row for row in rows if row and row[0] == str(scope.scratch_mount_id)]
        if (len(mounts) != 1 or "-" not in mounts[0]
            or mounts[0][mounts[0].index("-") + 1] != scope.filesystem):
            raise ValueError("node recovery requires the protected local filesystem")
        cgroup_root = _open_directory(Path("/sys/fs/cgroup"), stack)
        if _require_cgroup_mount(cgroup_root) != prepared.cgroup_mount_id:
            raise ValueError("node recovery cgroup mount differs from historical scope")
        relative = Path(prepared.cgroup_path).relative_to("/")

        def verify() -> None:
            require_native_cgroup_authority(cgroup_root, relative=relative)
            with ExitStack() as observed:
                job = _open_directory(Path("/sys/fs/cgroup") / relative, observed)
                metadata = os.fstat(job)
                if (metadata.st_dev, metadata.st_ino, _mount_id(job)) != (
                    prepared.cgroup_device, prepared.cgroup_inode, prepared.cgroup_mount_id):
                    raise ValueError("node recovery job identity differs from retained history")
                _require_unpopulated(_events(job))
        verify()
        yield
        verify()


def run_native_recovery_helper(*, policy_path: str, policy_sha256: str) -> NativeNodeRecoveryResultV1:
    """Called only by the root-owned, pinned, no-argument installed wrapper.

    SUDO_* are trusted only because sudo itself overwrites them for the sole
    allowed command; arbitrary direct root code is already outside this boundary.
    SSH must separately enforce this fixed command and disallow all forwarding,
    environment, alternate login, user RC and other sudo commands.
    """
    _require_initial_root()
    if len(sys.argv) != 1 or os.environ.get("SSH_ORIGINAL_COMMAND", ""):
        raise ValueError("node recovery helper accepts no command arguments")
    policy = read_native_node_recovery_policy(Path(policy_path), expected_sha256=policy_sha256)
    if (os.environ.get("SUDO_USER") != "loom-native-recovery"
        or os.environ.get("SUDO_UID") != str(policy.management_uid)
        or pwd.getpwnam("loom-native-recovery").pw_uid != policy.management_uid):
        raise ValueError("node recovery requires the dedicated management caller")
    request = _read_request()
    digest = canonical_digest(request)
    try:
        bound = bind_native_node_recovery(policy, request)
    except ValueError:
        return NativeNodeRecoveryResultV1(request_sha256=digest, state="retained", reason="identity")
    prepared = request.history.preparation.request.record
    assert isinstance(prepared, NativeRecoveryPreparationV1)
    try:
        with _quiescent_scope(bound, prepared):
            with NativeQuarantineJournal(Path(bound.scope.quarantine_root), key=bound.key, source=bound.source,
                identity=bound.identity, locator_wire=bound.locator_wire) as journal:
                journal.reconcile()
    except (ValueError, OSError):
        return NativeNodeRecoveryResultV1(request_sha256=digest, state="retained", reason="quiescence")
    return NativeNodeRecoveryResultV1(request_sha256=digest, state="completed", reason="complete")
