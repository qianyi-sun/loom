"""Read actual legacy supervisor process retirement, not scheduler/SQL closure.

Timer disablement and privileged writer exclusion belong to the enclosing
cutover. This read-only probe neither stops units nor signals a process. It
cannot prove that previously accepted Slurm or SQL requests have retired.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
import re
import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from uuid import UUID

from loom_cli.rollout.external_supervisor_readiness import ExternalSupervisorArtifact

from .protected_application_admission_recovery import admission_record_digest

_PROPERTIES = frozenset({"Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlPID",
    "ControlGroup", "Job", "KillMode", "Delegate", "ExecMainStartTimestampMonotonic",
    "InvocationID", "NeedDaemonReload", "Transient", "DropInPaths", "FragmentPath"})
_UNIT_ROOTS = {"oldlab": Path("/var/lib/loom-staging-rollout/.config/systemd/user"),
               "gb10": Path("/var/lib/loom-rollout/.config/systemd/user")}


def controller_cgroup_empty(group: str) -> bool:
    """Read a pinned cgroup-v2 path; disappearance counts only below a real mount."""
    if not isinstance(group, str) or not 1 <= len(group) <= 4096:
        raise ValueError("legacy controller cgroup path is invalid")
    path = Path(group)
    if (not group.startswith("/") or str(path) != group
        or group == "/" or ".." in path.parts or any(ord(char) < 32 or ord(char) == 127 for char in group)):
        raise ValueError("legacy controller cgroup path is invalid")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open("/sys/fs/cgroup", flags)
    try:
        # statfs first proves that a missing child is in the cgroup filesystem,
        # rather than a missing/unmounted host path. Linux amd64/arm64 use long
        # for the first statfs field; the oversized buffer holds either ABI.
        libc = ctypes.CDLL(None, use_errno=True)
        statfs = libc.fstatfs
        statfs.argtypes = [ctypes.c_int, ctypes.c_void_p]
        statfs.restype = ctypes.c_int
        buffer = ctypes.create_string_buffer(256)
        if statfs(descriptor, buffer) != 0 or ctypes.c_long.from_buffer(buffer).value != 0x63677270:
            raise RuntimeError("legacy controller cgroup-v2 mount is unavailable")
        for part in path.parts[1:]:
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                return True
            os.close(descriptor)
            descriptor = child
        try:
            events = os.open("cgroup.events", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=descriptor)
        except FileNotFoundError:
            # A removed, still-open cgroup directory has no events file. A live
            # cgroup-v2 non-root directory must have one; distinguish the two.
            if os.fstat(descriptor).st_nlink == 0:
                return True
            raise RuntimeError("legacy controller cgroup events are unavailable") from None
        try:
            payload = os.read(events, 4097)
        finally:
            os.close(events)
        if len(payload) > 4096:
            raise RuntimeError("legacy controller cgroup events are oversized")
        values: dict[str, str] = {}
        for line in payload.decode("ascii").splitlines():
            fields = line.split()
            if len(fields) != 2 or fields[0] in values:
                raise RuntimeError("legacy controller cgroup events changed")
            values[fields[0]] = fields[1]
        if values.get("populated") not in {"0", "1"}:
            raise RuntimeError("legacy controller cgroup population is unavailable")
        return values["populated"] == "0"
    finally:
        os.close(descriptor)


def legacy_service_processes_retired(properties: Mapping[str, str]) -> bool:
    """Require no manager job, main/control process, delegated or populated group."""
    if set(properties) != _PROPERTIES:
        raise ValueError("legacy controller process properties are incomplete")
    if any(not isinstance(value, str) or len(value) > 512 for value in properties.values()):
        raise ValueError("legacy controller process properties are invalid")
    if not _inert_properties(properties):
        return False
    group = properties["ControlGroup"]
    return group == "" or controller_cgroup_empty(group)


def _inert_properties(properties: Mapping[str, str]) -> bool:
    return all(properties.get(name) == value for name, value in {
        "LoadState": "loaded", "ActiveState": "inactive", "SubState": "dead",
        "MainPID": "0", "ControlPID": "0", "Job": "", "KillMode": "control-group",
        "Delegate": "no", "NeedDaemonReload": "no", "Transient": "no", "DropInPaths": "",
    }.items())


def observe_legacy_controller_processes(
    *, pool: str, expected_unit_sha256: str,
    read_unit: Callable[[str], bytes | None], environment: Mapping[str, str],
) -> dict[str, object]:
    """Bracket process/cgroup evidence with exact source and systemd readbacks.

    The installed caller supplies its protected unit store and fixed service-user
    environment. This supports only the two staging trial-pool supervisors; it
    does not stop or certify independent task-image builders or personal services.
    """
    if pool not in {"oldlab", "gb10"} or re.fullmatch(r"[0-9a-f]{64}", expected_unit_sha256) is None:
        raise ValueError("legacy controller process binding is invalid")
    unit = f"loom-autoscaler-{pool}-staging.service"

    def source() -> None:
        payload = read_unit(unit)
        if not isinstance(payload, bytes) or hashlib.sha256(payload).hexdigest() != expected_unit_sha256:
            raise RuntimeError("legacy controller source changed")

    def capture() -> dict[str, str]:
        result = subprocess.run(["systemctl", "--user", "show", unit, "--no-pager",
            *["--property=" + name for name in sorted(_PROPERTIES)]],
            capture_output=True, env=dict(environment), timeout=15, check=False)
        if result.returncode != 0 or result.stderr or len(result.stdout) > 16 * 1024:
            raise RuntimeError("legacy controller process observation failed")
        properties: dict[str, str] = {}
        for line in result.stdout.decode("utf-8").splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in _PROPERTIES or key in properties:
                raise RuntimeError("legacy controller process response changed")
            properties[key] = value
        if (set(properties) != _PROPERTIES or properties["Id"] != unit
            or properties["FragmentPath"] != str(_UNIT_ROOTS[pool] / unit)):
            raise RuntimeError("legacy controller process identity changed")
        return properties

    boot = str(UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip()))
    source()
    properties = capture()
    retired = legacy_service_processes_retired(properties)
    if capture() != properties:
        raise RuntimeError("legacy controller changed during process observation")
    source()
    if str(UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip())) != boot:
        raise RuntimeError("legacy controller boot changed during process observation")
    record: dict[str, object] = {"schema_version": 1, "pool": pool, "boot_id": boot,
        "unit_sha256": expected_unit_sha256, "properties": properties, "processes_retired": retired}
    return {**record, "evidence_sha256": admission_record_digest(record)}


def validate_controller_process_observation(value: object, artifact: ExternalSupervisorArtifact) -> dict[str, object]:
    """Validate a protected remote read without inspecting its cgroup locally."""
    if not isinstance(value, dict) or set(value) != {"schema_version", "candidate_sha", "candidate_tree",
        "artifact_digest", "canonical_digest", "process_evidence", "evidence_sha256"}:
        raise ValueError("legacy controller process envelope is invalid")
    supervisors = [item for item in artifact.supervisors if item.pool_name in _UNIT_ROOTS]
    if len(supervisors) != 1:
        raise ValueError("legacy controller process artifact scope is invalid")
    supervisor = supervisors[0]
    pool = supervisor.pool_name
    process = value["process_evidence"]
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1
        or value["candidate_sha"] != artifact.candidate_sha or value["candidate_tree"] != artifact.candidate_tree
        or value["artifact_digest"] != artifact.artifact_digest
        or not isinstance(value["canonical_digest"], str)
        or re.fullmatch(r"[0-9a-f]{64}", value["canonical_digest"]) is None
        or value["canonical_digest"] == "0" * 64
        or not isinstance(process, dict) or set(process) != {"schema_version", "pool", "boot_id",
            "unit_sha256", "properties", "processes_retired", "evidence_sha256"}):
        raise ValueError("legacy controller process binding changed")
    properties = process["properties"]
    if (type(process["schema_version"]) is not int or process["schema_version"] != 1
        or process["pool"] != pool or process["unit_sha256"] != artifact.unit_sha256[supervisor.service_name]
        or type(process["processes_retired"]) is not bool
        or not isinstance(properties, dict) or set(properties) != _PROPERTIES
        or any(not isinstance(item, str) or len(item) > 512 for item in properties.values())
        or properties["Id"] != supervisor.service_name
        or properties["FragmentPath"] != str(_UNIT_ROOTS[pool] / supervisor.service_name)
        or (process["processes_retired"] and not _inert_properties(properties))):
        raise ValueError("legacy controller process evidence changed")
    boot = process["boot_id"]
    if not isinstance(boot, str) or str(UUID(boot)) != boot or UUID(boot).int == 0:
        raise ValueError("legacy controller boot identity is invalid")
    if (process["evidence_sha256"] != admission_record_digest({key: item for key, item in process.items() if key != "evidence_sha256"})
        or value["evidence_sha256"] != admission_record_digest({key: item for key, item in value.items() if key != "evidence_sha256"})):
        raise ValueError("legacy controller process evidence digest changed")
    return value
