"""Stdlib-only native containment protocol primitives.

This file may be installed byte-identically in an immutable root-owned bundle
and loaded using Python -I -S -B. Keep it free of package/site imports. Scheduler
facts alone are not authority to delegate a cgroup or start a runtime.
"""

from __future__ import annotations

import ctypes
import fcntl
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import cast
from uuid import UUID

_MAX_BYTES = 1024 * 1024
_MEMORY = re.compile(r"([1-9][0-9]{0,18})([KMGT])", re.ASCII)
_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?", re.ASCII)
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", re.ASCII)
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}", re.ASCII)
_OWNERSHIP = re.compile(r"[A-Za-z0-9_-]{43,4096}", re.ASCII)
_EXPECTATION_FIELDS = frozenset({
    "job_id", "cluster", "hostname", "submitter", "uid", "account", "partition",
    "qos", "cpus", "memory_bytes", "ownership_token",
})
_MAX_SIGNATURE_INPUT_BYTES = 32768
_MAX_OPENSSL_BYTES = 64 * 1024 * 1024
# RFC 8410 SubjectPublicKeyInfo header for one raw 32-byte Ed25519 public key.
_ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")
_DIGEST = re.compile(r"[0-9a-f]{64}", re.ASCII)
_KEY_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,127}", re.ASCII)
_PREPARATION_DOMAIN = b"loom.native-worker-cgroup-preparation-signature/v1\0"
_POLICY_FIELDS = frozenset({
    "schema", "environment", "pool_id", "pool_generation", "node", "cluster", "submitter", "uid",
    "account", "partition", "qos", "authority_incarnation", "execution_epoch",
    "execution_manifest_sha256", "executor_id", "executor_incarnation",
    "controller_authority_sha256", "trusted_fleet_release_sha256", "issuer_key_id",
    "issuer_public_key_hex", "not_before_ms", "expires_at_ms", "profiles",
})
_PREPARATION_FIELDS = frozenset({
    "schema", "purpose", "policy_sha256", "grant_id", "grant_generation", "intent_id", "binding_sha256",
    "physical_binding_sha256", "bootstrap_registration_epoch", "bootstrap_sha256",
    "agent_incarnation", "ownership_evidence_sha256", "manager_observation_sha256",
    "bootstrap_observation_sha256", "scheduler_observation_sha256", "profile_digest",
    "pids_max", "job_id", "ownership_token", "cpus", "memory_bytes", "submitted_at_ms",
    "started_at_ms", "issued_at_ms", "expires_at_ms",
})


def _canonical_native(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def _closed_mapping(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise NativeContainmentVerificationError("native preparation fields are invalid")
    return cast(dict[str, object], value)


def _closed_native_document(raw: bytes, fields: frozenset[str]) -> dict[str, object]:
    if type(raw) is not bytes or not 0 < len(raw) <= _MAX_SIGNATURE_INPUT_BYTES:
        raise NativeContainmentVerificationError("native preparation document exceeds its bound")
    value = _closed_mapping(json.loads(raw.decode("ascii"), object_pairs_hook=_object, parse_constant=_constant), fields)
    if _canonical_native(value) != raw:
        raise NativeContainmentVerificationError("native preparation document is not canonical")
    return value


def _native_text(value: object, pattern: re.Pattern[str]) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise NativeContainmentVerificationError("native preparation identity is invalid")
    return value


def _native_quantity(value: object, *, minimum: int = 1, maximum: int = (1 << 53) - 1) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise NativeContainmentVerificationError("native preparation quantity is invalid")
    return value


def _native_uuid(value: object) -> None:
    if type(value) is not str or str(UUID(value)) != value or UUID(value).int == 0:
        raise NativeContainmentVerificationError("native preparation UUID is invalid")


def _native_millis(value: datetime) -> int:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise NativeContainmentVerificationError("native observation clock is invalid")
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def _validate_native_policy(policy: dict[str, object]) -> dict[str, int]:
    if policy["schema"] != "loom.native-worker-cgroup-policy/v1" or policy["pool_id"] not in {"oldlab", "gb10"}:
        raise NativeContainmentVerificationError("native root policy schema or pool is invalid")
    for name in ("environment", "node", "cluster", "submitter", "account", "partition", "qos", "executor_id"):
        _native_text(policy[name], _IDENTIFIER)
    for name in ("authority_incarnation", "executor_incarnation"):
        _native_uuid(policy[name])
    for name in ("execution_manifest_sha256", "controller_authority_sha256", "trusted_fleet_release_sha256", "issuer_public_key_hex"):
        _native_text(policy[name], _DIGEST)
    _native_text(policy["issuer_key_id"], _KEY_ID)
    _native_quantity(policy["uid"], minimum=0, maximum=(1 << 31) - 1)
    for name in ("pool_generation", "execution_epoch", "not_before_ms", "expires_at_ms"):
        _native_quantity(policy[name])
    if cast(int, policy["not_before_ms"]) >= cast(int, policy["expires_at_ms"]):
        raise NativeContainmentVerificationError("native root policy interval is invalid")
    profiles = policy["profiles"]
    if not isinstance(profiles, list) or not 1 <= len(profiles) <= 128:
        raise NativeContainmentVerificationError("native root profiles are invalid")
    result: dict[str, int] = {}
    previous = ""
    for value in profiles:
        profile = _closed_mapping(value, frozenset({"profile_digest", "pids_max"}))
        digest = _native_text(profile["profile_digest"], _DIGEST)
        if digest <= previous:
            raise NativeContainmentVerificationError("native root profiles are not uniquely ordered")
        result[digest] = _native_quantity(profile["pids_max"], maximum=1_048_576)
        previous = digest
    return result


def _validate_native_preparation(payload: dict[str, object], policy: dict[str, object], profiles: dict[str, int], policy_digest: str) -> None:
    if (payload["schema"] != "loom.native-worker-cgroup-preparation/v1"
        or payload["purpose"] != "prepare-application-worker-cgroup"
        or payload["policy_sha256"] != policy_digest):
        raise NativeContainmentVerificationError("native preparation purpose or policy differs")
    for name in ("grant_id", "intent_id", "agent_incarnation"):
        _native_uuid(payload[name])
    for name in (
        "binding_sha256", "physical_binding_sha256", "bootstrap_sha256", "ownership_evidence_sha256",
        "manager_observation_sha256", "bootstrap_observation_sha256", "scheduler_observation_sha256", "profile_digest",
    ):
        _native_text(payload[name], _DIGEST)
    for name in ("grant_generation", "bootstrap_registration_epoch", "submitted_at_ms", "started_at_ms", "issued_at_ms", "expires_at_ms"):
        _native_quantity(payload[name])
    _native_text(payload["job_id"], _JOB_ID)
    _native_text(payload["ownership_token"], _OWNERSHIP)
    _native_quantity(payload["cpus"], maximum=65_536)
    _native_quantity(payload["memory_bytes"], maximum=(1 << 63) - 1)
    pids = _native_quantity(payload["pids_max"], maximum=1_048_576)
    if profiles.get(cast(str, payload["profile_digest"])) != pids:
        raise NativeContainmentVerificationError("native preparation profile or PID cap is not approved")
    submitted, started, issued, expires = (cast(int, payload[name]) for name in (
        "submitted_at_ms", "started_at_ms", "issued_at_ms", "expires_at_ms"))
    if (submitted % 1000 or started % 1000 or not submitted <= started <= issued < expires
        or expires - issued > 10_000
        or not cast(int, policy["not_before_ms"]) <= issued < expires <= cast(int, policy["expires_at_ms"])):
        raise NativeContainmentVerificationError("native preparation interval is invalid")


def verify_native_preparation(
    *, signed_packet: bytes, root_policy: bytes, scheduler_raw: str,
    scheduler_observed_at: datetime, openssl_path: str, openssl_sha256: str,
) -> dict[str, object]:
    """Authenticate preparation scope and fresh exact scheduler incarnation.

    Policy and executable pins MUST come from the independently root-installed
    bundle, never the packet. The guard supplies its own bounded scheduler read
    and pre-query timestamp. This does not consume a grant, delegate controllers,
    inspect cgroup ancestry, or replace durable admission/cleanup bookkeeping.
    """
    try:
        policy = _closed_native_document(root_policy, _POLICY_FIELDS)
        profiles = _validate_native_policy(policy)
        packet = _closed_native_document(signed_packet, frozenset({"schema", "key_id", "payload", "signature_hex"}))
        if packet["schema"] != "loom.native-worker-cgroup-envelope/v1" or packet["key_id"] != policy["issuer_key_id"]:
            raise NativeContainmentVerificationError("native preparation envelope or issuer differs")
        payload = _closed_mapping(packet["payload"], _PREPARATION_FIELDS)
        _validate_native_preparation(payload, policy, profiles, hashlib.sha256(root_policy).hexdigest())
        signature = _native_text(packet["signature_hex"], re.compile(r"[0-9a-f]{128}", re.ASCII))
        message = _PREPARATION_DOMAIN + _canonical_native({key: value for key, value in packet.items() if key != "signature_hex"})
        now_ms = time.time_ns() // 1_000_000
        observed_ms = _native_millis(scheduler_observed_at)
        if not cast(int, payload["issued_at_ms"]) <= now_ms < cast(int, payload["expires_at_ms"]) or not observed_ms <= now_ms < observed_ms + 10_000:
            raise NativeContainmentVerificationError("native preparation or scheduler observation expired")
        verify_native_ed25519(message=message, signature=bytes.fromhex(signature),
            public_key=bytes.fromhex(cast(str, policy["issuer_public_key_hex"])),
            openssl_path=openssl_path, openssl_sha256=openssl_sha256)
        facts = parse_native_scheduler_record(scheduler_raw, observed_at=scheduler_observed_at, expected={
            "job_id": payload["job_id"], "cluster": policy["cluster"], "hostname": policy["node"],
            "submitter": policy["submitter"], "uid": policy["uid"], "account": policy["account"],
            "partition": policy["partition"], "qos": policy["qos"], "cpus": payload["cpus"],
            "memory_bytes": payload["memory_bytes"], "ownership_token": payload["ownership_token"],
        })
        if (_native_millis(cast(datetime, facts["submitted_at"])) != payload["submitted_at_ms"]
            or _native_millis(cast(datetime, facts["started_at"])) != payload["started_at_ms"]):
            raise NativeContainmentVerificationError("native preparation scheduler incarnation changed")
        final_ms = time.time_ns() // 1_000_000
        if not now_ms <= final_ms < min(cast(int, payload["expires_at_ms"]), observed_ms + 10_000):
            raise NativeContainmentVerificationError("native preparation expired during verification")
        return payload
    except NativeContainmentVerificationError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError, RecursionError):
        raise NativeContainmentVerificationError("native preparation evidence is malformed") from None


class NativeContainmentVerificationError(ValueError):
    """Sanitized verifier failure; packet bytes and process diagnostics stay private."""


def _sealed_bytes(value: bytes, label: str, *, executable: bool = False) -> int:
    flags = getattr(os, "MFD_CLOEXEC", 1) | getattr(os, "MFD_ALLOW_SEALING", 2)
    memfd_create = getattr(os, "memfd_create", None)
    if memfd_create is not None:
        descriptor = int(memfd_create(label, flags))
    else:
        # Some supported standalone Python builds omit the wrapper even on a
        # capable kernel. Use libc's standard API, never an architecture syscall.
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            create = libc.memfd_create
        except AttributeError:
            raise NativeContainmentVerificationError("native sealed input support is unavailable") from None
        create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
        create.restype = ctypes.c_int
        descriptor = int(create(label.encode("ascii"), flags))
        if descriptor < 0:
            raise OSError(ctypes.get_errno(), "native sealed input creation failed")
    try:
        offset = 0
        while offset < len(value):
            written = os.write(descriptor, value[offset:])
            if written <= 0:
                raise NativeContainmentVerificationError("cannot prepare signature verification input")
            offset += written
        os.fchmod(descriptor, 0o500 if executable else 0o400)
        os.lseek(descriptor, 0, os.SEEK_SET)
        seals = 0x0001 | 0x0002 | 0x0004 | 0x0008  # SEAL, SHRINK, GROW, WRITE
        fcntl.fcntl(descriptor, getattr(fcntl, "F_ADD_SEALS", 1033), seals)
        if int(fcntl.fcntl(descriptor, getattr(fcntl, "F_GET_SEALS", 1034))) & seals != seals:
            raise NativeContainmentVerificationError("native verification input was not sealed")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _openssl_snapshot(path: str, digest: str) -> int:
    """Open through root-owned directories and execute only the verified bytes."""
    parsed = PurePosixPath(path)
    if (not parsed.is_absolute() or str(parsed) != path or parsed == PurePosixPath("/")
        or ".." in parsed.parts or "\0" in path
        or re.fullmatch(r"[0-9a-f]{64}", digest, re.ASCII) is None):
        raise NativeContainmentVerificationError("native verifier executable pin is invalid")
    descriptor = os.open("/", os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY)
    try:
        for index, component in enumerate(parsed.parts[1:]):
            parent = os.fstat(descriptor)
            if parent.st_uid != 0 or parent.st_mode & 0o022 or not stat.S_ISDIR(parent.st_mode):
                raise NativeContainmentVerificationError("native verifier executable path is unprotected")
            final = index == len(parsed.parts) - 2
            child = os.open(component,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK | (0 if final else os.O_DIRECTORY),
                dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
            or not info.st_mode & 0o111 or not 0 < info.st_size <= _MAX_OPENSSL_BYTES):
            raise NativeContainmentVerificationError("native verifier executable is unprotected or oversized")
        parts = []
        total = 0
        while total <= _MAX_OPENSSL_BYTES:
            chunk = os.read(descriptor, min(1024 * 1024, _MAX_OPENSSL_BYTES + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            parts.append(chunk)
        value = b"".join(parts)
        if (total != info.st_size or total > _MAX_OPENSSL_BYTES or not value.startswith(b"\x7fELF")
            or hashlib.sha256(value).hexdigest() != digest):
            raise NativeContainmentVerificationError("native verifier executable bytes changed")
        return _sealed_bytes(value, "loom-native-openssl", executable=True)
    finally:
        os.close(descriptor)


def verify_native_ed25519(
    *, message: bytes, signature: bytes, public_key: bytes,
    openssl_path: str, openssl_sha256: str, timeout_seconds: int = 2,
) -> None:
    """Verify a bounded one-shot signature using a fixed protected OpenSSL ELF.

    The caller supplies independently root-approved key and executable pins, not
    packet-selected trust. This verifies cryptography only, not purpose, expiry,
    current authority, or permission to modify a cgroup. All inputs are sealed
    seekable descriptors: OpenSSL Ed25519 rawin cannot rely on streaming stdin.
    """
    if (type(message) is not bytes or not 0 < len(message) <= _MAX_SIGNATURE_INPUT_BYTES
        or type(signature) is not bytes or len(signature) != 64
        or type(public_key) is not bytes or len(public_key) != 32
        or type(openssl_path) is not str or type(openssl_sha256) is not str
        or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 5):
        raise NativeContainmentVerificationError("native signature verification input is invalid")
    descriptors: list[int] = []
    try:
        executable = _openssl_snapshot(openssl_path, openssl_sha256)
        descriptors.append(executable)
        for value, label in (
            (message, "loom-native-message"), (signature, "loom-native-signature"),
            (_ED25519_SPKI_PREFIX + public_key, "loom-native-public-key"),
        ):
            descriptors.append(_sealed_bytes(value, label))
        _, message_fd, signature_fd, key_fd = descriptors
        process = subprocess.Popen([
            "openssl", "pkeyutl", "-verify", "-rawin", "-pubin", "-keyform", "DER",
            "-inkey", f"/proc/self/fd/{key_fd}", "-in", f"/proc/self/fd/{message_fd}",
            "-sigfile", f"/proc/self/fd/{signature_fd}", "-provider", "default",
        ], executable=f"/proc/self/fd/{executable}", pass_fds=tuple(descriptors),
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={"OPENSSL_CONF": "/dev/null", "LC_ALL": "C"}, cwd="/", start_new_session=True)
        try:
            status = process.wait(timeout=timeout_seconds)
        except BaseException:
            # Popen.wait can reap before propagating KeyboardInterrupt. Once
            # reaped, this PID is no longer ours and may already be recycled.
            # Only this function owns/reaps this child; an unreaped PID cannot
            # be reused between poll and signaling its private process group.
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait(timeout=2)
            raise
        if status != 0:
            raise NativeContainmentVerificationError("native signature verification failed")
    except (OSError, subprocess.SubprocessError):
        raise NativeContainmentVerificationError("native signature verifier is unavailable") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


class NativeSlurmObservationError(ValueError):
    """The exact current native allocation could not be established."""


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise NativeSlurmObservationError("duplicate native scheduler JSON key")
        result[key] = value
    return result


def _integer(value: object, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value < 1 << 63:
        raise NativeSlurmObservationError("invalid native scheduler integer")
    return value


def _number(value: object, *, minimum: int = 0) -> int:
    if (not isinstance(value, dict) or set(value) != {"set", "infinite", "number"}
        or value["set"] is not True or value["infinite"] is not False):
        raise NativeSlurmObservationError("native scheduler quantity is unset or infinite")
    return _integer(value["number"], minimum=minimum)


def _constant(value: str) -> object:
    raise NativeSlurmObservationError("nonfinite native scheduler JSON constant")


def _allocation_tres(value: object) -> tuple[int, int]:
    if not isinstance(value, str) or len(value) > 4096:
        raise NativeSlurmObservationError("native allocated TRES are unavailable")
    fields: dict[str, str] = {}
    for component in value.split(","):
        key, separator, quantity = component.partition("=")
        if not separator or key in fields or key not in {"cpu", "mem", "node", "billing"}:
            raise NativeSlurmObservationError("native allocated TRES are ambiguous or unsupported")
        fields[key] = quantity
    if not {"cpu", "mem", "node"} <= fields.keys() or fields["node"] != "1":
        raise NativeSlurmObservationError("native allocation requires exactly one node")
    if re.fullmatch(r"[1-9][0-9]{0,4}", fields["cpu"], re.ASCII) is None:
        raise NativeSlurmObservationError("native allocated CPUs are malformed")
    if "billing" in fields and _DECIMAL.fullmatch(fields["billing"]) is None:
        raise NativeSlurmObservationError("native billing TRES are malformed")
    memory = _MEMORY.fullmatch(fields["mem"])
    if memory is None:
        raise NativeSlurmObservationError("native allocated memory lacks exact units")
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(fields["cpu"]), _integer(int(memory[1]) * units[memory[2]], minimum=1)


def _validate_expectation(expected: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(expected, Mapping) or set(expected) != _EXPECTATION_FIELDS:
        raise NativeSlurmObservationError("native scheduler expectation fields are invalid")
    result = dict(expected)
    for key in ("cluster", "hostname", "submitter", "account", "partition", "qos"):
        value = result[key]
        if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
            raise NativeSlurmObservationError("native scheduler expectation identifier is invalid")
    for key, pattern in (("job_id", _JOB_ID), ("ownership_token", _OWNERSHIP)):
        value = result[key]
        if not isinstance(value, str) or pattern.fullmatch(value) is None:
            raise NativeSlurmObservationError("native scheduler expectation identity is invalid")
    if _integer(result["uid"]) > (1 << 31) - 1 or _integer(result["cpus"], minimum=1) > 65_536:
        raise NativeSlurmObservationError("native scheduler expectation quantity is invalid")
    _integer(result["memory_bytes"], minimum=1)
    return result


def parse_native_scheduler_record(
    raw: str, *, expected: Mapping[str, object], observed_at: datetime,
) -> dict[str, object]:
    """One shared parser for executor and immutable node-side verifier.

    Expected facts must come from a separately authenticated launch/delegation;
    matching scheduler bytes does not authenticate whoever supplied expectations.
    The observation timestamp precedes the scheduler query, never its completion.
    """
    try:
        if (not isinstance(raw, str) or len(raw) > _MAX_BYTES
            or len(raw.encode("utf-8")) > _MAX_BYTES):
            raise NativeSlurmObservationError("native scheduler output exceeds its bound")
        facts = _validate_expectation(expected)
        if not isinstance(observed_at, datetime) or observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise NativeSlurmObservationError("native scheduler observation time must be timezone-aware")
        document = json.loads(raw, object_pairs_hook=_object, parse_constant=_constant)
        if (not isinstance(document, dict)
            or document["meta"]["plugin"]["data_parser"] != "v0.0.40"
            or document["errors"] != [] or document["warnings"] != []
            or not isinstance(document["jobs"], list) or len(document["jobs"]) != 1):
            raise NativeSlurmObservationError("native scheduler response is partial or has the wrong parser")
        job = document["jobs"][0]
        if not isinstance(job, dict):
            raise NativeSlurmObservationError("native scheduler record is not an object")
        for name in ("array_job_id", "het_job_id", "het_job_offset"):
            if _number(job[name]) != 0:
                raise NativeSlurmObservationError("native allocation refuses array or heterogeneous jobs")
        task = job["array_task_id"]
        if (not isinstance(task, dict) or set(task) != {"set", "infinite", "number"}
            or task["set"] is not False or task["infinite"] is not False
            or _integer(task["number"]) != 0
            or job["array_task_string"] != "" or job["het_job_id_set"] != ""
            or job["batch_flag"] is not True
            or job["requeue"] is not False or _integer(job["restart_cnt"]) != 0
            or job["job_state"] != ["RUNNING"] or _number(job["node_count"]) != 1):
            raise NativeSlurmObservationError("native allocation is not one running non-requeue batch job")
        cpus, memory = _allocation_tres(job["tres_alloc_str"])
        if (_number(job["cpus"], minimum=1) != cpus or cpus != facts["cpus"]
            or memory != facts["memory_bytes"]
            or str(_integer(job["job_id"], minimum=1)) != facts["job_id"]
            or _integer(job["user_id"]) != facts["uid"]
            or any(job[key] != facts[field] for key, field in (
                ("nodes", "hostname"), ("cluster", "cluster"), ("user_name", "submitter"),
                ("account", "account"), ("partition", "partition"), ("qos", "qos"),
                ("comment", "ownership_token"),
            ))):
            raise NativeSlurmObservationError("native scheduler allocation differs from expected launch")
        submitted_at = datetime.fromtimestamp(_number(job["submit_time"], minimum=1), UTC)
        started_at = datetime.fromtimestamp(_number(job["start_time"], minimum=1), UTC)
        if not submitted_at <= started_at <= observed_at:
            raise NativeSlurmObservationError("native scheduler incarnation times are inconsistent")
        return facts | {
            "submitted_at": submitted_at, "started_at": started_at, "observed_at": observed_at,
            "schema_version": 1, "parser_version": "v0.0.40", "state": "RUNNING",
            "requeue": False, "restart_count": 0,
        }
    except NativeSlurmObservationError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError, RecursionError):
        raise NativeSlurmObservationError("native scheduler observation is malformed") from None
