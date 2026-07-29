#!/usr/bin/python3 -I
"""One-shot privileged-Docker bootstrap for persistent node authority.

The container is only an initial root channel.  It validates one canonical
request and exact Git bundle, creates a root-owned temporary checkout under the
host root, chroots into that host, and invokes the existing node-authority or
transport transaction.  Successful state is installed on the host; the
container checkout and trust-input staging are removed before exit.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

SCHEMA_VERSION: Final = 2
KIND: Final = "loom.developer-sandbox.node-docker-bootstrap"
HOST_ROOT: Final = Path("/host")
REQUEST_PATH: Final = Path("/run/loom-node-bootstrap/request.json")
BUNDLE_PATH: Final = Path("/run/loom-node-bootstrap/candidate.bundle")
INPUT_ROOT: Final = Path("/run/loom-node-bootstrap/input")
MOUNTINFO: Final = Path("/proc/self/mountinfo")
CGROUP_PATHS: Final = (Path("/proc/self/cgroup"), Path("/proc/1/cgroup"))
CONTAINER_MARKERS: Final = (Path("/.dockerenv"), Path("/run/.containerenv"))
HOST_STATE_ROOT: Final = HOST_ROOT / "var/lib/loom-developer-sandbox-node-bootstrap"
HOST_RECEIPT_ROOT: Final = HOST_STATE_ROOT / "receipts"
HOST_LOCK: Final = HOST_STATE_ROOT / "bootstrap.lock"
HOST_STAGE_ROOT: Final = HOST_ROOT / "run/loom-developer-sandbox-node-bootstrap"
HOST_TRANSPORT_CLIENT_POLICY: Final = (
    HOST_ROOT / "etc/loom/developer-sandbox-node-transport/client-policy.json"
)
HOST_TRANSPORT_SERVER_POLICY: Final = (
    HOST_ROOT / "etc/loom/developer-sandbox-node-transport/server-policy.json"
)
MAX_REQUEST_BYTES: Final = 256 * 1024
MAX_BUNDLE_BYTES: Final = 256 * 1024 * 1024
MAX_INPUT_BYTES: Final = 2 * 1024 * 1024
MAX_CHILD_REPORT_BYTES: Final = 2 * 1024 * 1024
MAX_FIXED_COMMAND_STDERR_BYTES: Final = 64 * 1024
MAX_HOST_ERROR_BYTES: Final = 4096
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
NODE_RE: Final = re.compile(r"^(?:oldlab-[1-5]|trt-gb10-(?:[1-9]|1[0-5]))$")
INPUT_NAME_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}(?:[.]pub)?$|^known_hosts$")
ACTIONS: Final = frozenset(
    {
        "authority-bootstrap",
        "authority-upgrade",
        "transport-server-bootstrap",
        "transport-client-bootstrap",
        "transport-upgrade",
        "readback",
    },
)
REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "request_id",
    "operation_id",
    "transport_expectation",
    "action",
    "candidate_sha",
    "candidate_tree",
    "candidate_bundle_sha256",
    "expected_node",
    "inputs",
}
CONTAINER_TOKENS: Final = ("docker", "containerd", "kubepods", "podman", "libpod", "lxc")
LAUNCHER_RELATIVE: Final = Path("scripts/ops/developer_sandbox_node_docker_bootstrap.py")


class DockerNodeBootstrapError(RuntimeError):
    """A bounded, secret-safe one-shot bootstrap failure."""


@dataclass(frozen=True, slots=True)
class MountRecord:
    """Small immutable mountinfo projection."""

    root: str
    mount_point: str
    options: frozenset[str]


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_regular(
    path: Path,
    *,
    limit: int,
    allowed_modes: frozenset[int] | None = None,
) -> bytes:
    try:
        lexical = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW)
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap input is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise DockerNodeBootstrapError("Docker bootstrap input exceeds its size bound")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or before.st_nlink != 1
            or _metadata_identity(lexical) != _metadata_identity(before)
            or _metadata_identity(before) != _metadata_identity(after)
            or _metadata_identity(after) != _metadata_identity(current)
            or (allowed_modes is not None and stat.S_IMODE(before.st_mode) not in allowed_modes)
        ):
            raise DockerNodeBootstrapError("Docker bootstrap input metadata is unsafe")
        return b"".join(chunks)
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap input read failed safely") from exc
    finally:
        os.close(descriptor)


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _decode_mount_path(value: str) -> str:
    for encoded, decoded in (
        ("\\040", " "),
        ("\\011", "\t"),
        ("\\012", "\n"),
        ("\\134", "\\"),
    ):
        value = value.replace(encoded, decoded)
    return value


def _parse_mountinfo(payload: str) -> tuple[MountRecord, ...]:
    records: list[MountRecord] = []
    for line in payload.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError as exc:
            raise DockerNodeBootstrapError("Docker bootstrap mountinfo is invalid") from exc
        if separator < 6:
            raise DockerNodeBootstrapError("Docker bootstrap mountinfo is invalid")
        records.append(
            MountRecord(
                _decode_mount_path(fields[3]),
                _decode_mount_path(fields[4]),
                frozenset(fields[5].split(",")),
            ),
        )
    return tuple(records)


def _single_mount(records: Sequence[MountRecord], path: Path) -> MountRecord:
    matches = [record for record in records if record.mount_point == str(path)]
    if len(matches) != 1:
        raise DockerNodeBootstrapError("Docker bootstrap mount contract is incomplete")
    return matches[0]


def _container_identity() -> bool:
    if any(path.exists() for path in CONTAINER_MARKERS) or "container" in os.environ:
        return True
    for path in CGROUP_PATHS:
        try:
            value = path.read_text(encoding="utf-8").lower()
        except OSError:
            continue
        if any(token in value for token in CONTAINER_TOKENS):
            return True
    return False


def _validate_runtime() -> None:
    if sys.argv != [sys.argv[0]] or os.getuid() != 0 or os.geteuid() != 0:
        raise DockerNodeBootstrapError("Docker bootstrap invocation is not fixed root")
    if os.uname().machine not in {"x86_64", "aarch64"}:
        raise DockerNodeBootstrapError("Docker bootstrap architecture is unsupported")
    if any(name.startswith("SUDO_") for name in os.environ):
        raise DockerNodeBootstrapError("Docker bootstrap environment is not closed")
    if not _container_identity():
        raise DockerNodeBootstrapError("Docker bootstrap requires the one-shot container channel")
    try:
        records = _parse_mountinfo(MOUNTINFO.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap mountinfo is unavailable") from exc
    host = _single_mount(records, HOST_ROOT)
    if host.root != "/" or "rw" not in host.options:
        raise DockerNodeBootstrapError("Docker bootstrap host-root bind is invalid")
    for path in (REQUEST_PATH, BUNDLE_PATH, INPUT_ROOT):
        mounted = _single_mount(records, path)
        if "ro" not in mounted.options:
            raise DockerNodeBootstrapError("Docker bootstrap inputs must be read-only binds")
    try:
        metadata = HOST_ROOT.lstat()
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap host root is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise DockerNodeBootstrapError("Docker bootstrap host root is invalid")


def _load_request() -> dict[str, Any]:
    raw = _read_regular(
        REQUEST_PATH,
        limit=MAX_REQUEST_BYTES,
        allowed_modes=frozenset({0o400, 0o440, 0o444, 0o600, 0o640, 0o644}),
    )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DockerNodeBootstrapError("Docker bootstrap request is invalid") from exc
    if not isinstance(payload, dict) or set(payload) != REQUEST_FIELDS:
        raise DockerNodeBootstrapError("Docker bootstrap request shape is invalid")
    unsigned = dict(payload)
    unsigned.pop("request_id")
    inputs = payload.get("inputs")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("kind") != KIND
        or payload.get("action") not in ACTIONS
        or SHA256_RE.fullmatch(str(payload.get("operation_id"))) is None
        or payload.get("transport_expectation")
        not in {"not-checked", "absent", "server", "client-server"}
        or (
            payload.get("action") == "readback"
            and payload.get("transport_expectation") == "not-checked"
        )
        or (
            payload.get("action") != "readback"
            and payload.get("transport_expectation") != "not-checked"
        )
        or SHA_RE.fullmatch(str(payload.get("candidate_sha"))) is None
        or SHA_RE.fullmatch(str(payload.get("candidate_tree"))) is None
        or SHA256_RE.fullmatch(str(payload.get("candidate_bundle_sha256"))) is None
        or NODE_RE.fullmatch(str(payload.get("expected_node"))) is None
        or not isinstance(inputs, dict)
        or any(
            not isinstance(name, str)
            or INPUT_NAME_RE.fullmatch(name) is None
            or not isinstance(value, str)
            or SHA256_RE.fullmatch(value) is None
            for name, value in inputs.items()
        )
        or SHA256_RE.fullmatch(str(payload.get("request_id"))) is None
        or payload.get("request_id") != _digest(_canonical(unsigned))
        or raw != _canonical(payload)
    ):
        raise DockerNodeBootstrapError("Docker bootstrap request binding is invalid")
    if payload["action"] in {"authority-bootstrap", "authority-upgrade", "readback"} and inputs:
        raise DockerNodeBootstrapError("Docker bootstrap request has unexpected trust inputs")
    return payload


def _clean_env() -> dict[str, str]:
    return {
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _run(*argv: str, cwd: Path | None = None) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=_clean_env(),
        check=False,
        capture_output=True,
        timeout=120,
    )


def _checked(*argv: str, cwd: Path | None = None) -> bytes:
    result = _run(*argv, cwd=cwd)
    if result.returncode != 0 or len(result.stderr) > MAX_FIXED_COMMAND_STDERR_BYTES:
        raise DockerNodeBootstrapError("Docker bootstrap fixed command failed safely")
    return result.stdout


def _bundle_payload(request: Mapping[str, Any]) -> bytes:
    payload = _read_regular(
        BUNDLE_PATH,
        limit=MAX_BUNDLE_BYTES,
        allowed_modes=frozenset({0o400, 0o440, 0o444, 0o600, 0o640, 0o644}),
    )
    if not payload or _digest(payload) != request["candidate_bundle_sha256"]:
        raise DockerNodeBootstrapError("Docker bootstrap bundle identity is invalid")
    return payload


def _ensure_directory(path: Path, mode: int) -> None:
    created = False
    try:
        path.mkdir(mode=mode)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap host state is unavailable") from exc
    try:
        if created:
            path.chmod(mode)
        metadata = path.lstat()
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap host state is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise DockerNodeBootstrapError("Docker bootstrap host state is unsafe")


def _validate_input_inventory(request: Mapping[str, Any]) -> None:
    try:
        metadata = INPUT_ROOT.lstat()
        entries = tuple(INPUT_ROOT.iterdir())
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap trust inventory is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise DockerNodeBootstrapError("Docker bootstrap trust inventory is unsafe")
    names = {path.name for path in entries}
    if len(names) != len(entries) or names != set(request["inputs"]):
        raise DockerNodeBootstrapError("Docker bootstrap trust inventory is not closed")
    for path in entries:
        try:
            item = path.lstat()
        except OSError as exc:
            raise DockerNodeBootstrapError(
                "Docker bootstrap trust inventory is unavailable",
            ) from exc
        if (
            INPUT_NAME_RE.fullmatch(path.name) is None
            or not stat.S_ISREG(item.st_mode)
            or stat.S_ISLNK(item.st_mode)
        ):
            raise DockerNodeBootstrapError("Docker bootstrap trust inventory is unsafe")


def _copy_input(source: Path, destination: Path, expected_digest: str) -> None:
    payload = _read_regular(
        source,
        limit=MAX_INPUT_BYTES,
        allowed_modes=frozenset({0o400, 0o440, 0o444, 0o600, 0o640, 0o644}),
    )
    if _digest(payload) != expected_digest:
        raise DockerNodeBootstrapError("Docker bootstrap trust input identity is invalid")
    mode = 0o644 if source.name.endswith(".pub") or source.name == "known_hosts" else 0o600
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
        mode,
    )
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.chown(destination, 0, 0)
    os.chmod(destination, mode)


def _remove_stage(stage: Path) -> None:
    try:
        metadata = stage.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap stage cleanup failed safely") from exc
    if (
        stage.parent != HOST_STAGE_ROOT
        or SHA256_RE.fullmatch(stage.name) is None
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
    ):
        raise DockerNodeBootstrapError("Docker bootstrap stage cleanup target is unsafe")
    try:
        shutil.rmtree(stage)
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap stage cleanup failed safely") from exc
    if stage.exists() or stage.is_symlink():
        raise DockerNodeBootstrapError("Docker bootstrap stage cleanup failed safely")


def _validate_launcher_identity(source: Path, candidate_sha: str) -> None:
    running = _read_regular(
        Path(__file__),
        limit=MAX_INPUT_BYTES,
        allowed_modes=frozenset({0o444, 0o555, 0o644, 0o755}),
    )
    checked_out = _read_regular(
        source / LAUNCHER_RELATIVE,
        limit=MAX_INPUT_BYTES,
        allowed_modes=frozenset({0o444, 0o555, 0o644, 0o755}),
    )
    committed = _checked(
        "/usr/bin/git",
        "-C",
        str(source),
        "cat-file",
        "blob",
        f"{candidate_sha}:{LAUNCHER_RELATIVE.as_posix()}",
    )
    if running != checked_out or checked_out != committed:
        raise DockerNodeBootstrapError("Docker bootstrap launcher is not the exact candidate")


def _prepare_stage(request: Mapping[str, Any], bundle_payload: bytes) -> tuple[Path, Path]:
    _ensure_directory(HOST_STAGE_ROOT, 0o700)
    stage = HOST_STAGE_ROOT / str(request["request_id"])
    _remove_stage(stage)
    try:
        stage.mkdir(mode=0o700)
    except OSError as exc:
        raise DockerNodeBootstrapError("Docker bootstrap stage already exists") from exc
    bundle = stage / "candidate.bundle"
    input_root = stage / "input"
    source = stage / "source"
    try:
        input_root.mkdir(mode=0o700)
        descriptor = os.open(
            bundle,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            os.write(descriptor, bundle_payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        for name, digest in sorted(request["inputs"].items()):
            _copy_input(INPUT_ROOT / name, input_root / name, digest)
        _checked("/usr/bin/git", "clone", "--no-hardlinks", str(bundle), str(source))
        _checked("/usr/bin/git", "-C", str(source), "bundle", "verify", str(bundle))
        _checked(
            "/usr/bin/git",
            "-C",
            str(source),
            "checkout",
            "--detach",
            str(request["candidate_sha"]),
        )
        sha = _checked("/usr/bin/git", "-C", str(source), "rev-parse", "HEAD").decode().strip()
        tree = (
            _checked("/usr/bin/git", "-C", str(source), "rev-parse", "HEAD^{tree}").decode().strip()
        )
        status = _checked(
            "/usr/bin/git",
            "-C",
            str(source),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
        if sha != request["candidate_sha"] or tree != request["candidate_tree"] or status:
            raise DockerNodeBootstrapError("Docker bootstrap checkout identity is invalid")
        _validate_launcher_identity(source, str(request["candidate_sha"]))
        os.chmod(source, 0o700)
        return stage, source
    except Exception:
        _remove_stage(stage)
        raise


def _transport_input_argv(
    host_input_root: Path,
    chroot_input_root: Path | None = None,
) -> tuple[list[str], list[str], str | None]:
    rendered_root = host_input_root if chroot_input_root is None else chroot_input_root
    identities: list[str] = []
    public_keys: list[str] = []
    known_hosts: str | None = None
    for path in sorted(host_input_root.iterdir()):
        rendered = rendered_root / path.name
        if path.name == "known_hosts":
            known_hosts = str(rendered)
        elif path.name.endswith(".pub"):
            public_keys.extend(
                ("--public-key", f"{path.name.removesuffix('.pub')}={rendered}"),
            )
        else:
            identities.extend(("--identity", f"{path.name}={rendered}"))
    return identities, public_keys, known_hosts


def _fixed_action_argv(
    request: Mapping[str, Any],
    stage: Path,
) -> tuple[list[str], Path]:
    source = Path("/") / stage.relative_to(HOST_ROOT) / "source"
    input_root = source.parent / "input"
    host_input_root = stage / "input"
    authority = source / "scripts/ops/developer_sandbox_node_authority.py"
    transport = source / "scripts/ops/developer_sandbox_node_transport.py"
    prefix = ["/usr/bin/python3", "-I"]
    action = request["action"]
    if action in {"authority-bootstrap", "authority-upgrade"}:
        command = action.removeprefix("authority-")
        return (
            [
                *prefix,
                str(authority),
                command,
                "--candidate-sha",
                str(request["candidate_sha"]),
                "--candidate-tree",
                str(request["candidate_tree"]),
                "--execute",
            ],
            source,
        )
    if action == "readback":
        return ([*prefix, str(authority), "validate-install"], source)
    identities, public_keys, known_hosts = _transport_input_argv(
        host_input_root,
        input_root,
    )
    if action == "transport-server-bootstrap":
        if identities or known_hosts is not None:
            raise DockerNodeBootstrapError("Docker bootstrap server input set is invalid")
        return (
            [*prefix, str(transport), "bootstrap-server", *public_keys, "--execute"],
            source,
        )
    if action == "transport-client-bootstrap":
        if known_hosts is None:
            raise DockerNodeBootstrapError("Docker bootstrap client known_hosts is missing")
        return (
            [
                *prefix,
                str(transport),
                "bootstrap-client",
                *identities,
                *public_keys,
                "--known-hosts",
                known_hosts,
                "--execute",
            ],
            source,
        )
    if action == "transport-upgrade":
        known_hosts_argv = [] if known_hosts is None else ["--known-hosts", known_hosts]
        return (
            [
                *prefix,
                str(transport),
                "upgrade",
                *identities,
                *public_keys,
                *known_hosts_argv,
                "--execute",
            ],
            source,
        )
    raise DockerNodeBootstrapError("Docker bootstrap action is outside authority")


def _run_host_python(argv: Sequence[str], cwd: Path) -> dict[str, Any]:
    if (
        list(argv[:2]) != ["/usr/bin/python3", "-I"]
        or len(argv) < 4
        or not str(argv[2]).startswith(str(cwd) + "/scripts/ops/")
    ):
        raise DockerNodeBootstrapError("Docker bootstrap host command is outside authority")

    def enter_host() -> None:
        os.chroot(HOST_ROOT)
        os.chdir(cwd)

    try:
        completed = subprocess.run(
            list(argv),
            stdin=subprocess.DEVNULL,
            env=_clean_env(),
            check=False,
            capture_output=True,
            timeout=600,
            preexec_fn=enter_host,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DockerNodeBootstrapError("Docker bootstrap host command failed safely") from exc
    if completed.returncode != 0 or completed.stderr:
        detail = completed.stderr[: MAX_HOST_ERROR_BYTES + 1]
        if 0 < len(detail) <= MAX_HOST_ERROR_BYTES:
            try:
                decoded = detail.decode("utf-8").strip()
            except UnicodeDecodeError:
                decoded = ""
            if decoded and all(
                character.isprintable() or character in "\r\n\t" for character in decoded
            ):
                normalized = " ".join(decoded.split())
                raise DockerNodeBootstrapError(
                    f"Docker bootstrap host transaction rejected: {normalized}",
                )
        raise DockerNodeBootstrapError("Docker bootstrap host transaction failed safely")
    if not completed.stdout or len(completed.stdout) > MAX_CHILD_REPORT_BYTES:
        raise DockerNodeBootstrapError("Docker bootstrap host transaction failed safely")
    raw = completed.stdout
    try:
        result = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DockerNodeBootstrapError("Docker bootstrap child evidence is invalid") from exc
    if not isinstance(result, dict) or raw != _canonical(result):
        raise DockerNodeBootstrapError("Docker bootstrap child evidence is invalid")
    return result


def _validate_action_result(
    request: Mapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    binding_field = "initiator" if request["action"] == "transport-client-bootstrap" else "node"
    if result.get("status") != "succeeded" or result.get(binding_field) != request["expected_node"]:
        raise DockerNodeBootstrapError("Docker bootstrap child evidence is invalid")
    if request["action"] in {"authority-bootstrap", "authority-upgrade", "readback"} and (
        result.get("source_sha") != request["candidate_sha"]
        or result.get("source_tree") != request["candidate_tree"]
    ):
        raise DockerNodeBootstrapError("Docker bootstrap child candidate evidence drifted")


def _run_chroot_action(request: Mapping[str, Any], stage: Path) -> dict[str, Any]:
    argv, cwd = _fixed_action_argv(request, stage)
    authority_result = _run_host_python(argv, cwd)
    if request["action"] != "readback":
        _validate_action_result(request, authority_result)
        return authority_result
    client_result = None
    server_result = None
    transport = cwd / "scripts/ops/developer_sandbox_node_transport.py"
    if HOST_TRANSPORT_CLIENT_POLICY.exists():
        client_result = _run_host_python(
            ["/usr/bin/python3", "-I", str(transport), "check-client"],
            cwd,
        )
    if HOST_TRANSPORT_SERVER_POLICY.exists():
        server_result = _run_host_python(
            ["/usr/bin/python3", "-I", str(transport), "check-server"],
            cwd,
        )
    result = {
        "schema_version": SCHEMA_VERSION,
        "action": "readback",
        "node": authority_result.get("node"),
        "source_sha": authority_result.get("source_sha"),
        "source_tree": authority_result.get("source_tree"),
        "authority": authority_result,
        "transport_client": client_result,
        "transport_server": server_result,
        "status": "succeeded",
    }
    _validate_action_result(request, result)
    expectation = request["transport_expectation"]
    if (
        (expectation == "absent" and (client_result is not None or server_result is not None))
        or (expectation == "server" and (client_result is not None or server_result is None))
        or (expectation == "client-server" and (client_result is None or server_result is None))
    ):
        raise DockerNodeBootstrapError("Docker bootstrap transport readback is incomplete")
    for transport_result, binding_field in (
        (client_result, "initiator"),
        (server_result, "node"),
    ):
        if transport_result is not None and (
            transport_result.get("status") != "succeeded"
            or transport_result.get(binding_field) != request["expected_node"]
        ):
            raise DockerNodeBootstrapError("Docker bootstrap transport readback is invalid")
    return result


def _write_receipt(request: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    path = HOST_RECEIPT_ROOT / f"{request['request_id']}.json"
    result_digest = _digest(_canonical(result))
    if path.exists():
        existing_raw = _read_regular(
            path,
            limit=MAX_REQUEST_BYTES,
            allowed_modes=frozenset({0o600}),
        )
        try:
            existing = json.loads(existing_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DockerNodeBootstrapError("Docker bootstrap receipt is invalid") from exc
        if (
            not isinstance(existing, dict)
            or set(existing)
            != {
                "schema_version",
                "kind",
                "request_id",
                "operation_id",
                "action",
                "candidate_sha",
                "candidate_tree",
                "expected_node",
                "result_sha256",
                "completed_at",
                "status",
            }
            or existing_raw != _canonical(existing)
            or existing.get("schema_version") != SCHEMA_VERSION
            or existing.get("kind") != "loom.developer-sandbox.node-docker-bootstrap-receipt"
            or existing.get("request_id") != request["request_id"]
            or existing.get("operation_id") != request["operation_id"]
            or existing.get("action") != request["action"]
            or existing.get("candidate_sha") != request["candidate_sha"]
            or existing.get("candidate_tree") != request["candidate_tree"]
            or existing.get("expected_node") != request["expected_node"]
            or existing.get("result_sha256") != result_digest
            or not isinstance(existing.get("completed_at"), str)
            or existing.get("status") != "succeeded"
        ):
            raise DockerNodeBootstrapError("Docker bootstrap receipt replay drifted")
        return existing
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.node-docker-bootstrap-receipt",
        "request_id": request["request_id"],
        "operation_id": request["operation_id"],
        "action": request["action"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "expected_node": request["expected_node"],
        "result_sha256": result_digest,
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": "succeeded",
    }
    payload = _canonical(receipt)
    temporary_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{request['request_id']}.",
        dir=HOST_RECEIPT_ROOT,
    )
    try:
        os.fchmod(temporary_descriptor, 0o600)
        os.write(temporary_descriptor, payload)
        os.fsync(temporary_descriptor)
    finally:
        os.close(temporary_descriptor)
    try:
        os.link(temporary_name, path)
        directory = os.open(
            HOST_RECEIPT_ROOT,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as exc:
        raise DockerNodeBootstrapError(
            "Docker bootstrap receipt publication failed safely"
        ) from exc
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return receipt


def execute() -> dict[str, Any]:
    _validate_runtime()
    request = _load_request()
    _validate_input_inventory(request)
    bundle_payload = _bundle_payload(request)
    _ensure_directory(HOST_STATE_ROOT, 0o700)
    _ensure_directory(HOST_RECEIPT_ROOT, 0o700)
    lock = os.open(
        HOST_LOCK,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        0o600,
    )
    lock_metadata = os.fstat(lock)
    if (
        not stat.S_ISREG(lock_metadata.st_mode)
        or lock_metadata.st_uid != 0
        or lock_metadata.st_gid != 0
        or stat.S_IMODE(lock_metadata.st_mode) != 0o600
        or lock_metadata.st_nlink != 1
    ):
        os.close(lock)
        raise DockerNodeBootstrapError("Docker bootstrap lock is unsafe")
    stage: Path | None = None
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        stage, _source = _prepare_stage(request, bundle_payload)
        result = _run_chroot_action(request, stage)
        receipt = _write_receipt(request, result)
        return {
            "schema_version": SCHEMA_VERSION,
            "action": request["action"],
            "candidate_sha": request["candidate_sha"],
            "candidate_tree": request["candidate_tree"],
            "node": request["expected_node"],
            "request_id": request["request_id"],
            "operation_id": request["operation_id"],
            "receipt": receipt,
            "result": result,
            "status": "succeeded",
        }
    finally:
        if stage is not None:
            _remove_stage(stage)
        os.close(lock)


def main() -> int:
    try:
        report = execute()
    except DockerNodeBootstrapError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except (OSError, subprocess.SubprocessError, ValueError):
        sys.stderr.write("error: Docker node bootstrap failed safely\n")
        return 1
    sys.stdout.buffer.write(_canonical(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
