"""Evidence directory management for the rollout driver (#340).

Layout under ``<rollout-root>/rollouts/<rollout_id>/``:

* ``state.json`` — top-level RolloutState (see :mod:`state`)
* ``inputs.json`` — resolved CLI args + config shas (for --resume audit)
* ``logs/driver.log`` — top-level driver log
* ``NN-<step-name>/`` — per-step directory:
    * ``result.json`` — {state, inputs_hash, timestamps, exit_code}
    * ``stdout.log``, ``stderr.log`` — captured subprocess output
    * any step-specific artifacts (rendered.yaml, loaded-images.json, …)
* ``99-summary/summary.md`` — human-readable summary written at the end

The evidence root path model is inherited from #174: the parent
``<rollout-root>/rollouts/`` directory is created by
``loom cluster bootstrap-evidence-paths`` and owned by the operator user.
The driver assumes it can create the per-rollout subdirectory without
sudo — if it can't, the diagnostic points at the bootstrap subcommand.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loom_cli.rollout.operator.redaction import (
    redact_rollout_mapping,
    redact_rollout_text,
)

_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700
_MAX_JSON_ARTIFACT_BYTES = 16 * 1024 * 1024
_TEMP_NAME_ATTEMPTS = 128


def _directory_open_flags() -> int:
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    if not no_follow or not directory:
        raise OSError("no-follow directory traversal is unavailable")
    return os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | directory | no_follow


def _open_directory_chain(path: Path) -> int:
    """Open an absolute directory path without following any component."""
    absolute = Path(os.path.abspath(path))
    flags = _directory_open_flags()
    current_fd = os.open("/", flags)
    try:
        for component in absolute.parts[1:]:
            next_fd = os.open(component, flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
    except Exception:
        os.close(current_fd)
        raise
    return current_fd


def _validate_component(name: str) -> None:
    if (
        not name
        or name in {".", ".."}
        or "/" in name
        or (os.altsep is not None and os.altsep in name)
        or "\x00" in name
    ):
        raise ValueError(f"unsafe evidence path component: {name!r}")


def _open_directory_at(parent_fd: int, name: str) -> int:
    _validate_component(name)
    return os.open(name, _directory_open_flags(), dir_fd=parent_fd)


def _create_and_open_directory_at(parent_fd: int, name: str) -> int:
    """Create one child directory, then bind to it without following links."""
    _validate_component(name)
    try:
        os.mkdir(name, mode=_PRIVATE_DIRECTORY_MODE, dir_fd=parent_fd)
    except FileExistsError:
        pass
    else:
        os.fsync(parent_fd)
    return _open_directory_at(parent_fd, name)


def _open_rollout_directory(root: Path, rollout_id: str) -> int:
    root_fd = _open_directory_chain(root)
    try:
        rollouts_fd = _open_directory_at(root_fd, "rollouts")
    finally:
        os.close(root_fd)
    try:
        return _open_directory_at(rollouts_fd, rollout_id)
    finally:
        os.close(rollouts_fd)


def _open_rollout_directory_if_present(root: Path, rollout_id: str) -> int | None:
    """Open an existing rollout, returning ``None`` only for genuine absence."""
    try:
        root_fd = _open_directory_chain(root)
    except FileNotFoundError:
        return None
    try:
        try:
            rollouts_fd = _open_directory_at(root_fd, "rollouts")
        except FileNotFoundError:
            return None
    finally:
        os.close(root_fd)
    try:
        try:
            return _open_directory_at(rollouts_fd, rollout_id)
        except FileNotFoundError:
            return None
    finally:
        os.close(rollouts_fd)


def _read_json_object_at(directory_fd: int, name: str) -> dict[str, Any]:
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if not no_follow:
        raise OSError("no-follow file reads are unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | no_follow | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(name, flags, dir_fd=directory_fd)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{name} must be a regular file")
        if metadata.st_size > _MAX_JSON_ARTIFACT_BYTES:
            raise ValueError(f"{name} exceeds the JSON artifact size limit")
        payload = bytearray()
        while len(payload) <= _MAX_JSON_ARTIFACT_BYTES:
            chunk = os.read(
                fd,
                min(65536, _MAX_JSON_ARTIFACT_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > _MAX_JSON_ARTIFACT_BYTES:
            raise ValueError(f"{name} exceeds the JSON artifact size limit")
        final_metadata = os.fstat(fd)
        if (
            final_metadata.st_dev,
            final_metadata.st_ino,
            final_metadata.st_size,
            final_metadata.st_mtime_ns,
        ) != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
        ):
            raise ValueError(f"{name} changed while it was being read")
    finally:
        os.close(fd)
    decoded = bytes(payload).decode("utf-8")
    parsed = json.loads(decoded)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return parsed


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:  # pragma: no cover - os.write contract
            raise OSError("evidence write made no progress")
        offset += written


def _atomic_private_write_text_at(directory_fd: int, name: str, text: str) -> None:
    """Durably replace one private artifact relative to a trusted directory."""
    _validate_component(name)
    payload = text.encode("utf-8")
    if len(payload) > _MAX_JSON_ARTIFACT_BYTES:
        raise ValueError(f"{name} exceeds the JSON artifact size limit")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if not no_follow:
        raise OSError("no-follow file writes are unavailable")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | no_follow
    temp_name: str | None = None
    fd = -1
    for _ in range(_TEMP_NAME_ATTEMPTS):
        candidate = f".{name}.{secrets.token_hex(12)}.tmp"
        try:
            fd = os.open(candidate, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
        except FileExistsError:
            continue
        temp_name = candidate
        break
    if temp_name is None:
        raise FileExistsError(f"could not allocate a private temporary file for {name}")
    try:
        os.fchmod(fd, _PRIVATE_FILE_MODE)
        _write_all(fd, payload)
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(
            temp_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temp_name = None
        os.fsync(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temp_name is not None:
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass


def _append_private_text_at(directory_fd: int, name: str, text: str) -> None:
    """Append to one regular private file without following a link."""
    _validate_component(name)
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if not no_follow:
        raise OSError("no-follow file writes are unavailable")
    flags = (
        os.O_WRONLY
        | os.O_APPEND
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | no_follow
    )
    fd = os.open(name, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{name} must be a regular file")
        if metadata.st_nlink != 1:
            raise ValueError(f"{name} must not be hard-linked")
        os.fchmod(fd, _PRIVATE_FILE_MODE)
        _write_all(fd, text.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.fsync(directory_fd)


def new_rollout_id(*, image_tag: str, now: datetime | None = None) -> str:
    """Return a deterministic rollout id: ``<utc-timestamp>-<image-tag-slug>``.

    The timestamp comes first so directory listings sort chronologically.
    The image tag gets normalised to RFC 1123 DNS-label shape so the id
    is safe to use in kubectl object names / paths.
    """
    now = now or datetime.now(UTC)
    ts = now.strftime("%Y%m%dt%H%M%Sz")
    slug = _slugify(image_tag)
    return f"{ts}-{slug}"


def _slugify(text: str) -> str:
    """Normalise to lowercase-alnum-dash form."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


@dataclass(frozen=True, slots=True)
class StepDir:
    """A per-step subdirectory under the rollout evidence dir."""

    number: int
    name: str
    path: Path

    def result_path(self) -> Path:
        return self.path / "result.json"

    def stdout_path(self) -> Path:
        return self.path / "stdout.log"

    def stderr_path(self) -> Path:
        return self.path / "stderr.log"

    def artifact_path(self, name: str) -> Path:
        """Path for a step-specific artifact file (rendered.yaml, …)."""
        return self.path / name


class EvidenceDirectory:
    """Root of one rollout's evidence tree.

    Creates the tree on first use; safe to construct against an
    already-populated tree (resume path). All writes are string I/O;
    subprocess stdout/stderr should be piped to :meth:`stdout_path` /
    :meth:`stderr_path` by the caller — we don't own the subprocess
    lifecycle here so the pipe-writing is left to the step.
    """

    STATE_JSON = "state.json"
    INPUTS_JSON = "inputs.json"

    def __init__(self, root: Path, rollout_id: str) -> None:
        _validate_component(rollout_id)
        self.root = root
        self.rollout_id = rollout_id
        self.path = root / "rollouts" / rollout_id

    def ensure(self) -> None:
        """Create the rollout tree without following any path component."""
        root_fd = _open_directory_chain(self.root)
        try:
            rollouts_fd = _create_and_open_directory_at(root_fd, "rollouts")
        finally:
            os.close(root_fd)
        try:
            rollout_fd = _create_and_open_directory_at(rollouts_fd, self.rollout_id)
        finally:
            os.close(rollouts_fd)
        try:
            logs_fd = _create_and_open_directory_at(rollout_fd, "logs")
            os.close(logs_fd)
        finally:
            os.close(rollout_fd)

    def exists(self) -> bool:
        try:
            fd = _open_rollout_directory(self.root, self.rollout_id)
        except OSError:
            return False
        os.close(fd)
        return True

    # -------- top-level artifacts --------

    def state_path(self) -> Path:
        return self.path / self.STATE_JSON

    def inputs_path(self) -> Path:
        return self.path / self.INPUTS_JSON

    def driver_log_path(self) -> Path:
        return self.path / "logs" / "driver.log"

    def write_inputs(self, inputs: dict[str, Any]) -> None:
        """Persist the resolved CLI args + config shas for --resume audit."""
        directory_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            _atomic_private_write_text_at(
                directory_fd,
                self.INPUTS_JSON,
                json.dumps(inputs, indent=2, sort_keys=True) + "\n",
            )
        finally:
            os.close(directory_fd)

    def write_state(self, document: dict[str, Any]) -> None:
        """Atomically persist a redacted state document in the rollout dir."""
        redacted = redact_rollout_mapping(document)
        if not isinstance(redacted, dict):  # pragma: no cover - mapping contract
            raise TypeError("redacted rollout state must remain a mapping")
        directory_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            _atomic_private_write_text_at(
                directory_fd,
                self.STATE_JSON,
                json.dumps(redacted, indent=2, sort_keys=True) + "\n",
            )
        finally:
            os.close(directory_fd)

    def append_driver_log(self, text: str) -> None:
        """Append one redacted diagnostic through the trusted logs descriptor."""
        rollout_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            logs_fd = _open_directory_at(rollout_fd, "logs")
        finally:
            os.close(rollout_fd)
        try:
            _append_private_text_at(
                logs_fd,
                "driver.log",
                redact_rollout_text(text),
            )
        finally:
            os.close(logs_fd)

    def read_inputs(self) -> dict[str, Any]:
        directory_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            return _read_json_object_at(directory_fd, self.INPUTS_JSON)
        finally:
            os.close(directory_fd)

    def read_resume_documents(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """Read pre-existing resume anchors without following symlinks.

        Opening the rollout directory and both files by descriptor ensures a
        brokered resume cannot silently create, repair, or follow a replaced
        evidence path while validating its immutable inputs and state.
        """
        documents = self.read_resume_documents_if_present()
        if documents is None:
            raise FileNotFoundError(self.path)
        return documents

    def read_resume_documents_if_present(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """Read resume anchors, or ``None`` only if the rollout is absent.

        An existing rollout with missing, malformed, oversized, or linked
        inputs/state is an error. Parent and rollout symlinks are likewise an
        error, so callers cannot mistake an unsafe path for a fresh rollout.
        """
        directory_fd = _open_rollout_directory_if_present(self.root, self.rollout_id)
        if directory_fd is None:
            return None
        try:
            metadata = os.fstat(directory_fd)
            if not stat.S_ISDIR(metadata.st_mode):  # pragma: no cover - O_DIRECTORY
                raise ValueError("rollout evidence path must be a directory")
            inputs = _read_json_object_at(directory_fd, self.INPUTS_JSON)
            state = _read_json_object_at(directory_fd, self.STATE_JSON)
        finally:
            os.close(directory_fd)
        return inputs, state

    # -------- per-step subdir --------

    def step_dir(self, number: int, name: str) -> StepDir:
        """Return the per-step subdirectory, creating it if missing."""
        slug = _slugify(name)
        subdir_name = f"{number:02d}-{slug}"
        subdir = self.path / subdir_name
        rollout_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            step_fd = _create_and_open_directory_at(rollout_fd, subdir_name)
            os.close(step_fd)
        finally:
            os.close(rollout_fd)
        return StepDir(number=number, name=name, path=subdir)

    def existing_step_dir(self, number: int, name: str) -> StepDir | None:
        """Return the per-step subdirectory only if it already exists.

        Used by resume: distinguishing "step never started" (no dir)
        from "step was interrupted" (dir + partial result.json) is
        one of the recovery signals.
        """
        slug = _slugify(name)
        subdir_name = f"{number:02d}-{slug}"
        subdir = self.path / subdir_name
        rollout_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            try:
                step_fd = _open_directory_at(rollout_fd, subdir_name)
            except FileNotFoundError:
                return None
            os.close(step_fd)
            return StepDir(number=number, name=name, path=subdir)
        finally:
            os.close(rollout_fd)

    def _step_component(self, step_dir: StepDir) -> str:
        component = f"{step_dir.number:02d}-{_slugify(step_dir.name)}"
        if step_dir.path != self.path / component:
            raise ValueError("step directory does not belong to this evidence directory")
        return component

    def write_step_result(self, step_dir: StepDir, result: dict[str, Any]) -> None:
        redacted = redact_rollout_mapping(result)
        if not isinstance(redacted, dict):  # pragma: no cover - mapping contract
            raise TypeError("redacted step result must remain a mapping")
        rollout_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            step_fd = _open_directory_at(rollout_fd, self._step_component(step_dir))
        finally:
            os.close(rollout_fd)
        try:
            _atomic_private_write_text_at(
                step_fd,
                "result.json",
                json.dumps(redacted, indent=2, sort_keys=True) + "\n",
            )
        finally:
            os.close(step_fd)

    def read_step_result(self, step_dir: StepDir) -> dict[str, Any] | None:
        rollout_fd = _open_rollout_directory(self.root, self.rollout_id)
        try:
            try:
                step_fd = _open_directory_at(rollout_fd, self._step_component(step_dir))
            except FileNotFoundError:
                return None
        finally:
            os.close(rollout_fd)
        try:
            try:
                return _read_json_object_at(step_fd, "result.json")
            except FileNotFoundError:
                return None
        finally:
            os.close(step_fd)

    # -------- discovery: find in-progress rollouts --------

    @classmethod
    def find_in_progress(
        cls,
        root: Path,
        image_tag: str,
    ) -> EvidenceDirectory | None:
        """Return the most-recent rollout dir under ``root`` whose id
        contains the slugified ``image_tag`` and whose state.json exists
        with ``status != done``.

        Used by --resume auto-detect: if the operator re-invokes the
        driver without --resume against an in-progress rollout, we
        surface the existing dir so they can pick up rather than
        accidentally start a fresh one.
        """
        rollouts_dir = root / "rollouts"
        try:
            rollouts_fd = _open_directory_chain(rollouts_dir)
        except OSError:
            return None
        try:
            slug = _slugify(image_tag)
            candidates = sorted(
                (name for name in os.listdir(rollouts_fd) if name.endswith(f"-{slug}")),
                reverse=True,
            )
            for candidate_name in candidates:
                try:
                    candidate_fd = os.open(
                        candidate_name,
                        _directory_open_flags(),
                        dir_fd=rollouts_fd,
                    )
                except OSError:
                    continue
                try:
                    state_doc = _read_json_object_at(candidate_fd, cls.STATE_JSON)
                except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                    continue
                finally:
                    os.close(candidate_fd)
                if state_doc.get("status") != "done":
                    return cls(root, candidate_name)
            return None
        finally:
            os.close(rollouts_fd)
