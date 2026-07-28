"""Strict loading and semantic validation for broker-published driver envelopes."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from loom_cli.cluster_backup_guard import validate_backup_manifest

from .backup_limits import operator_backup_traversal_limits
from .config import OperatorConfig, environment_authority
from .model import DriverEnvelope, RolloutRequest

DEFAULT_OPERATOR_CONFIG_PATH = Path("/etc/loom/staging-rollout.toml")
OPERATOR_CONFIG_ENV = "LOOM_STAGING_ROLLOUT_CONFIG"
ROLLOUT_CONFIG_ENV = "LOOM_ROLLOUT_CONFIG"
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_ENVELOPE_BYTES = 128 * 1024
_MAX_MANIFEST_BYTES = 1024 * 1024


class EnvelopeValidationError(ValueError):
    """Raised before rollout evidence or mutation when an envelope is unsafe."""


def fixed_operator_config_path(
    environ: Mapping[str, str] | None = None,
    *,
    environment: str = "staging",
) -> Path:
    """Return the one installed config path bound to ``environment``."""
    source = os.environ if environ is None else environ
    try:
        authority = environment_authority(environment)
    except ValueError as exc:
        raise EnvelopeValidationError(str(exc)) from exc
    generic = source.get(ROLLOUT_CONFIG_ENV)
    legacy = source.get(OPERATOR_CONFIG_ENV) if environment == "staging" else None
    if generic and legacy and generic != legacy:
        raise EnvelopeValidationError("operator config path authorities conflict")
    rendered = generic or legacy
    path = Path(rendered) if rendered else authority.config_path
    if path != authority.config_path:
        raise EnvelopeValidationError(
            f"operator config path must be the installed {environment} authority"
        )
    return path


def _validate_private_directory(path: Path, *, effective_uid: int, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise EnvelopeValidationError(
            f"{label} is unavailable in the private request store"
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise EnvelopeValidationError(f"{label} must be a directory, not a symlink")
    if metadata.st_uid != effective_uid:
        raise EnvelopeValidationError(f"{label} must be owned by the effective service UID")
    if stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE:
        raise EnvelopeValidationError(f"{label} must have mode 0700")


def _read_private_file(
    path: Path,
    *,
    effective_uid: int,
    label: str,
    max_bytes: int,
) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise EnvelopeValidationError(f"{label} must be a private regular file") from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise EnvelopeValidationError(f"{label} must be a regular file")
        if metadata.st_uid != effective_uid:
            raise EnvelopeValidationError(f"{label} must be owned by the effective service UID")
        if stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE:
            raise EnvelopeValidationError(f"{label} must have mode 0600")
        if metadata.st_size > max_bytes:
            raise EnvelopeValidationError(f"{label} exceeds the bounded size limit")
        payload = bytearray()
        while True:
            chunk = os.read(fd, min(65536, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > max_bytes:
                raise EnvelopeValidationError(f"{label} exceeds the bounded size limit")
        after = os.fstat(fd)
        if (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise EnvelopeValidationError(f"{label} changed while it was read")
        return bytes(payload)
    finally:
        os.close(fd)


def _strict_json_object(payload: bytes) -> dict[str, object]:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EnvelopeValidationError("driver envelope must be valid UTF-8") from exc

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key!r}")
            result[key] = value
        return result

    try:
        loaded = json.loads(
            decoded,
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite value {value!r}")
            ),
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise EnvelopeValidationError(f"driver envelope is not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise EnvelopeValidationError("driver envelope must be a JSON object")
    return cast(dict[str, object], loaded)


def _read_envelope(path: Path, config: OperatorConfig, *, effective_uid: int) -> DriverEnvelope:
    if not path.is_absolute() or ".." in path.parts:
        raise EnvelopeValidationError("driver envelope must be inside the private request store")
    try:
        relative = path.relative_to(config.state_root)
    except ValueError as exc:
        raise EnvelopeValidationError(
            "driver envelope must be inside the private request store"
        ) from exc
    parts = relative.parts
    if (
        len(parts) != 5
        or parts[0] != "requests"
        or parts[2] != "attempts"
        or parts[4] != "envelope.json"
        or not parts[3].isdigit()
        or int(parts[3]) < 1
    ):
        raise EnvelopeValidationError(
            "driver envelope path does not match the request store layout"
        )

    directories = (
        (config.state_root, "request store root"),
        (config.state_root / "requests", "requests directory"),
        (config.state_root / "requests" / parts[1], "request directory"),
        (config.state_root / "requests" / parts[1] / "attempts", "attempts directory"),
        (path.parent, "attempt directory"),
    )
    for directory, label in directories:
        _validate_private_directory(directory, effective_uid=effective_uid, label=label)
    payload = _read_private_file(
        path,
        effective_uid=effective_uid,
        label="driver envelope",
        max_bytes=_MAX_ENVELOPE_BYTES,
    )
    try:
        envelope = DriverEnvelope.from_dict(_strict_json_object(payload))
    except ValueError as exc:
        raise EnvelopeValidationError(str(exc)) from exc
    if envelope.request_id != parts[1] or envelope.attempt_number != int(parts[3]):
        raise EnvelopeValidationError(
            "driver envelope identity does not match its request store path"
        )
    request_payload = _read_private_file(
        config.state_root / "requests" / parts[1] / "request.json",
        effective_uid=effective_uid,
        label="immutable request",
        max_bytes=_MAX_ENVELOPE_BYTES,
    )
    try:
        request = RolloutRequest.from_dict(_strict_json_object(request_payload))
    except (EnvelopeValidationError, ValueError):
        raise EnvelopeValidationError("immutable request record is invalid") from None
    if request.status != "pending":
        raise EnvelopeValidationError("driver envelope requires a pending request record")
    request_binding = {
        "request_id": request.request_id,
        "rollout_id": request.rollout_id,
        "initiating_operator": request.caller.username,
        "initiating_uid": request.caller.uid,
        "remote_url": request.candidate.remote_url,
        "target_ref": request.candidate.target_ref,
        "resolved_sha": request.candidate.resolved_sha,
        "image_tag": request.candidate.image_tag,
        "fetched_at": request.candidate.fetched_at,
        "source_mode": request.candidate.source_mode,
        "resolved_tree": request.candidate.resolved_tree,
        "approved_base_sha": request.candidate.approved_base_sha,
        "runner_config_sha256": request.runner_config_sha256,
        "preflight_attestation_sha256": request.preflight_attestation_sha256,
        "preflight_registry_sha256": request.preflight_registry_sha256,
        "preflight_coverage_sha256": request.preflight_coverage_sha256,
    }
    envelope_binding = {
        "request_id": envelope.request_id,
        "rollout_id": envelope.rollout_id,
        "initiating_operator": envelope.initiating_operator,
        "initiating_uid": envelope.initiating_uid,
        "remote_url": envelope.remote_url,
        "target_ref": envelope.target_ref,
        "resolved_sha": envelope.resolved_sha,
        "image_tag": envelope.image_tag,
        "fetched_at": envelope.fetched_at,
        "source_mode": envelope.source_mode,
        "resolved_tree": envelope.resolved_tree,
        "approved_base_sha": envelope.approved_base_sha,
        "runner_config_sha256": envelope.runner_config_sha256,
        "preflight_attestation_sha256": envelope.preflight_attestation_sha256,
        "preflight_registry_sha256": envelope.preflight_registry_sha256,
        "preflight_coverage_sha256": envelope.preflight_coverage_sha256,
    }
    if request_binding != envelope_binding:
        raise EnvelopeValidationError("driver envelope does not match immutable request binding")
    if envelope.source_mode != config.source_mode:
        raise EnvelopeValidationError("driver envelope source mode does not match config")
    if config.source_mode == "sealed-cumulative" and (
        envelope.resolved_sha != config.source_commit_sha
        or envelope.resolved_tree != config.source_tree_sha
        or envelope.approved_base_sha != config.source_base_sha
    ):
        raise EnvelopeValidationError("driver envelope sealed source does not match config")
    if envelope.attempt_number > 1:
        first_directory = config.state_root / "requests" / envelope.request_id / "attempts" / "1"
        _validate_private_directory(
            first_directory,
            effective_uid=effective_uid,
            label="first attempt directory",
        )
        first_payload = _read_private_file(
            first_directory / "envelope.json",
            effective_uid=effective_uid,
            label="first attempt envelope",
            max_bytes=_MAX_ENVELOPE_BYTES,
        )
        try:
            first = DriverEnvelope.from_dict(_strict_json_object(first_payload))
        except (EnvelopeValidationError, ValueError):
            raise EnvelopeValidationError("first attempt envelope is invalid") from None
        if first.attempt_number != 1 or first.resume:
            raise EnvelopeValidationError(
                "first attempt identity must be attempt 1 with resume disabled"
            )
        first_binding = first.to_dict()
        current_binding = envelope.to_dict()
        for field_name in (
            "attempt_number",
            "attempt_operator",
            "attempt_uid",
            "resume",
        ):
            first_binding.pop(field_name)
            current_binding.pop(field_name)
        if first_binding != current_binding:
            raise EnvelopeValidationError("resume envelope does not match first attempt binding")
    return envelope


def _validate_config_binding(envelope: DriverEnvelope, config: OperatorConfig) -> None:
    expected: dict[str, object] = {
        "remote_url": config.remote_url,
        "cluster_name": config.cluster_name,
        "namespace": config.namespace,
        "environment": config.environment,
        "cp_url": config.cp_url,
        "cluster_config_path": str(config.cluster_config_path),
        "rollout_root": str(config.rollout_root),
        "admin_token_source": config.admin_token_source,
        "worker_token_source": config.worker_token_source,
        "service_token_source": config.service_token_source,
        "expect_admin_token_fingerprint": config.expect_admin_token_fingerprint,
        "smoke_on_behalf_username": config.smoke_on_behalf_username,
        "smoke_on_behalf_team_id": config.smoke_on_behalf_team_id,
        "scope": config.scope,
        "gb10_prep_concurrency": config.gb10_prep_concurrency,
    }
    for field_name, expected_value in expected.items():
        if getattr(envelope, field_name) != expected_value:
            raise EnvelopeValidationError(f"driver envelope {field_name} does not match config")
    authority = environment_authority(config.short_name)
    if (
        config.target_ref != authority.target_ref
        or envelope.target_ref != authority.pinned_target_ref
    ):
        raise EnvelopeValidationError(
            "driver envelope target ref does not match fixed environment policy"
        )
    if envelope.runner_config_sha256 != config.config_sha256:
        raise EnvelopeValidationError("driver envelope runner config digest does not match config")


def _validate_backup(
    envelope: DriverEnvelope, config: OperatorConfig, *, effective_uid: int
) -> None:
    manifest = Path(envelope.backup_manifest_path)
    backup_root = config.rollout_root / "backups"
    try:
        relative = manifest.relative_to(backup_root)
    except ValueError as exc:
        raise EnvelopeValidationError(
            "backup manifest is outside the configured backup root"
        ) from exc
    if len(relative.parts) != 2 or relative.parts[1] != "backup-manifest.json":
        raise EnvelopeValidationError("backup manifest must be an immutable timestamped manifest")
    if relative.parts[0] == "latest":
        raise EnvelopeValidationError("backup manifest must not use the mutable latest pointer")

    problems = validate_backup_manifest(
        manifest,
        environment=envelope.environment,
        namespace=envelope.namespace,
        min_remaining_hours=2,
        expected_owner_uid=effective_uid,
        require_private_files=True,
        enforce_freshness=not envelope.resume,
        limits=operator_backup_traversal_limits(config),
    )
    if problems:
        raise EnvelopeValidationError("backup manifest validation failed: " + "; ".join(problems))
    payload = _read_private_file(
        manifest,
        effective_uid=effective_uid,
        label="backup manifest",
        max_bytes=_MAX_MANIFEST_BYTES,
    )
    if hashlib.sha256(payload).hexdigest() != envelope.backup_manifest_sha256:
        raise EnvelopeValidationError("backup manifest digest does not match driver envelope")


def load_validated_envelope(
    path: Path,
    config: OperatorConfig,
    *,
    effective_uid: int,
) -> DriverEnvelope:
    """Load a private immutable envelope and revalidate all protected bindings."""
    try:
        envelope = _read_envelope(Path(path), config, effective_uid=effective_uid)
        _validate_config_binding(envelope, config)
        _validate_backup(envelope, config, effective_uid=effective_uid)
        return envelope
    except EnvelopeValidationError as exc:
        raise EnvelopeValidationError(str(exc)) from None
    except Exception:
        raise EnvelopeValidationError("driver envelope validation failed safely") from None


__all__ = [
    "DEFAULT_OPERATOR_CONFIG_PATH",
    "OPERATOR_CONFIG_ENV",
    "EnvelopeValidationError",
    "fixed_operator_config_path",
    "load_validated_envelope",
]
