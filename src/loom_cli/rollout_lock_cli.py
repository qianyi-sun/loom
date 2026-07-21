"""Shared CLI contracts for protected rollout mutation leases.

Both ``loom cluster up`` and ``loom admin environment-state`` expose the same
rollout-lock arguments and broker-owned evidence path checks.  Keep those
security-sensitive contracts here so the two command surfaces cannot drift.
"""

from __future__ import annotations

import argparse
import os
import stat
from pathlib import Path
from typing import Any

from loom_cli.rollout_lock import DEFAULT_ROLLOUT_LOCK_TTL_SECONDS

EXPLICIT_ROLLOUT_LOCK_OPTIONS_ATTR = "_explicit_rollout_lock_options"
BROKER_LOCK_OPTIONS = frozenset(
    {
        "--rollout-id",
        "--rollout-lock-dir",
        "--rollout-lock-ttl-seconds",
        "--rollout-lock-evidence",
        "--force-rollout-lock",
    }
)


def _record_explicit_rollout_lock_option(
    namespace: argparse.Namespace,
    option_string: str | None,
) -> None:
    options = set(getattr(namespace, EXPLICIT_ROLLOUT_LOCK_OPTIONS_ATTR, ()))
    if option_string is not None:
        options.add(option_string)
    setattr(namespace, EXPLICIT_ROLLOUT_LOCK_OPTIONS_ATTR, options)


class ExplicitRolloutLockStore(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del parser
        setattr(namespace, self.dest, values)
        _record_explicit_rollout_lock_option(namespace, option_string)


class ExplicitRolloutLockStoreTrue(argparse.Action):
    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        default: object = False,
        required: bool = False,
        help: str | None = None,
    ) -> None:
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            nargs=0,
            default=default,
            required=required,
            help=help,
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del parser, values
        setattr(namespace, self.dest, True)
        _record_explicit_rollout_lock_option(namespace, option_string)


def add_rollout_lock_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rollout-id",
        default=None,
        action=ExplicitRolloutLockStore,
        help=(
            "Operator-visible protected rollout owner id. Defaults to "
            "environment-hostname-pid when a lock is required."
        ),
    )
    parser.add_argument(
        "--rollout-lock-dir",
        type=Path,
        default=None,
        action=ExplicitRolloutLockStore,
        help=(
            "Directory for per-environment rollout mutation leases. Defaults "
            "to $LOOM_ROLLOUT_LOCK_DIR or ~/.loom/rollout-locks for protected "
            "environments."
        ),
    )
    parser.add_argument(
        "--rollout-lock-ttl-seconds",
        type=int,
        default=DEFAULT_ROLLOUT_LOCK_TTL_SECONDS,
        action=ExplicitRolloutLockStore,
        help=(
            "Protected rollout mutation lease TTL in seconds "
            f"(default: {DEFAULT_ROLLOUT_LOCK_TTL_SECONDS})."
        ),
    )
    parser.add_argument(
        "--rollout-lock-evidence",
        type=Path,
        default=None,
        action=ExplicitRolloutLockStore,
        help="Optional JSON evidence path for rollout lock acquire/release events.",
    )
    parser.add_argument(
        "--force-rollout-lock",
        action=ExplicitRolloutLockStoreTrue,
        help=(
            "Replace an active protected rollout mutation lease. Use only "
            "after preserving evidence that the recorded owner is stale."
        ),
    )


def load_broker_rollout_envelope(path: Path) -> tuple[Any, Any]:
    from loom_cli.rollout.operator.config import OperatorConfig
    from loom_cli.rollout.operator.envelope import (
        fixed_operator_config_path,
        load_validated_envelope,
    )

    config = OperatorConfig.load(fixed_operator_config_path())
    envelope = load_validated_envelope(
        path,
        config,
        effective_uid=os.geteuid(),
    )
    return config, envelope


def require_real_directory(
    path: Path,
    *,
    label: str,
    expected_owner_uid: int | None = None,
) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{label} must be a real directory, not a symlink")
    if expected_owner_uid is not None and metadata.st_uid != expected_owner_uid:
        raise ValueError(f"{label} must be service-owned")


def require_real_file(
    path: Path,
    *,
    label: str,
    expected_owner_uid: int | None = None,
) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular file, not a symlink")
    if expected_owner_uid is not None and metadata.st_uid != expected_owner_uid:
        raise ValueError(f"{label} must be service-owned")


def fixed_rollout_lock_evidence_path(
    config: Any,
    envelope: Any,
    *,
    step_directory: str,
) -> Path:
    root = Path(config.rollout_root)
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("rollout lock evidence root must be an absolute fixed path")
    rollout_parent = root / "rollouts"
    rollout_dir = rollout_parent / str(envelope.rollout_id)
    evidence_parent = rollout_dir / step_directory
    service_uid = os.geteuid()
    for path, label, owner_uid in (
        (root, "rollout lock evidence root", None),
        (rollout_parent, "rollout lock evidence rollouts directory", None),
        (rollout_dir, "rollout lock evidence rollout directory", service_uid),
        (evidence_parent, "rollout lock evidence parent", service_uid),
    ):
        require_real_directory(path, label=label, expected_owner_uid=owner_uid)
    evidence_path = evidence_parent / "rollout-lock.json"
    try:
        metadata = evidence_path.lstat()
    except FileNotFoundError:
        return evidence_path
    except OSError as exc:
        raise ValueError("rollout lock evidence path is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("rollout lock evidence path must not be a symlink")
    if metadata.st_uid != service_uid:
        raise ValueError("rollout lock evidence path must be service-owned")
    return evidence_path
