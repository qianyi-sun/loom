"""Caller authentication and child-environment policy for the rollout broker."""

from __future__ import annotations

import pwd
import re
from collections.abc import Callable, Collection, Mapping

from .config import OperatorConfig
from .model import CallerIdentity

_UID_RE = re.compile(r"^(0|[1-9][0-9]*)$")
_CHILD_SYSTEM_PATH = "/usr/local/bin:/usr/bin:/bin"

GroupResolver = Callable[[str], Collection[str]]


class PolicyError(PermissionError):
    """Raised when broker caller authentication fails closed."""


def _passwd_entry(username: str, *, description: str) -> pwd.struct_passwd:
    try:
        return pwd.getpwnam(username)
    except (KeyError, OSError) as exc:
        raise PolicyError(f"unknown {description}: {username!r}") from exc


def caller_from_sudo(
    config: OperatorConfig,
    environ: Mapping[str, str],
    *,
    euid: int,
    groups: GroupResolver,
) -> CallerIdentity:
    """Authenticate an approved operator from sudo-provided identity metadata."""
    service_entry = _passwd_entry(config.service_user, description="service account")
    if euid != service_entry.pw_uid:
        raise PolicyError("broker effective UID does not match the configured service account")

    sudo_user = environ.get("SUDO_USER")
    sudo_uid_raw = environ.get("SUDO_UID")
    if not sudo_user or sudo_uid_raw is None:
        raise PolicyError("SUDO_USER and SUDO_UID are required")
    if _UID_RE.fullmatch(sudo_uid_raw) is None:
        raise PolicyError("SUDO_UID must be an unsigned decimal integer")
    sudo_uid = int(sudo_uid_raw)

    caller_entry = _passwd_entry(sudo_user, description="sudo user")
    if caller_entry.pw_uid != sudo_uid:
        raise PolicyError("sudo username/UID pair does not match the passwd database")

    try:
        caller_groups = groups(sudo_user)
    except (KeyError, OSError) as exc:
        raise PolicyError("could not resolve caller operator group membership") from exc
    if config.operator_group not in caller_groups:
        raise PolicyError("caller is not a member of the configured operator group")

    return CallerIdentity(username=sudo_user, uid=sudo_uid)


def sanitized_child_environment(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> dict[str, str]:
    """Build the complete, non-inheriting environment for broker child processes."""
    runtime_dir = f"/run/user/{service_uid}"
    candidate_venv_bin = config.runner_repo.parent / "venv" / "bin"
    environment = {
        "HOME": str(config.state_root),
        "USER": config.service_user,
        "LOGNAME": config.service_user,
        "PATH": f"{candidate_venv_bin}:{_CHILD_SYSTEM_PATH}",
        "PYTHONDONTWRITEBYTECODE": "1",
        "XDG_RUNTIME_DIR": runtime_dir,
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path={runtime_dir}/bus",
        "KUBECONFIG": str(config.kubeconfig_path),
        "LC_ALL": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
    }
    config_variable = (
        "LOOM_STAGING_ROLLOUT_CONFIG"
        if config.short_name == "staging"
        else "LOOM_ROLLOUT_CONFIG"
    )
    environment[config_variable] = str(config.config_path)
    return environment


__all__ = [
    "GroupResolver",
    "PolicyError",
    "caller_from_sudo",
    "sanitized_child_environment",
]
