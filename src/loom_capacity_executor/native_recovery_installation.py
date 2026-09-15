"""Deterministic restricted endpoint assets for protected bootstrap.

Rendering writes nothing and activates no authority. A merged-source installer
must verify root ownership, immutable interpreter/import tree, retained policy,
OS SSH/sudo binaries, local lifecycle constraints, account/key isolation, exact
allowlist and rollback before installing these bytes. Use a separate SSH daemon
and port so inherited host SSH policy cannot add commands or accepted env vars.
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from pathlib import Path

from pydantic import Field, field_validator

from loom_capacity_executor.native_installed_release import _path
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes


class NativeRecoveryEndpointV1(StrictV1Model):
    address: str
    port: int = Field(ge=1024, le=65535)
    release_root: str
    python: str
    policy: str
    policy_sha256: Digest
    host_key: str
    authorized_keys: str

    @field_validator("address")
    @classmethod
    def _address(cls, value: str) -> str:
        address = ipaddress.ip_address(value)
        if str(address) != value or "%" in value or address.is_unspecified or address.is_multicast:
            raise ValueError("recovery endpoint requires one canonical specific address")
        return value

    @field_validator("release_root", "python", "policy", "host_key", "authorized_keys")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        _path(value)
        if len(value) > 210 or re.fullmatch(r"/[A-Za-z0-9_./-]+", value) is None:
            raise ValueError("recovery endpoint path is unsafe for fixed launch assets")
        return value


@dataclass(frozen=True, slots=True)
class NativeRecoveryEndpointAssets:
    helper: bytes
    ssh_entry: bytes
    sshd_config: bytes
    sudoers: bytes


def render_native_recovery_endpoint(config: NativeRecoveryEndpointV1) -> NativeRecoveryEndpointAssets:
    config = NativeRecoveryEndpointV1.model_validate_json(canonical_bytes(config))
    helper = str(Path(config.release_root) / "helper")
    entry = str(Path(config.release_root) / "ssh-entry")
    helper_wire = f'''#!{config.python} -IB
import signal
import sys
from loom_capacity_executor.native_node_recovery import run_native_recovery_helper
from loom_capacity_manager.contracts import canonical_bytes

def expired(_signal, _frame):
    raise TimeoutError("native recovery deadline")

signal.signal(signal.SIGALRM, expired)
signal.alarm(80)
try:
    result = run_native_recovery_helper(policy_path={config.policy!r}, policy_sha256={config.policy_sha256!r})
    sys.stdout.buffer.write(canonical_bytes(result) + b"\\n")
    sys.stdout.buffer.flush()
except (Exception, KeyboardInterrupt):
    sys.stderr.write("native recovery incomplete\\n")
    raise SystemExit(1) from None
finally:
    signal.alarm(0)
'''.encode("ascii")
    ssh_entry = f'''#!/bin/sh
set -eu
[ "$#" -eq 0 ] || exit 64
[ -z "${{SSH_ORIGINAL_COMMAND-}}" ] || exit 64
exec /usr/bin/sudo -n -- {helper}
'''.encode("ascii")
    sshd = f'''Port {config.port}
ListenAddress {config.address}
HostKey {config.host_key}
PidFile /run/loom-native-recovery/sshd.pid
AllowUsers loom-native-recovery
AuthorizedKeysFile {config.authorized_keys}
AuthenticationMethods publickey
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
HostbasedAuthentication no
PermitRootLogin no
PermitEmptyPasswords no
UsePAM no
StrictModes yes
PermitUserEnvironment no
PermitUserRC no
DisableForwarding yes
AllowAgentForwarding no
AllowTcpForwarding no
X11Forwarding no
PermitTunnel no
PermitTTY no
PermitOpen none
PermitListen none
GatewayPorts no
MaxSessions 1
MaxStartups 2:30:4
LoginGraceTime 10
PrintMotd no
LogLevel ERROR
ForceCommand {entry}
'''.encode("ascii")
    sudoers = f'''Defaults:loom-native-recovery env_reset
Defaults:loom-native-recovery !setenv
Defaults:loom-native-recovery secure_path=/usr/sbin:/usr/bin:/sbin:/bin
loom-native-recovery ALL=(root:root) NOPASSWD:NOSETENV: {helper} ""
'''.encode("ascii")
    return NativeRecoveryEndpointAssets(helper=helper_wire, ssh_entry=ssh_entry, sshd_config=sshd, sudoers=sudoers)
