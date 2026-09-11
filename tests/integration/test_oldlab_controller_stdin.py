"""Exercise real Docker stdin forwarding without controller or host authority."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.operator.installed_preflight_commands import InstalledPreflightCommands
from loom_cli.rollout.operator.protected_controller_discovery import ControllerDiscoveryRequest
from loom_cli.rollout.operator.protected_controller_prerequisite_transport import (
    FixedOldlabControllerPrerequisiteInvoker,
)
from loom_cli.rollout.operator.protected_pool_credential_transport import (
    FixedOldlabPoolCredentialInvoker,
)
from loom_cli.rollout.operator.protected_prepared_controller_transport import (
    FixedOldlabPreparedControllerInvoker,
)
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config
from tests.loom_cli.rollout.operator.test_installed_preflight_commands import _environment
from tests.loom_cli.rollout.operator.test_protected_pool_credential_transport import _payload
from tests.loom_cli.rollout.operator.test_protected_prepared_controller_transport import _request

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]

_IMAGE = "192.168.50.13:5000/loom-capacity-executor@sha256:" + "a" * 64
_BUSYBOX = (
    "docker.io/library/busybox@sha256:"
    "dc2d74b28e4cf8984fa52af1f39bc7c3d9c73760b41a74d629f5d11b1ab28616"
)


def _echo_in_unprivileged_container(
    argv: Sequence[str],
    *,
    cwd: Path | None,
    env: Mapping[str, str],
    input: str | None,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Keep actual transport I/O flags; replace privileged execution with cat.

    InstalledPreflightCommands has already validated the production command.
    This test never runs its installer, host mount, or host PID namespace.
    It proves only request byte delivery, not controller discovery/admission.
    """
    command = list(argv)
    command.remove("--privileged")
    command.remove("--pid=host")
    for option in ("--mount", "--entrypoint"):
        index = command.index(option)
        del command[index:index + 2]
    command[command.index("--user") + 1] = "65534:65534"
    image_index = next(
        index for index, value in enumerate(command)
        if value.startswith("localhost:5000/loom-capacity-executor@sha256:")
    )
    command[image_index:] = [_BUSYBOX, "cat"]
    return subprocess.run(
        command, cwd=cwd, env=dict(env), input=input,
        capture_output=True, text=True, check=False, timeout=min(timeout, 60),
    )


@pytest.mark.parametrize(
    "operation",
    [
        "discover-controller", "observe-prerequisite", "converge-prerequisite",
        "observe-credential", "publish-credential", "observe-prepared",
        "converge-prepared-files", "enable-prepared-timer", "run-prepared-tick",
        "disable-prepared-timer",
    ],
)
def test_oldlab_controller_delivers_canonical_request_to_container(
    tmp_path: Path, operation: str,
) -> None:
    commands = InstalledPreflightCommands(
        _config(tmp_path), _environment(),
        run_subprocess=_echo_in_unprivileged_container,
    )
    prerequisite_invoker = FixedOldlabControllerPrerequisiteInvoker(
        run=commands.oldlab_controller, image=_IMAGE,
    )
    if operation == "discover-controller":
        payload = ControllerDiscoveryRequest(
            schema_version=1, pool_id="oldlab",
            transport_authority_sha256=prerequisite_invoker.authority_sha256,
        ).to_bytes()
        result = prerequisite_invoker(operation, payload)
    elif operation in {"observe-credential", "publish-credential"}:
        payload = _payload(tmp_path, "oldlab").to_bytes()
        result = FixedOldlabPoolCredentialInvoker(
            run=commands.oldlab_controller, image=_IMAGE,
        )(operation, payload)
    else:
        prepared = _request(tmp_path, pool_id="oldlab")
        prerequisite = replace(
            prepared.prerequisite, image=_IMAGE,
            transport_authority_sha256=prerequisite_invoker.authority_sha256,
        )
        if operation in {"observe-prerequisite", "converge-prerequisite"}:
            payload = prerequisite.to_bytes()
            result = prerequisite_invoker(operation, payload)
        else:
            payload = replace(
                prepared, prerequisite=prerequisite,
                transport_authority_sha256=prerequisite_invoker.authority_sha256,
            ).to_bytes()
            result = FixedOldlabPreparedControllerInvoker(
                run=commands.oldlab_controller, image=_IMAGE,
            )(operation, payload)

    assert result.returncode == 0, result.stderr
    assert result.stdout.encode("ascii") == payload
