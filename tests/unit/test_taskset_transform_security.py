"""Malicious transform containment tests (#242 sub-plan 8).

These tests mock subprocess/resource calls instead of running real fork bombs
or long-blocking network scripts. Real subprocess timeout behaviour is covered
in ``test_taskset_transform_sandbox.py``; spawning fork bombs here can exhaust
macOS process slots on developer machines.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from loom.taskset.transform_sandbox import (
    TransformSandboxConfig,
    TransformSandboxError,
    _child_setup,
    run_transform,
)

_CONFIG = TransformSandboxConfig(
    enabled=True,
    network_isolated=True,
    wall_timeout_sec=2,
    cpu_limit_sec=2,
    memory_limit_mb=64,
)


def test_run_transform_passes_empty_env() -> None:
    """Transform subprocess must not inherit parent environment."""
    with patch("loom.taskset.transform_sandbox.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"id": "1"}).encode(),
            stderr=b"",
        )
        out = run_transform(
            transform_script=b"def transform(row):\n    return row\n",
            row={"id": "1"},
            config=_CONFIG,
            manifest_timeout_s=None,
        )
    assert out == {"id": "1"}
    assert mock_run.call_args.kwargs["env"] == {}


def test_child_setup_applies_resource_limits_and_network_isolation() -> None:
    """Child preexec applies CPU/memory/fd limits and requests network isolation."""
    with (
        patch("loom.taskset.transform_sandbox._try_unshare_network") as mock_unshare,
        patch("loom.taskset.transform_sandbox.resource.setrlimit") as mock_setrlimit,
    ):
        _child_setup(cpu_limit_sec=10, memory_limit_mb=256, network_isolated=True)

    mock_unshare.assert_called_once()
    limit_names = {call.args[0] for call in mock_setrlimit.call_args_list}
    import resource

    assert resource.RLIMIT_CPU in limit_names
    assert resource.RLIMIT_AS in limit_names
    assert resource.RLIMIT_NOFILE in limit_names


def test_run_transform_surfaces_subprocess_timeout_as_limit_exceeded() -> None:
    """Wall-clock timeout in the parent maps to transform_limit_exceeded."""
    import subprocess

    with patch("loom.taskset.transform_sandbox.subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd=["python"],
            timeout=2,
            stderr=b"blocked on network",
        )
        with pytest.raises(TransformSandboxError) as exc_info:
            run_transform(
                transform_script=b"def transform(row):\n    return row\n",
                row={"id": "1"},
                config=_CONFIG,
                manifest_timeout_s=None,
            )
    assert exc_info.value.code == "transform_limit_exceeded"


def test_run_transform_surfaces_signal_kill_as_limit_exceeded() -> None:
    """A child killed by signal (e.g. RLIMIT) maps to transform_limit_exceeded."""
    with patch("loom.taskset.transform_sandbox.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=-9,
            stdout=b"",
            stderr=b"killed",
        )
        with pytest.raises(TransformSandboxError) as exc_info:
            run_transform(
                transform_script=b"def transform(row):\n    return row\n",
                row={"id": "1"},
                config=_CONFIG,
                manifest_timeout_s=None,
            )
    assert exc_info.value.code == "transform_limit_exceeded"
