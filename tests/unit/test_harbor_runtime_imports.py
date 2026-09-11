"""The native controller does not import worker-only storage or DB clients."""

from __future__ import annotations

import subprocess
import sys


def test_native_controller_import_has_no_storage_or_database_side_effects() -> None:
    subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import sys; import loom.service_execution_sandbox_task; "
            "assert not any(name in sys.modules for name in "
            "('loom.db.schema', 'loom.trajectory.writer', 'loom.agent.litellm', "
            "'loom.security.secret_store', 'boto3', 'sqlalchemy', "
            "'loom.execution_image_admission', 'rfc8785'))",
        ],
        check=True,
    )


def test_lazy_security_exports_preserve_public_objects() -> None:
    import loom.security as security
    from loom.security import secret_store

    for name in security.__all__:
        assert getattr(security, name) is getattr(secret_store, name)
    try:
        _ = security.not_a_security_export
    except AttributeError:
        pass
    else:
        raise AssertionError("unknown module attributes must still raise AttributeError")


def test_unknown_agent_error_does_not_expose_its_message() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import runpy; from unittest.mock import patch; from loom.errors import AgentError; "
            "patcher = patch('argparse.ArgumentParser.parse_args', "
            "side_effect=AgentError('private-token-fixture')); patcher.start(); "
            "runpy.run_module('loom.service_execution_sandbox_task', run_name='__main__')",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    assert result.stderr == "isolated execution failed (AgentError)\n"
    assert result.stdout == ""
