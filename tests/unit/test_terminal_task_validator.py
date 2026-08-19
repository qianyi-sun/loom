from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from loom.integrations.terminalgen.authority import TERMINALGEN_VALIDATION_POLICY_DIGEST
from loom.pipeline.keys import canonical_document
from loom_worker.terminal_task_validator import (
    TerminalTaskValidatorError,
    attest_terminal_task_validator,
)


def _helper(tmp_path: Path, *, policy_digest: str = TERMINALGEN_VALIDATION_POLICY_DIGEST) -> Path:
    probe = canonical_document(
        {
            "schema_version": "loom.terminal-task-validator-probe.v1",
            "backend": "rootless-buildkit-oci-v1",
            "validation_policy_sha256": policy_digest,
            "rootless": True,
            "network_profile": "none",
            "runtime_socket_exposed": False,
            "process_group_isolation": True,
        }
    )
    path = tmp_path / "validator"
    path.write_text(
        "#!/bin/sh\n"
        "[ \"$1:$2:$3\" = 'probe:--format:json' ] || exit 64\n"
        f"printf '%s' '{probe.decode('utf-8')}'\n",
        encoding="utf-8",
    )
    path.chmod(0o700)
    return path


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def test_attested_validator_requires_exact_binary_and_closed_probe(tmp_path: Path) -> None:
    helper = _helper(tmp_path)

    attestation = attest_terminal_task_validator(
        helper,
        _digest(helper),
        expected_owner_uid=os.getuid(),
    )

    assert attestation.executable == helper
    assert attestation.probe.rootless is True
    assert attestation.probe.runtime_socket_exposed is False
    assert attestation.probe.network_profile == "none"


def test_attested_validator_rejects_binary_and_policy_drift(tmp_path: Path) -> None:
    helper = _helper(tmp_path)
    digest = _digest(helper)
    helper.write_text(helper.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8")

    with pytest.raises(TerminalTaskValidatorError, match="digest_drift"):
        attest_terminal_task_validator(helper, digest, expected_owner_uid=os.getuid())

    stale = _helper(tmp_path, policy_digest="sha256:" + "f" * 64)
    with pytest.raises(TerminalTaskValidatorError, match="policy_drift"):
        attest_terminal_task_validator(stale, _digest(stale), expected_owner_uid=os.getuid())


def test_attested_validator_rejects_symlink_and_writable_file(tmp_path: Path) -> None:
    helper = _helper(tmp_path)
    helper.chmod(0o722)
    with pytest.raises(TerminalTaskValidatorError, match="file_untrusted"):
        attest_terminal_task_validator(helper, _digest(helper), expected_owner_uid=os.getuid())

    helper.chmod(0o700)
    link = tmp_path / "validator-link"
    link.symlink_to(helper)
    with pytest.raises(TerminalTaskValidatorError, match="file_untrusted"):
        attest_terminal_task_validator(link, _digest(helper), expected_owner_uid=os.getuid())
