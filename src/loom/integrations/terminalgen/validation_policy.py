"""Dependency-light TerminalTask validation policy and argv contract."""

from __future__ import annotations

from collections.abc import Sequence

from loom.pipeline.keys import canonical_digest

TERMINALGEN_VALIDATION_POLICY_DIGEST = canonical_digest(
    {
        "schema_version": "loom.terminal-task-validation-policy.v1",
        "backend": "rootless-buildkit-oci-v1",
        "network_profile": "none",
        "repeat_count": 2,
        "modes": ["baseline_unsolved", "reference_solution"],
        "stage_receives_runtime_socket": False,
        "task_base_must_be_digest_pinned": True,
        "dependency_resolver_must_be_digest_pinned": True,
        "dependency_lock_required": True,
    }
)

_VALIDATION_ARGV_PREFIX = (
    "python",
    "-m",
    "loom.integrations.terminalgen.cli",
    "run",
)


def terminalgen_validation_argv(
    *,
    node_key: str,
    task_base_image: str,
    dependency_resolver_image: str,
    dependency_allowlist_digest: str,
) -> list[str]:
    """Return the one byte-stable validator argv accepted by claim authority."""

    return [
        *_VALIDATION_ARGV_PREFIX,
        node_key,
        "--validation-backend",
        "rootless-buildkit-oci-v1",
        "--task-base-image",
        task_base_image,
        "--dependency-resolver-image",
        dependency_resolver_image,
        "--dependency-allowlist-sha256",
        dependency_allowlist_digest,
        "--validation-policy-sha256",
        TERMINALGEN_VALIDATION_POLICY_DIGEST,
    ]


def parse_terminalgen_validation_argv(
    *,
    node_key: str,
    argv: Sequence[object],
) -> tuple[str, str, str]:
    """Parse only the exact code-owned validation argv shape."""

    values = list(argv)
    if len(values) != 15 or values[:5] != [*_VALIDATION_ARGV_PREFIX, node_key]:
        raise ValueError("terminalgen_validation_argv_mismatch")
    expected_literals = {
        5: "--validation-backend",
        6: "rootless-buildkit-oci-v1",
        7: "--task-base-image",
        9: "--dependency-resolver-image",
        11: "--dependency-allowlist-sha256",
        13: "--validation-policy-sha256",
        14: TERMINALGEN_VALIDATION_POLICY_DIGEST,
    }
    if any(values[index] != expected for index, expected in expected_literals.items()):
        raise ValueError("terminalgen_validation_argv_mismatch")
    if not all(isinstance(values[index], str) for index in (8, 10, 12)):
        raise ValueError("terminalgen_validation_argv_mismatch")
    return str(values[8]), str(values[10]), str(values[12])


__all__ = [
    "TERMINALGEN_VALIDATION_POLICY_DIGEST",
    "parse_terminalgen_validation_argv",
    "terminalgen_validation_argv",
]
