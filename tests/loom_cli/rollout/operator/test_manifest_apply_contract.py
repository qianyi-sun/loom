from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.operator.manifest_apply_contract import (
    MANIFEST_APPLY_CONTRACT_DIGEST,
    MANIFEST_FIELD_MANAGER,
    server_side_apply_argv,
    server_side_diff_argv,
)


def test_preflight_dry_run_and_final_apply_share_one_exact_contract() -> None:
    final = server_side_apply_argv("loom-staging")
    preflight = server_side_apply_argv(
        "loom-staging",
        kubeconfig=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        dry_run=True,
    )

    assert len(MANIFEST_APPLY_CONTRACT_DIGEST) == 64
    assert f"--field-manager={MANIFEST_FIELD_MANAGER}" in final
    assert "--server-side=true" in final
    assert "--validate=strict" in final
    assert "--force-conflicts" not in final
    assert "--dry-run=server" not in final
    assert (
        tuple(
            item
            for item in preflight
            if item
            not in {
                "--kubeconfig",
                "/var/lib/loom-staging-rollout/kubeconfig",
                "--dry-run=server",
            }
        )
        == final
    )


def test_diff_and_input_validation_preserve_no_force_boundary() -> None:
    diff = server_side_diff_argv("loom-staging")
    assert f"--field-manager={MANIFEST_FIELD_MANAGER}" in diff
    assert "--server-side=true" in diff
    assert "--force-conflicts" not in diff

    with pytest.raises(ValueError, match="namespace"):
        server_side_apply_argv("../loom-staging")
    with pytest.raises(ValueError, match="kubeconfig"):
        server_side_apply_argv("loom-staging", kubeconfig=Path("relative/config"))
