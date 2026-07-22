"""PreflightStep backup traversal policy contract."""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s08_preflight import PreflightStep


def test_passes_broker_bound_backup_traversal_limits(tmp_path: Path) -> None:
    ctx = make_ctx(
        tmp_path,
        request_id="req-123",
        backup_manifest_max_files=1_000_004,
        backup_manifest_max_entries=16_000_000,
        backup_manifest_max_total_bytes=16 * 1024**4,
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(8, "preflight")

    argv = list(PreflightStep().argv(ctx, step_dir))

    assert argv[argv.index("--backup-max-files") + 1] == "1000004"
    assert argv[argv.index("--backup-max-entries") + 1] == "16000000"
    assert argv[argv.index("--backup-max-total-bytes") + 1] == str(16 * 1024**4)
