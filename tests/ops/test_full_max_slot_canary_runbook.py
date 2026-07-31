from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_RUNBOOK = _ROOT / "docs" / "runbooks" / "full-max-slot-canary-runbook.md"


def test_full_max_slot_runbook_defaults_to_external_worker_pools() -> None:
    """#383: staging/prod external-worker profiles must not require k8s-worker."""
    text = _RUNBOOK.read_text(encoding="utf-8")

    assert "--required-worker-pool oldlab" in text
    assert "--required-worker-pool gb10" in text
    assert "--required-worker-pool k8s-worker" not in text
    assert '"oldlab","k8s-worker","gb10"' not in text
    # #1109: coverage is on a separate admin canary, not user eval create.
    assert "loom admin batches submit-on-behalf" in text
    assert "Separate operator pool-coverage canary" in text
    assert (
        "--storage-preflight-evidence \"$CANARY_DIR/01-clean-anchor/"
        "minio-storage-preflight-$IMAGE_TAG.json\" \\\n"
        "  --required-worker-pool oldlab"
    ) not in text
