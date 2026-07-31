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
    # #49/#1109: architecture coverage uses two operator-only canaries, not
    # additional trials or pool selectors in the normal user eval batch.
    assert "loom admin batches submit-on-behalf" in text
    assert "Separate operator architecture canaries" in text
    assert "export X86_COVERAGE_BATCH_ID=" in text
    assert "export ARM_COVERAGE_BATCH_ID=" in text
    assert text.count("$DUAL_ARCH_CANARY_TASK_ID") == 2
    assert "Stop if `required_worker_pools` is not `[]`" in text
    assert "--batch-id \"$X86_COVERAGE_BATCH_ID\"" in text
    assert "--batch-id \"$ARM_COVERAGE_BATCH_ID\"" in text
    assert (
        "--storage-preflight-evidence \"$CANARY_DIR/01-clean-anchor/"
        "minio-storage-preflight-$IMAGE_TAG.json\" \\\n"
        "  --required-worker-pool oldlab"
    ) not in text
