from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_RUNBOOK = _ROOT / "docs" / "full-max-slot-canary-runbook.md"


def test_full_max_slot_runbook_defaults_to_external_worker_pools() -> None:
    """#383: staging/prod external-worker profiles must not require k8s-worker."""
    text = _RUNBOOK.read_text(encoding="utf-8")

    assert "--required-worker-pool oldlab" in text
    assert "--required-worker-pool gb10-arm64" in text
    assert "--required-worker-pool k8s-worker" not in text
    assert '"oldlab","k8s-worker","gb10-arm64"' not in text
