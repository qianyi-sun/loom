"""Machine-readable Package 2C mutation inventory policy."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from loom_capacity_agent.legacy_fence import (
    LEGACY_MUTATION_INVENTORY_DIGEST,
    LEGACY_MUTATION_PATH_IDS,
)

INVENTORY = Path("config/capacity-mutation-path-inventory.json")

EXECUTION_ATTEMPT_MUTATION_SOURCE_FLOOR = {
    "src/loom_control_plane/routes/execution_attempts.py:heartbeat_attempt",
    "src/loom_control_plane/routes/execution_attempts.py:report_attempt_started",
    "src/loom_control_plane/routes/execution_attempts.py:_terminal_report",
    "src/loom_control_plane/routes/execution_attempts.py:report_attempt_complete",
    "src/loom_control_plane/routes/execution_attempts.py:report_worker_lost_cleanup",
    "src/loom_control_plane/scheduler/claim.py:claim_one",
    "src/loom_pipeline_orchestrator/repository.py:create_attempt",
    "src/loom_pipeline_orchestrator/repository.py:schedule_retry",
    "src/loom_pipeline_orchestrator/repository.py:_fail_provider_attempt_budget",
    "src/loom_pipeline_orchestrator/repository.py:_fail_accounting_violation",
    "src/loom_pipeline_orchestrator/repository.py:_latch_terminal_cause",
    "src/loom_pipeline_orchestrator/repository.py:_cancel_not_started_attempts",
    "src/loom_pipeline_orchestrator/repository.py:acknowledge_cancellation",
}


def test_capacity_mutation_inventory_is_complete_and_activation_blocking() -> None:
    inventory_bytes = INVENTORY.read_bytes()
    document = json.loads(inventory_bytes)
    assert document["schema_version"] == 2
    assert document["activation_blocking"] is True
    entries = document["entries"]
    assert isinstance(entries, list)
    assert len(entries) >= 27
    identities = [entry["id"] for entry in entries]
    assert len(identities) == len(set(identities))
    assert {
        "trial-submission",
        "neutral-pool-assignment",
        "queued-to-claimed",
        "worker-result-state",
        "single-trial-cancel",
        "dead-worker-reclaim",
        "worker-drain-and-release",
        "slurm-job-launch-registry-release",
        "dev-environment-destroy",
        "legacy-compatibility-writer",
        "pipeline-attempt-submission",
        "execution-attempt-queued-to-claimed",
        "execution-attempt-heartbeat",
        "execution-attempt-result-state",
        "pipeline-attempt-cancellation",
        "execution-attempt-worker-loss",
        "pipeline-attempt-retry",
    } <= set(identities)
    for entry in entries:
        assert entry["closure_status"] == "open"
        assert entry["category"]
        assert entry["current_mutation"]
        assert entry["current_authority"]
        assert entry["required_replacement"]
        sources = entry["sources"]
        assert isinstance(sources, list) and sources
        for source in sources:
            path_text, separator, symbol = source.partition(":")
            path = Path(path_text)
            assert path.is_file(), source
            if separator:
                assert symbol.rsplit(".", 1)[-1] in path.read_text(encoding="utf-8"), source

    assert tuple(sorted(identities)) == LEGACY_MUTATION_PATH_IDS
    assert hashlib.sha256(inventory_bytes).hexdigest() == LEGACY_MUTATION_INVENTORY_DIGEST


def test_unified_execution_attempt_capacity_mutations_cannot_fall_out_of_inventory() -> None:
    document = json.loads(INVENTORY.read_bytes())
    inventoried_sources = {source for entry in document["entries"] for source in entry["sources"]}
    assert EXECUTION_ATTEMPT_MUTATION_SOURCE_FLOOR <= inventoried_sources
