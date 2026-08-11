"""Machine-readable Package 2C mutation inventory policy."""

from __future__ import annotations

import json
from pathlib import Path

INVENTORY = Path("docs/architecture/capacity-mutation-path-inventory.json")


def test_capacity_mutation_inventory_is_complete_and_activation_blocking() -> None:
    document = json.loads(INVENTORY.read_text(encoding="utf-8"))
    assert document["schema_version"] == 1
    assert document["activation_blocking"] is True
    entries = document["entries"]
    assert isinstance(entries, list)
    assert len(entries) >= 19
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
