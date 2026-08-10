from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.pipeline.work_protocol import PipelineInputMaterializationEvidenceReportV1


def test_acceptance_input_evidence_is_exactly_five_ordered_descriptors() -> None:
    payload = {
        "schema_version": "loom.pipeline-input-materialization-evidence-report.v1",
        "execution_attempt_id": uuid4(),
        "worker_id": uuid4(),
        "lease_epoch": 3,
        "cache_expectation": "cold_after_eviction",
        "ordered_manifest_sha256s": [f"sha256:{str(index) * 64}" for index in range(5)],
        "manifest_open_count": 5,
        "file_open_count": 8,
        "file_bytes": 1024,
        "archive_extraction_count": 2,
        "cas_rename_count": 5,
        "input_view_sha256": "sha256:" + "a" * 64,
    }
    report = PipelineInputMaterializationEvidenceReportV1.model_validate(payload)
    assert len(report.ordered_manifest_sha256s) == 5
    with pytest.raises(ValidationError):
        PipelineInputMaterializationEvidenceReportV1.model_validate(
            {**payload, "ordered_manifest_sha256s": payload["ordered_manifest_sha256s"][:4]}
        )
