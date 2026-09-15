"""Dedicated process observation must not change legacy canonical wire records."""

import json

from loom_cli.rollout.operator import protected_gb10_external_supervisor_transport as remote
from scripts.ops import gb10_external_supervisor_broker as broker
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_transition import _artifact


def test_process_observation_uses_its_own_read_only_broker_operation(tmp_path):
    artifact = _artifact(tmp_path, execution_host="gx10-01c7")
    wire = remote._encode_helper_request(operation="observe_processes",
        candidate_sha=artifact.candidate_sha, candidate_tree=artifact.candidate_tree,
        artifact=artifact)
    request = broker._parse_request(wire.encode())
    assert request["operation"] == "observe_processes"
    assert request["predecessor_authority"] is None
    assert set(request) == {"schema_version", "candidate_sha", "candidate_tree",
                            "operation", "artifact", "predecessor_authority"}
    assert json.loads(wire)["artifact"] == json.loads(artifact.to_bytes())
