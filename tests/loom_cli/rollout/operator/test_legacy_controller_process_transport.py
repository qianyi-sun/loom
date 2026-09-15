"""Dedicated process observation must not change legacy canonical wire records."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import gb10_external_supervisor_broker as broker

from loom_cli.rollout.operator import protected_gb10_external_supervisor_transport as remote
from loom_cli.rollout.operator import protected_legacy_controller_process as process
from loom_cli.rollout.operator.protected_application_admission_recovery import (
    admission_record_digest,
)
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    FixedExternalSupervisorTransport,
)
from tests.loom_cli.rollout.operator.test_legacy_controller_process import _properties
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_transition import (
    _artifact,
    _target,
)


def test_process_observation_uses_its_own_read_only_broker_operation(tmp_path):
    artifact = _artifact(tmp_path, execution_host="gx10-01c7", include_builder=True)
    wire = remote._encode_helper_request(operation="observe_processes",
        candidate_sha=artifact.candidate_sha, candidate_tree=artifact.candidate_tree,
        artifact=artifact)
    request = broker._parse_request(wire.encode())
    assert request["operation"] == "observe_processes"
    assert request["predecessor_authority"] is None
    assert set(request) == {"schema_version", "candidate_sha", "candidate_tree",
                            "operation", "artifact", "predecessor_authority"}
    assert json.loads(wire)["artifact"] == json.loads(artifact.to_bytes())


@pytest.mark.parametrize("drift", [None, "candidate", "unit", "active", "digest"])
def test_process_probe_composes_fixed_transport_helper_and_canonical_source(tmp_path, monkeypatch, drift):
    artifact = _artifact(tmp_path, execution_host="gx10-01c7", include_builder=True)
    unit_dir = Path("/var/lib/loom-rollout/.config/systemd/user")
    canonical = _target(artifact, unit_dir=str(unit_dir))
    unit = "loom-autoscaler-gb10-staging.service"
    properties = {**_properties(), "Id": unit, "FragmentPath": str(unit_dir / unit)}
    record = {"schema_version": 1, "pool": "gb10", "boot_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "unit_sha256": artifact.unit_sha256[unit], "properties": properties, "processes_retired": True}
    record["evidence_sha256"] = admission_record_digest(record)

    def probe(**kwargs):
        assert kwargs["pool"] == "gb10"
        assert kwargs["expected_unit_sha256"] == artifact.unit_sha256[unit]
        return record

    monkeypatch.setattr(process, "observe_legacy_controller_processes", probe)
    store = SimpleNamespace(read_canonical=lambda: canonical, compensation_blockers=lambda: {},
                            read_unit=lambda name: canonical.unit_payloads[name].encode())
    local = FixedExternalSupervisorTransport(store, SimpleNamespace(environment={}), unit_dir=unit_dir)
    commands = []

    def run(argv, payload):
        commands.append(argv)
        assert broker._parse_request(payload.encode())["operation"] == "observe_processes"
        response = json.loads(remote._handle_helper_request(payload, transport=local))
        evidence = response["process_observation"]
        inner = evidence["process_evidence"]
        if drift == "candidate":
            evidence["candidate_sha"] = "f" * 40
        elif drift == "unit":
            inner["unit_sha256"] = "f" * 64
        elif drift == "active":
            inner["properties"]["MainPID"] = "123"
        if drift is not None:
            inner["evidence_sha256"] = admission_record_digest({k: v for k, v in inner.items() if k != "evidence_sha256"})
            evidence["evidence_sha256"] = "f" * 64 if drift == "digest" else admission_record_digest(
                {k: v for k, v in evidence.items() if k != "evidence_sha256"})
        return SimpleNamespace(returncode=0, stdout=remote._canonical_json(response), stderr="")

    transport = remote.FixedGB10ExternalSupervisorTransport(artifact.candidate_sha,
        artifact.candidate_tree, tmp_path / "identity", run)
    if drift is None:
        evidence = transport.observe_processes(artifact)
        assert evidence["canonical_digest"] == canonical.evidence_digest
        assert evidence["process_evidence"]["processes_retired"] is True
    else:
        with pytest.raises(ValueError):
            transport.observe_processes(artifact)
    assert len(commands) == 1
    assert commands[0][-1] == "loom-external-supervisor-v1"
