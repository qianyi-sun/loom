from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from loom_cli.rollout.operator.protected_controller_discovery import (
    ControllerDiscoveryEvidence,
    ControllerDiscoveryRequest,
)
from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
    controller_local_authority_sha256,
)


def _request(*, pool_id: str = "gb10") -> ControllerDiscoveryRequest:
    return ControllerDiscoveryRequest(
        schema_version=1,
        pool_id=pool_id,
        transport_authority_sha256="1" * 64,
    )


def _evidence(*, pool_id: str = "gb10") -> ControllerDiscoveryEvidence:
    if pool_id == "gb10":
        architecture = "arm64"
        hostname = "gx10-01c7"
        cluster = "trt-gb10"
        nodes = tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16)))
        manager_client_cidr = "192.168.60.11/32"
    else:
        architecture = "amd64"
        hostname = "TRT-EAI-OLDLAB-1"
        cluster = "trt-oldlab"
        nodes = tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6))
        manager_client_cidr = "192.168.50.103/32"
    executables = {
        name: hashlib.sha256(name.encode("ascii")).hexdigest()
        for name in ("sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue")
    }
    configuration = {"slurm.conf": hashlib.sha256(b"slurm.conf").hexdigest()}
    job_visibility = hashlib.sha256(f"{pool_id}:scheduler-admission".encode("ascii")).hexdigest()
    local_authority = controller_local_authority_sha256(
        pool_id=pool_id,
        architecture=architecture,
        controller_hostname=hostname,
        service_uid=991,
        service_gid=992,
        slurm_cluster=cluster,
        partition="loom-staging",
        target_nodes=nodes,
        executable_sha256=executables,
        configuration_sha256=configuration,
        job_visibility_evidence_sha256=job_visibility,
    )
    return ControllerDiscoveryEvidence(
        schema_version=1,
        pool_id=pool_id,
        transport_authority_sha256="1" * 64,
        controller_hostname=hostname,
        architecture=architecture,
        service_user="loom_capacity_executor",
        service_uid=991,
        service_gid=992,
        slurm_cluster=cluster,
        partition="loom-staging",
        target_nodes=nodes,
        slurm_version=(23, 11, 4),
        data_parser="data_parser/v0.0.40",
        query_principal="loom_capacity_executor",
        manager_client_cidr=manager_client_cidr,
        executable_sha256=executables,
        configuration_sha256=configuration,
        job_visibility_evidence_sha256=job_visibility,
        local_authority_sha256=local_authority,
    )


def test_discovery_contracts_are_canonical_secret_free_and_pool_bound() -> None:
    """Catch accepting replayable, malleable, or secret-bearing discovery evidence."""
    request = _request()
    evidence = _evidence()

    assert ControllerDiscoveryRequest.from_bytes(request.to_bytes()) == request
    assert ControllerDiscoveryEvidence.from_bytes(evidence.to_bytes()) == evidence
    assert evidence.evidence_sha256 == hashlib.sha256(evidence.to_bytes()).hexdigest()
    assert b"token" not in evidence.to_bytes().lower()
    assert b"private" not in evidence.to_bytes().lower()

    payload = json.loads(evidence.to_bytes())
    payload["controller_hostname"] = "TRT-EAI-OLDLAB-1"
    with pytest.raises(ValueError, match="controller discovery evidence identity"):
        ControllerDiscoveryEvidence.from_bytes(
            (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )


def test_discovery_contracts_reject_noncanonical_duplicate_and_unknown_fields() -> None:
    """Catch JSON ambiguity or ignored fields at the controller trust boundary."""
    request = _request()
    evidence = _evidence()

    with pytest.raises(ValueError, match="not canonical"):
        ControllerDiscoveryRequest.from_bytes(
            request.to_bytes().replace(b'"pool_id"', b' "pool_id"')
        )
    with pytest.raises(ValueError, match="duplicate"):
        ControllerDiscoveryRequest.from_bytes(
            request.to_bytes().replace(b"{", b'{"pool_id":"gb10",', 1)
        )
    value = json.loads(evidence.to_bytes())
    value["unexpected"] = "2" * 64
    with pytest.raises(ValueError, match="fields"):
        ControllerDiscoveryEvidence.from_bytes(
            (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )


def test_discovery_contracts_reject_boolean_schema_versions() -> None:
    """Catch JSON booleans passing Python's integer equality at the trust boundary."""
    request = json.loads(_request().to_bytes())
    request["schema_version"] = True
    evidence = json.loads(_evidence().to_bytes())
    evidence["schema_version"] = True

    with pytest.raises(ValueError, match="controller discovery"):
        ControllerDiscoveryRequest.from_bytes(
            (json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )
    with pytest.raises(ValueError, match="controller discovery"):
        ControllerDiscoveryEvidence.from_bytes(
            (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )


@pytest.mark.parametrize(
    "changed",
    (
        {"manager_client_cidr": "192.168.60.0/24"},
        {"manager_client_cidr": "8.8.8.8/32"},
        {"service_uid": 0},
        {"slurm_version": (24, 5, 0)},
        {"target_nodes": tuple(f"trt-gb10-{index}" for index in range(1, 16))},
        {"local_authority_sha256": "0" * 64},
        {"job_visibility_evidence_sha256": "0" * 64},
    ),
)
def test_discovery_evidence_rejects_incomplete_or_invented_controller_authority(
    changed: dict[str, object],
) -> None:
    """Catch widening a host route or inventing a controller-local binding."""
    with pytest.raises(ValueError, match="controller discovery"):
        replace(_evidence(), **changed)


def test_discovery_contracts_cover_oldlab_without_oldlab2() -> None:
    """Catch accidentally broadening the OLDLAB target inventory."""
    evidence = _evidence(pool_id="oldlab")

    assert ControllerDiscoveryEvidence.from_bytes(evidence.to_bytes()) == evidence
    assert evidence.target_nodes == (
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5",
    )
    assert b"oldlab-2" not in evidence.to_bytes().lower()
