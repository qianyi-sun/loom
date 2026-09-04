from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from loom_capacity_pool_executor.config import SlurmInventoryNodeDocument
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
    ControllerDirectoryEvidence,
    ControllerPrerequisiteEvidence,
    ControllerPrerequisiteRequest,
    KubernetesProtectedControllerPrerequisiteComponent,
)
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisiteStore,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _artifacts,
    _attestation,
    _baseline,
    _envelope,
    _lease,
    _predecessor_evidence,
    _systemd_evidence,
)

_EXECUTABLES = {
    "sacct": "/usr/bin/sacct",
    "sacctmgr": "/usr/bin/sacctmgr",
    "sbatch": "/usr/bin/sbatch",
    "scancel": "/usr/bin/scancel",
    "scontrol": "/usr/bin/scontrol",
    "squeue": "/usr/bin/squeue",
}
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_TARGETS = {
    "gb10": tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
}
_HOSTS = {"gb10": "gx10-01c7", "oldlab": "TRT-EAI-OLDLAB-1"}
_CLUSTERS = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}
_ARCHITECTURES = {"gb10": "arm64", "oldlab": "amd64"}


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _local_authority(
    *,
    pool_id: str,
    uid: int,
    gid: int,
    executable_sha256: dict[str, str],
    configuration_sha256: dict[str, str],
    job_visibility_evidence_sha256: str,
) -> str:
    return _canonical_digest(
        {
            "architecture": _ARCHITECTURES[pool_id],
            "configuration_sha256": configuration_sha256,
            "controller_hostname": _HOSTS[pool_id],
            "executable_sha256": executable_sha256,
            "job_visibility_evidence_sha256": job_visibility_evidence_sha256,
            "partition": "loom-staging",
            "pool_id": pool_id,
            "service_gid": gid,
            "service_uid": uid,
            "slurm_cluster": _CLUSTERS[pool_id],
            "target_nodes": list(_TARGETS[pool_id]),
        }
    )


def _plan_and_artifact(tmp_path: Path):  # type: ignore[no-untyped-def]
    artifacts = _artifacts(tmp_path)
    lease = _lease()
    original = execution_prerequisite_artifact(
        core_bundle_sha256=artifacts.bundle_digest,
        backup_lease_sha256=lease.evidence_digest,
    )
    executable_sha256 = {name: f"{index + 31:064x}" for index, name in enumerate(_EXECUTABLES)}
    configuration_sha256 = {"slurm.conf": "4" * 64}
    bindings = []
    for binding in original.executor_profile_seed.pools:
        pool_id = binding.pool_id
        uid = 2100 if pool_id == "gb10" else 2200
        gid = uid + 1
        exemplar = binding.inventory.nodes[0]
        nodes = tuple(
            SlurmInventoryNodeDocument(
                pool_id=pool_id,
                node_id=node,
                allocatable=exemplar.allocatable,
                features=(_ARCHITECTURES[pool_id],),
            )
            for node in _TARGETS[pool_id]
        )
        inventory = binding.inventory.model_copy(
            update={
                "controller_cluster": _CLUSTERS[pool_id],
                "nodes": tuple(sorted(nodes, key=lambda node: node.node_id)),
                "query_uid": uid,
                "relevant_partitions": ("loom-staging",),
                "scontrol_sha256": executable_sha256["scontrol"],
                "squeue_sha256": executable_sha256["squeue"],
                "slurm_conf_sha256": configuration_sha256["slurm.conf"],
            }
        )
        bindings.append(
            binding.model_copy(
                update={
                    "controller_host": _HOSTS[pool_id],
                    "inventory": inventory,
                    "local_authority_sha256": _local_authority(
                        pool_id=pool_id,
                        uid=uid,
                        gid=gid,
                        executable_sha256=executable_sha256,
                        configuration_sha256=configuration_sha256,
                        job_visibility_evidence_sha256=(inventory.job_visibility_evidence_sha256),
                    ),
                    "local_uid": uid,
                    "partition": "loom-staging",
                    "slurm_cluster": _CLUSTERS[pool_id],
                }
            )
        )
    seed = replace(original.executor_profile_seed, pools=tuple(bindings))
    policy = original.execution_policy.model_copy(
        update={
            "executors": tuple(
                executor.model_copy(
                    update={
                        "local_authority_sha256": next(
                            binding.local_authority_sha256
                            for binding in bindings
                            if binding.pool_id == executor.pool_id
                        )
                    }
                )
                for executor in original.execution_policy.executors
            )
        }
    )
    artifact = replace(
        original,
        source_configuration_epoch=lease.manager_configuration_epoch,
        source_configuration_sha256=lease.manager_configuration_digest,
        executor_profile_seed=seed,
        execution_policy=policy,
    )
    store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "execution-authority",
        service_uid=tmp_path.stat().st_uid,
    )
    publication = store.publish(artifact)
    attestation = _attestation(artifact, execution_prerequisite_path=publication.path)
    plan = FinalGatePlan.build(
        _envelope(attestation),
        attestation,
        artifacts,
        lease,
        _baseline(),
        _systemd_evidence(),
        _predecessor_evidence(),
        execution_prerequisite_publication=publication,
        execution_prerequisite_store=store,
    )
    return (
        plan,
        artifact,
        executable_sha256,
        configuration_sha256,
        {item.pool_id: item for item in bindings},
    )


def _evidence(
    *,
    plan: FinalGatePlan,
    artifact,  # type: ignore[no-untyped-def]
    binding,  # type: ignore[no-untyped-def]
    executable_sha256: dict[str, str],
    configuration_sha256: dict[str, str],
    transport_authority_sha256: str,
) -> ControllerPrerequisiteEvidence:
    pool_id = binding.pool_id
    gid = binding.local_uid + 1
    credential_metadata = {
        name: artifact.credential_metadata_sha256[name]
        for name in (f"pool-executor-{pool_id}", f"pool-ownership-{pool_id}")
    }
    prerequisite_input = {
        "binding": binding.model_dump(mode="json"),
        "credential_metadata_sha256": credential_metadata,
        "executor_image": artifact.executor_profile_seed.executor_image,
        "schema_version": 1,
        "service_user": "loom_capacity_executor",
        "source_sha": plan.candidate_sha,
    }
    release_root = (
        f"/opt/loom-capacity-executor-releases/{plan.candidate_sha}-"
        f"{_ARCHITECTURES[pool_id]}-{artifact.executor_profile_seed.executor_image.rsplit(':', 1)[1]}"
    )
    directories = {
        "/etc/loom-capacity-executor": ControllerDirectoryEvidence(
            path="/etc/loom-capacity-executor", mode=0o700, uid=binding.local_uid, gid=gid
        ),
        "/run/loom-capacity-executor": ControllerDirectoryEvidence(
            path="/run/loom-capacity-executor", mode=0o700, uid=binding.local_uid, gid=gid
        ),
        f"/run/loom-capacity-executor/{pool_id}": ControllerDirectoryEvidence(
            path=f"/run/loom-capacity-executor/{pool_id}",
            mode=0o700,
            uid=binding.local_uid,
            gid=gid,
        ),
        "/var/lib/loom-capacity-executor": ControllerDirectoryEvidence(
            path="/var/lib/loom-capacity-executor", mode=0o700, uid=binding.local_uid, gid=gid
        ),
        binding.state_directory: ControllerDirectoryEvidence(
            path=binding.state_directory, mode=0o700, uid=binding.local_uid, gid=gid
        ),
    }
    return ControllerPrerequisiteEvidence(
        schema_version=1,
        pool_id=pool_id,
        controller_hostname=_HOSTS[pool_id],
        transport_authority_sha256=transport_authority_sha256,
        image=artifact.executor_profile_seed.executor_image,
        source_sha=plan.candidate_sha,
        architecture=_ARCHITECTURES[pool_id],
        release_root=release_root,
        release_manifest_sha256="5" * 64,
        service_user="loom_capacity_executor",
        service_uid=binding.local_uid,
        service_gid=gid,
        slurm_cluster=_CLUSTERS[pool_id],
        partition="loom-staging",
        target_nodes=_TARGETS[pool_id],
        executable_sha256=executable_sha256,
        configuration_sha256=configuration_sha256,
        job_visibility_evidence_sha256=binding.inventory.job_visibility_evidence_sha256,
        directories=directories,
        unit_sha256={name: f"{index + 71:064x}" for index, name in enumerate(_UNITS)},
        unit_active_state={name: "inactive" for name in _UNITS},
        unit_file_state={
            name: "disabled" if name.endswith(".timer") else "static" for name in _UNITS
        },
        prerequisite_input_path=f"/etc/loom-capacity-executor/{pool_id}-prerequisite.json",
        prerequisite_input_sha256=hashlib.sha256(
            (json.dumps(prerequisite_input, sort_keys=True, separators=(",", ":")) + "\n").encode(
                "ascii"
            )
        ).hexdigest(),
        credential_metadata_sha256=credential_metadata,
        controller_authority_sha256=binding.controller_authority_sha256,
        local_authority_sha256=binding.local_authority_sha256,
    )


class _Transport:
    def __init__(self, evidence: ControllerPrerequisiteEvidence | None) -> None:
        self.authority_sha256 = "7" * 64
        self.evidence = evidence
        self.converge_evidence: ControllerPrerequisiteEvidence | None = None
        self.observe_calls = 0
        self.converge_calls = 0

    def observe(self, request):  # type: ignore[no-untyped-def]
        self.observe_calls += 1
        assert request.transport_authority_sha256 == self.authority_sha256
        return self.evidence

    def converge(self, request):  # type: ignore[no-untyped-def]
        self.converge_calls += 1
        assert request.transport_authority_sha256 == self.authority_sha256
        if self.converge_evidence is None:
            raise AssertionError("test must install exact evidence before convergence")
        self.evidence = self.converge_evidence
        return self.evidence


def _component(tmp_path: Path, *, pool_id: str, evidence_present: bool):  # type: ignore[no-untyped-def]
    plan, artifact, executable_sha256, configuration_sha256, bindings = _plan_and_artifact(tmp_path)
    binding = bindings[pool_id]
    transport = _Transport(None)
    evidence = _evidence(
        plan=plan,
        artifact=artifact,
        binding=binding,
        executable_sha256=executable_sha256,
        configuration_sha256=configuration_sha256,
        transport_authority_sha256=transport.authority_sha256,
    )
    transport.evidence = evidence if evidence_present else None
    component = KubernetesProtectedControllerPrerequisiteComponent(
        pool_id=pool_id,
        transport=transport,
        prerequisite_reader=lambda _plan: artifact,
    )
    return component, transport, plan, artifact, evidence


def test_request_accepts_the_authoritative_digest_pinned_staging_registry_image(
    tmp_path: Path,
) -> None:
    plan, artifact, _executables, _configuration, bindings = _plan_and_artifact(tmp_path)
    image = "192.168.50.13:5000/loom-capacity-executor@sha256:" + "8" * 64

    request = ControllerPrerequisiteRequest(
        pool_id="oldlab",
        source_sha=plan.candidate_sha,
        architecture="amd64",
        image=image,
        service_user=artifact.executor_profile_seed.service_user,
        binding=bindings["oldlab"],
        credential_metadata_sha256={
            name: artifact.credential_metadata_sha256[name]
            for name in ("pool-executor-oldlab", "pool-ownership-oldlab")
        },
        transport_authority_sha256="7" * 64,
    )

    assert request.image == image


@pytest.mark.parametrize("pool_id", ("oldlab", "gb10"))
def test_component_classifies_absent_installation_ready_and_exact_inert_installation_exact(
    tmp_path: Path,
    pool_id: str,
) -> None:
    """Catch treating absence as drift or accepting an incompletely verified installation."""
    component, transport, plan, _artifact, evidence = _component(
        tmp_path, pool_id=pool_id, evidence_present=False
    )

    assert component.classify(plan)[0] is ComponentState.READY
    transport.evidence = evidence
    assert component.classify(plan)[0] is ComponentState.EXACT


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("controller_hostname", "wrong-controller"),
        ("transport_authority_sha256", "8" * 64),
        ("architecture", "amd64"),
        ("image", "ghcr.io/qianyi-sun/loom-capacity-executor@sha256:" + "8" * 64),
        ("source_sha", "8" * 40),
        ("service_uid", 9999),
        ("slurm_cluster", "wrong-cluster"),
        ("partition", "wrong-partition"),
        ("target_nodes", ("trt-gb10-2",)),
        ("controller_authority_sha256", "8" * 64),
        ("local_authority_sha256", "8" * 64),
    ),
)
def test_component_rejects_controller_identity_or_authority_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    """Catch accepting a controller that is not the exact prerequisite binding."""
    component, transport, plan, _artifact, evidence = _component(
        tmp_path, pool_id="gb10", evidence_present=True
    )
    drifted = copy.copy(evidence)
    object.__setattr__(drifted, field, value)
    transport.evidence = drifted

    assert component.classify(plan)[0] is ComponentState.DRIFTED


@pytest.mark.parametrize("unit", _UNITS)
def test_component_rejects_an_active_service_or_enabled_timer(
    tmp_path: Path,
    unit: str,
) -> None:
    """Catch admitting any controller unit that can already execute work."""
    component, transport, plan, _artifact, evidence = _component(
        tmp_path, pool_id="oldlab", evidence_present=True
    )
    if unit.endswith(".timer"):
        states = dict(evidence.unit_file_state)
        states[unit] = "enabled"
        drifted = copy.copy(evidence)
        object.__setattr__(drifted, "unit_file_state", states)
        transport.evidence = drifted
    else:
        states = dict(evidence.unit_active_state)
        states[unit] = "active"
        drifted = copy.copy(evidence)
        object.__setattr__(drifted, "unit_active_state", states)
        transport.evidence = drifted

    assert component.classify(plan)[0] is ComponentState.DRIFTED


def test_component_rejects_executable_or_configuration_drift(tmp_path: Path) -> None:
    """Catch a local-authority digest that no longer describes controller bytes."""
    component, transport, plan, _artifact, evidence = _component(
        tmp_path, pool_id="gb10", evidence_present=True
    )
    executables = dict(evidence.executable_sha256)
    executables["scontrol"] = "8" * 64
    drifted = copy.copy(evidence)
    object.__setattr__(drifted, "executable_sha256", executables)
    transport.evidence = drifted

    assert component.classify(plan)[0] is ComponentState.DRIFTED


def test_component_converges_once_and_requires_exact_readback(tmp_path: Path) -> None:
    """Catch reporting success from a write response without an independent readback."""
    component, transport, plan, _artifact, evidence = _component(
        tmp_path, pool_id="oldlab", evidence_present=False
    )

    transport.converge_evidence = evidence
    component.apply(plan)

    assert transport.converge_calls == 1
    assert transport.observe_calls >= 2
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_component_rejects_prerequisite_rotation_before_convergence(tmp_path: Path) -> None:
    """Catch applying a controller request after its immutable source was replaced."""
    component, transport, plan, artifact, _evidence_value = _component(
        tmp_path, pool_id="gb10", evidence_present=False
    )
    reads = 0

    def rotating_reader(_plan):  # type: ignore[no-untyped-def]
        nonlocal reads
        reads += 1
        if reads >= 2:
            return replace(artifact, candidate_tree="9" * 40)
        return artifact

    component = replace(component, prerequisite_reader=rotating_reader)

    with pytest.raises(RuntimeError, match="source changed before mutation"):
        component.apply(plan)

    assert transport.converge_calls == 0


def test_evidence_is_canonical_and_contains_no_secret_payload_fields(tmp_path: Path) -> None:
    """Catch secret material or nondeterministic structure entering controller evidence."""
    _component_value, _transport, _plan_value, _artifact_value, evidence = _component(
        tmp_path, pool_id="gb10", evidence_present=True
    )

    encoded = evidence.to_bytes()

    assert ControllerPrerequisiteEvidence.from_bytes(encoded) == evidence
    assert encoded.endswith(b"\n")
    assert all(
        marker not in encoded.lower()
        for marker in (
            b'"bearer-token":',
            b'"private-key":',
            b'"password":',
            b'"certificate.pem":',
        )
    )


def test_request_is_canonical_secret_free_and_strict(tmp_path: Path) -> None:
    """Catch a transport request that cannot be validated independently on-controller."""
    component, _transport, plan, artifact, _evidence_value = _component(
        tmp_path, pool_id="gb10", evidence_present=False
    )
    request = component._request(plan, artifact)

    encoded = request.to_bytes()

    assert ControllerPrerequisiteRequest.from_bytes(encoded) == request
    assert encoded.endswith(b"\n")
    assert all(
        marker not in encoded.lower()
        for marker in (
            b'"bearer-token":',
            b'"private-key":',
            b'"password":',
            b'"certificate.pem":',
        )
    )
    value = json.loads(encoded)
    value["unexpected"] = True
    with pytest.raises(ValueError, match="request"):
        ControllerPrerequisiteRequest.from_bytes(
            (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )
