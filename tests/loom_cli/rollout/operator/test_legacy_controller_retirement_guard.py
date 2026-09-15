"""Activation must observe exact disabled timers and empty legacy cgroups."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.external_supervisor_predecessor import external_supervisor_unit_directory
from loom_cli.rollout.operator.protected_application_admission_recovery import admission_record_digest
from loom_cli.rollout.operator.protected_external_supervisor_component import ProtectedExternalSupervisorComponent
from tests.loom_cli.rollout.operator.test_legacy_controller_process import _properties
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_component import (
    _bound_multi_artifacts,
    _build_active_artifact,
    _epoch,
    _observation,
)


@pytest.mark.parametrize("host", ["gx10-01c7", "TRT-EAI-OLDLAB-1"])
@pytest.mark.parametrize("drift", [None, "desired-active", "timer", "process", "canonical", "late-timer"])
def test_retirement_guard_brackets_real_process_evidence_with_exact_runtime(tmp_path, host, drift):
    plan, root, artifacts = _bound_multi_artifacts(tmp_path, retired=drift != "desired-active")
    artifact = artifacts[host]
    unit_dir = Path(external_supervisor_unit_directory(host))
    pool = "gb10" if host == "gx10-01c7" else "oldlab"
    trial = next(item for item in artifact.supervisors if item.pool_name == pool)
    live = _observation(artifact, files="exact", runtime="exact", unit_dir=unit_dir,
        plan_digest=plan.plan_digest, attestation_digest=plan.attestation_digest)
    timers = dict(live.timer_statuses)
    timers[trial.timer_name] = replace(timers[trial.timer_name], unit_file_state="disabled", active_state="inactive")
    live = replace(live, timer_statuses=timers)
    calls = []

    def observe(_artifact):
        assert _artifact == artifact
        calls.append("runtime")
        if drift == "timer" or (drift == "late-timer" and calls.count("runtime") > 1):
            return replace(live, timer_statuses={**timers,
                trial.timer_name: replace(timers[trial.timer_name], active_state="active", unit_file_state="enabled")})
        return live

    def processes(_artifact):
        assert _artifact == artifact
        calls.append("process")
        properties = {**_properties(), "Id": trial.service_name, "FragmentPath": str(unit_dir / trial.service_name)}
        if drift == "process":
            properties["MainPID"] = "42"
        inner = {"schema_version": 1, "pool": pool, "boot_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "unit_sha256": artifact.unit_sha256[trial.service_name], "properties": properties,
            "processes_retired": drift != "process"}
        inner["evidence_sha256"] = admission_record_digest(inner)
        envelope = {"schema_version": 1, "candidate_sha": artifact.candidate_sha,
            "candidate_tree": artifact.candidate_tree, "artifact_digest": artifact.artifact_digest,
            "canonical_digest": "d" * 64 if drift == "canonical" else live.canonical_identity.evidence_digest,
            "process_evidence": inner}
        return {**envelope, "evidence_sha256": admission_record_digest(envelope)}

    component = ProtectedExternalSupervisorComponent(root,
        SimpleNamespace(observe=observe, observe_processes=processes), _epoch,
        execution_host=host, unit_dir=unit_dir, artifact_builder=_build_active_artifact)
    if drift is not None:
        with pytest.raises((RuntimeError, ValueError), match="legacy controller"):
            component.observe_retirement(plan)
    else:
        evidence = component.observe_retirement(plan)
        assert evidence["plan_digest"] == plan.plan_digest
        assert evidence["pool"] == pool
        assert evidence["process_evidence"]["process_evidence"]["processes_retired"] is True
        assert calls == ["runtime", "process", "runtime"]
        assert component.observe_retirement(plan) == evidence
