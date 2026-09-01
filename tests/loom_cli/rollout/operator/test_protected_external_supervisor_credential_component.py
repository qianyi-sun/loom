from __future__ import annotations

import os
from pathlib import Path

from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_external_supervisor_credential_component import (
    ProtectedExternalSupervisorCredentialComponent,
)
from loom_cli.rollout.operator.protected_external_supervisor_credential_transport import (
    ExternalSupervisorCredentialEvidence,
)
from tests.loom_cli.rollout.operator.test_protected_apply_executor import _plan


def _evidence(host: str) -> ExternalSupervisorCredentialEvidence:
    return ExternalSupervisorCredentialEvidence(
        execution_host=host,
        kubeconfig_sha256="d" * 64,
        uid=os.geteuid(),
        gid=os.getegid(),
        mode=0o600,
        size=4096,
        database_secret_readable=True,
        witness_config_map_readable=True,
        pods_exec_denied=True,
    )


class _Transport:
    def __init__(self, host: str, *, exact: bool, fail: bool = False) -> None:
        self.host = host
        self.exact = exact
        self.fail = fail
        self.calls: list[str] = []

    def observe(self):
        self.calls.append("observe")
        if self.fail:
            raise ValueError("unsafe credential")
        return _evidence(self.host) if self.exact else None

    def publish(self):
        self.calls.append("publish")
        self.exact = True
        return _evidence(self.host)


def _epoch(plan) -> ComponentObservation:
    return ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )


def test_absent_credential_is_repairable_and_converges_before_exact(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    transport = _Transport("gx10-01c7", exact=False)
    component = ProtectedExternalSupervisorCredentialComponent(
        transport=transport,
        epoch_guard=_epoch,
        execution_host="gx10-01c7",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )

    protected = component.component(plan)
    assert protected.component_id == "external-supervisor-credential-gb10"
    assert protected.classify(plan).state is ComponentState.READY
    protected.apply(plan)
    assert protected.classify(plan).state is ComponentState.EXACT
    assert transport.calls == ["observe", "observe", "publish", "observe"]


def test_unsafe_or_wrong_controller_credential_is_drift(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    unsafe = ProtectedExternalSupervisorCredentialComponent(
        transport=_Transport("TRT-EAI-OLDLAB-1", exact=False, fail=True),
        epoch_guard=_epoch,
        execution_host="TRT-EAI-OLDLAB-1",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )
    wrong = ProtectedExternalSupervisorCredentialComponent(
        transport=_Transport("gx10-01c7", exact=True),
        epoch_guard=_epoch,
        execution_host="TRT-EAI-OLDLAB-1",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )

    assert unsafe.classify(plan).state is ComponentState.DRIFTED
    assert wrong.classify(plan).state is ComponentState.DRIFTED
