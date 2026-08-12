from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_cli.rollout.operator.protected_gb10_external_supervisor_transport as remote
from loom_cli.rollout.external_supervisor_predecessor import (
    ExternalSupervisorPredecessorAuthority,
    load_predecessor_manifest,
)
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    ExternalSupervisorLiveObservation,
    FixedUserSystemdControl,
    ServiceRuntimeStatus,
    TimerRuntimeStatus,
)
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_transition import (
    _artifact,
)


def _authority() -> ExternalSupervisorPredecessorAuthority:
    predecessor = load_predecessor_manifest(execution_host="gx10-01c7")
    return ExternalSupervisorPredecessorAuthority(
        kind="legacy-manifest",
        authority_digest=predecessor.manifest_digest,
        unit_sha256=predecessor.unit_sha256,
    )


def _observation(tmp_path: Path) -> tuple[object, ExternalSupervisorLiveObservation]:
    artifact = _artifact(tmp_path, execution_host="gx10-01c7")
    supervisor = artifact.supervisors[0]
    unit_dir = remote.GB10_CONTROLLER_UNIT_DIR
    observation = ExternalSupervisorLiveObservation(
        unit_payloads={
            supervisor.service_name: supervisor.service_unit.encode(),
            supervisor.timer_name: supervisor.timer_unit.encode(),
        },
        timer_statuses={
            supervisor.timer_name: TimerRuntimeStatus(
                load_state="loaded",
                unit_file_state="enabled",
                active_state="active",
                fragment_path=str(unit_dir / supervisor.timer_name),
                need_daemon_reload="no",
            )
        },
        service_statuses={
            supervisor.service_name: ServiceRuntimeStatus(
                load_state="loaded",
                result="success",
                exec_main_status=0,
                fragment_path=str(unit_dir / supervisor.service_name),
                need_daemon_reload="no",
            )
        },
        predecessor_authority=_authority(),
    )
    return artifact, observation


class _Run:
    def __init__(self, response: str) -> None:
        self.response = response
        self.calls: list[tuple[tuple[str, ...], str]] = []

    def __call__(self, argv, input_payload):
        self.calls.append((tuple(argv), input_payload))
        return SimpleNamespace(returncode=0, stdout=self.response, stderr="")


def _transport(artifact, run: _Run, identity: Path) -> remote.FixedGB10ExternalSupervisorTransport:
    identity.write_text("dedicated-private-key", encoding="ascii")
    identity.chmod(0o600)
    return remote.FixedGB10ExternalSupervisorTransport(
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        identity=identity,
        run=run,
    )


def test_gb10_user_systemd_control_uses_controller_service_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "geteuid", lambda: 995)

    control = FixedUserSystemdControl(
        service_uid=995,
        service_home=remote.GB10_CONTROLLER_HOME,
    )

    assert control.environment == {
        "HOME": "/var/lib/loom-rollout",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "XDG_CONFIG_HOME": "/var/lib/loom-rollout/.config",
        "XDG_RUNTIME_DIR": "/run/user/995",
    }


def test_remote_observe_round_trips_typed_evidence_and_forces_exact_ssh_argv(
    tmp_path: Path,
) -> None:
    artifact, observation = _observation(tmp_path)
    run = _Run(remote._encode_helper_response("observe", observation=observation))
    identity = tmp_path / "controller-ed25519"
    transport = _transport(artifact, run, identity)

    assert transport.observe(artifact, _authority()) == observation

    assert run.calls[0][0] == (
        "ssh",
        "-F",
        "/dev/null",
        "-i",
        str(identity),
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "UpdateHostKeys=no",
        "-p",
        "2221",
        "-l",
        "qianyi",
        "207.35.188.227",
        "loom-external-supervisor-v1",
    )
    request = json.loads(run.calls[0][1])
    assert request["operation"] == "observe"
    assert request["candidate_sha"] == artifact.candidate_sha
    assert request["candidate_tree"] == artifact.candidate_tree
    assert request["predecessor_authority"] == _authority().to_dict()


def test_remote_apply_and_reconcile_expose_only_typed_operations(tmp_path: Path) -> None:
    artifact, observation = _observation(tmp_path)
    run = _Run(remote._encode_helper_response("apply"))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    transport.apply(
        artifact,
        observation,
        plan_digest="a" * 64,
        attestation_digest="b" * 64,
        transition_digest="c" * 64,
    )

    apply_request = json.loads(run.calls[0][1])
    assert apply_request["operation"] == "apply"
    assert apply_request["expected"]["predecessor_authority"] == _authority().to_dict()
    assert apply_request["plan_digest"] == "a" * 64
    assert apply_request["attestation_digest"] == "b" * 64
    assert apply_request["transition_digest"] == "c" * 64

    run.response = remote._encode_helper_response("reconcile_compensations")
    transport.reconcile_compensations()
    assert json.loads(run.calls[1][1]) == {
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "operation": "reconcile_compensations",
        "schema_version": 1,
    }


def test_remote_transport_rejects_non_controller_artifact_before_ssh(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, execution_host="TRT-EAI-OLDLAB-1")
    run = _Run(remote._encode_helper_response("reconcile_compensations"))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    with pytest.raises(ValueError, match="controller artifact"):
        transport.observe(artifact)

    assert run.calls == []


def test_candidate_helper_dispatches_existing_local_transport_without_reimplementation(
    tmp_path: Path,
) -> None:
    artifact, observation = _observation(tmp_path)

    class _LocalTransport:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def observe(self, actual_artifact, authority=None):
            self.calls.append(("observe", actual_artifact, authority))
            return observation

        def apply(self, actual_artifact, expected, **digests):
            self.calls.append(("apply", actual_artifact, expected, digests))

        def reconcile_compensations(self):
            self.calls.append(("reconcile_compensations",))

    local = _LocalTransport()
    request = remote._encode_helper_request(
        operation="observe",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        artifact=artifact,
        predecessor_authority=_authority(),
    )

    response = remote._handle_helper_request(request, transport=local)

    assert remote._decode_helper_observation(response) == observation
    assert local.calls == [("observe", artifact, _authority())]
