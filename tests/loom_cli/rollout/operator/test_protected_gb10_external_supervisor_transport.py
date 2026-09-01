from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import loom_cli.rollout.operator.protected_gb10_external_supervisor_transport as remote
from loom_cli.rollout.external_supervisor_predecessor import (
    ExternalSupervisorPredecessorAuthority,
    load_predecessor_manifest,
)
from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
    build_external_supervisor_artifact,
)
from loom_cli.rollout.operator.protected_external_supervisor_credential_transport import (
    ExternalSupervisorCredentialEvidence,
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
from tests.loom_cli.rollout.rehearsal_fixtures import active_staging_profile_text

NORMAL_GB10_WORKER_HOSTS = (
    "trt-gb10-1",
    "trt-gb10-3",
    "trt-gb10-4",
    "trt-gb10-5",
    "trt-gb10-6",
    "trt-gb10-7",
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)


def _controller_artifact(tmp_path: Path) -> ExternalSupervisorArtifact:
    candidate = tmp_path / "candidate"
    profile = candidate / "deploy/environment-state/staging.toml"
    worker_script = candidate / "scripts/ops/worker_pool_autoscaler_external_once.py"
    builder_script = candidate / "scripts/ops/task_image_builder_autoscaler_external_once.py"
    profile.parent.mkdir(parents=True)
    worker_script.parent.mkdir(parents=True)
    repository = Path(__file__).resolve().parents[4]
    profile.write_text(active_staging_profile_text(), encoding="utf-8")
    shutil.copyfile(
        repository / "scripts/ops/worker_pool_autoscaler_external_once.py",
        worker_script,
    )
    shutil.copyfile(
        repository / "scripts/ops/task_image_builder_autoscaler_external_once.py",
        builder_script,
    )
    profile.chmod(0o600)
    worker_script.chmod(0o700)
    builder_script.chmod(0o700)
    with patch(
        "loom_cli.environment_state.staging_gb10_external_activation_blockers",
        return_value=(),
    ):
        artifact = build_external_supervisor_artifact(
            candidate,
            candidate_sha="a" * 40,
            candidate_tree="b" * 40,
            image_tag="staging-aaaaaaa",
            execution_host="gx10-01c7",
        )
    assert [item.pool_name for item in artifact.supervisors] == [
        "gb10",
        "task-image-builder-gb10",
    ]
    return artifact


def _authority() -> ExternalSupervisorPredecessorAuthority:
    predecessor = load_predecessor_manifest(execution_host="gx10-01c7")
    return ExternalSupervisorPredecessorAuthority(
        kind="legacy-manifest",
        authority_digest=predecessor.manifest_digest,
        unit_sha256=predecessor.unit_sha256,
    )


def _observation(tmp_path: Path) -> tuple[object, ExternalSupervisorLiveObservation]:
    artifact = _controller_artifact(tmp_path)
    unit_dir = remote.GB10_CONTROLLER_UNIT_DIR
    observation = ExternalSupervisorLiveObservation(
        unit_payloads={
            unit_name: unit.encode()
            for supervisor in artifact.supervisors
            for unit_name, unit in (
                (supervisor.service_name, supervisor.service_unit),
                (supervisor.timer_name, supervisor.timer_unit),
            )
        },
        timer_statuses={
            supervisor.timer_name: TimerRuntimeStatus(
                load_state="loaded",
                unit_file_state="enabled",
                active_state="active",
                fragment_path=str(unit_dir / supervisor.timer_name),
                need_daemon_reload="no",
            )
            for supervisor in artifact.supervisors
        },
        service_statuses={
            supervisor.service_name: ServiceRuntimeStatus(
                load_state="loaded",
                result="success",
                exec_main_status=0,
                fragment_path=str(unit_dir / supervisor.service_name),
                need_daemon_reload="no",
            )
            for supervisor in artifact.supervisors
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


def _capacity_artifact(artifact: ExternalSupervisorArtifact) -> dict[str, object]:
    generated_at = datetime.now(UTC)
    return {
        "schema_version": 1,
        "kind": "loom_gb10_slurm_acceptance",
        "result": "pass",
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "profile_sha256": "c" * 64,
        "cluster_name": "trt-gb10",
        "controller_host": "gx10-01c7",
        "service_identity": {
            "user": "loom-rollout",
            "uid": 995,
            "gid": 2007,
            "account": "loom-staging",
            "qos": "loom-staging",
        },
        "nodes": list(NORMAL_GB10_WORKER_HOSTS),
        "node_count": 14,
        "probed_nodes": list(NORMAL_GB10_WORKER_HOSTS[1:]),
        "probed_node_count": 13,
        "deferred_busy_nodes": [NORMAL_GB10_WORKER_HOSTS[0]],
        "trial_cache_registry": {
            "ca_sha256": "539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3",
            "canary_digest": "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e",
            "repository": "192.168.50.103:5443/loom-trial-cache",
        },
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(minutes=15)).isoformat(),
    }


def _capacity_response(acceptance: dict[str, object]) -> str:
    return (
        json.dumps(
            {
                "acceptance": acceptance,
                "operation": "accept_capacity",
                "schema_version": 1,
                "status": "ok",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )


def _credential_evidence() -> ExternalSupervisorCredentialEvidence:
    return ExternalSupervisorCredentialEvidence(
        execution_host="gx10-01c7",
        kubeconfig_sha256="d" * 64,
        uid=995,
        gid=2007,
        mode=0o600,
        size=4096,
        database_secret_readable=True,
        witness_config_map_readable=True,
        pods_exec_denied=True,
    )


def test_remote_capacity_accepts_normal_nodes_without_exclusive_builder(
    tmp_path: Path,
) -> None:
    artifact, _ = _observation(tmp_path)
    acceptance = _capacity_artifact(artifact)
    acceptance.update(
        {
            "nodes": list(NORMAL_GB10_WORKER_HOSTS),
            "node_count": 14,
            "probed_nodes": list(NORMAL_GB10_WORKER_HOSTS),
            "probed_node_count": 14,
            "deferred_busy_nodes": [],
        }
    )
    run = _Run(_capacity_response(acceptance))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    evidence = transport.accept_capacity(
        profile_sha256="c" * 64,
        nodes=NORMAL_GB10_WORKER_HOSTS,
    )

    assert evidence.payload["nodes"] == list(NORMAL_GB10_WORKER_HOSTS)


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

    with pytest.raises(ValueError, match="observe request"):
        remote._encode_helper_request(
            operation="observe",
            candidate_sha=artifact.candidate_sha,
            candidate_tree=artifact.candidate_tree,
            artifact=artifact,
            profile_sha256="d" * 64,
        )
    with pytest.raises(ValueError, match="apply request"):
        remote._encode_helper_request(
            operation="apply",
            candidate_sha=artifact.candidate_sha,
            candidate_tree=artifact.candidate_tree,
            artifact=artifact,
            expected=observation,
            plan_digest="a" * 64,
            attestation_digest="b" * 64,
            transition_digest="c" * 64,
            nodes=NORMAL_GB10_WORKER_HOSTS,
        )


def test_remote_credential_operations_carry_only_non_secret_fixed_evidence(
    tmp_path: Path,
) -> None:
    artifact = _controller_artifact(tmp_path)
    evidence = _credential_evidence()
    run = _Run(
        remote._encode_helper_response(
            "observe_credential",
            credential=evidence,
        )
    )
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    assert transport.observe_credential() == evidence
    observe_request = json.loads(run.calls[0][1])
    assert observe_request == {
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "operation": "observe_credential",
        "schema_version": 1,
    }

    run.response = remote._encode_helper_response(
        "publish_credential",
        credential=evidence,
    )
    assert transport.publish_credential() == evidence
    publish_request = json.loads(run.calls[1][1])
    assert publish_request == {
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "operation": "publish_credential",
        "schema_version": 1,
    }
    wire = json.dumps([observe_request, publish_request, evidence.to_dict()], sort_keys=True)
    for forbidden in ("token", "certificate", "kubeconfig_bytes", "path", "command"):
        assert forbidden not in wire


def test_remote_capacity_acceptance_round_trips_candidate_bound_evidence(
    tmp_path: Path,
) -> None:
    artifact = _controller_artifact(tmp_path)
    acceptance = _capacity_artifact(artifact)
    run = _Run(_capacity_response(acceptance))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    evidence = transport.accept_capacity(
        profile_sha256="c" * 64,
        nodes=NORMAL_GB10_WORKER_HOSTS,
    )

    assert dict(evidence.payload) == acceptance
    request = json.loads(run.calls[0][1])
    assert request == {
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "nodes": list(NORMAL_GB10_WORKER_HOSTS),
        "operation": "accept_capacity",
        "profile_sha256": "c" * 64,
        "schema_version": 1,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("candidate_tree", "d" * 40),
        ("profile_sha256", "d" * 64),
        ("nodes", [*NORMAL_GB10_WORKER_HOSTS[:-1], "trt-gb10-16"]),
        (
            "service_identity",
            {
                "user": "qianyi",
                "uid": 1000,
                "gid": 1000,
                "account": "loom-staging",
                "qos": "loom-staging",
            },
        ),
    ],
)
def test_remote_capacity_acceptance_rejects_malformed_or_drifted_evidence(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    artifact = _controller_artifact(tmp_path)
    acceptance = _capacity_artifact(artifact)
    acceptance[field] = value
    run = _Run(_capacity_response(acceptance))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    with pytest.raises(ValueError, match="acceptance evidence"):
        transport.accept_capacity(
            profile_sha256="c" * 64,
            nodes=NORMAL_GB10_WORKER_HOSTS,
        )


def test_remote_capacity_rejects_noncanonical_request_before_ssh(tmp_path: Path) -> None:
    artifact = _controller_artifact(tmp_path)
    run = _Run(_capacity_response(_capacity_artifact(artifact)))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    with pytest.raises(ValueError, match="capacity request"):
        transport.accept_capacity(
            profile_sha256="c" * 64,
            nodes=(*NORMAL_GB10_WORKER_HOSTS[:-1], "trt-gb10-16"),
        )

    assert run.calls == []


def test_remote_capacity_rejects_non_integer_envelope_schema(tmp_path: Path) -> None:
    artifact = _controller_artifact(tmp_path)
    response = json.loads(_capacity_response(_capacity_artifact(artifact)))
    response["schema_version"] = True
    run = _Run(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    with pytest.raises(RuntimeError, match="response drifted"):
        transport.accept_capacity(
            profile_sha256="c" * 64,
            nodes=NORMAL_GB10_WORKER_HOSTS,
        )


def test_capacity_validator_normalizes_unhashable_expected_nodes(tmp_path: Path) -> None:
    artifact = _controller_artifact(tmp_path)
    acceptance = _capacity_artifact(artifact)
    acceptance["nodes"] = [["not-a-node"]]
    acceptance["node_count"] = 1
    acceptance["probed_nodes"] = [NORMAL_GB10_WORKER_HOSTS[0]]
    acceptance["probed_node_count"] = 1
    acceptance["deferred_busy_nodes"] = []

    with pytest.raises(ValueError, match="acceptance evidence"):
        remote.validate_gb10_slurm_acceptance(
            acceptance,
            candidate_sha=artifact.candidate_sha,
            candidate_tree=artifact.candidate_tree,
            profile_sha256="c" * 64,
            nodes=[["not-a-node"]],  # type: ignore[list-item]
        )


def test_capacity_validator_rejects_evidence_lifetime_over_thirty_minutes(
    tmp_path: Path,
) -> None:
    artifact = _controller_artifact(tmp_path)
    acceptance = _capacity_artifact(artifact)
    generated_at = datetime(2026, 8, 27, 16, tzinfo=UTC)
    acceptance["generated_at"] = generated_at.isoformat()
    acceptance["expires_at"] = (generated_at + timedelta(minutes=30, seconds=1)).isoformat()

    with pytest.raises(ValueError, match="acceptance evidence"):
        remote.validate_gb10_slurm_acceptance(
            acceptance,
            candidate_sha=artifact.candidate_sha,
            candidate_tree=artifact.candidate_tree,
            profile_sha256="c" * 64,
            nodes=NORMAL_GB10_WORKER_HOSTS,
            now=generated_at + timedelta(seconds=1),
        )


def test_remote_transport_rejects_non_controller_artifact_before_ssh(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, execution_host="TRT-EAI-OLDLAB-1")
    run = _Run(remote._encode_helper_response("reconcile_compensations"))
    transport = _transport(artifact, run, tmp_path / "controller-ed25519")

    with pytest.raises(ValueError, match="controller artifact"):
        transport.observe(artifact)

    assert run.calls == []


@pytest.mark.parametrize(
    "pools",
    [
        ("gb10",),
        ("gb10", "task-image-builder-gb10", "task-image-builder-gb10"),
        ("gb10", "oldlab"),
    ],
    ids=["missing-builder", "extra-builder", "foreign-pool"],
)
def test_controller_authority_rejects_non_exact_pool_sets(
    tmp_path: Path,
    pools: tuple[str, ...],
) -> None:
    artifact = _controller_artifact(tmp_path)
    supervisors = tuple(
        SimpleNamespace(execution_host="gx10-01c7", pool_name=pool_name) for pool_name in pools
    )
    invalid = SimpleNamespace(
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
        supervisors=supervisors,
    )

    with pytest.raises(ValueError, match="exceeds fixed authority"):
        remote._validate_controller_artifact(
            invalid,
            candidate_sha=artifact.candidate_sha,
            candidate_tree=artifact.candidate_tree,
        )


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


def test_candidate_helper_dispatches_credential_operations_to_local_transport(
    tmp_path: Path,
) -> None:
    artifact = _controller_artifact(tmp_path)
    evidence = _credential_evidence()

    class _CredentialTransport:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def observe(self):
            self.calls.append("observe")
            return None

        def publish(self):
            self.calls.append("publish")
            return evidence

    local = _CredentialTransport()
    observe_request = remote._encode_helper_request(
        operation="observe_credential",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
    )
    publish_request = remote._encode_helper_request(
        operation="publish_credential",
        candidate_sha=artifact.candidate_sha,
        candidate_tree=artifact.candidate_tree,
    )

    observed = remote._decode_helper_credential(
        remote._handle_helper_request(
            observe_request,
            transport=None,
            credential_transport=local,
        ),
        operation="observe_credential",
    )
    published = remote._decode_helper_credential(
        remote._handle_helper_request(
            publish_request,
            transport=None,
            credential_transport=local,
        ),
        operation="publish_credential",
    )

    assert observed is None
    assert published == evidence
    assert local.calls == ["observe", "publish"]
