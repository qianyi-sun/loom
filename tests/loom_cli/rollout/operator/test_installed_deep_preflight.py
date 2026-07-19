from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget, GB10SharedMountReadiness
from loom_cli.rollout.operator.deep_preflight_authority import RuntimePurpose
from loom_cli.rollout.operator.installed_deep_preflight import InstalledDeepPreflightComposition
from loom_cli.rollout.operator.installed_preflight_inputs import InstalledPreflightInputs
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_registered_checks import CredentialProbeSource
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )


class _Artifacts:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def load_exact(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            publication=SimpleNamespace(
                candidate_sha=kwargs["candidate_sha"],
                candidate_tree=kwargs["candidate_tree"],
                mutation_epoch=kwargs["mutation_epoch"],
            )
        )


def test_composition_uses_one_source_graph_and_loads_outputs_only_for_detached(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    candidate = _candidate()
    artifacts = _Artifacts()
    inputs = InstalledPreflightInputs(
        runner_install_digest="1" * 64,
        credential_sources=(CredentialProbeSource(label="admin", path=tmp_path / "admin"),),
        kubeconfig_metadata_digest="2" * 64,
        gb10_targets=(GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),),
        gb10_ssh_config=tmp_path / "ssh-config",
        gb10_identity=tmp_path / "identity",
        gb10_ssh_config_sha256="3" * 64,
        gb10_identity_metadata_fingerprint="4" * 64,
        gb10_mount_binding={"service_uid": 501},
        gb10_mount_binding_digest="5" * 64,
        migration_policy_digest="6" * 64,
    )

    def command(*_args, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    composition = InstalledDeepPreflightComposition(
        config=config,
        service_uid=501,
        service_gid=20,
        inputs=inputs,
        artifact_store=artifacts,  # type: ignore[arg-type]
        attestation_store=PreflightAttestationStore(tmp_path / "state"),
        git_run=command,
        executable_lookup=lambda _name: "/fixed/tool",
        docker_runtime_run=command,
        kubernetes_run=command,
        readonly_authority_source=lambda: ReadonlyAuthorityEvidence(
            principal="loom-rollout-readonly",
            environment="staging",
            namespace="loom-staging",
            kubernetes_verbs=("get", "list", "watch"),
            kubernetes_resources=("deployments", "pods", "services"),
            http_methods=("GET", "HEAD"),
            capability_source_digest="7" * 64,
        ),
        capacity_source=lambda: StagingCapacity(0, 0, 100, 100),
        backup_authority_factory=lambda _epoch: None,  # type: ignore[arg-type,return-value]
        systemd_run=command,
        gb10_run=command,
        gb10_mount_source=lambda: GB10SharedMountReadiness(
            host_digests={"trt-gb10-1": "8" * 64},
            failed_hosts=(),
        ),
        systemd_analyze_run=command,
        image_run=command,
        render_manifest=lambda: "",
        server_dry_run=lambda _rendered: command(),
        browser_run=command,
        baseline_probe_factory=lambda _epoch: {},
        route="https://staging.example.invalid/dev",
        rehearsal_factory=lambda *_args: (
            lambda *_inner: {},
            lambda *_inner: ("rehearsal-exact", "9" * 64),
        ),
        read_mutation_epoch=lambda: 9,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    admission = composition.sources(candidate, 9, RuntimePurpose.ADMISSION)
    detached = composition.sources(candidate, 9, RuntimePurpose.DETACHED_REHEARSAL)

    assert admission.loaded_artifacts is None
    assert detached.loaded_artifacts is not None
    assert artifacts.calls == [
        {
            "candidate_sha": candidate.resolved_sha,
            "candidate_tree": candidate.resolved_tree,
            "mutation_epoch": 9,
            "image_tag": candidate.image_tag,
            "namespace": "loom-staging",
            "image_run": command,
        }
    ]
    assert composition.authority().current_mutation_epoch() == 9
