import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
    build_external_supervisor_artifact,
)
from loom_cli.rollout.gb10_readiness import ACTIVE_GB10_HOSTS
from loom_cli.rollout.gb10_rehearsal import GB10RehearsalAuthority, GB10RehearsalEvidence
from loom_cli.rollout.systemd_readiness import RehearsalSystemdActivation

REPO_ROOT = Path(__file__).resolve().parents[3]


def active_external_supervisor_artifact(
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> ExternalSupervisorArtifact:
    """Build an active supervisor fixture isolated from the production gate."""

    with tempfile.TemporaryDirectory(prefix="loom-test-supervisor-") as raw_dir:
        candidate = Path(raw_dir)
        profile = candidate / "deploy/environment-state/staging.toml"
        script = candidate / "scripts/ops/worker_pool_autoscaler_external_once.py"
        profile.parent.mkdir(parents=True)
        script.parent.mkdir(parents=True)
        profile_text = (REPO_ROOT / "deploy/environment-state/staging.toml").read_text(
            encoding="utf-8"
        )
        profile.write_text(profile_text, encoding="utf-8")
        shutil.copyfile(
            REPO_ROOT / "scripts/ops/worker_pool_autoscaler_external_once.py",
            script,
        )
        profile.chmod(0o600)
        script.chmod(0o700)
        with patch(
            "loom_cli.environment_state.staging_gb10_external_activation_blockers",
            return_value=(),
        ):
            return build_external_supervisor_artifact(
                candidate,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                image_tag=image_tag,
                environment="staging",
            )


def gb10_rehearsal_authority() -> GB10RehearsalAuthority:
    return GB10RehearsalAuthority(
        hosts=ACTIVE_GB10_HOSTS,
        ssh_config=Path("/opt/loom-staging-rollout/current/deploy/worker-pools/gb10/ssh_config"),
        identity=Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519"),
        ssh_config_sha256="a" * 64,
        identity_metadata_fingerprint="b" * 64,
    )


class PassingGB10RehearsalTransport:
    def execute(self, _contract: RehearsalSystemdActivation) -> GB10RehearsalEvidence:
        return self._evidence()

    def cleanup(self, _contract: RehearsalSystemdActivation) -> GB10RehearsalEvidence:
        return self._evidence()

    @staticmethod
    def _evidence() -> GB10RehearsalEvidence:
        return GB10RehearsalEvidence(
            host_boot_ids={
                host: "11111111-1111-4111-8111-111111111111" for host in ACTIVE_GB10_HOSTS
            },
            host_evidence_digests={host: "c" * 64 for host in ACTIVE_GB10_HOSTS},
            blockers={},
            cleanup_verified=True,
        )


def passing_gb10_transport_factory(
    _authority: GB10RehearsalAuthority,
) -> PassingGB10RehearsalTransport:
    return PassingGB10RehearsalTransport()


__all__ = [
    "PassingGB10RehearsalTransport",
    "active_external_supervisor_artifact",
    "gb10_rehearsal_authority",
    "passing_gb10_transport_factory",
]
