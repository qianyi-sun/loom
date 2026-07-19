from pathlib import Path

from loom_cli.rollout.gb10_readiness import ACTIVE_GB10_HOSTS
from loom_cli.rollout.gb10_rehearsal import GB10RehearsalAuthority, GB10RehearsalEvidence
from loom_cli.rollout.systemd_readiness import RehearsalSystemdActivation


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
    "gb10_rehearsal_authority",
    "passing_gb10_transport_factory",
]
