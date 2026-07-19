"""Load exact installed inputs for the shared deep-preflight authority."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.credential_authority import TrustedFileRead, read_trusted_file
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.install_attestation import VerifiedRunnerInstall, verify_runner_install
from loom_cli.rollout.migration_readiness import DEFAULT_MIGRATION_POLICY
from loom_cli.rollout.preflight_credential_paths import (
    READONLY_KUBECONFIG_PATH,
    READONLY_TOKEN_PATH,
    REHEARSAL_KUBECONFIG_PATH,
)
from loom_cli.rollout.preflight_registered_checks import CredentialProbeSource

from .config import OperatorConfig
from .preflight import (
    EXPECTED_GB10_SSH_CONFIG_SHA256,
    GB10PreflightInputs,
    load_catalog_environment_path,
    load_gb10_preflight_inputs,
    load_shared_repository_binding,
    shared_repository_binding_digest,
)

InstallVerifier = Callable[..., VerifiedRunnerInstall]
TrustedReader = Callable[..., TrustedFileRead]
CatalogPathLoader = Callable[..., Path | None]
GB10InputsLoader = Callable[..., GB10PreflightInputs | None]
SharedBindingLoader = Callable[..., dict[str, int] | None]


@dataclass(frozen=True, slots=True)
class InstalledPreflightInputs:
    """Secret-free identities and protected paths loaded once per runtime build."""

    runner_install_digest: str
    credential_sources: tuple[CredentialProbeSource, ...]
    kubeconfig_metadata_digest: str
    gb10_targets: tuple[GB10ProbeTarget, ...]
    gb10_ssh_config: Path
    gb10_identity: Path
    gb10_ssh_config_sha256: str
    gb10_identity_metadata_fingerprint: str
    gb10_mount_binding: dict[str, int]
    gb10_mount_binding_digest: str
    migration_policy_digest: str

    def __post_init__(self) -> None:
        digests = (
            self.runner_install_digest,
            self.kubeconfig_metadata_digest,
            self.gb10_ssh_config_sha256,
            self.gb10_identity_metadata_fingerprint,
            self.gb10_mount_binding_digest,
            self.migration_policy_digest,
        )
        if (
            any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for value in digests
            )
            or not self.credential_sources
            or not self.gb10_targets
            or not self.gb10_ssh_config.is_absolute()
            or not self.gb10_identity.is_absolute()
        ):
            raise ValueError("installed preflight inputs are invalid")

    @classmethod
    def load(
        cls,
        config: OperatorConfig,
        *,
        service_uid: int,
        verify_install: InstallVerifier = verify_runner_install,
        read_file: TrustedReader = read_trusted_file,
        catalog_path_loader: CatalogPathLoader = load_catalog_environment_path,
        gb10_inputs_loader: GB10InputsLoader = load_gb10_preflight_inputs,
        shared_binding_loader: SharedBindingLoader = load_shared_repository_binding,
        migration_policy_path: Path = DEFAULT_MIGRATION_POLICY,
    ) -> InstalledPreflightInputs:
        """Fail closed while reading all static installed authorities."""
        if service_uid < 0:
            raise ValueError("installed preflight service identity is invalid")
        verified = verify_install(service_uid=service_uid)
        if not verified.ready:
            raise ValueError("installed preflight runner assets drifted")
        credentials = _credential_sources(
            config,
            service_uid=service_uid,
            catalog_path_loader=catalog_path_loader,
        )
        kubeconfig = read_file(
            config.kubeconfig_path,
            service_uid=service_uid,
            private=True,
            allow_qianyi_owner=True,
            require_nonempty=True,
        )
        gb10 = gb10_inputs_loader(config, service_uid=service_uid)
        binding = shared_binding_loader(service_uid=service_uid)
        if gb10 is None or binding is None:
            raise ValueError("installed preflight GB10 authority is unavailable")
        ssh_config = read_file(
            gb10.ssh_config,
            service_uid=service_uid,
            private=False,
            require_nonempty=True,
        )
        identity = read_file(
            gb10.identity,
            service_uid=service_uid,
            private=True,
            require_nonempty=True,
        )
        ssh_digest = hashlib.sha256(ssh_config.payload).hexdigest()
        if ssh_digest != EXPECTED_GB10_SSH_CONFIG_SHA256:
            raise ValueError("installed preflight GB10 SSH topology drifted")
        try:
            migration_policy_digest = hashlib.sha256(migration_policy_path.read_bytes()).hexdigest()
        except OSError as exc:
            raise ValueError("installed preflight migration policy is unavailable") from exc
        return cls(
            runner_install_digest=verified.attestation.payload_digest,
            credential_sources=credentials,
            kubeconfig_metadata_digest=kubeconfig.metadata_fingerprint,
            gb10_targets=gb10.targets,
            gb10_ssh_config=gb10.ssh_config,
            gb10_identity=gb10.identity,
            gb10_ssh_config_sha256=ssh_digest,
            gb10_identity_metadata_fingerprint=identity.metadata_fingerprint,
            gb10_mount_binding=dict(binding),
            gb10_mount_binding_digest=shared_repository_binding_digest(binding),
            migration_policy_digest=migration_policy_digest,
        )

    @property
    def browser_token_path(self) -> Path:
        for source in self.credential_sources:
            if source.label == "admin":
                return source.path
        raise ValueError("installed preflight admin credential is unavailable")


def _credential_sources(
    config: OperatorConfig,
    *,
    service_uid: int,
    catalog_path_loader: CatalogPathLoader,
) -> tuple[CredentialProbeSource, ...]:
    entries: list[CredentialProbeSource] = []
    for label, source, fingerprint in (
        ("admin", config.admin_token_source, config.expect_admin_token_fingerprint),
        ("worker", config.worker_token_source, None),
        ("service", config.service_token_source, None),
    ):
        path = _file_source_path(source)
        entries.append(
            CredentialProbeSource(
                label=label,
                path=path,
                expected_content_fingerprint=fingerprint,
            )
        )
    catalog = catalog_path_loader(config, service_uid=service_uid)
    if catalog is None:
        raise ValueError("installed preflight catalog authority is unavailable")
    entries.append(CredentialProbeSource(label="catalog", path=catalog))
    entries.extend(
        (
            CredentialProbeSource(
                label="readonly-probe",
                path=READONLY_TOKEN_PATH,
            ),
            CredentialProbeSource(
                label="readonly-kubeconfig",
                path=READONLY_KUBECONFIG_PATH,
            ),
            CredentialProbeSource(
                label="rehearsal-kubeconfig",
                path=REHEARSAL_KUBECONFIG_PATH,
            ),
            CredentialProbeSource(
                label="server-dry-run-kubeconfig",
                path=config.kubeconfig_path,
            ),
        )
    )
    return tuple(entries)


def _file_source_path(source: str) -> Path:
    if not source.startswith("file:"):
        raise ValueError("installed preflight credential source is invalid")
    path = Path(source.removeprefix("file:"))
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("installed preflight credential source is invalid")
    return path


__all__ = ["InstalledPreflightInputs"]
