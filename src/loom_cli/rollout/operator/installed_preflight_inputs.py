"""Load exact installed inputs for the shared deep-preflight authority."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.credential_authority import TrustedFileRead, read_trusted_file
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.install_attestation import VerifiedRunnerInstall, verify_runner_install
from loom_cli.rollout.preflight_credential_paths import (
    READONLY_DATABASE_CREDENTIAL_PATH,
    READONLY_KUBECONFIG_PATH,
    READONLY_MINIO_CREDENTIAL_PATH,
    READONLY_TOKEN_PATH,
    REHEARSAL_KUBECONFIG_PATH,
)
from loom_cli.rollout.preflight_registered_checks import CredentialProbeSource

from .config import OperatorConfig
from .preflight import (
    EXPECTED_GB10_SSH_CONFIG_SHA256,
    ExternalGB10AuthorityProfile,
    GB10PreflightInputs,
    load_catalog_environment_path,
    load_external_gb10_authority_profile,
    load_gb10_preflight_inputs,
    load_shared_repository_binding,
    load_system_shared_repository_binding,
    shared_repository_binding_digest,
)

InstallVerifier = Callable[..., VerifiedRunnerInstall]
TrustedReader = Callable[..., TrustedFileRead]
CatalogPathLoader = Callable[..., Path | None]
GB10InputsLoader = Callable[..., GB10PreflightInputs | None]
SharedBindingLoader = Callable[..., dict[str, int] | None]
SystemBindingLoader = Callable[..., dict[str, int] | None]
ExternalProfileLoader = Callable[..., ExternalGB10AuthorityProfile | None]


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
    gb10_mount_binding: dict[str, int] | None
    gb10_mount_binding_digest: str | None
    migration_policy_path: Path
    migration_policy_digest: str
    gb10_external_profile_digest: str | None = None
    external_mount_binding_loader: Callable[[], dict[str, int] | None] | None = None

    def __post_init__(self) -> None:
        digests = (
            self.runner_install_digest,
            self.kubeconfig_metadata_digest,
            self.gb10_ssh_config_sha256,
            self.gb10_identity_metadata_fingerprint,
            self.migration_policy_digest,
            *((self.gb10_mount_binding_digest,) if self.gb10_mount_binding_digest else ()),
            *(
                ()
                if self.gb10_external_profile_digest is None
                else (self.gb10_external_profile_digest,)
            ),
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
            or not self.migration_policy_path.is_absolute()
            or (
                self.gb10_external_profile_digest is None
                and (
                    self.gb10_mount_binding is None
                    or self.gb10_mount_binding_digest is None
                    or self.external_mount_binding_loader is not None
                )
            )
            or (
                self.gb10_external_profile_digest is not None
                and (
                    self.gb10_mount_binding is not None
                    or self.gb10_mount_binding_digest is not None
                    or self.external_mount_binding_loader is None
                )
            )
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
        system_binding_loader: SystemBindingLoader = load_system_shared_repository_binding,
        external_profile_loader: ExternalProfileLoader = load_external_gb10_authority_profile,
        migration_policy_path: Path | None = None,
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
        external_profile = external_profile_loader(config, service_uid=service_uid)
        binding = (
            shared_binding_loader(service_uid=service_uid) if external_profile is None else None
        )
        if gb10 is None or (external_profile is None and binding is None):
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
        selected_migration_policy = (
            config.runner_repo / "config/staging-migration-policy.json"
            if migration_policy_path is None
            else migration_policy_path
        )
        if not selected_migration_policy.is_absolute() or ".." in selected_migration_policy.parts:
            raise ValueError("installed preflight migration policy path is invalid")
        try:
            migration_policy_digest = hashlib.sha256(
                selected_migration_policy.read_bytes()
            ).hexdigest()
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
            gb10_mount_binding=None if binding is None else dict(binding),
            gb10_mount_binding_digest=(
                None if binding is None else shared_repository_binding_digest(binding)
            ),
            gb10_external_profile_digest=(
                None if external_profile is None else external_profile.profile_digest
            ),
            migration_policy_path=selected_migration_policy,
            migration_policy_digest=migration_policy_digest,
            external_mount_binding_loader=(
                None
                if external_profile is None
                else lambda: system_binding_loader(service_uid=service_uid)
            ),
        )

    def resolve_gb10_mount_binding(self) -> tuple[dict[str, int], str]:
        """Read the external system mount only after explicit preparation."""
        if self.gb10_external_profile_digest is None:
            if self.gb10_mount_binding is None or self.gb10_mount_binding_digest is None:
                raise ValueError("installed preflight GB10 authority is unavailable")
            return dict(self.gb10_mount_binding), self.gb10_mount_binding_digest
        loader = self.external_mount_binding_loader
        if loader is None:
            raise ValueError("installed external GB10 mount authority is unavailable")
        binding = loader()
        if binding is None:
            raise ValueError("installed external GB10 mount authority is unavailable")
        return dict(binding), shared_repository_binding_digest(binding)

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
                label="readonly-database",
                path=READONLY_DATABASE_CREDENTIAL_PATH,
            ),
            CredentialProbeSource(
                label="readonly-minio",
                path=READONLY_MINIO_CREDENTIAL_PATH,
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
