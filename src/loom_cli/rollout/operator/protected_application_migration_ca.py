"""Read only the fixed staging CNPG public CA and retain its exact identity."""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from cryptography import x509

from .protected_application_credential_recovery import CredentialRecoveryRunner
from .protected_application_migration_resources import ApplicationMigrationResourceIdentity
from .protected_cnpg_writer_configuration import _json, _mapping


@dataclass(frozen=True, slots=True)
class ApplicationMigrationCA:
    cluster_uid: str
    secret_uid: str
    resource_version: str
    certificate: bytes = field(repr=False)

    def binding(self) -> dict[str, str]:
        return {"cluster_uid": self.cluster_uid, "secret_uid": self.secret_uid,
            "resource_version": self.resource_version, "certificate_sha256": hashlib.sha256(self.certificate).hexdigest()}


def observe_application_migration_ca(runner: CredentialRecoveryRunner, *, cluster_uid: str) -> ApplicationMigrationCA:
    def cluster() -> None:
        value = _json(runner.capture_stdout(("kubectl", "--namespace", "loom-staging", "get",
            "cluster.postgresql.cnpg.io/loom-postgres", "--output=json", "--request-timeout=30s"),
            env=runner.environment, timeout_seconds=35))
        if (_mapping(value.get("metadata")).get("uid") != cluster_uid
                or _mapping(_mapping(value.get("status")).get("certificates")).get("serverCASecret") != "loom-postgres-ca"):
            raise ValueError("application migration CA cluster binding changed")

    def certificate() -> ApplicationMigrationCA:
        value = _json(runner.capture_stdout(("kubectl", "--namespace", "loom-staging", "get", "secret",
            "loom-postgres-ca", "--output=json", "--request-timeout=30s"), env=runner.environment, timeout_seconds=35))
        metadata = _mapping(value.get("metadata"))
        owners = metadata.get("ownerReferences")
        if (value.get("apiVersion") != "v1" or value.get("kind") != "Secret" or value.get("type") != "Opaque"
                or metadata.get("namespace") != "loom-staging" or metadata.get("name") != "loom-postgres-ca"
                or "deletionTimestamp" in metadata or not isinstance(owners, list) or len(owners) != 1):
            raise ValueError("application migration CA Secret identity changed")
        owner = _mapping(owners[0])
        if (owner.get("apiVersion") != "postgresql.cnpg.io/v1" or owner.get("kind") != "Cluster"
                or owner.get("name") != "loom-postgres" or owner.get("uid") != cluster_uid or owner.get("controller") is not True):
            raise ValueError("application migration CA Secret owner changed")
        try:
            identity = ApplicationMigrationResourceIdentity(str(metadata.get("uid")), str(metadata.get("resourceVersion")))
            encoded = _mapping(value.get("data")).get("ca.crt")
            if not isinstance(encoded, str) or not 64 <= len(encoded) <= 90000:
                raise ValueError
            pem = base64.b64decode(encoded, validate=True)
            certificates = x509.load_pem_x509_certificates(pem)
            if len(certificates) != 1 or len(pem) > 65536:
                raise ValueError
            ca = certificates[0]
            now = datetime.now(UTC)
            if (not ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
                    or ca.not_valid_before_utc > now or ca.not_valid_after_utc < now + timedelta(hours=1)):
                raise ValueError
        except (ValueError, x509.ExtensionNotFound):
            raise ValueError("application migration CA certificate or identity is invalid") from None
        return ApplicationMigrationCA(cluster_uid, identity.uid, identity.resource_version, pem)

    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", cluster_uid) is None:
        raise ValueError("application migration CA cluster identity is invalid")
    cluster()
    observed = certificate()
    cluster()
    if certificate() != observed:
        raise ValueError("application migration CA changed during observation")
    return observed
