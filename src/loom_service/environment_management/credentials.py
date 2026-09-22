"""Generate isolated child credentials; persist encrypted before publishing Secrets."""

from __future__ import annotations

import base64
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from sqlalchemy.engine import URL

from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
from loom_service.environment_management.provider import ProviderBlockedError, ProvisioningContext
from loom_service.environment_management.registry import EnvironmentRegistry
from loom_service.environment_management.steps import ProvisioningStep


def generate_material(*, namespace: str, tls_secret_name: str) -> dict[str, dict[str, str]]:
    now = datetime.now(UTC)
    host = "loom-postgres." + namespace + ".svc"
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Loom database CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name).public_key(ca_key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=5))
          .not_valid_after(now + timedelta(days=3650))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=False,
                                      data_encipherment=False, key_agreement=False, key_cert_sign=True,
                                      crl_sign=True, encipher_only=False, decipher_only=False), critical=True)
          .sign(ca_key, hashes.SHA256()))
    # CN is limited to 64 bytes; the full namespace DNS identities live in SAN.
    certificate = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, namespace)]))
                   .issuer_name(ca_name).public_key(server_key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=365))
                   .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                   .add_extension(x509.SubjectAlternativeName([x509.DNSName(host), x509.DNSName(host + ".cluster.local")]), critical=False)
                   .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
                   .sign(ca_key, hashes.SHA256()))
    pem = serialization.Encoding.PEM
    database = {"ca.crt": ca.public_bytes(pem).decode()}
    for role in ("admin", "service", "control-plane", "gateway", "actuator"):
        password = secrets.token_urlsafe(32)
        user = "postgres" if role == "admin" else "loom_" + role.replace("-", "_")
        database["postgres-password" if role == "admin" else role + "-password"] = password
        database[role + "-url"] = URL.create("postgresql", username=user, password=password, host=host,
                                            port=5432, database="loom", query={
                                                "sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt",
                                            }).render_as_string(hide_password=False)
    return {
        "loom-platform-db": database,
        tls_secret_name: {"tls.crt": certificate.public_bytes(pem).decode(), "tls.key": server_key.private_bytes(
            pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
        ).decode()},
        "loom-platform-auth": {"jwt-signing-key": secrets.token_urlsafe(32),
                               "secret-store-master-key": base64.b64encode(secrets.token_bytes(32)).decode()},
        "loom-admin-secret": {"secrets.toml": '[admin]\ntoken = "loom_admin_' + secrets.token_urlsafe(32) + '"\n'},
        "loom-platform-collector": {"token": "loom_ecc_" + secrets.token_urlsafe(32)},
        "loom-platform-batch-runner": {"token": "loom_br_" + secrets.token_urlsafe(32)},
    }


class EnvironmentCredentialProvider:
    def __init__(self, registry: EnvironmentRegistry, cloud: NebiusSdkEnvironmentApi, kubernetes: KubernetesEnvironmentProvider):
        self.registry, self.cloud, self.kubernetes = registry, cloud, kubernetes

    async def apply(self, context: ProvisioningContext, step: ProvisioningStep) -> str:
        if step.kind != "credentials" or step.payload.get("namespace") != context.registration["application_namespace"]:
            raise ProviderBlockedError("credential_intent_not_allowed")
        action = step.payload.get("action")
        if action == "material":
            material = generate_material(namespace=step.payload["namespace"], tls_secret_name=context.config["db_tls_secret_name"])
            storage: dict[str, str] = {}
            for purpose in ("canonical", "source", "backup"):
                identity = context.identities.get(f"iam:{purpose}:access_key")
                if not identity:
                    raise ProviderBlockedError("credential_dependency_missing")
                value = await self.cloud.access_key_secret(identity)
                prefix = "" if purpose == "canonical" else purpose + "-"
                storage.update({prefix + key: secret for key, secret in value.items()})
            material["loom-platform-storage"] = storage
            return await self.registry.store_material(context.lease, step.key, material)
        if action == "kubernetes_secret":
            material = await self.registry.load_material(context.lease, "credentials:material")
            name = step.payload["name"]
            if name not in material:
                raise ProviderBlockedError("credential_secret_not_planned")
            doc: dict[str, Any] = {
                "apiVersion": "v1", "kind": "Secret", "immutable": True,
                "metadata": {"namespace": step.payload["namespace"], "name": name},
                "type": "kubernetes.io/tls" if name == context.config["db_tls_secret_name"] else "Opaque",
                "data": {key: base64.b64encode(value.encode()).decode() for key, value in material[name].items()},
            }
            return await self.kubernetes.apply(context, ProvisioningStep(step.key, "kubernetes", doc))
        raise ProviderBlockedError("credential_action_not_supported")
