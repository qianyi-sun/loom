"""Each child receives new namespace-bound credentials, not management secrets."""

from __future__ import annotations

import base64
import tomllib

from cryptography import x509
from sqlalchemy.engine import make_url


def test_material_has_independent_roles_tls_host_and_no_management_credential():
    from loom_service.environment_management.credentials import generate_material

    first = generate_material(namespace="loom-dev-alice", tls_secret_name="loom-postgres-tls")
    second = generate_material(namespace="loom-dev-bob", tls_secret_name="loom-postgres-tls")
    assert first.keys() == {
        "loom-platform-db", "loom-postgres-tls", "loom-platform-auth", "loom-admin-secret",
        "loom-platform-collector", "loom-platform-batch-runner",
    }
    passwords = set()
    for role in ("admin", "service", "control-plane", "gateway", "actuator"):
        url = make_url(first["loom-platform-db"][role + "-url"])
        assert url.host == "loom-postgres.loom-dev-alice.svc"
        assert url.database == "loom" and url.query["sslmode"] == "verify-full"
        assert len(url.password) >= 32
        assert url.username == ("postgres" if role == "admin" else "loom_" + role.replace("-", "_"))
        passwords.add(url.password)
    assert len(passwords) == 5
    cert = x509.load_pem_x509_certificate(first["loom-postgres-tls"]["tls.crt"].encode())
    assert cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName) == [
        "loom-postgres.loom-dev-alice.svc", "loom-postgres.loom-dev-alice.svc.cluster.local",
    ]
    assert first["loom-platform-db"]["ca.crt"] != second["loom-platform-db"]["ca.crt"]
    for name, key in (("loom-platform-auth", "jwt-signing-key"), ("loom-platform-auth", "secret-store-master-key"),
                      ("loom-admin-secret", "secrets.toml"), ("loom-platform-collector", "token")):
        assert first[name][key] != second[name][key]
    assert len(base64.b64decode(first["loom-platform-auth"]["secret-store-master-key"])) == 32
    assert tomllib.loads(first["loom-admin-secret"]["secrets.toml"])["admin"]["token"].startswith("loom_admin_")


def test_longest_namespace_can_get_a_valid_database_certificate():
    from loom_service.environment_management.credentials import generate_material

    namespace = "loom-dev-" + "a" * 54
    material = generate_material(namespace=namespace, tls_secret_name="db-tls")
    cert = x509.load_pem_x509_certificate(material["db-tls"]["tls.crt"].encode())
    assert "loom-postgres." + namespace + ".svc" in cert.extensions.get_extension_for_class(
        x509.SubjectAlternativeName,
    ).value.get_values_for_type(x509.DNSName)
