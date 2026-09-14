"""The migration trusts only the fixed CNPG CA under unchanged cluster identity."""

import base64
import copy
import json
from datetime import UTC, datetime, timedelta

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.x509.oid import NameOID


def _ca():
    key = ed25519.Ed25519PrivateKey.generate()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "disposable-ca")])
    now = datetime.now(UTC)
    return x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(1).not_valid_before(
        now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1)).add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(key, algorithm=None).public_bytes(serialization.Encoding.PEM)


@pytest.mark.parametrize("drift", [None, "cluster", "name", "owner", "certificate", "racing-secret"])
def test_migration_ca_requires_unchanged_fixed_cluster_certificate(drift):
    from loom_cli.rollout.operator.protected_application_migration_ca import observe_application_migration_ca

    ca = _ca()
    uid = "11111111-1111-4111-8111-111111111111"
    cluster = {"metadata": {"uid": uid}, "status": {"certificates": {"serverCASecret": "loom-postgres-ca"}}}
    secret = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "metadata": {
        "namespace": "loom-staging", "name": "loom-postgres-ca", "uid": "22222222-2222-4222-8222-222222222222", "resourceVersion": "5",
        "ownerReferences": [{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": "loom-postgres", "uid": uid, "controller": True}]},
        "data": {"ca.crt": base64.b64encode(ca).decode(), "ca.key": "private-key-must-not-escape"}}
    if drift == "cluster":
        cluster["metadata"]["uid"] = "33333333-3333-4333-8333-333333333333"
    elif drift == "name":
        cluster["status"]["certificates"]["serverCASecret"] = "foreign-ca"
    elif drift == "owner":
        secret["metadata"]["ownerReferences"][0]["uid"] = "33333333-3333-4333-8333-333333333333"
    elif drift == "certificate":
        secret["data"]["ca.crt"] = base64.b64encode(b"not-a-certificate").decode()
    class Runner:
        def __init__(self):
            self.environment = {}
            self.reads = 0
        def capture_stdout(self, argv, **kwargs):
            if "secret" in argv:
                self.reads += 1
                value = copy.deepcopy(secret)
                if drift == "racing-secret" and self.reads > 1:
                    value["metadata"]["resourceVersion"] = "6"
                return json.dumps(value).encode()
            return json.dumps(cluster).encode()
    if drift:
        with pytest.raises(ValueError, match="migration CA"):
            observe_application_migration_ca(Runner(), cluster_uid=uid)
    else:
        observed = observe_application_migration_ca(Runner(), cluster_uid=uid)
        assert observed.certificate == ca
        assert "private-key-must-not-escape" not in repr(observed) + str(observed.binding())
        assert observed.cluster_uid == uid
