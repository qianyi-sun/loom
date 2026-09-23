"""Real digest-only registry copy, private TLS, raw bytes and lost-reply recovery."""
from __future__ import annotations

import json
import os
import shutil
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import closing
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address

import docker
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from scripts.ops import nebius_ingress_image as publication

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicit disposable infrastructure opt-in")
REGISTRY = "docker.io/library/registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"


@pytest.mark.timeout(180)
def test_real_tls_registry_copy_by_digest_recovers_lost_reply(tmp_path, monkeypatch):
    assert shutil.which("skopeo"), "the cluster lane requires its pinned Skopeo package"
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "disposable-registry")])
    now = datetime.now(UTC)
    certificate = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=1))
                   .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                   .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ip_address("127.0.0.1"))]), critical=False)
                   .sign(key, hashes.SHA256()))
    cert_dir = tmp_path / "tls"
    cert_dir.mkdir(mode=0o700)
    cert_file = cert_dir / "cert.pem"
    cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file = cert_dir / "key.pem"
    key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()))
    key_file.chmod(0o600)
    monkeypatch.setenv("SSL_CERT_FILE", str(cert_file))
    prefix = "cr.eu-north1.nebius.cloud/disposable"
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps({"auths": {"cr.eu-north1.nebius.cloud": {"auth": "aWFtOmZpeHR1cmU="}}}))
    auth_file.chmod(0o600)
    with closing(docker.from_env()) as engine:
        container = engine.containers.run(
            REGISTRY, detach=True, auto_remove=True,
            ports={"5000/tcp": ("127.0.0.1", None)},
            volumes={str(cert_dir): {"bind": "/tls", "mode": "ro"}},
            environment={"REGISTRY_HTTP_TLS_CERTIFICATE": "/tls/cert.pem", "REGISTRY_HTTP_TLS_KEY": "/tls/key.pem"},
        )
        try:
            container.reload()
            port = container.attrs["NetworkSettings"]["Ports"]["5000/tcp"][0]["HostPort"]
            destination = "127.0.0.1:" + port
            context = ssl.create_default_context(cafile=str(cert_file))
            deadline = time.monotonic() + 20
            while True:
                try:
                    with urllib.request.urlopen("https://" + destination + "/v2/", context=context, timeout=2) as response:
                        assert response.status == 200
                    break
                except urllib.error.URLError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
            run = subprocess.run
            writes = []

            def local_transport(command, **kwargs):
                # Change only the registry endpoint; preserve real TLS checking,
                # Skopeo's raw/config/copy semantics and production digest pin.
                assert command[0] == "skopeo"
                mapped = [argument.replace("docker://" + prefix + "/", "docker://" + destination + "/")
                          for argument in command]
                result = run(mapped, **kwargs)
                if command[1] == "copy":
                    assert not writes
                    if result.returncode:
                        pytest.fail("disposable registry copy failed: " + result.stderr.decode(errors="replace"))
                    writes.append(command)
                    raise subprocess.TimeoutExpired(command, kwargs["timeout"])
                return result

            monkeypatch.setattr(publication.subprocess, "run", local_transport)
            options = dict(registry_prefix=prefix, region="eu-north1", auth_file=auth_file,
                           state_dir=tmp_path / "state")
            receipt = publication.mirror_ingress_image(**options)
            assert receipt["image"] == prefix + "/loom-shared-ingress@sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18"
            assert receipt["platform"] == "linux/amd64" and receipt["version"] == "v3.7.13"
            assert publication.mirror_ingress_image(**options) == receipt
            assert len(writes) == 1
        finally:
            container.stop(timeout=2)
