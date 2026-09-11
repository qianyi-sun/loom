"""Real standard-crypto verification without importing a site-package runtime."""

import hashlib
import json
import os
import subprocess
import sys
from importlib import import_module
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

_OPENSSL = Path("/usr/bin/openssl")
_MESSAGE = b"loom.native-worker-containment/v1\x00{\"purpose\":\"prepare\"}"


@pytest.fixture
def crypto():
    module = import_module("loom_capacity_executor.native_containment_protocol")
    key = Ed25519PrivateKey.generate()
    assert _OPENSSL.is_file(), "native containment requires installed OpenSSL"
    return module, {
        "message": _MESSAGE, "signature": key.sign(_MESSAGE),
        "public_key": key.public_key().public_bytes_raw(),
        "openssl_path": str(_OPENSSL),
        "openssl_sha256": hashlib.sha256(_OPENSSL.read_bytes()).hexdigest(),
    }


def test_native_signature_uses_real_openssl_and_seekable_sealed_inputs(crypto, monkeypatch):
    module, values = crypto
    # Ambient cryptographic configuration must not select providers or trust.
    monkeypatch.setenv("OPENSSL_CONF", "/missing/untrusted-config")
    monkeypatch.setenv("OPENSSL_MODULES", "/missing/untrusted-providers")
    before = set(os.listdir("/proc/self/fd"))
    module.verify_native_ed25519(**values)
    assert set(os.listdir("/proc/self/fd")) == before


@pytest.mark.parametrize("changed", ("message", "signature", "key", "binary", "empty", "oversize", "short-signature", "short-key", "relative-path", "timeout-alias"))
def test_native_signature_rejects_substitution_and_invalid_bounds(crypto, changed):
    module, original = crypto
    values = dict(original)
    if changed == "message":
        values["message"] += b"x"
    elif changed == "signature":
        values["signature"] = bytes([values["signature"][0] ^ 1]) + values["signature"][1:]
    elif changed == "key":
        values["public_key"] = Ed25519PrivateKey.generate().public_key().public_bytes_raw()
    elif changed == "binary":
        values["openssl_sha256"] = "f" * 64
    elif changed == "empty":
        values["message"] = b""
    elif changed == "oversize":
        values["message"] = b"x" * (32768 + 1)
    elif changed == "short-signature":
        values["signature"] = values["signature"][:-1]
    elif changed == "short-key":
        values["public_key"] = values["public_key"][:-1]
    elif changed == "relative-path":
        values["openssl_path"] = "openssl"
    else:
        values["timeout_seconds"] = True
    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(module.NativeContainmentVerificationError):
        module.verify_native_ed25519(**values)
    assert set(os.listdir("/proc/self/fd")) == before


def test_native_crypto_helper_executes_without_site_or_repository_imports(crypto):
    module, values = crypto
    data = {key: value.hex() if isinstance(value, bytes) else value for key, value in values.items()}
    probe = """
import json, runpy, sys
namespace = runpy.run_path(sys.argv[1])
value = json.load(sys.stdin)
for key in ('message', 'signature', 'public_key'):
    value[key] = bytes.fromhex(value[key])
namespace['verify_native_ed25519'](**value)
print('verified')
"""
    result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", probe, module.__file__],
        input=json.dumps(data), capture_output=True, text=True, check=False, timeout=5,
        env={"OPENSSL_CONF": "/missing/untrusted-config", "OPENSSL_MODULES": "/missing/untrusted-providers"})
    assert result.returncode == 0, result.stderr
    assert result.stdout == "verified\n"


def test_native_crypto_does_not_execute_an_unprotected_binary(crypto, tmp_path):
    module, values = crypto
    binary = tmp_path / "openssl"
    binary.write_bytes(_OPENSSL.read_bytes())
    binary.chmod(0o777)
    values["openssl_path"] = str(binary)
    with pytest.raises(module.NativeContainmentVerificationError):
        module.verify_native_ed25519(**values)
