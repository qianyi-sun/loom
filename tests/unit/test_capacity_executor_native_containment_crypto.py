"""Real standard-crypto verification without importing a site-package runtime."""

import fcntl
import hashlib
import json
import os
import signal
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


def test_native_verification_seals_every_input_before_start(crypto, monkeypatch):
    module, values = crypto
    original = subprocess.Popen
    calls = []

    def inspected(args, **kwargs):
        calls.append((args, kwargs))
        assert args[:4] == ["openssl", "pkeyutl", "-verify", "-rawin"]
        assert kwargs["env"] == {"OPENSSL_CONF": "/dev/null", "LC_ALL": "C"}
        assert kwargs["cwd"] == "/"
        assert kwargs["start_new_session"] is True
        assert kwargs["executable"].startswith("/proc/self/fd/")
        assert len(kwargs["pass_fds"]) == 4
        for descriptor in kwargs["pass_fds"]:
            assert fcntl.fcntl(descriptor, getattr(fcntl, "F_GET_SEALS", 1034)) & 15 == 15
            with pytest.raises(PermissionError):
                os.write(descriptor, b"tamper")
        return original(args, **kwargs)

    monkeypatch.setattr(module.subprocess, "Popen", inspected)
    module.verify_native_ed25519(**values)
    assert len(calls) == 1


def test_native_verifier_timeout_kills_and_reaps_exact_owned_process(crypto, monkeypatch):
    module, values = crypto
    original = subprocess.Popen
    children = []

    def stuck_verifier(args, **kwargs):
        # Substitute only the crypto process to exercise actual OS timeout,
        # kill and reap behavior; normal tests run the real pinned OpenSSL.
        child = original(["/usr/bin/sleep", "30"], start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        children.append(child)
        return child

    monkeypatch.setattr(module.subprocess, "Popen", stuck_verifier)
    values["timeout_seconds"] = 1
    with pytest.raises(module.NativeContainmentVerificationError, match="unavailable"):
        module.verify_native_ed25519(**values)
    assert len(children) == 1
    assert children[0].returncode == -signal.SIGKILL
    with pytest.raises(ProcessLookupError):
        os.kill(children[0].pid, 0)


def test_interrupted_wait_does_not_signal_an_already_reaped_process_group(crypto, monkeypatch):
    module, values = crypto

    class ReapedDuringInterrupt:
        pid = 12345
        returncode = None

        def wait(self, timeout):
            if self.returncode is None:
                # Popen.wait may reap during its brief KeyboardInterrupt grace
                # period, then still propagate the interrupt to its caller.
                self.returncode = 0
                raise KeyboardInterrupt
            return self.returncode

        def poll(self):
            return self.returncode

    def foreign_group(*args):
        pytest.fail("a reaped verifier PID may already name a foreign process group")

    monkeypatch.setattr(module.subprocess, "Popen", lambda *args, **kwargs: ReapedDuringInterrupt())
    monkeypatch.setattr(module.os, "killpg", foreign_group)
    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(KeyboardInterrupt):
        module.verify_native_ed25519(**values)
    assert set(os.listdir("/proc/self/fd")) == before
