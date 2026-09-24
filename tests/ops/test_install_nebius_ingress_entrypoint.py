"""Ingress authority preserves every existing SSH grant and verifies local sources."""
from __future__ import annotations

import base64
import hashlib
import importlib
import json
import struct
import subprocess
import sys
from pathlib import Path

import pytest
from tests.ops.test_nebius_ingress_bootstrap import archive, configuration


def module():
    return importlib.import_module("scripts.ops.install_nebius_ingress_entrypoint")


@pytest.fixture
def inputs(tmp_path):
    (tmp_path / ".loom").mkdir(mode=0o700)
    (tmp_path / ".ssh").mkdir(mode=0o700)
    keys = tmp_path / ".ssh/authorized_keys"
    keys.write_bytes(b'# operator\nrestrict,command="certificate-command" ssh-ed25519 FOREIGN old\n')
    keys.chmod(0o600)
    config = configuration(tmp_path / ".loom")
    source = b'def authorized_main(digest):\n    print("ingress fixture")\n    return 0\n'
    content = archive({"installation.json": json.dumps(config).encode(),
                       "scripts/ops/nebius_ingress_bootstrap.py": source,
                       "scripts/ops/nebius_certificate_gateway.py": b"# unchanged supervisor\n"})
    wire = struct.pack(">I", 11) + b"ssh-ed25519" + struct.pack(">I", 32) + b"i" * 32
    key = "ssh-ed25519 " + base64.b64encode(wire).decode()
    return tmp_path / ".loom/nebius-ingress", keys, key, content, hashlib.sha256(content).hexdigest()


def test_install_preserves_certificate_key_and_replays_without_changes(inputs):
    root, keys, key, content, digest = inputs
    before = keys.read_bytes()
    report = module().install(content, expected_sha256=digest, public_key=key, apply=True)
    after = keys.read_bytes()
    assert after.startswith(before) and len(after.splitlines()) == 3
    assert after[len(before):].startswith(b'restrict,command="/usr/bin/python3 -I ')
    assert module().install(content, expected_sha256=digest, public_key=key, apply=True) == report
    assert keys.read_bytes() == after and not (root / "state").exists()
    entry = root / "authority" / digest / "entrypoint.py"
    for command, expected in [("loom-nebius-ingress-v1", 0), ("loom-nebius-ingress-rollback-v1", 0),
                              ("loom-nebius-ingress-image-intent-v1", 0),
                              ("loom-nebius-ingress-dns-v1", 0), ("loom-nebius-ingress-dns-v1 example.test 8.8.8.8", 126),
                              ("loom-nebius-certificate-v1", 126), ("python -c pass", 126)]:
        result = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
                                env={"SSH_ORIGINAL_COMMAND": command}, timeout=10)
        assert result.returncode == expected, result.stderr
    supervisor = entry.parent / "scripts/ops/nebius_certificate_gateway.py"
    supervisor.write_bytes(b'print("replaced")')
    denied = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
                            env={"SSH_ORIGINAL_COMMAND": "loom-nebius-ingress-v1"}, timeout=10)
    assert denied.returncode == 126 and b"replaced" not in denied.stdout
    assert not (root.parent / "nebius-certificates").exists()


def test_preview_never_changes_keys_or_creates_authority(inputs):
    root, keys, key, content, digest = inputs
    before = keys.read_bytes()
    assert module().install(content, expected_sha256=digest, public_key=key)["status"] == "prepared"
    assert keys.read_bytes() == before and not root.exists()


@pytest.mark.parametrize("case", ["wrong_digest", "key_options", "other_authority", "symlink", "public_keys", "changed_source"])
def test_conflicting_or_untrusted_input_never_overwrites_grants(inputs, case, tmp_path):
    root, keys, key, content, digest = inputs
    if case == "wrong_digest":
        digest = "0" * 64
    elif case == "key_options":
        key = 'command="arbitrary" ' + key
    elif case == "other_authority":
        keys.write_text(keys.read_text() + key.replace(" ", "\t") + "\n")
    elif case == "symlink":
        keys.rename(tmp_path / "saved")
        keys.symlink_to(tmp_path / "saved")
    elif case == "public_keys":
        keys.chmod(0o644)
    elif case == "changed_source":
        module().install(content, expected_sha256=digest, public_key=key, apply=True)
        (root / "authority" / digest / "entrypoint.py").write_bytes(b"different")
    before = keys.read_bytes()
    with pytest.raises(module().InstallError):
        module().install(content, expected_sha256=digest, public_key=key, apply=True)
    assert keys.read_bytes() == before


def test_installer_cli_runs_without_checkout_imports(inputs, tmp_path):
    root, _keys, key, content, digest = inputs
    bundle_path, public_path = tmp_path / "approved.zip", tmp_path / "key.pub"
    bundle_path.write_bytes(content)
    public_path.write_text(key)
    for path in (bundle_path, public_path):
        path.chmod(0o600)
    script = Path(__file__).resolve().parents[2] / "scripts/ops/install_nebius_ingress_entrypoint.py"
    result = subprocess.run([sys.executable, "-I", str(script), "--bundle", str(bundle_path),
                             "--bundle-sha256", digest, "--public-key", str(public_path)],
                            capture_output=True, timeout=10, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "prepared"
    assert not root.exists()
