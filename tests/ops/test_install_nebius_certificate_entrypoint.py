"""The certificate identity adds no authority to existing Kubernetes SSH keys."""
from __future__ import annotations

import base64
import hashlib
import importlib
import io
import json
import os
import struct
import subprocess
import sys
import zipfile

import pytest


def module():
    return importlib.import_module("scripts.ops.install_nebius_certificate_entrypoint")


@pytest.fixture
def inputs(tmp_path):
    root = tmp_path / ".loom" / "nebius-certificates"
    root.parent.mkdir(mode=0o700)
    ssh = tmp_path / ".ssh"
    ssh.mkdir(mode=0o700)
    keys = ssh / "authorized_keys"
    keys.write_bytes(b'# preserved operator configuration\nrestrict,command="kubectl-only" ssh-ed25519 FOREIGN old\n')
    keys.chmod(0o600)
    wire = struct.pack(">I", 11) + b"ssh-ed25519" + struct.pack(">I", 32) + b"k" * 32
    key = "ssh-ed25519 " + base64.b64encode(wire).decode() + " test operator\n"
    source = b'import sys\ndef authorized_main(expected):\n    print("authorized-fixture")\n    return 0\n'
    config = {"state_dir": str(root / "state"), "installation_id": "f09f269d-3048-4cf5-af78-e30320960e4a"}
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("scripts/ops/nebius_certificate_gateway.py", source)
        archive.writestr("installation.json", json.dumps(config))
    bundle = stream.getvalue()
    return root, keys, key, bundle, hashlib.sha256(bundle).hexdigest()


def test_install_keeps_foreign_keys_and_replays_exact_immutable_entrypoint(inputs):
    root, keys, key, bundle, digest = inputs
    previous = keys.read_bytes()
    report = module().install(bundle, expected_sha256=digest, public_key=key, apply=True)
    after = keys.read_bytes()
    assert after.startswith(previous)
    added = after[len(previous):].decode()
    assert added.startswith('restrict,command="/usr/bin/python3 -I ')
    assert '" ssh-ed25519 ' in added
    assert "kubectl-only" not in added
    assert module().install(bundle, expected_sha256=digest, public_key=key, apply=True) == report
    assert keys.read_bytes() == after
    assert report["bundle_sha256"] == digest
    assert report["status"] == "installed"
    assert not (root / "state").exists()
    entry = root / "authority" / digest / "entrypoint.py"
    for command, expected in [("python3 -c pass", 126), ("loom-nebius-certificate-v1", 0)]:
        result = subprocess.run([sys.executable, "-I", str(entry)], input=bundle, capture_output=True,
                                env={"SSH_ORIGINAL_COMMAND": command}, timeout=10)
        assert result.returncode == expected
    (entry.parent / "gateway.py").write_bytes(b'print("substituted")')
    denied = subprocess.run([sys.executable, "-I", str(entry)], input=bundle, capture_output=True,
                            env={"SSH_ORIGINAL_COMMAND": "loom-nebius-certificate-v1"}, timeout=10)
    assert denied.returncode == 126 and b"substituted" not in denied.stdout


def test_dry_run_validates_without_creating_authority(inputs):
    root, keys, key, bundle, digest = inputs
    before = keys.read_bytes()
    report = module().install(bundle, expected_sha256=digest, public_key=key, apply=False)
    assert report["status"] == "prepared"
    assert not root.exists() and keys.read_bytes() == before


@pytest.mark.parametrize("case", ["wrong_digest", "key_options", "key_other_authority", "symlink", "shared_keys"])
def test_unknown_or_conflicting_authority_is_not_overwritten(inputs, case, tmp_path):
    root, keys, key, bundle, digest = inputs
    if case == "wrong_digest":
        digest = "0" * 64
    elif case == "key_options":
        key = 'command="arbitrary" ' + key
    elif case == "key_other_authority":
        keys.write_text(keys.read_text() + key)
    elif case == "symlink":
        retained = tmp_path / "retained"
        keys.rename(retained)
        keys.symlink_to(retained)
    elif case == "shared_keys":
        keys.chmod(0o644)
    before = keys.read_bytes()
    with pytest.raises(module().InstallError):
        module().install(bundle, expected_sha256=digest, public_key=key, apply=True)
    assert keys.read_bytes() == before
    assert not (root / "state").exists()


def test_incomplete_or_changed_installed_tooling_does_not_authorize_key(inputs):
    root, keys, key, bundle, digest = inputs
    module().install(bundle, expected_sha256=digest, public_key=key, apply=True)
    entry = root / "authority" / digest / "entrypoint.py"
    entry.write_bytes(b"changed")
    before = keys.read_bytes()
    with pytest.raises(module().InstallError):
        module().install(bundle, expected_sha256=digest, public_key=key, apply=True)
    assert keys.read_bytes() == before
    assert os.stat(keys).st_mode & 0o077 == 0


@pytest.mark.parametrize("separator", ["\t", "  ", " \t "])
def test_existing_same_key_with_ssh_whitespace_cannot_keep_broader_authority(inputs, separator):
    root, keys, key, bundle, digest = inputs
    fields = key.split()
    keys.write_text(keys.read_text() + fields[0] + separator + fields[1] + " unrestricted\n")
    before = keys.read_bytes()
    with pytest.raises(module().InstallError, match="different authority"):
        module().install(bundle, expected_sha256=digest, public_key=key, apply=True)
    assert keys.read_bytes() == before and not root.exists()
