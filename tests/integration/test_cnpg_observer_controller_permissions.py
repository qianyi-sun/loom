"""Real root/service ownership inside Docker, without host or remote authority."""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

import docker
import pytest

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]

_DOCKERFILE = b"""FROM python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7
RUN apt-get update -qq && apt-get install -y --no-install-recommends openssh-client && rm -rf /var/lib/apt/lists/*
"""

_PROBE = r'''
import base64, hashlib, importlib.util, json, os, stat, subprocess
from pathlib import Path
spec = importlib.util.spec_from_file_location("observer", "/source/scripts/ops/staging_cnpg_observer_controller.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
root = Path("/case")
root.mkdir(mode=0o755)
config = root / "etc"
config.mkdir(mode=0o755)
service = root / "service"
service.mkdir(mode=0o700)
os.chown(service, 1234, 1234)
m.STATE = root / "state"
m.CONFIG = config / "config"
m.KNOWN_HOSTS = config / "known_hosts"
m.PUBLIC_KEY = config / "key.pub"
m.IDENTITY = service / "key"
# Source admission is tested separately; this disposable check isolates actual
# filesystem ownership. No installer source or host permission is admitted here.
m._require_authority = lambda inventory: (1234, 1234, "a" * 40)
nodes = []
for n in (3, 4, 5):
    blob = b"\0\0\0\x0bssh-ed25519\0\0\0\x20" + bytes([n]) * 32
    nodes.append({"node": f"trt-eai-oldlab-{n}", "address": f"192.168.50.{n}",
                  "port": 22, "host_key": "ssh-ed25519 " + base64.b64encode(blob).decode()})
inventory = root / "inventory.json"
inventory.write_bytes(m._json({"schema_version": 1, "nodes": nodes}))
inventory.chmod(0o600)
args = {"inventory_file": inventory, "inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest()}
result = m.prepare_controller(**args)
assert m.prepare_controller(**args) == result
assert m.IDENTITY.stat().st_uid == 1234 and m.IDENTITY.stat().st_gid == 1234
assert stat.S_IMODE(m.IDENTITY.stat().st_mode) == 0o600
for path in (m.CONFIG, m.KNOWN_HOSTS, m.PUBLIC_KEY):
    assert path.stat().st_uid == path.stat().st_gid == 0
    assert stat.S_IMODE(path.stat().st_mode) == 0o444
child = subprocess.run(["/usr/local/bin/python", "-I", "-c", """
from pathlib import Path
assert b'OPENSSH PRIVATE KEY' in Path('/case/service/key').read_bytes()
for path, operation in [('/case/state/identity', 'read'), ('/case/etc/config', 'write')]:
    try:
        if operation == 'read': Path(path).read_bytes()
        else: Path(path).write_text('changed')
    except PermissionError: pass
    else: raise AssertionError('service crossed root authority boundary')
print('service-isolated')
"""], user=1234, group=1234, extra_groups=(), capture_output=True, text=True, check=True)
assert child.stdout.strip() == 'service-isolated'
foreign = root / "foreign"
foreign.write_bytes(b"unchanged")
m.IDENTITY.unlink()
m.IDENTITY.symlink_to(foreign)
try: m.prepare_controller(**args)
except (ValueError, OSError): pass
else: raise AssertionError('adopted symlink')
assert foreign.read_bytes() == b"unchanged"
print('real-root-service-boundaries-passed')
'''


def test_observer_preparation_enforces_real_root_and_service_ownership() -> None:
    repository = Path(__file__).resolve().parents[2]
    client = docker.from_env()
    try:
        image, _ = client.images.build(
            fileobj=io.BytesIO(_DOCKERFILE),
            tag="loom-test-cnpg-observer:" + hashlib.sha256(_DOCKERFILE).hexdigest()[:16],
            rm=True,
        )
        output = client.containers.run(
            image.id,
            ["python", "-I", "-B", "-c", _PROBE],
            volumes={str(repository): {"bind": "/source", "mode": "ro"}},
            network_disabled=True,
            remove=True,
        )
        assert output.strip() == b"real-root-service-boundaries-passed"
    finally:
        client.close()
