"""A root-installed policy, never a packet-selected key, scopes preparation."""

import hashlib
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from tests.unit.test_native_slurm_allocation import _fixture

_DOMAIN = b"loom.native-worker-cgroup-preparation-signature/v1\0"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def signed(key, payload):
    value = {"schema": "loom.native-worker-cgroup-envelope/v1", "key_id": "test-issuer", "payload": payload}
    return canonical(value | {"signature_hex": key.sign(_DOMAIN + canonical(value)).hex()})


@pytest.fixture
def preparation(tmp_path):
    module = import_module("loom_capacity_executor.native_containment_protocol")
    key = Ed25519PrivateKey.generate()
    fake, request, scheduler = _fixture(tmp_path)
    now = time.time_ns() // 1_000_000
    policy = {
        "schema": "loom.native-worker-cgroup-policy/v1", "environment": "staging",
        "pool_id": "oldlab", "pool_generation": 1,
        "node": request.nodes[0], "cluster": request.cluster,
        "submitter": request.submitter, "uid": fake.backend().authority.local_uid,
        "account": request.account, "partition": request.partition, "qos": request.qos,
        "authority_incarnation": str(UUID(int=11)), "execution_epoch": 1,
        "execution_manifest_sha256": "1" * 64, "executor_id": "oldlab-executor",
        "executor_incarnation": str(UUID(int=12)), "controller_authority_sha256": "2" * 64,
        "trusted_fleet_release_sha256": "3" * 64,
        "issuer_key_id": "test-issuer", "issuer_public_key_hex": key.public_key().public_bytes_raw().hex(),
        "not_before_ms": now - 60_000, "expires_at_ms": now + 60_000,
        "profiles": [{"profile_digest": "4" * 64, "pids_max": 4096}],
    }
    submit_ms = (now // 1000 - 30) * 1000
    start_ms = (now // 1000 - 15) * 1000
    scheduler["jobs"][0]["submit_time"]["number"] = submit_ms // 1000
    scheduler["jobs"][0]["start_time"]["number"] = start_ms // 1000
    payload = {
        "schema": "loom.native-worker-cgroup-preparation/v1", "purpose": "prepare-application-worker-cgroup",
        "policy_sha256": hashlib.sha256(canonical(policy)).hexdigest(),
        "grant_id": str(UUID(int=13)), "grant_generation": 1, "intent_id": str(UUID(int=14)),
        "binding_sha256": "5" * 64, "physical_binding_sha256": "6" * 64,
        "bootstrap_registration_epoch": 1, "bootstrap_sha256": "7" * 64,
        "agent_incarnation": str(UUID(int=15)), "ownership_evidence_sha256": "8" * 64,
        "manager_observation_sha256": "9" * 64, "bootstrap_observation_sha256": "a" * 64,
        "scheduler_observation_sha256": "b" * 64,
        "profile_digest": "4" * 64, "pids_max": 4096,
        "job_id": "101", "ownership_token": request.ownership_token,
        "cpus": request.cpus, "memory_bytes": request.memory_bytes,
        "submitted_at_ms": submit_ms, "started_at_ms": start_ms,
        "issued_at_ms": now - 100, "expires_at_ms": now + 9000,
    }
    return module, key, policy, payload, json.dumps(scheduler), datetime.now(UTC)


def verify(preparation, *, packet=None, policy=None, scheduler=None):
    module, key, trusted, payload, raw, observed = preparation
    return module.verify_native_preparation(
        signed_packet=signed(key, payload) if packet is None else packet,
        root_policy=canonical(trusted) if policy is None else policy,
        scheduler_raw=raw if scheduler is None else scheduler, scheduler_observed_at=observed,
        openssl_path="/usr/bin/openssl", openssl_sha256=hashlib.sha256(Path("/usr/bin/openssl").read_bytes()).hexdigest(),
    )


def test_native_preparation_matches_independent_policy_and_scheduler(preparation):
    assert verify(preparation) == preparation[3]


@pytest.mark.parametrize("changed", (
    "purpose", "schema", "policy", "profile", "pids", "uid", "node", "key", "signature",
    "future", "expired", "overlong", "epoch-alias", "zero-intent", "job", "start", "extra",
    "duplicate", "noncanonical", "oversize", "wrong-domain",
))
def test_native_preparation_rejects_scope_replay_and_wire_substitution(preparation, changed):
    module, key, policy, payload, _scheduler, _observed = preparation
    packet = None
    if changed in {"uid", "node", "key"}:
        if changed == "uid":
            policy["uid"] += 1
        elif changed == "node":
            policy["node"] = "foreign-node"
        else:
            policy["issuer_public_key_hex"] = Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex()
        payload["policy_sha256"] = hashlib.sha256(canonical(policy)).hexdigest()
    elif changed == "purpose":
        payload["purpose"] = "start-trial"
    elif changed == "schema":
        payload["schema"] = "loom.native-worker-cgroup-preparation/v0"
    elif changed == "policy":
        payload["policy_sha256"] = "c" * 64
    elif changed == "profile":
        payload["profile_digest"] = "c" * 64
    elif changed == "pids":
        payload["pids_max"] += 1
    elif changed == "future":
        payload["issued_at_ms"] += 60_000
        payload["expires_at_ms"] += 60_000
    elif changed == "expired":
        payload["issued_at_ms"] -= 60_000
        payload["expires_at_ms"] -= 60_000
    elif changed == "overlong":
        payload["expires_at_ms"] = payload["issued_at_ms"] + 10_001
    elif changed == "epoch-alias":
        payload["bootstrap_registration_epoch"] = True
    elif changed == "zero-intent":
        payload["intent_id"] = str(UUID(int=0))
    elif changed == "job":
        payload["job_id"] = "102"
    elif changed == "start":
        payload["started_at_ms"] -= 1000
    elif changed == "extra":
        payload["cgroup_path"] = "/sys/fs/cgroup/foreign"
    else:
        packet = signed(key, payload)
        if changed == "signature":
            value = json.loads(packet)
            value["signature_hex"] = "00" * 64
            packet = canonical(value)
        elif changed == "wrong-domain":
            value = json.loads(packet)
            value.pop("signature_hex")
            packet = canonical(value | {"signature_hex": key.sign(canonical(value)).hex()})
        elif changed == "duplicate":
            packet = packet.replace(b'"job_id":"101"', b'"job_id":"102","job_id":"101"')
        elif changed == "noncanonical":
            packet = b" " + packet
        elif changed == "oversize":
            packet += b" " * 32769
    with pytest.raises(module.NativeContainmentVerificationError):
        verify(preparation, packet=packet)


@pytest.mark.parametrize("changed", ("extra", "epoch-alias", "duplicate-profile", "zero-pids", "expires", "unknown-pool"))
def test_root_policy_is_closed_and_strict(preparation, changed):
    module, _key, policy, payload, *_ = preparation
    if changed == "extra":
        policy["executable"] = "/untrusted/openssl"
    elif changed == "epoch-alias":
        policy["execution_epoch"] = True
    elif changed == "duplicate-profile":
        policy["profiles"] *= 2
    elif changed == "zero-pids":
        policy["profiles"][0]["pids_max"] = 0
    elif changed == "expires":
        policy["expires_at_ms"] = payload["expires_at_ms"] - 1
    else:
        policy["pool_id"] = "foreign"
    payload["policy_sha256"] = hashlib.sha256(canonical(policy)).hexdigest()
    with pytest.raises(module.NativeContainmentVerificationError):
        verify(preparation)


def test_native_preparation_rechecks_expiry_after_crypto(preparation, monkeypatch):
    module, _key, _policy, payload, *_ = preparation
    original = module.verify_native_ed25519

    def delayed(**kwargs):
        original(**kwargs)
        monkeypatch.setattr(module.time, "time_ns", lambda: payload["expires_at_ms"] * 1_000_000)

    monkeypatch.setattr(module, "verify_native_ed25519", delayed)
    with pytest.raises(module.NativeContainmentVerificationError, match="expired"):
        verify(preparation)


def test_expired_preparation_is_rejected_before_starting_crypto(preparation, monkeypatch):
    module, _key, _policy, payload, *_ = preparation
    monkeypatch.setattr(module.time, "time_ns", lambda: payload["expires_at_ms"] * 1_000_000)

    def should_not_start(**kwargs):
        pytest.fail("expired requests must not spend privileged verifier capacity")

    monkeypatch.setattr(module, "verify_native_ed25519", should_not_start)
    with pytest.raises(module.NativeContainmentVerificationError, match="expired"):
        verify(preparation)


@pytest.mark.parametrize("field,replacement", (
    ("environment", "production"), ("pool_generation", 2),
    ("authority_incarnation", str(UUID(int=81))), ("executor_incarnation", str(UUID(int=82))),
    ("execution_epoch", 2), ("trusted_fleet_release_sha256", "f" * 64),
))
def test_signed_preparation_cannot_move_to_another_root_policy(preparation, field, replacement):
    module, key, policy, payload, *_ = preparation
    packet = signed(key, payload)
    policy[field] = replacement
    with pytest.raises(module.NativeContainmentVerificationError, match="policy differs"):
        verify(preparation, packet=packet)


@pytest.mark.parametrize("delta_ms", (-10_001, 1000))
def test_scheduler_clock_must_be_current_before_crypto(preparation, monkeypatch, delta_ms):
    from datetime import timedelta

    module, *_ = preparation
    observed = preparation[-1]
    monkeypatch.setattr(module.time, "time_ns", lambda: int(observed.timestamp() * 1000) * 1_000_000)
    changed = (*preparation[:-1], observed + timedelta(milliseconds=delta_ms))

    def should_not_start(**kwargs):
        pytest.fail("invalid observation clocks must not start verification")

    monkeypatch.setattr(module, "verify_native_ed25519", should_not_start)
    with pytest.raises(module.NativeContainmentVerificationError, match="expired"):
        verify(changed)


def test_preparation_rejects_clock_rollback_during_crypto(preparation, monkeypatch):
    module, _key, _policy, payload, *_ = preparation
    original = module.verify_native_ed25519
    initial_ns = (payload["issued_at_ms"] + 200) * 1_000_000
    monkeypatch.setattr(module.time, "time_ns", lambda: initial_ns)

    def delayed(**kwargs):
        original(**kwargs)
        monkeypatch.setattr(module.time, "time_ns", lambda: initial_ns - 1_000_000)

    monkeypatch.setattr(module, "verify_native_ed25519", delayed)
    with pytest.raises(module.NativeContainmentVerificationError, match="during verification"):
        verify(preparation)


def test_complete_native_preparation_verifier_is_stdlib_only(preparation):
    module, key, policy, payload, scheduler, observed = preparation
    probe = """
import json, runpy, sys
from datetime import datetime
namespace = runpy.run_path(sys.argv[1])
value = json.load(sys.stdin)
for key in ('signed_packet', 'root_policy'):
    value[key] = value[key].encode('ascii')
value['scheduler_observed_at'] = datetime.fromisoformat(value['scheduler_observed_at'])
print(json.dumps(namespace['verify_native_preparation'](**value), sort_keys=True))
"""
    data = {
        "signed_packet": signed(key, payload).decode("ascii"), "root_policy": canonical(policy).decode("ascii"),
        "scheduler_raw": scheduler, "scheduler_observed_at": observed.isoformat(),
        "openssl_path": "/usr/bin/openssl", "openssl_sha256": hashlib.sha256(Path("/usr/bin/openssl").read_bytes()).hexdigest(),
    }
    result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", probe, module.__file__],
        input=json.dumps(data), capture_output=True, text=True, check=False, timeout=5)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == payload
