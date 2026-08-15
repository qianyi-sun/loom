"""Digest-pinned owner-policy loading for executable preparation."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.api import create_app
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.capacity_execution_fixtures import execution_policy


def _policy_module():  # type: ignore[no-untyped-def]
    return importlib.import_module("loom_capacity_manager.execution_policy")


def _write_policy(path: Path) -> tuple[Path, str]:
    payload = canonical_executable_bytes(execution_policy())
    path.write_bytes(payload)
    path.chmod(0o600)
    return path, hashlib.sha256(payload).hexdigest()


def _settings(**changes: object) -> CapacityManagerSettings:
    values: dict[str, object] = {
        "principals_file": Path("/run/loom/principals.json"),
        "db_url_file": Path("/run/loom/database-url"),
        "expected_authority_incarnation": UUID("11111111-1111-4111-8111-111111111111"),
        "tls_cert_file": Path("/run/loom/server.pem"),
        "tls_key_file": Path("/run/loom/server-key.pem"),
        "tls_client_ca_file": Path("/run/loom/client-ca.pem"),
    }
    values.update(changes)
    return CapacityManagerSettings(**values)  # type: ignore[arg-type]


def test_exact_owner_policy_round_trips_from_a_digest_pinned_file(tmp_path: Path) -> None:
    module = _policy_module()
    path, digest = _write_policy(tmp_path / "execution-policy.json")

    loaded = module.load_execution_preparation_policy(path, digest)

    assert loaded == execution_policy()


@pytest.mark.parametrize(
    "digest",
    (
        "",
        "a" * 63,
        "A" * 64,
        "g" * 64,
        "0" * 64,
    ),
)
def test_policy_loader_rejects_noncanonical_expected_digest(
    tmp_path: Path,
    digest: str,
) -> None:
    module = _policy_module()
    path, _ = _write_policy(tmp_path / "execution-policy.json")

    with pytest.raises(module.ExecutionPolicyError) as caught:
        module.load_execution_preparation_policy(path, digest)

    assert str(caught.value) == "execution preparation policy is invalid"


@pytest.mark.parametrize("digest", (None, b"a" * 64, 7))
def test_policy_loader_maps_non_string_digest_to_generic_error(
    tmp_path: Path,
    digest: object,
) -> None:
    module = _policy_module()
    path, _ = _write_policy(tmp_path / "execution-policy.json")

    with pytest.raises(module.ExecutionPolicyError) as caught:
        module.load_execution_preparation_policy(path, digest)

    assert str(caught.value) == "execution preparation policy is invalid"


def test_policy_loader_rejects_digest_mismatch_without_echoing_input(tmp_path: Path) -> None:
    module = _policy_module()
    path, _ = _write_policy(tmp_path / "sensitive-policy-name.json")

    with pytest.raises(module.ExecutionPolicyError) as caught:
        module.load_execution_preparation_policy(path, "f" * 64)

    assert str(caught.value) == "execution preparation policy is invalid"
    assert str(path) not in str(caught.value)


def test_policy_loader_rejects_symlink_and_group_writable_file(tmp_path: Path) -> None:
    module = _policy_module()
    target, digest = _write_policy(tmp_path / "target.json")
    link = tmp_path / "policy.json"
    link.symlink_to(target)

    with pytest.raises(module.ExecutionPolicyError):
        module.load_execution_preparation_policy(link, digest)

    target.chmod(0o620)
    with pytest.raises(module.ExecutionPolicyError):
        module.load_execution_preparation_policy(target, digest)


def test_policy_loader_opens_fifo_nonblocking_and_rejects_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _policy_module()
    path = tmp_path / "execution-policy.fifo"
    os.mkfifo(path, mode=0o600)
    real_open = os.open

    def open_nonblocking(candidate: Path, flags: int) -> int:
        assert flags & os.O_NONBLOCK
        return real_open(candidate, flags)

    monkeypatch.setattr(module.os, "open", open_nonblocking)

    with pytest.raises(module.ExecutionPolicyError) as caught:
        module.load_execution_preparation_policy(path, "a" * 64)

    assert str(caught.value) == "execution preparation policy is invalid"


def test_policy_loader_rejects_oversized_or_malformed_content(tmp_path: Path) -> None:
    module = _policy_module()
    path = tmp_path / "policy.json"
    path.write_bytes(b"x" * (module.MAX_EXECUTION_POLICY_BYTES + 1))
    path.chmod(0o600)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(module.ExecutionPolicyError):
        module.load_execution_preparation_policy(path, digest)

    path.write_bytes(b"{not-json}")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(module.ExecutionPolicyError):
        module.load_execution_preparation_policy(path, digest)


def test_policy_loader_rejects_file_metadata_change_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _policy_module()
    path, digest = _write_policy(tmp_path / "execution-policy.json")
    real_fstat = os.fstat
    calls = 0

    def changed_fstat(descriptor: int):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        result = real_fstat(descriptor)
        if calls == 1:
            return result
        values = list(result)
        values[6] = result.st_size + 1
        return os.stat_result(values)

    monkeypatch.setattr(module.os, "fstat", changed_fstat)

    with pytest.raises(module.ExecutionPolicyError):
        module.load_execution_preparation_policy(path, digest)


def test_execution_policy_settings_must_be_configured_as_an_exact_pair() -> None:
    disabled = _settings()
    enabled = _settings(
        execution_policy_file=Path("/etc/loom/execution-policy.json"),
        execution_policy_sha256="a" * 64,
    )

    assert disabled.execution_policy_file is None
    assert disabled.execution_policy_sha256 is None
    assert enabled.execution_policy_file == Path("/etc/loom/execution-policy.json")
    assert enabled.execution_policy_sha256 == "a" * 64

    with pytest.raises(ValidationError):
        _settings(execution_policy_file=Path("/etc/loom/execution-policy.json"))
    with pytest.raises(ValidationError):
        _settings(execution_policy_sha256="a" * 64)
    with pytest.raises(ValidationError):
        _settings(
            execution_policy_file=Path("/etc/loom/execution-policy.json"),
            execution_policy_sha256="0" * 64,
        )


def test_default_manager_store_loads_policy_before_serving_requests(tmp_path: Path) -> None:
    module = _policy_module()
    registry = tmp_path / "principals.json"
    registry.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    {
                        "principal_id": "operator",
                        "token_sha256": hashlib.sha256(b"token").hexdigest(),
                        "scopes": ["capacity:reconcile"],
                        "subject_id": None,
                        "subject_incarnation": None,
                        "demand_reporter_incarnation": None,
                        "pool_id": None,
                        "pool_reporter_incarnation": None,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    registry.chmod(0o600)
    policy_file = tmp_path / "execution-policy.json"
    policy_file.write_bytes(b"{malformed}")
    policy_file.chmod(0o600)
    settings = _settings(
        principals_file=registry,
        execution_policy_file=policy_file,
        execution_policy_sha256=hashlib.sha256(policy_file.read_bytes()).hexdigest(),
    )

    with pytest.raises(module.ExecutionPolicyError):
        create_app(
            settings,
            verifier=CapacityPrincipalVerifier.from_file(registry),
        )
