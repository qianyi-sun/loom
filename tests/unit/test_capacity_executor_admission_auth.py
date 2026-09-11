"""Build admission uses controller-only credentials, never manager operators."""

import pytest

from loom_capacity_manager.auth import (
    AuthorizationError,
    CapacityPrincipalVerifier,
    PrincipalRegistryError,
)
from tests.unit.test_capacity_auth import (
    EXECUTOR_INCARNATION,
    _demand_reporter,
    _operator,
    _pool_executor,
    _write_registry,
)


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
def test_executor_only_registry_preserves_full_pool_identity(tmp_path, pool):
    executor = _pool_executor()
    executor.update(pool_id=pool, executor_id=f"{pool}-executor")
    path = _write_registry(tmp_path / "build-admission.json", [executor])
    verifier = CapacityPrincipalVerifier.from_pool_executor_file(path)
    principal = verifier.verify_bearer("Bearer executor-secret")
    assert principal.scopes == frozenset({"capacity:execute:pool"})
    assert principal.matches_executor(pool_id=pool, executor_id=f"{pool}-executor",
        executor_incarnation=EXECUTOR_INCARNATION, pool_generation=1)
    assert not principal.matches_executor(pool_id=pool, executor_id=f"{pool}-executor",
        executor_incarnation=EXECUTOR_INCARNATION, pool_generation=2)
    with pytest.raises(AuthorizationError):
        verifier.verify_bearer("Bearer operator-secret")
    # Manager configuration still requires its operator; do not weaken its loader.
    with pytest.raises(PrincipalRegistryError, match="operator"):
        CapacityPrincipalVerifier.from_file(path)


@pytest.mark.parametrize("foreign", [_operator(), _demand_reporter()])
def test_executor_only_registry_rejects_other_authority(tmp_path, foreign):
    path = _write_registry(tmp_path / "mixed.json", [_pool_executor(), foreign])
    with pytest.raises(PrincipalRegistryError, match="executor-only"):
        CapacityPrincipalVerifier.from_pool_executor_file(path)


@pytest.mark.parametrize("boundary", ["duplicate-id", "duplicate-token", "mode", "symlink", "incomplete", "extra-scope"])
def test_executor_only_registry_retains_file_and_identity_safety(tmp_path, boundary):
    first, second = _pool_executor(), _pool_executor("second-secret")
    second["principal_id"] = "second-executor"
    if boundary=="duplicate-id":
        second["principal_id"] = first["principal_id"]
    elif boundary=="duplicate-token":
        second["token_sha256"] = first["token_sha256"]
    elif boundary=="incomplete":
        second["executor_incarnation"] = None
    elif boundary=="extra-scope":
        second["scopes"] = ["capacity:execute:pool", "capacity:read"]
    path = _write_registry(tmp_path / "executors.json", [first, second])
    if boundary=="mode":
        path.chmod(0o644)
    elif boundary=="symlink":
        link = tmp_path / "linked.json"
        link.symlink_to(path)
        path = link
    with pytest.raises(PrincipalRegistryError):
        CapacityPrincipalVerifier.from_pool_executor_file(path)


def test_executor_registry_pins_the_same_bytes_it_parses(tmp_path):
    from hashlib import sha256

    path = _write_registry(tmp_path/"pinned.json",[_pool_executor()])
    digest = sha256(path.read_bytes()).hexdigest()
    verifier = CapacityPrincipalVerifier.from_pool_executor_file(path,expected_sha256=digest)
    assert verifier.verify_bearer("Bearer executor-secret").executor_incarnation == EXECUTOR_INCARNATION
    with pytest.raises(PrincipalRegistryError,match="digest"):
        CapacityPrincipalVerifier.from_pool_executor_file(path,expected_sha256="0"*64)
