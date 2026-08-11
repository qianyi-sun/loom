"""Strict owner-only bearer principal registry tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_manager.auth import (
    AuthorizationError,
    CapacityPrincipalVerifier,
    PrincipalRegistryError,
)

SUBJECT_ID = UUID("00000000-0000-4000-8000-000000000101")
SUBJECT_INCARNATION = UUID("00000000-0000-4000-8000-000000000102")
REPORTER_INCARNATION = UUID("00000000-0000-4000-8000-000000000103")
EXECUTOR_INCARNATION = UUID("00000000-0000-4000-8000-000000000104")


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _operator(token: str = "operator-secret") -> dict[str, object]:
    return {
        "principal_id": "fleet-operator",
        "token_sha256": _digest(token),
        "scopes": [
            "capacity:configure:fleet",
            "capacity:configure:subject",
            "capacity:configure:activate",
            "capacity:reconcile",
            "capacity:read",
        ],
        "subject_id": None,
        "subject_incarnation": None,
        "demand_reporter_incarnation": None,
        "pool_id": None,
        "pool_reporter_incarnation": None,
    }


def _demand_reporter(token: str = "reporter-secret") -> dict[str, object]:
    return {
        "principal_id": "dev-a-reporter",
        "token_sha256": _digest(token),
        "scopes": ["capacity:report:demand"],
        "subject_id": str(SUBJECT_ID),
        "subject_incarnation": str(SUBJECT_INCARNATION),
        "demand_reporter_incarnation": str(REPORTER_INCARNATION),
        "pool_id": None,
        "pool_reporter_incarnation": None,
    }


def _write_registry(path: Path, principals: list[dict[str, object]]) -> Path:
    path.write_text(
        json.dumps({"schema_version": 1, "principals": principals}),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _pool_executor(token: str = "executor-secret") -> dict[str, object]:
    return {
        "principal_id": "oldlab-executor",
        "token_sha256": _digest(token),
        "scopes": ["capacity:execute:pool"],
        "subject_id": None,
        "subject_incarnation": None,
        "demand_reporter_incarnation": None,
        "pool_id": "oldlab",
        "pool_reporter_incarnation": None,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(EXECUTOR_INCARNATION),
    }


def test_registry_verifies_hash_without_storing_raw_token(tmp_path: Path) -> None:
    path = _write_registry(tmp_path / "principals.json", [_operator(), _demand_reporter()])
    verifier = CapacityPrincipalVerifier.from_file(path)

    operator = verifier.verify_bearer("Bearer operator-secret")
    reporter = verifier.verify_bearer("Bearer reporter-secret")

    assert operator.principal_id == "fleet-operator"
    assert "capacity:reconcile" in operator.scopes
    assert reporter.subject_id == SUBJECT_ID
    assert reporter.subject_incarnation == SUBJECT_INCARNATION
    assert reporter.demand_reporter_incarnation == REPORTER_INCARNATION
    assert "operator-secret" not in path.read_text(encoding="utf-8")


def test_registry_accepts_only_a_complete_pool_executor_binding(tmp_path: Path) -> None:
    verifier = CapacityPrincipalVerifier.from_file(
        _write_registry(tmp_path / "principals.json", [_operator(), _pool_executor()])
    )

    executor = verifier.verify_bearer("Bearer executor-secret")

    assert executor.pool_id == "oldlab"
    assert executor.executor_id == "oldlab-executor"
    assert executor.executor_incarnation == EXECUTOR_INCARNATION
    incomplete = _pool_executor()
    incomplete["executor_incarnation"] = None
    with pytest.raises(PrincipalRegistryError, match="executor binding"):
        CapacityPrincipalVerifier.from_file(
            _write_registry(tmp_path / "invalid.json", [_operator(), incomplete])
        )
    overprivileged = _pool_executor()
    overprivileged["scopes"] = [
        "capacity:execute:pool",
        "capacity:grant:manage",
    ]
    with pytest.raises(PrincipalRegistryError, match="single-purpose"):
        CapacityPrincipalVerifier.from_file(
            _write_registry(
                tmp_path / "overprivileged.json",
                [_operator(), overprivileged],
            )
        )


@pytest.mark.parametrize(
    "header",
    (None, "", "Basic abc", "Bearer", "Bearer wrong", "Bearer secret extra"),
)
def test_all_bearer_failures_have_one_generic_error(tmp_path: Path, header: str | None) -> None:
    verifier = CapacityPrincipalVerifier.from_file(
        _write_registry(tmp_path / "principals.json", [_operator()])
    )

    with pytest.raises(AuthorizationError) as error:
        verifier.verify_bearer(header)

    assert str(error.value) == "invalid capacity credentials"


def test_registry_rejects_unsafe_file_metadata(tmp_path: Path) -> None:
    unsafe = _write_registry(tmp_path / "unsafe.json", [_operator()])
    unsafe.chmod(0o640)
    with pytest.raises(PrincipalRegistryError, match="0600"):
        CapacityPrincipalVerifier.from_file(unsafe)

    target = _write_registry(tmp_path / "target.json", [_operator()])
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(PrincipalRegistryError, match="regular nonsymlink"):
        CapacityPrincipalVerifier.from_file(link)


@pytest.mark.parametrize(
    "mutation, message",
    (
        (lambda registry: registry.update({"unknown": True}), "unknown"),
        (
            lambda registry: registry["principals"].append(dict(_operator())),
            "duplicate principal",
        ),
        (
            lambda registry: registry["principals"].append(_demand_reporter("operator-secret")),
            "duplicate token",
        ),
        (
            lambda registry: registry["principals"][0]["scopes"].append("capacity:execute"),
            "scope",
        ),
    ),
)
def test_registry_rejects_unknown_and_duplicate_authority(
    tmp_path: Path,
    mutation,  # type: ignore[no-untyped-def]
    message: str,
) -> None:
    registry: dict[str, object] = {"schema_version": 1, "principals": [_operator()]}
    mutation(registry)
    path = _write_registry(tmp_path / "principals.json", registry["principals"])  # type: ignore[arg-type]
    if "unknown" in registry:
        path.write_text(json.dumps(registry), encoding="utf-8")

    with pytest.raises(PrincipalRegistryError, match=message):
        CapacityPrincipalVerifier.from_file(path)


def test_registry_requires_operator_and_complete_reporter_binding(tmp_path: Path) -> None:
    with pytest.raises(PrincipalRegistryError, match="operator"):
        CapacityPrincipalVerifier.from_file(
            _write_registry(tmp_path / "no-operator.json", [_demand_reporter()])
        )

    incomplete = _demand_reporter()
    incomplete["subject_incarnation"] = None
    with pytest.raises(PrincipalRegistryError, match="subject binding"):
        CapacityPrincipalVerifier.from_file(
            _write_registry(tmp_path / "incomplete.json", [_operator(), incomplete])
        )
