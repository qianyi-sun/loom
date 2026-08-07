from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from loom.data_lifecycle_runtime import (
    LifecycleDatabaseRuntime,
    LifecycleObjectStoreRuntime,
    build_lifecycle_engine,
    build_lifecycle_object_store_client,
    load_lifecycle_database_runtime,
    load_lifecycle_object_store_runtime,
    load_lifecycle_runtime,
)

_ROOT = Path(__file__).resolve().parents[2]
_DB_URL = "postgresql+psycopg://lifecycle:db-secret@postgres:5432/loom"
_ACCESS_KEY = "lifecycle-access-key"
_SECRET_KEY = "lifecycle-secret-key"


def _runtime_environment() -> dict[str, str]:
    return {
        "LOOM_LIFECYCLE_DB_URL": _DB_URL,
        "LOOM_LIFECYCLE_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_LIFECYCLE_MINIO_ACCESS_KEY": _ACCESS_KEY,
        "LOOM_LIFECYCLE_MINIO_SECRET_KEY": _SECRET_KEY,
        "LOOM_LIFECYCLE_MINIO_REGION": "ca-central-1",
        "LOOM_LIFECYCLE_STORAGE_AUTH_KIND": "static_keys",
        # Deliberately incomplete and invalid application configuration.  The
        # lifecycle runtime must neither parse nor require it.
        "LOOM_CP_JWT_SIGNING_SECRET": "unrelated-invalid-control-plane-secret",
    }


def test_lifecycle_runtime_requires_only_dedicated_environment() -> None:
    runtime = load_lifecycle_runtime(_runtime_environment())

    assert runtime.database.url == _DB_URL
    assert runtime.object_store.endpoint_url == "http://minio:9000"
    assert runtime.object_store.auth_kind == "static_keys"
    assert runtime.object_store.access_key == _ACCESS_KEY
    assert runtime.object_store.secret_key == _SECRET_KEY
    assert runtime.object_store.region == "ca-central-1"


def test_database_only_bootstrap_does_not_require_object_store() -> None:
    database = load_lifecycle_database_runtime({"LOOM_LIFECYCLE_DB_URL": _DB_URL})

    assert database.url == _DB_URL


def test_secret_values_are_not_represented_or_reported() -> None:
    runtime = load_lifecycle_runtime(_runtime_environment())

    rendered = repr(runtime)
    assert _DB_URL not in rendered
    assert _ACCESS_KEY not in rendered
    assert _SECRET_KEY not in rendered

    with pytest.raises(RuntimeError) as exc_info:
        load_lifecycle_object_store_runtime(
            {
                "LOOM_LIFECYCLE_MINIO_ENDPOINT": "http://user:password@minio:9000",
                "LOOM_LIFECYCLE_MINIO_ACCESS_KEY": _ACCESS_KEY,
                "LOOM_LIFECYCLE_MINIO_SECRET_KEY": _SECRET_KEY,
            }
        )
    message = str(exc_info.value)
    assert "user" not in message
    assert "password" not in message
    assert _ACCESS_KEY not in message
    assert _SECRET_KEY not in message


@pytest.mark.parametrize(
    "missing",
    [
        "LOOM_LIFECYCLE_DB_URL",
        "LOOM_LIFECYCLE_MINIO_ENDPOINT",
        "LOOM_LIFECYCLE_MINIO_ACCESS_KEY",
        "LOOM_LIFECYCLE_MINIO_SECRET_KEY",
    ],
)
def test_missing_required_lifecycle_authority_fails_closed(missing: str) -> None:
    environment = _runtime_environment()
    del environment[missing]

    with pytest.raises(RuntimeError, match="lifecycle"):
        load_lifecycle_runtime(environment)


def test_irsa_runtime_does_not_require_static_keys() -> None:
    runtime = load_lifecycle_object_store_runtime(
        {
            "LOOM_LIFECYCLE_MINIO_ENDPOINT": "https://s3.ca-central-1.amazonaws.com",
            "LOOM_LIFECYCLE_STORAGE_AUTH_KIND": "irsa",
        }
    )

    assert runtime.auth_kind == "irsa"
    assert runtime.access_key is None
    assert runtime.secret_key is None


def test_runtime_builders_forward_only_the_dedicated_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}
    engine_sentinel = object()
    client_sentinel = object()

    def fake_create_engine(url: str) -> object:
        observed["database_url"] = url
        return engine_sentinel

    def fake_build_s3_client(**kwargs: object) -> object:
        observed["object_store"] = kwargs
        return client_sentinel

    monkeypatch.setattr("loom.data_lifecycle_runtime.create_engine", fake_create_engine)
    monkeypatch.setattr("loom.data_lifecycle_runtime.build_s3_client", fake_build_s3_client)

    assert build_lifecycle_engine(LifecycleDatabaseRuntime(url=_DB_URL)) is engine_sentinel
    assert (
        build_lifecycle_object_store_client(
            LifecycleObjectStoreRuntime(
                endpoint_url="http://minio:9000",
                auth_kind="static_keys",
                access_key=_ACCESS_KEY,
                secret_key=_SECRET_KEY,
                region="ca-central-1",
            )
        )
        is client_sentinel
    )
    assert observed == {
        "database_url": _DB_URL,
        "object_store": {
            "endpoint_url": "http://minio:9000",
            "auth_kind": "static_keys",
            "access_key": _ACCESS_KEY,
            "secret_key": _SECRET_KEY,
            "region": "ca-central-1",
        },
    }


def test_all_lifecycle_entry_points_share_one_runtime_contract() -> None:
    paths = [
        _ROOT / "src/loom/data_lifecycle_maintenance.py",
        _ROOT / "scripts/ops/staging_data_lifecycle_bootstrap.py",
        _ROOT / "scripts/ops/staging_data_lifecycle_capacity.py",
        _ROOT / "scripts/ops/staging_data_lifecycle_classify.py",
        _ROOT / "scripts/ops/staging_data_lifecycle_dirty_epoch_reconcile.py",
        _ROOT / "scripts/ops/staging_data_lifecycle_gc.py",
        _ROOT / "scripts/ops/staging_data_lifecycle_prepare.py",
    ]

    for path in paths:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        assert "loom.data_lifecycle_runtime" in imports, path
        assert "loom_control_plane.config" not in imports, path
        assert "ControlPlaneSettings" not in source, path
