"""Management build admission starts only with complete private pinned inputs."""

import json
from hashlib import sha256
from importlib import import_module
from types import SimpleNamespace

import pytest

from tests.unit.test_capacity_agent_client import _owner_file
from tests.unit.test_capacity_auth import _pool_executor, _write_registry


async def test_unconfigured_build_admission_has_no_runtime():
    module = import_module("loom_service.personal_dev_build_admission")
    settings = SimpleNamespace(
        personal_dev_build_admission_config_file=None, personal_dev_build_admission_config_sha256=""
    )
    assert await module.build_personal_build_admission_runtime(settings) is None


def inputs(tmp_path):
    principals = _write_registry(tmp_path / "principals.json", [_pool_executor()])
    database = _owner_file(
        tmp_path / "database-url",
        b"postgresql+psycopg://build-agent:private-password@database.test/management?sslmode=verify-full",
    )
    document = {
        "schema_version": 1,
        "mode": "prepare-bind-only",
        "database_url_file": str(database),
        "database_url_sha256": sha256(database.read_bytes()).hexdigest(),
        "principals_file": str(principals),
        "principals_sha256": sha256(principals.read_bytes()).hexdigest(),
    }
    return document, database, principals


def settings(tmp_path, document):
    wire = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )
    config = _owner_file(tmp_path / "admission.json", wire)
    return SimpleNamespace(
        personal_dev_build_admission_config_file=config,
        personal_dev_build_admission_config_sha256=sha256(wire).hexdigest(),
    )


@pytest.mark.parametrize(
    "boundary",
    [
        "missing-config",
        "missing-hash",
        "config-hash",
        "config-mode",
        "symlink",
        "database-hash",
        "database-mode",
        "plaintext-database",
        "principals-hash",
        "operator",
        "mode",
        "unknown-field",
    ],
)
async def test_invalid_private_runtime_inputs_never_open_database(tmp_path, monkeypatch, boundary):
    module = import_module("loom_service.personal_dev_build_admission")
    document, database, principals = inputs(tmp_path)
    if boundary == "database-hash":
        document["database_url_sha256"] = "f" * 64
    elif boundary == "database-mode":
        database.chmod(0o644)
    elif boundary == "plaintext-database":
        database.write_bytes(
            b"postgresql+psycopg://build-agent:private-password@database.test/management?sslmode=disable"
        )
        document["database_url_sha256"] = sha256(database.read_bytes()).hexdigest()
    elif boundary == "principals-hash":
        document["principals_sha256"] = "f" * 64
    elif boundary == "operator":
        from tests.unit.test_capacity_auth import _operator

        _write_registry(principals, [_operator(), _pool_executor()])
        document["principals_sha256"] = sha256(principals.read_bytes()).hexdigest()
    elif boundary == "mode":
        document["mode"] = "ready"
    elif boundary == "unknown-field":
        document["enable_execution"] = True
    configured = settings(tmp_path, document)
    if boundary == "missing-config":
        configured.personal_dev_build_admission_config_file = None
    elif boundary == "missing-hash":
        configured.personal_dev_build_admission_config_sha256 = ""
    elif boundary == "config-hash":
        configured.personal_dev_build_admission_config_sha256 = "f" * 64
    elif boundary == "config-mode":
        configured.personal_dev_build_admission_config_file.chmod(0o644)
    elif boundary == "symlink":
        link = tmp_path / "linked.json"
        link.symlink_to(configured.personal_dev_build_admission_config_file)
        configured.personal_dev_build_admission_config_file = link

    def unexpected(*args, **kwargs):
        pytest.fail("invalid private inputs must fail before opening database resources")

    monkeypatch.setattr(module, "create_async_engine", unexpected)
    with pytest.raises((RuntimeError, ValueError, OSError)) as failure:
        await module.build_personal_build_admission_runtime(configured)
    assert "private-password" not in str(failure.value)
