from __future__ import annotations

import asyncio
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

SOURCE = {
    "service_execution_source_endpoint": "https://spool.example",
    "service_execution_source_access_key": "spool-access",
    "service_execution_source_secret_key": "spool-secret",
    "service_execution_source_region": "eu-north1",
    "service_execution_source_bucket": "nebius-spool",
}


def _settings(service: str, **values: object) -> object:
    module = import_module(f"loom_{service}.config")
    cls = module.ControlPlaneSettings if service == "control_plane" else module.GatewaySettings
    return cls(
        _env_file=None,
        db_url="postgresql+psycopg://loom:loom@example/loom",
        minio_endpoint="https://canonical.example",
        minio_access_key="canonical-access",
        minio_secret_key="canonical-secret",
        artifacts_bucket="canonical-artifacts",
        **(
            {"step_jwt_signing_key": "test-step-jwt-signing-key"}
            if service == "control_plane"
            else {}
        ),
        **values,
    )


@pytest.mark.parametrize("service", ["control_plane", "llm_gateway"])
@pytest.mark.parametrize("field", list(SOURCE))
def test_partial_spool_config_fails_before_startup(service: str, field: str) -> None:
    app_module = import_module(f"loom_{service}.app")
    with pytest.raises(ValueError, match="service_execution_source"):
        app_module.create_app(_settings(service, **{field: SOURCE[field]}))


@pytest.mark.parametrize("service", ["control_plane", "llm_gateway"])
@pytest.mark.parametrize("field", list(SOURCE))
@pytest.mark.parametrize("value", [None, ""])
def test_incomplete_spool_never_borrows_canonical_credentials(
    service: str,
    field: str,
    value: str | None,
) -> None:
    app_module = import_module(f"loom_{service}.app")
    settings = _settings(service, **(SOURCE | {field: value}))
    with pytest.raises(ValueError, match="service_execution_source"):
        app_module.create_app(settings)


@pytest.mark.parametrize("service", ["control_plane", "llm_gateway"])
@pytest.mark.parametrize("separate", [False, True])
def test_app_keeps_canonical_inputs_and_selects_output_source(
    monkeypatch: pytest.MonkeyPatch,
    service: str,
    separate: bool,
) -> None:
    app_module = import_module(f"loom_{service}.app")
    stores: list[SimpleNamespace] = []
    materializers: list[dict[str, object]] = []

    async def noop(*_args: object, **_kwargs: object) -> int:
        return 0

    async def background(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    def store(**kwargs: object) -> SimpleNamespace:
        result = SimpleNamespace(**kwargs)
        stores.append(result)
        return result

    monkeypatch.setattr(
        app_module, "create_async_engine", lambda *_a, **_kw: SimpleNamespace(dispose=noop)
    )
    monkeypatch.setattr(app_module, "_assert_schema_startup", noop)
    monkeypatch.setattr(app_module, "MinioObjectStore", store)
    if service == "control_plane":
        monkeypatch.setattr(app_module, "build_s3_client", lambda **_kw: object())
        for name in (
            "run_crash_detector_loop",
            "run_metrics_refresher_loop",
            "run_retry_exhausted_sweeper_loop",
            "run_service_execution_materializer_loop",
        ):
            monkeypatch.setattr(app_module, name, background)

        def materializer(**kwargs: object) -> object:
            materializers.append(kwargs)
            return object()

        monkeypatch.setattr(app_module, "ServiceExecutionMaterializer", materializer)
    else:
        monkeypatch.setattr(app_module, "_assert_secret_store_startup", noop)

    app = app_module.create_app(_settings(service, **(SOURCE if separate else {})))
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        canonical = stores[0]
        assert canonical.endpoint_url == "https://canonical.example"
        if service == "control_plane":
            wiring = materializers[0]
            assert wiring["canonical_store"] is canonical
            assert wiring["artifacts_bucket"] == "canonical-artifacts"
            assert app.state.artifact_read_service._store is canonical
            output_store = wiring["source_store"]
            output_bucket = wiring["source_bucket"]
        else:
            assert app.state.artifact_store is canonical
            output = app.state.service_execution_output_service._service
            output_store = output._store
            output_bucket = output._bucket
            assert output._repository._store is output_store
            assert output._repository._bucket == output_bucket
        if separate:
            assert output_store is not canonical
            assert output_store.endpoint_url == "https://spool.example"
            assert output_store.access_key == "spool-access"
            assert output_store.secret_key == "spool-secret"
            assert output_store.region == "eu-north1"
            assert output_bucket == "nebius-spool"
        else:
            assert output_store is canonical
            assert output_bucket == "canonical-artifacts"
    assert len(stores) == (2 if separate else 1)


def test_source_spool_file_credentials(tmp_path: Path) -> None:
    from loom.trajectory.source_spool import ServiceExecutionSourceConfig

    access = tmp_path / "access"
    secret = tmp_path / "secret"
    access.write_text("file-access\n")
    secret.write_text("file-secret\n")
    values = SOURCE | {
        "service_execution_source_access_key": None,
        "service_execution_source_secret_key": None,
        "service_execution_source_access_key_file": access,
        "service_execution_source_secret_key_file": secret,
    }
    config = ServiceExecutionSourceConfig.from_settings(_settings("control_plane", **values))
    assert config is not None
    assert config.access_key.get_secret_value() == "file-access"
    assert config.secret_key.get_secret_value() == "file-secret"
    assert "file-secret" not in repr(config)


def test_source_spool_missing_file_fails_without_secret_disclosure(tmp_path: Path) -> None:
    from loom.trajectory.source_spool import ServiceExecutionSourceConfig

    values = SOURCE | {
        "service_execution_source_secret_key": None,
        "service_execution_source_secret_key_file": tmp_path / "private-key-name",
    }
    with pytest.raises(ValueError) as error:
        ServiceExecutionSourceConfig.from_settings(_settings("control_plane", **values))
    assert "private-key-name" not in str(error.value)
    assert "spool-access" not in str(error.value)


def test_source_spool_rejects_ambiguous_credentials(tmp_path: Path) -> None:
    from loom.trajectory.source_spool import ServiceExecutionSourceConfig

    settings = _settings(
        "control_plane",
        **(
            SOURCE
            | {
                "service_execution_source_secret_key_file": tmp_path / "secret",
            }
        ),
    )
    with pytest.raises(ValueError, match="exactly one"):
        ServiceExecutionSourceConfig.from_settings(settings)


@pytest.mark.parametrize(
    "endpoint",
    [
        "s3://spool.example",
        "https://user:private-secret@spool.example",
        "https://spool.example?token=private-secret",
        "https://[malformed",
    ],
)
def test_source_spool_endpoint_errors_are_secret_safe(endpoint: str) -> None:
    from loom.trajectory.source_spool import ServiceExecutionSourceConfig

    settings = _settings(
        "control_plane",
        **(
            SOURCE
            | {
                "service_execution_source_endpoint": endpoint,
            }
        ),
    )
    with pytest.raises(ValueError, match="service_execution_source_endpoint") as error:
        ServiceExecutionSourceConfig.from_settings(settings)
    assert endpoint not in str(error.value)
    assert "private-secret" not in str(error.value)


@pytest.mark.parametrize("service,prefix", [("control_plane", "CP"), ("llm_gateway", "GW")])
def test_source_spool_settings_from_service_environment(
    monkeypatch: pytest.MonkeyPatch,
    service: str,
    prefix: str,
) -> None:
    from loom.trajectory.source_spool import ServiceExecutionSourceConfig

    for field, value in SOURCE.items():
        monkeypatch.setenv(f"LOOM_{prefix}_{field.upper()}", value)
    config = ServiceExecutionSourceConfig.from_settings(_settings(service))
    assert config is not None
    assert config.bucket == "nebius-spool"
