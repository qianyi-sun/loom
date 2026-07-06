from __future__ import annotations

import json
import os
import subprocess

from scripts.ops.frontend_route_smoke import validate_config_document


def test_validate_config_document_accepts_expected_prod_metadata() -> None:
    errors = validate_config_document(
        route_url="https://yylx.world/prod",
        expected_environment="production",
        expected_api_base="https://yylx.world/prod/api",
        cache_control="no-store, must-revalidate",
        document={
            "environment": "production",
            "environmentLabel": "Production",
            "routePath": "/prod",
            "apiBase": "/prod",
            "apiRouteBase": "https://yylx.world/prod/api",
        },
    )

    assert errors == []


def test_validate_config_document_rejects_cross_environment_api_base() -> None:
    errors = validate_config_document(
        route_url="https://yylx.world/dev",
        expected_environment="development",
        expected_api_base="https://yylx.world/dev/api",
        cache_control="max-age=3600",
        document={
            "environment": "development",
            "environmentLabel": "Development / public beta",
            "routePath": "/dev",
            "apiBase": "/prod",
            "apiRouteBase": "https://yylx.world/prod/api",
        },
    )

    assert "apiRouteBase must be https://yylx.world/dev/api" in errors
    assert "apiBase must match routePath /dev" in errors
    assert "runtime config response must be no-store" in errors


def test_web_runtime_config_script_writes_public_metadata(tmp_path) -> None:
    config_path = tmp_path / "loom-frontend-config.json"
    env = {
        **os.environ,
        "LOOM_FRONTEND_CONFIG_PATH": str(config_path),
        "LOOM_FRONTEND_ENVIRONMENT": "production",
        "LOOM_FRONTEND_ENVIRONMENT_LABEL": "Production",
        "LOOM_FRONTEND_ROUTE_PATH": "/prod",
        "LOOM_FRONTEND_API_BASE": "/prod",
        "LOOM_FRONTEND_PUBLIC_ORIGIN": "https://yylx.world",
    }

    subprocess.run(
        ["sh", "deploy/web-runtime-config.sh"],
        check=True,
        env=env,
        text=True,
        capture_output=True,
    )

    document = json.loads(config_path.read_text(encoding="utf-8"))
    assert document == {
        "environment": "production",
        "environmentLabel": "Production",
        "routePath": "/prod",
        "apiBase": "/prod",
        "apiRouteBase": "https://yylx.world/prod/api",
    }
