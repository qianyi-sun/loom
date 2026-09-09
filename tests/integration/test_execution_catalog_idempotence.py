"""Catalog reapplication preserves set semantics across Control Plane processes."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import ServiceExecutionClass, ServiceExecutionTarget
from loom.nebius_platform_render import build_platform
from loom.pipeline.keys import canonical_digest
from loom_control_plane.routes import service_executions
from tests.unit.test_nebius_platform_render import platform_inputs  # noqa: F401


@pytest.mark.asyncio
async def test_catalog_reapply_accepts_legacy_set_order_and_rejects_definition_changes(
    postgres_url: str, request: pytest.FixtureRequest
) -> None:
    config, candidate, profile = request.getfixturevalue("platform_inputs")
    files = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).parents[2])
    catalog = json.loads(files["10-config-network.yaml"][0]["data"]["catalog.json"])
    class_id = "catalog-order-" + uuid4().hex
    target_id = "catalog-target-" + uuid4().hex
    catalog["execution_class"]["class_id"] = class_id
    catalog["topology"]["execution_class_id"] = class_id
    target = catalog["topology"]["targets"][0]
    target["target_id"] = target_id
    target["execution_class_id"] = class_id
    legacy = deepcopy(catalog["execution_class"])
    # Another process's set order, with its legitimate historical digest.
    access = legacy["network_access"]
    legacy["network_access"] = access[1:] + access[:1]
    legacy_digest = canonical_digest(legacy)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = FastAPI()
    app.state.session_factory = sessions
    token = "loom_admin_" + "e" * 64
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)
    app.include_router(service_executions.router)
    try:
        async with sessions() as session, session.begin():
            session.add(
                ServiceExecutionClass(
                    id=class_id,
                    schema_version=legacy["schema_version"],
                    spec_json=legacy,
                    spec_sha256=legacy_digest,
                    enabled=True,
                )
            )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://catalog.test",
            headers={"Authorization": "Bearer " + token},
        ) as client:
            for _ in range(2):
                response = await client.post("/admin/service-execution/catalog", json=catalog)
                assert response.status_code == 200, response.text
            changed = deepcopy(catalog)
            changed["execution_class"]["network_access"] = ["none"]
            response = await client.post("/admin/service-execution/catalog", json=changed)
            assert response.status_code == 409
            changed = deepcopy(catalog)
            changed["topology"]["targets"][0]["namespace_name"] = "different-namespace"
            response = await client.post("/admin/service-execution/catalog", json=changed)
            assert response.status_code == 409
        async with sessions() as session:
            stored = await session.get(ServiceExecutionClass, class_id)
            assert stored is not None
            assert stored.spec_json == legacy
            assert stored.spec_sha256 == legacy_digest
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target_id)
            )
            await session.execute(
                delete(ServiceExecutionClass).where(ServiceExecutionClass.id == class_id)
            )
        await engine.dispose()
