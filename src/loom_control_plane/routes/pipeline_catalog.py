"""Read-only server-owned Pipeline resource and image contract catalogs."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Header, HTTPException, Request

from loom.auth import verify_bearer_token
from loom.pipeline.image_runtime import ImageRuntimeRegistry
from loom.pipeline.resource_profiles import ResourceProfileRegistry

router = APIRouter(prefix="/pipeline")
_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


async def _require_authenticated(request: Request, authorization: str | None) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")


def _resource_registry() -> ResourceProfileRegistry:
    return ResourceProfileRegistry.load(_REPOSITORY_ROOT / "config/resource-profiles.toml")


def _image_registry() -> ImageRuntimeRegistry:
    return ImageRuntimeRegistry.load(
        _REPOSITORY_ROOT / "config/image-runtime-contracts.toml"
    )


@router.get("/resource-profiles")
async def list_resource_profiles(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_authenticated(request, authorization)
    return {
        "schema_version": "loom.resource-profile-list.v1",
        "items": [
            {
                "snapshot": record.profile.model_dump(mode="json"),
                "snapshot_sha256": record.snapshot_sha256,
            }
            for record in _resource_registry().list()
        ],
    }


@router.get("/resource-profiles/{name}/{version}")
async def read_resource_profile(
    name: str,
    version: int,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_authenticated(request, authorization)
    try:
        record = _resource_registry().get(f"{name}@{version}")
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="resource_profile_not_found") from exc
    return {
        "snapshot": record.profile.model_dump(mode="json"),
        "snapshot_sha256": record.snapshot_sha256,
    }


@router.get("/image-runtime-contracts")
async def list_image_runtime_contracts(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_authenticated(request, authorization)
    return {
        "schema_version": "loom.image-runtime-contract-list.v1",
        "items": [
            {
                "snapshot": record.contract.model_dump(mode="json"),
                "snapshot_sha256": record.snapshot_sha256,
            }
            for record in _image_registry().list()
        ],
    }


@router.get("/image-runtime-contracts/by-digest/{snapshot_sha256}")
async def read_image_runtime_contract(
    snapshot_sha256: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_authenticated(request, authorization)
    matches = [
        record
        for record in _image_registry().list()
        if record.snapshot_sha256 == snapshot_sha256
    ]
    if len(matches) != 1:
        raise HTTPException(status_code=404, detail="image_runtime_contract_not_found")
    return {
        "snapshot": matches[0].contract.model_dump(mode="json"),
        "snapshot_sha256": matches[0].snapshot_sha256,
    }
