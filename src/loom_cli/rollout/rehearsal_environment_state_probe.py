"""Seed exact autoscaler policy into the isolated release rehearsal database."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_cli.environment_state import (
    autoscaler_policy_payload,
    load_environment_state_profile,
)
from loom_control_plane.worker_pool_autoscaler import (
    autoscaler_policy_to_dict,
    get_autoscaler_policy,
    upsert_autoscaler_policy,
)

_MAX_PROFILE_BYTES = 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_IMAGE_TAG_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_DATABASE_RE = re.compile(r"loom_rehearsal_[0-9a-f]{24}\Z")

Policy = dict[str, object]
PolicyApplier = Callable[[str, list[Policy]], list[Policy]]


class RehearsalEnvironmentStateError(RuntimeError):
    """A bounded, secret-free isolated policy convergence failure."""


def _canonical_policy(policy: dict[str, Any]) -> Policy:
    return {
        "environment": policy.get("environment"),
        "pool_name": policy.get("pool_name"),
        **autoscaler_policy_payload(policy),
    }


def _validate_database_authority(database_url: str, expected_database: str) -> None:
    try:
        authority = make_url(database_url)
    except Exception as exc:
        raise RehearsalEnvironmentStateError("rehearsal database authority is invalid") from exc
    if (
        _DATABASE_RE.fullmatch(expected_database) is None
        or authority.drivername != "postgresql+psycopg"
        or authority.username != "loom_rehearsal"
        or authority.password is not None
        or authority.host != "127.0.0.1"
        or authority.port != 5432
        or authority.database != expected_database
        or authority.query
    ):
        raise RehearsalEnvironmentStateError("rehearsal database authority is invalid")


async def _apply_policies_async(database_url: str, policies: list[Policy]) -> list[Policy]:
    engine = create_async_engine(database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            for policy in policies:
                payload = autoscaler_policy_payload(policy)
                await upsert_autoscaler_policy(
                    session,
                    environment=str(policy["environment"]),
                    pool_name=str(policy["pool_name"]),
                    **payload,
                )
            await session.commit()

            observed: list[Policy] = []
            for policy in policies:
                row = await get_autoscaler_policy(
                    session,
                    environment=str(policy["environment"]),
                    pool_name=str(policy["pool_name"]),
                )
                if row is None:
                    raise RehearsalEnvironmentStateError(
                        "rehearsal autoscaler policy did not converge exactly"
                    )
                observed.append(_canonical_policy(autoscaler_policy_to_dict(row)))
            return observed
    finally:
        await engine.dispose()


def _apply_policies(database_url: str, policies: list[Policy]) -> list[Policy]:
    return asyncio.run(_apply_policies_async(database_url, policies))


def _load_profile(
    profile_bytes: bytes,
    *,
    candidate_sha: str,
    image_tag: str,
) -> list[Policy]:
    with tempfile.TemporaryDirectory(prefix="loom-rehearsal-environment-state-") as raw_dir:
        path = Path(raw_dir) / "staging.toml"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags, 0o600)
        try:
            offset = 0
            while offset < len(profile_bytes):
                offset += os.write(descriptor, profile_bytes[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        profile = load_environment_state_profile(
            path,
            variables={
                "ENV_CONFIG_VERSION": image_tag,
                "GIT_SHA": candidate_sha,
                "IMAGE_TAG": image_tag,
            },
            expected_environment="staging",
        )
    policies = [_canonical_policy(policy) for policy in profile.autoscaler_policies]
    if (
        profile.control_plane_environment != "staging"
        or not 1 <= len(policies) <= 64
        or len({(item["environment"], item["pool_name"]) for item in policies}) != len(policies)
    ):
        raise RehearsalEnvironmentStateError("rehearsal autoscaler policy profile is invalid")
    return policies


def run_probe(
    *,
    profile_bytes: bytes,
    database_url: str,
    expected_database: str,
    plan_sha256: str,
    expected_profile_sha256: str,
    expected_candidate_sha: str,
    expected_candidate_tree: str,
    expected_image_tag: str,
    apply_policies: PolicyApplier = _apply_policies,
) -> dict[str, object]:
    if (
        not 1 <= len(profile_bytes) <= _MAX_PROFILE_BYTES
        or _SHA256_RE.fullmatch(plan_sha256) is None
        or _SHA256_RE.fullmatch(expected_profile_sha256) is None
        or _GIT_SHA_RE.fullmatch(expected_candidate_sha) is None
        or _GIT_SHA_RE.fullmatch(expected_candidate_tree) is None
        or _IMAGE_TAG_RE.fullmatch(expected_image_tag) is None
    ):
        raise RehearsalEnvironmentStateError("rehearsal environment-state binding is invalid")
    if hashlib.sha256(profile_bytes).hexdigest() != expected_profile_sha256:
        raise RehearsalEnvironmentStateError("rehearsal environment-state profile identity drifted")
    _validate_database_authority(database_url, expected_database)
    try:
        desired = _load_profile(
            profile_bytes,
            candidate_sha=expected_candidate_sha,
            image_tag=expected_image_tag,
        )
        observed = apply_policies(database_url, desired)
    except RehearsalEnvironmentStateError:
        raise
    except Exception as exc:
        raise RehearsalEnvironmentStateError(
            "rehearsal autoscaler policy convergence failed"
        ) from exc
    if observed != desired:
        raise RehearsalEnvironmentStateError("rehearsal autoscaler policy did not converge exactly")
    evidence = {
        "candidate_sha": expected_candidate_sha,
        "candidate_tree": expected_candidate_tree,
        "image_tag": expected_image_tag,
        "plan_sha256": plan_sha256,
        "policies": desired,
        "profile_sha256": expected_profile_sha256,
    }
    return {
        "candidate_sha": expected_candidate_sha,
        "candidate_tree": expected_candidate_tree,
        "evidence_sha256": hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "image_tag": expected_image_tag,
        "plan_sha256": plan_sha256,
        "policy_count": len(desired),
        "profile_sha256": expected_profile_sha256,
        "schema_version": 1,
        "status": "ready",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loom-rehearsal-environment-state-probe")
    parser.add_argument("--database", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--profile-sha256", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-tree", required=True)
    parser.add_argument("--image-tag", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    profile = sys.stdin.buffer.read(_MAX_PROFILE_BYTES + 1)
    try:
        result = run_probe(
            profile_bytes=profile,
            database_url=os.environ.get("LOOM_DB_URL", ""),
            expected_database=args.database,
            plan_sha256=args.plan_sha256,
            expected_profile_sha256=args.profile_sha256,
            expected_candidate_sha=args.candidate_sha,
            expected_candidate_tree=args.candidate_tree,
            expected_image_tag=args.image_tag,
        )
    except (RehearsalEnvironmentStateError, ValueError):
        print(
            json.dumps(
                {"failure_code": "rehearsal-environment-state-failed", "status": "blocked"},
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI boundary
    raise SystemExit(main())


__all__ = ["RehearsalEnvironmentStateError", "run_probe"]
