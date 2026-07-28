#!/usr/bin/env python3
"""Validate developer-sandbox profile isolation (static, secret-safe).

Checks the three checked-in profiles under deploy/developer-sandboxes/
for pairwise-disjoint identity fields and secret-free dry-run artifacts.
Does not contact oldlab-2 or resolve secret values.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ALLOWED_SANDBOXES = ("qianyi", "hongjian", "devansh")

REQUIRED_DISTINCT_FIELDS = (
    "compose_project",
    "provider_connection_namespace",
    "candidate_root",
    "state_root",
    "cache_root",
    "evidence_root",
    "runtime_root",
    "database_name",
    "task_bucket",
    "trajectories_bucket",
    "artifacts_bucket",
)

PORT_FIELDS = (
    "postgres",
    "minio",
    "minio_console",
    "control_plane",
    "loom_service",
    "llm_gateway",
    "egress_xds",
    "egress_proxy",
    "egress_admin",
    "web",
)

SECRET_NEEDLES = (
    "Bearer ",
    "loom_w_",
    "loom_admin_",
    "sk-",
    "AKIA",
    "BEGIN PRIVATE KEY",
    "password=",
    "secret=",
)


@dataclass(frozen=True)
class DeveloperSandboxProfile:
    sandbox: str
    compose_project: str
    provider_connection_namespace: str
    candidate_root: str
    state_root: str
    cache_root: str
    evidence_root: str
    runtime_root: str
    database_name: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    ports: dict[str, int]

    @classmethod
    def from_toml(cls, path: Path) -> DeveloperSandboxProfile:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        ports_raw = raw.get("ports")
        if not isinstance(ports_raw, dict):
            raise ValueError(f"{path}: missing [ports]")
        database = raw.get("database")
        if not isinstance(database, dict):
            raise ValueError(f"{path}: missing [database]")
        object_store = raw.get("object_store")
        if not isinstance(object_store, dict):
            raise ValueError(f"{path}: missing [object_store]")
        ports: dict[str, int] = {}
        for field in PORT_FIELDS:
            value = ports_raw.get(field)
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"{path}: ports.{field} must be an int")
            ports[field] = value
        sandbox = _require_str(raw, path, "sandbox")
        return cls(
            sandbox=sandbox,
            compose_project=_require_str(raw, path, "compose_project"),
            provider_connection_namespace=_require_str(
                raw,
                path,
                "provider_connection_namespace",
            ),
            candidate_root=_require_str(raw, path, "candidate_root"),
            state_root=_require_str(raw, path, "state_root"),
            cache_root=_require_str(raw, path, "cache_root"),
            evidence_root=_require_str(raw, path, "evidence_root"),
            runtime_root=_require_str(raw, path, "runtime_root"),
            database_name=_require_str(database, path, "name"),
            task_bucket=_require_str(object_store, path, "task_bucket"),
            trajectories_bucket=_require_str(
                object_store,
                path,
                "trajectories_bucket",
            ),
            artifacts_bucket=_require_str(object_store, path, "artifacts_bucket"),
            ports=ports,
        )


def _require_str(raw: dict[str, Any], path: Path, key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}: {key} must be a non-empty string")
    return value.strip()


def load_profiles(profiles_dir: Path) -> list[DeveloperSandboxProfile]:
    profiles: list[DeveloperSandboxProfile] = []
    for owner in ALLOWED_SANDBOXES:
        path = profiles_dir / f"{owner}.toml"
        if not path.is_file():
            raise ValueError(f"missing profile {path}")
        profile = DeveloperSandboxProfile.from_toml(path)
        if profile.sandbox != owner:
            raise ValueError(f"{path}: sandbox={profile.sandbox!r} != {owner!r}")
        profiles.append(profile)
    return profiles


def validate_profiles(profiles: list[DeveloperSandboxProfile]) -> list[str]:
    errors: list[str] = []
    names = [p.sandbox for p in profiles]
    if names != list(ALLOWED_SANDBOXES):
        errors.append(f"profiles must be exactly {list(ALLOWED_SANDBOXES)}, got {names}")

    for field in REQUIRED_DISTINCT_FIELDS:
        values = [getattr(p, field) for p in profiles]
        if len(values) != len(set(values)):
            errors.append(f"{field} must be pairwise distinct across sandboxes")

    all_ports: list[int] = []
    for profile in profiles:
        for field, value in profile.ports.items():
            all_ports.append(value)
            if value <= 0 or value > 65535:
                errors.append(
                    f"{profile.sandbox}: ports.{field}={value} out of range",
                )
        if profile.compose_project != f"loom-sandbox-{profile.sandbox}":
            errors.append(
                f"{profile.sandbox}: compose_project must be 'loom-sandbox-{profile.sandbox}'",
            )
        if profile.provider_connection_namespace != f"sandbox-{profile.sandbox}":
            errors.append(
                f"{profile.sandbox}: provider_connection_namespace must be "
                f"'sandbox-{profile.sandbox}' (profile identity only until a "
                f"runtime consumer enforces it)",
            )
        if profile.database_name != f"loom_sandbox_{profile.sandbox}":
            errors.append(
                f"{profile.sandbox}: database name must be 'loom_sandbox_{profile.sandbox}'",
            )
        for field, expected_suffix in (
            ("task_bucket", "tasks"),
            ("trajectories_bucket", "trajectories"),
            ("artifacts_bucket", "artifacts"),
        ):
            expected = f"loom-sandbox-{profile.sandbox}-{expected_suffix}"
            if getattr(profile, field) != expected:
                errors.append(
                    f"{profile.sandbox}: {field} must equal {expected!r}",
                )
        for root_field in (
            "state_root",
            "cache_root",
            "evidence_root",
            "runtime_root",
        ):
            root = getattr(profile, root_field)
            if f"/{profile.sandbox}" not in root and not root.endswith(
                f"/{profile.sandbox}",
            ):
                # roots must be under that developer's path segment
                if f"developer-sandboxes/{profile.sandbox}" not in root:
                    errors.append(
                        f"{profile.sandbox}: {root_field}={root!r} must be "
                        f"under developer-sandboxes/{profile.sandbox}",
                    )

    if len(all_ports) != len(set(all_ports)):
        errors.append("host ports must be pairwise distinct across all sandboxes")

    return errors


def build_dry_run_artifact(
    profiles: list[DeveloperSandboxProfile],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-isolation-dry-run",
        "notes": [
            "provider_connection_namespace is profile identity only until a "
            "runtime consumer enforces it",
            "LOOM_DEV_TASK_BUCKET is planner/ops identity for MinIO naming; "
            "compose wires trajectories/artifacts buckets into loom-service",
        ],
        "profiles": [
            {
                **{k: v for k, v in asdict(profile).items() if k != "ports"},
                "ports": dict(sorted(profile.ports.items())),
            }
            for profile in profiles
        ],
    }


def assert_artifact_secret_free(artifact: dict[str, Any]) -> list[str]:
    blob = json.dumps(artifact, sort_keys=True)
    return [
        f"dry-run artifact must not contain {needle!r}"
        for needle in SECRET_NEEDLES
        if needle.lower() in blob.lower()
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="validate_developer_sandbox_isolation")
    parser.add_argument(
        "--profiles-dir",
        type=Path,
        default=Path("deploy/developer-sandboxes"),
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--write-dry-run",
        type=Path,
        default=None,
        help="Write secret-safe dry-run artifact to this path",
    )
    args = parser.parse_args(argv)

    profiles: list[DeveloperSandboxProfile] = []
    try:
        profiles = load_profiles(args.profiles_dir)
        errors = validate_profiles(profiles)
        artifact = build_dry_run_artifact(profiles)
        errors.extend(assert_artifact_secret_free(artifact))
    except ValueError as exc:
        errors = [str(exc)]
        artifact = {
            "schema_version": 1,
            "artifact_type": "developer-sandbox-isolation-dry-run",
            "profiles": [],
            "error": str(exc),
        }

    status = "pass" if not errors else "fail"
    report = {
        "status": status,
        "errors": errors,
        "profiles": [
            {
                "sandbox": p.sandbox,
                "compose_project": p.compose_project,
                "database_name": p.database_name,
                "task_bucket": p.task_bucket,
                "trajectories_bucket": p.trajectories_bucket,
                "artifacts_bucket": p.artifacts_bucket,
                "provider_connection_namespace": p.provider_connection_namespace,
            }
            for p in profiles
        ],
        "dry_run": artifact,
    }

    if args.write_dry_run is not None:
        args.write_dry_run.write_text(
            json.dumps(artifact, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"developer-sandbox isolation: {status}")
        for err in errors:
            print(f"  - {err}", file=sys.stderr)

    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
