"""Registry-only root authority for one developer environment's distributed runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, cast

INSTALLED_SELF: Final = Path(
    "/usr/local/libexec/scripts/ops/developer_environment_runtime_authority.py"
)
INSTALLED_MODULE_ROOT: Final = Path("/usr/local/libexec")
SOURCE_MODULE_ROOT: Final = Path(__file__).absolute().parents[2]
MODULE_ROOT: Final = (
    INSTALLED_MODULE_ROOT if Path(__file__).absolute() == INSTALLED_SELF else SOURCE_MODULE_ROOT
)
INSTALLED_IMPORT_FILES: Final = {
    INSTALLED_SELF: 0o444,
    INSTALLED_MODULE_ROOT / "scripts/__init__.py": 0o444,
    INSTALLED_MODULE_ROOT / "scripts/ops/__init__.py": 0o444,
    INSTALLED_MODULE_ROOT / "scripts/ops/developer_environment_acceptance_probe.py": 0o444,
    INSTALLED_MODULE_ROOT / "scripts/ops/developer_environment_registry.py": 0o555,
    INSTALLED_MODULE_ROOT / "scripts/ops/developer_sandbox_capacity_contract.py": 0o444,
    INSTALLED_MODULE_ROOT / "scripts/ops/developer_sandbox_host.py": 0o444,
    INSTALLED_MODULE_ROOT / "scripts/ops/shared_capacity_runtime_host.py": 0o444,
}


def _validate_installed_import_file(path: Path, expected_mode: int) -> None:
    current = Path("/")
    for part in path.relative_to("/").parts[:-1]:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise RuntimeError("installed runtime import ancestry is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_mode & 0o022
        ):
            raise RuntimeError("installed runtime import ancestry is unsafe")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RuntimeError("installed runtime import asset is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != expected_mode
    ):
        raise RuntimeError("installed runtime import asset is unsafe")
    cache = path.parent / "__pycache__"
    if cache.exists() or cache.is_symlink():
        raise RuntimeError("installed runtime import inventory is not closed")


if Path(__file__).absolute() == INSTALLED_SELF:
    for _path, _mode in INSTALLED_IMPORT_FILES.items():
        _validate_installed_import_file(_path, _mode)

if __package__ in {None, ""}:
    sys.path.insert(0, str(MODULE_ROOT))

from scripts.ops import developer_environment_acceptance_probe as acceptance_probe  # noqa: E402
from scripts.ops import developer_sandbox_host as legacy_host  # noqa: E402
from scripts.ops import shared_capacity_runtime_host as capacity_host  # noqa: E402
from scripts.ops.developer_environment_registry import (  # noqa: E402
    DEPLOY_PHASES,
    DEPLOYMENT_ID_RE,
    SYSTEM_SNAPSHOT,
    DeveloperEnvironmentRegistry,
    RegistryError,
)

if Path(__file__).absolute() == INSTALLED_SELF:
    _IMPORTED_MODULE_PATHS = {
        acceptance_probe: INSTALLED_MODULE_ROOT
        / "scripts/ops/developer_environment_acceptance_probe.py",
        legacy_host: INSTALLED_MODULE_ROOT / "scripts/ops/developer_sandbox_host.py",
        capacity_host: INSTALLED_MODULE_ROOT / "scripts/ops/shared_capacity_runtime_host.py",
        sys.modules["scripts.ops.developer_environment_registry"]: (
            INSTALLED_MODULE_ROOT / "scripts/ops/developer_environment_registry.py"
        ),
        sys.modules["scripts.ops.developer_sandbox_capacity_contract"]: (
            INSTALLED_MODULE_ROOT / "scripts/ops/developer_sandbox_capacity_contract.py"
        ),
    }
    if any(
        Path(cast(str, module.__file__)).absolute() != expected
        for module, expected in _IMPORTED_MODULE_PATHS.items()
    ):
        raise RuntimeError("installed runtime import closure escaped fixed assets")

ROOT: Final = Path("/var/lib/loom-developer-environment-runtime")
REQUEST_ROOT: Final = ROOT / "requests"
RECEIPT_ROOT: Final = ROOT / "receipts"
EXPECTED_HOSTNAME: Final = "trt-eai-oldlab-2"
NODES: Final = (
    *(f"oldlab-{index}" for index in range(1, 6)),
    *(f"trt-gb10-{index}" for index in range(1, 16)),
)


class RuntimeAuthorityError(RuntimeError):
    """A secret-safe distributed-runtime failure."""


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )


def _digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chown(path.parent, 0, 0)
    os.chmod(path.parent, 0o700)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, 0, 0)
        with os.fdopen(descriptor, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _read_root_file(path: Path, *, limit: int) -> bytes:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise RuntimeAuthorityError("root authority input exceeds its size bound")
            chunks.append(chunk)
        rebound = os.fstat(descriptor)
        current = path.lstat()
        raw = b"".join(chunks)
    except RuntimeAuthorityError:
        raise
    except OSError as exc:
        raise RuntimeAuthorityError("root authority input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identities = {
        (
            row.st_dev,
            row.st_ino,
            row.st_mode,
            row.st_uid,
            row.st_gid,
            row.st_nlink,
            row.st_size,
            row.st_mtime_ns,
            row.st_ctime_ns,
        )
        for row in (lexical, opened, rebound, current)
    }
    if (
        stat.S_ISLNK(lexical.st_mode)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or (opened.st_uid, opened.st_gid) != (0, 0)
        or stat.S_IMODE(opened.st_mode) != 0o600
        or len(identities) != 1
    ):
        raise RuntimeAuthorityError("root authority input metadata is unsafe")
    return raw


@contextmanager
def _verified_bundle(candidate: Mapping[str, Any]) -> Iterator[Path]:
    source = Path(cast(str, candidate["bundle_path"]))
    raw = _read_root_file(source, limit=256 * 1024 * 1024)
    if (
        len(raw) != candidate["bundle_size"]
        or hashlib.sha256(raw).hexdigest() != candidate["bundle_sha256"]
    ):
        raise RuntimeAuthorityError("registry candidate bundle binding is invalid")
    stage = ROOT / "staging" / cast(str, candidate["candidate_id"]) / "candidate.bundle"
    _atomic_write(stage, raw)
    try:
        if _read_root_file(stage, limit=256 * 1024 * 1024) != raw:
            raise RuntimeAuthorityError("verified candidate bundle staging drifted")
        yield stage
    finally:
        stage.unlink(missing_ok=True)


def _snapshot() -> dict[str, Any]:
    try:
        return DeveloperEnvironmentRegistry.verify_snapshot(
            _read_root_file(SYSTEM_SNAPSHOT, limit=16 * 1024 * 1024)
        )
    except RegistryError as exc:
        raise RuntimeAuthorityError("registry snapshot is invalid") from exc


def _request(deployment_id: str, action: str) -> dict[str, Any]:
    path = REQUEST_ROOT / f"{deployment_id}-{action}.json"
    raw = _read_root_file(path, limit=1024 * 1024)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeAuthorityError("runtime request is invalid") from exc
    fields = {
        "schema_version",
        "kind",
        "action",
        "deployment_id",
        "env_id",
        "principal_id",
        "runtime_id",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "resource_generation",
        "registry_generation",
        "registry_snapshot_sha256",
        "payload_sha256",
    }
    unsigned = (
        {key: value for key, value in payload.items() if key != "payload_sha256"}
        if isinstance(payload, dict)
        else {}
    )
    if (
        not isinstance(payload, dict)
        or set(payload) != fields
        or raw != _canonical(payload)
        or payload.get("schema_version") != 1
        or payload.get("kind") != "loom.developer-environment.runtime-request"
        or payload.get("action") != action
        or payload.get("deployment_id") != deployment_id
        or payload.get("payload_sha256") != _digest(unsigned)
    ):
        raise RuntimeAuthorityError("runtime request binding is invalid")
    return payload


def _binding(
    snapshot: Mapping[str, Any],
    request: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any] | None,
]:
    deployments = [
        row for row in snapshot["deployments"] if row["deployment_id"] == request["deployment_id"]
    ]
    environments = [row for row in snapshot["environments"] if row["env_id"] == request["env_id"]]
    requested_candidates = [
        row for row in snapshot["candidates"] if row["candidate_id"] == request["candidate_id"]
    ]
    if len(deployments) != 1 or len(environments) != 1 or len(requested_candidates) != 1:
        raise RuntimeAuthorityError("runtime registry binding is unavailable")
    if (
        request["registry_generation"] != snapshot["generation"]
        or request["registry_snapshot_sha256"] != snapshot["payload_sha256"]
    ):
        raise RuntimeAuthorityError("runtime registry snapshot binding drifted")
    deployment = deployments[0]
    environment = environments[0]
    requested_candidate = requested_candidates[0]
    exact = {
        "deployment_id": deployment["deployment_id"],
        "env_id": environment["env_id"],
        "principal_id": environment["principal_id"],
        "runtime_id": environment["runtime_id"],
        "candidate_id": requested_candidate["candidate_id"],
        "candidate_sha": requested_candidate["candidate_sha"],
        "candidate_tree": requested_candidate["candidate_tree"],
        "resource_generation": (
            environment["resource_generation"]
            if environment["state"] == "active" and deployment["phase"] == "committed"
            else (
                deployment["applied_resource_generation"]
                if deployment["phase"] == "verified"
                and deployment["applied_resource_generation"] is not None
                else deployment["expected_resource_generation"]
            )
        ),
    }
    if any(request.get(key) != value for key, value in exact.items()):
        raise RuntimeAuthorityError("runtime registry identity drifted")
    if request["action"] == "rollback":
        effective_id = environment["current_candidate_id"]
        candidates = [row for row in snapshot["candidates"] if row["candidate_id"] == effective_id]
        if deployment["phase"] != "failed" or len(candidates) > 1:
            raise RuntimeAuthorityError("runtime rollback registry binding is invalid")
        effective_candidate = candidates[0] if candidates else None
    elif request["action"] in {"fence", "retire"}:
        effective_candidate = requested_candidate
        if (
            environment["state"] != "quarantined"
            or deployment["phase"] != "committed"
            or environment["current_candidate_id"] != requested_candidate["candidate_id"]
            or deployment["applied_resource_generation"] != environment["resource_generation"]
        ):
            raise RuntimeAuthorityError("runtime retirement registry binding is invalid")
    else:
        effective_candidate = requested_candidate
        if deployment["phase"] == "failed":
            raise RuntimeAuthorityError("failed deployment cannot mutate distributed runtime")
        if environment["state"] == "deploying":
            if DEPLOY_PHASES.index(deployment["phase"]) < DEPLOY_PHASES.index("services-prepared"):
                raise RuntimeAuthorityError("runtime deployment phase is too early")
        elif environment["state"] != "active" or deployment["phase"] != "committed":
            raise RuntimeAuthorityError("runtime environment is not provisionable")
    return environment, requested_candidate, deployment, effective_candidate


def _profile(
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    candidate: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> legacy_host.Profile:
    state_root = Path(cast(str, environment["state_root"]))
    return legacy_host.Profile(
        sandbox=cast(str, environment["runtime_id"]),
        compose_project=cast(str, environment["compose_project"]),
        canonical_hostname=EXPECTED_HOSTNAME,
        candidate_root=Path(cast(str, environment["candidate_root"])),
        state_root=state_root,
        cache_root=state_root / "cache",
        evidence_root=Path(cast(str, environment["evidence_root"])),
        runtime_root=Path(cast(str, environment["runtime_root"])),
        ports={
            str(name): int(port)
            for name, port in cast(dict[str, int], environment["ports"]).items()
        },
        env_id=cast(str, environment["env_id"]),
        resource_generation=cast(
            int,
            (
                environment["resource_generation"]
                if environment["state"] == "active"
                else (
                    deployment["applied_resource_generation"]
                    if deployment["phase"] == "verified"
                    and deployment["applied_resource_generation"] is not None
                    else deployment["expected_resource_generation"]
                )
            ),
        ),
        registry_generation=cast(int, snapshot["generation"]),
        registry_payload_sha256=cast(str, snapshot["payload_sha256"]),
        candidate_id=cast(str, candidate["candidate_id"]),
        candidate_tree=cast(str, candidate["candidate_tree"]),
        service_user=cast(str, environment["service_user"]),
    )


def _reconcile(
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    candidate: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> None:
    profile = _profile(snapshot, environment, candidate, deployment)
    sha = cast(str, candidate["candidate_sha"])
    tree = cast(str, candidate["candidate_tree"])
    authority = legacy_host._identity("root", legacy_host.SHARED_GROUP)
    legacy_host._bootstrap_domain_runtime_hosts(profile, sha, tree)
    # The root-installed node authority's fixed materialize action consumes
    # only the registry-bound git bundle bytes. It does not import or execute
    # any Python/ops code from that bundle.
    with _verified_bundle(candidate) as bundle:
        legacy_host._materialize_domain_candidates(profile, sha, tree, bundle)
    legacy_host._converge_domain_runtime_hosts(profile, sha, tree, authority)
    legacy_host._install_remote_link_fleet(profile, sha, tree, authority)
    legacy_host._publish_domain_attestations(profile, sha, tree)
    capacity_host.reconcile_registry_environment(profile.sandbox)


def _check(
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    candidate: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> dict[str, Any]:
    profile = _profile(snapshot, environment, candidate, deployment)
    sha = cast(str, candidate["candidate_sha"])
    tree = cast(str, candidate["candidate_tree"])
    legacy_host._collect_and_persist_remote_link_fleet(profile, sha, tree)
    return capacity_host.check_registry_environment(profile.sandbox)


def _retire(environment: Mapping[str, Any]) -> dict[str, Any]:
    runtime_id = cast(str, environment["runtime_id"])
    report = capacity_host.retire_registry_environment(runtime_id)
    legacy_host._run(
        (
            "systemctl",
            "disable",
            "--now",
            f"loom-developer-sandbox-link@{runtime_id}.service",
        ),
        expected={0, 1, 5},
    )
    return report


def _fence(environment: Mapping[str, Any]) -> dict[str, Any]:
    return capacity_host.fence_registry_environment(
        cast(str, environment["runtime_id"]),
    )


def _activate(environment: Mapping[str, Any]) -> dict[str, Any]:
    return capacity_host.reopen_registry_environment_admission(
        cast(str, environment["runtime_id"]),
    )


def execute(action: str, deployment_id: str) -> dict[str, Any]:
    if os.geteuid() != 0:
        raise RuntimeAuthorityError("distributed runtime authority requires root")
    if DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None:
        raise RuntimeAuthorityError("deployment identity is invalid")
    request = _request(deployment_id, action)
    if action == "acceptance-probe":
        combined_path = ROOT / "acceptance-probes" / deployment_id / "combined.json"
        if combined_path.exists() or combined_path.is_symlink():
            try:
                combined = acceptance_probe._load_json(
                    combined_path,
                    description="combined acceptance probe receipt",
                    require_root_ownership=True,
                )
            except acceptance_probe.AcceptanceProbeError as exc:
                raise RuntimeAuthorityError("acceptance probe replay receipt is invalid") from exc
            exact = {
                "deployment_id": request["deployment_id"],
                "env_id": request["env_id"],
                "principal_id": request["principal_id"],
                "runtime_id": request["runtime_id"],
                "candidate_id": request["candidate_id"],
                "candidate_sha": request["candidate_sha"],
                "candidate_tree": request["candidate_tree"],
                "applied_resource_generation": request["resource_generation"],
                "registry_generation": request["registry_generation"],
                "registry_snapshot_sha256": request["registry_snapshot_sha256"],
                "runtime_request_sha256": request["payload_sha256"],
            }
            domains = combined.get("domains")
            unsigned = {key: value for key, value in combined.items() if key != "payload_sha256"}
            if (
                set(combined) != acceptance_probe.COMBINED_FIELDS
                or combined.get("schema_version") != 1
                or combined.get("kind") != acceptance_probe.COMBINED_RECEIPT_KIND
                or combined.get("status") != "passed"
                or combined.get("action") != "acceptance-probe"
                or any(combined.get(field) != value for field, value in exact.items())
                or not isinstance(domains, dict)
                or set(domains) != {"oldlab", "gb10"}
                or any(
                    not isinstance(domains[domain], dict)
                    or domains[domain].get("domain") != domain
                    or domains[domain].get("deployment_id") != deployment_id
                    or domains[domain].get("env_id") != request["env_id"]
                    or domains[domain].get("candidate_id") != request["candidate_id"]
                    or domains[domain].get("payload_sha256")
                    != acceptance_probe._digest(
                        {
                            key: value
                            for key, value in domains[domain].items()
                            if key != "payload_sha256"
                        }
                    )
                    for domain in ("oldlab", "gb10")
                )
                or combined.get("payload_sha256") != acceptance_probe._digest(unsigned)
            ):
                raise RuntimeAuthorityError("acceptance probe replay receipt binding is invalid")
            return combined
    snapshot = _snapshot()
    environment, _requested_candidate, deployment, effective_candidate = _binding(
        snapshot,
        request,
    )
    capacity_report: dict[str, Any]
    if action == "reconcile":
        assert effective_candidate is not None
        _reconcile(snapshot, environment, effective_candidate, deployment)
        capacity_report = _check(
            snapshot,
            environment,
            effective_candidate,
            deployment,
        )
    elif action == "rollback":
        if effective_candidate is None:
            _retire(environment)
        else:
            _reconcile(snapshot, environment, effective_candidate, deployment)
        _activate(environment)
        if effective_candidate is None:
            capacity_report = {
                "status": "ready",
                "runtime_id": environment["runtime_id"],
            }
        else:
            capacity_report = _check(
                snapshot,
                environment,
                effective_candidate,
                deployment,
            )
    elif action == "check":
        assert effective_candidate is not None
        capacity_report = _check(
            snapshot,
            environment,
            effective_candidate,
            deployment,
        )
    elif action == "acceptance-probe":
        try:
            return acceptance_probe.execute(deployment_id)
        except acceptance_probe.AcceptanceProbeError as exc:
            raise RuntimeAuthorityError("acceptance probe failed safely") from exc
    elif action == "fence":
        capacity_report = _fence(environment)
    elif action == "activate":
        capacity_report = _activate(environment)
    elif action == "retire":
        capacity_report = _retire(environment)
    else:
        raise RuntimeAuthorityError("runtime action is invalid")
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.runtime-receipt",
        "status": cast(str, capacity_report["status"]),
        "action": action,
        "deployment_id": request["deployment_id"],
        "env_id": request["env_id"],
        "runtime_id": request["runtime_id"],
        "candidate_id": request["candidate_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "effective_candidate_id": (
            None if effective_candidate is None else effective_candidate["candidate_id"]
        ),
        "effective_candidate_sha": (
            None if effective_candidate is None else effective_candidate["candidate_sha"]
        ),
        "effective_candidate_tree": (
            None if effective_candidate is None else effective_candidate["candidate_tree"]
        ),
        "resource_generation": request["resource_generation"],
        "registry_generation": request["registry_generation"],
        "registry_snapshot_sha256": request["registry_snapshot_sha256"],
        "request_sha256": request["payload_sha256"],
        "domains": ["oldlab", "gb10"],
        "nodes": list(NODES),
        "remote_link": {"status": "ready"},
        "domain_runtime": {"status": "ready"},
        "shared_capacity": capacity_report,
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    receipt = {**unsigned, "payload_sha256": _digest(unsigned)}
    _atomic_write(
        RECEIPT_ROOT / f"{deployment_id}-{action}.json",
        _canonical(receipt),
    )
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "action",
        choices=(
            "reconcile",
            "check",
            "acceptance-probe",
            "activate",
            "fence",
            "rollback",
            "retire",
        ),
    )
    parser.add_argument("--deployment-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    print(json.dumps(execute(args.action, args.deployment_id), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason": (
                        str(exc)
                        if isinstance(exc, RuntimeAuthorityError)
                        else "distributed runtime convergence failed safely"
                    ),
                },
                sort_keys=True,
            )
        )
        raise SystemExit(2) from None
