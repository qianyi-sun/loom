"""Prepare a recoverable personal-development database schema transition."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from alembic.config import Config
from alembic.script import ScriptDirectory

from loom import personal_dev_control_plane_render
from loom.personal_dev_acceptance_evidence import (
    PersonalDevAcceptanceEvidenceError,
    load_personal_dev_backup_restore_evidence,
    validate_personal_dev_rollback_shadow_manifest,
)
from loom.personal_dev_control_plane_config import (
    PersonalDevControlPlaneProfile,
    PersonalDevTrustedRelease,
)
from loom.personal_dev_control_plane_render import (
    render_shadow_personal_dev_control_plane,
)

_DIGEST = re.compile(r"[0-9a-f]{64}")
_REVISION = re.compile(r"[A-Za-z0-9_]+")
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_MAX_BACKUP_ARTIFACT_BYTES = 4 * 1024 * 1024 * 1024
_GIT_ENVIRONMENT = {
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_OPTIONAL_LOCKS": "0",
    "GIT_TERMINAL_PROMPT": "0",
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin",
}
_ManifestIdentity = tuple[str, str, str, str]
_FORWARD_ONLY_IDENTITIES: dict[_ManifestIdentity, str] = {
    (
        "apps/v1",
        "Deployment",
        "loom-dev",
        "loom-personal-dev-web",
    ): "deployment.apps/loom-personal-dev-web",
    (
        "networking.k8s.io/v1",
        "NetworkPolicy",
        "loom-dev",
        "loom-personal-dev-web-ingress",
    ): "networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress",
    (
        "v1",
        "Service",
        "loom-dev",
        "loom-personal-dev-web",
    ): "service/loom-personal-dev-web",
}


class PersonalDevSchemaTransitionError(ValueError):
    """Raised when schema-transition preparation inputs are not exact."""


@dataclass(frozen=True)
class PreparedPersonalDevSchemaTransition:
    """Canonical plan and exact migration Job prepared from reviewed inputs."""

    plan: dict[str, object]
    plan_json: bytes
    migration_job_json: bytes


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _open_owner_only(path: Path, *, maximum_bytes: int) -> tuple[int, os.stat_result]:
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or not 0 < before.st_size <= maximum_bytes
    ):
        raise ValueError
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    opened = os.fstat(descriptor)
    if _identity(opened) != _identity(before):
        os.close(descriptor)
        raise ValueError
    return descriptor, before


def _read_owner_only(path: Path, *, maximum_bytes: int) -> bytes:
    descriptor: int | None = None
    try:
        descriptor, before = _open_owner_only(path, maximum_bytes=maximum_bytes)
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != before.st_size
            or _identity(os.fstat(descriptor)) != _identity(before)
            or _identity(path.lstat()) != _identity(before)
        ):
            raise ValueError
        return bytes(payload)
    except (OSError, ValueError):
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _sha256_owner_only(path: Path) -> str:
    descriptor: int | None = None
    try:
        descriptor, before = _open_owner_only(
            path,
            maximum_bytes=_MAX_BACKUP_ARTIFACT_BYTES,
        )
        digest = hashlib.sha256()
        observed = 0
        while observed <= _MAX_BACKUP_ARTIFACT_BYTES:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            observed += len(chunk)
            digest.update(chunk)
        if (
            observed != before.st_size
            or _identity(os.fstat(descriptor)) != _identity(before)
            or _identity(path.lstat()) != _identity(before)
        ):
            raise ValueError
        return digest.hexdigest()
    except (OSError, ValueError):
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _script_directory(alembic_ini_path: Path) -> ScriptDirectory:
    resolved = alembic_ini_path.resolve(strict=True)
    metadata = alembic_ini_path.lstat()
    if (
        alembic_ini_path != resolved
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
    ):
        raise PersonalDevSchemaTransitionError("personal-dev schema transition inputs are invalid")
    config = Config(str(resolved))
    config.set_main_option("path_separator", "os")
    config.set_main_option("script_location", str(resolved.parent))
    return ScriptDirectory.from_config(config)


def _single_target_and_revision_path(
    alembic_ini_path: Path,
) -> tuple[str, list[str], ScriptDirectory]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            scripts = _script_directory(alembic_ini_path)
            heads = tuple(scripts.get_heads())
            if len(heads) != 1 or _REVISION.fullmatch(heads[0]) is None:
                raise ValueError
            target = heads[0]
            walked = list(scripts.walk_revisions(base="base", head=target))
            ancestors = [revision.revision for revision in walked]
        if len(ancestors) != len(set(ancestors)) or any(
            _REVISION.fullmatch(revision) is None for revision in ancestors
        ):
            raise ValueError
        return target, ancestors, scripts
    except PersonalDevSchemaTransitionError:
        raise
    except Exception:
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None


def _linear_forward_revision_path(
    scripts: ScriptDirectory,
    *,
    predecessor: str,
    target: str,
) -> list[str]:
    try:
        reverse_path: list[str] = []
        seen: set[str] = set()
        current = target
        while current != predecessor:
            if current in seen or _REVISION.fullmatch(current) is None:
                raise ValueError
            seen.add(current)
            revision = scripts.get_revision(current)
            if (
                revision is None
                or revision.dependencies
                or revision.branch_labels
                or not isinstance(revision.down_revision, str)
            ):
                raise ValueError
            reverse_path.append(current)
            current = revision.down_revision
        if not reverse_path:
            raise ValueError
        return list(reversed(reverse_path))
    except PersonalDevSchemaTransitionError:
        raise
    except Exception:
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None


def _load_predecessor_backup(
    *,
    path: Path,
    expected_sha256: str,
    release: PersonalDevTrustedRelease,
    release_sha256: str,
    candidate_heads: list[str],
) -> tuple[str, Any]:
    matches: list[tuple[str, Any]] = []
    for head in candidate_heads:
        try:
            evidence = load_personal_dev_backup_restore_evidence(
                path,
                expected_sha256=expected_sha256,
                release=release,
                release_sha256=release_sha256,
                expected_schema_head=head,
            )
        except (OSError, TypeError, ValueError, PersonalDevAcceptanceEvidenceError):
            continue
        matches.append((head, evidence))
    if len(matches) != 1:
        raise PersonalDevSchemaTransitionError("personal-dev schema transition inputs are invalid")
    return matches[0]


def _validate_predecessor_shadow(
    path: Path,
    *,
    expected_sha256: str,
    release_sha256: str,
    service_image: str,
) -> tuple[set[_ManifestIdentity], _ManifestIdentity]:
    if _DIGEST.fullmatch(expected_sha256) is None:
        raise PersonalDevSchemaTransitionError("personal-dev schema transition inputs are invalid")
    payload = _read_owner_only(path, maximum_bytes=_MAX_MANIFEST_BYTES)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise PersonalDevSchemaTransitionError("personal-dev schema transition inputs are invalid")
    try:
        documents = [item for item in yaml.safe_load_all(payload) if item is not None]
        input_digests = {
            str(item["metadata"]["annotations"]["loom.dev/render-input-sha256"])
            for item in documents
        }
        if len(input_digests) != 1:
            raise ValueError
        input_sha256 = input_digests.pop()
        validate_personal_dev_rollback_shadow_manifest(
            path,
            expected_sha256,
            expected_input_sha256=input_sha256,
            expected_release_sha256=release_sha256,
        )
        migrations = [
            item
            for item in documents
            if item.get("kind") == "Job"
            and item.get("metadata", {}).get("labels", {}).get("app")
            == "loom-personal-dev-migration"
        ]
        if len(migrations) != 1:
            raise ValueError
        container = migrations[0]["spec"]["template"]["spec"]["containers"]
        if len(container) != 1 or container[0]["image"] != service_image:
            raise ValueError
        identities = _manifest_identities(documents)
        migration_identity = _manifest_identity(migrations[0])
    except (KeyError, TypeError, ValueError, yaml.YAMLError):
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None
    return identities, migration_identity


def _manifest_identity(item: Any) -> _ManifestIdentity:
    api_version = item.get("apiVersion") if isinstance(item, dict) else None
    kind = item.get("kind") if isinstance(item, dict) else None
    metadata = item.get("metadata") if isinstance(item, dict) else None
    namespace = metadata.get("namespace", "") if isinstance(metadata, dict) else None
    name = metadata.get("name") if isinstance(metadata, dict) else None
    if (
        not isinstance(api_version, str)
        or not api_version
        or not isinstance(kind, str)
        or not kind
        or not isinstance(namespace, str)
        or not isinstance(name, str)
        or not name
    ):
        raise ValueError
    return api_version, kind, namespace, name


def _manifest_identities(documents: list[Any]) -> set[_ManifestIdentity]:
    identities = {_manifest_identity(item) for item in documents}
    if len(identities) != len(documents):
        raise ValueError
    return identities


def _git_output(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["/usr/bin/git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        env=_GIT_ENVIRONMENT,
        text=True,
        timeout=15,
    ).stdout


def _assert_exact_source_tree(root: Path, *, commit: str) -> None:
    object_format = _git_output(root, "rev-parse", "--show-object-format").strip()
    if object_format == "sha1":
        digest_factory = hashlib.sha1
    elif object_format == "sha256":
        digest_factory = hashlib.sha256
    else:
        raise ValueError
    listing = _git_output(root, "ls-tree", "-r", "-z", commit)
    seen: set[str] = set()
    for entry in listing.split("\0"):
        if not entry:
            continue
        metadata, separator, relative = entry.partition("\t")
        parts = metadata.split(" ")
        if (
            not separator
            or len(parts) != 3
            or parts[0] not in {"100644", "100755", "120000"}
            or parts[1] != "blob"
            or not relative
            or relative in seen
        ):
            raise ValueError
        seen.add(relative)
        path = root / relative
        before = path.lstat()
        if parts[0] == "120000":
            if (
                not stat.S_ISLNK(before.st_mode)
                or before.st_uid != os.getuid()
                or before.st_nlink != 1
            ):
                raise ValueError
            payload = os.readlink(os.fsencode(path))
            after = path.lstat()
            digest = digest_factory()
            digest.update(f"blob {len(payload)}\0".encode("ascii"))
            digest.update(payload)
            if _identity(after) != _identity(before) or digest.hexdigest() != parts[2]:
                raise ValueError
            continue
        expected_executable = parts[0] == "100755"
        if (
            not stat.S_ISREG(before.st_mode)
            or bool(before.st_mode & stat.S_IXUSR) != expected_executable
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        try:
            opened = os.fstat(descriptor)
            if _identity(opened) != _identity(before):
                raise ValueError
            digest = digest_factory()
            digest.update(f"blob {opened.st_size}\0".encode("ascii"))
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            after = path.lstat()
            if _identity(after) != _identity(opened) or digest.hexdigest() != parts[2]:
                raise ValueError
        finally:
            os.close(descriptor)
    if not seen:
        raise ValueError
    for relative_root in ("migrations", "src/loom", "src/loom_cli"):
        for candidate in (root / relative_root).rglob("*"):
            if candidate.is_symlink():
                raise ValueError
            if candidate.is_file() and (
                candidate.suffix in {".py", ".pyi"} or candidate.name.endswith(".so")
            ):
                relative = candidate.relative_to(root).as_posix()
                if relative not in seen:
                    raise ValueError


def _current_migration_job(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
) -> tuple[bytes, str, set[_ManifestIdentity], _ManifestIdentity]:
    rendered = render_shadow_personal_dev_control_plane(profile, release)
    try:
        documents = [item for item in yaml.safe_load_all(rendered.yaml_text) if item is not None]
        migrations = [
            item
            for item in documents
            if item.get("kind") == "Job"
            and item.get("metadata", {}).get("labels", {}).get("app")
            == "loom-personal-dev-migration"
        ]
        if len(migrations) != 1:
            raise ValueError
        job = migrations[0]
        identities = _manifest_identities(documents)
        migration_identity = _manifest_identity(job)
        containers = job["spec"]["template"]["spec"]["containers"]
        if (
            job["metadata"].get("namespace") != "loom-dev"
            or len(containers) != 1
            or containers[0].get("name") != "migrate"
            or containers[0].get("image") != release.images.loom_service
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, yaml.YAMLError):
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None
    return (
        _canonical_json(job),
        hashlib.sha256(rendered.yaml_text.encode("utf-8")).hexdigest(),
        identities,
        migration_identity,
    )


def validate_personal_dev_schema_transition_source_root(
    source_root: Path,
    *,
    release: PersonalDevTrustedRelease,
    alembic_ini_path: Path,
) -> None:
    """Require the selected clean checkout to own code, graph, commit, and tree."""

    try:
        root = source_root.resolve(strict=True)
        if (
            not root.is_dir()
            or source_root != root
            or alembic_ini_path.resolve(strict=True)
            != (root / "migrations" / "alembic.ini").resolve(strict=True)
            or Path(__file__).resolve(strict=True)
            != (root / "src" / "loom" / "personal_dev_schema_transition.py").resolve(strict=True)
            or Path(personal_dev_control_plane_render.__file__).resolve(strict=True)
            != (root / "src" / "loom" / "personal_dev_control_plane_render.py").resolve(strict=True)
        ):
            raise ValueError
        top_level = _git_output(root, "rev-parse", "--show-toplevel").strip()
        untracked = _git_output(
            root,
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
        )
        index_entries = _git_output(root, "ls-files", "-v", "-z", "--")
        commit = _git_output(root, "rev-parse", "HEAD").strip()
        tree = _git_output(root, "rev-parse", "HEAD^{tree}").strip()
        if (
            Path(top_level).resolve(strict=True) != root
            or untracked
            or any(
                entry[:1] == "S" or entry[:1].islower()
                for entry in index_entries.split("\0")
                if entry
            )
            or commit != release.source_sha
            or tree != release.source_tree
        ):
            raise ValueError
        _assert_exact_source_tree(root, commit=release.source_sha)
        if (
            _git_output(root, "rev-parse", "HEAD").strip() != release.source_sha
            or _git_output(root, "rev-parse", "HEAD^{tree}").strip() != release.source_tree
        ):
            raise ValueError
    except (OSError, subprocess.SubprocessError, ValueError):
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None


def prepare_personal_dev_schema_transition(
    *,
    profile: PersonalDevControlPlaneProfile,
    current_release: PersonalDevTrustedRelease,
    current_release_sha256: str,
    predecessor_release: PersonalDevTrustedRelease,
    predecessor_release_sha256: str,
    backup_evidence_path: Path,
    backup_evidence_sha256: str,
    postgres_dump_path: Path,
    postgres_source_state_path: Path,
    predecessor_shadow_path: Path,
    predecessor_shadow_sha256: str,
    alembic_ini_path: Path,
    expected_predecessor_head: str,
    expected_target_head: str,
) -> PreparedPersonalDevSchemaTransition:
    """Bind one forward migration to a proven full-restore predecessor."""

    try:
        if (
            _DIGEST.fullmatch(current_release_sha256) is None
            or _DIGEST.fullmatch(predecessor_release_sha256) is None
            or hashlib.sha256(current_release.canonical_bytes()).hexdigest()
            != current_release_sha256
            or hashlib.sha256(predecessor_release.canonical_bytes()).hexdigest()
            != predecessor_release_sha256
            or _REVISION.fullmatch(expected_predecessor_head) is None
            or _REVISION.fullmatch(expected_target_head) is None
            or expected_predecessor_head == expected_target_head
        ):
            raise ValueError
        target_head, ancestors, scripts = _single_target_and_revision_path(alembic_ini_path)
        if target_head != expected_target_head or expected_predecessor_head not in ancestors[1:]:
            raise ValueError
        predecessor_head, backup = _load_predecessor_backup(
            path=backup_evidence_path,
            expected_sha256=backup_evidence_sha256,
            release=predecessor_release,
            release_sha256=predecessor_release_sha256,
            candidate_heads=[expected_predecessor_head],
        )
        if predecessor_head != expected_predecessor_head:
            raise ValueError
        revisions = _linear_forward_revision_path(
            scripts,
            predecessor=predecessor_head,
            target=target_head,
        )
        if not revisions or revisions[-1] != target_head:
            raise ValueError
        dump_sha256 = _sha256_owner_only(postgres_dump_path)
        state_sha256 = _sha256_owner_only(postgres_source_state_path)
        if (
            dump_sha256 != backup.postgres.dump_sha256
            or state_sha256 != backup.postgres.source_state_sha256
            or backup.postgres.source_state_sha256 != backup.postgres.restored_state_sha256
        ):
            raise ValueError
        predecessor_identities, predecessor_migration_identity = _validate_predecessor_shadow(
            predecessor_shadow_path,
            expected_sha256=predecessor_shadow_sha256,
            release_sha256=predecessor_release_sha256,
            service_image=predecessor_release.images.loom_service,
        )
        (
            migration_job_json,
            current_shadow_sha256,
            current_identities,
            current_migration_identity,
        ) = _current_migration_job(
            profile,
            current_release,
        )
        if current_identities - predecessor_identities != {
            *_FORWARD_ONLY_IDENTITIES,
            current_migration_identity,
        } or predecessor_identities - current_identities != {predecessor_migration_identity}:
            raise ValueError
        migration_job = json.loads(migration_job_json)
        job_sha256 = hashlib.sha256(migration_job_json).hexdigest()
        plan: dict[str, object] = {
            "backup": {
                "evidence_sha256": backup_evidence_sha256,
                "postgres_dump_sha256": dump_sha256,
                "postgres_state_sha256": state_sha256,
            },
            "capacity": {"executable_new_capacity_ceiling": 0},
            "migration": {
                "job_name": migration_job["metadata"]["name"],
                "job_sha256": job_sha256,
                "revisions": revisions,
                "service_image": current_release.images.loom_service,
            },
            "namespace": "loom-dev",
            "predecessor": {
                "migration_job_name": predecessor_migration_identity[3],
                "release_sha256": predecessor_release_sha256,
                "schema_head": predecessor_head,
                "shadow_manifest_sha256": predecessor_shadow_sha256,
                "source_commit": predecessor_release.source_sha,
                "source_tree": predecessor_release.source_tree,
            },
            "rollback": {
                "delete_after_predecessor_apply": sorted(_FORWARD_ONLY_IDENTITIES.values()),
                "method": "full-predecessor-database-restore",
                "requires_exact_state_match": True,
            },
            "schema": "loom-personal-dev-schema-transition-plan-v1",
            "target": {
                "release_sha256": current_release_sha256,
                "schema_head": target_head,
                "shadow_manifest_sha256": current_shadow_sha256,
                "source_commit": current_release.source_sha,
                "source_tree": current_release.source_tree,
            },
        }
        plan_json = _canonical_json(plan)
    except (
        OSError,
        TypeError,
        ValueError,
        PersonalDevAcceptanceEvidenceError,
        PersonalDevSchemaTransitionError,
    ):
        raise PersonalDevSchemaTransitionError(
            "personal-dev schema transition inputs are invalid"
        ) from None
    return PreparedPersonalDevSchemaTransition(
        plan=plan,
        plan_json=plan_json,
        migration_job_json=migration_job_json,
    )


__all__ = [
    "PersonalDevSchemaTransitionError",
    "PreparedPersonalDevSchemaTransition",
    "prepare_personal_dev_schema_transition",
    "validate_personal_dev_schema_transition_source_root",
]
