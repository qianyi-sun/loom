#!/usr/bin/env python3
"""Load and validate Loom's component and test ownership authority."""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
import tomllib
from collections import Counter
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any


class ManifestError(ValueError):
    """The component authority is malformed or unsafe to consume."""


@dataclass(frozen=True)
class Component:
    id: str
    kind: str
    dockerfile: str
    build_context: str
    source_paths: tuple[str, ...]
    smoke_owner: str
    scan_owner: str
    attestation_owner: str
    release_digest: str | None
    runtime_policy: str
    rollout_role: str


@dataclass(frozen=True)
class TestSuite:
    id: str
    language: str
    include_paths: tuple[str, ...]
    exclude_paths: tuple[str, ...]
    lane: str
    execution_policy: str | None


@dataclass(frozen=True)
class FixtureFile:
    path: str
    content: str | None
    size: int | None


@dataclass(frozen=True)
class ExecutionPolicy:
    id: str
    language: str
    runner: str
    container_image: str
    virtual_root: str


@dataclass(frozen=True)
class ExecutionCase:
    path: str
    policy: str
    fixture_files: tuple[FixtureFile, ...]


@dataclass(frozen=True)
class Manifest:
    schema_version: int
    ci_lanes: tuple[str, ...]
    execution_policies: tuple[ExecutionPolicy, ...]
    execution_cases: tuple[ExecutionCase, ...]
    smoke_owners: tuple[str, ...]
    scan_owners: tuple[str, ...]
    attestation_owners: tuple[str, ...]
    components: tuple[Component, ...]
    test_suites: tuple[TestSuite, ...]

    def execution_policy(self, policy_id: str) -> ExecutionPolicy:
        matches = tuple(policy for policy in self.execution_policies if policy.id == policy_id)
        if len(matches) != 1:
            raise ManifestError(
                f"execution policy must have exactly one definition: {policy_id}"
            )
        return matches[0]

    def execution_case(self, path: str) -> ExecutionCase:
        matches = tuple(case for case in self.execution_cases if case.path == path)
        if len(matches) != 1:
            raise ManifestError(f"execution case must have exactly one definition: {path}")
        return matches[0]

    def component_owners_for_path(self, path: str) -> tuple[Component, ...]:
        normalized = _safe_path(path, context="query path")
        return tuple(
            component
            for component in self.components
            if component.dockerfile == normalized
            or any(matches_path(normalized, pattern) for pattern in component.source_paths)
        )

    def test_owners_for_path(self, path: str) -> tuple[TestSuite, ...]:
        normalized = _safe_path(path, context="query path")
        return tuple(
            suite
            for suite in self.test_suites
            if any(matches_path(normalized, pattern) for pattern in suite.include_paths)
            and not any(matches_path(normalized, pattern) for pattern in suite.exclude_paths)
        )

    def test_owner_for_path(self, path: str) -> TestSuite:
        owners = self.test_owners_for_path(path)
        if not owners:
            raise ManifestError(f"unowned test path: {path}")
        if len(owners) != 1:
            owner_ids = ", ".join(owner.id for owner in owners)
            raise ManifestError(f"ambiguous test path {path}: {owner_ids}")
        return owners[0]

    def release_components(self) -> tuple[Component, ...]:
        """Return the ordered release-image authority."""

        return tuple(
            component for component in self.components if component.kind == "release-image"
        )


@lru_cache(maxsize=512)
def _glob_regex(pattern: str) -> re.Pattern[str]:
    """Compile a repository-relative glob without letting ``*`` cross ``/``."""

    pieces: list[str] = ["^"]
    segments = pattern.split("/")
    for index, segment in enumerate(segments):
        if segment == "**":
            if index == len(segments) - 1:
                pieces.append(".*")
            else:
                pieces.append("(?:[^/]+/)*")
            continue

        for character in segment:
            if character == "*":
                pieces.append("[^/]*")
            elif character == "?":
                pieces.append("[^/]")
            else:
                pieces.append(re.escape(character))
        if index != len(segments) - 1:
            pieces.append("/")
    pieces.append("$")
    return re.compile("".join(pieces))


def matches_path(path: str, pattern: str) -> bool:
    """Return whether a normalized repository path matches a manifest glob."""

    return _glob_regex(pattern).fullmatch(path) is not None


def _reject_unknown_keys(raw: dict[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ManifestError(f"{context} has unknown keys: {', '.join(unknown)}")


def _required_string(raw: dict[str, Any], key: str, context: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise ManifestError(f"{context}.{key} must be a non-empty string")
    return value


def _required_slug(raw: dict[str, Any], key: str, context: str) -> str:
    value = _required_string(raw, key, context)
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", value) is None:
        raise ManifestError(f"{context}.{key} must be a lowercase slug")
    return value


def _safe_path(
    value: str,
    *,
    context: str,
    allow_dot: bool = False,
    allow_glob: bool = False,
) -> str:
    if allow_dot and value == ".":
        return value
    segments = value.split("/")
    unsafe = (
        not value
        or value.startswith("/")
        or value.endswith("/")
        or "\\" in value
        or any(ord(character) < 32 for character in value)
        or any(segment in {"", ".", ".."} for segment in segments)
    )
    if not allow_glob and any(character in value for character in "*?"):
        unsafe = True
    if unsafe:
        raise ManifestError(f"{context} must be a safe repository-relative path")
    return value


def _string_list(raw: dict[str, Any], key: str, context: str) -> tuple[str, ...]:
    value = raw.get(key)
    if not isinstance(value, list) or not value:
        raise ManifestError(f"{context}.{key} must be a non-empty string array")
    if not all(isinstance(item, str) for item in value):
        raise ManifestError(f"{context}.{key} must contain only strings")
    return tuple(value)


def _slug_registry(raw: dict[str, Any], key: str) -> tuple[str, ...]:
    value = raw.get(key, [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ManifestError(f"manifest.{key} must be a string array")
    registry = tuple(value)
    if len(set(registry)) != len(registry):
        raise ManifestError(f"manifest.{key} must not contain duplicates")
    for item in registry:
        if re.fullmatch(r"[a-z0-9][a-z0-9-]*", item) is None:
            raise ManifestError(f"invalid {key} slug: {item!r}")
    return registry


def _component(raw: dict[str, Any]) -> Component:
    context = f"component {raw.get('id', '<missing>')!r}"
    _reject_unknown_keys(
        raw,
        {
            "id",
            "kind",
            "dockerfile",
            "build_context",
            "source_paths",
            "smoke_owner",
            "scan_owner",
            "attestation_owner",
            "release_digest",
            "runtime_policy",
            "rollout_role",
        },
        context,
    )
    dockerfile = _safe_path(
        _required_string(raw, "dockerfile", context),
        context=f"{context}.dockerfile",
    )
    build_context = _safe_path(
        _required_string(raw, "build_context", context),
        context=f"{context}.build_context",
        allow_dot=True,
    )
    source_paths = tuple(
        _safe_path(item, context=f"{context}.source_paths", allow_glob=True)
        for item in _string_list(raw, "source_paths", context)
    )
    kind = _required_string(raw, "kind", context)
    runtime_policy = _required_string(raw, "runtime_policy", context)
    release_digest = (
        _required_string(raw, "release_digest", context) if "release_digest" in raw else None
    )
    rollout_role = raw.get("rollout_role", "none")
    if not isinstance(rollout_role, str) or rollout_role not in {
        "none",
        "primary",
        "auxiliary",
    }:
        raise ManifestError(
            f"{context}.rollout_role must be one of none, primary, auxiliary"
        )
    kind_contract_valid = (
        kind == "release-image"
        and runtime_policy in {"start", "conformance"}
        and release_digest is not None
        and re.fullmatch(r"loom-[a-z0-9][a-z0-9-]*", release_digest) is not None
    ) or (
        kind == "runtime-payload-image"
        and runtime_policy == "runtime-payload"
        and release_digest is None
    )
    if not kind_contract_valid:
        raise ManifestError(
            f"{context} violates component kind contract: release images require a unique "
            "loom-* digest and start/conformance policy; runtime payload images require no "
            "release digest and runtime-payload policy"
        )
    if rollout_role == "primary" and not (
        kind == "release-image" and runtime_policy == "start"
    ):
        raise ManifestError(
            f"{context}.rollout_role primary requires a start-policy release image"
        )
    if rollout_role == "auxiliary" and not (
        kind == "release-image" and runtime_policy == "conformance"
    ):
        raise ManifestError(
            f"{context}.rollout_role auxiliary requires a conformance release image"
        )
    return Component(
        id=_required_slug(raw, "id", context),
        kind=kind,
        dockerfile=dockerfile,
        build_context=build_context,
        source_paths=source_paths,
        smoke_owner=_required_slug(raw, "smoke_owner", context),
        scan_owner=_required_slug(raw, "scan_owner", context),
        attestation_owner=_required_slug(raw, "attestation_owner", context),
        release_digest=release_digest,
        runtime_policy=runtime_policy,
        rollout_role=rollout_role,
    )


def _test_suite(raw: dict[str, Any]) -> TestSuite:
    context = f"test suite {raw.get('id', '<missing>')!r}"
    _reject_unknown_keys(
        raw,
        {
            "id",
            "language",
            "lane",
            "include_paths",
            "exclude_paths",
            "execution_policy",
        },
        context,
    )
    lane = _required_slug(raw, "lane", context)
    execution_policy = raw.get("execution_policy")
    if execution_policy is not None:
        if not isinstance(execution_policy, str) or re.fullmatch(
            r"[a-z0-9][a-z0-9-]*", execution_policy
        ) is None:
            raise ManifestError(f"{context}.execution_policy must be a lowercase slug")
    include_paths = tuple(
        _safe_path(item, context=f"{context}.include_paths", allow_glob=True)
        for item in _string_list(raw, "include_paths", context)
    )
    raw_exclude_paths = raw.get("exclude_paths", [])
    if not isinstance(raw_exclude_paths, list) or not all(
        isinstance(item, str) for item in raw_exclude_paths
    ):
        raise ManifestError(f"{context}.exclude_paths must be a string array")
    exclude_paths = tuple(
        _safe_path(item, context=f"{context}.exclude_paths", allow_glob=True)
        for item in raw_exclude_paths
    )
    language = _required_string(raw, "language", context)
    if language not in {"python", "go", "web"}:
        raise ManifestError(f"{context}.language must be one of python, go, web")
    return TestSuite(
        id=_required_slug(raw, "id", context),
        language=language,
        include_paths=include_paths,
        exclude_paths=exclude_paths,
        lane=lane,
        execution_policy=execution_policy,
    )


def _fixture_file(raw: dict[str, Any], *, context: str) -> FixtureFile:
    _reject_unknown_keys(raw, {"path", "content", "size"}, context)
    path = _safe_path(_required_string(raw, "path", context), context=f"{context}.path")
    content = raw.get("content")
    size = raw.get("size")
    if (content is None) == (size is None):
        raise ManifestError(f"{context} must set exactly one of content or size")
    if content is not None and not isinstance(content, str):
        raise ManifestError(f"{context}.content must be a string")
    if size is not None and (type(size) is not int or size <= 0):
        raise ManifestError(f"{context}.size must be a positive integer")
    return FixtureFile(path=path, content=content, size=size)


def _execution_policy(raw: dict[str, Any]) -> ExecutionPolicy:
    context = f"execution policy {raw.get('id', '<missing>')!r}"
    _reject_unknown_keys(
        raw,
        {
            "id",
            "language",
            "runner",
            "container_image",
            "virtual_root",
        },
        context,
    )
    policy_id = _required_slug(raw, "id", context)
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*-v[1-9][0-9]*", policy_id) is None:
        raise ManifestError(f"{context}.id must end in an explicit -vN version")
    language = _required_string(raw, "language", context)
    if language != "python":
        raise ManifestError(f"{context}.language must be python")
    runner = _required_string(raw, "runner", context)
    if runner != "python-zero-arg-v1":
        raise ManifestError(f"{context}.runner is unsupported: {runner}")
    container_image = _required_string(raw, "container_image", context)
    if re.fullmatch(r"[a-z0-9][a-z0-9./_-]*@sha256:[0-9a-f]{64}", container_image) is None:
        raise ManifestError(
            f"{context}.container_image must be pinned by a full sha256 digest"
        )
    virtual_root = _required_string(raw, "virtual_root", context)
    if virtual_root not in {"/app", "/workspace"}:
        raise ManifestError(f"{context}.virtual_root must be /app or /workspace")
    return ExecutionPolicy(
        id=policy_id,
        language=language,
        runner=runner,
        container_image=container_image,
        virtual_root=virtual_root,
    )


def _execution_case(raw: dict[str, Any], *, index: int) -> ExecutionCase:
    context = f"execution case [{index}]"
    _reject_unknown_keys(raw, {"path", "policy", "fixture_files"}, context)
    path = _safe_path(
        _required_string(raw, "path", context),
        context=f"{context}.path",
    )
    policy = _required_slug(raw, "policy", context)
    raw_files = raw.get("fixture_files")
    if not isinstance(raw_files, list) or not raw_files or not all(
        isinstance(item, dict) for item in raw_files
    ):
        raise ManifestError(f"{context}.fixture_files must be a non-empty table array")
    fixture_files = tuple(
        _fixture_file(item, context=f"{context}.fixture_files[{index}]")
        for index, item in enumerate(raw_files)
    )
    paths = [item.path for item in fixture_files]
    if len(set(paths)) != len(paths):
        raise ManifestError(f"{context}.fixture_files must not contain duplicate paths")
    total_size = sum(
        item.size if item.size is not None else len((item.content or "").encode("utf-8"))
        for item in fixture_files
    )
    if len(fixture_files) > 32 or total_size > 128 * 1024 * 1024:
        raise ManifestError(f"{context}.fixture_files exceed conformance resource limits")
    return ExecutionCase(path=path, policy=policy, fixture_files=fixture_files)


def load_manifest(path: Path) -> Manifest:
    """Load the TOML authority as immutable typed data."""

    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ManifestError(f"failed to read {path}: {exc}") from exc
    _reject_unknown_keys(
        raw,
        {
            "schema_version",
            "ci_lanes",
            "execution_policies",
            "execution_cases",
            "smoke_owners",
            "scan_owners",
            "attestation_owners",
            "components",
            "test_suites",
        },
        "manifest",
    )
    schema_version = raw.get("schema_version")
    if type(schema_version) is not int or schema_version != 2:
        raise ManifestError("manifest.schema_version must be the integer 2")
    raw_components = raw.get("components", [])
    raw_test_suites = raw.get("test_suites", [])
    raw_ci_lanes = raw.get("ci_lanes", [])
    if not isinstance(raw_ci_lanes, list) or not all(
        isinstance(item, str) for item in raw_ci_lanes
    ):
        raise ManifestError("manifest.ci_lanes must be a string array")
    ci_lanes = tuple(raw_ci_lanes)
    if len(set(ci_lanes)) != len(ci_lanes):
        raise ManifestError("manifest.ci_lanes must not contain duplicates")
    for lane in ci_lanes:
        if re.fullmatch(r"[a-z0-9][a-z0-9-]*", lane) is None:
            raise ManifestError(f"invalid CI lane slug: {lane!r}")
    if not isinstance(raw_components, list) or not all(
        isinstance(item, dict) for item in raw_components
    ):
        raise ManifestError("manifest.components must be an array of tables")
    if not isinstance(raw_test_suites, list) or not all(
        isinstance(item, dict) for item in raw_test_suites
    ):
        raise ManifestError("manifest.test_suites must be an array of tables")
    raw_execution_policies = raw.get("execution_policies", [])
    if not isinstance(raw_execution_policies, list) or not all(
        isinstance(item, dict) for item in raw_execution_policies
    ):
        raise ManifestError("manifest.execution_policies must be an array of tables")
    execution_policies = tuple(_execution_policy(item) for item in raw_execution_policies)
    execution_policy_ids = Counter(policy.id for policy in execution_policies)
    for policy_id, count in sorted(execution_policy_ids.items()):
        if count > 1:
            raise ManifestError(f"execution policy has multiple definitions: {policy_id}")
    raw_execution_cases = raw.get("execution_cases", [])
    if not isinstance(raw_execution_cases, list) or not all(
        isinstance(item, dict) for item in raw_execution_cases
    ):
        raise ManifestError("manifest.execution_cases must be an array of tables")
    execution_cases = tuple(
        _execution_case(item, index=index)
        for index, item in enumerate(raw_execution_cases)
    )
    execution_case_paths = Counter(case.path for case in execution_cases)
    for case_path, count in sorted(execution_case_paths.items()):
        if count > 1:
            raise ManifestError(f"execution case has multiple definitions: {case_path}")
    declared_policy_ids = {policy.id for policy in execution_policies}
    for case in execution_cases:
        if case.policy not in declared_policy_ids:
            raise ManifestError(
                f"execution case {case.path!r} uses undeclared execution policy: "
                f"{case.policy}"
            )
    test_suites = tuple(_test_suite(item) for item in raw_test_suites)
    for suite in test_suites:
        if suite.lane not in ci_lanes:
            raise ManifestError(f"test suite {suite.id!r} uses undeclared CI lane: {suite.lane}")
        runtime_payload = suite.lane == "runtime-payload"
        if runtime_payload != (suite.execution_policy is not None):
            raise ManifestError(
                f"test suite {suite.id!r} must set execution_policy exactly when "
                "lane is runtime-payload"
            )
        if runtime_payload and suite.language != "python":
            raise ManifestError(
                f"test suite {suite.id!r} runtime-payload lane requires Python"
            )
        if suite.execution_policy is not None and suite.execution_policy not in {
            policy.id for policy in execution_policies
        }:
            raise ManifestError(
                f"test suite {suite.id!r} uses undeclared execution policy: "
                f"{suite.execution_policy}"
            )
    return Manifest(
        schema_version=2,
        ci_lanes=ci_lanes,
        execution_policies=execution_policies,
        execution_cases=execution_cases,
        smoke_owners=_slug_registry(raw, "smoke_owners"),
        scan_owners=_slug_registry(raw, "scan_owners"),
        attestation_owners=_slug_registry(raw, "attestation_owners"),
        components=tuple(_component(item) for item in raw_components),
        test_suites=test_suites,
    )


def _is_dockerfile(path: str) -> bool:
    name = PurePosixPath(path).name
    return name == "Dockerfile" or name.startswith("Dockerfile.")


def _test_language(path: str) -> str | None:
    pure_path = PurePosixPath(path)
    parts = pure_path.parts
    if path.endswith(".py") and ("tests" in parts or pure_path.name.startswith("test_")):
        return "python"
    if path.endswith("_test.go"):
        return "go"
    if re.search(r"\.(?:test|spec)\.(?:js|jsx|ts|tsx)$", path):
        return "web"
    return None


def _is_runnable_test_path(path: str) -> bool:
    """Return whether a tracked test-owned path is an executable test module."""

    language = _test_language(path)
    if language != "python":
        return language is not None
    name = PurePosixPath(path).name
    return name.startswith("test_") or name.endswith("_test.py")


def _uses_pytest_docker_marker(source: str) -> bool:
    tree = ast.parse(source)
    return any(
        isinstance(node, ast.Attribute)
        and node.attr == "docker"
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "pytest"
        for node in ast.walk(tree)
    )


def validate_manifest(
    manifest: Manifest,
    *,
    repo_root: Path,
    tracked_paths: tuple[str, ...],
) -> list[str]:
    """Return deterministic authority errors for tracked repository inputs."""

    errors: list[str] = []
    component_ids = Counter(component.id for component in manifest.components)
    for component_id, count in sorted(component_ids.items()):
        if count > 1:
            errors.append(f"component id has multiple definitions: {component_id}")
    test_suite_ids = Counter(suite.id for suite in manifest.test_suites)
    for suite_id, count in sorted(test_suite_ids.items()):
        if count > 1:
            errors.append(f"test suite id has multiple definitions: {suite_id}")
    for lane in manifest.ci_lanes:
        if not any(suite.lane == lane for suite in manifest.test_suites):
            errors.append(f"CI lane has no test suite owner: {lane}")
    for policy in manifest.execution_policies:
        if not any(suite.execution_policy == policy.id for suite in manifest.test_suites):
            errors.append(f"execution policy has no test suite owner: {policy.id}")
    runtime_paths = (
        set(
            test_paths_for_lane(
                manifest,
                tracked_paths=tracked_paths,
                lane="runtime-payload",
            )
        )
        if "runtime-payload" in manifest.ci_lanes
        else set()
    )
    case_paths = {case.path for case in manifest.execution_cases}
    for path in sorted(runtime_paths - case_paths):
        errors.append(f"runtime payload has no execution case: {path}")
    for path in sorted(case_paths - runtime_paths):
        errors.append(f"execution case does not own a tracked runtime payload: {path}")
    for case in manifest.execution_cases:
        owners = manifest.test_owners_for_path(case.path)
        if len(owners) == 1 and owners[0].execution_policy != case.policy:
            errors.append(
                f"execution case policy differs from test owner: {case.path}: "
                f"{case.policy} != {owners[0].execution_policy}"
            )
    owner_registries = (
        ("smoke", manifest.smoke_owners, "smoke_owner"),
        ("scan", manifest.scan_owners, "scan_owner"),
        ("attestation", manifest.attestation_owners, "attestation_owner"),
    )
    for owner_kind, registry, field_name in owner_registries:
        for owner in registry:
            if not any(
                getattr(component, field_name) == owner for component in manifest.components
            ):
                errors.append(f"{owner_kind} owner has no component: {owner}")
    tracked_set = set(tracked_paths)
    resolved_repo_root = repo_root.resolve()
    for component in manifest.components:
        for owner_kind, registry, field_name in owner_registries:
            owner = getattr(component, field_name)
            if owner not in registry:
                errors.append(
                    f"component {owner_kind} owner is undeclared: {component.id}: {owner}"
                )
        if component.dockerfile not in tracked_set:
            errors.append(
                f"component Dockerfile is not tracked: {component.id}: {component.dockerfile}"
            )
        build_context_path = (repo_root / component.build_context).resolve()
        dockerfile_path = (repo_root / component.dockerfile).resolve()
        if not build_context_path.is_dir():
            errors.append(
                f"component build context is not a directory: {component.id}: "
                f"{component.build_context}"
            )
        elif not build_context_path.is_relative_to(
            resolved_repo_root
        ) or not dockerfile_path.is_relative_to(build_context_path):
            errors.append(
                f"component Dockerfile is outside build context: {component.id}: "
                f"{component.dockerfile} not under {component.build_context}"
            )
        if not any(
            matches_path(component.dockerfile, pattern) for pattern in component.source_paths
        ):
            errors.append(
                f"component source paths do not own Dockerfile: {component.id}: "
                f"{component.dockerfile}"
            )
        for pattern in component.source_paths:
            if not any(matches_path(path, pattern) for path in tracked_paths):
                errors.append(
                    f"component source pattern matches no tracked path: {component.id}: {pattern}"
                )
        expected_kind = (
            "release-image"
            if component.dockerfile.startswith("deploy/Dockerfile.")
            else "runtime-payload-image"
        )
        if component.kind != expected_kind:
            errors.append(
                f"component kind does not match Dockerfile location: {component.id}: "
                f"{component.kind}; expected {expected_kind}"
            )
    release_digests: dict[str, list[str]] = {}
    for component in manifest.components:
        if component.release_digest is not None:
            release_digests.setdefault(component.release_digest, []).append(component.id)
    for digest, owner_ids in sorted(release_digests.items()):
        if len(owner_ids) > 1:
            errors.append(
                f"release digest has multiple component owners: {digest}: " + ", ".join(owner_ids)
            )
    for path in sorted(item for item in tracked_paths if _is_dockerfile(item)):
        component_owners = [
            component for component in manifest.components if component.dockerfile == path
        ]
        if not component_owners:
            errors.append(f"Dockerfile has no component owner: {path}")
        elif len(component_owners) > 1:
            errors.append(
                f"Dockerfile has multiple component owners: {path}: "
                + ", ".join(component.id for component in component_owners)
            )
    tracked_tests_by_language = {
        language: tuple(path for path in tracked_paths if _test_language(path) == language)
        for language in ("python", "go", "web")
    }
    for suite in manifest.test_suites:
        language_tests = tracked_tests_by_language[suite.language]
        for pattern in suite.include_paths:
            if not any(matches_path(path, pattern) for path in language_tests):
                errors.append(
                    f"test include pattern matches no tracked test: {suite.id}: {pattern}"
                )
        for pattern in suite.exclude_paths:
            if not any(matches_path(path, pattern) for path in language_tests):
                errors.append(
                    f"test exclude pattern matches no tracked test: {suite.id}: {pattern}"
                )
    for path in sorted(tracked_paths):
        language = _test_language(path)
        if language is None:
            continue
        test_owners = manifest.test_owners_for_path(path)
        if not test_owners:
            errors.append(f"{language} test has no CI owner: {path}")
        elif len(test_owners) > 1:
            errors.append(
                f"{language} test has multiple CI owners: {path}: "
                + ", ".join(owner.id for owner in test_owners)
            )
        elif test_owners[0].language != language:
            errors.append(
                f"{language} test has mismatched owner language: {path}: "
                f"{test_owners[0].id} declares {test_owners[0].language}"
            )
        elif language == "python":
            try:
                source = (repo_root / path).read_text(encoding="utf-8")
            except OSError as exc:
                errors.append(f"tracked test cannot be read: {path}: {exc}")
                continue
            try:
                uses_docker_marker = _uses_pytest_docker_marker(source)
            except SyntaxError as exc:
                errors.append(
                    f"tracked Python test cannot be parsed: {path}: "
                    f"line {exc.lineno or '?'}: {exc.msg}"
                )
                continue
            if uses_docker_marker and test_owners[0].lane != "integration-docker":
                errors.append(
                    "docker-marked pytest module must use integration-docker lane: "
                    f"{path}: {test_owners[0].lane}"
                )
    return errors


def validate_release_image_ownership(
    manifest: Manifest,
    release_images: tuple[tuple[str, str], ...],
) -> list[str]:
    """Validate a consumer's image-name/Dockerfile pairs against the authority."""

    errors: list[str] = []
    image_names = Counter(image_name for image_name, _ in release_images)
    for image_name, count in sorted(image_names.items()):
        if count > 1:
            errors.append(f"consumer release image name is duplicated: {image_name}")
    dockerfiles = Counter(dockerfile for _, dockerfile in release_images)
    for dockerfile, count in sorted(dockerfiles.items()):
        if count > 1:
            errors.append(f"consumer release Dockerfile is duplicated: {dockerfile}")
    for image_name, dockerfile in release_images:
        owners = [item for item in manifest.components if item.dockerfile == dockerfile]
        if not owners:
            errors.append(f"release image has no component owner: {image_name}: {dockerfile}")
            continue
        if len(owners) > 1:
            errors.append(
                f"release image has multiple component owners: {image_name}: {dockerfile}: "
                + ", ".join(owner.id for owner in owners)
            )
            continue
        owner = owners[0]
        if owner.kind != "release-image":
            errors.append(f"release image is owned as {owner.kind}: {image_name}: {dockerfile}")
        if owner.release_digest != image_name:
            errors.append(
                f"release image digest mismatch: {dockerfile}: "
                f"manifest={owner.release_digest} consumer={image_name}"
            )
    return errors


def release_image_matrix(manifest: Manifest) -> tuple[dict[str, str], ...]:
    """Render the image workflow matrix from the component authority."""

    return tuple(
        {
            "image": component.id,
            "image_name": component.release_digest or "",
            "dockerfile": component.dockerfile,
            "context": component.build_context,
        }
        for component in manifest.release_components()
    )


def native_release_image_matrix(
    images: tuple[dict[str, str], ...],
) -> tuple[dict[str, str], ...]:
    """Expand selected images into deterministic native architecture builds."""

    architectures = (
        {"architecture": "amd64", "platform": "linux/amd64"},
        {"architecture": "arm64", "platform": "linux/arm64"},
    )
    return tuple(
        {**image, **architecture}
        for image in images
        for architecture in architectures
    )


def release_images_for_runtime_policy(
    manifest: Manifest,
    *,
    runtime_policy: str,
) -> tuple[dict[str, str], ...]:
    """Render candidate-bound rollout inputs for one runtime policy."""

    if runtime_policy not in {"start", "conformance"}:
        raise ManifestError(f"unsupported release runtime policy: {runtime_policy}")
    return tuple(
        {
            "image_name": component.release_digest or "",
            "dockerfile": component.dockerfile,
            "context": component.build_context,
        }
        for component in manifest.release_components()
        if component.runtime_policy == runtime_policy
    )


def release_images_for_rollout_role(
    manifest: Manifest,
    *,
    rollout_role: str,
) -> tuple[dict[str, str], ...]:
    """Render candidate-bound rollout inputs for one explicit rollout role."""

    if rollout_role not in {"primary", "auxiliary"}:
        raise ManifestError(f"unsupported release rollout role: {rollout_role}")
    return tuple(
        {
            "image_name": component.release_digest or "",
            "dockerfile": component.dockerfile,
            "context": component.build_context,
        }
        for component in manifest.release_components()
        if component.rollout_role == rollout_role
    )


def select_release_image_matrix(
    manifest: Manifest,
    *,
    changed_paths: tuple[str, ...],
    force_all: bool,
    fallback_all: bool = False,
) -> tuple[dict[str, str], ...]:
    """Select release images whose manifest-owned inputs changed."""

    release_components = manifest.release_components()
    if force_all or not changed_paths:
        selected_ids = {component.id for component in release_components}
    else:
        selected_ids = {
            component.id
            for path in changed_paths
            for component in manifest.component_owners_for_path(path)
            if component.kind == "release-image"
        }
        if any(
            path
            in {
                ".github/workflows/images.yml",
                "config/component-ownership.toml",
                "scripts/component_ownership.py",
            }
            for path in changed_paths
        ):
            selected_ids = {component.id for component in release_components}
    matrix = tuple(
        entry for entry in release_image_matrix(manifest) if entry["image"] in selected_ids
    )
    if fallback_all and not matrix:
        return release_image_matrix(manifest)
    return matrix


def validate_release_image_pair(
    manifest: Manifest,
    *,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
) -> list[str]:
    """Validate an untrusted workflow matrix row against the authority."""

    matches = [component for component in manifest.release_components() if component.id == image]
    if len(matches) != 1:
        return [f"release image id must have exactly one owner: {image}"]
    component = matches[0]
    observed = (image_name, dockerfile, build_context)
    expected = (
        component.release_digest or "",
        component.dockerfile,
        component.build_context,
    )
    if observed != expected:
        return [
            "release image matrix row differs from manifest: "
            f"{image}: observed={observed!r} expected={expected!r}"
        ]
    return []


def test_paths_for_lane(
    manifest: Manifest,
    *,
    tracked_paths: tuple[str, ...],
    lane: str,
) -> tuple[str, ...]:
    """Return every tracked test path assigned to one required CI lane."""

    if lane not in manifest.ci_lanes:
        raise ManifestError(f"undeclared CI lane: {lane}")
    return tuple(
        path
        for path in sorted(tracked_paths)
        if _is_runnable_test_path(path)
        and len(owners := manifest.test_owners_for_path(path)) == 1
        and owners[0].lane == lane
    )


def test_paths_for_policy(
    manifest: Manifest,
    *,
    tracked_paths: tuple[str, ...],
    policy: str,
) -> tuple[str, ...]:
    """Return runnable payload tests assigned to one declared execution policy."""

    if policy not in {item.id for item in manifest.execution_policies}:
        raise ManifestError(f"undeclared execution policy: {policy}")
    return tuple(
        path
        for path in sorted(tracked_paths)
        if _is_runnable_test_path(path)
        and len(owners := manifest.test_owners_for_path(path)) == 1
        and owners[0].execution_policy == policy
    )


def lane_execution_plan(
    manifest: Manifest,
    *,
    tracked_paths: tuple[str, ...],
    lane: str,
) -> tuple[dict[str, Any], ...]:
    """Render the exact policy-grouped execution plan for a policy lane."""

    lane_paths = test_paths_for_lane(
        manifest,
        tracked_paths=tracked_paths,
        lane=lane,
    )
    if not lane_paths:
        raise ManifestError(f"CI lane has no tracked test paths: {lane}")
    if any(
        manifest.test_owner_for_path(path).execution_policy is None for path in lane_paths
    ):
        raise ManifestError(f"CI lane is not fully policy-owned: {lane}")
    cases_by_path = {case.path: case for case in manifest.execution_cases}
    if set(cases_by_path) != set(lane_paths):
        raise ManifestError(f"execution cases do not exactly cover CI lane: {lane}")
    for path in lane_paths:
        if cases_by_path[path].policy != manifest.test_owner_for_path(path).execution_policy:
            raise ManifestError(f"execution case policy differs from CI lane owner: {path}")
    plan: tuple[dict[str, Any], ...] = tuple(
        {
            "policy": asdict(policy),
            "cases": [
                asdict(cases_by_path[path])
                for path in lane_paths
                if cases_by_path[path].policy == policy.id
            ],
        }
        for policy in manifest.execution_policies
        if any(
            manifest.test_owner_for_path(path).execution_policy == policy.id
            for path in lane_paths
        )
    )
    planned_paths = [case["path"] for entry in plan for case in entry["cases"]]
    if len(planned_paths) != len(set(planned_paths)) or set(planned_paths) != set(lane_paths):
        raise ManifestError(f"execution plan does not exactly cover CI lane: {lane}")
    return plan


def _tracked_paths(repo_root: Path) -> tuple[str, ...]:
    try:
        output = subprocess.check_output(
            ["git", "ls-files", "-z"],
            cwd=repo_root,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ManifestError(f"failed to list tracked repository paths: {exc}") from exc
    return tuple(item.decode("utf-8") for item in output.split(b"\0") if item)


def shard_paths(
    paths: tuple[str, ...],
    *,
    shard_index: int,
    shard_count: int,
    strategy: str = "round-robin",
) -> tuple[str, ...]:
    """Partition an ordered manifest-owned path list without a second path authority."""

    if shard_count < 1:
        raise ManifestError("shard count must be at least 1")
    if shard_index < 0 or shard_index >= shard_count:
        raise ManifestError(
            f"shard index must be between 0 and {shard_count - 1}: {shard_index}"
        )
    if strategy == "round-robin":
        return paths[shard_index::shard_count]
    if strategy == "contiguous":
        base_size, larger_shards = divmod(len(paths), shard_count)
        start = shard_index * base_size + min(shard_index, larger_shards)
        stop = start + base_size + int(shard_index < larger_shards)
        return paths[start:stop]
    raise ManifestError(f"unsupported shard strategy: {strategy}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or query Loom's component and test ownership authority.",
    )
    repo_root = Path(__file__).resolve().parents[1]
    parser.add_argument("--repo-root", type=Path, default=repo_root)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Manifest path (defaults to <repo-root>/config/component-ownership.toml).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate", help="Validate all tracked Dockerfiles and tests.")
    query = subparsers.add_parser("query", help="Print the owners for one repository path.")
    query.add_argument("path")
    plan_images = subparsers.add_parser(
        "plan-images",
        help="Write the affected release-image matrix to GitHub output.",
    )
    plan_images.add_argument("--changed-files", type=Path, required=True)
    plan_images.add_argument("--github-output", type=Path, required=True)
    plan_images.add_argument("--force-all", action="store_true")
    plan_images.add_argument("--fallback-all", action="store_true")
    validate_image = subparsers.add_parser(
        "validate-image",
        help="Validate one untrusted image workflow matrix row.",
    )
    validate_image.add_argument("--image", required=True)
    validate_image.add_argument("--image-name", required=True)
    validate_image.add_argument("--dockerfile", required=True)
    validate_image.add_argument("--build-context", required=True)
    test_paths = subparsers.add_parser(
        "test-paths",
        help="Print every tracked test path assigned to one CI lane.",
    )
    test_paths.add_argument("--lane", required=True)
    test_paths.add_argument("--shard-index", type=int, default=0)
    test_paths.add_argument("--shard-count", type=int, default=1)
    test_paths.add_argument(
        "--shard-strategy",
        choices=("round-robin", "contiguous"),
        default="round-robin",
    )
    execution_plan = subparsers.add_parser(
        "execution-plan",
        help="Print a JSON execution plan for one policy-owned CI lane.",
    )
    execution_plan.add_argument("--lane", required=True)
    release_images = subparsers.add_parser(
        "release-images",
        help="Print candidate-bound release images for one policy or rollout role as JSON.",
    )
    release_filter = release_images.add_mutually_exclusive_group(required=True)
    release_filter.add_argument(
        "--runtime-policy",
        choices=("start", "conformance"),
    )
    release_filter.add_argument(
        "--rollout-role",
        choices=("primary", "auxiliary"),
    )
    return parser


def _query_payload(manifest: Manifest, path: str) -> dict[str, Any]:
    components = manifest.component_owners_for_path(path)
    test_owners = manifest.test_owners_for_path(path)
    if len(test_owners) > 1:
        owner_ids = ", ".join(owner.id for owner in test_owners)
        raise ManifestError(f"ambiguous test path {path}: {owner_ids}")
    if not components and not test_owners:
        raise ManifestError(f"unowned path: {path}")
    return {
        "path": path,
        "components": [asdict(component) for component in components],
        "test_owners": [asdict(owner) for owner in test_owners],
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    manifest_path = args.manifest or repo_root / "config/component-ownership.toml"
    try:
        manifest = load_manifest(manifest_path)
        tracked_paths = _tracked_paths(repo_root)
        errors = validate_manifest(
            manifest,
            repo_root=repo_root,
            tracked_paths=tracked_paths,
        )
        if errors:
            print("FAIL: component ownership validation failed:", file=sys.stderr)
            for error in errors:
                print(f"- {error}", file=sys.stderr)
            return 1
        if args.command == "query":
            print(json.dumps(_query_payload(manifest, args.path), sort_keys=True))
            return 0
        if args.command == "plan-images":
            changed_paths = tuple(
                line.strip()
                for line in args.changed_files.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
            matrix = select_release_image_matrix(
                manifest,
                changed_paths=changed_paths,
                force_all=args.force_all,
                fallback_all=args.fallback_all,
            )
            payload = json.dumps(matrix, separators=(",", ":"))
            native_payload = json.dumps(
                native_release_image_matrix(matrix),
                separators=(",", ":"),
            )
            with args.github_output.open("a", encoding="utf-8") as handle:
                handle.write(f"images={payload}\n")
                handle.write(f"native_builds={native_payload}\n")
                handle.write(f"required={str(bool(matrix)).lower()}\n")
            print(f"selected_images={payload}")
            return 0
        if args.command == "validate-image":
            errors = validate_release_image_pair(
                manifest,
                image=args.image,
                image_name=args.image_name,
                dockerfile=args.dockerfile,
                build_context=args.build_context,
            )
            if errors:
                print("FAIL: component ownership validation failed:", file=sys.stderr)
                for error in errors:
                    print(f"- {error}", file=sys.stderr)
                return 1
            return 0
        if args.command == "test-paths":
            paths = test_paths_for_lane(
                manifest,
                tracked_paths=tracked_paths,
                lane=args.lane,
            )
            if not paths:
                raise ManifestError(f"CI lane has no tracked test paths: {args.lane}")
            paths = shard_paths(
                paths,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                strategy=args.shard_strategy,
            )
            if not paths:
                raise ManifestError(
                    f"CI lane shard has no tracked test paths: {args.lane} "
                    f"({args.shard_index}/{args.shard_count})"
                )
            print("\n".join(paths))
            return 0
        if args.command == "execution-plan":
            print(
                json.dumps(
                    lane_execution_plan(
                        manifest,
                        tracked_paths=tracked_paths,
                        lane=args.lane,
                    ),
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "release-images":
            images = (
                release_images_for_rollout_role(
                    manifest,
                    rollout_role=args.rollout_role,
                )
                if args.rollout_role is not None
                else release_images_for_runtime_policy(
                    manifest,
                    runtime_policy=args.runtime_policy,
                )
            )
            print(
                json.dumps(
                    images,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            return 0
        test_count = sum(_test_language(path) is not None for path in tracked_paths)
        print(
            f"validated {len(manifest.components)} components and {test_count} tracked test files"
        )
        return 0
    except ManifestError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
