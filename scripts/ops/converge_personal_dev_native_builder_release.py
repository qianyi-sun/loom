#!/usr/bin/env python3
"""Converge exact native-builder images without daemon-wide garbage collection."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import NoReturn, Protocol, cast

_PRIMARY_ENDPOINT = "unix:///var/run/docker.sock"
_DEDICATED_ENDPOINT = "unix:///run/loom-personal-dev-builder/docker.sock"
_AGENT_REPOSITORY = (
    "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent"
)
_BUILDER_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-builder"
_SOURCE = "https://github.com/qianyi-sun/loom"
_MANAGED_LABEL = "io.loom.personal-dev.native-builder.release-managed"
_SCHEMA = "loom.personal-dev-native-builder-release-convergence.v1"
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_MAX_COMMAND_OUTPUT = 4 * 1024 * 1024
_COMMAND_TIMEOUT_SECONDS = 300
_ROOT_ENV = {
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}


class PersonalDevNativeBuilderReleaseError(RuntimeError):
    """Release images cannot converge without broadening mutation scope."""

    def __init__(self, code: str) -> None:
        safe_code = (
            code
            if isinstance(code, str) and _ERROR_CODE.fullmatch(code)
            else "internal_error"
        )
        self.code = safe_code
        super().__init__(safe_code)


@dataclass(frozen=True, slots=True)
class NativeBuilderReleaseImage:
    reference: str
    revision: str


@dataclass(frozen=True, slots=True)
class NativeBuilderReleaseConfig:
    current_agent: NativeBuilderReleaseImage
    current_builder: NativeBuilderReleaseImage
    previous_agent: NativeBuilderReleaseImage | None = None
    previous_builder: NativeBuilderReleaseImage | None = None


@dataclass(frozen=True, slots=True)
class NativeBuilderImageRecord:
    image_id: str
    repo_digests: tuple[str, ...]
    os: str
    architecture: str
    labels: Mapping[str, str]


class NativeBuilderDockerApi(Protocol):
    endpoint: str

    def images(self) -> Sequence[NativeBuilderImageRecord]: ...
    def pull(self, reference: str) -> None: ...
    def containers_using(self, image_id: str) -> Sequence[str]: ...
    def remove(self, reference: str) -> None: ...


@dataclass(frozen=True, slots=True)
class _EndpointPlan:
    endpoint: str
    pull: tuple[str, ...]
    retain: tuple[str, ...]
    remove: tuple[str, ...]
    removal_image_ids: Mapping[str, str]

    def public(self) -> dict[str, object]:
        return {
            "endpoint": self.endpoint,
            "pull": list(self.pull),
            "remove": list(self.remove),
            "retain": list(self.retain),
        }


def _repository_reference(repository: str, reference: str) -> bool:
    prefix = repository + "@"
    if not reference.startswith(prefix):
        return False
    return _DIGEST.fullmatch(reference[len(prefix) :]) is not None


def _validate_release_image(
    release: NativeBuilderReleaseImage,
    *,
    repository: str,
) -> None:
    if (
        not isinstance(release, NativeBuilderReleaseImage)
        or not isinstance(release.reference, str)
        or not _repository_reference(repository, release.reference)
        or not isinstance(release.revision, str)
        or _REVISION.fullmatch(release.revision) is None
    ):
        raise PersonalDevNativeBuilderReleaseError("release_binding_invalid")


def _validate_config(config: NativeBuilderReleaseConfig) -> None:
    if not isinstance(config, NativeBuilderReleaseConfig):
        raise PersonalDevNativeBuilderReleaseError("release_config_invalid")
    _validate_release_image(config.current_agent, repository=_AGENT_REPOSITORY)
    _validate_release_image(config.current_builder, repository=_BUILDER_REPOSITORY)
    if config.current_agent.revision != config.current_builder.revision:
        raise PersonalDevNativeBuilderReleaseError("release_revision_mismatch")
    has_previous_agent = config.previous_agent is not None
    has_previous_builder = config.previous_builder is not None
    if has_previous_agent != has_previous_builder:
        raise PersonalDevNativeBuilderReleaseError("previous_release_invalid")
    if not has_previous_agent:
        return
    previous_agent = cast(NativeBuilderReleaseImage, config.previous_agent)
    previous_builder = cast(NativeBuilderReleaseImage, config.previous_builder)
    _validate_release_image(previous_agent, repository=_AGENT_REPOSITORY)
    _validate_release_image(previous_builder, repository=_BUILDER_REPOSITORY)
    if (
        previous_agent.revision != previous_builder.revision
        or previous_agent.revision == config.current_agent.revision
        or previous_agent.reference == config.current_agent.reference
        or previous_builder.reference == config.current_builder.reference
    ):
        raise PersonalDevNativeBuilderReleaseError("previous_release_invalid")


class PersonalDevNativeBuilderReleaseConverger:
    def __init__(
        self,
        *,
        config: NativeBuilderReleaseConfig,
        primary: NativeBuilderDockerApi,
        dedicated: NativeBuilderDockerApi,
    ) -> None:
        _validate_config(config)
        if (
            getattr(primary, "endpoint", None) != _PRIMARY_ENDPOINT
            or getattr(dedicated, "endpoint", None) != _DEDICATED_ENDPOINT
            or primary is dedicated
        ):
            raise PersonalDevNativeBuilderReleaseError("docker_endpoint_invalid")
        self.config = config
        self.primary = primary
        self.dedicated = dedicated

    def _inventory(
        self,
        api: NativeBuilderDockerApi,
        *,
        repository: str,
    ) -> dict[str, NativeBuilderImageRecord]:
        result: dict[str, NativeBuilderImageRecord] = {}
        records = api.images()
        if isinstance(records, (str, bytes)):
            raise PersonalDevNativeBuilderReleaseError("image_inventory_invalid")
        for record in records:
            if not isinstance(record, NativeBuilderImageRecord):
                raise PersonalDevNativeBuilderReleaseError(
                    "image_inventory_invalid"
                )
            matching = tuple(
                reference
                for reference in record.repo_digests
                if isinstance(reference, str)
                and _repository_reference(repository, reference)
            )
            if not matching:
                continue
            if (
                len(matching) != 1
                or _DIGEST.fullmatch(record.image_id) is None
                or record.os != "linux"
                or record.architecture != "arm64"
                or not isinstance(record.labels, Mapping)
                or record.labels.get("org.opencontainers.image.source") != _SOURCE
                or record.labels.get(_MANAGED_LABEL) != "true"
            ):
                raise PersonalDevNativeBuilderReleaseError("image_identity_invalid")
            revision = record.labels.get("org.opencontainers.image.revision")
            if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
                raise PersonalDevNativeBuilderReleaseError("image_identity_invalid")
            reference = matching[0]
            if reference in result:
                raise PersonalDevNativeBuilderReleaseError(
                    "image_inventory_invalid"
                )
            result[reference] = record
        return result

    def _endpoint_plan(
        self,
        api: NativeBuilderDockerApi,
        *,
        repository: str,
        current: NativeBuilderReleaseImage,
        previous: NativeBuilderReleaseImage | None,
    ) -> _EndpointPlan:
        inventory = self._inventory(api, repository=repository)
        releases = (current,) if previous is None else (current, previous)
        desired = tuple(release.reference for release in releases)
        pulls: list[str] = []
        for release in releases:
            record = inventory.get(release.reference)
            if record is None:
                pulls.append(release.reference)
                continue
            if record.labels.get("org.opencontainers.image.revision") != release.revision:
                raise PersonalDevNativeBuilderReleaseError(
                    "image_revision_invalid"
                )
        removals: dict[str, str] = {}
        for reference, record in inventory.items():
            if reference in desired:
                continue
            containers = tuple(api.containers_using(record.image_id))
            if containers:
                raise PersonalDevNativeBuilderReleaseError("image_in_use")
            removals[reference] = record.image_id
        return _EndpointPlan(
            endpoint=api.endpoint,
            pull=tuple(pulls),
            retain=desired,
            remove=tuple(sorted(removals)),
            removal_image_ids=dict(removals),
        )

    def _build_plan(self) -> tuple[_EndpointPlan, _EndpointPlan]:
        primary = self._endpoint_plan(
            self.primary,
            repository=_AGENT_REPOSITORY,
            current=self.config.current_agent,
            previous=self.config.previous_agent,
        )
        dedicated = self._endpoint_plan(
            self.dedicated,
            repository=_BUILDER_REPOSITORY,
            current=self.config.current_builder,
            previous=self.config.previous_builder,
        )
        return primary, dedicated

    def plan(self) -> dict[str, object]:
        primary, dedicated = self._build_plan()
        return {
            "dedicated": dedicated.public(),
            "operation": "plan",
            "primary": primary.public(),
            "schema": _SCHEMA,
        }

    def _validate_after_pull(
        self,
        api: NativeBuilderDockerApi,
        *,
        repository: str,
        current: NativeBuilderReleaseImage,
        previous: NativeBuilderReleaseImage | None,
    ) -> dict[str, NativeBuilderImageRecord]:
        inventory = self._inventory(api, repository=repository)
        releases = (current,) if previous is None else (current, previous)
        for release in releases:
            record = inventory.get(release.reference)
            if record is None:
                raise PersonalDevNativeBuilderReleaseError(
                    "required_image_missing"
                )
            if record.labels.get("org.opencontainers.image.revision") != release.revision:
                raise PersonalDevNativeBuilderReleaseError(
                    "image_revision_invalid"
                )
        return inventory

    def _pull_and_validate_endpoint(
        self,
        api: NativeBuilderDockerApi,
        plan: _EndpointPlan,
        *,
        repository: str,
        current: NativeBuilderReleaseImage,
        previous: NativeBuilderReleaseImage | None,
    ) -> dict[str, NativeBuilderImageRecord]:
        for reference in plan.pull:
            api.pull(reference)
        return self._validate_after_pull(
            api,
            repository=repository,
            current=current,
            previous=previous,
        )

    def _remove_planned_images(
        self,
        api: NativeBuilderDockerApi,
        plan: _EndpointPlan,
        inventory: Mapping[str, NativeBuilderImageRecord],
    ) -> None:
        for reference in plan.remove:
            expected_image_id = plan.removal_image_ids[reference]
            record = inventory.get(reference)
            if record is None or record.image_id != expected_image_id:
                raise PersonalDevNativeBuilderReleaseError("image_race_detected")
            if tuple(api.containers_using(record.image_id)):
                raise PersonalDevNativeBuilderReleaseError("image_in_use")
            api.remove(reference)

    def apply(self) -> dict[str, object]:
        primary, dedicated = self._build_plan()
        primary_inventory = self._pull_and_validate_endpoint(
            self.primary,
            primary,
            repository=_AGENT_REPOSITORY,
            current=self.config.current_agent,
            previous=self.config.previous_agent,
        )
        dedicated_inventory = self._pull_and_validate_endpoint(
            self.dedicated,
            dedicated,
            repository=_BUILDER_REPOSITORY,
            current=self.config.current_builder,
            previous=self.config.previous_builder,
        )
        self._remove_planned_images(
            self.primary,
            primary,
            primary_inventory,
        )
        self._remove_planned_images(
            self.dedicated,
            dedicated,
            dedicated_inventory,
        )
        self._verify_exact()
        return {
            "operation": "apply",
            "schema": _SCHEMA,
            "state": "converged",
        }

    def _verify_exact(self) -> None:
        for api, repository, current, previous in (
            (
                self.primary,
                _AGENT_REPOSITORY,
                self.config.current_agent,
                self.config.previous_agent,
            ),
            (
                self.dedicated,
                _BUILDER_REPOSITORY,
                self.config.current_builder,
                self.config.previous_builder,
            ),
        ):
            inventory = self._inventory(api, repository=repository)
            releases = (current,) if previous is None else (current, previous)
            desired = {release.reference: release for release in releases}
            missing = set(desired).difference(inventory)
            if missing:
                raise PersonalDevNativeBuilderReleaseError(
                    "required_image_missing"
                )
            if set(inventory) != set(desired):
                raise PersonalDevNativeBuilderReleaseError(
                    "image_retention_invalid"
                )
            for reference, release in desired.items():
                if (
                    inventory[reference].labels.get(
                        "org.opencontainers.image.revision"
                    )
                    != release.revision
                ):
                    raise PersonalDevNativeBuilderReleaseError(
                        "image_revision_invalid"
                    )

    def verify(self) -> dict[str, object]:
        self._verify_exact()
        return {
            "operation": "verify",
            "schema": _SCHEMA,
            "state": "converged",
        }


@dataclass(frozen=True, slots=True)
class _CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class NativeBuilderDockerCliApi:
    def __init__(self, endpoint: str) -> None:
        if endpoint not in {_PRIMARY_ENDPOINT, _DEDICATED_ENDPOINT}:
            raise PersonalDevNativeBuilderReleaseError("docker_endpoint_invalid")
        self.endpoint = endpoint

    def _run(self, *arguments: str) -> _CommandResult:
        argv = ["/usr/bin/docker", "-H", self.endpoint, *arguments]
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                check=False,
                env=_ROOT_ENV,
                timeout=_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise PersonalDevNativeBuilderReleaseError("docker_timeout") from exc
        if (
            len(completed.stdout.encode("utf-8", errors="replace"))
            > _MAX_COMMAND_OUTPUT
            or len(completed.stderr.encode("utf-8", errors="replace"))
            > _MAX_COMMAND_OUTPUT
        ):
            raise PersonalDevNativeBuilderReleaseError(
                "docker_output_invalid"
            )
        result = _CommandResult(
            completed.returncode,
            completed.stdout,
            completed.stderr,
        )
        if result.returncode != 0 or result.stderr:
            raise PersonalDevNativeBuilderReleaseError("docker_command_failed")
        return result

    def _json(self, *arguments: str) -> object:
        result = self._run(*arguments)
        if not result.stdout.endswith("\n"):
            raise PersonalDevNativeBuilderReleaseError("docker_output_invalid")
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise PersonalDevNativeBuilderReleaseError(
                "docker_output_invalid"
            ) from exc

    def images(self) -> tuple[NativeBuilderImageRecord, ...]:
        listed = self._run("image", "ls", "--all", "--quiet", "--no-trunc")
        image_ids = tuple(sorted(set(listed.stdout.splitlines())))
        if any(_DIGEST.fullmatch(image_id) is None for image_id in image_ids):
            raise PersonalDevNativeBuilderReleaseError("docker_output_invalid")
        records: list[NativeBuilderImageRecord] = []
        for image_id in image_ids:
            value = self._json("image", "inspect", image_id, "--format", "{{json .}}")
            if not isinstance(value, dict):
                raise PersonalDevNativeBuilderReleaseError(
                    "docker_output_invalid"
                )
            config = value.get("Config")
            labels: object = config.get("Labels") if isinstance(config, dict) else None
            repo_digests = value.get("RepoDigests")
            if labels is None:
                labels = {}
            if repo_digests is None:
                repo_digests = []
            if (
                value.get("Id") != image_id
                or not isinstance(repo_digests, list)
                or any(not isinstance(item, str) for item in repo_digests)
                or not isinstance(labels, dict)
                or any(
                    not isinstance(key, str) or not isinstance(item, str)
                    for key, item in labels.items()
                )
                or not isinstance(value.get("Os"), str)
                or not isinstance(value.get("Architecture"), str)
            ):
                raise PersonalDevNativeBuilderReleaseError(
                    "docker_output_invalid"
                )
            records.append(
                NativeBuilderImageRecord(
                    image_id=image_id,
                    repo_digests=tuple(cast(list[str], repo_digests)),
                    os=cast(str, value["Os"]),
                    architecture=cast(str, value["Architecture"]),
                    labels=cast(dict[str, str], labels),
                )
            )
        return tuple(records)

    def pull(self, reference: str) -> None:
        self._run("image", "pull", "--quiet", reference)

    def containers_using(self, image_id: str) -> tuple[str, ...]:
        listed = self._run(
            "container",
            "ls",
            "--all",
            "--quiet",
            "--no-trunc",
        )
        container_ids = tuple(sorted(set(listed.stdout.splitlines())))
        if any(
            re.fullmatch(r"[0-9a-f]{64}", container_id) is None
            for container_id in container_ids
        ):
            raise PersonalDevNativeBuilderReleaseError("docker_output_invalid")
        matching: list[str] = []
        for container_id in container_ids:
            value = self._json(
                "container",
                "inspect",
                container_id,
                "--format",
                "{{json .}}",
            )
            if not isinstance(value, dict) or not isinstance(value.get("Image"), str):
                raise PersonalDevNativeBuilderReleaseError(
                    "docker_output_invalid"
                )
            if value["Image"] == image_id:
                matching.append(container_id)
        return tuple(matching)

    def remove(self, reference: str) -> None:
        self._run("image", "rm", reference)


class _ConvergerOperations(Protocol):
    def plan(self) -> Mapping[str, object]: ...
    def apply(self) -> Mapping[str, object]: ...
    def verify(self) -> Mapping[str, object]: ...


ConvergerFactory = Callable[[NativeBuilderReleaseConfig], _ConvergerOperations]


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise PersonalDevNativeBuilderReleaseError("arguments_invalid")


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(add_help=True)
    parser.add_argument("operation", choices=("plan", "apply", "verify"))
    parser.add_argument("--current-agent", required=True)
    parser.add_argument("--current-builder", required=True)
    parser.add_argument("--current-revision", required=True)
    parser.add_argument("--previous-agent")
    parser.add_argument("--previous-builder")
    parser.add_argument("--previous-revision")
    return parser


def _default_converger(
    config: NativeBuilderReleaseConfig,
) -> _ConvergerOperations:
    return PersonalDevNativeBuilderReleaseConverger(
        config=config,
        primary=NativeBuilderDockerCliApi(_PRIMARY_ENDPOINT),
        dedicated=NativeBuilderDockerCliApi(_DEDICATED_ENDPOINT),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    converger_factory: ConvergerFactory = _default_converger,
) -> int:
    try:
        arguments = _parser().parse_args(argv)
        previous_values = (
            arguments.previous_agent,
            arguments.previous_builder,
            arguments.previous_revision,
        )
        if any(value is not None for value in previous_values) and not all(
            value is not None for value in previous_values
        ):
            raise PersonalDevNativeBuilderReleaseError("arguments_invalid")
        previous_agent = (
            NativeBuilderReleaseImage(
                reference=arguments.previous_agent,
                revision=arguments.previous_revision,
            )
            if arguments.previous_agent is not None
            else None
        )
        previous_builder = (
            NativeBuilderReleaseImage(
                reference=arguments.previous_builder,
                revision=arguments.previous_revision,
            )
            if arguments.previous_builder is not None
            else None
        )
        config = NativeBuilderReleaseConfig(
            current_agent=NativeBuilderReleaseImage(
                reference=arguments.current_agent,
                revision=arguments.current_revision,
            ),
            current_builder=NativeBuilderReleaseImage(
                reference=arguments.current_builder,
                revision=arguments.current_revision,
            ),
            previous_agent=previous_agent,
            previous_builder=previous_builder,
        )
        converger = converger_factory(config)
        if arguments.operation == "plan":
            receipt = converger.plan()
        elif arguments.operation == "apply":
            receipt = converger.apply()
        else:
            receipt = converger.verify()
        encoded = json.dumps(
            receipt,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        if len(encoded) > 64 * 1024:
            raise PersonalDevNativeBuilderReleaseError("receipt_invalid")
        sys.stdout.write(encoded + "\n")
        return 0
    except PersonalDevNativeBuilderReleaseError as exc:
        sys.stderr.write(f"error:{exc.code}\n")
        return 2 if exc.code == "arguments_invalid" else 1
    except Exception:
        sys.stderr.write("error:internal_error\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "NativeBuilderDockerApi",
    "NativeBuilderDockerCliApi",
    "NativeBuilderImageRecord",
    "NativeBuilderReleaseConfig",
    "NativeBuilderReleaseImage",
    "PersonalDevNativeBuilderReleaseConverger",
    "PersonalDevNativeBuilderReleaseError",
    "main",
]
