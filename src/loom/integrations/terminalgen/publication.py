"""Server-owned TerminalGen publication validation and TaskSet projection."""

from __future__ import annotations

import hashlib
import io
import tarfile
import tempfile
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import tomli_w
import yaml  # type: ignore[import-untyped]

from loom.integrations.terminalgen.artifacts import (
    CorpusTaskEntryV1,
    TerminalGenCorpusArtifactV1,
    TerminalGenFinalAuditArtifactV1,
    TerminalGenPublicationRequestV1,
)
from loom.models.task import TaskConfig, normalize_steps
from loom.models.taskset import UserTaskSetManifest
from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml

MAX_TASKSET_SMOKE_TASKS = 500
MAX_TASKSET_SMOKE_BYTES = 5_368_709_120
_SAFE_MODES = frozenset({0o644, 0o755})


class TerminalGenPublicationError(ValueError):
    """A closed publication invariant failed."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class TerminalGenPublicationMaterial:
    request: TerminalGenPublicationRequestV1
    final_audit: TerminalGenFinalAuditArtifactV1
    authoring_corpus: TerminalGenCorpusArtifactV1
    runtime_corpus: TerminalGenCorpusArtifactV1

    @property
    def corpus_version_sha256(self) -> str:
        return canonical_digest(
            {
                "authoring_corpus_tree_sha256": self.authoring_corpus.corpus_tree_sha256,
                "corpus_id": self.request.corpus_id,
                "corpus_version": self.request.corpus_version,
                "final_audit_manifest_sha256": (
                    self.request.final_audit_artifact.manifest_sha256
                ),
                "plan_identity_sha256": self.final_audit.plan_identity_sha256,
                "recipe_digest": self.request.recipe_digest,
                "runtime_corpus_tree_sha256": self.runtime_corpus.corpus_tree_sha256,
            }
        )


def _same_task_lineage(
    authoring: CorpusTaskEntryV1,
    runtime: CorpusTaskEntryV1,
) -> bool:
    return (
        authoring.slot_id == runtime.slot_id
        and authoring.task_id == runtime.task_id
        and authoring.task_name == runtime.task_name
        and authoring.source_task_tree_sha256 == runtime.source_task_tree_sha256
        and authoring.source_task_artifact == runtime.source_task_artifact
        and authoring.validation_artifact == runtime.validation_artifact
        and authoring.verifier_bridge_sha256 == runtime.verifier_bridge_sha256
    )


def validate_publication_material(
    material: TerminalGenPublicationMaterial,
) -> TerminalGenPublicationMaterial:
    request = material.request
    audit = material.final_audit
    authoring = material.authoring_corpus
    runtime = material.runtime_corpus
    if request.pipeline_run_id != audit.provenance.pipeline_run_id:
        raise TerminalGenPublicationError("publication_run_identity_drift")
    documents = (audit, authoring, runtime)
    if any(item.provenance.recipe_digest != request.recipe_digest for item in documents):
        raise TerminalGenPublicationError("publication_recipe_digest_drift")
    if audit.terminal_outcome != "complete":
        raise TerminalGenPublicationError("publication_final_audit_incomplete")
    if authoring.corpus_kind != "authoring" or runtime.corpus_kind != "runtime":
        raise TerminalGenPublicationError("publication_corpus_kind_drift")
    if (
        authoring.corpus_id != request.corpus_id
        or runtime.corpus_id != request.corpus_id
        or authoring.corpus_version != request.corpus_version
        or runtime.corpus_version != request.corpus_version
    ):
        raise TerminalGenPublicationError("publication_corpus_identity_drift")
    if (
        authoring.final_audit_artifact != request.final_audit_artifact
        or runtime.final_audit_artifact != request.final_audit_artifact
    ):
        raise TerminalGenPublicationError("publication_final_audit_reference_drift")
    if (
        authoring.plan_identity_sha256 != audit.plan_identity_sha256
        or runtime.plan_identity_sha256 != audit.plan_identity_sha256
    ):
        raise TerminalGenPublicationError("publication_plan_identity_drift")
    if (
        authoring.task_count != audit.counts.requested
        or runtime.task_count != audit.counts.requested
        or authoring.task_count != runtime.task_count
    ):
        raise TerminalGenPublicationError("publication_task_count_drift")
    if request.taskset_smoke_count > runtime.task_count:
        raise TerminalGenPublicationError("publication_smoke_count_exceeds_corpus")
    for authoring_task, runtime_task in zip(
        authoring.tasks,
        runtime.tasks,
        strict=True,
    ):
        if not _same_task_lineage(authoring_task, runtime_task):
            raise TerminalGenPublicationError("publication_task_lineage_drift")
    return material


@dataclass(frozen=True, slots=True)
class RuntimeTaskArchive:
    entry: CorpusTaskEntryV1
    body: bytes


@dataclass(slots=True)
class TaskSetSmokeArchive:
    file: tempfile.SpooledTemporaryFile[bytes]
    size_bytes: int
    sha256: str
    manifest_bytes: bytes
    manifest_sha256: str
    task_ids: tuple[str, ...]

    def close(self) -> None:
        self.file.close()


def _task_member_path(value: str) -> str:
    prefix = "payload/"
    if not value.startswith(prefix):
        raise TerminalGenPublicationError("publication_task_path_drift")
    result = value.removeprefix(prefix)
    if not result or result.startswith("/") or "\\" in result:
        raise TerminalGenPublicationError("publication_task_path_unsafe")
    if any(part in {"", ".", ".."} for part in result.split("/")):
        raise TerminalGenPublicationError("publication_task_path_unsafe")
    return result


def validate_task_archive(task: RuntimeTaskArchive) -> dict[str, tuple[bytes, int]]:
    """Validate one opaque task tar against its typed inventory and identity."""

    expected = {_task_member_path(item.relative_path): item for item in task.entry.files}
    observed: dict[str, tuple[bytes, int]] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(task.body), mode="r:") as archive:
            for member in archive.getmembers():
                if not member.isfile() or member.issym() or member.islnk():
                    raise TerminalGenPublicationError("publication_task_archive_unsafe")
                name = member.name.removeprefix("./")
                if name.startswith("payload/"):
                    name = name.removeprefix("payload/")
                if name not in expected or name in observed:
                    raise TerminalGenPublicationError("publication_task_inventory_drift")
                mode = member.mode & 0o777
                if mode not in _SAFE_MODES:
                    raise TerminalGenPublicationError("publication_task_mode_unsafe")
                source = archive.extractfile(member)
                if source is None:
                    raise TerminalGenPublicationError("publication_task_archive_unreadable")
                body = source.read(expected[name].size_bytes + 1)
                if len(body) != expected[name].size_bytes:
                    raise TerminalGenPublicationError("publication_task_size_drift")
                if digest_bytes(body) != expected[name].sha256:
                    raise TerminalGenPublicationError("publication_task_digest_drift")
                observed[name] = (body, mode)
    except tarfile.TarError as exc:
        raise TerminalGenPublicationError("publication_task_archive_unreadable") from exc
    if set(observed) != set(expected):
        raise TerminalGenPublicationError("publication_task_inventory_drift")
    if digest_bytes(task.body) != task.entry.bundle_sha256:
        raise TerminalGenPublicationError("publication_task_bundle_digest_drift")
    if len(task.body) != task.entry.bundle_size_bytes:
        raise TerminalGenPublicationError("publication_task_bundle_size_drift")
    _validate_canonical_task_config(task.entry, observed)
    return observed


def _validate_canonical_task_config(
    entry: CorpusTaskEntryV1,
    files: Mapping[str, tuple[bytes, int]],
) -> None:
    task_file = next(item for item in entry.files if item.role == "task_config")
    task_path = _task_member_path(task_file.relative_path)
    try:
        raw = tomllib.loads(files[task_path][0].decode("utf-8"))
        config = normalize_steps(TaskConfig.model_validate(raw))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, ValueError) as exc:
        raise TerminalGenPublicationError("publication_task_config_invalid") from exc
    if config.task.id != entry.task_id or config.task.name != entry.task_name:
        raise TerminalGenPublicationError("publication_task_identity_drift")


def project_terminal_bench_task_config(
    source: bytes,
    *,
    task_id: str,
    task_name: str,
) -> bytes:
    """Project one delivered TB-style config to an exact runnable Loom TaskConfig."""

    try:
        raw = tomllib.loads(source.decode("utf-8"))
        projected = normalize_terminal_bench_task_toml(raw)
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, ValueError) as exc:
        raise TerminalGenPublicationError("publication_source_task_config_invalid") from exc
    task = projected.get("task")
    if not isinstance(task, dict):
        task = {}
        projected["task"] = task
    task["id"] = task_id
    task["name"] = task_name
    try:
        config = normalize_steps(TaskConfig.model_validate(projected))
    except ValueError as exc:
        raise TerminalGenPublicationError("publication_task_projection_invalid") from exc
    rendered = tomli_w.dumps(config.model_dump(mode="json", exclude_none=True))
    return rendered.encode("utf-8")


def taskset_smoke_manifest(
    *,
    corpus_id: str,
    corpus_version: int,
    task_count: int,
) -> bytes:
    slug = f"{corpus_id}-v{corpus_version}-smoke"
    model = UserTaskSetManifest.model_validate(
        {
            "apiVersion": "loom.taskset/v1",
            "kind": "UserTaskSet",
            "metadata": {
                "name": slug,
                "display_name": f"{corpus_id} v{corpus_version} publication smoke",
            },
            "intents": ["evaluation"],
            "source": {
                "type": "bundle-upload",
                "locator": "terminalgen-smoke.tar",
                "subset": "tasks",
            },
            "limits": {"max_instances": task_count, "timeout_per_task_s": 300},
        }
    )
    payload = model.model_dump(mode="json", by_alias=True, exclude_none=True)
    rendered = cast(str, yaml.safe_dump(payload, sort_keys=True))
    return rendered.encode("utf-8")


class TaskSetSmokeArchiveBuilder:
    """Incrementally build a deterministic TaskSet tar without retaining task bodies."""

    def __init__(
        self,
        *,
        corpus_id: str,
        corpus_version: int,
        expected_task_count: int,
        max_bytes: int = MAX_TASKSET_SMOKE_BYTES,
    ) -> None:
        if not 1 <= expected_task_count <= MAX_TASKSET_SMOKE_TASKS:
            raise TerminalGenPublicationError("publication_smoke_count_invalid")
        self._corpus_id = corpus_id
        self._corpus_version = corpus_version
        self._expected_task_count = expected_task_count
        self._max_bytes = max_bytes
        self._task_ids: list[str] = []
        self._output = tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024, mode="w+b")
        self._archive = tarfile.open(fileobj=self._output, mode="w")
        self._finished = False

    def add(self, task: RuntimeTaskArchive) -> None:
        if self._finished or len(self._task_ids) >= self._expected_task_count:
            raise TerminalGenPublicationError("publication_smoke_count_invalid")
        if any(item.role == "reference_solution" for item in task.entry.files):
            raise TerminalGenPublicationError("publication_smoke_contains_solution")
        task_id = task.entry.task_id
        if self._task_ids and task_id.encode() <= self._task_ids[-1].encode():
            raise TerminalGenPublicationError("publication_smoke_tasks_noncanonical")
        files = validate_task_archive(task)
        for relative_path in sorted(files, key=str.encode):
            body, mode = files[relative_path]
            info = tarfile.TarInfo(f"tasks/{task_id}/{relative_path}")
            info.size = len(body)
            info.mode = mode
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            self._archive.addfile(info, io.BytesIO(body))
            if self._output.tell() > self._max_bytes:
                raise TerminalGenPublicationError("publication_smoke_archive_too_large")
        self._task_ids.append(task_id)

    def finish(self) -> TaskSetSmokeArchive:
        if self._finished or len(self._task_ids) != self._expected_task_count:
            raise TerminalGenPublicationError("publication_smoke_count_invalid")
        self._finished = True
        try:
            self._archive.close()
            size_bytes = self._output.tell()
            if not 0 < size_bytes <= self._max_bytes:
                raise TerminalGenPublicationError("publication_smoke_archive_too_large")
            self._output.seek(0)
            digest = hashlib.sha256()
            while chunk := self._output.read(1024 * 1024):
                digest.update(chunk)
            self._output.seek(0)
            manifest = taskset_smoke_manifest(
                corpus_id=self._corpus_id,
                corpus_version=self._corpus_version,
                task_count=len(self._task_ids),
            )
            return TaskSetSmokeArchive(
                file=self._output,
                size_bytes=size_bytes,
                sha256=f"sha256:{digest.hexdigest()}",
                manifest_bytes=manifest,
                manifest_sha256=digest_bytes(manifest),
                task_ids=tuple(self._task_ids),
            )
        except Exception:
            self._output.close()
            raise

    def close(self) -> None:
        if not self._finished:
            self._archive.close()
            self._finished = True
        self._output.close()


def build_taskset_smoke_archive(
    *,
    corpus_id: str,
    corpus_version: int,
    tasks: Sequence[RuntimeTaskArchive],
    max_bytes: int = MAX_TASKSET_SMOKE_BYTES,
) -> TaskSetSmokeArchive:
    builder = TaskSetSmokeArchiveBuilder(
        corpus_id=corpus_id,
        corpus_version=corpus_version,
        expected_task_count=len(tasks),
        max_bytes=max_bytes,
    )
    try:
        for task in tasks:
            builder.add(task)
        return builder.finish()
    except Exception:
        builder.close()
        raise


def iter_archive_chunks(
    archive: TaskSetSmokeArchive,
    *,
    chunk_size: int = 8 * 1024 * 1024,
) -> Iterable[bytes]:
    archive.file.seek(0)
    while chunk := archive.file.read(chunk_size):
        yield chunk


__all__ = [
    "MAX_TASKSET_SMOKE_BYTES",
    "MAX_TASKSET_SMOKE_TASKS",
    "RuntimeTaskArchive",
    "TaskSetSmokeArchive",
    "TaskSetSmokeArchiveBuilder",
    "TerminalGenPublicationError",
    "TerminalGenPublicationMaterial",
    "build_taskset_smoke_archive",
    "iter_archive_chunks",
    "project_terminal_bench_task_config",
    "taskset_smoke_manifest",
    "validate_publication_material",
    "validate_task_archive",
]
