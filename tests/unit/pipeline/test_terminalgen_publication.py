from __future__ import annotations

import io
import tarfile
import tomllib
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from loom.integrations.terminalgen.artifacts import (
    ArtifactRefV1,
    CorpusTaskEntryV1,
    FinalAuditCountsV1,
    PipelineArtifactProvenanceV1,
    TaskBundleFileV1,
    TerminalGenCorpusArtifactV1,
    TerminalGenFinalAuditArtifactV1,
    TerminalGenPublicationRequestV1,
)
from loom.integrations.terminalgen.publication import (
    RuntimeTaskArchive,
    TerminalGenPublicationError,
    TerminalGenPublicationMaterial,
    build_taskset_smoke_archive,
    project_terminal_bench_task_config,
    validate_publication_material,
)
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.storage import FakeObjectStore
from loom_pipeline_orchestrator.repository import (
    PublicationStoredFile,
    RunLease,
    TerminalGenPublicationArtifactSource,
    TerminalGenPublicationCandidate,
)
from loom_pipeline_orchestrator.terminalgen_publication import (
    TerminalGenCorpusPublicationRuntime,
)

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.invalid/loom/terminalgen@sha256:" + "b" * 64
SLOT = "capability-00__same-domain-parametric__0001"
TASK_ID = f"terminalgen-{SLOT}"


def _ref(index: int, artifact_type: str) -> ArtifactRefV1:
    return ArtifactRefV1(
        artifact_id=UUID(int=index),
        artifact_type=artifact_type,
        manifest_sha256=DIGEST,
    )


def _provenance(*refs: ArtifactRefV1) -> PipelineArtifactProvenanceV1:
    return PipelineArtifactProvenanceV1(
        producer_kind="pipeline",
        loom_commit_sha="c" * 40,
        pipeline_run_id=UUID(int=20),
        stage_run_id=UUID(int=21),
        execution_attempt_id=UUID(int=22),
        recipe_digest=DIGEST,
        execution_spec_digest=DIGEST,
        image_digest=IMAGE,
        compatibility_manifest_sha256=DIGEST,
        source_artifacts=sorted(refs, key=lambda item: item.artifact_id.bytes),
    )


def _runtime_files(
    *,
    task_id: str = TASK_ID,
    task_name: str = "Durable task",
) -> dict[str, tuple[str, bytes, int]]:
    source = b"""
version = "1.0"
[metadata]
difficulty = "hard"
tags = ["synthetic"]
[verifier]
timeout_sec = 1200.0
[agent]
timeout_sec = 5400.0
[environment]
build_timeout_sec = 1200.0
cpus = 2
memory = "4G"
storage = "10G"
"""
    task_toml = project_terminal_bench_task_config(
        source,
        task_id=task_id,
        task_name=task_name,
    )
    return {
        "task.toml": ("task_config", task_toml, 0o644),
        "instruction.md": ("instruction", b"Repair the target.\n", 0o644),
        "environment/Dockerfile": ("environment", b"FROM alpine:3.20\n", 0o644),
        "dependencies.lock": ("dependency_lock", b"none\n", 0o644),
        "tests/test_task.py": ("verifier", b"def test_ok(): assert True\n", 0o644),
    }


def _inner_tar(files: dict[str, tuple[str, bytes, int]]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for path, (_role, body, mode) in sorted(files.items()):
            info = tarfile.TarInfo(path)
            info.size = len(body)
            info.mode = mode
            info.mtime = 0
            archive.addfile(info, io.BytesIO(body))
    return output.getvalue()


def _entry(
    *,
    archive_body: bytes,
    include_solution: bool,
    slot_id: str = SLOT,
    task_name: str = "Durable task",
) -> CorpusTaskEntryV1:
    task_id = f"terminalgen-{slot_id}"
    files = _runtime_files(task_id=task_id, task_name=task_name)
    if include_solution:
        files["solution/solve.sh"] = ("reference_solution", b"#!/bin/sh\n", 0o755)
    inventory = [
        TaskBundleFileV1(
            role=role,  # type: ignore[arg-type]
            relative_path=f"payload/{path}",
            sha256=digest_bytes(body),
            size_bytes=len(body),
            media_type="text/plain",
        )
        for path, (role, body, _mode) in sorted(files.items())
    ]
    verifier = next(item for item in inventory if item.role == "verifier")
    return CorpusTaskEntryV1(
        slot_id=slot_id,
        task_id=task_id,
        task_name=task_name,
        source_task_tree_sha256=DIGEST,
        projected_task_tree_sha256=DIGEST,
        source_task_artifact=_ref(1, "terminalgen_task_bundle.v1"),
        validation_artifact=_ref(2, "terminalgen_task_validation.v1"),
        bundle_relative_path=f"payload/tasks/{task_id}.tar",
        bundle_sha256=digest_bytes(archive_body),
        bundle_size_bytes=len(archive_body),
        verifier_bridge_sha256=verifier.sha256,
        files=inventory,
    )


def _audit() -> TerminalGenFinalAuditArtifactV1:
    return TerminalGenFinalAuditArtifactV1(
        schema_version="terminalgen_final_audit.v1",
        access_class="sanitized_audit",
        terminal_outcome="complete",
        reason_code="quota_complete",
        plan_identity_sha256=DIGEST,
        slot_terminal_set_sha256=DIGEST,
        task_artifact_set_sha256=DIGEST,
        validation_artifact_set_sha256=DIGEST,
        counts=FinalAuditCountsV1(
            requested=1,
            accepted=1,
            rejected=0,
            exhausted=0,
            cancelled=0,
            cleanup_failed=0,
            dynamically_validated=1,
        ),
        all_slot_ids_unique=True,
        all_template_family_ids_unique=True,
        quota_complete=True,
        validation_complete=True,
        provenance=_provenance(),
    )


def _material() -> tuple[TerminalGenPublicationMaterial, bytes]:
    files = _runtime_files()
    runtime_tar = _inner_tar(files)
    authoring_files = {
        **files,
        "solution/solve.sh": ("reference_solution", b"#!/bin/sh\n", 0o755),
    }
    authoring_tar = _inner_tar(authoring_files)
    audit_ref = _ref(3, "terminalgen_final_audit.v1")
    common = {
        "schema_version": "terminalgen_corpus.v1",
        "corpus_id": "terminalgen-authorized",
        "corpus_version": 1,
        "final_audit_artifact": audit_ref,
        "plan_identity_sha256": DIGEST,
        "task_count": 1,
        "corpus_tree_sha256": DIGEST,
        "task_archive_format": "tar",
        "provenance": _provenance(audit_ref),
    }
    authoring = TerminalGenCorpusArtifactV1(
        corpus_kind="authoring",
        access_class="authoring_restricted",
        contains_reference_solutions=True,
        tasks=[_entry(archive_body=authoring_tar, include_solution=True)],
        **common,  # type: ignore[arg-type]
    )
    runtime = TerminalGenCorpusArtifactV1(
        corpus_kind="runtime",
        access_class="team_runtime",
        contains_reference_solutions=False,
        tasks=[_entry(archive_body=runtime_tar, include_solution=False)],
        **common,  # type: ignore[arg-type]
    )
    request = TerminalGenPublicationRequestV1(
        schema_version="terminalgen.publication-request.v1",
        pipeline_run_id=UUID(int=20),
        recipe_digest=DIGEST,
        corpus_id="terminalgen-authorized",
        corpus_version=1,
        alias="terminalgen-current",
        expected_previous_version_sha256=None,
        final_audit_artifact=audit_ref,
        authoring_corpus_artifact=_ref(4, "terminalgen_corpus.v1"),
        runtime_corpus_artifact=_ref(5, "terminalgen_corpus.v1"),
        taskset_smoke_count=1,
    )
    return TerminalGenPublicationMaterial(
        request=request,
        final_audit=_audit(),
        authoring_corpus=authoring,
        runtime_corpus=runtime,
    ), runtime_tar


def test_material_closes_exact_audit_corpus_and_task_lineage() -> None:
    material, _runtime_tar = _material()
    assert validate_publication_material(material) is material
    assert material.corpus_version_sha256.startswith("sha256:")

    drifted = material.runtime_corpus.model_copy(
        update={"tasks": [material.runtime_corpus.tasks[0].model_copy(update={"task_name": "Drift"})]}
    )
    with pytest.raises(TerminalGenPublicationError, match="publication_task_lineage_drift"):
        validate_publication_material(
            TerminalGenPublicationMaterial(
                request=material.request,
                final_audit=material.final_audit,
                authoring_corpus=material.authoring_corpus,
                runtime_corpus=drifted,
            )
        )


def test_taskset_smoke_tar_is_deterministic_solution_free_and_runnable() -> None:
    material, runtime_tar = _material()
    task = RuntimeTaskArchive(entry=material.runtime_corpus.tasks[0], body=runtime_tar)
    first = build_taskset_smoke_archive(
        corpus_id=material.request.corpus_id,
        corpus_version=material.request.corpus_version,
        tasks=[task],
    )
    second = build_taskset_smoke_archive(
        corpus_id=material.request.corpus_id,
        corpus_version=material.request.corpus_version,
        tasks=[task],
    )
    try:
        assert first.sha256 == second.sha256
        assert first.file.read() == second.file.read()
        first.file.seek(0)
        with tarfile.open(fileobj=first.file, mode="r:") as archive:
            members = archive.getmembers()
            names = [item.name for item in members]
            assert f"tasks/{TASK_ID}/task.toml" in names
            assert not any("solution" in name for name in names)
            assert all(item.mtime == 0 and item.uid == 0 and item.gid == 0 for item in members)
            task_toml = archive.extractfile(f"tasks/{TASK_ID}/task.toml")
            assert task_toml is not None
            config = tomllib.loads(task_toml.read().decode())
            assert config["task"] == {
                "id": TASK_ID,
                "labels": ["synthetic"],
                "name": "Durable task",
            }
        manifest = material.request.corpus_id.encode()
        assert manifest in first.manifest_bytes
    finally:
        first.close()
        second.close()


def test_taskset_smoke_rejects_bundle_digest_and_mode_drift() -> None:
    material, runtime_tar = _material()
    task = RuntimeTaskArchive(entry=material.runtime_corpus.tasks[0], body=runtime_tar + b"drift")
    with pytest.raises(TerminalGenPublicationError, match="bundle_digest_drift"):
        build_taskset_smoke_archive(
            corpus_id=material.request.corpus_id,
            corpus_version=1,
            tasks=[task],
        )


def _source(
    *,
    artifact_id: int,
    name: str,
    artifact_type: str,
    access_class: str,
    node_key: str,
    semantic: bytes,
    payloads: list[tuple[str, bytes]] | None = None,
) -> TerminalGenPublicationArtifactSource:
    payloads = payloads or []
    prefix = f"fixture/{artifact_id}/"
    root = canonical_document({"artifact_id": artifact_id})
    marker = canonical_document({"committed": artifact_id})
    files = [
        PublicationStoredFile(
            relative_path="artifact.json",
            role="semantic_document",
            archive_format="none",
            media_type="application/json",
            size_bytes=len(semantic),
            sha256=digest_bytes(semantic),
            storage_key=f"{prefix}artifact.json",
        )
    ]
    files.extend(
        PublicationStoredFile(
            relative_path=path,
            role="payload_archive",
            archive_format="tar",
            media_type="application/x-tar",
            size_bytes=len(body),
            sha256=digest_bytes(body),
            storage_key=f"{prefix}{path}",
        )
        for path, body in payloads
    )
    return TerminalGenPublicationArtifactSource(
        artifact_id=UUID(int=artifact_id),
        artifact_name=name,
        artifact_type=artifact_type,
        access_class=access_class,
        manifest_sha256=DIGEST,
        content_sha256=digest_bytes(semantic),
        node_key=node_key,
        root_manifest_bytes=root,
        root_manifest_sha256=digest_bytes(root),
        root_manifest_key=f"{prefix}_manifest.json",
        committed_marker_bytes=marker,
        committed_marker_sha256=digest_bytes(marker),
        committed_marker_key=f"{prefix}_COMMITTED",
        files=tuple(files),
    )


class _PublicationRepository:
    def __init__(self, candidate: TerminalGenPublicationCandidate) -> None:
        self.candidate = candidate
        self.published: dict[str, Any] | None = None
        self.failed: dict[str, Any] | None = None

    async def terminalgen_publication_candidate(
        self,
        _lease: RunLease,
    ) -> TerminalGenPublicationCandidate | None:
        return self.candidate

    async def publish_terminalgen_corpus(
        self,
        _lease: RunLease,
        **values: Any,
    ) -> object:
        self.published = values
        return object()

    async def fail_terminalgen_publication(
        self,
        _lease: RunLease,
        **values: Any,
    ) -> None:
        self.failed = values


async def _seed_source(
    store: FakeObjectStore,
    source: TerminalGenPublicationArtifactSource,
    semantic: bytes,
    payloads: list[tuple[str, bytes]] | None = None,
) -> None:
    payloads = payloads or []
    await store.put_object(bucket="artifacts", key=source.root_manifest_key, body=source.root_manifest_bytes)
    await store.put_object(
        bucket="artifacts",
        key=source.committed_marker_key,
        body=source.committed_marker_bytes,
    )
    await store.put_object(bucket="artifacts", key=source.files[0].storage_key, body=semantic)
    for stored, (_path, body) in zip(source.files[1:], payloads, strict=True):
        await store.put_object(bucket="artifacts", key=stored.storage_key, body=body)


@pytest.mark.asyncio
async def test_runtime_reads_markers_publishes_content_addressed_smoke_and_receipt() -> None:
    material, runtime_tar = _material()
    authoring_files = _runtime_files()
    authoring_files["solution/solve.sh"] = ("reference_solution", b"#!/bin/sh\n", 0o755)
    authoring_tar = _inner_tar(authoring_files)
    request_bytes = canonical_document(material.request)
    audit_bytes = canonical_document(material.final_audit)
    authoring_bytes = canonical_document(material.authoring_corpus)
    runtime_bytes = canonical_document(material.runtime_corpus)
    request = _source(
        artifact_id=6,
        name="publication_request",
        artifact_type="terminalgen.publication-request.v1",
        access_class="authoring_restricted",
        node_key="publish_boundary",
        semantic=request_bytes,
    )
    audit = _source(
        artifact_id=3,
        name="final_audit",
        artifact_type="terminalgen_final_audit.v1",
        access_class="sanitized_audit",
        node_key="global_finalize",
        semantic=audit_bytes,
    )
    authoring = _source(
        artifact_id=4,
        name="corpus",
        artifact_type="terminalgen_corpus.v1",
        access_class="authoring_restricted",
        node_key="package_authoring",
        semantic=authoring_bytes,
        payloads=[(material.authoring_corpus.tasks[0].bundle_relative_path, authoring_tar)],
    )
    runtime = _source(
        artifact_id=5,
        name="corpus",
        artifact_type="terminalgen_corpus.v1",
        access_class="team_runtime",
        node_key="package_runtime",
        semantic=runtime_bytes,
        payloads=[(material.runtime_corpus.tasks[0].bundle_relative_path, runtime_tar)],
    )
    candidate = TerminalGenPublicationCandidate(
        pipeline_run_id=material.request.pipeline_run_id,
        team_id=UUID(int=30),
        recipe_digest=DIGEST,
        request=request,
        final_audit=audit,
        authoring_corpus=authoring,
        runtime_corpus=runtime,
    )
    store = FakeObjectStore()
    for source, semantic, payloads in (
        (request, request_bytes, []),
        (audit, audit_bytes, []),
        (authoring, authoring_bytes, [("unused", authoring_tar)]),
        (runtime, runtime_bytes, [("unused", runtime_tar)]),
    ):
        await _seed_source(store, source, semantic, payloads)
    repository = _PublicationRepository(candidate)
    lease = RunLease(
        pipeline_run_id=candidate.pipeline_run_id,
        claimed_by="controller",
        lease_epoch=1,
        lease_expires_at=datetime.now(UTC),
    )
    assert await TerminalGenCorpusPublicationRuntime(
        repository=repository,
        store=store,
        bucket="artifacts",
    ).reconcile(lease)
    assert repository.failed is None
    assert repository.published is not None
    smoke = repository.published["smoke"]
    assert smoke.task_count == 1
    smoke_key = repository.published["smoke_object_key"]
    assert smoke.archive_sha256 == digest_bytes(store.objects[("artifacts", smoke_key)])
    assert material.corpus_version_sha256.removeprefix("sha256:") in smoke_key


@pytest.mark.asyncio
async def test_runtime_validates_every_task_archive_beyond_smoke_selection() -> None:
    material, first_body = _material()
    second_slot = "capability-00__same-domain-parametric__0002"
    second_task_id = f"terminalgen-{second_slot}"
    hidden_files = _runtime_files(task_id=second_task_id, task_name="Second task")
    hidden_files["solution/solve.sh"] = ("reference_solution", b"#!/bin/sh\n", 0o755)
    hidden_body = _inner_tar(hidden_files)
    second = _entry(
        archive_body=hidden_body,
        include_solution=False,
        slot_id=second_slot,
        task_name="Second task",
    )
    corpus = material.runtime_corpus.model_copy(
        update={
            "task_count": 2,
            "tasks": [material.runtime_corpus.tasks[0], second],
        }
    )
    source = _source(
        artifact_id=50,
        name="corpus",
        artifact_type="terminalgen_corpus.v1",
        access_class="team_runtime",
        node_key="package_runtime",
        semantic=canonical_document(corpus),
        payloads=[
            (corpus.tasks[0].bundle_relative_path, first_body),
            (corpus.tasks[1].bundle_relative_path, hidden_body),
        ],
    )
    store = FakeObjectStore()
    await _seed_source(
        store,
        source,
        canonical_document(corpus),
        [("first", first_body), ("second", hidden_body)],
    )
    runtime = TerminalGenCorpusPublicationRuntime(
        repository=_PublicationRepository(
            TerminalGenPublicationCandidate(
                pipeline_run_id=material.request.pipeline_run_id,
                team_id=UUID(int=30),
                recipe_digest=DIGEST,
                request=source,
                final_audit=source,
                authoring_corpus=source,
                runtime_corpus=source,
            )
        ),
        store=store,
        bucket="artifacts",
    )
    with pytest.raises(TerminalGenPublicationError, match="task_inventory_drift"):
        archive = await runtime._build_and_validate_runtime_tasks(source, corpus, 1)
        archive.close()
