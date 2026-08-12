"""Production boundary adapter for BEHAVIOR input import/materialization."""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    AdminAuditEvent,
    Artifact,
    ArtifactLineageEdge,
    ArtifactUploadFile,
    ArtifactUploadSession,
    PipelineInputImport,
    PipelineInputMaterialization,
    Task,
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
)
from loom.db.task_set_visibility import visible_task_sets
from loom.integrations.behavior.contracts import (
    BehaviorDatasetSnapshotArtifactV1,
    BehaviorMopBankArtifactV1,
    CompanionInputsV1,
    ContentArtifactRefV1,
    DatasetCompatibilityV1,
    MopBankCompatibilityV1,
    ObjectTaskBundleRefV1,
    SourceTaskSetRefV1,
    TaskSnapshotRowV1,
)
from loom.pipeline.artifact_commit import (
    ArtifactCommitError,
    ArtifactCommitService,
    CommittedArtifactsV1,
    InputImportProducerV1,
    InputMaterializationProducerV1,
    ProducerAuthV1,
    UploadAuthV1,
    UploadFilePlanV1,
    multipart_part_size,
)
from loom.pipeline.behavior_input_import import (
    MAX_ARTIFACT_DOCUMENT_BYTES,
    BehaviorInputImportManifestV1,
    verify_bundle_stream,
)
from loom.pipeline.behavior_materialization import (
    BehaviorMaterializationSourceSnapshotV1,
    behavior_materializer_registry,
)
from loom.pipeline.input_materialization import (
    InputMaterializationRequestV1,
    PipelineInputMaterializationService,
)
from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.pipeline.recipes import OfficialRecipeRegistry
from loom.trajectory.storage import MinioObjectStore, ObjectStore
from loom_service.pipeline_api_service import PipelineApiError

_ARTIFACT_NAMESPACE = UUID("f65d8895-6d9f-47d7-88a0-4fdb91b974d2")
_IMPORT_TTL = timedelta(hours=24)


def _target_type(kind: str) -> str:
    return {
        "dataset": "behavior_dataset_snapshot.v1",
        "policy": "behavior_policy_checkpoint.v1",
        "mop_bank": "behavior_mop_bank.v1",
    }[kind]


def _kind_limit(kind: str) -> int:
    return 1024**4 if kind == "dataset" else 100 * 1024**3


def _public_error(exc: Exception) -> PipelineApiError:
    reason = exc.reason if isinstance(exc, ArtifactCommitError) else "behavior_input_invalid"
    status = 409 if reason in {"idempotency_conflict", "part_conflict"} else 422
    return PipelineApiError(status, reason, "BEHAVIOR input operation failed closed")


class BehaviorPipelinePublicAdapter:
    """Own the exact public import/materialization service wiring.

    The generic #1214 protocol remains the only object writer.  This adapter
    supplies SQL authority snapshots and publishes its results in the route's
    existing transaction.
    """

    def __init__(
        self,
        *,
        store: ObjectStore,
        bucket: str,
        recipe_registry: OfficialRecipeRegistry,
        loom_commit_sha: str,
    ) -> None:
        if len(loom_commit_sha) != 40 or any(
            ch not in "0123456789abcdef" for ch in loom_commit_sha
        ):
            raise ValueError("loom_commit_sha must be 40 lowercase hexadecimal characters")
        self._store = store
        self._bucket = bucket
        self._recipes = recipe_registry
        self._loom_commit_sha = loom_commit_sha
        self._committer = ArtifactCommitService(store=store, bucket=bucket)
        self._import_sessions: dict[UUID, UUID] = {}
        self._materializations = PipelineInputMaterializationService(
            artifact_committer=self._committer,
            materializers=behavior_materializer_registry(),
        )

    def _recipe(self) -> Any:
        try:
            registration = self._recipes.get("behavior-recovery", 1)
        except KeyError as exc:
            raise PipelineApiError(
                503, "recipe_unavailable", "behavior-recovery@1 is not registered"
            ) from exc
        if registration.submission_policy != "ordinary":
            raise PipelineApiError(503, "recipe_unavailable", "BEHAVIOR Recipe is not ordinary")
        return registration

    async def create_import(
        self,
        *,
        session: AsyncSession,
        team_id: UUID,
        user_id: UUID,
        payload: Any,
        idempotency_key: str,
    ) -> dict[str, Any]:
        registration = self._recipe()
        manifest = BehaviorInputImportManifestV1.model_validate(payload.manifest)
        request_digest = canonical_digest(
            {
                "kind": payload.kind,
                "manifest": manifest,
                "recipe": registration.identity,
                "team_id": team_id,
            },
            persisted=False,
        )
        existing = (
            await session.execute(
                select(PipelineInputImport).where(
                    PipelineInputImport.team_id == team_id,
                    PipelineInputImport.idempotency_key == idempotency_key,
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing.request_digest != request_digest:
                raise PipelineApiError(409, "idempotency_conflict", "Import request changed")
            return {
                "import_id": str(existing.id),
                "state": existing.state,
                "upload_session_id": str(existing.artifact_upload_session_id)
                if existing.artifact_upload_session_id
                else None,
            }
        import_id = uuid4()
        artifact_id = uuid5(_ARTIFACT_NAMESPACE, f"input-import:{import_id}")
        producer = InputImportProducerV1(
            commit_kind="input_import",
            team_id=team_id,
            pipeline_input_import_id=import_id,
            actor_user_id=user_id,
        )
        plans = [
            UploadFilePlanV1(
                file_index=0,
                preallocated_artifact_id=artifact_id,
                relative_path="payload.tar.zst",
                artifact_name=payload.kind,
                artifact_type=_target_type(payload.kind),
                producer="platform",
                media_type="application/zstd",
                role="payload_archive",
                archive_format="tar.zst",
                expected_max_bytes=_kind_limit(payload.kind),
                expected_sha256=None,
                expected_size=None,
            ),
            UploadFilePlanV1(
                file_index=1,
                preallocated_artifact_id=artifact_id,
                relative_path="artifact.json",
                artifact_name=payload.kind,
                artifact_type=_target_type(payload.kind),
                producer="service",
                media_type="application/json",
                role="semantic_document",
                archive_format="none",
                expected_max_bytes=MAX_ARTIFACT_DOCUMENT_BYTES,
                expected_sha256=None,
                expected_size=None,
            ),
        ]
        try:
            grant = await self._committer.prepare_session(
                producer=producer,
                files=plans,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
            )
        except (ArtifactCommitError, ValueError) as exc:
            raise _public_error(exc) from exc
        row = PipelineInputImport(
            id=import_id,
            team_id=team_id,
            created_by_user_id=user_id,
            recipe_name="behavior-recovery",
            recipe_version=1,
            recipe_digest=registration.digest,
            kind=payload.kind,
            target_artifact_type=_target_type(payload.kind),
            input_manifest_json=manifest.model_dump(mode="json"),
            input_manifest_digest=canonical_digest(manifest),
            trust_class="internal_trusted",
            max_bundle_bytes=_kind_limit(payload.kind),
            max_file_count=100_000 if payload.kind == "dataset" else 10_000,
            state="uploading",
            artifact_upload_session_id=None,
            committed_artifact_id=None,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
            abort_reason=None,
            expires_at=datetime.now(UTC) + _IMPORT_TTL,
            committed_at=None,
            aborted_at=None,
        )
        session.add(row)
        await session.commit()
        self._import_sessions[import_id] = grant.upload_session_id
        return {
            "import_id": str(import_id),
            "state": "uploading",
            "upload_session_id": str(grant.upload_session_id),
            "upload_token": grant.upload_token,
            "token_expires_at": grant.token_expires_at.isoformat(),
            "part_size_bytes": multipart_part_size(plans[0].expected_max_bytes),
        }

    async def _locked_import(
        self, session: AsyncSession, *, team_id: UUID, import_id: UUID
    ) -> PipelineInputImport:
        row = (
            await session.execute(
                select(PipelineInputImport)
                .where(PipelineInputImport.id == import_id, PipelineInputImport.team_id == team_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise PipelineApiError(404, "not_found", "Input import was not found")
        return row

    async def put_import_part(self, **kwargs: Any) -> dict[str, Any]:
        row = await self._locked_import(
            kwargs["session"], team_id=kwargs["team_id"], import_id=kwargs["import_id"]
        )
        if row.state != "uploading":
            raise PipelineApiError(409, "import_not_uploading", "Input import is not uploading")
        if self._import_sessions.get(row.id) != kwargs["upload_session_id"]:
            raise PipelineApiError(
                409, "upload_session_drift", "Upload session differs from import"
            )
        try:
            receipt = await self._committer.write_part(
                session_id=kwargs["upload_session_id"],
                file_index=0,
                part_number=kwargs["part_number"],
                content_length=kwargs["content_length"],
                content_sha256=kwargs["content_sha256"],
                body=kwargs["body"],
                auth=UploadAuthV1(upload_token=kwargs["upload_token"]),
            )
        except (ArtifactCommitError, ValueError) as exc:
            raise _public_error(exc) from exc
        await kwargs["session"].commit()
        return receipt.model_dump(mode="json")

    async def renew_import(self, **kwargs: Any) -> dict[str, Any]:
        row = await self._locked_import(
            kwargs["session"], team_id=kwargs["team_id"], import_id=kwargs["import_id"]
        )
        if row.state != "uploading":
            raise PipelineApiError(409, "import_not_uploading", "Input import is not uploading")
        if self._import_sessions.get(row.id) != kwargs["payload"].upload_session_id:
            raise PipelineApiError(
                409, "upload_session_drift", "Upload session differs from import"
            )
        try:
            grant = await self._committer.renew_upload_token(
                session_id=kwargs["payload"].upload_session_id,
                auth=ProducerAuthV1(subject_kind="team_admin", subject_id=kwargs["user_id"]),
            )
        except (ArtifactCommitError, ValueError) as exc:
            raise _public_error(exc) from exc
        return grant.model_dump(mode="json")

    async def _bundle_file(self, *, team_id: UUID, import_id: UUID, target: Path) -> None:
        key = f"pipeline-input-imports/{team_id}/{import_id}/artifacts/"
        # The deterministic Artifact UUID is stable and never caller-selected.
        artifact_id = uuid5(_ARTIFACT_NAMESPACE, f"input-import:{import_id}")
        key += f"{artifact_id}/payload.tar.zst"
        with target.open("wb") as sink:
            async for chunk in self._store.stream_object(
                bucket=self._bucket, key=key, chunk_size=64 * 1024 * 1024
            ):
                sink.write(chunk)

    async def complete_import(self, **kwargs: Any) -> dict[str, Any]:
        session: AsyncSession = kwargs["session"]
        row = await self._locked_import(
            session, team_id=kwargs["team_id"], import_id=kwargs["import_id"]
        )
        payload = kwargs["payload"]
        if row.state == "committed" and row.committed_artifact_id is not None:
            artifact = await session.get(Artifact, row.committed_artifact_id)
            assert artifact is not None
            return self._import_projection(row, artifact)
        if row.state != "uploading":
            raise PipelineApiError(409, "import_not_uploading", "Input import is not uploading")
        if self._import_sessions.get(row.id) != payload.upload_session_id:
            raise PipelineApiError(
                409, "upload_session_drift", "Upload session differs from import"
            )
        # Completion authenticates the exact receipts and create authority;
        # rotate a short-lived token rather than persisting/replaying a raw one.
        try:
            grant = await self._committer.renew_upload_token(
                session_id=payload.upload_session_id,
                auth=ProducerAuthV1(subject_kind="team_admin", subject_id=kwargs["user_id"]),
            )
            auth = UploadAuthV1(upload_token=grant.upload_token)
            verified_archive = await self._committer.complete_file(
                session_id=payload.upload_session_id,
                file_index=0,
                ordered_parts=payload.parts,
                auth=auth,
            )
            with tempfile.NamedTemporaryFile() as temporary:
                await self._bundle_file(
                    team_id=kwargs["team_id"],
                    import_id=kwargs["import_id"],
                    target=Path(temporary.name),
                )
                with open(temporary.name, "rb") as source:
                    verified = verify_bundle_stream(
                        source,
                        bundle_sha256=payload.bundle_sha256,
                        bundle_size_bytes=payload.bundle_size_bytes,
                        manifest=BehaviorInputImportManifestV1.model_validate(
                            row.input_manifest_json
                        ),
                        actor_user_id=kwargs["user_id"],
                        control_event_id=kwargs["import_id"],
                        recipe_digest=row.recipe_digest,
                        loom_commit_sha=self._loom_commit_sha,
                    )
            if (
                verified_archive.sha256 != verified.bundle_sha256
                or verified_archive.size_bytes != verified.bundle_size_bytes
            ):
                raise ValueError("bundle completion differs from streamed verification")
            semantic = verified.artifact_bytes

            async def semantic_body() -> Any:
                yield semantic

            receipt = await self._committer.write_part(
                session_id=payload.upload_session_id,
                file_index=1,
                part_number=1,
                content_length=len(semantic),
                content_sha256=digest_bytes(semantic),
                body=semantic_body(),
                auth=auth,
            )
            await self._committer.complete_file(
                session_id=payload.upload_session_id,
                file_index=1,
                ordered_parts=[receipt],
                auth=auth,
            )
            committed = await self._committer.commit_session(
                session_id=payload.upload_session_id,
                auth=ProducerAuthV1(subject_kind="official_service", subject_id=UUID(int=0)),
            )
            if not isinstance(committed, CommittedArtifactsV1):
                raise ValueError("input import did not reach committed state")
            artifact = await self._publish_committed(
                session,
                committed=committed,
                team_id=row.team_id,
                actor_user_id=row.created_by_user_id,
                producer_kind="input_import",
                producer_id=row.id,
            )
        except (ArtifactCommitError, ValueError, KeyError) as exc:
            await session.rollback()
            raise _public_error(exc) from exc
        row.artifact_upload_session_id = committed.upload_session_id
        row.committed_artifact_id = artifact.id
        row.state = "committed"
        row.committed_at = datetime.now(UTC)
        session.add(
            AdminAuditEvent(
                id=uuid4(),
                actor=str(kwargs["user_id"]),
                action="pipeline.input_import.committed",
                target_type="pipeline_input_import",
                target_id=str(row.id),
                event_metadata={"artifact_id": str(artifact.id), "kind": row.kind},
            )
        )
        await session.commit()
        return self._import_projection(row, artifact)

    @staticmethod
    def _import_projection(row: PipelineInputImport, artifact: Artifact) -> dict[str, Any]:
        return {
            "import_id": str(row.id),
            "state": row.state,
            "artifact": {
                "artifact_id": str(artifact.id),
                "artifact_type": artifact.artifact_type,
                "manifest_sha256": artifact.manifest_sha256,
                "content_sha256": artifact.content_hash,
                "safety_state": artifact.safety_state,
                "trust_class": row.trust_class,
            },
        }

    async def abort_import(self, **kwargs: Any) -> dict[str, Any]:
        session: AsyncSession = kwargs["session"]
        row = await self._locked_import(
            session, team_id=kwargs["team_id"], import_id=kwargs["import_id"]
        )
        if row.state == "committed":
            raise PipelineApiError(409, "import_committed", "Committed import cannot be aborted")
        if row.state != "aborted":
            try:
                await self._committer.abort_session(
                    session_id=kwargs["payload"].upload_session_id,
                    reason=kwargs["payload"].reason,
                    auth=ProducerAuthV1(subject_kind="team_admin", subject_id=kwargs["user_id"]),
                )
            except ArtifactCommitError as exc:
                raise _public_error(exc) from exc
            row.state = "aborted"
            row.abort_reason = kwargs["payload"].reason
            row.aborted_at = datetime.now(UTC)
            await session.commit()
        return {"import_id": str(row.id), "state": "aborted"}

    async def _publish_committed(
        self,
        session: AsyncSession,
        *,
        committed: CommittedArtifactsV1,
        team_id: UUID,
        actor_user_id: UUID,
        producer_kind: str,
        producer_id: UUID,
    ) -> Artifact:
        manifest, root_digest, marker_digest = await self._committer.committed_session_evidence(
            committed.upload_session_id
        )
        upload = ArtifactUploadSession(
            id=committed.upload_session_id,
            team_id=team_id,
            commit_kind=manifest.commit_kind,
            pipeline_input_import_id=producer_id if producer_kind == "input_import" else None,
            pipeline_input_materialization_id=(
                producer_id if producer_kind == "recipe_input_materialization" else None
            ),
            actor_user_id=actor_user_id,
            idempotency_key="published",
            request_digest=manifest.request_digest,
            prefix=(
                f"pipeline-input-imports/{team_id}/{producer_id}/"
                if producer_kind == "input_import"
                else f"pipeline-input-materializations/{team_id}/{producer_id}/"
            ),
            state="committed",
            expected_total_max_bytes=max(1, manifest.total_bytes),
            actual_total_bytes=manifest.total_bytes,
            canonical_manifest_json=manifest.model_dump(mode="json"),
            manifest_sha256=root_digest,
            committed_marker_sha256=marker_digest,
            upload_token_digest=None,
            expires_at=datetime.now(UTC),
            committed_at=datetime.now(UTC),
        )
        session.add(upload)
        by_id = {item.id: item for item in committed.artifacts}
        first: Artifact | None = None
        for root in manifest.artifacts:
            record = by_id[root.artifact_id]
            artifact = Artifact(
                id=record.id,
                artifact_type=record.artifact_type,
                name=record.name,
                team_id=team_id,
                producer_kind=producer_kind,
                pipeline_input_import_id=producer_id if producer_kind == "input_import" else None,
                pipeline_input_materialization_id=(
                    producer_id if producer_kind == "recipe_input_materialization" else None
                ),
                actor_user_id=actor_user_id,
                artifact_upload_session_id=committed.upload_session_id,
                manifest_sha256=record.manifest_sha256,
                stored_size_bytes=record.stored_size_bytes,
                unpacked_size_bytes=record.stored_size_bytes,
                file_count=record.file_count,
                created_by={"kind": producer_kind, "actor_user_id": str(actor_user_id)},
                content_hash=record.content_sha256,
                storage={"files": [item.model_dump(mode="json") for item in root.stored_files]},
                visibility="team",
                share_status="pending_scan",
                redaction_state="pending",
                safety_state=record.safety_state,
                retention={},
                provenance={"root_manifest_sha256": root_digest, "marker_sha256": marker_digest},
                artifact_metadata={},
            )
            session.add(artifact)
            first = first or artifact
            for stored in root.stored_files:
                session.add(
                    ArtifactUploadFile(
                        session_id=committed.upload_session_id,
                        file_index=stored.file_index,
                        preallocated_artifact_id=record.id,
                        relative_path=stored.relative_path,
                        artifact_name=record.name,
                        artifact_type=record.artifact_type,
                        producer="service" if stored.role == "semantic_document" else "client",
                        media_type=stored.media_type,
                        role=stored.role,
                        archive_format=stored.archive_format,
                        expected_max_bytes=max(1, stored.size_bytes),
                        expected_sha256=stored.sha256,
                        expected_size=stored.size_bytes,
                        computed_sha256=stored.sha256,
                        actual_size=stored.size_bytes,
                        state="verified",
                        ordered_part_receipts_json=[],
                    )
                )
        assert first is not None
        return first

    async def _semantic_document(
        self, upload: ArtifactUploadSession, artifact: Artifact
    ) -> dict[str, Any]:
        key = f"{upload.prefix}artifacts/{artifact.id}/artifact.json"
        import json

        value = json.loads(await self._store.get_object(bucket=self._bucket, key=key))
        if not isinstance(value, dict):
            raise ValueError("Artifact semantic document is not an object")
        return value

    async def _companion(
        self,
        session: AsyncSession,
        *,
        team_id: UUID,
        artifact_id: UUID,
        kind: str,
        recipe_digest: str,
    ) -> tuple[Artifact, DatasetCompatibilityV1 | MopBankCompatibilityV1 | None]:
        artifact = await session.get(Artifact, artifact_id)
        if (
            artifact is None
            or artifact.team_id != team_id
            or artifact.artifact_type != _target_type(kind)
            or artifact.producer_kind != "input_import"
            or artifact.safety_state != "unknown"
            or artifact.pipeline_input_import_id is None
            or artifact.artifact_upload_session_id is None
        ):
            raise PipelineApiError(422, "input_not_reusable", "Companion input is not admissible")
        imported = await session.get(PipelineInputImport, artifact.pipeline_input_import_id)
        upload = await session.get(ArtifactUploadSession, artifact.artifact_upload_session_id)
        if (
            imported is None
            or upload is None
            or imported.team_id != team_id
            or imported.kind != kind
            or imported.state != "committed"
            or imported.trust_class != "internal_trusted"
            or imported.recipe_name != "behavior-recovery"
            or imported.recipe_version != 1
            or imported.recipe_digest != recipe_digest
            or imported.committed_artifact_id != artifact.id
            or upload.state != "committed"
            or upload.manifest_sha256 != artifact.provenance.get("root_manifest_sha256")
            or upload.committed_marker_sha256 != artifact.provenance.get("marker_sha256")
        ):
            raise PipelineApiError(422, "input_not_reusable", "Companion transport proof failed")
        document = await self._semantic_document(upload, artifact)
        if kind == "dataset":
            compatibility = BehaviorDatasetSnapshotArtifactV1.model_validate(
                document
            ).payload.compatibility
            assert isinstance(compatibility, DatasetCompatibilityV1)
            return artifact, compatibility
        if kind == "mop_bank":
            compatibility = BehaviorMopBankArtifactV1.model_validate(document).payload.compatibility
            assert isinstance(compatibility, MopBankCompatibilityV1)
            return artifact, compatibility
        return artifact, None

    async def materialize_inputs(self, **kwargs: Any) -> dict[str, Any]:
        session: AsyncSession = kwargs["session"]
        if (kwargs["recipe_name"], kwargs["recipe_version"]) != ("behavior-recovery", 1):
            raise PipelineApiError(404, "recipe_not_found", "Recipe is not registered")
        registration = self._recipe()
        payload = kwargs["payload"]
        request_digest = canonical_digest(
            {
                "inputs": payload.inputs,
                "parameters": payload.parameters,
                "recipe": registration.identity,
                "task_set_id": payload.task_set_id,
                "team_id": kwargs["team_id"],
            },
            persisted=False,
        )
        existing = (
            await session.execute(
                select(PipelineInputMaterialization).where(
                    PipelineInputMaterialization.team_id == kwargs["team_id"],
                    PipelineInputMaterialization.idempotency_key == kwargs["idempotency_key"],
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            if existing.request_digest != request_digest:
                raise PipelineApiError(
                    409, "idempotency_conflict", "Materialization request changed"
                )
            return await self._materialization_projection(session, existing, payload.inputs)
        companions: dict[str, Artifact] = {}
        dataset_compatibility: DatasetCompatibilityV1 | None = None
        mop_compatibility: MopBankCompatibilityV1 | None = None
        for kind in ("dataset", "policy", "mop_bank"):
            artifact, compatibility = await self._companion(
                session,
                team_id=kwargs["team_id"],
                artifact_id=payload.inputs[kind],
                kind=kind,
                recipe_digest=registration.digest,
            )
            companions[kind] = artifact
            if isinstance(compatibility, DatasetCompatibilityV1):
                dataset_compatibility = compatibility
            elif isinstance(compatibility, MopBankCompatibilityV1):
                mop_compatibility = compatibility
        assert dataset_compatibility is not None and mop_compatibility is not None
        task_set_id = f"ts/{kwargs['team_id']}/{payload.task_set_id}"
        task_set = (
            await session.execute(
                visible_task_sets(team_id=kwargs["team_id"])
                .where(TaskSet.id == task_set_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if (
            task_set is None
            or task_set.status != "ready"
            or not task_set.evaluation_ready
            or "trajectory_generation" not in task_set.intents
        ):
            raise PipelineApiError(422, "task_set_not_ready", "BEHAVIOR TaskSet is not ready")
        manifest = await session.get(TaskSetManifest, task_set.id)
        generation = (
            await session.execute(
                select(TaskSetMaterializationJob.published_materialization_generation)
                .where(TaskSetMaterializationJob.task_set_id == task_set.id)
                .order_by(TaskSetMaterializationJob.finished_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        task_rows = list(
            (
                await session.execute(
                    select(Task).where(Task.task_set_id == task_set.id).order_by(Task.id)
                )
            ).scalars()
        )
        if manifest is None or generation is None or not task_rows:
            raise PipelineApiError(422, "task_set_not_ready", "TaskSet snapshot is incomplete")
        tasks = [self._task_snapshot(row, dataset_compatibility) for row in task_rows]
        if any(item.manifest_sha256 is None for item in companions.values()):
            raise PipelineApiError(422, "input_not_reusable", "Companion manifest is absent")
        refs = {
            kind: ContentArtifactRefV1(
                artifact_id=item.id,
                artifact_type=item.artifact_type,
                manifest_sha256=str(item.manifest_sha256),
                content_sha256=item.content_hash,
            )
            for kind, item in companions.items()
        }
        materialization_id = uuid4()
        snapshot = BehaviorMaterializationSourceSnapshotV1(
            source_task_set=SourceTaskSetRefV1.model_validate(
                {
                    "task_set_id": task_set.id,
                    "owning_team_id": task_set.owning_team_id,
                    "manifest_generation": generation,
                    "manifest_sha256": canonical_digest(manifest.manifest),
                    "intents": sorted(task_set.intents, key=lambda item: item.encode()),
                    "evaluation_ready": task_set.evaluation_ready,
                }
            ),
            tasks=tasks,
            companion_inputs=CompanionInputsV1(**refs),
            dataset_compatibility=dataset_compatibility,
            mop_bank_compatibility=mop_compatibility,
            control_event_id=materialization_id,
            loom_commit_sha=self._loom_commit_sha,
            caller_idempotency_key=kwargs["idempotency_key"],
        )
        frozen = InputMaterializationRequestV1(
            schema_version="loom.input-materialization-request.v1",
            materialization_id=materialization_id,
            team_id=kwargs["team_id"],
            actor_user_id=kwargs["user_id"],
            recipe=registration.identity,
            source_snapshot=snapshot.model_dump(mode="json"),
            source_snapshot_digest=canonical_digest(snapshot),
            parameters=payload.parameters,
            parameters_digest=canonical_digest(payload.parameters),
        )
        producer = InputMaterializationProducerV1(
            commit_kind="input_materialization",
            team_id=kwargs["team_id"],
            pipeline_input_materialization_id=materialization_id,
            actor_user_id=kwargs["user_id"],
            input_lineage_artifact_ids=sorted(
                (item.id for item in companions.values()), key=lambda item: item.bytes
            ),
            input_lineage_digests=sorted(str(item.manifest_sha256) for item in companions.values()),
        )
        try:
            committed, declaration = await self._materializations.materialize(
                frozen_request=frozen,
                producer=producer,
                materializer_kind="behavior-recovery",
                idempotency_key=kwargs["idempotency_key"],
            )
            row = PipelineInputMaterialization(
                id=materialization_id,
                team_id=kwargs["team_id"],
                created_by_user_id=kwargs["user_id"],
                recipe_name="behavior-recovery",
                recipe_version=1,
                recipe_digest=registration.digest,
                source_snapshot_json=frozen.source_snapshot,
                source_snapshot_digest=frozen.source_snapshot_digest,
                parameters_json=frozen.parameters,
                parameters_digest=frozen.parameters_digest,
                materialization_identity_digest=declaration.materialization_identity_digest,
                state="preparing",
                declared_outputs_json=[
                    item.model_dump(mode="json") for item in declaration.outputs
                ],
                declared_outputs_digest=canonical_digest(declaration.outputs),
                artifact_upload_session_id=None,
                result_bindings_json=None,
                official_materialization_kind=None,
                official_materialization_authority_id=None,
                official_materialization_authority_snapshot_digest=None,
                official_materialization_identity_digest=None,
                idempotency_key=kwargs["idempotency_key"],
                request_digest=request_digest,
                abort_reason=None,
                committed_at=None,
                aborted_at=None,
            )
            session.add(row)
            # Break the schema's intentional materialization<->upload FK cycle:
            # the preparation row exists inside this still-uncommitted SQL
            # transaction before the upload row points back to it.
            await session.flush()
            await self._publish_committed(
                session,
                committed=committed,
                team_id=kwargs["team_id"],
                actor_user_id=kwargs["user_id"],
                producer_kind="recipe_input_materialization",
                producer_id=materialization_id,
            )
        except (ArtifactCommitError, ValueError) as exc:
            await session.rollback()
            raise _public_error(exc) from exc
        by_name = {item.name: item for item in committed.artifacts}
        result_bindings = [
            {
                "graph_input_name": item.graph_input_name,
                "logical_name": item.logical_name,
                "artifact_id": str(by_name[item.logical_name].id),
            }
            for item in declaration.result_bindings
        ]
        row.state = "committed"
        row.artifact_upload_session_id = committed.upload_session_id
        row.result_bindings_json = result_bindings
        row.committed_at = datetime.now(UTC)
        for committed_artifact in committed.artifacts:
            for parent in companions.values():
                session.add(
                    ArtifactLineageEdge(
                        child_artifact_id=committed_artifact.id,
                        parent_artifact_id=parent.id,
                        relation="pipeline_input_materialization",
                        edge_metadata={"materialization_id": str(materialization_id)},
                    )
                )
        await session.commit()
        return await self._materialization_projection(session, row, payload.inputs)

    @staticmethod
    def _task_snapshot(row: Task, dataset: DatasetCompatibilityV1) -> TaskSnapshotRowV1:
        config = row.config
        required = {
            "task_format",
            "behavior_task_id",
            "task_name",
            "semantic_task_id",
            "eligible_eval_instance_ids",
            "source_bddl_path",
            "task_bundle_size_bytes",
        }
        if not required.issubset(config) or config["task_format"] != "behavior-1k/v1":
            raise PipelineApiError(422, "task_not_behavior", "Task config is not BEHAVIOR v1")
        behavior_id = config["behavior_task_id"]
        signed = next(
            (item for item in dataset.test_instance_sets if item.behavior_task_id == behavior_id),
            None,
        )
        if signed is None:
            raise PipelineApiError(422, "task_not_covered", "Task is absent from the dataset")
        checksum = row.checksum if row.checksum.startswith("sha256:") else f"sha256:{row.checksum}"
        return TaskSnapshotRowV1(
            loom_task_id=row.id,
            behavior_task_id=behavior_id,
            task_name=config["task_name"],
            semantic_task_id=config["semantic_task_id"],
            task_checksum=checksum,
            source_bddl_path=config["source_bddl_path"],
            eligible_eval_instance_ids=config["eligible_eval_instance_ids"],
            engine_task_instance_ids=signed.engine_task_instance_ids,
            task_bundle=ObjectTaskBundleRefV1(
                kind="object",
                object_sha256=checksum,
                size_bytes=config["task_bundle_size_bytes"],
            ),
        )

    @staticmethod
    async def _materialization_projection(
        session: AsyncSession, row: PipelineInputMaterialization, inputs: dict[str, UUID]
    ) -> dict[str, Any]:
        if row.state != "committed" or row.result_bindings_json is None:
            return {"materialization_id": str(row.id), "state": row.state, "results": []}
        result: list[dict[str, Any]] = []
        binding_ids = {
            item["graph_input_name"]: UUID(item["artifact_id"]) for item in row.result_bindings_json
        }
        for name in ("task_set", "task_instances", "dataset", "policy", "mop_bank"):
            artifact = await session.get(Artifact, binding_ids.get(name, inputs.get(name)))
            if artifact is None or artifact.manifest_sha256 is None:
                raise PipelineApiError(
                    409, "materialization_drift", "Committed input is unavailable"
                )
            result.append(
                {
                    "name": name,
                    "artifact_id": str(artifact.id),
                    "artifact_type": artifact.artifact_type,
                    "manifest_sha256": artifact.manifest_sha256,
                }
            )
        return {"materialization_id": str(row.id), "state": "committed", "results": result}


def build_behavior_pipeline_public_adapter(
    *, settings: Any, recipe_registry: OfficialRecipeRegistry
) -> BehaviorPipelinePublicAdapter:
    """Build the one process-wide explicit registry/adapter at startup."""

    store = MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )
    loom_commit = os.environ.get("LOOM_COMMIT_SHA", "0" * 40).strip()
    return BehaviorPipelinePublicAdapter(
        store=store,
        bucket=settings.artifacts_bucket,
        recipe_registry=recipe_registry,
        loom_commit_sha=loom_commit,
    )


def install_behavior_pipeline_public_adapter(*, app: Any, settings: Any) -> None:
    """Install the explicit registry and adapter during application startup."""

    registry = getattr(app.state, "pipeline_recipe_registry", None)
    if not isinstance(registry, OfficialRecipeRegistry):
        registry = OfficialRecipeRegistry()
        app.state.pipeline_recipe_registry = registry
    app.state.pipeline_public_adapter = build_behavior_pipeline_public_adapter(
        settings=settings,
        recipe_registry=registry,
    )


__all__ = [
    "BehaviorPipelinePublicAdapter",
    "build_behavior_pipeline_public_adapter",
    "install_behavior_pipeline_public_adapter",
]
