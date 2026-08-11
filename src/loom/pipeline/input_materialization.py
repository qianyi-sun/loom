"""Closed two-phase Recipe input materialization protocol."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Annotated, Any, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.artifact_commit import (
    ArtifactCommitService,
    CommittedArtifactsV1,
    InputMaterializationProducerV1,
    ProducerAuthV1,
    UploadAuthV1,
    UploadFilePlanV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom.pipeline.spec import (
    ArtifactType,
    BindingName,
    Digest,
    PipelineModel,
    PositiveSafeInt,
    RecipeIdentityV1,
)


class InputMaterializationRequestV1(PipelineModel):
    schema_version: Literal["loom.input-materialization-request.v1"]
    materialization_id: UUID
    team_id: UUID
    actor_user_id: UUID
    recipe: RecipeIdentityV1
    source_snapshot: dict[str, Any]
    source_snapshot_digest: Digest
    parameters: dict[str, Any]
    parameters_digest: Digest

    @model_validator(mode="after")
    def frozen_digests_are_exact(self) -> InputMaterializationRequestV1:
        if canonical_digest(self.source_snapshot) != self.source_snapshot_digest:
            raise ValueError("source snapshot digest drift")
        if canonical_digest(self.parameters) != self.parameters_digest:
            raise ValueError("materialization parameter digest drift")
        return self


class OfficialOneInputMaterializationRequestV1(PipelineModel):
    schema_version: Literal["loom.official-one-input-materialization.v1"]
    official_materialization_kind: Annotated[str, StringConstraints(min_length=1, max_length=64)]
    authority_id: UUID
    request_identity_digest: Digest


class OfficialInputMaterializationAuthorityV1(PipelineModel):
    service_name: str
    service_instance_id: UUID


class OfficialInputMaterializationGrantV1(PipelineModel):
    team_id: UUID
    official_materialization_kind: str
    authority_id: UUID
    authority_snapshot_digest: Digest
    request_identity_digest: Digest
    actor_user_id: UUID
    frozen_request: InputMaterializationRequestV1


class OfficialInputMaterializationAuthorityRepositoryV1(Protocol):
    async def load_and_lock(
        self, request: OfficialOneInputMaterializationRequestV1
    ) -> OfficialInputMaterializationGrantV1: ...

    async def complete_locked(
        self, materialization_id: UUID, result_bindings_digest: str
    ) -> None: ...


class MaterializationOutputV1(PipelineModel):
    logical_name: BindingName
    artifact_type: ArtifactType
    max_bytes: Annotated[PositiveSafeInt, Field(le=16_777_216)]
    lineage_source_ids: list[UUID]

    @field_validator("lineage_source_ids")
    @classmethod
    def lineage_is_ordered_and_unique(cls, value: list[UUID]) -> list[UUID]:
        if value != sorted(value, key=lambda item: item.bytes) or len(value) != len(
            set(value)
        ):
            raise ValueError("materialization lineage must be ordered and unique")
        return value


class MaterializationResultBindingV1(PipelineModel):
    graph_input_name: BindingName
    logical_name: BindingName


class MaterializationSourceArtifactRefV1(PipelineModel):
    artifact_id: UUID
    artifact_type: ArtifactType
    manifest_sha256: Digest


class MaterializationDeclarationV1(PipelineModel):
    materialization_identity_digest: Digest
    outputs: Annotated[list[MaterializationOutputV1], Field(min_length=1, max_length=202)]
    result_bindings: list[MaterializationResultBindingV1]
    source_artifact_refs: list[MaterializationSourceArtifactRefV1]

    @model_validator(mode="after")
    def declaration_is_ordered_and_closed(self) -> MaterializationDeclarationV1:
        names = [item.logical_name for item in self.outputs]
        if names != sorted(names, key=lambda value: value.encode()) or len(names) != len(set(names)):
            raise ValueError("materialization outputs must be bytewise ordered and unique")
        bindings = [(item.graph_input_name, item.logical_name) for item in self.result_bindings]
        if bindings != sorted(
            bindings, key=lambda value: (value[0].encode(), value[1].encode())
        ) or len(bindings) != len(set(bindings)):
            raise ValueError("materialization result bindings must be bytewise ordered")
        if any(item.logical_name not in set(names) for item in self.result_bindings):
            raise ValueError("materialization result binding references an unknown output")
        source_ids = [item.artifact_id for item in self.source_artifact_refs]
        if source_ids != sorted(source_ids, key=lambda item: item.bytes) or len(
            source_ids
        ) != len(set(source_ids)):
            raise ValueError("materialization source references must be ordered and unique")
        declared_sources = set(source_ids)
        if any(
            not set(output.lineage_source_ids).issubset(declared_sources)
            for output in self.outputs
        ):
            raise ValueError("materialization lineage references an unknown source")
        identity = self.model_dump(mode="python", exclude={"materialization_identity_digest"})
        if canonical_digest(identity, persisted=False) != self.materialization_identity_digest:
            raise ValueError("materialization declaration digest drift")
        return self


class MaterializedArtifactDocumentV1(PipelineModel):
    logical_name: BindingName
    value: Any

    @field_validator("value")
    @classmethod
    def value_is_canonical_json(cls, value: Any) -> Any:
        canonical_digest(value)
        return value


class RecipeInputMaterializer(Protocol):
    def declare(
        self, *, frozen_request: InputMaterializationRequestV1
    ) -> MaterializationDeclarationV1: ...

    def render(
        self,
        *,
        declaration: MaterializationDeclarationV1,
        artifact_ids: dict[str, UUID],
    ) -> Iterator[MaterializedArtifactDocumentV1]: ...


async def _one_payload(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


class PipelineInputMaterializationService:
    def __init__(
        self,
        *,
        artifact_committer: ArtifactCommitService,
        materializers: dict[str, RecipeInputMaterializer],
    ) -> None:
        if not materializers or "*" in materializers or "default" in materializers:
            raise ValueError("materializer registry must be explicit and nonempty")
        self._artifact_committer = artifact_committer
        self._materializers = dict(materializers)

    async def materialize(
        self,
        *,
        frozen_request: InputMaterializationRequestV1,
        producer: InputMaterializationProducerV1,
        materializer_kind: str,
        idempotency_key: str,
    ) -> tuple[CommittedArtifactsV1, MaterializationDeclarationV1]:
        try:
            materializer = self._materializers[materializer_kind]
        except KeyError as exc:
            raise ValueError("materializer kind is not registered") from exc
        declaration = materializer.declare(frozen_request=frozen_request)
        artifact_ids = {
            item.logical_name: uuid5(frozen_request.materialization_id, item.logical_name)
            for item in declaration.outputs
        }
        plans = [
            UploadFilePlanV1(
                file_index=index,
                preallocated_artifact_id=artifact_ids[output.logical_name],
                relative_path="artifact.json",
                artifact_name=output.logical_name,
                artifact_type=output.artifact_type,
                producer="service",
                media_type="application/json",
                role="semantic_document",
                archive_format="none",
                expected_max_bytes=output.max_bytes,
                expected_sha256=None,
                expected_size=None,
            )
            for index, output in enumerate(declaration.outputs)
        ]
        request_digest = canonical_digest(
            {
                "artifact_ids": artifact_ids,
                "declaration": declaration,
                "frozen_request": frozen_request,
            },
            persisted=False,
        )
        replay = await self._artifact_committer.replay_committed(
            producer=producer,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
        )
        if replay is not None:
            return replay, declaration
        grant = await self._artifact_committer.prepare_session(
            producer=producer,
            files=plans,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
        )
        auth = UploadAuthV1(upload_token=grant.upload_token)
        documents = iter(
            materializer.render(declaration=declaration, artifact_ids=artifact_ids)
        )
        for index, output in enumerate(declaration.outputs):
            try:
                document = next(documents)
            except StopIteration as exc:
                raise ValueError("materializer yielded too few documents") from exc
            if document.logical_name != output.logical_name:
                raise ValueError("materializer render order or name drifted")
            payload = canonical_document(document.value)
            if not payload or len(payload) > output.max_bytes:
                raise ValueError("materialized document size exceeds declaration")

            receipt = await self._artifact_committer.write_part(
                session_id=grant.upload_session_id,
                file_index=index,
                part_number=1,
                content_length=len(payload),
                content_sha256=digest_bytes(payload),
                body=_one_payload(payload),
                auth=auth,
            )
            await self._artifact_committer.complete_file(
                session_id=grant.upload_session_id,
                file_index=index,
                ordered_parts=[receipt],
                auth=auth,
            )
            del document, payload
        try:
            next(documents)
        except StopIteration:
            pass
        else:
            raise ValueError("materializer yielded too many documents")
        committed = await self._artifact_committer.commit_session(
            session_id=grant.upload_session_id,
            auth=ProducerAuthV1(subject_kind="official_service", subject_id=UUID(int=0)),
        )
        if not isinstance(committed, CommittedArtifactsV1):
            raise ValueError("input materialization cannot stop at committed_ready")
        return committed, declaration

    async def materialize_official_one(
        self,
        request: OfficialOneInputMaterializationRequestV1,
        authority: OfficialInputMaterializationAuthorityRepositoryV1,
    ) -> CommittedArtifactsV1:
        grant = await authority.load_and_lock(request)
        if (
            grant.authority_id != request.authority_id
            or grant.official_materialization_kind != request.official_materialization_kind
            or grant.request_identity_digest != request.request_identity_digest
            or grant.team_id != grant.frozen_request.team_id
            or grant.actor_user_id != grant.frozen_request.actor_user_id
        ):
            raise ValueError("official materialization grant identity drift")
        producer = InputMaterializationProducerV1(
            commit_kind="input_materialization",
            team_id=grant.team_id,
            pipeline_input_materialization_id=grant.frozen_request.materialization_id,
            actor_user_id=grant.actor_user_id,
        )
        committed, declaration = await self.materialize(
            frozen_request=grant.frozen_request,
            producer=producer,
            materializer_kind=grant.official_materialization_kind,
            idempotency_key=(
                f"official-one:{grant.authority_id}:{grant.official_materialization_kind}:"
                f"{grant.request_identity_digest}"
            ),
        )
        await authority.complete_locked(
            grant.frozen_request.materialization_id,
            canonical_digest(declaration.result_bindings),
        )
        return committed


__all__ = [
    "InputMaterializationRequestV1",
    "MaterializationDeclarationV1",
    "MaterializationOutputV1",
    "MaterializationResultBindingV1",
    "MaterializationSourceArtifactRefV1",
    "MaterializedArtifactDocumentV1",
    "OfficialInputMaterializationAuthorityRepositoryV1",
    "OfficialInputMaterializationAuthorityV1",
    "OfficialInputMaterializationGrantV1",
    "OfficialOneInputMaterializationRequestV1",
    "PipelineInputMaterializationService",
    "RecipeInputMaterializer",
]
