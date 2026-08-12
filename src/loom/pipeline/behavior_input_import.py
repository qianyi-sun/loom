"""Validation and canonicalization for BEHAVIOR Pipeline input imports.

The import manifest is an untrusted declaration.  This module deliberately
does not fetch its provenance locator and does not extract payloads onto the
host: callers provide either an already-open deterministic tar+zstd stream or
an inventory assembled while streaming it.
"""

from __future__ import annotations

import csv
import hashlib
import io
import tarfile
import unicodedata
import zipfile
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import IO, Annotated, Any, Literal, cast
from uuid import UUID

import numpy as np
import zstandard
from pydantic import Field, StringConstraints, model_validator

from loom.integrations.behavior.contracts import (
    MOP_REQUIRED_COLUMNS,
    ArtifactFileV1,
    BehaviorDatasetSnapshotArtifactV1,
    BehaviorMopBankArtifactV1,
    BehaviorPolicyCheckpointArtifactV1,
    ChallengeInstancesRuntimeRootV1,
    ControlArtifactProvenanceV1,
    DatasetCompatibilityV1,
    ImportedInputPayloadV1,
    MopBankCompatibilityV1,
    PolicyCompatibilityV1,
    SourceProvenanceV1,
    TestInstanceSetV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import PipelineModel

MAX_ARTIFACT_DOCUMENT_BYTES = 67_108_864
MAX_NPZ_BYTES = 10 * 1024**3
MAX_IMPORT_BYTES = 100 * 1024**3
MAX_NPY_HEADER_BYTES = 65_536
MAX_COMPRESSION_RATIO = 100

InputKind = Literal["dataset", "policy", "mop_bank"]


class InputImportFileV1(PipelineModel):
    path: str
    sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    size_bytes: Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]
    media_type: str

    @model_validator(mode="after")
    def path_and_media_are_closed(self) -> InputImportFileV1:
        _safe_path(self.path)
        _nfc(self.media_type, "media type", 256)
        return self


class BehaviorInputImportManifestV1(PipelineModel):
    schema_version: Literal["behavior.input-import.v1"]
    kind: InputKind
    name: str
    version: str
    upstream: SourceProvenanceV1
    compatibility: DatasetCompatibilityV1 | PolicyCompatibilityV1 | MopBankCompatibilityV1
    files: list[InputImportFileV1]

    @model_validator(mode="after")
    def closed_inventory_and_kind(self) -> BehaviorInputImportManifestV1:
        _nfc(self.name, "name", 256)
        _nfc(self.version, "version", 256)
        paths = [item.path for item in self.files]
        if paths != sorted(paths, key=lambda item: item.encode("utf-8")):
            raise ValueError("import files must be bytewise sorted")
        if len(paths) != len(set(paths)) or len({item.casefold() for item in paths}) != len(paths):
            raise ValueError("import files must be unique without case collisions")
        expected_type = {
            "dataset": DatasetCompatibilityV1,
            "policy": PolicyCompatibilityV1,
            "mop_bank": MopBankCompatibilityV1,
        }[self.kind]
        if not isinstance(self.compatibility, expected_type):
            raise ValueError("import kind and compatibility branch disagree")
        maximum = 100_000 if self.kind == "dataset" else 10_000
        if not self.files or len(self.files) > maximum:
            raise ValueError(f"{self.kind} import file count is outside 1..{maximum}")
        if sum(item.size_bytes for item in self.files) > _kind_byte_limit(self.kind):
            raise ValueError(f"{self.kind} import bytes exceed the fixed limit")
        _validate_semantic_indexes(self)
        return self


@dataclass(frozen=True)
class VerifiedBundleFile:
    path: str
    sha256: str
    size_bytes: int
    media_type: str
    data: bytes | None = None


@dataclass(frozen=True)
class VerifiedBehaviorInput:
    artifact_document: dict[str, Any]
    artifact_bytes: bytes
    bundle_sha256: str
    bundle_size_bytes: int
    file_count: int


def _nfc(value: str, label: str, maximum: int) -> str:
    if not value or unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{label} must be nonempty NFC")
    if len(value.encode("utf-8")) > maximum:
        raise ValueError(f"{label} exceeds {maximum} UTF-8 bytes")
    return value


def _safe_path(value: str) -> str:
    if (
        not value
        or "\\" in value
        or value.startswith("/")
        or unicodedata.normalize("NFC", value) != value
    ):
        raise ValueError("import file path is not a canonical bundle-relative path")
    path = PurePosixPath(value)
    if any(part in {"", ".", ".."} for part in path.parts) or path.as_posix() != value:
        raise ValueError("import file path contains traversal or normalization drift")
    return value


def _kind_byte_limit(kind: InputKind) -> int:
    return 1024**4 if kind == "dataset" else MAX_IMPORT_BYTES


def _declared_map(manifest: BehaviorInputImportManifestV1) -> dict[str, InputImportFileV1]:
    return {item.path: item for item in manifest.files}


def _tree_digest(files: Iterable[InputImportFileV1], *, prefix: str) -> str:
    prefix_slash = prefix.rstrip("/") + "/"
    inventory = [
        {
            "relative_path": item.path.removeprefix(prefix_slash),
            "sha256": f"sha256:{item.sha256}",
            "size_bytes": item.size_bytes,
        }
        for item in files
        if item.path.startswith(prefix_slash)
    ]
    inventory.sort(key=lambda item: str(item["relative_path"]).encode("utf-8"))
    if not inventory:
        raise ValueError(f"declared tree {prefix} is empty")
    return canonical_digest(inventory, persisted=False)


def _validate_semantic_indexes(manifest: BehaviorInputImportManifestV1) -> None:
    declared = _declared_map(manifest)
    compatibility = manifest.compatibility
    if isinstance(compatibility, PolicyCompatibilityV1):
        expected = _tree_digest(manifest.files, prefix="checkpoint")
        if expected != compatibility.checkpoint_tree_sha256:
            raise ValueError("policy checkpoint_tree_sha256 drift")
        if any(not path.startswith("checkpoint/") for path in declared):
            raise ValueError("policy inventory must be strictly below checkpoint/")
        return
    if isinstance(compatibility, DatasetCompatibilityV1):
        _validate_dataset_indexes(compatibility, declared, manifest.files)
        return
    _validate_mop_indexes(compatibility, declared)


def _validate_dataset_indexes(
    compatibility: DatasetCompatibilityV1,
    declared: Mapping[str, InputImportFileV1],
    files: list[InputImportFileV1],
) -> None:
    cards = {item.behavior_task_id: item for item in compatibility.agentic_task_cards}
    global_demo_paths: set[str] = set()
    for card in compatibility.agentic_task_cards:
        file = declared.get(card.relative_path)
        if (
            file is None
            or file.sha256 != card.sha256.removeprefix("sha256:")
            or file.size_bytes != card.size_bytes
        ):
            raise ValueError("task-card inventory does not match declared files")
    for video_set in compatibility.agentic_demo_video_sets:
        for episode in video_set.episodes:
            for video in episode.files:
                expected = (
                    f"videos/task-{video_set.behavior_task_id:04d}/"
                    f"observation.images.rgb.{video.camera}/{episode.episode_id}.mp4"
                )
                if video.relative_path != expected or video.relative_path in global_demo_paths:
                    raise ValueError("demo path is wrong-task, duplicate, or noncanonical")
                global_demo_paths.add(video.relative_path)
                file = declared.get(video.relative_path)
                if (
                    file is None
                    or file.sha256 != video.sha256.removeprefix("sha256:")
                    or file.size_bytes != video.size_bytes
                ):
                    raise ValueError("demo inventory does not match declared files")
    for item in compatibility.test_instance_sets:
        if cards[item.behavior_task_id].task_name != item.task_name:
            raise ValueError("dataset task-card and test-instance names disagree")
    roots = compatibility.runtime_roots
    for root in roots:
        expected = _tree_digest(files, prefix=root.relative_path)
        if expected != root.tree_sha256:
            raise ValueError("dataset runtime-root tree digest drift")
    challenge = roots[1]
    assert isinstance(challenge, ChallengeInstancesRuntimeRootV1)
    exact = {
        "omnigibson/2025-challenge-task-instances/episodes.jsonl": challenge.episodes_jsonl_sha256,
        "omnigibson/2025-challenge-task-instances/metadata/test_instances.csv": (
            challenge.test_instances_csv_sha256
        ),
    }
    for path, digest in exact.items():
        file = declared.get(path)
        if file is None or f"sha256:{file.sha256}" != digest:
            raise ValueError("dataset fixed index is missing or has digest drift")
    if (
        _tree_digest(files, prefix="omnigibson/2025-challenge-task-instances/scenes")
        != challenge.scenes_tree_sha256
    ):
        raise ValueError("dataset scenes tree digest drift")


def _validate_mop_indexes(
    compatibility: MopBankCompatibilityV1,
    declared: Mapping[str, InputImportFileV1],
) -> None:
    bank_paths = {item.relative_path for item in compatibility.bank_files}
    for bank in compatibility.bank_files:
        file = declared.get(bank.relative_path)
        if (
            file is None
            or file.sha256 != bank.sha256.removeprefix("sha256:")
            or file.size_bytes != bank.size_bytes
        ):
            raise ValueError("MOP bank inventory does not match declared files")
    for path in bank_paths:
        if not path.endswith(".npz"):
            raise ValueError("MOP bank must contain only NPZ semantic banks")


def validate_test_instances_csv(encoded: bytes, expected: list[TestInstanceSetV1]) -> None:
    if b"\r" in encoded or not encoded.endswith(b"\n") or encoded.startswith(b"\xef\xbb\xbf"):
        raise ValueError("test_instances.csv must be UTF-8/LF RFC4180")
    try:
        rows = list(csv.reader(io.StringIO(encoded.decode("utf-8"), newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ValueError("test_instances.csv is malformed") from exc
    if not rows or rows[0] != ["task_id", "task_name", "test_instances"]:
        raise ValueError("test_instances.csv has the wrong header")
    if len(rows) != len(expected) + 1:
        raise ValueError("test_instances.csv task universe mismatch")
    for ordinal, (row, expected_item) in enumerate(zip(rows[1:], expected, strict=True)):
        if len(row) != 3 or row[0] != str(ordinal) or row[1] != expected_item.task_name:
            raise ValueError("test_instances.csv task row identity drift")
        tokens = row[2].split(",")
        if any(not token.strip().isdigit() for token in tokens):
            raise ValueError("test_instances.csv contains a malformed engine ID")
        observed = [int(token.strip(), 10) for token in tokens]
        if observed != expected_item.engine_task_instance_ids:
            raise ValueError("test_instances.csv selector order differs from the signed index")


def _tar_payload_inventory(
    stream: IO[bytes], manifest: BehaviorInputImportManifestV1
) -> Iterator[VerifiedBundleFile]:
    declared = _declared_map(manifest)
    seen: set[str] = set()
    decompressor = zstandard.ZstdDecompressor()
    with decompressor.stream_reader(stream, read_across_frames=False) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            for member in archive:
                if not member.isfile() or member.issym() or member.islnk():
                    raise ValueError("payload archive contains a non-regular member")
                if member.uid != 0 or member.gid != 0 or member.mtime != 0:
                    raise ValueError("payload archive metadata is nondeterministic")
                if member.mode not in {0o644, 0o755}:
                    raise ValueError("payload archive mode is outside 0644/0755")
                if not member.name.startswith("payload/"):
                    raise ValueError("payload archive member is outside payload/")
                relative = _safe_path(member.name.removeprefix("payload/"))
                expected = declared.get(relative)
                if expected is None or relative in seen or member.size != expected.size_bytes:
                    raise ValueError("payload archive inventory differs from the manifest")
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError("payload archive member cannot be read")
                digest = hashlib.sha256()
                captured = bytearray() if _capture_required(manifest, relative) else None
                size = 0
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
                    if captured is not None:
                        captured.extend(chunk)
                if size != expected.size_bytes or digest.hexdigest() != expected.sha256:
                    raise ValueError("payload archive member hash/size drift")
                seen.add(relative)
                yield VerifiedBundleFile(
                    path=relative,
                    sha256=expected.sha256,
                    size_bytes=size,
                    media_type=expected.media_type,
                    data=bytes(captured) if captured is not None else None,
                )
    if seen != set(declared):
        raise ValueError("payload archive is missing declared files")


def _capture_required(manifest: BehaviorInputImportManifestV1, relative: str) -> bool:
    if manifest.kind == "mop_bank":
        return relative.endswith(".npz")
    return relative == "omnigibson/2025-challenge-task-instances/metadata/test_instances.csv"


def verify_bundle_stream(
    stream: IO[bytes],
    *,
    bundle_sha256: str,
    bundle_size_bytes: int,
    manifest: BehaviorInputImportManifestV1,
    actor_user_id: UUID,
    control_event_id: UUID,
    recipe_digest: str,
    loom_commit_sha: str,
) -> VerifiedBehaviorInput:
    if not stream.seekable():
        raise ValueError("verified bundle stream must be seekable for digest and archive readback")
    digest = hashlib.sha256()
    observed_size = 0
    stream.seek(0)
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        observed_size += len(chunk)
    observed_digest = f"sha256:{digest.hexdigest()}"
    if observed_size != bundle_size_bytes or observed_digest != bundle_sha256:
        raise ValueError("payload bundle readback digest or byte count drift")
    stream.seek(0)
    files = list(_tar_payload_inventory(stream, manifest))
    captured = {item.path: item.data for item in files if item.data is not None}
    if isinstance(manifest.compatibility, DatasetCompatibilityV1):
        validate_test_instances_csv(
            captured["omnigibson/2025-challenge-task-instances/metadata/test_instances.csv"],
            manifest.compatibility.test_instance_sets,
        )
    if isinstance(manifest.compatibility, MopBankCompatibilityV1):
        for bank in manifest.compatibility.bank_files:
            validate_mop_npz(
                captured[bank.relative_path],
                behavior_task_id=bank.behavior_task_id,
                row_count=bank.row_count,
                source_revision=manifest.compatibility.source_revision,
                declared_files=manifest.files,
            )
    document = build_artifact_document(
        manifest,
        actor_user_id=actor_user_id,
        control_event_id=control_event_id,
        recipe_digest=recipe_digest,
        loom_commit_sha=loom_commit_sha,
    )
    encoded = canonical_document(document)
    if len(encoded) > MAX_ARTIFACT_DOCUMENT_BYTES:
        raise ValueError("artifact.json exceeds the 64 MiB boundary")
    return VerifiedBehaviorInput(
        artifact_document=document,
        artifact_bytes=encoded,
        bundle_sha256=observed_digest,
        bundle_size_bytes=observed_size,
        file_count=len(files),
    )


def build_artifact_document(
    manifest: BehaviorInputImportManifestV1,
    *,
    actor_user_id: UUID,
    control_event_id: UUID,
    recipe_digest: str,
    loom_commit_sha: str,
) -> dict[str, Any]:
    artifact_type = {
        "dataset": "behavior_dataset_snapshot.v1",
        "policy": "behavior_policy_checkpoint.v1",
        "mop_bank": "behavior_mop_bank.v1",
    }[manifest.kind]
    files = [
        ArtifactFileV1(
            name=f"payload_{index:06d}",
            relative_path=f"payload/{item.path}",
            sha256=f"sha256:{item.sha256}",
            size_bytes=item.size_bytes,
            media_type=item.media_type,
            required=True,
        )
        for index, item in enumerate(manifest.files)
    ]
    provenance = ControlArtifactProvenanceV1(
        producer_kind="control",
        loom_commit_sha=loom_commit_sha,
        control_event_id=control_event_id,
        actor_id=actor_user_id,
        recipe_digest=recipe_digest,
        source_artifacts=[],
    )
    payload = ImportedInputPayloadV1(
        name=manifest.name,
        version=manifest.version,
        source_provenance=manifest.upstream,
        compatibility=manifest.compatibility,
    )
    model = {
        "dataset": BehaviorDatasetSnapshotArtifactV1,
        "policy": BehaviorPolicyCheckpointArtifactV1,
        "mop_bank": BehaviorMopBankArtifactV1,
    }[manifest.kind]
    return cast(
        dict[str, Any],
        model(
            schema_version=artifact_type,
            payload=payload,
            files=files,
            provenance=provenance,
        ).model_dump(mode="json"),
    )


def validate_mop_npz(
    encoded: bytes,
    *,
    behavior_task_id: int,
    row_count: int,
    source_revision: str,
    declared_files: list[InputImportFileV1] | None = None,
) -> None:
    if len(encoded) > MAX_NPZ_BYTES:
        raise ValueError("MOP NPZ exceeds 10 GiB")
    with zipfile.ZipFile(io.BytesIO(encoded), mode="r") as archive:
        infos = archive.infolist()
        names = [item.filename for item in infos]
        expected = [f"{name}.npy" for name in MOP_REQUIRED_COLUMNS]
        if names != expected or len(names) != len(set(names)):
            raise ValueError("MOP NPZ member order/set differs from the fixed 17-column contract")
        total_compressed = sum(item.compress_size for item in infos)
        total_uncompressed = sum(item.file_size for item in infos)
        if (
            total_uncompressed > MAX_NPZ_BYTES
            or total_uncompressed > max(1, total_compressed) * MAX_COMPRESSION_RATIO
        ):
            raise ValueError("MOP NPZ aggregate size or compression ratio is unsafe")
        headers: dict[str, tuple[str, tuple[int, ...], bytes]] = {}
        for info, name in zip(infos, MOP_REQUIRED_COLUMNS, strict=True):
            path = PurePosixPath(info.filename)
            if path.is_absolute() or len(path.parts) != 1 or ".." in path.parts:
                raise ValueError("MOP NPZ contains a traversal member")
            if info.flag_bits & 0x1 or (info.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError("MOP NPZ contains encrypted or symlink content")
            if (
                info.file_size > MAX_NPZ_BYTES
                or info.file_size > max(1, info.compress_size) * MAX_COMPRESSION_RATIO
            ):
                raise ValueError("MOP NPZ member size or compression ratio is unsafe")
            with archive.open(info) as source:
                headers[name] = _read_npy(source)
        _validate_npy_contract(headers, row_count=row_count)
        meta_dtype, meta_shape, meta_data = headers["meta"]
        if meta_dtype != "<U4096" or meta_shape != () or len(meta_data) != 4096 * 4:
            raise ValueError("MOP meta must be one <U4096 scalar")
        text = meta_data.decode("utf-32-le").rstrip("\x00")
        if len(text) > 4096:
            raise ValueError("MOP meta exceeds 4096 Unicode scalar values")
        import json

        try:
            value = json.loads(text)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("MOP meta is not closed canonical JSON") from exc
        expected_keys = {
            "schema_version",
            "behavior_task_id",
            "row_count",
            "sampling_mode",
            "pose_dim",
            "action_dim",
            "source_revision",
            "source_inputs_sha256",
        }
        if (
            set(value) != expected_keys
            or canonical_document(value).removesuffix(b"\n") != text.encode()
        ):
            raise ValueError("MOP meta is not exact RFC8785 JCS/no-LF")
        if (
            value["schema_version"] != "behavior.mop-bank-meta.v1"
            or value["behavior_task_id"] != behavior_task_id
            or value["row_count"] != row_count
            or value["sampling_mode"] != "event_and_temporal"
            or value["pose_dim"] != 28
            or value["action_dim"] != 23
            or value["source_revision"] != source_revision
        ):
            raise ValueError("MOP meta identity differs from the outer compatibility record")
        if declared_files is not None:
            prefix = f"banks/task-{behavior_task_id:04d}/sources/"
            source_inventory = [
                {
                    "relative_path": item.path,
                    "sha256": f"sha256:{item.sha256}",
                    "size_bytes": item.size_bytes,
                }
                for item in declared_files
                if item.path.startswith(prefix)
            ]
            source_inventory.sort(key=lambda item: str(item["relative_path"]).encode("utf-8"))
            if not source_inventory or (
                canonical_digest(source_inventory, persisted=False) != value["source_inputs_sha256"]
            ):
                raise ValueError("MOP source_inputs_sha256 differs from outer declared files")


def _read_npy(source: IO[bytes]) -> tuple[str, tuple[int, ...], bytes]:
    import ast
    import struct

    magic = source.read(8)
    if not magic.startswith(b"\x93NUMPY"):
        raise ValueError("NPZ member is not NPY")
    version = magic[6:8]
    length_size = 2 if version in {b"\x01\x00"} else 4
    raw_length = source.read(length_size)
    if len(raw_length) != length_size:
        raise ValueError("NPY header is truncated")
    header_length = struct.unpack("<H" if length_size == 2 else "<I", raw_length)[0]
    if not 1 <= header_length <= MAX_NPY_HEADER_BYTES:
        raise ValueError("NPY header exceeds 65536 bytes")
    header = source.read(header_length)
    try:
        value = ast.literal_eval(header.decode("latin1").strip())
    except (SyntaxError, ValueError) as exc:
        raise ValueError("NPY header is malformed") from exc
    if set(value) != {"descr", "fortran_order", "shape"} or value["fortran_order"] is not False:
        raise ValueError("NPY header has unsupported fields or Fortran order")
    descr = value["descr"]
    shape = value["shape"]
    if (
        not isinstance(descr, str)
        or not isinstance(shape, tuple)
        or any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in shape)
    ):
        raise ValueError("NPY dtype or shape is invalid")
    data = source.read()
    if (descr.startswith("|") and "O" in descr) or descr.startswith("<O") or descr.startswith(">O"):
        raise ValueError("object/pickle NPY dtype is forbidden")
    return descr, shape, data


def _validate_npy_contract(
    headers: Mapping[str, tuple[str, tuple[int, ...], bytes]], *, row_count: int
) -> None:
    vectors = {
        "kind": ("<U32", (row_count,)),
        "object": ("<U256", (row_count,)),
        "category": ("<U256", (row_count,)),
        "manip_object": ("<U256", (row_count,)),
        "episode_id": ("<i8", (row_count,)),
        "step": ("<i8", (row_count,)),
        "corrected_end_step": ("<i8", (row_count,)),
        "stage_frac": ("<f4", (row_count,)),
        "joint_positions": ("<f4", (row_count, 28)),
        "base_rel": ("<f4", (row_count, 3)),
        "standoff_left": ("<f4", (row_count,)),
        "standoff_right": ("<f4", (row_count,)),
        "eef_rel_pos_left": ("<f4", (row_count, 3)),
        "eef_rel_quat_left": ("<f4", (row_count, 4)),
        "eef_rel_pos_right": ("<f4", (row_count, 3)),
        "eef_rel_quat_right": ("<f4", (row_count, 4)),
    }
    for name, expected in vectors.items():
        if headers[name][:2] != expected:
            raise ValueError(f"MOP column {name} has the wrong dtype/rank/shape")
        dtype, shape, data = headers[name]
        expected_bytes = int(np.prod(shape, dtype=np.int64)) * np.dtype(dtype).itemsize
        if len(data) != expected_bytes:
            raise ValueError(f"MOP column {name} byte length differs from its NPY header")
    arrays = {
        name: np.frombuffer(data, dtype=np.dtype(dtype)).reshape(shape)
        for name, (dtype, shape, data) in headers.items()
        if name != "meta"
    }
    for name, array in arrays.items():
        if array.dtype.kind in {"f", "i", "u"} and not np.isfinite(array).all():
            raise ValueError(f"MOP column {name} contains non-finite values")
    if not np.logical_and(arrays["stage_frac"] >= 0, arrays["stage_frac"] <= 1).all():
        raise ValueError("MOP stage_frac is outside [0,1]")
    for name in ("eef_rel_quat_left", "eef_rel_quat_right"):
        norms = np.linalg.norm(arrays[name], axis=1)
        if not np.allclose(norms, 1.0, atol=1e-4, rtol=0):
            raise ValueError(f"MOP column {name} contains non-unit quaternions")
    keys = list(
        zip(
            arrays["episode_id"].tolist(),
            arrays["step"].tolist(),
            strict=True,
        )
    )
    if len(keys) != len(set(keys)):
        raise ValueError("MOP (episode_id,step) keys are not unique")
    kinds = {str(item) for item in arrays["kind"].tolist()}
    if not {"event", "temporal"}.issubset(kinds):
        raise ValueError("MOP bank lacks event and temporal coverage")


__all__ = [
    "BehaviorInputImportManifestV1",
    "InputImportFileV1",
    "VerifiedBehaviorInput",
    "build_artifact_document",
    "validate_mop_npz",
    "validate_test_instances_csv",
    "verify_bundle_stream",
]
