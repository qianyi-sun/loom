"""Deterministic writer for ``behavior_mop_bank_npz_v1``.

This is Loom-owned code and accepts already-materialized numeric/string
columns.  It never imports, converts, or executes a legacy pickle bank.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from loom.integrations.behavior.contracts import MOP_REQUIRED_COLUMNS
from loom.pipeline.keys import canonical_digest, canonical_identity

_DTYPES = {
    "kind": "<U32",
    "object": "<U256",
    "category": "<U256",
    "manip_object": "<U256",
    "episode_id": "<i8",
    "step": "<i8",
    "corrected_end_step": "<i8",
    "stage_frac": "<f4",
    "joint_positions": "<f4",
    "base_rel": "<f4",
    "standoff_left": "<f4",
    "standoff_right": "<f4",
    "eef_rel_pos_left": "<f4",
    "eef_rel_quat_left": "<f4",
    "eef_rel_pos_right": "<f4",
    "eef_rel_quat_right": "<f4",
}


def _source_inputs(
    values: Sequence[Mapping[str, Any]], *, behavior_task_id: int
) -> list[dict[str, Any]]:
    expected = {"relative_path", "sha256", "size_bytes"}
    result: list[dict[str, Any]] = []
    for item in values:
        if set(item) != expected:
            raise ValueError("MOP source input records have missing/extra fields")
        path = item["relative_path"]
        digest = item["sha256"]
        size = item["size_bytes"]
        if (
            not isinstance(path, str)
            or not path.startswith(f"banks/task-{behavior_task_id:04d}/sources/")
            or ".." in path.split("/")
            or not isinstance(digest, str)
            or not digest.startswith("sha256:")
            or len(digest) != 71
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
        ):
            raise ValueError("MOP source input record is invalid")
        result.append(dict(item))
    result.sort(key=lambda item: str(item["relative_path"]).encode("utf-8"))
    if not result or len({item["relative_path"] for item in result}) != len(result):
        raise ValueError("MOP source inputs must be nonempty and unique")
    return result


def _fixed_string(name: str, value: Any, dtype: str) -> np.ndarray[Any, Any]:
    source = np.asarray(value)
    flattened = [str(item) for item in source.flat]
    limit = int(dtype.removeprefix("<U"))
    if any(len(item) > limit for item in flattened):
        raise ValueError(f"MOP {name} contains a string beyond {limit} Unicode scalars")
    result = np.asarray(value, dtype=dtype)
    if [str(item) for item in result.flat] != flattened:
        raise ValueError(f"MOP {name} fixed-width cast changed source text")
    return result


def fixed_unicode_scalar(source_text: str, *, width: int = 4096) -> np.ndarray[Any, Any]:
    """Return one fixed-width Unicode scalar without permitting truncation.

    Python ``len(str)`` counts Unicode scalar values (including one astral code
    point as one value), which is the source contract required before NumPy's
    fixed-width assignment.  The post-cast UTF-8 comparison makes any future
    NumPy normalization or truncation fail closed.
    """

    if width != 4096:
        raise ValueError("MOP meta width must remain exactly 4096 Unicode scalars")
    if len(source_text) > width:
        raise ValueError("MOP meta exceeds 4096 Unicode scalar values")
    result = np.asarray(source_text, dtype="<U4096")
    if str(result[()]).encode("utf-8") != source_text.encode("utf-8"):
        raise ValueError("MOP meta <U4096 cast changed source text")
    return result


def _numeric(name: str, value: Any, dtype: str) -> np.ndarray[Any, Any]:
    source = np.asarray(value)
    if source.dtype.kind == "O":
        raise ValueError(f"MOP {name} object dtype is forbidden")
    result = np.asarray(value, dtype=dtype)
    if result.dtype.kind == "f" and not np.isfinite(result).all():
        raise ValueError(f"MOP {name} contains non-finite numeric values")
    return result


def _validate_shapes(columns: Mapping[str, np.ndarray[Any, Any]], row_count: int) -> None:
    shapes = {
        "kind": (row_count,),
        "object": (row_count,),
        "category": (row_count,),
        "manip_object": (row_count,),
        "episode_id": (row_count,),
        "step": (row_count,),
        "corrected_end_step": (row_count,),
        "stage_frac": (row_count,),
        "joint_positions": (row_count, 28),
        "base_rel": (row_count, 3),
        "standoff_left": (row_count,),
        "standoff_right": (row_count,),
        "eef_rel_pos_left": (row_count, 3),
        "eef_rel_quat_left": (row_count, 4),
        "eef_rel_pos_right": (row_count, 3),
        "eef_rel_quat_right": (row_count, 4),
    }
    for name, shape in shapes.items():
        if columns[name].shape != shape:
            raise ValueError(f"MOP {name} has shape {columns[name].shape}, expected {shape}")
    if not np.logical_and(columns["stage_frac"] >= 0, columns["stage_frac"] <= 1).all():
        raise ValueError("MOP stage_frac is outside [0,1]")
    for name in ("eef_rel_quat_left", "eef_rel_quat_right"):
        if not np.allclose(np.linalg.norm(columns[name], axis=1), 1.0, atol=1e-4, rtol=0):
            raise ValueError(f"MOP {name} contains a non-unit quaternion")
    keys = list(zip(columns["episode_id"].tolist(), columns["step"].tolist(), strict=True))
    if len(keys) != len(set(keys)):
        raise ValueError("MOP (episode_id,step) keys must be unique")
    kinds = {str(value) for value in columns["kind"].tolist()}
    if not {"event", "temporal"}.issubset(kinds):
        raise ValueError("MOP bank requires both event and temporal rows")


def build_training_bank(
    destination: Path,
    *,
    behavior_task_id: int,
    source_revision: str,
    source_inputs: Sequence[Mapping[str, Any]],
    columns: Mapping[str, Any],
) -> Path:
    """Write one deterministic, pickle-free v2 NPZ and return its path."""

    if not 0 <= behavior_task_id <= 9999:
        raise ValueError("behavior_task_id is outside uint32 recipe bounds")
    if not source_revision or len(source_revision.encode("utf-8")) > 512:
        raise ValueError("source_revision is empty or too long")
    expected = set(MOP_REQUIRED_COLUMNS) - {"meta"}
    if set(columns) != expected:
        raise ValueError("MOP columns differ from the exact 16 row-bearing columns")
    source_inventory = _source_inputs(source_inputs, behavior_task_id=behavior_task_id)
    episode = np.asarray(columns["episode_id"])
    if episode.ndim != 1 or not 1 <= episode.shape[0] <= 10_000_000:
        raise ValueError("MOP row_count is outside 1..10000000")
    row_count = int(episode.shape[0])
    arrays: dict[str, np.ndarray[Any, Any]] = {}
    for name in MOP_REQUIRED_COLUMNS[1:]:
        dtype = _DTYPES[name]
        arrays[name] = (
            _fixed_string(name, columns[name], dtype)
            if dtype.startswith("<U")
            else _numeric(name, columns[name], dtype)
        )
    _validate_shapes(arrays, row_count)
    meta = {
        "schema_version": "behavior.mop-bank-meta.v1",
        "behavior_task_id": behavior_task_id,
        "row_count": row_count,
        "sampling_mode": "event_and_temporal",
        "pose_dim": 28,
        "action_dim": 23,
        "source_revision": source_revision,
        "source_inputs_sha256": canonical_digest(source_inventory, persisted=False),
    }
    meta_text = canonical_identity(meta).decode("utf-8")
    meta_array = fixed_unicode_scalar(meta_text)
    arrays = {"meta": meta_array, **arrays}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        destination,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
        strict_timestamps=True,
    ) as archive:
        for name in MOP_REQUIRED_COLUMNS:
            buffer = io.BytesIO()
            np.save(buffer, arrays[name], allow_pickle=False)
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            info.create_system = 3
            archive.writestr(info, buffer.getvalue(), compresslevel=6)
    return destination


build_mop_bank = build_training_bank

__all__ = ["build_mop_bank", "build_training_bank", "fixed_unicode_scalar"]
