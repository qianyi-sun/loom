"""Step 02 — build the exact candidate-bound rollout image plan."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.image_readiness import (
    AUXILIARY_ROLLOUT_IMAGES,
    DEFAULT_ROLLOUT_IMAGE_PLAN,
    ROLLOUT_IMAGES,
    ImageArtifactSet,
    RolloutImage,
    RolloutImagePlan,
    build_exact_images,
    image_plan_digest,
    verify_image_contract,
)
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.candidate_source import (
    materialize_candidate_blob,
    validate_candidate_worktree_identity,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult, run_captured

_MATRIX_ARTIFACT = "image-matrix.json"
_DONE_ARTIFACT_KEYS = frozenset({"image_matrix"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PRIMARY_PLAN: RolloutImagePlan = tuple(
    (name, dockerfile, ".") for name, dockerfile in ROLLOUT_IMAGES
)
_AUXILIARY_PLAN: RolloutImagePlan = tuple(
    (name, dockerfile, ".") for name, dockerfile in AUXILIARY_ROLLOUT_IMAGES
)


def _parse_rollout_images(raw: Any, *, role: str) -> RolloutImagePlan:
    if not isinstance(raw, list) or not raw:
        raise RuntimeError(f"candidate {role} rollout image query returned no images")
    images: list[RolloutImage] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or set(item) != {
            "image_name",
            "dockerfile",
            "context",
        }:
            raise RuntimeError(f"candidate {role} rollout image row {index} has invalid schema")
        values = (item["image_name"], item["dockerfile"], item["context"])
        if not all(isinstance(value, str) and value for value in values):
            raise RuntimeError(f"candidate {role} rollout image row {index} has invalid values")
        images.append(values)
    if len({image for image, _, _ in images}) != len(images):
        raise RuntimeError(f"candidate {role} rollout image query returned duplicate images")
    return tuple(images)


def _canonical_exact_plans(
    primary: RolloutImagePlan,
    auxiliary: RolloutImagePlan,
) -> tuple[RolloutImagePlan, RolloutImagePlan]:
    if len(primary) != len(_PRIMARY_PLAN) or set(primary) != set(_PRIMARY_PLAN):
        raise RuntimeError("candidate primary image role differs from the exact seven-image plan")
    if len(auxiliary) != len(_AUXILIARY_PLAN) or set(auxiliary) != set(_AUXILIARY_PLAN):
        raise RuntimeError("candidate auxiliary image role differs from the exact two-image plan")
    if set(primary) & set(auxiliary):
        raise RuntimeError("candidate rollout image roles overlap")
    plan = _PRIMARY_PLAN + _AUXILIARY_PLAN
    if plan != DEFAULT_ROLLOUT_IMAGE_PLAN:
        raise RuntimeError("candidate rollout image union differs from the exact nine-image plan")
    return _PRIMARY_PLAN, _AUXILIARY_PLAN


def _matrix_payload(images: RolloutImagePlan) -> list[dict[str, str]]:
    return [
        {"image_name": image, "dockerfile": dockerfile, "context": context}
        for image, dockerfile, context in images
    ]


def _matrix_digest(images: RolloutImagePlan) -> str:
    payload = json.dumps(
        _matrix_payload(images),
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _role_matrix_digest(
    primary: RolloutImagePlan,
    auxiliary: RolloutImagePlan,
) -> str:
    payload = {
        "auxiliary_images": _matrix_payload(auxiliary),
        "primary_images": _matrix_payload(primary),
    }
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _query_role_from_worktree(worktree: Path, *, role: str) -> RolloutImagePlan:
    result = run_captured(
        [
            "python3",
            "scripts/component_ownership.py",
            "release-images",
            "--rollout-role",
            role,
        ],
        cwd=worktree,
    )
    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip()
            or f"candidate component ownership {role} rollout image query failed"
        )
    try:
        raw: Any = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"candidate {role} rollout image query returned invalid JSON") from exc
    return _parse_rollout_images(raw, role=role)


def rollout_images_from_worktree(worktree: Path) -> RolloutImagePlan:
    """Resolve the primary images through a candidate worktree's manifest."""

    return _query_role_from_worktree(worktree, role="primary")


def rollout_auxiliary_images_from_worktree(worktree: Path) -> RolloutImagePlan:
    """Resolve auxiliary images through a candidate worktree's manifest."""

    return _query_role_from_worktree(worktree, role="auxiliary")


def _query_materialized_role(
    *,
    worktree: Path,
    script: Path,
    manifest: Path,
    role: str,
) -> RolloutImagePlan:
    result = run_captured(
        [
            "python3",
            str(script),
            "--repo-root",
            str(worktree),
            "--manifest",
            str(manifest),
            "release-images",
            "--rollout-role",
            role,
        ],
        cwd=worktree,
    )
    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip()
            or f"commit-bound component ownership {role} rollout image query failed"
        )
    try:
        raw: Any = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"commit-bound {role} rollout image query returned invalid JSON"
        ) from exc
    return _parse_rollout_images(raw, role=role)


def _rollout_matrix_from_candidate(
    ctx: RolloutContext,
) -> tuple[RolloutImagePlan, RolloutImagePlan, str]:
    worktree = validate_candidate_worktree_identity(ctx)
    with tempfile.TemporaryDirectory(prefix="loom-rollout-manifest-") as temp_dir:
        materialized_root = Path(temp_dir)
        script = materialize_candidate_blob(
            ctx,
            Path("scripts/component_ownership.py"),
            materialized_root / "component_ownership.py",
        )
        manifest = materialize_candidate_blob(
            ctx,
            Path("config/component-ownership.toml"),
            materialized_root / "component-ownership.toml",
        )
        primary = _query_materialized_role(
            worktree=worktree,
            script=script.evidence_path,
            manifest=manifest.evidence_path,
            role="primary",
        )
        auxiliary = _query_materialized_role(
            worktree=worktree,
            script=script.evidence_path,
            manifest=manifest.evidence_path,
            role="auxiliary",
        )
    primary, auxiliary = _canonical_exact_plans(primary, auxiliary)
    return primary, auxiliary, hashlib.sha256(manifest.data).hexdigest()


def rollout_images_from_candidate(ctx: RolloutContext) -> RolloutImagePlan:
    """Return only the seven primary images consumed by S03 and S12."""

    primary, _auxiliary, _manifest_sha256 = _rollout_matrix_from_candidate(ctx)
    return primary


def _matrix_envelope(
    ctx: RolloutContext,
    *,
    primary: RolloutImagePlan,
    auxiliary: RolloutImagePlan,
    manifest_sha256: str,
    artifact: ImageArtifactSet,
) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "resolved_sha": ctx.resolved_sha,
        "component_manifest_sha256": manifest_sha256,
        "matrix_sha256": _role_matrix_digest(primary, auxiliary),
        "image_plan_sha256": artifact.plan_digest,
        "image_artifact_sha256": artifact.artifact_digest,
        "primary_images": _matrix_payload(primary),
        "auxiliary_images": _matrix_payload(auxiliary),
        "image_ids": dict(sorted(artifact.image_digests.items())),
    }


def _persisted_rollout_matrix(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> tuple[RolloutImagePlan, RolloutImagePlan, dict[str, str], str]:
    matrix_path = step_dir.path.parent / "02-build-images" / _MATRIX_ARTIFACT
    try:
        raw: Any = json.loads(matrix_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"candidate rollout image matrix is unavailable: {matrix_path}") from exc
    expected_keys = {
        "schema_version",
        "resolved_sha",
        "component_manifest_sha256",
        "matrix_sha256",
        "image_plan_sha256",
        "image_artifact_sha256",
        "primary_images",
        "auxiliary_images",
        "image_ids",
    }
    if not isinstance(raw, dict) or set(raw) != expected_keys:
        raise RuntimeError("persisted rollout image matrix has invalid envelope schema")
    if not (
        type(raw["schema_version"]) is int
        and raw["schema_version"] == 2
        and isinstance(raw["resolved_sha"], str)
        and raw["resolved_sha"] == ctx.resolved_sha
        and isinstance(raw["component_manifest_sha256"], str)
        and _SHA256_RE.fullmatch(raw["component_manifest_sha256"]) is not None
        and isinstance(raw["matrix_sha256"], str)
        and isinstance(raw["image_plan_sha256"], str)
        and isinstance(raw["image_artifact_sha256"], str)
    ):
        raise RuntimeError("persisted rollout image matrix has invalid candidate binding")
    primary = _parse_rollout_images(raw["primary_images"], role="primary")
    auxiliary = _parse_rollout_images(raw["auxiliary_images"], role="auxiliary")
    primary, auxiliary = _canonical_exact_plans(primary, auxiliary)
    plan = primary + auxiliary
    if raw["matrix_sha256"] != _role_matrix_digest(primary, auxiliary):
        raise RuntimeError("persisted rollout image matrix digest is invalid")
    if raw["image_plan_sha256"] != image_plan_digest(plan):
        raise RuntimeError("persisted rollout image plan digest is invalid")
    image_ids = raw["image_ids"]
    if not (
        isinstance(image_ids, dict)
        and set(image_ids) == {image for image, _, _ in plan}
        and all(
            isinstance(value, str) and _IMAGE_ID_RE.fullmatch(value) is not None
            for value in image_ids.values()
        )
        and _SHA256_RE.fullmatch(raw["image_artifact_sha256"]) is not None
    ):
        raise RuntimeError("persisted rollout image identities are invalid")
    candidate_primary, candidate_auxiliary, manifest_sha256 = _rollout_matrix_from_candidate(ctx)
    if (
        primary != candidate_primary
        or auxiliary != candidate_auxiliary
        or raw["component_manifest_sha256"] != manifest_sha256
    ):
        raise RuntimeError("persisted rollout image matrix differs from candidate manifest")
    return primary, auxiliary, dict(image_ids), raw["image_artifact_sha256"]


def rollout_images(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> RolloutImagePlan:
    """Return the persisted, candidate-revalidated primary image plan."""

    primary, _auxiliary, _image_ids, _artifact_digest = _persisted_rollout_matrix(
        ctx,
        step_dir,
    )
    return primary


def image_tag(image_name: str, ctx: RolloutContext) -> str:
    return f"{image_name}:{ctx.image_tag}"


def _run_docker(argv: Sequence[str], cwd: Path | None) -> SubprocessResult:
    return run_captured(list(argv), cwd=cwd)


class BuildImagesStep(BaseStep):
    number = 2
    name = "build-images"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        primary, auxiliary, manifest_sha256 = _rollout_matrix_from_candidate(ctx)
        plan = primary + auxiliary
        return {
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
            "component_manifest_sha256": manifest_sha256,
            "matrix_sha256": _role_matrix_digest(primary, auxiliary),
            "image_plan_sha256": image_plan_digest(plan),
            "primary_images": _matrix_payload(primary),
            "auxiliary_images": _matrix_payload(auxiliary),
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        try:
            primary, auxiliary, image_ids, artifact_digest = _persisted_rollout_matrix(
                ctx,
                step_dir,
            )
            artifact = verify_image_contract(
                _run_docker,
                plan=primary + auxiliary,
                image_tag=ctx.image_tag,
                resolved_sha=ctx.resolved_sha,
                expected_digests=image_ids,
            )
        except (RuntimeError, ValueError):
            return VerifyOutcome.MISMATCH
        return (
            VerifyOutcome.MATCH
            if artifact.artifact_digest == artifact_digest
            else VerifyOutcome.MISMATCH
        )

    def verify_done(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        """Revalidate candidate authority and immutable image IDs before skip."""

        return self._verify_impl(ctx, step_dir)

    def requires_strict_live_verification(self) -> bool:
        return True

    def validate_done_artifacts(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
        artifacts: dict[str, str],
    ) -> bool:
        if set(artifacts) != _DONE_ARTIFACT_KEYS:
            return False
        expected = step_dir.artifact_path(_MATRIX_ARTIFACT).resolve()
        try:
            recorded = Path(artifacts["image_matrix"]).resolve(strict=True)
        except OSError:
            return False
        return recorded == expected and self._verify_impl(ctx, step_dir) is VerifyOutcome.MATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        expected_worktree = step_dir.path.parent / "01-worktree" / "src"
        if not expected_worktree.is_dir():
            return RunResult(
                exit_code=1,
                error=(
                    f"worktree not found at {expected_worktree}; step 01 must "
                    "succeed before step 02 can build against it."
                ),
            )
        try:
            worktree = validate_candidate_worktree_identity(ctx)
            if worktree.resolve() != expected_worktree.resolve():
                raise RuntimeError("candidate worktree differs from rollout evidence path")
            primary, auxiliary, manifest_sha256 = _rollout_matrix_from_candidate(ctx)
        except (RuntimeError, OSError) as exc:
            return RunResult(exit_code=2, error=str(exc))
        plan = primary + auxiliary
        try:
            artifact = build_exact_images(
                _run_docker,
                plan=plan,
                candidate_root=worktree,
                image_tag=ctx.image_tag,
                resolved_sha=ctx.resolved_sha,
            )
        except ValueError as exc:
            return RunResult(exit_code=1, error=str(exc))
        self.write_artifact(
            step_dir,
            _MATRIX_ARTIFACT,
            json.dumps(
                _matrix_envelope(
                    ctx,
                    primary=primary,
                    auxiliary=auxiliary,
                    manifest_sha256=manifest_sha256,
                    artifact=artifact,
                ),
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        self.write_stdout(
            step_dir,
            "\n".join(
                f"{name}={image_id}" for name, image_id in sorted(artifact.image_digests.items())
            ),
        )
        return RunResult(
            exit_code=0,
            summary=f"verified {len(plan)} exact images at tag {ctx.image_tag}",
            artifacts={
                "image_matrix": str(step_dir.artifact_path(_MATRIX_ARTIFACT)),
            },
        )


__all__ = [
    "AUXILIARY_ROLLOUT_IMAGES",
    "ROLLOUT_IMAGES",
    "BuildImagesStep",
    "image_tag",
    "rollout_images",
    "rollout_images_from_candidate",
    "rollout_images_from_worktree",
]
