"""Step 04 — publish exact candidate images to the cluster registry."""

from __future__ import annotations

import json
import re
from pathlib import Path

from loom_cli.cluster_config import (
    load_cluster_config,
    validate_container_registry_publication,
)
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.s02_build_images import (
    _matrix_digest,
    image_tag,
    rollout_image_bindings,
    rollout_images_from_candidate,
)
from loom_cli.rollout.steps.subprocess_util import run_captured


def _registry_publication(ctx: RolloutContext) -> tuple[str, str] | None:
    return validate_container_registry_publication(
        load_cluster_config(ctx.cluster_config_path)
    )


def _required_registry_publication(ctx: RolloutContext) -> tuple[str, str]:
    publication = _registry_publication(ctx)
    if publication is None:
        raise RuntimeError(
            "protected rollouts require container_registry and "
            "container_registry_push"
        )
    return publication


def _registry_image_ids(reference: str, *, expected: str) -> tuple[str, ...] | None:
    result = run_captured(
        ["docker", "manifest", "inspect", "--insecure", "--verbose", reference]
    )
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    manifest = payload.get("SchemaV2Manifest", payload)
    descriptor = payload.get("Descriptor")
    config = manifest.get("config") if isinstance(manifest, dict) else None
    config_digest = config.get("digest") if isinstance(config, dict) else None
    descriptor_digest = descriptor.get("digest") if isinstance(descriptor, dict) else None
    if config_digest != expected:
        return None
    values = [expected]
    if isinstance(descriptor_digest, str) and re.fullmatch(
        r"sha256:[0-9a-f]{64}", descriptor_digest
    ):
        values.append(descriptor_digest)
    return tuple(dict.fromkeys(values))


def _registry_digest_path(step_dir: StepDir) -> Path:
    return step_dir.path.parent / "04-publish-images" / "registry-manifest-digests.json"


def registry_image_digests(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> dict[str, str]:
    """Load exact registry manifest digests published by step 04."""
    publication = _required_registry_publication(ctx)
    path = _registry_digest_path(step_dir)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("registry manifest digest artifact is unavailable") from exc
    expected_names = {name for name, _dockerfile, _context in rollout_images_from_candidate(ctx)}
    images = raw.get("images") if isinstance(raw, dict) else None
    if (
        not isinstance(raw, dict)
        or set(raw)
        != {
            "container_registry",
            "container_registry_push",
            "image_tag",
            "images",
        }
        or raw.get("container_registry") != publication[0]
        or raw.get("container_registry_push") != publication[1]
        or raw.get("image_tag") != ctx.image_tag
        or not isinstance(images, dict)
        or set(images) != expected_names
        or any(
            not isinstance(name, str)
            or not isinstance(digest, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
            for name, digest in images.items()
        )
    ):
        raise RuntimeError("registry manifest digest artifact drifted")
    return {str(name): str(digest) for name, digest in images.items()}


class PublishImagesStep(BaseStep):
    number = 4
    name = "publish-images"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        images = rollout_images_from_candidate(ctx)
        publication = _required_registry_publication(ctx)
        return {
            "cluster_name": ctx.cluster_name,
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
            "rollout_image_matrix_sha256": _matrix_digest(images),
            "container_registry": publication[0],
            "container_registry_push": publication[1],
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        try:
            images, image_ids = rollout_image_bindings(ctx, step_dir)
            push = _required_registry_publication(ctx)[1]
            persisted = registry_image_digests(ctx, step_dir)
        except (OSError, RuntimeError, ValueError):
            return VerifyOutcome.UNKNOWN
        return (
            VerifyOutcome.MATCH
            if all(
                (observed := _registry_image_ids(
                    f"{push}/{name}:{ctx.image_tag}",
                    expected=image_ids[name],
                ))
                is not None
                and len(observed) == 2
                and observed[1] == persisted[name]
                for name, _dockerfile, _context in images
            )
            else VerifyOutcome.MISMATCH
        )

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            pull, push = _required_registry_publication(ctx)
            images, image_ids = rollout_image_bindings(ctx, step_dir)
        except (OSError, RuntimeError, ValueError) as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=str(exc))

        registry_digests: dict[str, str] = {}
        for name, _dockerfile, _context in images:
            source = image_tag(name, ctx)
            target = f"{push}/{source}"
            tag = run_captured(
                ["docker", "tag", source, target],
                stdout_log=step_dir.artifact_path(f"{name}-tag.stdout"),
                stderr_log=step_dir.artifact_path(f"{name}-tag.stderr"),
            )
            if tag.returncode != 0:
                return RunResult(exit_code=tag.returncode, error=f"tagging {name} failed")
            pushed = run_captured(
                ["docker", "push", target],
                stdout_log=step_dir.artifact_path(f"{name}-push.stdout"),
                stderr_log=step_dir.artifact_path(f"{name}-push.stderr"),
            )
            observed = _registry_image_ids(target, expected=image_ids[name])
            if pushed.returncode != 0 or observed is None or len(observed) != 2:
                return RunResult(
                    exit_code=pushed.returncode or 1,
                    error=f"publishing exact {name} image failed",
                )
            registry_digests[name] = observed[1]
        digest_path = step_dir.artifact_path("registry-manifest-digests.json")
        digest_path.write_text(
            json.dumps(
                {
                    "container_registry": pull,
                    "container_registry_push": push,
                    "image_tag": ctx.image_tag,
                    "images": registry_digests,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        summary = (
            f"published {len(images)} exact images through {push} "
            f"for k3s pull prefix {pull}"
        )
        step_dir.stdout_path().write_text(summary + "\n", encoding="utf-8")
        return RunResult(
            exit_code=0,
            summary=summary,
            artifacts={"registry_manifest_digests": str(digest_path)},
        )


__all__ = ["PublishImagesStep", "registry_image_digests"]
