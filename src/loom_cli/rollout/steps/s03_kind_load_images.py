"""Step 03 — load images into kind cluster containerd (#340).

Delegates to the ``loom cluster load-images`` subcommand shipped in
#96 (see PR #344). Uses ``--check-only`` first so we don't waste time
re-loading images that are already present, then loads only the
missing ones.
"""

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
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
)
from loom_cli.rollout.steps.s02_build_images import (
    _matrix_digest,
    image_tag,
    rollout_image_bindings,
    rollout_images,
    rollout_images_from_candidate,
)
from loom_cli.rollout.steps.subprocess_util import run_captured


def _loom_cluster_load_images_argv(
    ctx: RolloutContext,
    *,
    images: tuple[tuple[str, str, str], ...],
    check_only: bool,
) -> list[str]:
    argv = candidate_loom_argv(
        "cluster",
        "load-images",
        "--cluster-name",
        ctx.cluster_name,
    )
    for image, _, _ in images:
        argv += ["--image", image_tag(image, ctx)]
    if check_only:
        argv.append("--check-only")
    return argv


def _registry_publication(ctx: RolloutContext) -> tuple[str, str] | None:
    return validate_container_registry_publication(load_cluster_config(ctx.cluster_config_path))


def _registry_image_ids(
    reference: str,
    *,
    expected: str,
) -> tuple[str, ...] | None:
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
    return step_dir.path.parent / "04-kind-load-images" / "registry-manifest-digests.json"


def registry_image_digests(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> dict[str, str]:
    """Load the exact registry manifest digests published by step 04."""
    publication = _registry_publication(ctx)
    if publication is None:
        return {}
    path = _registry_digest_path(step_dir)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("registry manifest digest artifact is unavailable") from exc
    expected_names = {name for name, _dockerfile, _context in rollout_images_from_candidate(ctx)}
    images = raw.get("images") if isinstance(raw, dict) else None
    if (
        not isinstance(raw, dict)
        or set(raw) != {
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


class KindLoadImagesStep(BaseStep):
    number = 4
    name = "kind-load-images"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        images = rollout_images_from_candidate(ctx)
        publication = _registry_publication(ctx)
        return {
            "cluster_name": ctx.cluster_name,
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
            "rollout_image_matrix_sha256": _matrix_digest(images),
            "container_registry": publication[0] if publication is not None else "",
            "container_registry_push": publication[1] if publication is not None else "",
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        # Cheap: call --check-only. Zero exit → all present.
        try:
            publication = _registry_publication(ctx)
            if publication is not None:
                images, image_ids = rollout_image_bindings(ctx, step_dir)
                push = publication[1]
                persisted = registry_image_digests(ctx, step_dir)
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
            images = rollout_images(ctx, step_dir)
            check = run_captured(
                _loom_cluster_load_images_argv(ctx, images=images, check_only=True),
                cwd=candidate_loom_cwd(step_dir),
                env=candidate_loom_env(step_dir),
            )
        except (CandidateToolingError, RuntimeError):
            return VerifyOutcome.UNKNOWN
        if check.returncode == 0:
            return VerifyOutcome.MATCH
        if check.returncode == 1:
            return VerifyOutcome.MISMATCH
        return VerifyOutcome.UNKNOWN

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
            publication = _registry_publication(ctx)
            if publication is not None:
                images, image_ids = rollout_image_bindings(ctx, step_dir)
            else:
                images = rollout_images(ctx, step_dir)
        except (CandidateToolingError, RuntimeError) as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        if publication is not None:
            pull, push = publication
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
                observed = _registry_image_ids(
                    target,
                    expected=image_ids[name],
                )
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

        # Try check-only first; skip the load if everything's already there.
        check = run_captured(
            _loom_cluster_load_images_argv(ctx, images=images, check_only=True),
            stdout_log=step_dir.artifact_path("check-only.stdout"),
            stderr_log=step_dir.artifact_path("check-only.stderr"),
            cwd=cwd,
            env=env,
        )
        if check.returncode == 0:
            step_dir.stdout_path().write_text(
                "check-only: all images already present in kind\n",
            )
            return RunResult(
                exit_code=0,
                summary="all images already loaded",
            )

        # Load. run_captured will overwrite the top-level stdout/stderr logs.
        result = run_captured(
            _loom_cluster_load_images_argv(ctx, images=images, check_only=False),
            stdout_log=step_dir.stdout_path(),
            stderr_log=step_dir.stderr_path(),
            cwd=cwd,
            env=env,
        )
        if result.returncode != 0:
            return RunResult(
                exit_code=result.returncode,
                error=(
                    result.stderr.strip().splitlines()[-1]
                    if result.stderr.strip()
                    else f"loom cluster load-images exited {result.returncode}"
                ),
            )
        return RunResult(
            exit_code=0,
            summary=f"loaded {len(images)} images into {ctx.cluster_name}",
        )
