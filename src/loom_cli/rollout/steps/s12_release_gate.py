"""Step 12 — release gate (#340, #444)."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from loom_cli.cluster_cmd import _rendered_deployment_images
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import RunResult
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    candidate_relative_path,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.s02_build_images import ROLLOUT_IMAGES, image_tag
from loom_cli.rollout.steps.s10_env_state import _profile_path_for
from loom_cli.rollout.steps.subcommand_step import SubcommandStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult, run_captured

_GB10_STATUS_MAX_ATTEMPTS = 180
_GB10_STATUS_RETRY_DELAY_SEC = 5.0


def _is_transient_cp_unreachable(stderr: str) -> bool:
    return "could not reach CP" in stderr


def _is_gb10_convergence_failure(result: SubprocessResult) -> bool:
    text = f"{result.stdout}\n{result.stderr}"
    return (
        "gb10-worker-convergence" in text
        or "GB10 worker" in text
        or "GB10 rollout target mismatch" in text
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _repo_part(image: str) -> str:
    image = image.split("@", 1)[0]
    last_slash = image.rfind("/")
    last_colon = image.rfind(":")
    if last_colon > last_slash:
        return image[:last_colon]
    return image


def _matching_repo_digest(image: str, repo_digests: list[str]) -> str | None:
    if not repo_digests:
        return None
    repo = _repo_part(image)
    for digest in repo_digests:
        if digest.split("@", 1)[0] == repo:
            return digest
    return repo_digests[0]


def _image_identities_from_inspect(
    *,
    rendered_images: dict[str, dict[str, str]],
    inspect_docs: list[dict[str, Any]],
    managed_images: set[str],
) -> dict[str, dict[str, dict[str, str]]]:
    docs_by_tag: dict[str, dict[str, Any]] = {}
    for doc in inspect_docs:
        for tag in _string_list(doc.get("RepoTags")):
            docs_by_tag[tag] = doc

    identities: dict[str, dict[str, dict[str, str]]] = {}
    for deployment_name, by_container in rendered_images.items():
        for container_name, image in by_container.items():
            if image not in managed_images:
                continue
            inspect_doc = docs_by_tag.get(image)
            if inspect_doc is None:
                raise ValueError(f"Docker inspect output missing managed image {image}")
            identity: dict[str, str] = {"image": image}
            image_id = inspect_doc.get("Id")
            if isinstance(image_id, str) and image_id:
                identity["image_id"] = image_id
            repo_digest = _matching_repo_digest(
                image,
                _string_list(inspect_doc.get("RepoDigests")),
            )
            if repo_digest:
                identity["repo_digest"] = repo_digest
            if "image_id" not in identity and "repo_digest" not in identity:
                raise ValueError(
                    f"Docker inspect output for {image} lacks Id and RepoDigests",
                )
            identities.setdefault(deployment_name, {})[container_name] = identity
    return identities


class ReleaseGateStep(SubcommandStep):
    number = 12
    name = "release-gate"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "environment": ctx.environment,
            "image_tag": ctx.image_tag,
            "namespace": ctx.namespace,
            "resolved_sha": ctx.resolved_sha,
        }

    def release_manifest_path(self, ctx: RolloutContext, step_dir: StepDir) -> Path:
        return step_dir.artifact_path(f"release-manifest-{ctx.image_tag}.json")

    def gb10_status_path(self, ctx: RolloutContext, step_dir: StepDir) -> Path:
        return step_dir.artifact_path(f"gb10-workers-status-{ctx.image_tag}.json")

    def expected_image_identities_path(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> Path:
        return step_dir.artifact_path(f"image-identities-{ctx.image_tag}.json")

    def environment_state_check_path(self, step_dir: StepDir) -> Path:
        return (
            step_dir.path.parent
            / "10-env-state"
            / "environment-state-check.json"
        )

    def release_manifest_argv(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> Sequence[str]:
        argv = candidate_loom_argv(
            "cluster",
            "release-manifest",
            "--config",
            str(rollout_cluster_config(ctx, step_dir)),
            "--environment",
            ctx.environment,
            "--image-tag",
            ctx.image_tag,
            "--git-sha",
            ctx.resolved_sha,
            "--expected-image-identities-json",
            str(self.expected_image_identities_path(ctx, step_dir)),
            "--output",
            str(self.release_manifest_path(ctx, step_dir)),
        )
        profile = _profile_path_for(ctx)
        if profile is not None:
            argv.extend([
                "--environment-state-file",
                str(candidate_relative_path(Path(profile), step_dir)),
                "--env-config-version",
                ctx.image_tag,
            ])
        return argv

    def _manifest_external_workers(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> dict[str, Any]:
        path = self.release_manifest_path(ctx, step_dir)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        external_workers = raw.get("external_workers")
        return external_workers if isinstance(external_workers, dict) else {}

    def _gb10_status_environment(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> str:
        external_workers = self._manifest_external_workers(ctx, step_dir)
        control_plane_environment = external_workers.get("control_plane_environment")
        if isinstance(control_plane_environment, str) and control_plane_environment:
            return control_plane_environment
        return ctx.environment

    def _gb10_desired_state_count(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> int | None:
        external_workers = self._manifest_external_workers(ctx, step_dir)
        if "gb10_desired_states" not in external_workers:
            return None
        desired = external_workers.get("gb10_desired_states")
        return len(desired) if isinstance(desired, list) else 0

    def gb10_status_argv(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> Sequence[str]:
        return candidate_loom_argv(
            "admin",
            "gb10-workers",
            "status",
            "--cp-url",
            ctx.cp_url,
            "--environment",
            self._gb10_status_environment(ctx, step_dir),
            "--release-image-tag",
            ctx.image_tag,
            "--release-env-config-version",
            ctx.image_tag,
            "--format",
            "json",
        )

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        # Point release-gate at the rendered manifest from step 07 and
        # release manifest generated by this rollout step.
        rendered = step_dir.path.parent / "07-render" / "rendered.yaml"
        argv = candidate_loom_argv(
            "cluster",
            "release-gate",
            "--manifest",
            str(self.release_manifest_path(ctx, step_dir)),
            "--namespace",
            ctx.namespace,
            "--environment",
            ctx.environment,
            "--config",
            str(rollout_cluster_config(ctx, step_dir)),
            "--rendered-manifest",
            str(rendered),
            "--gb10-workers-status",
            str(self.gb10_status_path(ctx, step_dir)),
        )
        environment_state_check = self.environment_state_check_path(step_dir)
        if environment_state_check.exists():
            argv.extend(["--environment-state-check", str(environment_state_check)])
        return argv

    def _write_expected_image_identities(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> Path:
        rendered_path = step_dir.path.parent / "07-render" / "rendered.yaml"
        if not rendered_path.is_file():
            raise FileNotFoundError(
                f"rendered manifest not found at {rendered_path}; step 07 must "
                "succeed before step 12 can record image identities"
            )
        rendered_images = _rendered_deployment_images(
            rendered_path.read_text(encoding="utf-8"),
        )
        managed_images = {image_tag(image, ctx) for image, _ in ROLLOUT_IMAGES}
        rendered_managed_images = sorted({
            image
            for by_container in rendered_images.values()
            for image in by_container.values()
            if image in managed_images
        })
        if not rendered_managed_images:
            raise ValueError(
                "rendered manifest does not reference any release-managed "
                f"images for tag {ctx.image_tag}"
            )

        inspect = run_captured(
            ["docker", "image", "inspect", *rendered_managed_images],
            stderr_log=step_dir.artifact_path("image-identities.stderr"),
        )
        if inspect.returncode != 0:
            raise RuntimeError(
                inspect.stderr.strip()
                or f"docker image inspect exited {inspect.returncode}"
            )
        raw = json.loads(inspect.stdout)
        if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
            raise ValueError("docker image inspect returned unexpected JSON")
        identities = _image_identities_from_inspect(
            rendered_images=rendered_images,
            inspect_docs=raw,
            managed_images=managed_images,
        )
        output_path = self.expected_image_identities_path(ctx, step_dir)
        output_path.write_text(
            json.dumps(identities, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return output_path

    def cwd(self, ctx: RolloutContext, step_dir: StepDir) -> Path:
        return candidate_loom_cwd(step_dir)

    def env(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> dict[str, str]:
        return candidate_loom_env(step_dir)

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            cwd = self.cwd(ctx, step_dir)
            env = self.env(ctx, step_dir)
        except CandidateToolingError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        artifacts = {
            "expected_image_identities": str(
                self.expected_image_identities_path(ctx, step_dir),
            ),
            "release_manifest": str(self.release_manifest_path(ctx, step_dir)),
            "gb10_workers_status": str(self.gb10_status_path(ctx, step_dir)),
        }
        try:
            self._write_expected_image_identities(ctx, step_dir)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            return RunResult(
                exit_code=2,
                summary="expected image identity collection failed",
                error=str(exc),
                artifacts=artifacts,
            )

        manifest_cmd = list(self.release_manifest_argv(ctx, step_dir))
        manifest = run_captured(
            manifest_cmd,
            stdout_log=step_dir.artifact_path("release-manifest.stdout"),
            stderr_log=step_dir.artifact_path("release-manifest.stderr"),
            cwd=cwd,
            env=env,
        )
        if manifest.returncode != 0:
            return RunResult(
                exit_code=manifest.returncode,
                summary=f"release-manifest exited {manifest.returncode}",
                error=(
                    manifest.stderr.strip().splitlines()[-1]
                    if manifest.stderr.strip()
                    else f"release-manifest exited {manifest.returncode}"
                ),
                artifacts=artifacts,
            )
        gb10_desired_count = self._gb10_desired_state_count(ctx, step_dir)
        if ctx.scope == "current-gb10" and gb10_desired_count == 0:
            return RunResult(
                exit_code=2,
                summary="release manifest lacks GB10 desired state",
                error=(
                    "current-gb10 rollout requires env_state_profile with at "
                    "least one gb10_worker_pool_desired_states entry"
                ),
                artifacts=artifacts,
            )

        gb10_cmd = list(self.gb10_status_argv(ctx, step_dir))
        gate_cmd = list(self.argv(ctx, step_dir))
        gb10_retry_log = step_dir.artifact_path("gb10-workers-status.retries.log")
        last_gate: SubprocessResult | None = None
        for attempt in range(1, _GB10_STATUS_MAX_ATTEMPTS + 1):
            gb10 = run_captured(
                gb10_cmd,
                stdout_log=self.gb10_status_path(ctx, step_dir),
                stderr_log=step_dir.artifact_path("gb10-workers-status.stderr"),
                cwd=cwd,
                env=env,
                timeout_sec=60,
            )
            if gb10.returncode != 0:
                if _is_transient_cp_unreachable(gb10.stderr):
                    gb10_retry_log.open("a", encoding="utf-8").write(
                        f"attempt {attempt}/{_GB10_STATUS_MAX_ATTEMPTS}: "
                        f"{gb10.stderr.strip()}\n"
                    )
                    if attempt < _GB10_STATUS_MAX_ATTEMPTS:
                        time.sleep(_GB10_STATUS_RETRY_DELAY_SEC)
                        continue
                return RunResult(
                    exit_code=gb10.returncode,
                    summary=f"gb10-workers status exited {gb10.returncode}",
                    error=(
                        gb10.stderr.strip().splitlines()[-1]
                        if gb10.stderr.strip()
                        else f"gb10-workers status exited {gb10.returncode}"
                    ),
                    artifacts=artifacts,
                )

            gate = run_captured(
                gate_cmd,
                stdout_log=step_dir.stdout_path(),
                stderr_log=step_dir.stderr_path(),
                cwd=cwd,
                env=env,
            )
            last_gate = gate
            if gate.returncode == 0:
                return RunResult(
                    exit_code=0,
                    summary="release-manifest + GB10 status + release-gate exited 0",
                    artifacts=artifacts,
                )
            if not _is_gb10_convergence_failure(gate):
                return RunResult(
                    exit_code=gate.returncode,
                    summary=f"release-gate exited {gate.returncode}",
                    error=(
                        gate.stderr.strip().splitlines()[-1]
                        if gate.stderr.strip()
                        else f"release-gate exited {gate.returncode}"
                    ),
                    artifacts=artifacts,
                )
            gb10_retry_log.open("a", encoding="utf-8").write(
                f"attempt {attempt}/{_GB10_STATUS_MAX_ATTEMPTS}: "
                "release-gate still reports GB10 convergence drift\n"
            )
            if attempt < _GB10_STATUS_MAX_ATTEMPTS:
                time.sleep(_GB10_STATUS_RETRY_DELAY_SEC)
        assert last_gate is not None
        return RunResult(
            exit_code=last_gate.returncode,
            summary=f"release-gate exited {last_gate.returncode}",
            error=(
                last_gate.stderr.strip().splitlines()[-1]
                if last_gate.stderr.strip()
                else f"release-gate exited {last_gate.returncode}"
            ),
            artifacts=artifacts,
        )
        raise AssertionError("unreachable release-gate branch")
