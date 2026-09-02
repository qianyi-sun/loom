"""Candidate-bound exact image-plan tests for rollout step 02."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.image_readiness import (
    AUXILIARY_ROLLOUT_IMAGES,
    DEFAULT_ROLLOUT_IMAGE_PLAN,
    ROLLOUT_IMAGES,
    image_plan_digest,
)
from loom_cli.rollout.steps import s02_build_images
from loom_cli.rollout.steps.s02_build_images import (
    BuildImagesStep,
    rollout_all_image_bindings,
    rollout_auxiliary_images_from_worktree,
    rollout_images,
    rollout_images_from_candidate,
    rollout_images_from_worktree,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult

PRIMARY_PLAN = tuple((name, dockerfile, ".") for name, dockerfile in ROLLOUT_IMAGES)
AUXILIARY_PLAN = tuple((name, dockerfile, ".") for name, dockerfile in AUXILIARY_ROLLOUT_IMAGES)


def _repo_root() -> Path:
    path = Path(__file__).resolve()
    for parent in path.parents:
        if (parent / "deploy" / "k8s").is_dir():
            return parent
    raise RuntimeError(f"could not locate repo root from {path}")


def _locally_tagged_deployment_images() -> set[str]:
    root = _repo_root() / "deploy" / "k8s"
    names: set[str] = set()
    for path in sorted(root.glob("*.yaml")):
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not isinstance(document, dict) or document.get("kind") not in {
                "Deployment",
                "StatefulSet",
            }:
                continue
            pod_spec = ((document.get("spec") or {}).get("template") or {}).get("spec") or {}
            for container in pod_spec.get("containers") or []:
                image = container.get("image") if isinstance(container, dict) else None
                if not image:
                    continue
                name = str(image).split(":", 1)[0]
                if "/" not in name and name.startswith("loom-"):
                    names.add(name)
    return names


def _image_ids() -> dict[str, str]:
    return {
        name: f"sha256:{index:064x}"
        for index, (name, _dockerfile, _context) in enumerate(
            DEFAULT_ROLLOUT_IMAGE_PLAN,
            start=1,
        )
    }


def _artifact(*, digest: str = "f" * 64) -> SimpleNamespace:
    return SimpleNamespace(
        image_digests=_image_ids(),
        plan_digest=image_plan_digest(DEFAULT_ROLLOUT_IMAGE_PLAN),
        artifact_digest=digest,
    )


def _write_matrix(
    tmp_path: Path,
    *,
    ctx: SimpleNamespace,
    schema_version: object = 2,
    primary=PRIMARY_PLAN,
    auxiliary=AUXILIARY_PLAN,
    manifest_sha256: str = "e" * 64,
) -> Path:
    step_dir = StepDir(number=4, name="publish-images", path=tmp_path / "04-publish")
    matrix_path = tmp_path / "02-build-images" / "image-matrix.json"
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    envelope = s02_build_images._matrix_envelope(
        ctx,  # type: ignore[arg-type]
        primary=primary,
        auxiliary=auxiliary,
        manifest_sha256=manifest_sha256,
        artifact=_artifact(),  # type: ignore[arg-type]
    )
    envelope["schema_version"] = schema_version
    matrix_path.write_text(json.dumps(envelope), encoding="utf-8")
    return step_dir.path


class TestBuildImagesCoverage:
    def test_primary_images_exactly_cover_managed_workloads(self) -> None:
        rendered = _locally_tagged_deployment_images()
        primary = {name for name, _, _ in rollout_images_from_worktree(_repo_root())}

        # The execution runtime is consumed by short-lived Jobs. The actuator
        # is a primary release image whose Nebius Deployment is deliberately
        # outside the provider-neutral default cluster render.
        assert primary == rendered | {
            "loom-execution-actuator",
            "loom-execution-runtime",
        }
        assert len(primary) == 10

    def test_candidate_roles_form_exact_ten_image_contract(self) -> None:
        primary = rollout_images_from_worktree(_repo_root())
        auxiliary = rollout_auxiliary_images_from_worktree(_repo_root())

        assert set(primary) == set(PRIMARY_PLAN)
        assert set(auxiliary) == set(AUXILIARY_PLAN)
        assert not set(primary) & set(auxiliary)
        assert s02_build_images._canonical_exact_plans(primary, auxiliary) == (
            PRIMARY_PLAN,
            AUXILIARY_PLAN,
        )
        assert PRIMARY_PLAN + AUXILIARY_PLAN == DEFAULT_ROLLOUT_IMAGE_PLAN

    @pytest.mark.parametrize(
        "image,dockerfile",
        list(ROLLOUT_IMAGES + AUXILIARY_ROLLOUT_IMAGES),
    )
    def test_every_exact_image_has_a_dockerfile(
        self,
        image: str,
        dockerfile: str,
    ) -> None:
        assert (_repo_root() / dockerfile).is_file(), image

    def test_browser_acceptance_image_is_content_addressed_and_revision_bound(
        self,
    ) -> None:
        dockerfile = (_repo_root() / "deploy/Dockerfile.staging-admin-browser-smoke").read_text(
            encoding="utf-8"
        )

        assert (
            "mcr.microsoft.com/playwright:v1.61.1-noble@sha256:"
            "5b8f294aff9041b7191c34a4bab3ac270157a28774d4b0660e9743297b697e48" in dockerfile
        )
        assert "ARG LOOM_BUILD_SHA" in dockerfile
        assert 'org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in dockerfile
        assert "npm ci --prefix web --ignore-scripts" in dockerfile
        assert "COPY --chmod=0444 web/scripts/staging-admin-browser-smoke.mjs" in dockerfile
        assert (
            'ENTRYPOINT ["node", "/opt/loom/web/scripts/'
            'staging-admin-browser-smoke.mjs"]' in dockerfile
        )

    def test_rehearsal_postgres_image_is_content_addressed_and_revision_bound(
        self,
    ) -> None:
        dockerfile = (_repo_root() / "deploy/Dockerfile.rehearsal-postgres").read_text(
            encoding="utf-8"
        )

        assert (
            "postgres:17.4@sha256:"
            "304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad" in dockerfile
        )
        assert "ARG LOOM_BUILD_SHA" in dockerfile
        assert 'org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in dockerfile


def test_rollout_image_query_is_candidate_worktree_and_role_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[list[str], object]] = []
    payload = [
        {
            "image_name": "loom-service",
            "dockerfile": "deploy/Dockerfile.service",
            "context": ".",
        }
    ]

    def fake_run(argv: list[str], **kwargs: object) -> SubprocessResult:
        observed.append((argv, kwargs.get("cwd")))
        return SubprocessResult(argv=argv, returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(s02_build_images, "run_captured", fake_run)

    assert rollout_images_from_worktree(tmp_path) == (
        ("loom-service", "deploy/Dockerfile.service", "."),
    )
    assert observed == [
        (
            [
                "python3",
                "scripts/component_ownership.py",
                "release-images",
                "--rollout-role",
                "primary",
            ],
            tmp_path,
        )
    ]


def test_rollout_image_query_uses_commit_bound_script_manifest_and_roles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worktree = tmp_path / "candidate"
    worktree.mkdir()
    seen_paths: list[Path] = []
    seen_roles: list[str] = []

    def fake_materialize(_ctx, repo_path: Path, target: Path):
        data = b"script" if repo_path.parts[0] == "scripts" else b"manifest"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        seen_paths.append(repo_path)
        return SimpleNamespace(data=data, evidence_path=target)

    def fake_run(argv: list[str], **kwargs: object) -> SubprocessResult:
        assert Path(argv[1]).parent != worktree / "scripts"
        assert argv[2:5] == ["--repo-root", str(worktree), "--manifest"]
        assert kwargs["cwd"] == worktree
        role = argv[-1]
        seen_roles.append(role)
        payload = PRIMARY_PLAN if role == "primary" else AUXILIARY_PLAN
        rows = s02_build_images._matrix_payload(payload)
        return SubprocessResult(argv=argv, returncode=0, stdout=json.dumps(rows), stderr="")

    monkeypatch.setattr(
        s02_build_images, "validate_candidate_worktree_identity", lambda _ctx: worktree
    )
    monkeypatch.setattr(s02_build_images, "materialize_candidate_blob", fake_materialize)
    monkeypatch.setattr(s02_build_images, "run_captured", fake_run)

    assert rollout_images_from_candidate(SimpleNamespace(resolved_sha="a" * 40)) == PRIMARY_PLAN  # type: ignore[arg-type]
    assert seen_paths == [
        Path("scripts/component_ownership.py"),
        Path("config/component-ownership.toml"),
    ]
    assert seen_roles == ["primary", "auxiliary"]


def test_candidate_role_drift_fails_closed() -> None:
    sandbox = ("loom-agent-sandbox", "deploy/Dockerfile.agent-sandbox", ".")

    with pytest.raises(RuntimeError, match="exact eight-image"):
        s02_build_images._canonical_exact_plans(
            (*PRIMARY_PLAN[:-1], sandbox),
            AUXILIARY_PLAN,
        )


def test_build_inputs_fingerprint_binds_manifest_roles_and_exact_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        s02_build_images,
        "_rollout_matrix_from_candidate",
        lambda _ctx: (PRIMARY_PLAN, AUXILIARY_PLAN, "f" * 64),
    )

    fingerprint = BuildImagesStep()._inputs_fingerprint(
        SimpleNamespace(image_tag="candidate", resolved_sha="a" * 40),  # type: ignore[arg-type]
    )

    assert fingerprint["component_manifest_sha256"] == "f" * 64
    assert fingerprint["matrix_sha256"] == s02_build_images._role_matrix_digest(
        PRIMARY_PLAN,
        AUXILIARY_PLAN,
    )
    assert fingerprint["image_plan_sha256"] == image_plan_digest(DEFAULT_ROLLOUT_IMAGE_PLAN)


def test_persisted_v2_matrix_returns_only_primary_images(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = SimpleNamespace(resolved_sha="a" * 40)
    step_path = _write_matrix(tmp_path, ctx=ctx)
    step_dir = StepDir(number=4, name="publish-images", path=step_path)
    monkeypatch.setattr(
        s02_build_images,
        "_rollout_matrix_from_candidate",
        lambda _ctx: (PRIMARY_PLAN, AUXILIARY_PLAN, "e" * 64),
    )

    assert rollout_images(ctx, step_dir) == PRIMARY_PLAN  # type: ignore[arg-type]


def test_persisted_v2_matrix_exposes_exact_union_for_registry_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = SimpleNamespace(resolved_sha="a" * 40)
    step_path = _write_matrix(tmp_path, ctx=ctx)
    step_dir = StepDir(number=7, name="render", path=step_path)
    monkeypatch.setattr(
        s02_build_images,
        "_rollout_matrix_from_candidate",
        lambda _ctx: (PRIMARY_PLAN, AUXILIARY_PLAN, "e" * 64),
    )

    plan, image_ids = rollout_all_image_bindings(ctx, step_dir)  # type: ignore[arg-type]

    assert plan == DEFAULT_ROLLOUT_IMAGE_PLAN
    assert set(image_ids) == {name for name, _dockerfile, _context in plan}


def test_persisted_v2_matrix_must_match_candidate_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = SimpleNamespace(resolved_sha="a" * 40)
    step_path = _write_matrix(tmp_path, ctx=ctx)
    step_dir = StepDir(number=4, name="publish-images", path=step_path)
    monkeypatch.setattr(
        s02_build_images,
        "_rollout_matrix_from_candidate",
        lambda _ctx: (tuple(reversed(PRIMARY_PLAN)), AUXILIARY_PLAN, "e" * 64),
    )

    with pytest.raises(RuntimeError, match="differs from candidate manifest"):
        rollout_images(ctx, step_dir)  # type: ignore[arg-type]


def test_persisted_matrix_rejects_boolean_schema_version(
    tmp_path: Path,
) -> None:
    ctx = SimpleNamespace(resolved_sha="a" * 40)
    step_path = _write_matrix(tmp_path, ctx=ctx, schema_version=True)
    step_dir = StepDir(number=4, name="publish-images", path=step_path)

    with pytest.raises(RuntimeError, match="invalid candidate binding"):
        rollout_images(ctx, step_dir)  # type: ignore[arg-type]


def test_build_done_revalidation_rejects_missing_matrix_artifact(tmp_path: Path) -> None:
    step_dir = StepDir(number=2, name="build-images", path=tmp_path / "02-build-images")

    outcome = BuildImagesStep().verify_done(
        SimpleNamespace(resolved_sha="a" * 40),  # type: ignore[arg-type]
        step_dir,
    )

    assert outcome is s02_build_images.VerifyOutcome.MISMATCH


def test_build_done_artifact_contract_rejects_missing_or_wrong_path(
    tmp_path: Path,
) -> None:
    step_dir = StepDir(number=2, name="build-images", path=tmp_path / "02-build-images")
    step_dir.path.mkdir()
    wrong = step_dir.artifact_path("wrong-matrix.json")
    wrong.write_text("{}", encoding="utf-8")
    step = BuildImagesStep()
    ctx = SimpleNamespace(resolved_sha="a" * 40)

    assert not step.validate_done_artifacts(ctx, step_dir, {})  # type: ignore[arg-type]
    assert not step.validate_done_artifacts(  # type: ignore[arg-type]
        ctx,
        step_dir,
        {"image_matrix": str(wrong)},
    )


def test_legacy_candidate_without_rollout_role_query_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worktree = tmp_path / "candidate"
    worktree.mkdir()

    def fake_materialize(_ctx, _repo_path: Path, target: Path):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("legacy", encoding="utf-8")
        return SimpleNamespace(data=b"legacy", evidence_path=target)

    monkeypatch.setattr(
        s02_build_images, "validate_candidate_worktree_identity", lambda _ctx: worktree
    )
    monkeypatch.setattr(s02_build_images, "materialize_candidate_blob", fake_materialize)
    monkeypatch.setattr(
        s02_build_images,
        "run_captured",
        lambda argv, **_kwargs: SubprocessResult(
            argv=argv,
            returncode=2,
            stdout="",
            stderr="unrecognized arguments: --rollout-role",
        ),
    )

    with pytest.raises(RuntimeError, match="unrecognized arguments"):
        rollout_images_from_candidate(SimpleNamespace(resolved_sha="a" * 40))  # type: ignore[arg-type]


def test_run_builds_exact_candidate_plan_and_persists_v2_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="c" * 40)
    rollout_dir = tmp_path / "rollout"
    worktree = rollout_dir / "01-worktree" / "src"
    worktree.mkdir(parents=True)
    step_path = rollout_dir / "02-build-images"
    step_path.mkdir()
    step_dir = StepDir(number=2, name="build-images", path=step_path)
    calls: list[dict[str, object]] = []

    def fake_build(_run, **kwargs):
        calls.append(kwargs)
        return _artifact()

    monkeypatch.setattr(
        s02_build_images,
        "_rollout_matrix_from_candidate",
        lambda _ctx: (PRIMARY_PLAN, AUXILIARY_PLAN, "e" * 64),
    )
    monkeypatch.setattr(
        s02_build_images, "validate_candidate_worktree_identity", lambda _ctx: worktree
    )
    monkeypatch.setattr(s02_build_images, "build_exact_images", fake_build)

    result = BuildImagesStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert calls == [
        {
            "plan": DEFAULT_ROLLOUT_IMAGE_PLAN,
            "candidate_root": worktree,
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
        }
    ]
    envelope = json.loads(step_dir.artifact_path("image-matrix.json").read_text())
    assert envelope["schema_version"] == 2
    assert envelope["image_ids"] == _image_ids()
    assert len(envelope["primary_images"]) == 10
    assert len(envelope["auxiliary_images"]) == 2


def test_verify_rejects_immutable_image_id_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="c" * 40)
    step_dir = StepDir(number=2, name="build-images", path=tmp_path)
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        s02_build_images,
        "_persisted_rollout_matrix",
        lambda _ctx, _step_dir: (PRIMARY_PLAN, AUXILIARY_PLAN, _image_ids(), "f" * 64),
    )

    def fake_verify(_run, **kwargs):
        calls.append(kwargs)
        raise ValueError("rollout image contract drifted for loom-service")

    monkeypatch.setattr(s02_build_images, "verify_image_contract", fake_verify)

    assert BuildImagesStep().verify(ctx, step_dir) is s02_build_images.VerifyOutcome.MISMATCH
    assert calls == [
        {
            "plan": DEFAULT_ROLLOUT_IMAGE_PLAN,
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
            "expected_digests": _image_ids(),
        }
    ]


def test_run_reports_exact_image_contract_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="c" * 40)
    rollout_dir = tmp_path / "rollout"
    worktree = rollout_dir / "01-worktree" / "src"
    worktree.mkdir(parents=True)
    step_path = rollout_dir / "02-build-images"
    step_path.mkdir()
    step_dir = StepDir(number=2, name="build-images", path=step_path)

    monkeypatch.setattr(
        s02_build_images,
        "_rollout_matrix_from_candidate",
        lambda _ctx: (PRIMARY_PLAN, AUXILIARY_PLAN, "e" * 64),
    )
    monkeypatch.setattr(
        s02_build_images, "validate_candidate_worktree_identity", lambda _ctx: worktree
    )
    monkeypatch.setattr(
        s02_build_images,
        "build_exact_images",
        lambda _run, **_kwargs: (_ for _ in ()).throw(
            ValueError("rollout image contract failed for loom-service")
        ),
    )

    result = BuildImagesStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error == "rollout image contract failed for loom-service"
