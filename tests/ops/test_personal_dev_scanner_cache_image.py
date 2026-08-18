from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from scripts import component_ownership

_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _ROOT / "deploy/Dockerfile.personal-dev-scanner-cache"
_SERVICE_DOCKERFILE = _ROOT / "deploy/Dockerfile.service"


def _workflow() -> dict[str, Any]:
    return yaml.safe_load((_ROOT / ".github/workflows/images.yml").read_text(encoding="utf-8"))


def test_scanner_cache_image_is_minimal_nonroot_and_immutable() -> None:
    lines = _DOCKERFILE.read_text(encoding="utf-8").splitlines()

    assert lines[0] == (
        "FROM python:3.11-slim@sha256:"
        "9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7"
    )
    assert "RUN " not in "\n".join(lines)
    assert not any(command in "\n".join(lines).lower() for command in ("curl", "wget", "apt", "pip"))
    assert lines.count(
        "COPY src/loom/personal_dev_scanner_cache.py ./loom/personal_dev_scanner_cache.py"
    ) == 1
    assert lines.count(
        "COPY src/loom/personal_dev_scanner_cache_init.py "
        "./loom/personal_dev_scanner_cache_init.py"
    ) == 1
    assert {
        line
        for line in lines
        if line.startswith("COPY --from=personal-dev-scanner-cache ")
    } == {
        "COPY --from=personal-dev-scanner-cache --chown=65531:65532 --chmod=0444 "
        "db/metadata.json /opt/loom-personal-dev-scanner-cache/assets/db/metadata.json",
        "COPY --from=personal-dev-scanner-cache --chown=65531:65532 --chmod=0444 "
        "db/trivy.db /opt/loom-personal-dev-scanner-cache/assets/db/trivy.db",
        "COPY --from=personal-dev-scanner-cache --chown=65531:65532 --chmod=0444 "
        "java-db/metadata.json /opt/loom-personal-dev-scanner-cache/assets/java-db/metadata.json",
        "COPY --from=personal-dev-scanner-cache --chown=65531:65532 --chmod=0444 "
        "java-db/trivy-java.db /opt/loom-personal-dev-scanner-cache/assets/java-db/trivy-java.db",
    }
    assert "WORKDIR /opt/loom-personal-dev-scanner-cache/assets/db" in lines
    assert "WORKDIR /opt/loom-personal-dev-scanner-cache/assets/java-db" in lines
    assert "USER 65531:65532" in lines
    assert "ENV PYTHONPATH=/opt/loom-personal-dev-scanner-cache" in lines
    assert lines[-1] == (
        'ENTRYPOINT ["python", "-m", "loom.personal_dev_scanner_cache_init"]'
    )


def test_service_image_uses_the_same_pinned_trivy_version_as_scanner_policy() -> None:
    assert _SERVICE_DOCKERFILE.read_text(encoding="utf-8").splitlines()[0] == (
        "FROM aquasec/trivy@sha256:"
        "be1190afcb28352bfddc4ddeb71470835d16462af68d310f9f4bca710961a41e AS trivy"
    )


def test_scanner_cache_image_has_one_exact_component_owner() -> None:
    manifest = component_ownership.load_manifest(_ROOT / "config/component-ownership.toml")
    matches = [item for item in manifest.components if item.id == "personal-dev-scanner-cache"]

    assert len(matches) == 1
    component = matches[0]
    assert component.kind == "release-image"
    assert component.dockerfile == "deploy/Dockerfile.personal-dev-scanner-cache"
    assert component.build_context == "."
    assert component.release_digest == "loom-personal-dev-scanner-cache"
    assert component.runtime_policy == "conformance"
    assert component.rollout_role == "none"
    assert set(component.source_paths) == {
        ".dockerignore",
        "deploy/Dockerfile.personal-dev-scanner-cache",
        "deploy/dev-fleet/personal-dev-scanner-cache-lock.json",
        "scripts/prepare_personal_dev_scanner_cache_assets.py",
        "src/loom/__init__.py",
        "src/loom/personal_dev_scanner_cache.py",
        "src/loom/personal_dev_scanner_cache_init.py",
    }


def test_workflow_prepares_one_release_bound_asset_and_routes_it_only_to_cache_builds() -> None:
    jobs = _workflow()["jobs"]
    assets = jobs["personal-dev-scanner-cache-assets"]
    build = jobs["build"]
    publish = jobs["publish"]

    assert assets["needs"] == ["plan", "trivy-binary"]
    assert assets["permissions"] == {"actions": "read", "contents": "read"}
    assert assets["runs-on"] == "ubuntu-24.04"
    assert "strategy" not in assets
    scripts = "\n".join(step.get("run", "") for step in assets["steps"])
    assert "scripts/prepare_personal_dev_scanner_cache_assets.py" in scripts
    assert "--lock deploy/dev-fleet/personal-dev-scanner-cache-lock.json" in scripts
    assert "--trivy /tmp/loom-trivy-binaries/amd64/trivy" in scripts
    assert "--output /tmp/loom-personal-dev-scanner-cache" in scripts
    upload = next(
        step for step in assets["steps"] if step.get("name") == "Upload exact scanner-cache assets"
    )
    assert upload["with"] == {
        "name": "personal-dev-scanner-cache-assets-run-${{ github.run_id }}-attempt-${{ github.run_attempt }}",
        "path": "/tmp/loom-personal-dev-scanner-cache",
        "if-no-files-found": "error",
        "retention-days": 1,
    }
    assert set(build["needs"]) == {
        "image-route",
        "personal-dev-scanner-cache-assets",
        "plan",
        "trivy-binary",
    }
    assert set(publish["needs"]) == {
        "personal-dev-scanner-cache-assets",
        "plan",
        "trivy-binary",
    }
    assert "matrix.image == 'personal-dev-scanner-cache'" in build["runs-on"]
    for job in (build, publish):
        download = next(
            step
            for step in job["steps"]
            if step.get("name") == "Download exact scanner-cache assets"
        )
        assert download["if"] == "matrix.image == 'personal-dev-scanner-cache'"
        assert download["with"] == {
            "name": "personal-dev-scanner-cache-assets-run-${{ github.run_id }}-attempt-${{ github.run_attempt }}",
            "path": "/tmp/loom-personal-dev-scanner-cache",
        }
    candidate_script = next(
        step["run"]
        for step in build["steps"]
        if step.get("name") == "Build without registry or cache write authority"
    )
    publish_script = next(
        step["run"]
        for step in publish["steps"]
        if step.get("name") == "Build trusted image archive"
    )
    for script in (candidate_script, publish_script):
        assert 'if [[ "$IMAGE_NAME" == "personal-dev-scanner-cache" ]]; then' in script
        assert "--build-context" in script
        assert (
            '"personal-dev-scanner-cache=/tmp/loom-personal-dev-scanner-cache"' in script
        )
        assert "verify_scanner_cache_assets" in script
