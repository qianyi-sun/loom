from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_EDITABLE_ROOT_IMAGES = (
    "Dockerfile.capacity-manager",
    "Dockerfile.control-plane",
    "Dockerfile.family-orchestrator",
    "Dockerfile.gateway",
    "Dockerfile.personal-dev-activation-agent",
    "Dockerfile.pipeline-orchestrator",
    "Dockerfile.service",
    "Dockerfile.worker",
)


def test_editable_root_images_install_neutral_bundle_checksum_first() -> None:
    """A clean image build must satisfy Loom's local checksum dependency."""
    for name in _EDITABLE_ROOT_IMAGES:
        dockerfile = ROOT / "deploy" / name
        text = dockerfile.read_text()
        assert (
            "COPY packages ./packages" in text
            or "COPY packages/loom-bundle-checksum ./packages/loom-bundle-checksum" in text
        ), dockerfile
        checksum_install = "pip install --no-cache-dir -e ./packages/loom-bundle-checksum"
        assert checksum_install in text, dockerfile
        install_lines = [
            line.strip().removeprefix("RUN ")
            for line in text.splitlines()
            if "pip install" in line
        ]
        checksum_index = next(
            index for index, line in enumerate(install_lines) if line.startswith(checksum_install)
        )
        root_index = next(
            index
            for index, line in enumerate(install_lines)
            if line in {"pip install --no-cache-dir -e .", "pip install --no-cache-dir -e . && \\"}
        )
        assert checksum_index < root_index, dockerfile


def test_db_facing_images_include_migrations_for_schema_startup() -> None:
    dockerfiles = [
        ROOT / "deploy" / "Dockerfile.control-plane",
        ROOT / "deploy" / "Dockerfile.gateway",
        ROOT / "deploy" / "Dockerfile.service",
    ]

    for dockerfile in dockerfiles:
        text = dockerfile.read_text()
        assert "COPY migrations ./migrations" in text, dockerfile


def test_service_image_exposes_immutable_build_revision_to_runtime() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "ARG LOOM_BUILD_SHA=unknown" in text
    assert 'LABEL org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in text
    assert 'ENV LOOM_BUILD_SHA="${LOOM_BUILD_SHA}"' not in text
    assert "printf '%s\\n' \"${LOOM_BUILD_SHA}\" > /opt/loom/build-sha" in text
    assert "chmod 0444 /opt/loom/build-sha" in text
    assert text.index("pip install --no-cache-dir -e .") < text.index("ARG LOOM_BUILD_SHA=unknown")


def test_control_plane_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.control-plane").read_text()

    assert "chmod -R a+rX ./src ./migrations" in text


def test_gateway_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.gateway").read_text()

    assert "chmod -R a+rX ./src ./migrations" in text


def test_service_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert (
        "chmod -R a+rX ./src ./packages ./migrations ./capacity_guard_migrations"
        in text
    )


def test_service_image_contains_capacity_guard_migrations() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "COPY capacity_guard_migrations ./capacity_guard_migrations" in text


def test_service_image_contains_digest_pinned_kubectl_for_personal_lifecycle() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "registry.k8s.io/kubectl:v1.36.2@sha256:" in text
    assert "COPY --from=kubectl /bin/kubectl /usr/local/bin/kubectl" in text
