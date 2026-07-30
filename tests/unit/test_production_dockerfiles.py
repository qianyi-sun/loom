from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


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


def test_worker_image_exposes_immutable_build_revision_for_runtime_binding() -> None:
    text = (ROOT / "deploy" / "Dockerfile.worker").read_text()

    assert "ARG LOOM_BUILD_SHA=unknown" in text
    assert 'LABEL org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in text


def test_control_plane_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.control-plane").read_text()

    assert "chmod -R a+rX ./src ./migrations" in text


def test_gateway_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.gateway").read_text()

    assert "chmod -R a+rX ./src ./migrations" in text


def test_service_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "chmod -R a+rX ./src ./packages ./migrations" in text
