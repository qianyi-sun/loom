from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom.personal_dev_builder_manifest import PersonalDevBuilderManifestConfig
from loom.personal_dev_builder_runtime import (
    KubectlPersonalDevBuildExecutor,
    PersonalDevBuildCapability,
    S3PersonalDevBuildCapabilityProvider,
    personal_dev_build_artifact_key,
)
from tests.unit.test_personal_dev_builder import _publication, _registration


class _Cluster:
    def __init__(self) -> None:
        self.applied: list[str] = []
        self.waited: list[tuple[str, str]] = []
        self.deleted: list[str] = []

    async def apply(self, manifest: str, **_kwargs) -> None:
        self.applied.append(manifest)

    async def wait_job(self, namespace: str, name: str) -> None:
        self.waited.append((namespace, name))

    async def delete_namespace(self, namespace: str) -> None:
        self.deleted.append(namespace)


class _Capabilities:
    async def issue(self, registration, *, platform):
        assert registration.build_attempt is not None
        architecture = platform.rsplit("/", 1)[1]
        return PersonalDevBuildCapability(
            source_get_url=f"https://minio.example/source?arch={architecture}",
            artifact_upload_url="https://minio.example/artifacts",
            artifact_upload_fields={
                "key": f"output/{architecture}",
                "policy": "bounded",
            },
            artifact_max_bytes=1024,
            expires_at=datetime.now(UTC) + timedelta(hours=2),
        )


class _Exporter:
    async def publish(self, registration):
        return _publication(registration.candidate)


async def test_kubectl_builder_uses_native_ephemeral_jobs_and_secret_stdin(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.tar"
    source.write_bytes(b"sealed-source")
    registration = _registration()
    registration = replace(
        registration,
        candidate=replace(
            registration.candidate,
            archive_sha256=hashlib.sha256(b"sealed-source").hexdigest(),
            archive_size_bytes=len(b"sealed-source"),
        ),
    )
    cluster = _Cluster()
    executor = KubectlPersonalDevBuildExecutor(
        cluster=cluster,  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    publication = await executor.build(registration, source_archive=source)

    assert publication == _publication(registration.candidate)
    assert {name for _namespace, name in cluster.waited} == {"build-amd64", "build-arm64"}
    secret_documents = [manifest for manifest in cluster.applied if "kind: Secret" in manifest]
    assert len(secret_documents) == 2
    assert any("source?arch=amd64" in manifest for manifest in secret_documents)
    assert any("output/amd64" in manifest for manifest in secret_documents)
    assert any("source?arch=arm64" in manifest for manifest in secret_documents)
    assert any("output/arm64" in manifest for manifest in secret_documents)
    nonsecret = "\n".join(
        manifest for manifest in cluster.applied if "kind: Secret" not in manifest
    )
    assert "minio.example" not in nonsecret
    assert "automountServiceAccountToken: false" in nonsecret
    assert "hostUsers:" not in nonsecret
    assert "nodeSelector:" not in nonsecret
    assert "runtimeClassName: loom-personal-dev-builder" in nonsecret

    await executor.cleanup(registration)
    assert cluster.deleted == [
        f"loom-build-{registration.build_attempt.id.hex}-"
        f"l{registration.build_attempt.lease_epoch:016x}"
    ]


async def test_stale_builder_cleanup_cannot_name_replacement_lease_namespace() -> None:
    registration = _registration()
    replacement = replace(
        registration,
        build_attempt=replace(
            registration.build_attempt,
            lease_epoch=registration.build_attempt.lease_epoch + 1,
        ),
    )
    cluster = _Cluster()
    executor = KubectlPersonalDevBuildExecutor(
        cluster=cluster,  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    await executor.cleanup(registration)
    await executor.cleanup(replacement)

    assert cluster.deleted[0] != cluster.deleted[1]


def test_s3_capability_provider_signs_only_exact_source_and_platform_output() -> None:
    class _S3:
        def __init__(self) -> None:
            self.url_calls: list[tuple[str, dict[str, object], int]] = []
            self.post_calls: list[dict[str, object]] = []

        def generate_presigned_url(self, operation, **kwargs):
            self.url_calls.append((operation, kwargs["Params"], kwargs["ExpiresIn"]))
            return f"https://minio.example/{operation}?signature=bounded"

        def generate_presigned_post(self, **kwargs):
            self.post_calls.append(kwargs)
            return {
                "url": "https://minio.example/artifacts",
                "fields": {**kwargs["Fields"], "key": kwargs["Key"], "policy": "bounded"},
            }

    registration = _registration()
    client = _S3()
    provider = S3PersonalDevBuildCapabilityProvider(
        object_store=client,  # type: ignore[arg-type]
        expected_bucket="artifacts",
        expiry_seconds=4200,
        max_artifact_bytes=8 * 1024 * 1024,
    )

    capability = provider.issue_sync(registration, platform="linux/amd64", now=datetime.now(UTC))

    assert capability.source_get_url.startswith("https://minio.example/get_object")
    assert capability.artifact_upload_url == "https://minio.example/artifacts"
    assert capability.artifact_max_bytes == 8 * 1024 * 1024
    assert [call[0] for call in client.url_calls] == ["get_object"]
    source_params = client.url_calls[0][1]
    output_params = client.post_calls[0]
    assert source_params == {
        "Bucket": registration.candidate.object_bucket,
        "Key": registration.candidate.object_key,
    }
    assert output_params["Key"] == personal_dev_build_artifact_key(
        registration,
        platform="linux/amd64",
    )
    assert output_params["Fields"]["x-amz-meta-build-lease-epoch"] == str(
        registration.build_attempt.lease_epoch
    )
    assert ["content-length-range", 1, 8 * 1024 * 1024] in output_params["Conditions"]
    assert capability.artifact_upload_fields["policy"] == "bounded"
