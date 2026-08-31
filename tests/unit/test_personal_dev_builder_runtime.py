from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom.personal_dev_builder_manifest import PersonalDevBuilderManifestConfig
from loom.personal_dev_builder_runtime import (
    KubectlPersonalDevBuildExecutor,
    KubectlPersonalDevPlatformBuildExecutor,
    PersonalDevBuildCapability,
    S3PersonalDevBuildCapabilityProvider,
    personal_dev_build_artifact_key,
)
from tests.unit.test_personal_dev_builder import _publication, _registration


class _Cluster:
    def __init__(self) -> None:
        self.applied: list[str] = []
        self.waited: list[tuple[str, str]] = []
        self.inspected: list[tuple[str, str]] = []
        self.deleted: list[str] = []
        self.job_pods: dict[str, dict[str, object]] = {
            name: {
                "items": [
                    {
                        "metadata": {"labels": {"job-name": name}},
                        "status": {
                            "phase": "Succeeded",
                            "containerStatuses": [
                                {
                                    "name": "builder",
                                    "restartCount": 0,
                                    "state": {"terminated": {"exitCode": 0}},
                                }
                            ],
                            "initContainerStatuses": [
                                {"name": "buildkitd", "restartCount": 0}
                            ],
                        },
                    }
                ]
            }
            for name in ("build-amd64", "build-arm64")
        }

    async def apply(self, manifest: str, **_kwargs) -> None:
        self.applied.append(manifest)

    async def wait_job(self, namespace: str, name: str) -> None:
        self.waited.append((namespace, name))

    async def wait_job_failure(self, namespace: str, name: str) -> None:
        await asyncio.Event().wait()

    async def list_job_pods(self, namespace: str, name: str) -> dict[str, object]:
        self.inspected.append((namespace, name))
        return self.job_pods[name]

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


async def test_kubectl_platform_executor_builds_only_requested_native_platform(
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
    executor = KubectlPersonalDevPlatformBuildExecutor(
        cluster=cluster,  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
        platform="linux/amd64",
    )

    await executor.build_platform(registration, source_archive=source)

    applied = "\n".join(cluster.applied)
    assert "build-amd64" in applied
    assert "build-arm64" not in applied
    assert cluster.waited == [
        (
            f"loom-build-{registration.build_attempt.id.hex}-"
            f"l{registration.build_attempt.lease_epoch:016x}",
            "build-amd64",
        )
    ]
    assert cluster.inspected == cluster.waited

    await executor.cleanup_platform(registration)
    assert cluster.deleted == [
        f"loom-build-{registration.build_attempt.id.hex}-"
        f"l{registration.build_attempt.lease_epoch:016x}"
    ]


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
    assert {name for _namespace, name in cluster.inspected} == {
        "build-amd64",
        "build-arm64",
    }
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


async def test_legacy_kubectl_builder_serializes_shared_resource_application(
    tmp_path: Path,
) -> None:
    class _SerialApplyCluster(_Cluster):
        def __init__(self) -> None:
            super().__init__()
            self.applying = False
            self.concurrent_apply = False

        async def apply(self, manifest: str, **kwargs) -> None:
            if self.applying:
                self.concurrent_apply = True
            self.applying = True
            try:
                await asyncio.sleep(0.001)
                await super().apply(manifest, **kwargs)
            finally:
                self.applying = False

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
    cluster = _SerialApplyCluster()
    executor = KubectlPersonalDevBuildExecutor(
        cluster=cluster,  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    await executor.build(registration, source_archive=source)

    assert cluster.concurrent_apply is False


@pytest.mark.parametrize(
    ("status_field", "container_name"),
    [
        ("containerStatuses", "builder"),
        ("initContainerStatuses", "buildkitd"),
    ],
)
async def test_kubectl_builder_rejects_any_container_restart_before_publication(
    tmp_path: Path,
    status_field: str,
    container_name: str,
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
    pod = cluster.job_pods["build-amd64"]["items"][0]
    status = pod["status"]
    entry = next(
        value for value in status[status_field] if value["name"] == container_name
    )
    entry["restartCount"] = 1
    executor = KubectlPersonalDevBuildExecutor(
        cluster=cluster,  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    with pytest.raises(RuntimeError, match="runtime integrity"):
        await executor.build(registration, source_archive=source)


async def test_kubectl_builder_inspects_each_job_as_soon_as_it_completes(
    tmp_path: Path,
) -> None:
    class _CompletionSensitiveCluster(_Cluster):
        def __init__(self) -> None:
            super().__init__()
            self.amd64_inspected = asyncio.Event()

        async def wait_job(self, namespace: str, name: str) -> None:
            await super().wait_job(namespace, name)
            if name == "build-arm64":
                await self.amd64_inspected.wait()

        async def list_job_pods(self, namespace: str, name: str) -> dict[str, object]:
            observation = await super().list_job_pods(namespace, name)
            if name == "build-amd64":
                self.amd64_inspected.set()
            return observation

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
    cluster = _CompletionSensitiveCluster()
    executor = KubectlPersonalDevBuildExecutor(
        cluster=cluster,  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    publication = await asyncio.wait_for(
        executor.build(registration, source_archive=source),
        timeout=0.5,
    )

    assert publication == _publication(registration.candidate)


async def test_kubectl_builder_stops_as_soon_as_a_job_reports_failure(
    tmp_path: Path,
) -> None:
    class _FailedCluster(_Cluster):
        async def wait_job(self, namespace: str, name: str) -> None:
            await super().wait_job(namespace, name)
            await asyncio.Event().wait()

        async def wait_job_failure(self, namespace: str, name: str) -> None:
            if name == "build-amd64":
                return
            await asyncio.Event().wait()

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
    executor = KubectlPersonalDevBuildExecutor(
        cluster=_FailedCluster(),  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    with pytest.raises(RuntimeError, match="reported failure"):
        await asyncio.wait_for(
            executor.build(registration, source_archive=source),
            timeout=0.5,
        )


async def test_kubectl_builder_failure_wins_when_both_job_conditions_are_observed(
    tmp_path: Path,
) -> None:
    class _ContradictoryCluster(_Cluster):
        async def wait_job_failure(self, namespace: str, name: str) -> None:
            return

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
    executor = KubectlPersonalDevBuildExecutor(
        cluster=_ContradictoryCluster(),  # type: ignore[arg-type]
        capabilities=_Capabilities(),  # type: ignore[arg-type]
        exporter=_Exporter(),  # type: ignore[arg-type]
        manifest_config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/loom-builder@sha256:" + "a" * 64,
        ),
    )

    with pytest.raises(RuntimeError, match="reported failure"):
        await executor.build(registration, source_archive=source)


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
