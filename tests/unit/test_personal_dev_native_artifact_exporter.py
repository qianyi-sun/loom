"""Native export uses accepted claim identity and exact full-archive evidence."""

import hashlib
from uuid import uuid4

import pytest

from loom.personal_dev_build_demand import personal_build_work_identity
from loom.personal_dev_builder_exporter import S3TrustedPersonalDevBuildPublicationExporter
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS, PERSONAL_DEV_PLATFORMS
from loom_capacity_agent.build_admission import (
    BuildArtifactV1,
    BuildClaimRequestV1,
    BuildOutcomeReceiptV1,
    BuildOutcomeRequestV1,
    native_build_artifact_key,
)
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_capacity_build_admission_client import native_registration
from tests.unit.test_personal_dev_builder_artifact import _artifact
from tests.unit.test_personal_dev_builder_exporter import (
    _Body,
    _ObjectStore,
    _Publisher,
    _running_registration,
    _Scanner,
)


@pytest.mark.parametrize("boundary", ["exact", "archive-digest", "archive-size", "request", "pool", "receipt", "missing"])
async def test_native_exporter_requires_exact_accepted_artifact_before_scanning(tmp_path, boundary):
    source = _running_registration()
    artifacts, receipts = {}, {}
    for platform in PERSONAL_DEV_PLATFORMS:
        bundle = tmp_path / (platform.rsplit("/", 1)[1] + ".tar")
        _artifact(bundle, platform=platform)
        data = bundle.read_bytes()
        artifacts[platform] = data
        worker = native_registration("oldlab" if platform == "linux/amd64" else "gb10")
        claim = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(),
            request_id=personal_build_work_identity(source, platform)[1],
            worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
        outcome = BuildOutcomeRequestV1(claim=claim, operation_id=uuid4(), result="artifact-ready",
            artifact=BuildArtifactV1(archive_sha256=hashlib.sha256(data).hexdigest(), archive_size_bytes=len(data)))
        receipts[platform] = BuildOutcomeReceiptV1(request=outcome, request_digest=canonical_digest(outcome))

    class Resolver:
        async def resolve(self, registration, *, platform):
            assert registration == source
            receipt = receipts[platform]
            outcome = receipt.request
            if boundary == "missing":
                raise ValueError("accepted artifact unavailable")
            if boundary == "receipt":
                return receipt.model_copy(update={"request_digest": "f" * 64})
            if boundary in {"request", "pool"}:
                claim = outcome.claim
                claim = claim.model_copy(update={"request_id": uuid4()} if boundary == "request" else
                    {"binding": claim.binding.model_copy(update={"pool_id": "gb10" if platform == "linux/amd64" else "oldlab"})})
                outcome = outcome.model_copy(update={"claim": claim})
            elif boundary in {"archive-digest", "archive-size"}:
                outcome = outcome.model_copy(update={"artifact": outcome.artifact.model_copy(update=
                    {"archive_sha256": "f" * 64} if boundary == "archive-digest" else {"archive_size_bytes": len(artifacts[platform])+1})})
            return receipt.model_copy(update={"request": outcome, "request_digest": canonical_digest(outcome)})

    keys = {native_build_artifact_key(receipts[p].request.claim,
        receipts[p].request.artifact.model_copy(update={"archive_sha256": "f" * 64})
        if boundary == "archive-digest" else receipts[p].request.artifact): p for p in PERSONAL_DEV_PLATFORMS}

    class Store(_ObjectStore):
        def get_object(self, **kwargs):
            assert kwargs["Key"] in keys, "native export must not fall back to legacy attempt keys"
            platform = keys[kwargs["Key"]]
            self.requests.append((kwargs["Bucket"], kwargs["Key"]))
            return {"Body": _Body(artifacts[platform]), "ContentLength": len(artifacts[platform]),
                "ContentType": "application/vnd.loom.personal-dev-build.v1+tar", "Metadata": {
                    "attestation-scope": "personal-dev-only", "build-attempt-id": str(source.build_attempt.id),
                    "build-lease-epoch": str(source.build_attempt.lease_epoch),
                    "candidate-sha256": source.candidate.candidate_sha, "platform": platform}}

    store, events = Store(artifacts), []
    exporter = S3TrustedPersonalDevBuildPublicationExporter(object_store=store, expected_bucket="artifacts",
        max_artifact_bytes=2*1024*1024, max_image_archive_bytes=256*1024, scanner=_Scanner(events),
        publisher=_Publisher(events), registry_prefix="registry.example/personal-dev", publisher_identity="trusted-exporter",
        trusted_launcher_profile_sha256="f"*64, protocol_versions={"personal-dev-activation": "v1"},
        accepted_artifact_resolver=Resolver())
    if boundary == "exact":
        await exporter.publish(source)
        assert {key for _, key in store.requests} == set(keys)
        assert next(i for i, event in enumerate(events) if event.startswith("push:")) == len(PERSONAL_DEV_COMPONENTS)*2
    else:
        with pytest.raises((ValueError, RuntimeError)):
            await exporter.publish(source)
        assert events == [] and store.objects == {}
        if boundary in {"request", "pool", "receipt", "missing"}:
            assert store.requests == []
