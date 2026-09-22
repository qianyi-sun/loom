"""Cloud mutations are incarnation-scoped, replayable and separately journaled."""

from __future__ import annotations

import copy

import pytest

from loom_service.environment_management.steps import ProvisioningStep, creation_steps
from tests.unit.test_nebius_environment_kubernetes_provider import context
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def cloud_context():
    ctx = context()
    ctx.config.update(project_id="project-owned", quota_parent_id="tenant-owned")
    return ctx


class CloudApi:
    def __init__(self):
        self.rows = {}
        self.created = []

    async def find(self, kind, expected):
        return copy.deepcopy(self.rows.get((kind, expected["metadata"]["parent_id"])))

    async def create(self, kind, expected, *, idempotency_key):
        self.created.append((kind, copy.deepcopy(expected), idempotency_key))
        actual = copy.deepcopy(expected)
        actual["metadata"]["id"] = kind + "-id"
        self.rows[kind, expected["metadata"]["parent_id"]] = actual
        return actual


async def test_service_account_replay_has_stable_key_and_rejects_foreign_match():
    from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
    from loom_service.environment_management.provider import ProviderBlockedError

    ctx, api = cloud_context(), CloudApi()
    step = ProvisioningStep("iam:canonical:service_account", "credentials", {
        "action": "service_account", "purpose": "canonical",
    })
    provider = NebiusEnvironmentCloudProvider(api)
    assert await provider.apply(ctx, step) == "service_account-id"
    assert await provider.apply(ctx, step) == "service_account-id"
    assert len(api.created) == 1
    kind, body, retry_key = api.created[0]
    assert kind == "service_account" and body["metadata"]["parent_id"] == "project-owned"
    assert ctx.registration["incarnation"].replace("-", "") in body["metadata"]["name"]
    # An unknown response retries the same request key, not a fresh IAM object.
    api.rows.clear()
    await provider.apply(ctx, step)
    assert api.created[1][2] == retry_key
    api.rows["service_account", "project-owned"]["metadata"]["labels"] = {}
    with pytest.raises(ProviderBlockedError, match="cloud_resource_identity_conflict"):
        await provider.apply(ctx, step)


@pytest.mark.parametrize("purpose,identity,versioning", [
    ("artifacts", "canonical", "ENABLED"), ("trajectories", "canonical", "ENABLED"),
    ("source", "source", "DISABLED"), ("backup", "backup", "ENABLED"),
])
async def test_bucket_policy_grants_only_the_matching_group_and_no_data_expiry(purpose, identity, versioning):
    from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider

    ctx, api = cloud_context(), CloudApi()
    ctx.identities["iam:" + identity + ":group"] = "isolated-group-id"
    name = "loom-" + ctx.registration["incarnation"].replace("-", "") + "-" + purpose
    step = ProvisioningStep("bucket:" + purpose, "object_bucket", {"purpose": purpose, "name": name})
    assert await NebiusEnvironmentCloudProvider(api).apply(ctx, step) == "bucket-id"
    spec = api.created[0][1]["spec"]
    assert spec["bucket_policy"]["rules"] == [{
        "group_id": "isolated-group-id", "paths": ["*"], "roles": ["storage.object-editor"],
    }]
    assert spec["versioning_policy"] == versioning
    assert spec["lifecycle_configuration"]["rules"] == [{
        "id": "abort-incomplete-uploads", "status": "ENABLED",
        "abort_incomplete_multipart_upload": {"days_after_initiation": 7},
    }]


async def test_cloud_dependency_and_bucket_name_cannot_escape_environment():
    from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
    from loom_service.environment_management.provider import ProviderBlockedError

    ctx, api = cloud_context(), CloudApi()
    with pytest.raises(ProviderBlockedError, match="cloud_dependency_missing"):
        await NebiusEnvironmentCloudProvider(api).apply(ctx, ProvisioningStep("group-member", "credentials", {
            "action": "membership", "purpose": "source",
        }))
    ctx.identities["iam:canonical:group"] = "owned-group"
    with pytest.raises(ProviderBlockedError, match="cloud_bucket_name_mismatch"):
        await NebiusEnvironmentCloudProvider(api).apply(ctx, ProvisioningStep("bucket", "object_bucket", {
            "purpose": "artifacts", "name": "foreign-data",
        }))
    assert api.created == []


def test_plan_records_each_iam_effect_before_bucket_and_secret_publication(platform_inputs):
    from uuid import uuid4

    from loom.nebius_environment_contract import new_environment_registration
    from loom.nebius_environment_render import render_environment
    from tests.unit.test_nebius_environment_contract import foundation_from
    from tests.unit.test_nebius_platform_render import ROOT

    foundation = foundation_from(platform_inputs[0])
    row = new_environment_registration(foundation, environment_id=uuid4(), incarnation=uuid4(),
                                       owner_user_id=uuid4(), owner_team_id=uuid4(), slug="alice")
    prepared = render_environment(row, platform_inputs[1], foundation,
                                  profile=platform_inputs[2], keyring={}, repo_root=ROOT)
    steps = creation_steps(prepared)
    keys = [step.key for step in steps]
    for identity in ("canonical", "source", "backup"):
        assert keys.index(f"iam:{identity}:service_account") < keys.index(f"iam:{identity}:membership")
        assert keys.index(f"iam:{identity}:group") < keys.index(f"iam:{identity}:membership")
        assert keys.index(f"iam:{identity}:membership") < keys.index(f"iam:{identity}:access_key")
        assert keys.index(f"iam:{identity}:access_key") < keys.index("secret:loom-platform-storage")
    assert keys.index("credentials:material") < keys.index("secret:loom-platform-db")
    assert keys.index("secret:loom-platform-db") < keys.index("ready:database")
    # Personal environments must not require a copied installation inference key.
    for document in prepared.files["40-services.yaml"]:
        if document["kind"] == "Deployment" and document["metadata"]["name"] == "loom-llm-gateway":
            env = document["spec"]["template"]["spec"]["containers"][0]["env"]
            assert all(item["name"] not in {"LOOM_GW_LOCAL_YIBU_API_KEY", "LOOM_GW_LOCAL_YIBU_BASE_URL"} for item in env)


@pytest.mark.parametrize("kind,purpose", [("service_account", "canonical"), ("group", "source"),
                                         ("membership", "backup"), ("access_key", "canonical"), ("bucket", "source")])
async def test_sdk_adapter_serializes_real_nebius_requests_and_pins_idempotency(kind, purpose):
    import json
    from types import SimpleNamespace

    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.storage import v1 as storage

    from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
    from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi

    ctx = cloud_context()
    for action in ("group", "service_account"):
        ctx.identities[f"iam:{purpose}:{action}"] = action + "-owned"
    step = ProvisioningStep("test", "object_bucket" if kind == "bucket" else "credentials", {
        "action": kind, "purpose": purpose,
        "name": "loom-" + ctx.registration["incarnation"].replace("-", "") + "-" + purpose,
    })
    _, expected = NebiusEnvironmentCloudProvider(CloudApi()).intent(ctx, step)
    resource_cls = {"service_account": v1.ServiceAccount, "group": v1.Group, "membership": v1.GroupMembership,
                    "access_key": v2.AccessKey, "bucket": storage.Bucket}[kind]
    captured = []

    class CompletedOperation:
        resource_id = "resource-owned"

        async def wait(self, **kwargs):
            return self

        def successful(self):
            return True

    class Client:
        async def create(self, request, **kwargs):
            captured.append((json.loads(request.to_json(preserving_proto_field_name=True)), kwargs))
            return CompletedOperation()

        async def get(self, request, **kwargs):
            assert request.id == "resource-owned"
            return resource_cls.from_json(json.dumps({
                **expected, "metadata": {**expected["metadata"], "id": "resource-owned"},
            }))

    api = NebiusSdkEnvironmentApi(SimpleNamespace(), clients={kind: Client()})
    actual = await api.create(kind, expected, idempotency_key="durable-retry-key")
    assert actual["metadata"]["id"] == "resource-owned"
    assert captured[0][1]["metadata"] == [("x-idempotency-key", "durable-retry-key")]
    assert captured[0][1]["retries"] == 0
    assert captured[0][0]["metadata"]["parent_id"] == expected["metadata"]["parent_id"]
    assert captured[0][0].get("spec", {}) == expected["spec"]


async def test_sdk_lookup_checks_all_pages_and_rejects_duplicate_access_keys():
    from types import SimpleNamespace

    from nebius.api.nebius.common.v1 import ResourceMetadata
    from nebius.api.nebius.iam.v2 import AccessKey, ListAccessKeysResponse

    from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
    from loom_service.environment_management.provider import ProviderBlockedError

    class Client:
        duplicate = False

        async def list(self, request, **kwargs):
            if not request.page_token:
                return ListAccessKeysResponse(items=[AccessKey(metadata=ResourceMetadata(
                    id="other-key", name="wanted" if self.duplicate else "other", parent_id="project",
                ))], next_page_token="second")
            assert request.page_token == "second"
            return ListAccessKeysResponse(items=[AccessKey(metadata=ResourceMetadata(id="wanted-key", name="wanted", parent_id="project"))])

    client = Client()
    api = NebiusSdkEnvironmentApi(SimpleNamespace(), clients={"access_key": client})
    expected = {"metadata": {"parent_id": "project", "name": "wanted"}}
    assert (await api.find("access_key", expected))["metadata"]["id"] == "wanted-key"
    client.duplicate = True
    with pytest.raises(ProviderBlockedError, match="identity_ambiguous"):
        await api.find("access_key", expected)


@pytest.mark.parametrize("code,expected_exception", [("NOT_FOUND", None), ("UNAVAILABLE", "retry"),
                                                    ("PERMISSION_DENIED", "blocked")])
async def test_sdk_lookup_sanitizes_failures_and_only_not_found_means_absent(code, expected_exception):
    from types import SimpleNamespace

    from grpc import StatusCode
    from nebius.aio.service_error import RequestError, RequestStatusExtended

    from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
    from loom_service.environment_management.provider import (
        ProviderBlockedError,
        ProviderRetryError,
    )

    class Client:
        async def get_by_name(self, request, **kwargs):
            raise RequestError(RequestStatusExtended(getattr(StatusCode, code), "password=upstream-private", [], "", "", []))

    api = NebiusSdkEnvironmentApi(SimpleNamespace(), clients={"bucket": Client()})
    expected = {"metadata": {"parent_id": "project", "name": "wanted"}}
    if expected_exception is None:
        assert await api.find("bucket", expected) is None
    else:
        with pytest.raises(ProviderRetryError if expected_exception == "retry" else ProviderBlockedError) as caught:
            await api.find("bucket", expected)
        assert "private" not in str(caught.value)
