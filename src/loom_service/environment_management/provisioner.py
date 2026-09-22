"""One ordered provider boundary across Kubernetes, IAM, Secrets and child API."""

from __future__ import annotations

import tomllib

from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.environment_management.child_client import ChildEnvironmentClient
from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
from loom_service.environment_management.credentials import EnvironmentCredentialProvider
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProvisioningContext
from loom_service.environment_management.registry import EnvironmentRegistry
from loom_service.environment_management.steps import ProvisioningStep


class EnvironmentProvisioner:
    def __init__(
        self, registry: EnvironmentRegistry, *, kubernetes: KubernetesEnvironmentProvider,
        cloud: NebiusEnvironmentCloudProvider, credentials: EnvironmentCredentialProvider, child: ChildEnvironmentClient,
    ):
        self.registry, self.kubernetes, self.cloud, self.credentials, self.child = registry, kubernetes, cloud, credentials, child

    async def _admin_token(self, context: ProvisioningContext) -> str:
        material = await self.registry.load_material(context.lease, "credentials:material")
        try:
            token = tomllib.loads(material["loom-admin-secret"]["secrets.toml"])["admin"]["token"]
            if not isinstance(token, str) or not token:
                raise ValueError
            return token
        except (KeyError, TypeError, ValueError):
            raise ProviderBlockedError("credential_admin_invalid") from None

    async def apply(self, context: ProvisioningContext, step: ProvisioningStep) -> str:
        if step.kind in {"kubernetes", "database_ready", "job_ready"}:
            return await self.kubernetes.apply(context, step)
        if step.kind == "object_bucket" or (step.kind == "credentials" and step.payload.get("action") in {
            "service_account", "group", "membership", "access_key",
        }):
            return await self.cloud.apply(context, step)
        if step.kind == "credentials" and step.payload.get("action") in {"material", "kubernetes_secret"}:
            return await self.credentials.apply(context, step)
        row = EnvironmentRegistrationV1.model_validate(context.registration)
        if step.kind == "credentials" and step.payload.get("action") == "child_owner":
            if (step.payload.get("namespace") != row.application_namespace
                    or step.payload.get("owner_user_id") != str(row.owner_user_id)
                    or step.payload.get("owner_team_id") != str(row.owner_team_id)):
                raise ProviderBlockedError("child_owner_intent_mismatch")
            await self.child.enroll(row, admin_token=await self._admin_token(context))
            return "owner:" + str(row.owner_user_id)
        if step.kind == "application_ready":
            deployments = await self.kubernetes.apply(context, step)
            if step.payload.get("phase") == "services":
                return deployments
            if context.identities.get("child:owner") != "owner:" + str(row.owner_user_id):
                raise ProviderBlockedError("child_owner_not_enrolled")
            # HTTPS validates the real DNS/certificate/ingress path. The protected
            # child response proves the host reached THIS owner/incarnation, not
            # merely some sibling's anonymous health endpoint.
            await self.child.owner(row, admin_token=await self._admin_token(context))
            return deployments + ":owner:" + str(row.owner_user_id)
        raise ProviderBlockedError("environment_step_not_supported")
