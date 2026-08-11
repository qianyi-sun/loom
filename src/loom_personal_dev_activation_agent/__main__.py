"""Run the candidate-independent personal-development activation agent."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from loom.dev_instance_runtime import KubectlClient
from loom.personal_dev_activation import load_personal_dev_activation_signer
from loom.personal_dev_activation_agent import (
    HttpPersonalDevActivationAuthority,
    KubectlPersonalDevActivationExecutor,
    PersonalDevActivationAgent,
)

logger = logging.getLogger(__name__)


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _positive_float(name: str, default: str) -> float:
    try:
        value = float(os.environ.get(name, default))
    except ValueError:
        raise RuntimeError(f"{name} must be a positive number") from None
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive number")
    return value


def _service_url() -> str:
    value = _required("LOOM_PERSONAL_DEV_ACTIVATION_SERVICE_URL")
    parsed = urlsplit(value)
    allow_http = os.environ.get("LOOM_PERSONAL_DEV_ACTIVATION_ALLOW_INSECURE_HTTP") == "1"
    if (
        parsed.scheme not in ({"http", "https"} if allow_http else {"https"})
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise RuntimeError("activation service URL must be an origin with approved transport")
    return value.rstrip("/")


async def _run() -> None:
    service_url = _service_url()
    key_id = _required("LOOM_PERSONAL_DEV_ACTIVATION_KEY_ID")
    signer = load_personal_dev_activation_signer(
        Path(_required("LOOM_PERSONAL_DEV_ACTIVATION_PRIVATE_KEY_FILE")),
        key_id=key_id,
    )
    poll_interval = _positive_float("LOOM_PERSONAL_DEV_ACTIVATION_POLL_INTERVAL_SEC", "2")
    timeout = _positive_float("LOOM_PERSONAL_DEV_ACTIVATION_HTTP_TIMEOUT_SEC", "15")
    kubectl = KubectlClient(
        os.environ.get("LOOM_PERSONAL_DEV_ACTIVATION_KUBECTL_PATH", "/usr/local/bin/kubectl"),
        context=os.environ.get("LOOM_PERSONAL_DEV_ACTIVATION_KUBE_CONTEXT", ""),
        field_manager="loom-personal-dev-activation-agent",
    )
    async with httpx.AsyncClient(
        base_url=service_url,
        timeout=timeout,
        trust_env=False,
    ) as client:
        authority = HttpPersonalDevActivationAuthority(client)
        agent = PersonalDevActivationAgent(
            source=authority,
            executor=KubectlPersonalDevActivationExecutor(
                kubectl=kubectl,
                minio_endpoint=_required("LOOM_PERSONAL_DEV_ACTIVATION_MINIO_ENDPOINT"),
                minio_region=os.environ.get(
                    "LOOM_PERSONAL_DEV_ACTIVATION_MINIO_REGION",
                    "us-east-1",
                ),
                ingress_class_name=os.environ.get(
                    "LOOM_PERSONAL_DEV_ACTIVATION_INGRESS_CLASS_NAME",
                    "nginx",
                ),
                ingress_cert_manager_cluster_issuer=os.environ.get(
                    "LOOM_PERSONAL_DEV_ACTIVATION_INGRESS_CLUSTER_ISSUER",
                    "letsencrypt-prod",
                ),
                image_pull_policy=os.environ.get(
                    "LOOM_PERSONAL_DEV_ACTIVATION_IMAGE_PULL_POLICY",
                    "IfNotPresent",
                ),
            ),
            publisher=authority,
            signer=signer,
            agent_key_id=key_id,
        )
        while True:
            try:
                progressed = await agent.reconcile_once(now=datetime.now(UTC))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "personal_dev_activation_iteration_failed",
                    extra={"error_type": type(exc).__name__},
                )
                progressed = False
            if not progressed:
                await asyncio.sleep(poll_interval)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOOM_PERSONAL_DEV_ACTIVATION_LOG_LEVEL", "INFO").upper(),
    )
    asyncio.run(_run())


if __name__ == "__main__":
    main()

