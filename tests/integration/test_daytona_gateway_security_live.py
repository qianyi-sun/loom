"""Opt-in live proof for the Daytona Gateway-only security profile.

This test creates and deletes one real sandbox and makes one real model call
with ``max_tokens=1``. It is intentionally separate from the ordinary Daytona
lifecycle integration flag so routine local/CI runs cannot spend provider or
model budget accidentally.
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit

import pytest

from loom.models.networking import Allowlist, NoNetwork
from loom_drivers.daytona.config import DaytonaConfig
from loom_drivers.daytona.driver import DaytonaDriver

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DAYTONA_GATEWAY_LIVE") != "1",
    reason=(
        "opt-in: set LOOM_RUN_DAYTONA_GATEWAY_LIVE=1 only for the reviewed "
        "one-call Gateway security proof"
    ),
)


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.fail(f"{name} is required for the live Daytona Gateway proof")
    return value


async def test_gateway_call_allowed_but_direct_internet_and_no_network_blocked() -> None:
    gateway_url = _required("LOOM_DAYTONA_GATEWAY_LIVE_URL").rstrip("/")
    parsed_gateway = urlsplit(gateway_url)
    gateway_host = parsed_gateway.hostname or ""
    assert parsed_gateway.scheme == "https"
    assert gateway_host and parsed_gateway.port in {None, 443}
    image = _required("LOOM_DAYTONA_GATEWAY_LIVE_IMAGE")
    model = _required("LOOM_DAYTONA_GATEWAY_LIVE_MODEL")
    provider_connection_id = _required(
        "LOOM_DAYTONA_GATEWAY_LIVE_PROVIDER_CONNECTION_ID"
    )
    step_token = _required("LOOM_DAYTONA_GATEWAY_LIVE_STEP_TOKEN")
    assert image.count("@sha256:") == 1
    assert step_token.startswith("loom_step_")

    drv = DaytonaDriver(
        image=image,
        config=DaytonaConfig.from_env(),
        network_policy_baseline=Allowlist(domains=(gateway_host,)),
        allow_public_network=False,
        allowed_network_domains=frozenset({gateway_host}),
        allow_network_cidrs=False,
        require_scoped_gateway_credentials=True,
    )
    await drv.start()
    try:
        model_call = await drv.exec(
            "python -c 'import json,os,urllib.request; "
            "u=os.environ[\"LOOM_GATEWAY_URL\"]+\"/chat/completions\"; "
            "b=json.dumps({\"model\":os.environ[\"LOOM_GATEWAY_MODEL\"],"
            "\"messages\":[{\"role\":\"user\",\"content\":\"Reply OK\"}],"
            "\"max_tokens\":1}).encode(); "
            "r=urllib.request.Request(u,data=b,headers={"
            "\"content-type\":\"application/json\","
            "\"x-loom-provider-connection-id\":"
            "os.environ[\"LOOM_PROVIDER_CONNECTION_ID\"],"
            "\"authorization\":\"Bearer \"+os.environ[\"LOOM_STEP_TOKEN\"]}); "
            "print(urllib.request.urlopen(r,timeout=30).status)'",
            env={
                "LOOM_GATEWAY_URL": gateway_url,
                "LOOM_GATEWAY_MODEL": model,
                "LOOM_PROVIDER_CONNECTION_ID": provider_connection_id,
                "LOOM_STEP_TOKEN": step_token,
            },
            timeout_sec=45,
        )
        assert model_call.return_code == 0
        assert b"200" in model_call.stdout

        bypass = await drv.exec(
            "python -c 'import socket; "
            "socket.create_connection((\"api.openai.com\",443),5); print(\"open\")'",
            timeout_sec=15,
        )
        assert bypass.return_code != 0
        assert b"open" not in bypass.stdout

        await drv.set_network_policy(NoNetwork())
        blocked_gateway = await drv.exec(
            "python -c 'import os,urllib.request; "
            "urllib.request.urlopen(os.environ[\"LOOM_GATEWAY_URL\"],timeout=5)'",
            env={"LOOM_GATEWAY_URL": gateway_url},
            timeout_sec=15,
        )
        assert blocked_gateway.return_code != 0
    finally:
        await drv.stop(delete=True)
