#!/usr/bin/env python3
"""Refresh ephemeral registry auth from an independently scoped authorized key."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def refresh_registry_auth(
    credentials: Path,
    registry_prefix: str,
    auth_file: Path,
    *,
    sdk_factory: Any = None,
) -> dict[str, str]:
    """Write Docker/containers auth atomically; never return or print the token."""
    if re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+", registry_prefix) is None:
        raise ValueError("registry prefix is not a native Nebius registry")
    if (
        credentials.is_symlink()
        or not credentials.is_file()
        or credentials.stat().st_mode & 0o077
        or credentials.stat().st_size > 1024 * 1024
        or credentials.resolve().is_relative_to(ROOT)
        or auth_file.resolve().is_relative_to(ROOT)
        or auth_file.is_symlink()
    ):
        raise ValueError("registry credentials must be private and outside the build context")
    subject = json.loads(credentials.read_bytes()).get("subject-credentials", {})
    if (
        not isinstance(subject, dict)
        or subject.get("alg") != "RS256"
        or subject.get("iss") != subject.get("sub")
        or not all(
            isinstance(subject.get(key), str) and subject[key]
            for key in ("private-key", "kid", "iss", "sub")
        )
    ):
        raise ValueError("registry authentication requires an authorized service-account key")
    if sdk_factory is None:
        from nebius.sdk import SDK

        sdk_factory = SDK

    async def mint() -> tuple[str, datetime]:
        async with sdk_factory(
            credentials_file_name=str(credentials), user_agent_prefix="loom-nebius-publication/1.0"
        ) as sdk:
            token = await sdk.get_token(timeout=30)
            if (
                not token.token
                or token.expiration is None
                or token.expiration <= datetime.now(UTC) + timedelta(minutes=1)
            ):
                raise ValueError("registry token is empty, expired, or has no bounded lifetime")
            return token.token, token.expiration

    token, expires = asyncio.run(mint())
    host = registry_prefix.split("/", 1)[0]
    encoded = base64.b64encode(("oauth2accesstoken:" + token).encode()).decode()
    payload = json.dumps({"auths": {host: {"auth": encoded}}}, sort_keys=True).encode() + b"\n"
    auth_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=".nebius-auth-", dir=auth_file.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, auth_file)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {"registry_host": host, "expires_at": expires.isoformat()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--registry-prefix", required=True)
    parser.add_argument("--auth-file", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = refresh_registry_auth(args.credentials, args.registry_prefix, args.auth_file)
    except Exception as exc:
        print(f"Nebius registry authentication failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
