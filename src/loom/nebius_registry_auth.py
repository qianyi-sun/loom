"""Mint short-lived native registry auth inside a trusted publication phase."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import stat
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def mint_registry_auth(
    credentials: Path,
    registry_prefix: str,
    auth_file: Path,
    *,
    sdk_factory: Any = None,
) -> dict[str, str]:
    """Atomically write private auth; return only the registry host and expiry.

    Trusted Kubernetes Secret projections may use symlinks and group-readable
    files. Callers own the mounted source and output directory; CLI callers add
    their stricter build-context and private-file boundaries before calling.
    """
    if re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+", registry_prefix) is None:
        raise ValueError("registry prefix is not a native Nebius registry")
    metadata = credentials.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_mode & 0o007
        or metadata.st_size > 1024 * 1024
        or auth_file.is_symlink()
    ):
        raise ValueError("registry credentials require a non-public regular file and private output")
    document = json.loads(credentials.read_bytes())
    subject = document.get("subject-credentials", {}) if isinstance(document, dict) else {}
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
                not isinstance(token.token, str)
                or not token.token
                or not isinstance(token.expiration, datetime)
                or token.expiration.utcoffset() is None
                or token.expiration <= datetime.now(UTC) + timedelta(minutes=1)
            ):
                raise ValueError("registry token is empty, expired, or has no bounded lifetime")
            return token.token, token.expiration

    token, expires = asyncio.run(mint())
    host = registry_prefix.split("/", 1)[0]
    encoded = base64.b64encode(("iam:" + token).encode()).decode()
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
