"""Worker-token helpers shared by release gates and control-plane records."""

from __future__ import annotations

import hashlib
from pathlib import Path

DEFAULT_WORKER_TOKEN_ENV_KEY = "LOOM_WORKER_TOKEN"
WORKER_AUTH_FINGERPRINT_ENV_KEY = "LOOM_WORKER_AUTH_FINGERPRINT"


def worker_token_fingerprint(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"sha256:{digest} len={len(value)}"


def read_env_file_value(path: Path, key: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        name, sep, value = line.partition("=")
        if sep != "=" or name.strip() != key:
            continue
        value = value.strip()
        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in {"'", '"'}
        ):
            value = value[1:-1]
        return value
    return None


def worker_token_fingerprint_from_env_file(
    path: Path,
    *,
    key: str = DEFAULT_WORKER_TOKEN_ENV_KEY,
) -> str | None:
    token = read_env_file_value(path, key)
    return worker_token_fingerprint(token) if token else None
