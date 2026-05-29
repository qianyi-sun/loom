from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4


SESSION_COOKIE_NAME = "adp_session"


class SessionCookieError(ValueError):
    pass


@dataclass(frozen=True)
class WebLoginCredential:
    username: str
    password: str
    user_id: str


def parse_web_login_credentials(raw_value: str) -> dict[str, WebLoginCredential]:
    credentials: dict[str, WebLoginCredential] = {}
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError("WEB_LOGIN_CREDENTIALS entries must use username=password[:user_id]")
        username, password_and_user = [part.strip() for part in item.split("=", 1)]
        if ":" in password_and_user:
            password, user_id = [part.strip() for part in password_and_user.split(":", 1)]
        else:
            password, user_id = password_and_user.strip(), username
        if not username or not password or not user_id:
            raise ValueError("WEB_LOGIN_CREDENTIALS entries must include username, password, and user_id")
        credentials[username] = WebLoginCredential(username=username, password=password, user_id=user_id)
    return credentials


def create_session_cookie(
    *,
    user_id: str,
    secret: str,
    ttl_seconds: int,
    now: datetime | None = None,
) -> tuple[str, datetime]:
    _require_non_empty("user_id", user_id)
    _require_non_empty("secret", secret)
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")

    issued_at = _utc(now)
    expires_at = issued_at + timedelta(seconds=ttl_seconds)
    payload = {
        "sub": user_id,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
        "nonce": uuid4().hex,
    }
    encoded_payload = _b64url(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    signature = _sign(encoded_payload, secret)
    return f"v1.{encoded_payload}.{signature}", expires_at


def verify_session_cookie(
    value: str,
    *,
    secret: str,
    now: datetime | None = None,
) -> str:
    _require_non_empty("secret", secret)
    parts = value.split(".")
    if len(parts) != 3 or parts[0] != "v1":
        raise SessionCookieError("invalid session cookie format")
    _, encoded_payload, signature = parts
    expected = _sign(encoded_payload, secret)
    if not hmac.compare_digest(signature, expected):
        raise SessionCookieError("invalid session cookie signature")
    try:
        payload = json.loads(_b64url_decode(encoded_payload))
    except (json.JSONDecodeError, ValueError) as exc:
        raise SessionCookieError("invalid session cookie payload") from exc
    user_id = payload.get("sub")
    expires_at = payload.get("exp")
    if not isinstance(user_id, str) or not user_id.strip():
        raise SessionCookieError("session cookie missing subject")
    if not isinstance(expires_at, int):
        raise SessionCookieError("session cookie missing expiry")
    if int(_utc(now).timestamp()) >= expires_at:
        raise SessionCookieError("session cookie expired")
    return user_id


def _sign(encoded_payload: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), encoded_payload.encode("ascii"), hashlib.sha256).digest()
    return _b64url(digest)


def _b64url(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> str:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii")).decode("utf-8")


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
