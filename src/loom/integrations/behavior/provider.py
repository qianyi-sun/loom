"""Closed Provider authentication seams for BEHAVIOR Pipeline stages.

Pipeline containers never receive a Provider API key.  The only credential
available to Provider-bound stages is the worker-rotated ExecutionAttempt JWT
at ``/run/loom/step-jwt``.  This module deliberately owns the file read and the
Messages transport together so a caller cannot accidentally cache the token or
redirect it to an arbitrary endpoint.
"""

from __future__ import annotations

import json
import os
import stat
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import httpx
import jwt

PIPELINE_STEP_JWT_PATH: Final = Path("/run/loom/step-jwt")
PRIMITIVE_STEP_ID: Final = "recovery_primitive"
OFFLINE_JUDGE_STEP_ID: Final = "offline_judge"
GATEWAY_MESSAGES_PATH: Final = "/v1/messages"
PRIMITIVE_DISPATCH_LIMIT: Final = 512
MAX_PIPELINE_STEP_JWT_TTL_SECONDS: Final = 30_000
_MAX_STEP_JWT_BYTES: Final = 16_384
_DIGEST_PREFIX: Final = "sha256:"
_FORBIDDEN_PAYLOAD_KEYS: Final = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "bearer",
        "credential",
        "credentials",
        "hf_token",
        "huggingface_token",
        "password",
        "provider_settings",
        "secret",
        "token",
    }
)


class PipelineProviderAuthError(ValueError):
    """A Pipeline credential or Provider request violated the closed contract."""


class PipelineProviderDispatchError(RuntimeError):
    """The fixed Gateway request failed without exposing credential material."""


class ProviderAttemptBudgetExhaustedError(PipelineProviderAuthError):
    """The next primitive dispatch would exceed its immutable local ceiling."""


@dataclass(frozen=True)
class PipelineStepJwtClaims:
    """The locally inspectable, non-secret identity of one step JWT.

    Signature verification remains the Gateway's authority; a container does
    not receive the signing key.  Local inspection exists to reject expired or
    cross-Attempt/cross-node files before any network I/O.
    """

    execution_attempt_id: UUID
    step_id: str
    expires_at: int
    binding_sha256: str | None


class RotatingPipelineStepJwtReader:
    """Open, verify, and read the current JWT inode for every request."""

    def __init__(
        self,
        path: Path,
        *,
        attempt_id: UUID,
        step_id: str,
        binding_sha256: str,
        expected_uid: int | None = None,
        expected_gid: int | None = None,
    ) -> None:
        if not path.is_absolute():
            raise PipelineProviderAuthError("step JWT path must be absolute")
        if not step_id or len(step_id.encode("utf-8")) > 64:
            raise PipelineProviderAuthError("step identity is invalid")
        _validate_digest(binding_sha256)
        self._path = path
        self._attempt_id = attempt_id
        self._step_id = step_id
        self._binding_sha256 = binding_sha256
        self._expected_uid = os.getuid() if expected_uid is None else expected_uid
        self._expected_gid = os.getgid() if expected_gid is None else expected_gid

    def read_for_request(self) -> str:
        """Return the current bearer after file and claim validation."""

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self._path, flags)
        except OSError as exc:
            raise PipelineProviderAuthError("step JWT is unavailable") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise PipelineProviderAuthError("step JWT must be one regular inode")
            if stat.S_IMODE(metadata.st_mode) != 0o400:
                raise PipelineProviderAuthError("step JWT mode must be 0400")
            if (metadata.st_uid, metadata.st_gid) != (
                self._expected_uid,
                self._expected_gid,
            ):
                raise PipelineProviderAuthError("step JWT owner drift")
            chunks: list[bytes] = []
            size = 0
            while True:
                chunk = os.read(descriptor, 4096)
                if not chunk:
                    break
                size += len(chunk)
                if size > _MAX_STEP_JWT_BYTES:
                    raise PipelineProviderAuthError("step JWT exceeds the closed size limit")
                chunks.append(chunk)
        finally:
            os.close(descriptor)
        try:
            token = b"".join(chunks).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PipelineProviderAuthError("step JWT is not UTF-8") from exc
        if token != token.strip() or not token:
            raise PipelineProviderAuthError("step JWT has non-canonical whitespace")
        self._validate_claims(token)
        return token

    def _validate_claims(self, token: str) -> PipelineStepJwtClaims:
        if not token.startswith("loom_step_"):
            raise PipelineProviderAuthError("step JWT has the wrong credential type")
        encoded = token.removeprefix("loom_step_")
        try:
            claims = jwt.decode(
                encoded,
                options={
                    "verify_signature": False,
                    "verify_exp": False,
                    "verify_aud": False,
                },
                algorithms=["HS256"],
            )
        except jwt.PyJWTError as exc:
            raise PipelineProviderAuthError("step JWT is malformed") from exc
        if claims.get("iss") != "loom-control-plane" or claims.get("sub") != "step-session":
            raise PipelineProviderAuthError("step JWT issuer or subject drift")
        if claims.get("subject_kind") != "execution_attempt" or "trial_id" in claims:
            raise PipelineProviderAuthError("step JWT must be ExecutionAttempt-scoped")
        if claims.get("execution_attempt_id") != str(self._attempt_id):
            raise PipelineProviderAuthError("step JWT Attempt subject drift")
        if claims.get("step_id") != self._step_id:
            raise PipelineProviderAuthError("step JWT node subject drift")
        if claims.get("scopes") != ["llm:call"]:
            raise PipelineProviderAuthError("step JWT scope drift")
        expires_at = claims.get("exp")
        issued_at = claims.get("iat")
        if (
            isinstance(expires_at, bool)
            or not isinstance(expires_at, int)
            or isinstance(issued_at, bool)
            or not isinstance(issued_at, int)
        ):
            raise PipelineProviderAuthError("step JWT lifetime claims are invalid")
        now = int(time.time())
        if expires_at <= now:
            raise PipelineProviderAuthError("step JWT is expired")
        if issued_at > now + 30 or expires_at <= issued_at:
            raise PipelineProviderAuthError("step JWT lifetime drift")

        raw_binding = claims.get("binding_sha256", claims.get("control_binding_sha256"))
        if raw_binding is not None and raw_binding != self._binding_sha256:
            raise PipelineProviderAuthError("step JWT binding drift")
        return PipelineStepJwtClaims(
            execution_attempt_id=self._attempt_id,
            step_id=self._step_id,
            expires_at=expires_at,
            binding_sha256=raw_binding if isinstance(raw_binding, str) else None,
        )


class _PipelineMessagesResource:
    def __init__(self, owner: PipelineAnthropicClient) -> None:
        self._owner = owner

    def create(self, **payload: object) -> dict[str, Any]:
        return self._owner.create_message(payload)


class PipelineAnthropicClient:
    """Minimal Anthropic Messages client pinned to the Loom Gateway route."""

    def __init__(
        self,
        *,
        messages_url: str,
        token_reader: RotatingPipelineStepJwtReader,
        attempt_id: UUID,
        binding_sha256: str,
    ) -> None:
        self._messages_url = messages_url
        self._token_reader = token_reader
        self._attempt_id = attempt_id
        self._binding_sha256 = binding_sha256
        self._dispatches = 0
        self._lock = threading.Lock()
        self._http = httpx.Client(
            timeout=httpx.Timeout(600.0),
            follow_redirects=False,
            trust_env=False,
        )
        self.messages = _PipelineMessagesResource(self)

    @property
    def dispatch_count(self) -> int:
        return self._dispatches

    def create_message(self, payload: Mapping[str, object]) -> dict[str, Any]:
        _validate_message_payload(payload)
        with self._lock:
            if self._dispatches >= PRIMITIVE_DISPATCH_LIMIT:
                raise ProviderAttemptBudgetExhaustedError("provider_attempt_budget_exhausted")
            # Reading occurs inside the dispatch lock immediately before the
            # request.  A rotation therefore changes the very next request and
            # the bearer is never retained on the client object.
            token = self._token_reader.read_for_request()
            self._dispatches += 1
        try:
            response = self._http.post(
                self._messages_url,
                json=dict(payload),
                headers={
                    "Authorization": f"Bearer {token}",
                    "content-type": "application/json",
                    "x-loom-control-binding-sha256": self._binding_sha256,
                    "x-loom-execution-attempt-id": str(self._attempt_id),
                },
            )
            response.raise_for_status()
            value = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            # Never include request/response objects: both can contain the
            # rotating bearer or an upstream echo of it.
            raise PipelineProviderDispatchError("Loom Gateway Messages request failed") from exc
        if not isinstance(value, dict):
            raise PipelineProviderDispatchError("Loom Gateway Messages response is not an object")
        return value

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> PipelineAnthropicClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def build_pipeline_anthropic_client(
    token_path: str | Path,
    base_url: str,
    attempt_id: UUID,
    binding_sha256: str,
) -> PipelineAnthropicClient:
    """Build the sole primitive Provider client.

    The public constructor accepts only the fixed runtime-secret path.  Tests
    of the lower-level reader may use a temporary file, but production callers
    cannot redirect a bearer to a home directory, shared mount, or cache.
    """

    path = Path(token_path)
    if path != PIPELINE_STEP_JWT_PATH:
        raise PipelineProviderAuthError("Pipeline primitive token path must be /run/loom/step-jwt")
    messages_url = _gateway_messages_url(base_url)
    reader = RotatingPipelineStepJwtReader(
        path,
        attempt_id=attempt_id,
        step_id=PRIMITIVE_STEP_ID,
        binding_sha256=binding_sha256,
    )
    return PipelineAnthropicClient(
        messages_url=messages_url,
        token_reader=reader,
        attempt_id=attempt_id,
        binding_sha256=binding_sha256,
    )


def pipeline_step_jwt_ttl_seconds(agent_timeout_seconds: int) -> int:
    """Return the exact #8 ExecutionAttempt token lifetime."""

    if (
        isinstance(agent_timeout_seconds, bool)
        or not isinstance(agent_timeout_seconds, int)
        or agent_timeout_seconds <= 0
    ):
        raise PipelineProviderAuthError("agent timeout must be a positive integer")
    return min(agent_timeout_seconds + 300, MAX_PIPELINE_STEP_JWT_TTL_SECONDS)


def _gateway_messages_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/", "/v1", GATEWAY_MESSAGES_PATH}
    ):
        raise PipelineProviderAuthError(
            "Gateway base URL must be server-owned HTTPS for the exact /v1/messages route"
        )
    return urlunsplit((parsed.scheme, parsed.netloc, GATEWAY_MESSAGES_PATH, "", ""))


def _validate_digest(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_DIGEST_PREFIX)
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise PipelineProviderAuthError("binding_sha256 must be sha256:<64 lowercase hex>")


def _validate_message_payload(payload: Mapping[str, object]) -> None:
    if not isinstance(payload, Mapping):
        raise PipelineProviderAuthError("Messages payload must be an object")
    if not isinstance(payload.get("model"), str) or not payload["model"]:
        raise PipelineProviderAuthError("Messages payload requires model")
    _reject_secret_fields(payload)
    try:
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PipelineProviderAuthError("Messages payload is not strict JSON") from exc
    if len(encoded) > 16_777_216:
        raise PipelineProviderAuthError("Messages payload exceeds the closed size limit")


def _reject_secret_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _FORBIDDEN_PAYLOAD_KEYS:
                raise PipelineProviderAuthError("Messages payload contains a credential field")
            _reject_secret_fields(nested)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_secret_fields(item)


__all__ = [
    "GATEWAY_MESSAGES_PATH",
    "MAX_PIPELINE_STEP_JWT_TTL_SECONDS",
    "OFFLINE_JUDGE_STEP_ID",
    "PIPELINE_STEP_JWT_PATH",
    "PRIMITIVE_DISPATCH_LIMIT",
    "PRIMITIVE_STEP_ID",
    "PipelineAnthropicClient",
    "PipelineProviderAuthError",
    "PipelineProviderDispatchError",
    "PipelineStepJwtClaims",
    "ProviderAttemptBudgetExhaustedError",
    "RotatingPipelineStepJwtReader",
    "build_pipeline_anthropic_client",
    "pipeline_step_jwt_ttl_seconds",
]
