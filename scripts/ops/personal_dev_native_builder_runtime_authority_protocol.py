"""Canonical framed requests for the personal-dev native-builder authority."""

from __future__ import annotations

import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import BinaryIO
from urllib.parse import urlsplit
from uuid import UUID

MAGIC = b"LOOMNBR1"
HEADER_MAX_BYTES = 64 * 1024
TOTAL_MAX_BYTES = 2 * 1024 * 1024
PRIVATE_KEY_LENGTH = 32
SERVICE_CA_MAX_BYTES = 1024 * 1024

_OPERATIONS = frozenset({"status", "prepare", "stage-agent", "activate", "remove"})
_COMMON_FIELDS = frozenset(
    {
        "authority_source_sha",
        "authority_source_tree",
        "operation",
        "request_id",
        "runtime_profile_sha256",
        "schema_version",
    }
)
_PREPARE_FIELDS = frozenset(
    {
        "archive_path",
        "archive_sha512",
        "current_agent",
        "current_builder",
        "current_revision",
        "previous_agent",
        "previous_builder",
        "previous_revision",
        "public_store_origin",
    }
)
_STAGE_AGENT_FIELDS = frozenset(
    {
        "agent_image",
        "agent_instance_id",
        "agent_key_id",
        "builder_image",
        "expected_public_key_sha256",
        "expected_state_sha256",
        "private_key_length",
        "service_ca_length",
        "service_origin",
    }
)
_STATE_FIELDS = frozenset({"expected_state_sha256"})
_HEX_40_RE = re.compile(r"[0-9a-f]{40}")
_HEX_64_RE = re.compile(r"[0-9a-f]{64}")
_HEX_128_RE = re.compile(r"[0-9a-f]{128}")
_KEY_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}")
_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*"
)
_AGENT_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent"
_BUILDER_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-builder"


class ProtocolError(ValueError):
    """The request frame or its security contract is invalid."""


def _canonical_json(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise ProtocolError("authority request header is not JSON") from exc


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("authority request header contains duplicate fields")
        result[key] = value
    return result


def _string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ProtocolError(f"authority request {field} is invalid")
    return value


def _hex(value: object, field: str, expression: re.Pattern[str], *, nonzero: bool = False) -> str:
    result = _string(value, field)
    if expression.fullmatch(result) is None or (nonzero and set(result) == {"0"}):
        raise ProtocolError(f"authority request {field} is invalid")
    return result


def _canonical_uuid(value: object, field: str) -> str:
    result = _string(value, field)
    try:
        parsed = UUID(result)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ProtocolError(f"authority request {field} is invalid") from exc
    if str(parsed) != result:
        raise ProtocolError(f"authority request {field} is invalid")
    return result


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise ProtocolError(f"authority request {field} is invalid")
    return value


def _image(value: object, field: str, repository: str) -> str:
    result = _string(value, field)
    prefix = f"{repository}@sha256:"
    if not result.startswith(prefix) or _HEX_64_RE.fullmatch(result[len(prefix) :]) is None:
        raise ProtocolError(f"authority request {field} is invalid")
    return result


def _origin(value: object, field: str) -> str:
    result = _string(value, field)
    if any(character in result for character in "\r\n\0"):
        raise ProtocolError(f"authority request {field} is invalid")
    try:
        parsed = urlsplit(result)
        port = parsed.port
    except ValueError as exc:
        raise ProtocolError(f"authority request {field} is invalid") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or port not in (None, 443)
    ):
        raise ProtocolError(f"authority request {field} is invalid")
    host = parsed.hostname
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if _HOST_RE.fullmatch(host) is None:
            raise ProtocolError(f"authority request {field} is invalid") from None
    else:
        if not address.is_global:
            raise ProtocolError(f"authority request {field} is invalid")
    return result


def _expected_fields(operation: str) -> frozenset[str]:
    if operation == "status":
        return _COMMON_FIELDS
    if operation == "prepare":
        return _COMMON_FIELDS | _PREPARE_FIELDS
    if operation == "stage-agent":
        return _COMMON_FIELDS | _STAGE_AGENT_FIELDS
    if operation in {"activate", "remove"}:
        return _COMMON_FIELDS | _STATE_FIELDS
    raise ProtocolError("authority request operation is invalid")


def _validate_header(mapping: Mapping[str, object]) -> tuple[dict[str, object], int]:
    if not isinstance(mapping, Mapping):
        raise ProtocolError("authority request header is invalid")
    values = dict(mapping)
    operation = _string(values.get("operation"), "operation")
    if operation not in _OPERATIONS or set(values) != _expected_fields(operation):
        raise ProtocolError("authority request fields are invalid")
    if _integer(values["schema_version"], "schema_version") != 1:
        raise ProtocolError("authority request schema version is invalid")
    _canonical_uuid(values["request_id"], "request_id")
    source_sha = _hex(values["authority_source_sha"], "authority_source_sha", _HEX_40_RE)
    source_tree = _hex(values["authority_source_tree"], "authority_source_tree", _HEX_40_RE)
    if source_sha == source_tree:
        raise ProtocolError("authority request source identities must differ")
    _hex(values["runtime_profile_sha256"], "runtime_profile_sha256", _HEX_64_RE, nonzero=True)
    payload_length = 0

    if operation == "prepare":
        request_id = _string(values["request_id"], "request_id")
        archive_path = (
            f"/var/tmp/loom-personal-dev-native-builder/{request_id}/"
            "gvisor-release-20260810.0-aarch64.tar.bz2"
        )
        if values["archive_path"] != archive_path:
            raise ProtocolError("authority request archive_path is invalid")
        _hex(values["archive_sha512"], "archive_sha512", _HEX_128_RE, nonzero=True)
        _image(values["current_agent"], "current_agent", _AGENT_REPOSITORY)
        _image(values["current_builder"], "current_builder", _BUILDER_REPOSITORY)
        current_revision = _hex(values["current_revision"], "current_revision", _HEX_40_RE)
        previous = (values["previous_agent"], values["previous_builder"], values["previous_revision"])
        if all(item == "" for item in previous):
            pass
        elif all(isinstance(item, str) and item for item in previous):
            _image(values["previous_agent"], "previous_agent", _AGENT_REPOSITORY)
            _image(values["previous_builder"], "previous_builder", _BUILDER_REPOSITORY)
            previous_revision = _hex(values["previous_revision"], "previous_revision", _HEX_40_RE)
            if previous_revision == current_revision:
                raise ProtocolError("authority request previous_revision is invalid")
        else:
            raise ProtocolError("authority request previous images are invalid")
        _origin(values["public_store_origin"], "public_store_origin")
    elif operation == "stage-agent":
        _hex(values["expected_state_sha256"], "expected_state_sha256", _HEX_64_RE, nonzero=True)
        _image(values["agent_image"], "agent_image", _AGENT_REPOSITORY)
        _image(values["builder_image"], "builder_image", _BUILDER_REPOSITORY)
        _origin(values["service_origin"], "service_origin")
        _canonical_uuid(values["agent_instance_id"], "agent_instance_id")
        key_id = _string(values["agent_key_id"], "agent_key_id")
        if _KEY_ID_RE.fullmatch(key_id) is None:
            raise ProtocolError("authority request agent_key_id is invalid")
        _hex(
            values["expected_public_key_sha256"],
            "expected_public_key_sha256",
            _HEX_64_RE,
            nonzero=True,
        )
        if _integer(values["private_key_length"], "private_key_length") != PRIVATE_KEY_LENGTH:
            raise ProtocolError("authority request private_key_length is invalid")
        service_ca_length = _integer(values["service_ca_length"], "service_ca_length")
        if not 1 <= service_ca_length <= SERVICE_CA_MAX_BYTES:
            raise ProtocolError("authority request service_ca_length is invalid")
        payload_length = PRIVATE_KEY_LENGTH + service_ca_length
    elif operation in {"activate", "remove"}:
        _hex(values["expected_state_sha256"], "expected_state_sha256", _HEX_64_RE, nonzero=True)

    return values, payload_length


@dataclass(frozen=True, slots=True)
class AuthorityRequestHeader:
    """Validated, immutable public JSON request metadata."""

    fields: Mapping[str, object]

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> AuthorityRequestHeader:
        values, _ = _validate_header(mapping)
        return cls(MappingProxyType(values))

    def as_mapping(self) -> Mapping[str, object]:
        return self.fields

    @property
    def operation(self) -> str:
        return _string(self.fields["operation"], "operation")

    def __getattr__(self, name: str) -> object:
        try:
            return self.fields[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


@dataclass(frozen=True, slots=True)
class AuthorityRequest:
    """A validated authority request with its optional secret payload."""

    header: AuthorityRequestHeader
    payload: bytes


def encode_request(header: Mapping[str, object], payload: bytes = b"") -> bytes:
    """Encode one fully validated request into its canonical binary frame."""
    values, payload_length = _validate_header(header)
    if not isinstance(payload, bytes) or len(payload) != payload_length:
        raise ProtocolError("authority request payload length is invalid")
    header_bytes = _canonical_json(values)
    if len(header_bytes) > HEADER_MAX_BYTES:
        raise ProtocolError("authority request header exceeds its bound")
    if len(MAGIC) + 4 + len(header_bytes) + len(payload) > TOTAL_MAX_BYTES:
        raise ProtocolError("authority request exceeds its bound")
    return MAGIC + len(header_bytes).to_bytes(4, "big") + header_bytes + payload


def _read_exact(stream: BinaryIO, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not isinstance(chunk, bytes) or not chunk:
            raise ProtocolError("authority request is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def parse_request(stream: BinaryIO) -> AuthorityRequest:
    """Read exactly one canonical, bounded request frame from *stream*."""
    prefix = _read_exact(stream, len(MAGIC) + 4)
    if prefix[: len(MAGIC)] != MAGIC:
        raise ProtocolError("authority request magic is invalid")
    header_length = int.from_bytes(prefix[len(MAGIC) :], "big")
    if header_length > HEADER_MAX_BYTES:
        raise ProtocolError("authority request header exceeds its bound")
    header_bytes = _read_exact(stream, header_length)
    try:
        decoded = header_bytes.decode("ascii")
        loaded = json.loads(decoded, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ProtocolError) as exc:
        raise ProtocolError("authority request header is invalid") from exc
    if not isinstance(loaded, dict):
        raise ProtocolError("authority request header is invalid")
    values, payload_length = _validate_header(loaded)
    if _canonical_json(values) != header_bytes:
        raise ProtocolError("authority request header is not canonical")
    if len(MAGIC) + 4 + header_length + payload_length > TOTAL_MAX_BYTES:
        raise ProtocolError("authority request exceeds its bound")
    payload = _read_exact(stream, payload_length)
    trailing = stream.read(1)
    if not isinstance(trailing, bytes) or trailing:
        raise ProtocolError("authority request contains trailing bytes")
    return AuthorityRequest(AuthorityRequestHeader(MappingProxyType(values)), payload)
