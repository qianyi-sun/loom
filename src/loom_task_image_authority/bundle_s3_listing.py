"""Bounded non-expanding ListObjectsV2 XML for exact native MinIO bundle prefixes."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import unquote, unquote_plus
from xml.parsers import expat

from loom_task_image_authority.bundle_capability import (
    TaskImageBundleCapabilityError,
    TaskImageBundleObject,
    _bucket,
    _relative_path,
)

MAX_S3_LIST_PAGE_BYTES = 4 * 1024 * 1024
MAX_S3_CONTINUATION_TOKEN_BYTES = 4096
_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/}"


@dataclass(frozen=True, slots=True)
class S3ListPage:
    objects: tuple[TaskImageBundleObject, ...] = field(repr=False)
    next_token: str | None = field(repr=False)


@dataclass(slots=True)
class _Node:
    name: str
    children: list[_Node] = field(default_factory=list)
    text: list[str] = field(default_factory=list)
    text_size: int = 0


def _tree(payload: bytes) -> _Node:
    parser = expat.ParserCreate(namespace_separator="}")
    stack: list[_Node] = []
    root: _Node | None = None
    nodes = 0
    segments = 0

    def reject(*args: object) -> None:
        raise ValueError("unsupported XML")

    def start(name: str, attributes: dict[str, str]) -> None:
        nonlocal root, nodes
        nodes += 1
        if (
            nodes > 16384 or len(stack) >= 4 or attributes
            or not name.startswith(_NAMESPACE) or len(name) > len(_NAMESPACE) + 64
        ):
            raise ValueError("invalid XML shape")
        node = _Node(name.removeprefix(_NAMESPACE))
        if stack:
            stack[-1].children.append(node)
        elif root is not None:
            raise ValueError("multiple roots")
        else:
            root = node
        stack.append(node)

    def text(value: str) -> None:
        nonlocal segments
        if not stack:
            if value.strip():
                raise ValueError("text outside root")
            return
        segments += 1
        node = stack[-1]
        node.text_size += len(value)
        if segments > 65536 or node.text_size > 8192:
            raise ValueError("XML text limit")
        node.text.append(value)

    def end(name: str) -> None:
        stack.pop()

    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = text
    parser.StartDoctypeDeclHandler = reject
    parser.EntityDeclHandler = reject
    parser.ProcessingInstructionHandler = reject
    parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)
    parser.Parse(payload, True)
    if root is None or stack:
        raise ValueError("incomplete XML")
    return root


def _scalar(node: _Node) -> str:
    if node.children:
        raise ValueError("nested scalar")
    return "".join(node.text)


def _fields(node: _Node, *, repeated: frozenset[str] = frozenset()) -> dict[str, _Node]:
    if "".join(node.text).strip():
        raise ValueError("mixed content")
    values: dict[str, _Node] = {}
    for child in node.children:
        if child.name in values and child.name not in repeated:
            raise ValueError("duplicate field")
        values[child.name] = child
    return values


def _number(value: str, ceiling: int) -> int:
    if len(value) > 20 or re.fullmatch(r"0|[1-9][0-9]*", value) is None:
        raise ValueError("invalid integer")
    result = int(value)
    if result > ceiling:
        raise ValueError("integer limit")
    return result


def _decoded(value: str, url_encoding: Literal["percent", "form"]) -> str:
    if re.search(r"%(?![0-9a-fA-F]{2})", value):
        raise ValueError("invalid percent encoding")
    decode = unquote_plus if url_encoding == "form" else unquote
    return decode(value, encoding="utf-8", errors="strict")


def _token(value: str | None) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str or not 0 < len(value.encode("utf-8")) <= MAX_S3_CONTINUATION_TOKEN_BYTES
        or not value.isascii() or any(ord(char) < 33 or ord(char) == 127 for char in value)
    ):
        raise ValueError("invalid continuation token")
    return value


def parse_list_objects_v2(
    payload: bytes, *, expected_bucket: str, prefix: str,
    maximum_keys: int, url_encoding: Literal["percent", "form"],
    continuation_token: str | None = None,
) -> S3ListPage:
    """Validate one bounded uncompressed page; caller bounds wire input first.

    Does not prove source immutability, authorization or cross-page progress.
    The asynchronous owner must share a total budget across all pages and check
    accumulated object identities/continuation tokens before returning inventory.

    The pinned native MinIO service uses form encoding (space -> +, plus -> %2B)
    in URL-encoded XML fields. Other S3 implementations use percent encoding.
    The caller must explicitly select its service's contract; never guess from
    response text, which would silently change object identity for literal '+'.
    """
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_S3_LIST_PAGE_BYTES:
            raise ValueError("page byte limit")
        _bucket(expected_bucket)
        if (
            type(prefix) is not str or not prefix.endswith("/")
            or len(prefix.encode("utf-8")) > 1024
            or any(ord(char) < 32 or ord(char) == 127 for char in prefix)
            or type(maximum_keys) is not int or not 1 <= maximum_keys <= 1000
            or type(url_encoding) is not str or url_encoding not in {"percent", "form"}
        ):
            raise ValueError("invalid page request")
        _relative_path(prefix[:-1])
        _token(continuation_token)
        root = _tree(payload)
        if root.name != "ListBucketResult":
            raise ValueError("wrong root")
        fields = _fields(root, repeated=frozenset({"Contents"}))
        allowed = {
            "Name", "Prefix", "MaxKeys", "KeyCount", "EncodingType", "IsTruncated", "Contents",
            "ContinuationToken", "NextContinuationToken", "Delimiter",
        }
        if fields.keys() - allowed:
            raise ValueError("unsupported listing fields")
        if (
            _scalar(fields["Name"]) != expected_bucket
            or _decoded(_scalar(fields["Prefix"]), url_encoding) != prefix
            or _scalar(fields["EncodingType"]) != "url"
            or _number(_scalar(fields["MaxKeys"]), 1000) != maximum_keys
            or ("Delimiter" in fields and _scalar(fields["Delimiter"]) != "")
        ):
            raise ValueError("listing request binding changed")
        echoed = _scalar(fields["ContinuationToken"]) if "ContinuationToken" in fields else None
        if (echoed or None) != continuation_token:
            raise ValueError("continuation binding changed")
        objects = []
        for child in root.children:
            if child.name != "Contents":
                continue
            values = _fields(child, repeated=frozenset({"ChecksumAlgorithm"}))
            if values.keys() - {
                "Key", "Size", "ETag", "LastModified", "StorageClass", "ChecksumAlgorithm",
                "ChecksumType", "Owner", "RestoreStatus",
            }:
                raise ValueError("unsupported object fields")
            key = _decoded(_scalar(values["Key"]), url_encoding)
            _relative_path(key)
            if (
                not key.startswith(prefix) or len(key.encode("utf-8")) > 1024
                or any(ord(char) < 32 or ord(char) == 127 for char in key)
            ):
                raise ValueError("invalid listed object")
            size = _number(_scalar(values["Size"]), 512 * 1024 * 1024)
            objects.append(TaskImageBundleObject(key=key, size_bytes=size))
        keys = [obj.key.encode("utf-8") for obj in objects]
        if (
            len(objects) > maximum_keys or len(set(keys)) != len(keys) or keys != sorted(keys)
            or _number(_scalar(fields["KeyCount"]), maximum_keys) != len(objects)
        ):
            raise ValueError("invalid object set")
        truncated = _scalar(fields["IsTruncated"])
        next_token = _scalar(fields["NextContinuationToken"]) if "NextContinuationToken" in fields else None
        next_token = _token(next_token or None)
        if (
            truncated not in {"true", "false"}
            or (truncated == "true" and (not objects or next_token is None or next_token == continuation_token))
            or (truncated == "false" and next_token is not None)
        ):
            raise ValueError("invalid page progression")
        return S3ListPage(tuple(objects), next_token)
    except (ValueError, TypeError, LookupError, AttributeError, expat.ExpatError):
        raise TaskImageBundleCapabilityError("S3 listing page is invalid or exceeds limits") from None
