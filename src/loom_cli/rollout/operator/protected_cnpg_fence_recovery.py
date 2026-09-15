"""Non-secret fence request/identity records, subordinate to an active apply.

These records do not install admission policies, prove their enforcement, or
authorize adoption/removal. The protected lifecycle must independently verify
live objects and exclude policy writers; a recorded UID is never quiescence.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass

from .protected_cnpg_input_fence import render_cnpg_input_fence


@dataclass(frozen=True, slots=True)
class CNPGFenceRequest:
    intent_digest: str
    target_pooler_names: tuple[str, ...]

    def __post_init__(self) -> None:
        self.documents()

    def documents(self) -> tuple[dict[str, object], ...]:
        return render_cnpg_input_fence(
            intent_digest=self.intent_digest, target_pooler_names=self.target_pooler_names,
        )

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 2, **asdict(self), "target_pooler_names": list(self.target_pooler_names),
                "documents_sha256": self.documents_sha256()}

    def documents_sha256(self) -> str:
        return hashlib.sha256(json.dumps(self.documents(), sort_keys=True,
                                         separators=(",", ":"), allow_nan=False).encode()).hexdigest()

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CNPGFenceRequest:
        names = value.get("target_pooler_names")
        # Legacy schema 1 permitted restart beneath a database-backed guard.
        # Never reinterpret its receipts or upgrade its live policies in place.
        if (set(value) != {"schema_version", "intent_digest", "target_pooler_names", "documents_sha256"}
                or type(value["schema_version"]) is not int or value["schema_version"] != 2
                or not isinstance(names, list) or any(not isinstance(v, str) for v in names)):
            raise ValueError("CNPG fence request fields are invalid")
        request = cls(_string(value, "intent_digest"), tuple(names))
        if _string(value, "documents_sha256") != request.documents_sha256():
            raise ValueError("CNPG fence rendered request changed")
        return request

    def document_sha256(self, ordinal: int) -> str:
        documents = self.documents()
        if type(ordinal) is not int or not 0 <= ordinal < len(documents):
            raise ValueError("CNPG fence object ordinal is invalid")
        return hashlib.sha256(json.dumps(documents[ordinal], sort_keys=True,
                                         separators=(",", ":"), allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CNPGFenceObjectReceipt:
    intent_digest: str
    ordinal: int
    uid: str
    document_sha256: str

    def __post_init__(self) -> None:
        if (type(self.ordinal) is not int or not 0 <= self.ordinal < 10
                or not isinstance(self.uid, str)
                or re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", self.uid) is None
                or any(not isinstance(v, str) or re.fullmatch(r"[0-9a-f]{64}", v) is None
                       for v in (self.intent_digest, self.document_sha256))):
            raise ValueError("CNPG fence object receipt is invalid")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CNPGFenceObjectReceipt:
        ordinal = value.get("ordinal")
        if (set(value) != {"schema_version", "intent_digest", "ordinal", "uid", "document_sha256"}
                or type(value["schema_version"]) is not int or value["schema_version"] != 1
                or type(ordinal) is not int):
            raise ValueError("CNPG fence object receipt fields are invalid")
        return cls(_string(value, "intent_digest"), ordinal, _string(value, "uid"),
                   _string(value, "document_sha256"))


@dataclass(frozen=True, slots=True)
class CNPGFenceCreateIntent:
    intent_digest: str
    ordinal: int
    nonce: str
    document_sha256: str
    creation_document_sha256: str

    def __post_init__(self) -> None:
        if (type(self.ordinal) is not int or not 0 <= self.ordinal < 10
                or not isinstance(self.nonce, str) or re.fullmatch(r"[0-9a-f]{32}", self.nonce) is None
                or any(not isinstance(v, str) or re.fullmatch(r"[0-9a-f]{64}", v) is None
                       for v in (self.intent_digest, self.document_sha256, self.creation_document_sha256))):
            raise ValueError("CNPG fence create intent is invalid")

    @classmethod
    def prepare(cls, request: CNPGFenceRequest, *, ordinal: int, nonce: str) -> CNPGFenceCreateIntent:
        document = _creation_document(request, ordinal=ordinal, nonce=nonce)
        return cls(request.intent_digest, ordinal, nonce, request.document_sha256(ordinal), _hash(document))

    def document(self, request: CNPGFenceRequest) -> dict[str, object]:
        document = _creation_document(request, ordinal=self.ordinal, nonce=self.nonce)
        if (self.intent_digest != request.intent_digest
                or self.document_sha256 != request.document_sha256(self.ordinal)
                or self.creation_document_sha256 != _hash(document)):
            raise ValueError("CNPG fence create intent binding changed")
        return document

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "observed_state": "absent", **asdict(self)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CNPGFenceCreateIntent:
        ordinal = value.get("ordinal")
        if (set(value) != {"schema_version", "observed_state", "intent_digest", "ordinal", "nonce",
                          "document_sha256", "creation_document_sha256"}
                or type(value["schema_version"]) is not int or value["schema_version"] != 1
                or value["observed_state"] != "absent" or type(ordinal) is not int):
            raise ValueError("CNPG fence create intent fields are invalid")
        return cls(_string(value, "intent_digest"), ordinal, _string(value, "nonce"),
                   _string(value, "document_sha256"), _string(value, "creation_document_sha256"))


def _creation_document(request: CNPGFenceRequest, *, ordinal: int, nonce: str) -> dict[str, object]:
    request.document_sha256(ordinal)  # Validate the index before selecting it.
    if not isinstance(nonce, str) or re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
        raise ValueError("CNPG fence create nonce is invalid")
    document = request.documents()[ordinal]
    metadata = document["metadata"]
    assert isinstance(metadata, dict) and isinstance(metadata["annotations"], dict)
    metadata["annotations"]["loom.dev/fence-create-nonce"] = nonce
    return document


def _hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(json.dumps(dict(value), sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _string(value: Mapping[str, object], key: str) -> str:
    item = value[key]
    if not isinstance(item, str):
        raise ValueError("CNPG fence record string is invalid")
    return item
