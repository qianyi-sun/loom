"""Ed25519 ownership proofs for executor inventory classification."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from collections.abc import Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import (
    ExecutableOwnershipMetadataV2,
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
)
from loom_capacity_manager.grant_contracts import (
    OwnershipMetadataV1,
    SignedOwnershipProofV1,
)

_KEY_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,127}")
_MAX_OWNERSHIP_KEYS = 256


class OwnershipKeyringError(ValueError):
    """Trusted ownership verification-key configuration is invalid."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise OwnershipKeyringError("ownership keyring contains duplicate JSON fields")
        result[key] = value
    return result


def _public_key_bytes(public_key: Ed25519PublicKey) -> bytes:
    return public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def public_key_fingerprint(public_key: Ed25519PublicKey) -> str:
    """Return the registration-safe SHA-256 fingerprint of one public key."""

    return hashlib.sha256(_public_key_bytes(public_key)).hexdigest()


def sign_ownership(
    private_key: Ed25519PrivateKey,
    *,
    signing_key_id: str,
    metadata: OwnershipMetadataV1,
) -> SignedOwnershipProofV1:
    """Sign only the canonical immutable metadata, never executor-local JSON."""

    signature = private_key.sign(canonical_bytes(metadata))
    return SignedOwnershipProofV1(
        metadata=metadata,
        signing_key_id=signing_key_id,
        signature_base64=base64.b64encode(signature).decode("ascii"),
    )


def sign_executable_ownership(
    private_key: Ed25519PrivateKey,
    *,
    signing_key_id: str,
    metadata: ExecutableOwnershipMetadataV2,
) -> SignedExecutableOwnershipProofV2:
    """Sign the complete canonical executable-v2 ownership metadata."""

    signature = private_key.sign(canonical_executable_bytes(metadata))
    return SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id=signing_key_id,
        signature_base64=base64.b64encode(signature).decode("ascii"),
    )


class OwnershipKeyring:
    """Bounded public verification keys supplied by trusted operator config."""

    def __init__(self, keys: Mapping[str, Ed25519PublicKey] | None = None) -> None:
        resolved = dict(keys or {})
        if len(resolved) > _MAX_OWNERSHIP_KEYS:
            raise OwnershipKeyringError("ownership keyring exceeds its key bound")
        fingerprints: set[str] = set()
        for key_id, public_key in resolved.items():
            if (
                not isinstance(key_id, str)
                or _KEY_ID_RE.fullmatch(key_id) is None
                or not isinstance(public_key, Ed25519PublicKey)
            ):
                raise OwnershipKeyringError("ownership key binding is invalid")
            fingerprint = public_key_fingerprint(public_key)
            if fingerprint in fingerprints:
                raise OwnershipKeyringError("duplicate ownership public key")
            fingerprints.add(fingerprint)
        self._keys = resolved

    @classmethod
    def from_json(cls, raw: str) -> OwnershipKeyring:
        """Load a strict bounded public-key registry with no private material."""

        try:
            document = json.loads(raw, object_pairs_hook=_unique_object)
        except json.JSONDecodeError as exc:
            raise OwnershipKeyringError("ownership keyring is invalid JSON") from exc
        if (
            not isinstance(document, dict)
            or set(document) != {"schema_version", "keys"}
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or not isinstance(document["keys"], list)
        ):
            raise OwnershipKeyringError("ownership keyring fields are invalid")
        entries = document["keys"]
        if len(entries) > _MAX_OWNERSHIP_KEYS:
            raise OwnershipKeyringError("ownership keyring exceeds its key bound")
        keys: dict[str, Ed25519PublicKey] = {}
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {
                "signing_key_id",
                "public_key_base64",
            }:
                raise OwnershipKeyringError("ownership key fields are invalid")
            key_id = entry["signing_key_id"]
            encoded = entry["public_key_base64"]
            if (
                not isinstance(key_id, str)
                or _KEY_ID_RE.fullmatch(key_id) is None
                or not isinstance(encoded, str)
            ):
                raise OwnershipKeyringError("ownership key binding is invalid")
            if key_id in keys:
                raise OwnershipKeyringError("duplicate ownership signing key")
            try:
                key_bytes = base64.b64decode(encoded, validate=True)
                if len(key_bytes) != 32 or base64.b64encode(key_bytes).decode("ascii") != encoded:
                    raise ValueError
                public_key = Ed25519PublicKey.from_public_bytes(key_bytes)
            except (ValueError, binascii.Error) as exc:
                raise OwnershipKeyringError("ownership public key is invalid") from exc
            keys[key_id] = public_key
        return cls(keys)

    def matches(self, signing_key_id: str, public_key_sha256: str) -> bool:
        """Return whether one registration names an exact configured trust root."""

        public_key = self._keys.get(signing_key_id)
        return public_key is not None and hmac.compare_digest(
            public_key_fingerprint(public_key),
            public_key_sha256,
        )

    def verify(
        self,
        proof: SignedOwnershipProofV1,
        *,
        expected_public_key_sha256: str,
    ) -> bool:
        public_key = self._keys.get(proof.signing_key_id)
        if public_key is None or not self.matches(
            proof.signing_key_id,
            expected_public_key_sha256,
        ):
            return False
        try:
            signature = base64.b64decode(proof.signature_base64, validate=True)
            public_key.verify(signature, canonical_bytes(proof.metadata))
        except (InvalidSignature, ValueError, binascii.Error):
            return False
        return True

    def verify_executable(
        self,
        proof: SignedExecutableOwnershipProofV2,
        *,
        expected_public_key_sha256: str,
    ) -> bool:
        """Verify an executable-v2 proof against the same configured trust roots."""

        public_key = self._keys.get(proof.signing_key_id)
        if public_key is None or not self.matches(
            proof.signing_key_id,
            expected_public_key_sha256,
        ):
            return False
        try:
            signature = base64.b64decode(proof.signature_base64, validate=True)
            public_key.verify(signature, canonical_executable_bytes(proof.metadata))
        except (InvalidSignature, ValueError, binascii.Error):
            return False
        return True


def verify_executable_ownership(
    proof: SignedExecutableOwnershipProofV2,
    *,
    keyring: OwnershipKeyring,
    expected_public_key_sha256: str,
) -> bool:
    """Verify one v2 proof only against its exact registered controller key."""

    if not isinstance(keyring, OwnershipKeyring):
        return False
    return keyring.verify_executable(
        proof,
        expected_public_key_sha256=expected_public_key_sha256,
    )


__all__ = [
    "OwnershipKeyring",
    "OwnershipKeyringError",
    "public_key_fingerprint",
    "sign_executable_ownership",
    "sign_ownership",
    "verify_executable_ownership",
]
