"""Private controller TLS files bound by an exact portable activation document."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field

from loom_capacity_executor.runtime import ActivationRuntimeDocumentV2


@dataclass(frozen=True, slots=True)
class NativeDeliveryMaterial:
    ca: bytes = field(repr=False)
    certificate: bytes = field(repr=False)
    private_key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if any(type(value) is not bytes or not 64 <= len(value) <= 65536
            for value in (self.ca, self.certificate, self.private_key)):
            raise ValueError("native delivery material must be bounded private bytes")

    def files(self, document: ActivationRuntimeDocumentV2) -> Mapping[str, bytes]:
        config = document.native_delivery
        if config is None:
            raise ValueError("native delivery material has no activation binding")
        config.assert_document(document)
        pairs = ((config.identity.ca, self.ca), (config.identity.certificate, self.certificate),
            (config.identity.private_key, self.private_key))
        if any(hashlib.sha256(value).hexdigest() != pin.sha256 for pin, value in pairs):
            raise ValueError("native delivery material differs from activation hashes")
        return {pin.path: value for pin, value in pairs}

    def to_dict(self) -> dict[str, str]:
        return {name: base64.b64encode(getattr(self, name)).decode("ascii")
            for name in ("ca", "certificate", "private_key")}

    @classmethod
    def from_dict(cls, value: object) -> NativeDeliveryMaterial:
        try:
            if (not isinstance(value, dict) or set(value) != {"ca", "certificate", "private_key"}
                or any(type(item) is not str or len(item) > 90000 for item in value.values())):
                raise ValueError
            result = cls(**{name: base64.b64decode(item, validate=True) for name, item in value.items()})
            if result.to_dict() != value:
                raise ValueError
            return result
        except (ValueError, TypeError):
            raise ValueError("native delivery material record is invalid") from None
