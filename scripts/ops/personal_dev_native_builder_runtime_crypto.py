"""Sealed stdlib-only Ed25519 and CA-certificate validation helpers."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime

_ED25519_FIELD = 2**255 - 19
_ED25519_D = (-121665 * pow(121666, _ED25519_FIELD - 2, _ED25519_FIELD)) % _ED25519_FIELD
_ED25519_BASE = (
    15112221349535400772501151409588531511454012693041857206046113283949847762202,
    46316835694926478169428394003475163141307993866256225615783033603165251855960,
    1,
    46827403850823179245072216630277197565144205554125654976674165829533817101731,
)
_PEM_BEGIN = b"-----BEGIN CERTIFICATE-----\n"
_PEM_END = b"-----END CERTIFICATE-----\n"
_BASIC_CONSTRAINTS_OID = b"\x55\x1d\x13"
_MAX_DER_DEPTH = 32
_NAME_STRING_TAGS = frozenset({0x0C, 0x12, 0x13, 0x14, 0x16, 0x1A, 0x1C, 0x1E})
_PRINTABLE_STRING_BYTES = frozenset(
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 '()+,-./:=?"
)


class _CertificateError(ValueError):
    pass


def _ed25519_add(
    left: tuple[int, int, int, int],
    right: tuple[int, int, int, int],
) -> tuple[int, int, int, int]:
    x1, y1, z1, t1 = left
    x2, y2, z2, t2 = right
    field = _ED25519_FIELD
    a = (y1 - x1) * (y2 - x2) % field
    b = (y1 + x1) * (y2 + x2) % field
    c = 2 * _ED25519_D * t1 * t2 % field
    d = 2 * z1 * z2 % field
    e = b - a
    f = d - c
    g = d + c
    h = b + a
    return (e * f % field, g * h % field, f * g % field, e * h % field)


def derive_ed25519_public_key(private_seed: bytes) -> bytes:
    """Return the RFC 8032 compressed public key for one 32-byte seed."""
    if not isinstance(private_seed, bytes) or len(private_seed) != 32:
        raise ValueError("invalid Ed25519 seed")
    import hashlib

    expanded = hashlib.sha512(private_seed).digest()
    scalar_bytes = bytearray(expanded[:32])
    scalar_bytes[0] &= 248
    scalar_bytes[31] &= 63
    scalar_bytes[31] |= 64
    scalar = int.from_bytes(scalar_bytes, "little")
    result = (0, 1, 1, 0)
    addend = _ED25519_BASE
    while scalar:
        if scalar & 1:
            result = _ed25519_add(result, addend)
        addend = _ed25519_add(addend, addend)
        scalar >>= 1
    x, y, z, _ = result
    inverse_z = pow(z, _ED25519_FIELD - 2, _ED25519_FIELD)
    affine_x = x * inverse_z % _ED25519_FIELD
    affine_y = y * inverse_z % _ED25519_FIELD
    encoded = affine_y | ((affine_x & 1) << 255)
    return encoded.to_bytes(32, "little")


def _read_tlv(
    payload: bytes,
    offset: int,
    limit: int,
) -> tuple[int, int, int, int, int]:
    start = offset
    if not 0 <= offset < limit <= len(payload):
        raise _CertificateError("truncated DER")
    tag = payload[offset]
    offset += 1
    if tag & 0x1F == 0x1F or offset >= limit:
        raise _CertificateError("unsupported DER tag")
    first_length = payload[offset]
    offset += 1
    if first_length < 0x80:
        length = first_length
    else:
        width = first_length & 0x7F
        if width == 0 or width > 4 or offset + width > limit:
            raise _CertificateError("invalid DER length")
        encoded_length = payload[offset : offset + width]
        if encoded_length[0] == 0:
            raise _CertificateError("noncanonical DER length")
        length = int.from_bytes(encoded_length, "big")
        if length < 0x80:
            raise _CertificateError("noncanonical DER length")
        offset += width
    end = offset + length
    if end > limit:
        raise _CertificateError("truncated DER value")
    return tag, start, offset, end, end


def _items(payload: bytes, start: int, end: int) -> list[tuple[int, int, int, int, int]]:
    values: list[tuple[int, int, int, int, int]] = []
    offset = start
    while offset < end:
        value = _read_tlv(payload, offset, end)
        values.append(value)
        offset = value[4]
    if offset != end:
        raise _CertificateError("invalid DER sequence")
    return values


def _validate_integer(payload: bytes, start: int, end: int) -> int:
    value = payload[start:end]
    if (
        not value
        or (len(value) > 1 and value[0] == 0 and value[1] < 0x80)
        or (len(value) > 1 and value[0] == 0xFF and value[1] >= 0x80)
    ):
        raise _CertificateError("noncanonical DER integer")
    return int.from_bytes(value, "big", signed=True)


def _validate_bit_string(payload: bytes, start: int, end: int) -> None:
    if start >= end or payload[start] > 7:
        raise _CertificateError("invalid DER bit string")
    unused = payload[start]
    bits = payload[start + 1 : end]
    if (unused and not bits) or (unused and bits[-1] & ((1 << unused) - 1)):
        raise _CertificateError("noncanonical DER bit string")


def _decode_oid(payload: bytes, start: int, end: int) -> bytes:
    encoded = payload[start:end]
    if not encoded:
        raise _CertificateError("empty DER OID")
    component_start = True
    for octet in encoded:
        if component_start and octet == 0x80:
            raise _CertificateError("noncanonical DER OID")
        component_start = not bool(octet & 0x80)
    if not component_start:
        raise _CertificateError("truncated DER OID")
    return encoded


def _validate_tree(
    payload: bytes,
    value: tuple[int, int, int, int, int],
    *,
    depth: int = 0,
) -> None:
    if depth > _MAX_DER_DEPTH:
        raise _CertificateError("DER nesting too deep")
    tag, _, content_start, content_end, _ = value
    tag_class = tag & 0xC0
    constructed = bool(tag & 0x20)
    number = tag & 0x1F
    if constructed:
        if tag_class == 0 and number not in {16, 17}:
            raise _CertificateError("invalid DER constructed value")
        for child in _items(payload, content_start, content_end):
            _validate_tree(payload, child, depth=depth + 1)
        return
    if tag_class != 0:
        return
    if number in {16, 17}:
        raise _CertificateError("invalid DER primitive value")
    if number == 1:
        if payload[content_start:content_end] not in {b"\x00", b"\xff"}:
            raise _CertificateError("invalid DER boolean")
    elif number == 2:
        _validate_integer(payload, content_start, content_end)
    elif number == 3:
        _validate_bit_string(payload, content_start, content_end)
    elif number == 5 and content_start != content_end:
        raise _CertificateError("invalid DER null")
    elif number == 6:
        _decode_oid(payload, content_start, content_end)


def _expect_tag(value: tuple[int, int, int, int, int], tag: int) -> None:
    if value[0] != tag:
        raise _CertificateError("unexpected DER tag")


def _validate_algorithm(
    payload: bytes,
    value: tuple[int, int, int, int, int],
) -> bytes:
    _expect_tag(value, 0x30)
    children = _items(payload, value[2], value[3])
    if not 1 <= len(children) <= 2:
        raise _CertificateError("invalid algorithm identifier")
    _expect_tag(children[0], 0x06)
    _decode_oid(payload, children[0][2], children[0][3])
    if len(children) == 2:
        _validate_tree(payload, children[1])
    return payload[value[1] : value[4]]


def _validate_name(
    payload: bytes,
    value: tuple[int, int, int, int, int],
) -> None:
    _expect_tag(value, 0x30)
    for relative_name in _items(payload, value[2], value[3]):
        _expect_tag(relative_name, 0x31)
        attributes = _items(payload, relative_name[2], relative_name[3])
        if not attributes:
            raise _CertificateError("empty relative distinguished name")
        encoded_attributes = [payload[item[1] : item[4]] for item in attributes]
        if encoded_attributes != sorted(encoded_attributes):
            raise _CertificateError("noncanonical relative distinguished name")
        for attribute in attributes:
            _expect_tag(attribute, 0x30)
            fields = _items(payload, attribute[2], attribute[3])
            if len(fields) != 2:
                raise _CertificateError("invalid name attribute")
            _expect_tag(fields[0], 0x06)
            _decode_oid(payload, fields[0][2], fields[0][3])
            _validate_name_value(payload, fields[1])


def _validate_name_value(
    payload: bytes,
    value: tuple[int, int, int, int, int],
) -> None:
    tag, _, start, end, _ = value
    if tag not in _NAME_STRING_TAGS:
        raise _CertificateError("invalid name attribute value")
    encoded = payload[start:end]
    try:
        if tag == 0x0C:
            encoded.decode("utf-8")
        elif tag == 0x12 and any(octet not in b" 0123456789" for octet in encoded):
            raise _CertificateError("invalid numeric string")
        elif tag == 0x13 and any(
            octet not in _PRINTABLE_STRING_BYTES for octet in encoded
        ):
            raise _CertificateError("invalid printable string")
        elif tag == 0x16 and any(octet >= 0x80 for octet in encoded):
            raise _CertificateError("invalid IA5 string")
        elif tag == 0x1A and any(not 0x20 <= octet <= 0x7E for octet in encoded):
            raise _CertificateError("invalid visible string")
        elif tag == 0x1C:
            encoded.decode("utf-32-be")
        elif tag == 0x1E:
            decoded = encoded.decode("utf-16-be")
            if any(0xD800 <= ord(character) <= 0xDFFF for character in decoded):
                raise _CertificateError("invalid BMP string")
    except UnicodeDecodeError as exc:
        raise _CertificateError("invalid name attribute value") from exc


def _validate_time(payload: bytes, value: tuple[int, int, int, int, int]) -> None:
    tag, _, start, end, _ = value
    encoded = payload[start:end]
    expected = 13 if tag == 0x17 else 15 if tag == 0x18 else 0
    if (
        len(encoded) != expected
        or not encoded.endswith(b"Z")
        or not encoded[:-1].isdigit()
    ):
        raise _CertificateError("invalid certificate time")
    digits = encoded[:-1]
    if tag == 0x17:
        short_year = int(digits[0:2])
        year = 2000 + short_year if short_year < 50 else 1900 + short_year
        offset = 2
    else:
        year = int(digits[0:4])
        offset = 4
    try:
        datetime(
            year,
            int(digits[offset : offset + 2]),
            int(digits[offset + 2 : offset + 4]),
            int(digits[offset + 4 : offset + 6]),
            int(digits[offset + 6 : offset + 8]),
            int(digits[offset + 8 : offset + 10]),
        )
    except ValueError as exc:
        raise _CertificateError("invalid certificate time") from exc


def _validate_extension_value(payload: bytes) -> None:
    value = _read_tlv(payload, 0, len(payload))
    if value[4] != len(payload):
        raise _CertificateError("trailing extension data")
    _validate_tree(payload, value)


def _basic_constraints_is_ca(payload: bytes) -> bool:
    outer = _read_tlv(payload, 0, len(payload))
    _expect_tag(outer, 0x30)
    if outer[4] != len(payload):
        raise _CertificateError("trailing basic constraints data")
    children = _items(payload, outer[2], outer[3])
    if len(children) > 2:
        raise _CertificateError("invalid basic constraints")
    is_ca = False
    offset = 0
    if children and children[0][0] == 0x01:
        boolean = payload[children[0][2] : children[0][3]]
        if boolean != b"\xff":
            raise _CertificateError("noncanonical basic constraints")
        is_ca = True
        offset = 1
    if offset < len(children):
        _expect_tag(children[offset], 0x02)
        if _validate_integer(payload, children[offset][2], children[offset][3]) < 0:
            raise _CertificateError("invalid path length")
        if not is_ca:
            raise _CertificateError("path length without CA")
        offset += 1
    if offset != len(children):
        raise _CertificateError("invalid basic constraints")
    return is_ca


def _extensions_are_ca(
    payload: bytes,
    value: tuple[int, int, int, int, int],
) -> bool:
    _expect_tag(value, 0xA3)
    explicit = _items(payload, value[2], value[3])
    if len(explicit) != 1:
        raise _CertificateError("invalid extensions wrapper")
    _expect_tag(explicit[0], 0x30)
    seen: set[bytes] = set()
    basic_constraints: bool | None = None
    for extension in _items(payload, explicit[0][2], explicit[0][3]):
        _expect_tag(extension, 0x30)
        fields = _items(payload, extension[2], extension[3])
        if len(fields) not in {2, 3}:
            raise _CertificateError("invalid extension")
        _expect_tag(fields[0], 0x06)
        oid = _decode_oid(payload, fields[0][2], fields[0][3])
        if oid in seen:
            raise _CertificateError("duplicate extension")
        seen.add(oid)
        value_index = 1
        if fields[1][0] == 0x01:
            if payload[fields[1][2] : fields[1][3]] != b"\xff":
                raise _CertificateError("noncanonical extension criticality")
            value_index = 2
        if value_index != len(fields) - 1:
            raise _CertificateError("invalid extension")
        _expect_tag(fields[value_index], 0x04)
        extension_payload = payload[fields[value_index][2] : fields[value_index][3]]
        _validate_extension_value(extension_payload)
        if oid == _BASIC_CONSTRAINTS_OID:
            basic_constraints = _basic_constraints_is_ca(extension_payload)
    return basic_constraints is True


def _certificate_is_ca(payload: bytes) -> bool:
    certificate = _read_tlv(payload, 0, len(payload))
    _expect_tag(certificate, 0x30)
    if certificate[4] != len(payload):
        raise _CertificateError("trailing certificate data")
    fields = _items(payload, certificate[2], certificate[3])
    if len(fields) != 3:
        raise _CertificateError("invalid certificate")
    _expect_tag(fields[0], 0x30)
    outer_algorithm = _validate_algorithm(payload, fields[1])
    _expect_tag(fields[2], 0x03)
    _validate_bit_string(payload, fields[2][2], fields[2][3])

    tbs = _items(payload, fields[0][2], fields[0][3])
    offset = 0
    version = 0
    if tbs and tbs[0][0] == 0xA0:
        version_fields = _items(payload, tbs[0][2], tbs[0][3])
        if len(version_fields) != 1:
            raise _CertificateError("invalid certificate version")
        _expect_tag(version_fields[0], 0x02)
        version = _validate_integer(payload, version_fields[0][2], version_fields[0][3])
        if version not in {0, 1, 2}:
            raise _CertificateError("invalid certificate version")
        offset += 1
    if len(tbs) < offset + 6:
        raise _CertificateError("truncated certificate body")
    serial = tbs[offset]
    _expect_tag(serial, 0x02)
    serial_value = _validate_integer(payload, serial[2], serial[3])
    if serial_value <= 0 or serial[3] - serial[2] > 21:
        raise _CertificateError("invalid certificate serial")
    inner_algorithm = _validate_algorithm(payload, tbs[offset + 1])
    if inner_algorithm != outer_algorithm:
        raise _CertificateError("signature algorithms differ")
    for name in (tbs[offset + 2], tbs[offset + 4]):
        _validate_name(payload, name)
    validity = tbs[offset + 3]
    _expect_tag(validity, 0x30)
    times = _items(payload, validity[2], validity[3])
    if len(times) != 2:
        raise _CertificateError("invalid certificate validity")
    for time_value in times:
        _validate_time(payload, time_value)
    subject_public_key = tbs[offset + 5]
    _expect_tag(subject_public_key, 0x30)
    public_key_fields = _items(payload, subject_public_key[2], subject_public_key[3])
    if len(public_key_fields) != 2:
        raise _CertificateError("invalid subject public key")
    _validate_algorithm(payload, public_key_fields[0])
    _expect_tag(public_key_fields[1], 0x03)
    _validate_bit_string(payload, public_key_fields[1][2], public_key_fields[1][3])

    extensions: tuple[int, int, int, int, int] | None = None
    seen_optional: set[int] = set()
    for optional in tbs[offset + 6 :]:
        tag = optional[0]
        if tag not in {0x81, 0x82, 0xA3} or tag in seen_optional:
            raise _CertificateError("invalid optional certificate field")
        if tag == 0x81 and (0x82 in seen_optional or 0xA3 in seen_optional):
            raise _CertificateError("misordered certificate field")
        if tag == 0x82 and 0xA3 in seen_optional:
            raise _CertificateError("misordered certificate field")
        seen_optional.add(tag)
        if tag in {0x81, 0x82}:
            _validate_bit_string(payload, optional[2], optional[3])
        else:
            extensions = optional
    if extensions is None or version != 2:
        return False
    return _extensions_are_ca(payload, extensions)


def _pem_certificates(payload: bytes) -> list[bytes]:
    certificates: list[bytes] = []
    offset = 0
    while offset < len(payload):
        if not payload.startswith(_PEM_BEGIN, offset):
            raise _CertificateError("non-certificate PEM data")
        body_start = offset + len(_PEM_BEGIN)
        footer = payload.find(_PEM_END, body_start)
        if footer < 0:
            raise _CertificateError("truncated PEM certificate")
        body = payload[body_start:footer]
        if not body.endswith(b"\n"):
            raise _CertificateError("noncanonical PEM certificate")
        encoded = b"".join(body.splitlines())
        try:
            der = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise _CertificateError("invalid PEM certificate") from exc
        if not der or base64.b64encode(der) != encoded:
            raise _CertificateError("noncanonical PEM certificate")
        canonical_body = b"\n".join(
            encoded[index : index + 64] for index in range(0, len(encoded), 64)
        ) + b"\n"
        end = footer + len(_PEM_END)
        if payload[offset:end] != _PEM_BEGIN + canonical_body + _PEM_END:
            raise _CertificateError("noncanonical PEM certificate")
        certificates.append(der)
        offset = end
    if not certificates:
        raise _CertificateError("empty certificate bundle")
    return certificates


def is_ca_certificate_bundle(payload: bytes) -> bool:
    """Return whether payload is exact canonical PEM containing only CA certificates."""
    if not isinstance(payload, bytes):
        return False
    try:
        return all(_certificate_is_ca(certificate) for certificate in _pem_certificates(payload))
    except (IndexError, OverflowError, ValueError):
        return False


__all__ = ["derive_ed25519_public_key", "is_ca_certificate_bundle"]
