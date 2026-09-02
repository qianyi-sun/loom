from __future__ import annotations

import pytest
from scripts.ops.normalize_secret_file import normalize_printable_ascii_secret


@pytest.mark.parametrize("terminator", [b"", b"\n", b"\r\n", b"\n\n"])
def test_normalizes_line_terminators(terminator: bytes) -> None:
    assert normalize_printable_ascii_secret(b"provider-key" + terminator) == b"provider-key"


@pytest.mark.parametrize("value", [b"", b"\n", b"bad key", b"bad\tkey", b"bad\nkey"])
def test_rejects_empty_or_control_character_values(value: bytes) -> None:
    with pytest.raises(ValueError, match="one printable ASCII value"):
        normalize_printable_ascii_secret(value)
