#!/usr/bin/env python3
"""Normalize a single-line secret before mounting it into a workload."""

from __future__ import annotations

import argparse
from pathlib import Path


def normalize_printable_ascii_secret(source: bytes) -> bytes:
    normalized = source.rstrip(b"\r\n")
    if not normalized or any(byte < 0x21 or byte > 0x7E for byte in normalized):
        raise ValueError("secret must contain one printable ASCII value")
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    args.destination.write_bytes(
        normalize_printable_ascii_secret(args.source.read_bytes())
    )


if __name__ == "__main__":
    main()
