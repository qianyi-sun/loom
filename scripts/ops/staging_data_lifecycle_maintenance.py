#!/usr/bin/env python3
"""Compatibility wrapper for the packaged lifecycle maintenance entry point."""

from __future__ import annotations

from loom.data_lifecycle_maintenance import main

if __name__ == "__main__":
    raise SystemExit(main())
