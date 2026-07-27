#!/usr/bin/env python3
"""Submit-host entrypoint for the shared developer-sandbox capacity broker."""

from __future__ import annotations

from loom_control_plane.shared_capacity_broker import main

if __name__ == "__main__":
    raise SystemExit(main())
