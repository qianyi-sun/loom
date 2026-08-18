#!/usr/bin/env python3
"""Compatibility entry point for the packaged capacity executor controller."""

from loom_capacity_pool_controller.runtime import main


if __name__ == "__main__":
    raise SystemExit(main())
