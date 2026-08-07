#!/usr/bin/env python3
"""Reconcile trusted oldlab-first CI route requests and lease releases."""

from __future__ import annotations

from loom_control_plane.ci_runner_route_controller import main

if __name__ == "__main__":
    raise SystemExit(main())
