"""Shared utilities for Postgres LISTEN/NOTIFY watchers (#609).

Provides a self-test primitive that watchers invoke at startup to
verify their connection preserves session semantics (i.e., is not
routed through pgbouncer transaction mode which would silently drop
NOTIFY delivery).
"""
