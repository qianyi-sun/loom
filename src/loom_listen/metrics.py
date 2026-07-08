"""Shared Prometheus metrics for LISTEN/NOTIFY watchers (#609).

Centralised here so multiple watcher modules can label the same
gauge without each re-registering a duplicate metric name.
"""
from __future__ import annotations

from prometheus_client import Gauge

PUSH_MODE_GAUGE: Gauge = Gauge(
    "loom_listen_watcher_push_mode",
    "1 when NOTIFY-push mode is active; 0 when watcher fell back to poll-only.",
    labelnames=["watcher"],
)
