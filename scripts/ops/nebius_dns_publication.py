"""Fixed personal/management A-record publication; no generic DNS mutation API."""
from __future__ import annotations


class PublicationError(RuntimeError):
    """Fixed diagnostic; preserve private journals and never expose credentials."""


def publish_dns(provider, *, target, state_dir, qualify, wait):
    raise NotImplementedError
