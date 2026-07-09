"""Runtime provenance constants for Harbor-embedded Terminus-2 (#744)."""

from __future__ import annotations

HARBOR_COMPAT_SHA = "527d50deb63a5d279e8c20593c18a2cbc7f61f9e"
HARBOR_RUNTIME_VERSION = "0.18.0"
LOOM_BRIDGE_REVISION = "1.0"


def harbor_template_hashes() -> dict[str, str]:
    """Best-effort template hashes from the installed Harbor package."""
    try:
        import hashlib
        from importlib.resources import files

        templates = files("harbor.agents.terminus_2.templates")
        out: dict[str, str] = {}
        for name in ("terminus-json-plain.txt", "timeout.txt"):
            try:
                data = (templates / name).read_bytes()
            except (FileNotFoundError, OSError, TypeError):
                continue
            out[name] = hashlib.sha256(data).hexdigest()
        return out
    except Exception:
        return {}
