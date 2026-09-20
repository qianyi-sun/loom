"""Supported execution policy, separate from historical record schemas.

Clients can run on any host architecture. New workloads, builds and workers
execute on x86_64; historical ARM records remain parseable for inspection.
"""

from __future__ import annotations

from typing import Literal


def execution_cpu_arch(architecture: str) -> Literal["x86_64"]:
    if architecture in {"x86_64", "any"}:
        return "x86_64"
    raise ValueError(
        f"unsupported execution CPU architecture {architecture!r}: Loom supports x86_64 only. "
        "Publish an x86_64 task bundle and use an x86_64 worker; ARM clients may submit remotely."
    )
