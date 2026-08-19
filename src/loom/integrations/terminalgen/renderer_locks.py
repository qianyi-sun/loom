"""Code-owned renderer locks for TerminalGen terminal barriers."""

from __future__ import annotations

from loom.pipeline.spec import RequestRendererLockFileV1, RequestRendererLockV1

_RENDERER_PATH = "src/loom/integrations/terminalgen/stage_request.py"
_RENDERER_SHA256 = "sha256:d12fbff4ce83dabdb39dd2b034a909515bc0bc6d9795790ef8216a3c70965661"
_NAMES = (
    "terminalgen_authoring_package",
    "terminalgen_card_finalize",
    "terminalgen_global_finalize",
    "terminalgen_plan_audit",
    "terminalgen_runtime_package",
    "terminalgen_stage_request",
)


def terminalgen_renderer_locks() -> tuple[RequestRendererLockV1, ...]:
    return tuple(
        RequestRendererLockV1(
            name=name,
            version=1,
            entrypoint="loom.integrations.terminalgen.stage_request:render",
            files=[
                RequestRendererLockFileV1(
                    repo_path=_RENDERER_PATH,
                    sha256=_RENDERER_SHA256,
                )
            ],
        )
        for name in _NAMES
    )


__all__ = ["terminalgen_renderer_locks"]
