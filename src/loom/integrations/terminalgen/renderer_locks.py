"""Code-owned renderer locks for TerminalGen terminal barriers."""

from __future__ import annotations

from loom.pipeline.spec import RequestRendererLockFileV1, RequestRendererLockV1

_RENDERER_PATH = "src/loom/integrations/terminalgen/stage_request.py"
_RENDERER_SHA256 = "sha256:cf64f1bb18d1996815018b8cff13306b9d6a1c03a46ec3db50f146c28388fda5"
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
