"""SubcommandStep — a step that shells out to a single command (#340).

The most common step pattern in the rollout driver: invoke one of the
existing ``loom cluster ...`` subcommands (or another well-defined CLI),
capture stdout/stderr into the evidence dir, and translate the exit code
into a :class:`RunResult`.

Subclasses either override :meth:`argv` to produce their own command,
or set ``argv_template`` as a class-level list of tokens with ``{key}``
placeholders that get formatted against the RolloutContext at run time.
"""

from __future__ import annotations

from collections.abc import Sequence

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.subprocess_util import (
    format_command,
    run_captured,
)


class SubcommandStep(BaseStep):
    """Run a fixed shell command; success == exit 0."""

    #: Timeout in seconds. Subclasses may override; defaults to unbounded.
    timeout_sec: float | None = None

    def argv(self, ctx: RolloutContext, step_dir: StepDir) -> Sequence[str]:
        raise NotImplementedError(
            f"step {self.name!r}: subclass must implement argv() "
            "or override _run_impl directly"
        )

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        cmd = list(self.argv(ctx, step_dir))
        result = run_captured(
            cmd,
            stdout_log=step_dir.stdout_path(),
            stderr_log=step_dir.stderr_path(),
            timeout_sec=self.timeout_sec,
        )
        if result.returncode == 0:
            return RunResult(
                exit_code=0,
                summary=(
                    f"{format_command(cmd)} exited 0"
                ),
            )
        return RunResult(
            exit_code=result.returncode,
            summary=f"{format_command(cmd)} exited {result.returncode}",
            error=(
                result.stderr.strip().splitlines()[-1]
                if result.stderr.strip() else
                f"non-zero exit: {result.returncode}"
            ),
        )
