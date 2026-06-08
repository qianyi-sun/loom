"""`python -m loom_cli` + `loom` console script entry point."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import cast

from loom_cli import __version__


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="loom",
        description=(
            "Loom CLI — ad-hoc agent evaluation. Run benchmarks against "
            "agents from a laptop with no server required."
        ),
    )
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run a benchmark or single task")
    g = p_run.add_mutually_exclusive_group(required=True)
    g.add_argument("--dataset", help="Benchmark slug (e.g. humaneval)")
    g.add_argument("--task", help="Single task id (e.g. humaneval/HumanEval-42)")
    p_run.add_argument("--split", default="test")
    p_run.add_argument("--agent", required=True,
                       help="Agent name (oracle / litellm / claude-code / ...)")
    p_run.add_argument("--model", default=None,
                       help="Model id in provider/name form, e.g. anthropic/claude-opus-4-7")
    p_run.add_argument("--backend", default="docker",
                       choices=("docker", "fake"),
                       help="Driver backend (daytona/modal land in Plans 26/27)")
    p_run.add_argument("--concurrency", type=int, default=1)
    p_run.add_argument("--output-dir", type=Path, default=Path("./runs"))
    p_run.add_argument("--json", dest="json_output", action="store_true",
                       help="Emit JSON one-result-per-line instead of text")
    p_run.add_argument("--server-url", default=None,
                       help="If set, also POST results to this Control Plane")
    p_run.set_defaults(handler=_run_handler)

    p_config = sub.add_parser("config", help="Manage CLI config")
    config_sub = p_config.add_subparsers(dest="config_cmd", required=True)
    p_config_set = config_sub.add_parser("set")
    p_config_set.add_argument("key")
    p_config_set.add_argument("value")
    config_sub.add_parser("show")
    p_config.set_defaults(handler=_config_handler)

    # `datasets` is registered as a help-only stub here so it appears in
    # `loom --help`. The real subcommand surface (list/show/install/
    # refresh-registry plus filters) is owned by loom_cli.datasets_cmd.dispatch
    # which receives raw argv from main() via a pre-route before argparse sees
    # the subcommand.
    sub.add_parser(
        "datasets",
        help="Discover datasets (list/show/install/refresh-registry)",
        add_help=False,
    )
    return p


def _run_handler(args: argparse.Namespace) -> int:
    from loom_cli.run_cmd import run

    return run(args)


def _config_handler(args: argparse.Namespace) -> int:
    from loom_cli.config_cmd import dispatch

    return dispatch(args)


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw and raw[0] == "datasets":
        from loom_cli.datasets_cmd import dispatch as datasets_dispatch
        return datasets_dispatch(raw[1:])
    parser = _build_parser()
    args = parser.parse_args(raw)
    return cast(int, args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
