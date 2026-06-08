"""`loom datasets <subcommand>` — discovery + install commands.

Subcommands:
- list [--installed | --available | --remote] [--json]
- show <slug>
- install <slug>
- refresh-registry
"""

from __future__ import annotations

import argparse
import sys

from loom_cli import builtin as builtin_mod
from loom_cli import install as install_mod
from loom_cli import registry as registry_mod
from loom_cli import remote as remote_mod
from loom_cli.discovery import DatasetEntry, union_entries
from loom_cli.output import render_datasets_json, render_datasets_table


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loom datasets")
    sub = p.add_subparsers(dest="subcmd", required=True)

    p_list = sub.add_parser("list")
    filt = p_list.add_mutually_exclusive_group()
    filt.add_argument("--installed", action="store_true")
    filt.add_argument("--available", action="store_true")
    filt.add_argument("--remote", action="store_true")
    p_list.add_argument("--json", action="store_true", dest="as_json")
    p_list.add_argument("--registry-url", default=None)
    p_list.add_argument("--server-url", default=None)
    p_list.add_argument("--token", default=None)

    p_show = sub.add_parser("show")
    p_show.add_argument("slug")
    p_show.add_argument("--registry-url", default=None)
    p_show.add_argument("--server-url", default=None)
    p_show.add_argument("--token", default=None)

    p_install = sub.add_parser("install")
    p_install.add_argument("slug")
    p_install.add_argument("--registry-url", default=None)

    sub.add_parser("refresh-registry")

    return p


def _gather(
    *,
    only: str | None,
    registry_url: str | None,
    server_url: str | None,
    token: str | None,
) -> list[DatasetEntry]:
    builtin = builtin_mod.load_builtin_entries() if only in (None, "installed") else []
    if only in (None, "available"):
        try:
            registry = registry_mod.load_registry_entries(url=registry_url)
        except registry_mod.RegistryFetchError as exc:
            print(f"warning: registry fetch failed: {exc}", file=sys.stderr)
            registry = []
    else:
        registry = []
    remote = (
        remote_mod.load_remote_entries(server_url=server_url, token=token)
        if only in (None, "remote") else []
    )
    return union_entries(builtin=builtin, registry=registry, remote=remote)


def _cmd_list(args: argparse.Namespace) -> int:
    only: str | None = None
    if args.installed:
        only = "installed"
    elif args.available:
        only = "available"
    elif args.remote:
        only = "remote"
    entries = _gather(
        only=only, registry_url=args.registry_url,
        server_url=args.server_url, token=args.token,
    )
    if args.as_json:
        print(render_datasets_json(entries))
    else:
        print(render_datasets_table(entries))
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    entries = _gather(
        only=None, registry_url=args.registry_url,
        server_url=args.server_url, token=args.token,
    )
    match = next((e for e in entries if e.slug == args.slug), None)
    if match is None:
        print(f"error: dataset {args.slug!r} not found", file=sys.stderr)
        return 2
    print(f"slug:           {match.slug}")
    print(f"display_name:   {match.display_name}")
    print(f"source:         {match.source}")
    print(f"status:         {match.status}")
    print(f"license:        {match.license_spdx}")
    print(f"license_url:    {match.license_url}")
    print(f"task_count:     {match.task_count if match.task_count is not None else '-'}")
    if match.entry_point:
        print(f"entry_point:    {match.entry_point}")
    if match.available_pip_spec:
        print(f"pip_spec:       {match.available_pip_spec}")
    return 0


def _cmd_install(args: argparse.Namespace) -> int:
    entries = _gather(
        only=None, registry_url=args.registry_url,
        server_url=None, token=None,
    )
    match = next((e for e in entries if e.slug == args.slug), None)
    if match is None or not match.available_pip_spec:
        print(
            f"error: dataset {args.slug!r} not found in registry "
            "(no pip spec available)",
            file=sys.stderr,
        )
        return 2
    try:
        output = install_mod.install_dataset(pip_spec=match.available_pip_spec)
    except install_mod.InstallError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(output)
    return 0


def _cmd_refresh(_args: argparse.Namespace) -> int:
    registry_mod.purge_registry_cache()
    print("registry cache purged")
    return 0


def dispatch(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.subcmd == "list":
        return _cmd_list(args)
    if args.subcmd == "show":
        return _cmd_show(args)
    if args.subcmd == "install":
        return _cmd_install(args)
    if args.subcmd == "refresh-registry":
        return _cmd_refresh(args)
    parser.error(f"unknown subcommand: {args.subcmd}")  # raises SystemExit
