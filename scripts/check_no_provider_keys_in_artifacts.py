#!/usr/bin/env python3
"""Audit script: scan a trial's artifacts for provider-secret leakage.

Acceptance proof for #78 Phase D: a sandbox-isolated trial MUST NOT
expose the team's BYO provider API keys ANYWHERE the team can
exfiltrate them — that includes the agent's stdout/stderr (captured
as ATIF events), the trajectory JSONL, declared artifacts in
/workspace, and any auxiliary files we ship alongside the trial.

Usage:
    check_no_provider_keys_in_artifacts.py <trial-id> \\
        [--minio-endpoint http://localhost:9000] \\
        [--trajectories-bucket trajectories] \\
        [--artifacts-bucket artifacts] \\
        [--team-id <uuid>]

Exits 0 when no secret patterns are found; exits 1 with the
matching path + offset on the first hit. Designed for CI: opt-in
job runs after a known-good trial, asserts the chain is clean.

Patterns we look for:

| Pattern        | Provider                                |
|----------------|-----------------------------------------|
| `sk-ant-`      | Anthropic                               |
| `sk-`          | OpenAI (`sk-…` standalone, ≥48 chars)   |
| `xai-`         | xAI                                     |
| `AIza`         | Google Cloud / Gemini                   |
| `gsk_`         | Groq                                    |
| `nvapi-`       | NVIDIA                                  |
| `tgp_v1_`      | Together (legacy)                       |
| `r8_`          | Replicate                               |

The script is regex-based; we accept some false positives (the
fix is to NOT have substrings that LOOK like keys in our docs or
tests) over the risk of false negatives.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

# A pattern is `(name, compiled regex)`. The regex MUST be specific
# enough to not match every base64 substring on the internet — long
# anchored fixed prefixes do that for ~all major providers.
_PATTERNS: tuple[tuple[str, re.Pattern[bytes]], ...] = (
    ("anthropic", re.compile(rb"sk-ant-[A-Za-z0-9_-]{20,}")),
    ("openai", re.compile(rb"sk-(?!ant-)[A-Za-z0-9_-]{40,}")),
    ("xai", re.compile(rb"xai-[A-Za-z0-9_-]{20,}")),
    ("google", re.compile(rb"AIza[0-9A-Za-z_-]{35}")),
    ("groq", re.compile(rb"gsk_[A-Za-z0-9_-]{20,}")),
    ("nvidia", re.compile(rb"nvapi-[A-Za-z0-9_-]{20,}")),
    ("together_legacy", re.compile(rb"tgp_v1_[A-Za-z0-9_-]{20,}")),
    ("replicate", re.compile(rb"r8_[A-Za-z0-9]{32,}")),
)


@dataclass
class Hit:
    """One secret-pattern match. `source` is human-readable for the
    failure log; `offset` is the byte offset within that source."""

    source: str
    provider: str
    offset: int
    snippet: str  # first 8 bytes around the hit, redacted middle


def scan_bytes(data: bytes, *, source: str) -> Iterator[Hit]:
    """Run every pattern against `data`. Yields one Hit per match;
    duplicates within the same source are yielded so callers can
    report them all rather than just the first."""
    for provider, regex in _PATTERNS:
        for m in regex.finditer(data):
            redacted = _redact(m.group(0))
            yield Hit(
                source=source,
                provider=provider,
                offset=m.start(),
                snippet=redacted,
            )


def _redact(token: bytes) -> str:
    """Show first 6 + last 4 bytes; replace the middle with `…`.
    Letting a CI log echo the full match would be exactly the leak
    we're trying to prevent."""
    if len(token) <= 12:
        return f"{token[:3].decode(errors='replace')}…(short)"
    head = token[:6].decode(errors="replace")
    tail = token[-4:].decode(errors="replace")
    return f"{head}…{tail}"


def scan_paths(paths: Iterable[Path]) -> Iterator[Hit]:
    """Recursively walk every input path; scan all regular files."""
    for root in paths:
        if root.is_file():
            yield from _scan_file(root)
        elif root.is_dir():
            for p in sorted(root.rglob("*")):
                if p.is_file():
                    yield from _scan_file(p)


def _scan_file(path: Path) -> Iterator[Hit]:
    """Open + scan one file. Treats every file as raw bytes — even
    JSON we still scan-as-bytes because a leaked key might be in a
    string-quoted value that happens to span a JSON line break."""
    try:
        data = path.read_bytes()
    except OSError as exc:
        # Don't fail the audit on an unreadable file (could be a
        # broken symlink), but mention it so the operator can fix
        # the disk state.
        print(
            f"WARN: could not read {path}: {exc}",
            file=sys.stderr,
        )
        return
    yield from scan_bytes(data, source=str(path))


def fetch_trial_artifacts(
    *,
    trial_id: str,
    minio_endpoint: str,
    trajectories_bucket: str,
    artifacts_bucket: str,
    team_id: str | None,
    dest: Path,
) -> list[Path]:
    """Pull the trial's events.jsonl + ATIF + artifacts/ subtree from
    MinIO into `dest`. Lazy import boto3 — the script's local-only
    mode (scanning a path) shouldn't require it.

    Returns the list of fetched paths. Empty if the trial has no
    artifacts yet (eg. cancelled before any verifier ran).
    """
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id="loomdev",
        aws_secret_access_key="loomdev123",
        region_name="us-east-1",
    )
    prefixes = [
        (trajectories_bucket, f"{team_id}/{trial_id}/" if team_id else f"{trial_id}/"),
        (artifacts_bucket, f"{team_id}/{trial_id}/" if team_id else f"{trial_id}/"),
    ]
    fetched: list[Path] = []
    for bucket, prefix in prefixes:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                target = dest / bucket / key.replace("/", "_")
                target.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, key, str(target))
                fetched.append(target)
    return fetched


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target", nargs="?",
        help="Trial ID to audit (via MinIO) OR a local path/directory to scan.",
    )
    parser.add_argument(
        "--minio-endpoint", default="http://localhost:9000",
        help="MinIO base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--trajectories-bucket", default="trajectories",
        help="S3 bucket for trajectories (default: %(default)s)",
    )
    parser.add_argument(
        "--artifacts-bucket", default="artifacts",
        help="S3 bucket for artifacts (default: %(default)s)",
    )
    parser.add_argument(
        "--team-id", default=None,
        help="Team UUID for the trial path prefix. Default: scan all.",
    )
    parser.add_argument(
        "--max-hits", type=int, default=20,
        help="Stop reporting after this many hits (default: %(default)s). "
             "Exit code 1 is set on the first hit regardless.",
    )
    args = parser.parse_args(argv)

    if not args.target:
        parser.error("target required: trial ID or local path")

    paths: list[Path]
    target_path = Path(args.target)
    if target_path.exists():
        paths = [target_path]
    else:
        # Treat target as a trial ID; fetch via MinIO.
        import tempfile
        tmp = Path(tempfile.mkdtemp(prefix="loom-audit-"))
        try:
            fetched = fetch_trial_artifacts(
                trial_id=args.target,
                minio_endpoint=args.minio_endpoint,
                trajectories_bucket=args.trajectories_bucket,
                artifacts_bucket=args.artifacts_bucket,
                team_id=args.team_id,
                dest=tmp,
            )
        except Exception as exc:
            print(
                f"ERROR: failed to fetch trial artifacts via MinIO: {exc}",
                file=sys.stderr,
            )
            return 2
        if not fetched:
            print(
                f"INFO: trial {args.target} has no artifacts at "
                f"{args.minio_endpoint} — nothing to audit.",
                file=sys.stderr,
            )
            return 0
        paths = [tmp]

    hits_reported = 0
    first_hit = True
    for hit in scan_paths(paths):
        if first_hit:
            print("SECRET LEAK DETECTED:", file=sys.stderr)
            first_hit = False
        if hits_reported < args.max_hits:
            print(
                f"  {hit.source}@{hit.offset}: "
                f"{hit.provider} match ({hit.snippet})",
                file=sys.stderr,
            )
            hits_reported += 1
        elif hits_reported == args.max_hits:
            print(
                f"  …(reached --max-hits={args.max_hits}; "
                f"more leaks present)",
                file=sys.stderr,
            )
            hits_reported += 1

    if hits_reported > 0:
        # JSON summary on stdout for downstream CI parsing.
        print(json.dumps({"leaks": True, "hits_reported": hits_reported}))
        return 1
    print(json.dumps({"leaks": False, "hits_reported": 0}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
