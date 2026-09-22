"""Declarations for bounded directory state transfer to a private verifier."""

from pathlib import PurePosixPath

MAX_MUTABLE_PATHS = 16
MAX_MUTABLE_BYTES = 256 * 1024 * 1024
MAX_MUTABLE_ENTRIES = 100_000
_PROTECTED = tuple(PurePosixPath(path) for path in (
    "/proc", "/sys", "/dev", "/run", "/var/run", "/loom", "/tests", "/verifier", "/solution",
    "/opt/verifier", "/opt/verifier-python", "/opt/verifier-assets",
))


def validate_mutable_paths(paths: tuple[PurePosixPath, ...], *, workdir: PurePosixPath) -> None:
    """Reject ambiguous roots and overlap with runtime or private verifier inputs."""
    if len(paths) > MAX_MUTABLE_PATHS:
        raise ValueError("mutable_paths supports at most 16 directory roots")
    previous: list[PurePosixPath] = []
    for path in paths:
        if (not path.is_absolute() or path == PurePosixPath("/") or ".." in path.parts
                or "\x00" in str(path) or len(str(path)) > 4096 or str(path).startswith("//")
                or path == PurePosixPath("/tmp")):
            raise ValueError(f"mutable_paths requires a safe absolute directory: {path}")
        for other in (workdir, *_PROTECTED, *previous):
            if path.is_relative_to(other) or other.is_relative_to(path):
                raise ValueError(f"mutable_paths overlap a workspace, protected or declared path: {path}")
        previous.append(path)
