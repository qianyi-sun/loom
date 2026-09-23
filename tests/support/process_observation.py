"""Linux process-death observations used only by subprocess regression tests."""
from pathlib import Path


def process_exited(pid: int) -> bool:
    status = Path(f"/proc/{pid}/stat")
    try:
        value = status.read_text()
    except (FileNotFoundError, ProcessLookupError):
        # Linux may remove the proc entry before open, or return ESRCH from
        # read after the process has been reaped. Both establish disappearance.
        return True
    return value.split()[2] in {"Z", "X"}
