"""Linux process-death observations used only by subprocess regression tests."""
from pathlib import Path


def process_exited(pid: int) -> bool:
    status = Path(f"/proc/{pid}/stat")
    return not status.exists() or status.read_text().split()[2] in {"Z", "X"}
