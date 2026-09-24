"""An archival retry requires explicit identifiers and application intent."""
import subprocess
import sys
from uuid import uuid4


def test_archival_retry_cli_requires_apply_before_loading_live_configuration() -> None:
    result = subprocess.run(
        [sys.executable, '-m', 'loom_control_plane.service_execution_archival_recovery',
         '--lease-id', str(uuid4()), '--team-id', str(uuid4())],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 2
    assert '--apply' in result.stderr
    assert 'Traceback' not in result.stderr
