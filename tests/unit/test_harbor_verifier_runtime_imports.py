"""The minimal Harbor controller must not import ingestion/control-plane code."""

import subprocess
import sys


def test_native_controller_import_does_not_load_ingestion_or_materialization() -> None:
    script = """
import importlib.abc
import sys
class RuntimeBoundary(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {'loom.nebius_terminus_ingest', 'loom.service_execution_materialization'}:
            raise ImportError('controller crossed the runtime dependency boundary: ' + fullname)
sys.meta_path.insert(0, RuntimeBoundary())
import loom.service_execution_sandbox_task
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
