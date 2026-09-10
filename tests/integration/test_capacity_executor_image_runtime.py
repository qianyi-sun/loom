"""Run with stdlib unittest inside the built executor image, on both architectures."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

_INSTALLER = Path(
    "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"
)


@unittest.skipUnless(__name__ == "__main__", "Executed by the capacity-executor image build")
class CapacityExecutorImageRuntimeTest(unittest.TestCase):
    def test_installer_can_load_its_command_contract(self) -> None:
        result = subprocess.run(
            [sys.executable, "-B", str(_INSTALLER), "--help"],
            capture_output=True, text=True, check=False, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("discover-controller", result.stdout)
        self.assertIn("converge-prerequisite", result.stdout)

    def test_invalid_discovery_reaches_validation_without_host_access(self) -> None:
        result = subprocess.run(
            [sys.executable, "-B", str(_INSTALLER), "--operation", "discover-controller"],
            input="{}\n", capture_output=True, text=True, check=False, timeout=30,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("controller discovery request is invalid", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
