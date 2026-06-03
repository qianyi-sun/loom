import subprocess
import unittest

from agentic_data_platform.scheduler.container_leak_audit import audit_run_containers
from agentic_data_platform.sandbox.docker_terminal import DockerOwnedContainerCleaner


class ContainerLeakAuditTest(unittest.TestCase):
    def test_audit_passes_when_run_has_no_owned_containers(self) -> None:
        runner = FakeDockerListRunner({"run_clean_001": ""})
        cleaner = DockerOwnedContainerCleaner(runner=runner)

        result = audit_run_containers(["run_clean_001"], cleaner=cleaner)

        self.assertEqual(result.checked_run_count, 1)
        self.assertEqual(result.leaked_container_count, 0)
        self.assertEqual(result.to_dict()["leaked_containers"], {})
        ps_args = runner.calls[0]["args"]
        self.assertIn("label=com.agentic-data-platform.managed=true", ps_args)
        self.assertIn("label=com.agentic-data-platform.run_id=run_clean_001", ps_args)
        self.assertIn("label=com.agentic-data-platform.resource=sandbox-container", ps_args)

    def test_audit_reports_leftover_owned_containers_by_run(self) -> None:
        runner = FakeDockerListRunner(
            {
                "run_leak_001": "container-one\ncontainer-two\n",
                "run_clean_002": "",
            }
        )
        cleaner = DockerOwnedContainerCleaner(runner=runner)

        result = audit_run_containers(["run_leak_001", "run_clean_002"], cleaner=cleaner)

        self.assertEqual(result.checked_run_count, 2)
        self.assertEqual(result.leaked_run_count, 1)
        self.assertEqual(result.leaked_container_count, 2)
        self.assertEqual(result.leaked_containers, {"run_leak_001": ["container-one", "container-two"]})


class FakeDockerListRunner:
    def __init__(self, containers_by_run_id: dict[str, str]) -> None:
        self.containers_by_run_id = containers_by_run_id
        self.calls = []

    def run(self, args, *, timeout):
        self.calls.append({"args": args, "timeout": timeout})
        if args[:3] != ["docker", "ps", "-aq"]:
            raise AssertionError(f"unexpected docker command: {args}")
        run_id = _run_id_from_args(args)
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=self.containers_by_run_id.get(run_id, ""),
            stderr="",
        )


def _run_id_from_args(args: list[str]) -> str:
    prefix = "label=com.agentic-data-platform.run_id="
    for index, item in enumerate(args):
        if item == "--filter" and index + 1 < len(args):
            value = args[index + 1]
            if value.startswith(prefix):
                return value[len(prefix) :]
    raise AssertionError(f"missing run-id label filter: {args}")
