import unittest
from enum import Enum
from types import SimpleNamespace

from agentic_data_platform.harbor.capabilities import probe_harbor_native_capabilities


class HarborCapabilityProbeTest(unittest.TestCase):
    def test_reports_native_available_when_required_harbor_symbols_exist(self):
        report = probe_harbor_native_capabilities(
            import_module=_fake_importer(
                job_symbols={
                    "Job": object,
                    "JobConfig": object,
                    "DatasetConfig": object,
                    "TaskConfig": object,
                    "TrialEvent": FakeTrialEvent,
                    "TrialHookEvent": object,
                    "JobResult": object,
                },
                cli_job_symbols={
                    "AgentConfig": object,
                    "EnvironmentConfig": object,
                },
            ),
            package_version=lambda package_name: "0.8.0",
        )

        self.assertEqual(report.package_version, "0.8.0")
        self.assertTrue(report.native_runner_available)
        self.assertEqual(report.missing_symbols, ())
        self.assertEqual(
            report.trial_events,
            ("START", "ENVIRONMENT_START", "AGENT_START", "VERIFICATION_START", "END", "CANCEL"),
        )

    def test_reports_missing_symbols_without_import_crashing(self):
        report = probe_harbor_native_capabilities(
            import_module=_fake_importer(
                job_symbols={
                    "Job": object,
                    "JobConfig": object,
                    "DatasetConfig": object,
                },
                cli_job_symbols={
                    "AgentConfig": object,
                },
            ),
            package_version=lambda package_name: "0.8.0",
        )

        self.assertFalse(report.native_runner_available)
        self.assertIn("harbor.job.TaskConfig", report.missing_symbols)
        self.assertIn("harbor.job.TrialEvent", report.missing_symbols)
        self.assertIn("harbor.job.TrialHookEvent", report.missing_symbols)
        self.assertIn("harbor.job.JobResult", report.missing_symbols)
        self.assertIn("harbor.cli.jobs.EnvironmentConfig", report.missing_symbols)
        self.assertEqual(report.trial_events, ())


class FakeTrialEvent(Enum):
    START = "START"
    ENVIRONMENT_START = "ENVIRONMENT_START"
    AGENT_START = "AGENT_START"
    VERIFICATION_START = "VERIFICATION_START"
    END = "END"
    CANCEL = "CANCEL"


def _fake_importer(*, job_symbols: dict[str, object], cli_job_symbols: dict[str, object]):
    def import_module(module_name: str):
        if module_name == "harbor.job":
            return SimpleNamespace(**job_symbols)
        if module_name == "harbor.cli.jobs":
            return SimpleNamespace(**cli_job_symbols)
        raise ModuleNotFoundError(module_name)

    return import_module
