import unittest

from agentic_data_platform.service.frontend_browser_smoke import (
    FrontendBrowserSmokeConfig,
    FrontendBrowserSmokeError,
    run_frontend_browser_smoke,
)


class FrontendBrowserSmokeTest(unittest.TestCase):
    def test_browser_smoke_logs_in_and_verifies_catalogs(self):
        driver = FakeBrowserDriver(
            title="Agentic Data Platform",
            values={
                "#session-user": "[REDACTED_OWNER]",
                "#catalog-state": "Ready",
                "#project-select": "pilot group (pilot-project)",
                "#model-select": "DeepSeek V4 Flash (deepseek)",
                "#harness-select": "Harbor Local Docker",
                "#benchmark-select": "terminal-bench (2.0)",
            },
        )

        result = run_frontend_browser_smoke(
            FrontendBrowserSmokeConfig(
                app_url="http://127.0.0.1:8000/app/",
                username="[REDACTED_OWNER]",
                password="[REDACTED_PASSWORD]",
                headless=True,
            ),
            driver=driver,
        )

        self.assertEqual(result.title, "Agentic Data Platform")
        self.assertEqual(result.session_user, "[REDACTED_OWNER]")
        self.assertEqual(result.catalog_state, "Ready")
        self.assertEqual(result.selected_project, "pilot group (pilot-project)")
        self.assertEqual(result.selected_model, "DeepSeek V4 Flash (deepseek)")
        self.assertEqual(result.selected_harness, "Harbor Local Docker")
        self.assertEqual(result.selected_benchmark, "terminal-bench (2.0)")
        self.assertEqual(
            driver.actions,
            [
                ("open", "http://127.0.0.1:8000/app/", True, 30000),
                ("wait_visible", "#login-form"),
                ("fill", "#login-username", "[REDACTED_OWNER]"),
                ("fill", "#login-password", "[REDACTED_PASSWORD]"),
                ("click", "#login-form button[type='submit']"),
                ("wait_visible", "#app-view:not(.hidden)"),
                ("wait_text", "#catalog-state", "Ready", 30000),
            ],
        )

    def test_browser_smoke_reports_playwright_install_hint(self):
        with self.assertRaisesRegex(FrontendBrowserSmokeError, "scripts/setup-browser-tools.sh"):
            run_frontend_browser_smoke(
                FrontendBrowserSmokeConfig(
                    app_url="http://127.0.0.1:8000/app/",
                    username="[REDACTED_OWNER]",
                    password="[REDACTED_PASSWORD]",
                    headless=True,
                ),
                driver=None,
                playwright_factory=lambda: (_ for _ in ()).throw(ModuleNotFoundError("playwright")),
            )

    def test_browser_smoke_preserves_open_failure_when_cleanup_runs(self):
        with self.assertRaisesRegex(FrontendBrowserSmokeError, "browser failed to start"):
            run_frontend_browser_smoke(
                FrontendBrowserSmokeConfig(
                    app_url="http://127.0.0.1:8000/app/",
                    username="[REDACTED_OWNER]",
                    password="[REDACTED_PASSWORD]",
                    headless=True,
                ),
                driver=None,
                playwright_factory=lambda: FailingPlaywrightManager(),
            )


class FakeBrowserDriver:
    def __init__(self, *, title, values):
        self.title_value = title
        self.values = values
        self.actions = []

    def open(self, url, *, headless, timeout_ms):
        self.actions.append(("open", url, headless, timeout_ms))

    def wait_visible(self, selector):
        self.actions.append(("wait_visible", selector))

    def fill(self, selector, value):
        self.actions.append(("fill", selector, value))

    def click(self, selector):
        self.actions.append(("click", selector))

    def wait_text(self, selector, text, *, timeout_ms):
        self.actions.append(("wait_text", selector, text, timeout_ms))

    def text(self, selector):
        return self.values[selector]

    def title(self):
        return self.title_value

    def close(self):
        self.actions.append(("close",))


class FailingPlaywrightManager:
    def start(self):
        raise RuntimeError("browser failed to start")
