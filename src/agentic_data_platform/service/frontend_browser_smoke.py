from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Mapping, Protocol


class FrontendBrowserSmokeError(RuntimeError):
    pass


@dataclass(frozen=True)
class FrontendBrowserSmokeConfig:
    app_url: str
    username: str
    password: str
    headless: bool = True
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class FrontendBrowserSmokeResult:
    title: str
    session_user: str
    catalog_state: str
    selected_project: str
    selected_model: str
    selected_harness: str
    selected_benchmark: str

    def to_dict(self) -> dict[str, object]:
        return {
            "title": self.title,
            "session_user": self.session_user,
            "catalog_state": self.catalog_state,
            "selected_project": self.selected_project,
            "selected_model": self.selected_model,
            "selected_harness": self.selected_harness,
            "selected_benchmark": self.selected_benchmark,
        }


class BrowserSmokeDriver(Protocol):
    def open(self, url: str, *, headless: bool, timeout_ms: int) -> None:
        ...

    def wait_visible(self, selector: str) -> None:
        ...

    def fill(self, selector: str, value: str) -> None:
        ...

    def click(self, selector: str) -> None:
        ...

    def wait_text(self, selector: str, text: str, *, timeout_ms: int) -> None:
        ...

    def text(self, selector: str) -> str:
        ...

    def title(self) -> str:
        ...

    def close(self) -> None:
        ...


def run_frontend_browser_smoke(
    config: FrontendBrowserSmokeConfig,
    *,
    driver: BrowserSmokeDriver | None = None,
    playwright_factory=None,
) -> FrontendBrowserSmokeResult:
    _validate_config(config)
    timeout_ms = int(config.timeout_seconds * 1000)
    active_driver = driver or PlaywrightBrowserSmokeDriver(playwright_factory=playwright_factory)
    owns_driver = driver is None
    try:
        active_driver.open(config.app_url, headless=config.headless, timeout_ms=timeout_ms)
        active_driver.wait_visible("#login-form")
        active_driver.fill("#login-username", config.username)
        active_driver.fill("#login-password", config.password)
        active_driver.click("#login-form button[type='submit']")
        active_driver.wait_visible("#app-view:not(.hidden)")
        active_driver.wait_text("#catalog-state", "Ready", timeout_ms=timeout_ms)
        return FrontendBrowserSmokeResult(
            title=active_driver.title(),
            session_user=active_driver.text("#session-user"),
            catalog_state=active_driver.text("#catalog-state"),
            selected_project=active_driver.text("#project-select"),
            selected_model=active_driver.text("#model-select"),
            selected_harness=active_driver.text("#harness-select"),
            selected_benchmark=active_driver.text("#benchmark-select"),
        )
    except ModuleNotFoundError as exc:
        missing_module = exc.name or str(exc)
        if "playwright" in missing_module:
            raise FrontendBrowserSmokeError(
                "Playwright is not installed. Run scripts/setup-browser-tools.sh before browser smoke testing."
            ) from exc
        raise
    except FrontendBrowserSmokeError:
        raise
    except Exception as exc:  # pragma: no cover - exact Playwright exception types vary by version.
        raise FrontendBrowserSmokeError(f"frontend browser smoke failed: {exc}") from exc
    finally:
        if owns_driver:
            active_driver.close()


class PlaywrightBrowserSmokeDriver:
    def __init__(self, *, playwright_factory=None) -> None:
        self._playwright_factory = playwright_factory or _sync_playwright_factory
        self._manager = None
        self._playwright = None
        self._browser = None
        self._page = None

    def open(self, url: str, *, headless: bool, timeout_ms: int) -> None:
        self._manager = self._playwright_factory()
        self._playwright = self._manager.start()
        self._browser = self._playwright.chromium.launch(headless=headless)
        self._page = self._browser.new_page()
        self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

    def wait_visible(self, selector: str) -> None:
        self._require_page().locator(selector).wait_for(state="visible")

    def fill(self, selector: str, value: str) -> None:
        self._require_page().locator(selector).fill(value)

    def click(self, selector: str) -> None:
        self._require_page().locator(selector).click()

    def wait_text(self, selector: str, text: str, *, timeout_ms: int) -> None:
        self._require_page().wait_for_function(
            """([selector, expected]) => {
              const element = document.querySelector(selector);
              return element && element.textContent.trim() === expected;
            }""",
            [selector, text],
            timeout=timeout_ms,
        )

    def text(self, selector: str) -> str:
        return str(
            self._require_page().eval_on_selector(
                selector,
                """(element) => {
                  if (element instanceof HTMLSelectElement) {
                    return element.selectedOptions[0]?.textContent?.trim() || "";
                  }
                  return element.textContent?.trim() || "";
                }""",
            )
        )

    def title(self) -> str:
        return str(self._require_page().title())

    def close(self) -> None:
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None
        self._manager = None

    def _require_page(self):
        if self._page is None:
            raise FrontendBrowserSmokeError("browser page was not opened")
        return self._page


def main() -> int:
    result = run_frontend_browser_smoke(_config_from_env())
    print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
    return 0


def _config_from_env(environ: Mapping[str, str] | None = None) -> FrontendBrowserSmokeConfig:
    values = os.environ if environ is None else environ
    return FrontendBrowserSmokeConfig(
        app_url=_env(values, "FRONTEND_BROWSER_SMOKE_APP_URL", "http://127.0.0.1:8000/app/"),
        username=_env(values, "FRONTEND_BROWSER_SMOKE_USERNAME", "[REDACTED_OWNER]"),
        password=_env(values, "FRONTEND_BROWSER_SMOKE_PASSWORD", "[REDACTED_PASSWORD]"),
        headless=_bool_env(_env(values, "FRONTEND_BROWSER_SMOKE_HEADLESS", "true")),
        timeout_seconds=float(_env(values, "FRONTEND_BROWSER_SMOKE_TIMEOUT_SECONDS", "30")),
    )


def _validate_config(config: FrontendBrowserSmokeConfig) -> None:
    if not config.app_url.startswith(("http://", "https://")):
        raise FrontendBrowserSmokeError("FRONTEND_BROWSER_SMOKE_APP_URL must be an HTTP(S) URL")
    if not config.username:
        raise FrontendBrowserSmokeError("FRONTEND_BROWSER_SMOKE_USERNAME is required")
    if not config.password:
        raise FrontendBrowserSmokeError("FRONTEND_BROWSER_SMOKE_PASSWORD is required")
    if config.timeout_seconds <= 0:
        raise FrontendBrowserSmokeError("FRONTEND_BROWSER_SMOKE_TIMEOUT_SECONDS must be positive")


def _sync_playwright_factory():
    from playwright.sync_api import sync_playwright

    return sync_playwright()


def _env(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name)
    return default if value is None or value == "" else value


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    raise SystemExit(main())
