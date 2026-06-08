import importlib


def test_loom_drivers_package_importable() -> None:
    mod = importlib.import_module("loom_drivers")
    assert mod.__name__ == "loom_drivers"


def test_daytona_subpackage_importable() -> None:
    mod = importlib.import_module("loom_drivers.daytona")
    assert mod.__name__ == "loom_drivers.daytona"


def test_daytona_sdk_available() -> None:
    mod = importlib.import_module("daytona")
    assert hasattr(mod, "AsyncDaytona")
    assert hasattr(mod, "DaytonaConfig")
    assert hasattr(mod, "CreateSandboxFromImageParams")
