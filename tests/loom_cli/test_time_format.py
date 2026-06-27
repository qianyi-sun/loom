from __future__ import annotations

from datetime import UTC, datetime

from loom_cli.time_format import format_local_datetime


def test_format_local_datetime_renders_aware_values_in_requested_timezone() -> None:
    assert (
        format_local_datetime(
            datetime(2026, 6, 27, 3, 4, 54, tzinfo=UTC),
            time_zone="America/Toronto",
        )
        == "2026-06-26 23:04 EDT"
    )


def test_format_local_datetime_keeps_invalid_strings_readable() -> None:
    assert format_local_datetime("not-a-date") == "not-a-date"
    assert format_local_datetime("", fallback="--") == "--"
