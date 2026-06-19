"""ConfigBuilder: rows → ConnectionAllowlist snapshot (#190)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from loom_egress_xds.config_builder import (
    ConnectionAllowlist,
    Snapshot,
    build_snapshot,
)


@dataclass
class _Row:
    """Minimal fake row matching the ProviderConnectionRow Protocol."""

    id: UUID
    resolved_egress_ips: list[str] = field(default_factory=list)
    upstream_host: str = "example.com"
    base_url: str = "https://example.com/v1"
    deleted_at: datetime | None = None


_C1 = UUID("00000000-0000-0000-0000-000000000001")
_C2 = UUID("00000000-0000-0000-0000-000000000002")
_C3 = UUID("00000000-0000-0000-0000-000000000003")


def test_empty_input_returns_empty_snapshot() -> None:
    snap = build_snapshot([])
    assert snap.entries == ()
    # Empty version is still deterministic + non-empty (sha256("") prefix).
    assert len(snap.version) == 16


def test_single_row_round_trips() -> None:
    rows = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4", "5.6.7.8"], upstream_host="api.openai.com")
    ]
    snap = build_snapshot(rows)
    assert snap.entries == (
        ConnectionAllowlist(
            connection_id=_C1,
            ips=("1.2.3.4", "5.6.7.8"),
            upstream_host="api.openai.com",
            upstream_scheme="https",
            upstream_port=443,
        ),
    )


def test_base_url_explicit_http_port_round_trips() -> None:
    rows = [
        _Row(
            id=_C1,
            resolved_egress_ips=["192.168.32.1"],
            upstream_host="192.168.32.1",
            base_url="http://192.168.32.1:28001/v1",
        )
    ]
    snap = build_snapshot(rows)
    assert snap.entries[0] == ConnectionAllowlist(
        connection_id=_C1,
        ips=("192.168.32.1",),
        upstream_host="192.168.32.1",
        upstream_scheme="http",
        upstream_port=28001,
    )


def test_base_url_default_ports_follow_scheme() -> None:
    https_snap = build_snapshot(
        [
            _Row(
                id=_C1,
                resolved_egress_ips=["1.2.3.4"],
                base_url="https://api.example.com/v1",
            )
        ]
    )
    http_snap = build_snapshot(
        [
            _Row(
                id=_C1,
                resolved_egress_ips=["1.2.3.4"],
                base_url="http://api.example.com/v1",
            )
        ]
    )

    assert https_snap.entries[0].upstream_port == 443
    assert http_snap.entries[0].upstream_port == 80


def test_invalid_base_url_row_excluded() -> None:
    rows = [
        _Row(
            id=_C1,
            resolved_egress_ips=["1.2.3.4"],
            base_url="ftp://example.com/v1",
        ),
        _Row(id=_C2, resolved_egress_ips=["5.6.7.8"]),
    ]
    snap = build_snapshot(rows)
    assert [e.connection_id for e in snap.entries] == [_C2]


def test_deleted_rows_excluded() -> None:
    rows = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
        _Row(id=_C2, resolved_egress_ips=["5.6.7.8"], deleted_at=datetime.now(UTC)),
    ]
    snap = build_snapshot(rows)
    assert [e.connection_id for e in snap.entries] == [_C1]


def test_rows_with_empty_ip_list_excluded() -> None:
    # Empty allowlist is semantically distinct from missing entry —
    # exclude so Envoy doesn't see a deny-all cluster.
    rows = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
        _Row(id=_C2, resolved_egress_ips=[]),
    ]
    snap = build_snapshot(rows)
    assert [e.connection_id for e in snap.entries] == [_C1]


def test_duplicate_ips_deduped() -> None:
    rows = [_Row(id=_C1, resolved_egress_ips=["1.2.3.4", "1.2.3.4", "5.6.7.8"])]
    snap = build_snapshot(rows)
    assert snap.entries[0].ips == ("1.2.3.4", "5.6.7.8")


def test_ips_sorted_lexicographically() -> None:
    rows = [_Row(id=_C1, resolved_egress_ips=["9.9.9.9", "1.1.1.1", "5.5.5.5"])]
    snap = build_snapshot(rows)
    assert snap.entries[0].ips == ("1.1.1.1", "5.5.5.5", "9.9.9.9")


def test_entries_sorted_by_connection_id() -> None:
    # Input out of order; output must be by connection_id.
    rows = [
        _Row(id=_C3, resolved_egress_ips=["3.3.3.3"]),
        _Row(id=_C1, resolved_egress_ips=["1.1.1.1"]),
        _Row(id=_C2, resolved_egress_ips=["2.2.2.2"]),
    ]
    snap = build_snapshot(rows)
    assert [e.connection_id for e in snap.entries] == [_C1, _C2, _C3]


def test_version_stable_across_calls() -> None:
    # Same input → same version (sha256 is deterministic; ordering
    # is enforced by build_snapshot regardless of input order).
    rows_a = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
        _Row(id=_C2, resolved_egress_ips=["5.6.7.8"]),
    ]
    rows_b = [
        _Row(id=_C2, resolved_egress_ips=["5.6.7.8"]),
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
    ]
    assert build_snapshot(rows_a).version == build_snapshot(rows_b).version


def test_version_changes_when_ips_change() -> None:
    rows1 = [_Row(id=_C1, resolved_egress_ips=["1.2.3.4"])]
    rows2 = [_Row(id=_C1, resolved_egress_ips=["1.2.3.4", "5.6.7.8"])]
    v1 = build_snapshot(rows1).version
    v2 = build_snapshot(rows2).version
    assert v1 != v2


def test_version_changes_when_entry_added() -> None:
    rows1 = [_Row(id=_C1, resolved_egress_ips=["1.2.3.4"])]
    rows2 = [*rows1, _Row(id=_C2, resolved_egress_ips=["5.6.7.8"])]
    assert build_snapshot(rows1).version != build_snapshot(rows2).version


def test_lookup_finds_entry() -> None:
    rows = [_Row(id=_C1, resolved_egress_ips=["1.2.3.4"])]
    snap = build_snapshot(rows)
    assert snap.lookup(_C1) is not None
    assert snap.lookup(_C2) is None


def test_snapshot_is_immutable() -> None:
    snap = Snapshot(entries=(), version="abc")
    # `entries` is a tuple, ConnectionAllowlist is frozen — runtime
    # mutation would raise. This test pins the contract.
    import dataclasses

    assert dataclasses.is_dataclass(snap)
    # frozen=True means setattr raises FrozenInstanceError.
    import pytest

    with pytest.raises(dataclasses.FrozenInstanceError):
        snap.version = "xyz"  # type: ignore[misc]
