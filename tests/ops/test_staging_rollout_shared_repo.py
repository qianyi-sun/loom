from __future__ import annotations

import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import staging_rollout_shared_repo as helper


def test_consumer_identity_keeps_distinct_primary_and_shared_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        helper.pwd,
        "getpwnam",
        lambda name: SimpleNamespace(pw_uid=2005, pw_gid=2005) if name == "qianyi" else None,
    )
    monkeypatch.setattr(helper.os, "getgrouplist", lambda _name, _gid: [2005, 2007])

    identity = helper._identity("qianyi")

    assert identity.uid == 2005
    assert identity.gid == 2005
    assert identity.groups == (2005, 2007)


def test_ensure_child_creates_and_validates_once(tmp_path: Path) -> None:
    parent = helper._open_absolute(tmp_path)
    try:
        child, created = helper._ensure_child(
            parent,
            "worker-repos",
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o750,
        )
        try:
            assert created is True
            assert child.path == tmp_path / "worker-repos"
            assert stat.S_IMODE(os.fstat(child.fd).st_mode) == 0o750
        finally:
            os.close(child.fd)
    finally:
        os.close(parent.fd)


def test_ensure_child_rejects_existing_metadata_without_mutation(tmp_path: Path) -> None:
    target = tmp_path / "worker-repos"
    target.mkdir(mode=0o700)
    parent = helper._open_absolute(tmp_path)
    try:
        with pytest.raises(helper.AuthorityError, match="metadata"):
            helper._ensure_child(
                parent,
                target.name,
                uid=os.geteuid(),
                gid=os.getegid(),
                mode=0o750,
            )
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
    finally:
        os.close(parent.fd)


def test_ensure_child_removes_only_its_exact_failed_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = helper._open_absolute(tmp_path)
    monkeypatch.setattr(
        helper.os,
        "fchown",
        lambda *_args: (_ for _ in ()).throw(OSError("injected owner failure")),
    )
    try:
        with pytest.raises(OSError, match="injected owner failure"):
            helper._ensure_child(
                parent,
                "worker-repos",
                uid=os.geteuid(),
                gid=os.getegid(),
                mode=0o750,
            )
        assert not (tmp_path / "worker-repos").exists()
    finally:
        os.close(parent.fd)


def _svc_identity(name: str, *_a: object, **_k: object) -> helper.Identity:
    # service: uid 2001, primary group 2001 (loom-rollout), NOT in sharedwork(2007)
    # consumer: uid 2005, in sharedwork(2007)
    if name == helper.SERVICE_USER:
        return helper.Identity(name, 2001, 2001, (2001,))
    return helper.Identity(name, 2005, 2005, (2005, 2007))


def test_service_owned_rejects_invalid_relative_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    for bad in ((), ("..",), ("candidates", ""), ("candidates", "a/b")):
        with pytest.raises(helper.AuthorityError, match="service path is invalid"):
            helper._converge_service_owned(Path("/shared_work/loom"), bad, ensure=False)


def test_service_owned_requires_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(helper.os, "geteuid", lambda: 1000)
    with pytest.raises(helper.AuthorityError, match="requires root"):
        helper._converge_service_owned(Path("/shared_work/loom"), ("candidates",), ensure=False)


def test_service_owned_rejects_bad_group_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    monkeypatch.setattr(helper, "_identity", _svc_identity)
    monkeypatch.setattr(helper.grp, "getgrnam", lambda name: SimpleNamespace(gr_gid=2007))
    # consumer NOT in sharedwork -> invalid
    monkeypatch.setattr(
        helper,
        "_identity",
        lambda name, *a, **k: (
            helper.Identity(name, 2001, 2001, (2001,))
            if name == helper.SERVICE_USER
            else helper.Identity(name, 2005, 2005, (2005,))
        ),
    )
    with pytest.raises(helper.AuthorityError, match="group membership is invalid"):
        helper._converge_service_owned(Path("/shared_work/loom"), ("candidates",), ensure=False)


def test_service_owned_rejects_service_in_shared_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    monkeypatch.setattr(helper.grp, "getgrnam", lambda name: SimpleNamespace(gr_gid=2007))
    # service IS in sharedwork(2007) -> invalid (must not be)
    monkeypatch.setattr(
        helper,
        "_identity",
        lambda name, *a, **k: (
            helper.Identity(name, 2001, 2001, (2001, 2007))
            if name == helper.SERVICE_USER
            else helper.Identity(name, 2005, 2005, (2005, 2007))
        ),
    )
    with pytest.raises(helper.AuthorityError, match="group membership is invalid"):
        helper._converge_service_owned(Path("/shared_work/loom"), ("candidates",), ensure=False)


def test_service_cli_rejects_short_or_nonhex_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    # The privileged CLI must validate a full 40-hex SHA and never touch the fs
    # for a bad one. We don't reach converge (geteuid check would fail non-root),
    # because SHA validation happens first and returns exit code 2.
    for bad in ("abc1234", "z" * 40, "ABC" + "0" * 37, "0" * 39, "0" * 41):
        rc = helper.main(["service-ensure", "--environment", "development", "--candidate-sha", bad])
        assert rc == 2


def test_service_cli_requires_environment_and_sha() -> None:
    assert helper.main(["service-ensure", "--candidate-sha", "a" * 40]) == 2
    assert helper.main(["service-check", "--environment", "staging"]) == 2


def test_service_cli_rejects_unknown_environment() -> None:
    # argparse choices reject anything outside development/staging/production.
    with pytest.raises(SystemExit):
        helper.main(["service-ensure", "--environment", "prod", "--candidate-sha", "a" * 40])


def test_service_cli_hardcodes_root_and_layout() -> None:
    # No --root / --path options exist anymore: an arbitrary path cannot be
    # requested through the privileged CLI.
    with pytest.raises(SystemExit):
        helper.main(["service-ensure", "--root", "/etc", "--path", "shadow"])
    assert helper.SERVICE_ROOT == Path("/shared_work/loom")
    assert helper.ALLOWED_ENVIRONMENTS == ("development", "staging", "production")


def test_service_ensure_never_precreates_sha_and_reports_target(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Privileged setup must ensure ONLY candidates/<env>; the final <sha> dir
    # is left absent so the materializer can atomically rename-no-replace into
    # it. Pre-creating an empty <sha> would defeat all-or-nothing publication.
    captured: dict[str, object] = {}

    def fake(root: object, chain: object, *, ensure: bool) -> dict[str, object]:
        captured["chain"] = chain
        captured["ensure"] = ensure
        return {"model": "service-owned"}

    monkeypatch.setattr(helper, "_converge_service_owned", fake)
    rc = helper.main(
        ["service-ensure", "--environment", "staging", "--candidate-sha", "a" * 40],
    )
    assert rc == 0
    assert captured["chain"] == ("candidates", "staging")  # NO sha component
    assert captured["ensure"] is True
    import json as _json

    report = _json.loads(capsys.readouterr().out)
    assert report["candidate_target"] == "/shared_work/loom/candidates/staging/" + "a" * 40
