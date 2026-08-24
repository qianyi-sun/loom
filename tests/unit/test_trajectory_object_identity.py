from pathlib import Path
from uuid import uuid4

import pytest

from loom.trajectory.object_identity import (
    TrajectoryObjectIdentity,
    resolve_trajectory_object_key,
)
from loom.trajectory.storage import FakeObjectStore


def test_attempts_have_distinct_local_events_and_atif_identities(
    tmp_path: Path,
) -> None:
    team_id = uuid4()
    trial_id = uuid4()

    first = TrajectoryObjectIdentity(
        bucket="trajectories",
        team_id=team_id,
        trial_id=trial_id,
        attempt_count=1,
    )
    second = TrajectoryObjectIdentity(
        bucket="trajectories",
        team_id=team_id,
        trial_id=trial_id,
        attempt_count=2,
    )

    assert first.local_path(tmp_path) == (
        tmp_path / f"{trial_id}.attempt-1.events.jsonl"
    )
    assert second.local_path(tmp_path) == (
        tmp_path / f"{trial_id}.attempt-2.events.jsonl"
    )
    assert first.events_key == (
        f"{team_id}/{trial_id}/attempts/1/events.jsonl"
    )
    assert second.events_key == (
        f"{team_id}/{trial_id}/attempts/2/events.jsonl"
    )
    assert first.atif_key == f"{team_id}/{trial_id}/attempts/1/atif.json"
    assert second.atif_key == f"{team_id}/{trial_id}/attempts/2/atif.json"
    assert first.events_uri != second.events_uri
    assert first.atif_uri != second.atif_uri


def test_identity_without_attempt_preserves_legacy_paths(tmp_path: Path) -> None:
    team_id = uuid4()
    trial_id = uuid4()
    identity = TrajectoryObjectIdentity(
        bucket="trajectories",
        team_id=team_id,
        trial_id=trial_id,
    )

    assert identity.local_path(tmp_path) == tmp_path / f"{trial_id}.jsonl"
    assert identity.events_key == f"{team_id}/{trial_id}/events.jsonl"
    assert identity.atif_key == f"{team_id}/{trial_id}/atif.json"


@pytest.mark.parametrize("attempt_count", [0, -1, True])
def test_identity_rejects_non_positive_or_boolean_attempts(
    attempt_count: int,
) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        TrajectoryObjectIdentity(
            bucket="trajectories",
            team_id=uuid4(),
            trial_id=uuid4(),
            attempt_count=attempt_count,
        )


async def test_late_old_attempt_completion_cannot_overwrite_new_attempt() -> None:
    store = FakeObjectStore()
    team_id = uuid4()
    trial_id = uuid4()
    old = TrajectoryObjectIdentity(
        bucket="trajectories",
        team_id=team_id,
        trial_id=trial_id,
        attempt_count=1,
    )
    new = TrajectoryObjectIdentity(
        bucket="trajectories",
        team_id=team_id,
        trial_id=trial_id,
        attempt_count=2,
    )
    old_upload = await store.create_multipart_upload(
        bucket=old.bucket,
        key=old.events_key,
    )
    new_upload = await store.create_multipart_upload(
        bucket=new.bucket,
        key=new.events_key,
    )
    await store.upload_part(old_upload, part_number=1, body=b"old-attempt\n")
    await store.upload_part(new_upload, part_number=1, body=b"new-attempt\n")

    await store.complete_multipart_upload(new_upload)
    await store.complete_multipart_upload(old_upload)

    assert store.objects[(new.bucket, new.events_key)] == b"new-attempt\n"
    assert store.objects[(old.bucket, old.events_key)] == b"old-attempt\n"


def test_resolver_uses_valid_attempt_scoped_uri() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    uri = f"s3://trajectories/{team_id}/{trial_id}/attempts/3/events.jsonl"

    assert resolve_trajectory_object_key(
        uri=uri,
        expected_bucket="trajectories",
        team_id=team_id,
        trial_id=trial_id,
        filename="events.jsonl",
    ) == f"{team_id}/{trial_id}/attempts/3/events.jsonl"


def test_resolver_preserves_missing_and_exact_legacy_uri_fallback() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    legacy_key = f"{team_id}/{trial_id}/atif.json"

    for uri in (None, f"s3://trajectories/{legacy_key}"):
        assert resolve_trajectory_object_key(
            uri=uri,
            expected_bucket="trajectories",
            team_id=team_id,
            trial_id=trial_id,
            filename="atif.json",
        ) == legacy_key


@pytest.mark.parametrize(
    "uri_template",
    [
        "https://trajectories/{team}/{trial}/attempts/2/events.jsonl",
        "s3://foreign/{team}/{trial}/attempts/2/events.jsonl",
        "s3://trajectories/{other}/{trial}/attempts/2/events.jsonl",
        "s3://trajectories/{team}/{other}/attempts/2/events.jsonl",
        "s3://trajectories/{team}/{trial}/attempts/0/events.jsonl",
        "s3://trajectories/{team}/{trial}/attempts/02/events.jsonl",
        "s3://trajectories/{team}/{trial}/attempts/2/atif.json",
        "s3://trajectories/{team}/{trial}/attempts/2/events.jsonl?version=1",
        "s3://trajectories/{team}\n/{trial}/attempts/2/events.jsonl",
    ],
)
def test_resolver_rejects_untrusted_or_noncanonical_uri(
    uri_template: str,
) -> None:
    team_id = uuid4()
    trial_id = uuid4()
    other = uuid4()
    uri = uri_template.format(team=team_id, trial=trial_id, other=other)

    with pytest.raises(ValueError, match="trajectory object URI"):
        resolve_trajectory_object_key(
            uri=uri,
            expected_bucket="trajectories",
            team_id=team_id,
            trial_id=trial_id,
            filename="events.jsonl",
        )
