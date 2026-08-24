from uuid import uuid4

import pytest

from loom.db.schema import Trial
from loom_service.delivery_export import (
    InvalidDeliveryBatchFamilyError,
    _object_ref_for_trial,
)


def test_delivery_export_rejects_persisted_uri_outside_trial_identity() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    other_trial_id = uuid4()
    trial = Trial(
        id=trial_id,
        team_id=team_id,
        trajectory_index={
            "trajectory_uri": (
                "s3://trajectories/"
                f"{team_id}/{other_trial_id}/attempts/2/events.jsonl"
            ),
        },
    )

    with pytest.raises(
        InvalidDeliveryBatchFamilyError,
        match="delivery_export_invalid_batch_family",
    ):
        _object_ref_for_trial(
            trial,
            kind="trajectory",
            trajectories_bucket="trajectories",
        )


def test_delivery_export_uses_attempt_scoped_persisted_uri() -> None:
    team_id = uuid4()
    trial_id = uuid4()
    key = f"{team_id}/{trial_id}/attempts/3/atif.json"
    trial = Trial(
        id=trial_id,
        team_id=team_id,
        trajectory_index={"atif_uri": f"s3://trajectories/{key}"},
    )

    ref = _object_ref_for_trial(
        trial,
        kind="atif",
        trajectories_bucket="trajectories",
    )

    assert ref.bucket == "trajectories"
    assert ref.key == key
