from loom_control_plane.terminus_recovery import checkpoint_checksum, verify_checkpoint


class _Row:
    def __init__(self) -> None:
        from uuid import uuid4

        self.execution_id = uuid4()
        self.episode = 4
        self.active_role = "teacher"
        self.last_call_ordinal = 7
        self.last_seq = 12
        self.tmux_session_id = "tmux-a"
        self.checksum = checkpoint_checksum(
            execution_id=self.execution_id,
            episode=self.episode,
            active_role=self.active_role,
            last_call_ordinal=self.last_call_ordinal,
            last_seq=self.last_seq,
            tmux_session_id=self.tmux_session_id,
        )


def test_checkpoint_checksum_roundtrip() -> None:
    row = _Row()
    assert verify_checkpoint(row)  # type: ignore[arg-type]
    row.checksum = "deadbeef"
    assert not verify_checkpoint(row)  # type: ignore[arg-type]
