from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from sqlalchemy import event
from sqlalchemy.orm import Session


_SESSION_EVENT_ROWS_KEY = "agentic_data_platform.run_event_fanout_rows"
_SESSION_EVENT_LISTENERS_KEY = "agentic_data_platform.run_event_fanout_listeners"
_DEFAULT_CHANNEL_PREFIX = "adp:run-events"
_DEFAULT_HOT_BUFFER_TTL_SECONDS = 300


@dataclass(frozen=True)
class RunEventSignal:
    run_id: str
    seq: int
    event_type: str
    event_id: str
    created_at: str

    @classmethod
    def from_row(cls, row: Any) -> "RunEventSignal":
        seq = int(getattr(row, "id", 0) or 0)
        if seq <= 0:
            raise ValueError("run event row must have a positive sequence before fanout")
        return cls(
            run_id=str(row.run_id),
            seq=seq,
            event_type=str(row.event_type),
            event_id=str(row.event_id),
            created_at=_datetime(getattr(row, "created_at")),
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> "RunEventSignal":
        data = json.loads(payload.decode("utf-8") if isinstance(payload, bytes) else payload)
        if not isinstance(data, dict):
            raise ValueError("run event signal payload must be an object")
        return cls(
            run_id=str(data["run_id"]),
            seq=int(data["seq"]),
            event_type=str(data["event_type"]),
            event_id=str(data["event_id"]),
            created_at=str(data["created_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seq": self.seq,
            "event_type": self.event_type,
            "event_id": self.event_id,
            "created_at": self.created_at,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


class RunEventFanout(Protocol):
    enabled: bool

    def publish(self, signal: RunEventSignal) -> None:
        ...

    def wait_for_event(self, *, run_id: str, after_seq: int, timeout_seconds: float) -> bool:
        ...


class NoopRunEventFanout:
    enabled = False

    def publish(self, signal: RunEventSignal) -> None:
        return None

    def wait_for_event(self, *, run_id: str, after_seq: int, timeout_seconds: float) -> bool:
        return False


class InMemoryRunEventFanout:
    enabled = True

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._signals_by_run: dict[str, list[RunEventSignal]] = {}
        self.wait_calls: list[tuple[str, int, float]] = []

    def publish(self, signal: RunEventSignal) -> None:
        with self._condition:
            self._signals_by_run.setdefault(signal.run_id, []).append(signal)
            self._condition.notify_all()

    def wait_for_event(self, *, run_id: str, after_seq: int, timeout_seconds: float) -> bool:
        self.wait_calls.append((run_id, after_seq, timeout_seconds))
        with self._condition:
            if any(signal.seq > after_seq for signal in self._signals_by_run.get(run_id, ())):
                return True
            timeout_seconds = max(float(timeout_seconds), 0.0)
            if timeout_seconds <= 0:
                return False
            deadline = time.monotonic() + timeout_seconds
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
                if any(signal.seq > after_seq for signal in self._signals_by_run.get(run_id, ())):
                    return True

    def signals_for_run(self, run_id: str) -> list[RunEventSignal]:
        with self._condition:
            return list(self._signals_by_run.get(run_id, ()))


class RedisRunEventFanout:
    enabled = True

    def __init__(
        self,
        redis_url: str,
        *,
        hot_buffer_size: int = 100,
        channel_prefix: str = _DEFAULT_CHANNEL_PREFIX,
        hot_buffer_ttl_seconds: int = _DEFAULT_HOT_BUFFER_TTL_SECONDS,
    ) -> None:
        if not redis_url.strip():
            raise ValueError("redis_url must be a non-empty string")
        if hot_buffer_size < 0:
            raise ValueError("hot_buffer_size must be non-negative")
        self.redis_url = redis_url
        self.hot_buffer_size = hot_buffer_size
        self.channel_prefix = channel_prefix
        self.hot_buffer_ttl_seconds = hot_buffer_ttl_seconds
        self._client = None

    def publish(self, signal: RunEventSignal) -> None:
        try:
            payload = signal.to_json()
            if self.hot_buffer_size > 0:
                pipe = self._redis().pipeline()
                pipe.lpush(self._hot_buffer_key(signal.run_id), payload)
                pipe.ltrim(self._hot_buffer_key(signal.run_id), 0, self.hot_buffer_size - 1)
                pipe.expire(self._hot_buffer_key(signal.run_id), self.hot_buffer_ttl_seconds)
                pipe.publish(self._channel(signal.run_id), payload)
                pipe.execute()
            else:
                self._redis().publish(self._channel(signal.run_id), payload)
        except Exception:
            return None

    def wait_for_event(self, *, run_id: str, after_seq: int, timeout_seconds: float) -> bool:
        timeout_seconds = max(float(timeout_seconds), 0.0)
        try:
            if self._hot_buffer_has_event(run_id=run_id, after_seq=after_seq):
                return True
            if timeout_seconds <= 0:
                return False
            with self._redis().pubsub(ignore_subscribe_messages=True) as pubsub:
                pubsub.subscribe(self._channel(run_id))
                if self._hot_buffer_has_event(run_id=run_id, after_seq=after_seq):
                    return True
                deadline = time.monotonic() + timeout_seconds
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    message = pubsub.get_message(timeout=min(remaining, 0.25))
                    if message is None:
                        continue
                    data = message.get("data")
                    if data is None:
                        continue
                    signal = RunEventSignal.from_json(data)
                    if signal.run_id == run_id and signal.seq > after_seq:
                        return True
        except Exception:
            if timeout_seconds > 0:
                time.sleep(timeout_seconds)
            return False

    def _hot_buffer_has_event(self, *, run_id: str, after_seq: int) -> bool:
        if self.hot_buffer_size <= 0:
            return False
        for item in self._redis().lrange(self._hot_buffer_key(run_id), 0, self.hot_buffer_size - 1):
            try:
                signal = RunEventSignal.from_json(item)
            except Exception:
                continue
            if signal.run_id == run_id and signal.seq > after_seq:
                return True
        return False

    def _redis(self):
        if self._client is None:
            from redis import Redis

            self._client = Redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1,
                health_check_interval=30,
            )
        return self._client

    def _channel(self, run_id: str) -> str:
        return f"{self.channel_prefix}:{run_id}"

    def _hot_buffer_key(self, run_id: str) -> str:
        return f"{self._channel(run_id)}:hot"


_NOOP_FANOUT = NoopRunEventFanout()
_ACTIVE_FANOUT: RunEventFanout = _NOOP_FANOUT


def configure_run_event_fanout(fanout: RunEventFanout | None) -> RunEventFanout:
    global _ACTIVE_FANOUT
    _ACTIVE_FANOUT = fanout or _NOOP_FANOUT
    return _ACTIVE_FANOUT


def build_run_event_fanout(settings: Any) -> RunEventFanout:
    if not getattr(settings, "run_event_redis_fanout_enabled", False):
        return _NOOP_FANOUT
    redis_url = str(getattr(settings, "redis_url", "") or "").strip()
    if not redis_url:
        return _NOOP_FANOUT
    return RedisRunEventFanout(
        redis_url,
        hot_buffer_size=int(getattr(settings, "run_event_redis_hot_buffer_size", 100)),
    )


def current_run_event_fanout() -> RunEventFanout:
    return _ACTIVE_FANOUT


def queue_run_event_fanout_after_commit(session: Session, row: Any) -> None:
    rows = session.info.setdefault(_SESSION_EVENT_ROWS_KEY, [])
    rows.append(row)
    if session.info.get(_SESSION_EVENT_LISTENERS_KEY):
        return
    session.info[_SESSION_EVENT_LISTENERS_KEY] = True
    event.listen(session, "after_commit", _publish_queued_run_events, once=True)
    event.listen(session, "after_rollback", _discard_queued_run_events, once=True)


def _publish_queued_run_events(session: Session) -> None:
    rows = list(session.info.pop(_SESSION_EVENT_ROWS_KEY, []))
    session.info.pop(_SESSION_EVENT_LISTENERS_KEY, None)
    fanout = current_run_event_fanout()
    if not fanout.enabled:
        return
    for row in rows:
        try:
            fanout.publish(RunEventSignal.from_row(row))
        except Exception:
            continue


def _discard_queued_run_events(session: Session) -> None:
    session.info.pop(_SESSION_EVENT_ROWS_KEY, None)
    session.info.pop(_SESSION_EVENT_LISTENERS_KEY, None)


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
