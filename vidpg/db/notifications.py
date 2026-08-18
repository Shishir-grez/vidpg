"""Metadata-only LISTEN/NOTIFY and dirty-generation coalescing."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from vidpg.contracts.frame import INT64_MAX

from .buckets import active_bucket, bucket_table, previous_bucket
from .connection import PgConnection

NOTIFY_CHANNEL = "vidpg_frame"
MAX_NOTIFICATION_BYTES = 7_999


class NotificationParseError(ValueError):
    """Raised for malformed or unsafe frame notification payloads."""


@dataclass(frozen=True, slots=True)
class FrameSignal:
    """The only frame information carried by PostgreSQL NOTIFY."""

    stream_id: UUID
    bucket: int
    sequence: int


@dataclass(slots=True)
class DirtyState:
    """Per-stream state used to coalesce notification bursts."""

    latest_signaled_seq: int = 0
    dirty_generation: int = 0
    fetch_in_progress: bool = False
    last_fetched_seq: int = 0
    last_published_seq: int = 0


class NotificationStream:
    """Safe iterator over a dedicated Psycopg notification connection."""

    def __init__(self, conn: PgConnection) -> None:
        self._conn = conn
        self.invalid_total = 0
        self.last_error: str | None = None

    def read(self, timeout: float | None = None) -> FrameSignal | None:
        """Read one signal, ignoring and recording malformed payloads."""

        for notification in self._conn.notifies(timeout=timeout, stop_after=1):
            payload = cast(
                str | bytes | bytearray | memoryview | None,
                getattr(notification, "payload", None),
            )
            if payload is None:
                self.invalid_total += 1
                self.last_error = "notification has no payload"
                continue
            try:
                return parse_frame_notification(payload)
            except NotificationParseError as exc:
                self.invalid_total += 1
                self.last_error = str(exc)
        return None

    def __iter__(self) -> Iterator[FrameSignal]:
        while True:
            signal = self.read()
            if signal is not None:
                yield signal


def listen_frames(conn: PgConnection) -> NotificationStream:
    """LISTEN, commit setup, and return a dedicated notification stream."""

    conn.execute(f"LISTEN {NOTIFY_CHANNEL}")
    if not conn.autocommit:
        conn.commit()
    return NotificationStream(conn)


def parse_frame_notification(
    payload: str | bytes | bytearray | memoryview,
) -> FrameSignal:
    """Parse exactly ``stream_uuid,bucket,sequence`` metadata."""

    if isinstance(payload, str):
        text = payload
        size = len(payload.encode("ascii", errors="strict"))
    elif isinstance(payload, (bytes, bytearray, memoryview)):
        raw = bytes(payload)
        size = len(raw)
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError as exc:
            raise NotificationParseError("notification must be ASCII") from exc
    else:
        raise NotificationParseError("notification payload must be text or bytes")
    if size >= 8_000:
        raise NotificationParseError("notification payload is too large")
    parts = text.split(",")
    if len(parts) != 3 or any(not part for part in parts):
        raise NotificationParseError("notification must have exactly three fields")
    try:
        stream_id = UUID(parts[0])
        bucket = int(parts[1], 10)
        sequence = int(parts[2], 10)
    except (TypeError, ValueError) as exc:
        raise NotificationParseError("notification contains invalid metadata") from exc
    if bucket not in (0, 1, 2):
        raise NotificationParseError("notification bucket is outside 0..2")
    if not 1 <= sequence <= INT64_MAX:
        raise NotificationParseError("notification sequence is outside int64 range")
    return FrameSignal(stream_id=stream_id, bucket=bucket, sequence=sequence)


def mark_dirty(registry: dict[UUID, DirtyState], signal: FrameSignal) -> None:
    """Advance one stream's dirty generation for a newer signal only."""

    state = registry.setdefault(signal.stream_id, DirtyState())
    if signal.sequence > state.latest_signaled_seq:
        state.latest_signaled_seq = signal.sequence
        state.dirty_generation += 1


def rescan_latest_after_listen(conn: PgConnection) -> list[FrameSignal]:
    """Rescan active/previous newest rows after LISTEN setup or reconnect."""

    state = conn.execute(
        "SELECT generation, active FROM vidpg.bucket_state WHERE singleton"
    ).fetchone()
    if state is None:
        raise NotificationParseError("bucket_state has no singleton row")
    generation = int(state[0])
    current = int(state[1])
    if current != active_bucket(generation):
        raise NotificationParseError("bucket_state active bucket is inconsistent")
    candidates: dict[UUID, FrameSignal] = {}
    for bucket in (current, previous_bucket(generation)):
        rows = conn.execute(
            f"""
            SELECT DISTINCT ON (stream_id) stream_id, seq
            FROM {bucket_table(bucket)}
            ORDER BY stream_id, seq DESC
            """,
            prepare=True,
            binary=True,
        ).fetchall()
        for row in rows:
            stream_id = row[0] if isinstance(row[0], UUID) else UUID(str(row[0]))
            signal = FrameSignal(stream_id, bucket, int(row[1]))
            previous = candidates.get(stream_id)
            if previous is None or signal.sequence > previous.sequence:
                candidates[stream_id] = signal
    if not conn.autocommit:
        conn.commit()
    return sorted(candidates.values(), key=lambda item: str(item.stream_id))


__all__ = [
    "DirtyState",
    "FrameSignal",
    "MAX_NOTIFICATION_BYTES",
    "NOTIFY_CHANNEL",
    "NotificationParseError",
    "NotificationStream",
    "listen_frames",
    "mark_dirty",
    "parse_frame_notification",
    "rescan_latest_after_listen",
]
