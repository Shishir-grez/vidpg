"""Newest-frame reads across the active and previous PostgreSQL buckets."""

from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256
from typing import Any
from uuid import UUID

from vidpg.contracts.frame import INT64_MAX, Codec, FrameEnvelope
from vidpg.protocol.header import WIRE_RUN_ID
from vidpg.protocol.validation import DEFAULT_LIMITS

from .buckets import BucketNumber, active_bucket, bucket_table, previous_bucket
from .connection import PgConnection


class FrameFetchError(RuntimeError):
    """Raised when a persisted row violates the frame contract."""


def fetch_latest_after(
    conn: PgConnection,
    stream_id: UUID,
    watermark: int,
) -> FrameEnvelope | None:
    """Return the newest active/previous frame strictly above ``watermark``."""

    _validate_watermark(watermark)
    state = conn.execute(
        "SELECT generation, active FROM vidpg.bucket_state WHERE singleton"
    ).fetchone()
    if state is None:
        raise FrameFetchError("vidpg.bucket_state has no singleton row")
    generation = int(state[0])
    current = int(state[1])
    expected = active_bucket(generation)
    if current != expected:
        raise FrameFetchError("bucket_state active bucket disagrees with generation")
    candidates = (
        fetch_bucket_candidate(conn, current, stream_id, watermark),
        fetch_bucket_candidate(conn, previous_bucket(generation), stream_id, watermark),
    )
    if not conn.autocommit:
        conn.commit()
    return choose_newer(candidates)


def fetch_bucket_candidate(
    conn: PgConnection,
    bucket: int,
    stream_id: UUID,
    watermark: int,
) -> FrameEnvelope | None:
    """Fetch at most one newer row from one fixed bucket."""

    selected = _validated_bucket(bucket)
    _validate_watermark(watermark)
    row = conn.execute(
        f"""
        SELECT stream_id, seq, captured_us, relay_received_at,
               inserted_at, codec, width, height, frame
        FROM {bucket_table(selected)}
        WHERE stream_id = %s AND seq > %s
        ORDER BY seq DESC
        LIMIT 1
        """,
        (stream_id, watermark),
        prepare=True,
        binary=True,
    ).fetchone()
    if not conn.autocommit:
        conn.commit()
    if row is None:
        return None
    return _row_to_frame(row)


def choose_newer(
    candidates: Iterable[FrameEnvelope | None],
) -> FrameEnvelope | None:
    """Choose the greatest sequence without retaining a frame backlog."""

    selected: FrameEnvelope | None = None
    for candidate in candidates:
        if candidate is None:
            continue
        if selected is None or candidate.sequence > selected.sequence:
            selected = candidate
    return selected


def _row_to_frame(row: Any) -> FrameEnvelope:
    try:
        stream_id = _row_value(row, 0, "stream_id")
        sequence = _row_value(row, 1, "seq")
        captured_us = _row_value(row, 2, "captured_us")
        codec_value = _row_value(row, 5, "codec")
        width = _row_value(row, 6, "width")
        height = _row_value(row, 7, "height")
        payload = bytes(_row_value(row, 8, "frame"))
        codec = _codec_from_database(int(codec_value))
        frame = FrameEnvelope(
            version=1,
            experiment_id="v1-postgres",
            run_id=WIRE_RUN_ID,
            stream_id=stream_id
            if isinstance(stream_id, UUID)
            else UUID(str(stream_id)),
            sequence=int(sequence),
            captured_wall_us=int(captured_us),
            captured_monotonic_ns=0,
            codec=codec,
            width=int(width),
            height=int(height),
            payload_length=len(payload),
            payload_sha256=sha256(payload).digest(),
            payload=payload,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise FrameFetchError("persisted frame row has an invalid shape") from exc
    result = frame.validate()
    metadata_result = frame.meta().version == 1
    if not result.ok or not metadata_result:
        raise FrameFetchError(result.message or "persisted frame failed validation")
    if frame.payload_length > DEFAULT_LIMITS.max_frame_bytes:
        raise FrameFetchError("persisted frame exceeds the V1 payload limit")
    return frame


def _row_value(row: Any, index: int, name: str) -> Any:
    if hasattr(row, "keys"):
        return row[name]
    return row[index]


def _codec_from_database(value: int) -> Codec:
    if value == 1:
        return Codec.JPEG
    if value == 127:
        return Codec.SYNTHETIC
    raise FrameFetchError(f"unsupported persisted codec: {value}")


def _validate_watermark(watermark: int) -> None:
    if (
        not isinstance(watermark, int)
        or isinstance(watermark, bool)
        or not 0 <= watermark <= INT64_MAX
    ):
        raise ValueError("watermark must fit the non-negative signed bigint range")


def _validated_bucket(bucket: int) -> BucketNumber:
    if isinstance(bucket, bool) or bucket not in (0, 1, 2):
        raise ValueError("bucket number must be one of 0, 1, or 2")
    return bucket  # type: ignore[return-value]


__all__ = [
    "FrameFetchError",
    "choose_newer",
    "fetch_bucket_candidate",
    "fetch_latest_after",
]
