"""Prepared binary frame inserts and transactional metadata notifications."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import UUID

from vidpg.contracts.frame import Codec, FrameEnvelope
from vidpg.protocol.validation import (
    DEFAULT_LIMITS,
    validate_frame_meta,
    validate_payload,
)

from .buckets import BucketNumber, bucket_table
from .connection import PgConnection

NOTIFY_CHANNEL = "vidpg_frame"


class FrameWriteError(ValueError):
    """Raised before SQL work when a frame cannot enter PostgreSQL."""


@dataclass(frozen=True, slots=True)
class PreparedStatement:
    """A fixed bucket SQL template executed with Psycopg prepare=True."""

    name: str
    bucket: BucketNumber
    sql: str
    notifies: bool


@dataclass(frozen=True, slots=True)
class InsertReceipt:
    """The committed identity and byte count of one inserted frame."""

    stream_id: UUID
    sequence: int
    bucket: BucketNumber
    payload_length: int
    notified: bool


def prepare_insert(conn: PgConnection, bucket: int) -> PreparedStatement:
    """Return the fixed prepared insert/notify template for ``bucket``."""

    selected = _validated_bucket(bucket)
    del conn
    return _statement(selected, notifies=True)


def insert_frame(
    conn: PgConnection,
    bucket: int,
    frame: FrameEnvelope,
) -> InsertReceipt:
    """Insert a validated frame without publishing a notification."""

    return _insert(conn, bucket, frame, notifies=False)


def insert_and_notify(
    conn: PgConnection,
    bucket: int,
    frame: FrameEnvelope,
) -> InsertReceipt:
    """Insert and transactionally signal a frame after commit."""

    return _insert(conn, bucket, frame, notifies=True)


def _insert(
    conn: PgConnection,
    bucket: int,
    frame: FrameEnvelope,
    *,
    notifies: bool,
) -> InsertReceipt:
    selected = _validated_bucket(bucket)
    _validate_frame(frame)
    statement = _statement(selected, notifies=notifies)
    params: tuple[Any, ...] = (
        frame.stream_id,
        frame.sequence,
        frame.captured_wall_us,
        datetime.now(UTC),
        _codec_to_database(frame.codec),
        frame.width,
        frame.height,
        bytes(frame.payload),
    )
    try:
        cursor = conn.execute(
            statement.sql,
            params,
            prepare=True,
            binary=True,
        )
        if notifies:
            cursor.fetchone()
        if not conn.autocommit:
            conn.commit()
    except BaseException:
        if not conn.autocommit:
            conn.rollback()
        raise
    return InsertReceipt(
        stream_id=frame.stream_id,
        sequence=frame.sequence,
        bucket=selected,
        payload_length=len(frame.payload),
        notified=notifies,
    )


def _statement(bucket: BucketNumber, *, notifies: bool) -> PreparedStatement:
    table = bucket_table(bucket)
    body = f"""
        INSERT INTO {table}
            (stream_id, seq, captured_us, relay_received_at,
             codec, width, height, frame)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    if notifies:
        body = f"""
            WITH inserted AS (
                {body.strip()}
                RETURNING stream_id, seq
            )
            SELECT pg_notify(
                '{NOTIFY_CHANNEL}',
                stream_id::text || ',{bucket},' || seq::text
            )
            FROM inserted
            """
    return PreparedStatement(
        name=f"insert_bucket_{bucket}",
        bucket=bucket,
        sql=" ".join(body.split()),
        notifies=notifies,
    )


def _validate_frame(frame: FrameEnvelope) -> None:
    if not isinstance(frame, FrameEnvelope):
        raise FrameWriteError("frame must be a FrameEnvelope")
    result = frame.validate()
    if not result.ok:
        raise FrameWriteError(result.message or "frame is invalid")
    metadata_result = validate_frame_meta(frame.meta(), DEFAULT_LIMITS)
    if not metadata_result.ok:
        raise FrameWriteError(metadata_result.message or "frame metadata is invalid")
    payload_result = validate_payload(frame.payload, DEFAULT_LIMITS, frame.codec)
    if not payload_result.ok:
        raise FrameWriteError(payload_result.message or "frame payload is invalid")
    if sha256(frame.payload).digest() != bytes(frame.payload_sha256):
        raise FrameWriteError("frame payload hash is invalid")


def _codec_to_database(codec: Codec | str) -> int:
    try:
        selected = Codec(codec)
    except (TypeError, ValueError) as exc:
        raise FrameWriteError("codec is not supported by P3") from exc
    if selected is Codec.JPEG:
        return 1
    if selected is Codec.SYNTHETIC:
        return 127
    raise FrameWriteError("codec is not supported by P3")


def _validated_bucket(bucket: int) -> BucketNumber:
    if isinstance(bucket, bool) or bucket not in (0, 1, 2):
        raise ValueError("bucket number must be one of 0, 1, or 2")
    return bucket  # type: ignore[return-value]


__all__ = [
    "FrameWriteError",
    "InsertReceipt",
    "PreparedStatement",
    "insert_and_notify",
    "insert_frame",
    "prepare_insert",
]
