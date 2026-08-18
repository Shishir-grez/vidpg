"""Safe three-bucket rotation with advisory locking and lock timeouts."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from threading import Event

import psycopg

from .buckets import BucketNumber, active_bucket, bucket_table, next_bucket
from .connection import PgConnection

LOGGER = logging.getLogger(__name__)
ROTATION_LOCK_KEY = 0x56495047524F5441
ROTATION_INTERVAL_SECONDS = 5.0
ROTATION_ALERT_FAILURES = 3


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """Outcome of clearing the next bucket inside a rotation transaction."""

    bucket: BucketNumber
    truncated: bool
    failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class RotationResult:
    """Observable result of one attempted generation advance."""

    advanced: bool
    lock_acquired: bool
    generation_before: int
    generation_after: int
    active_before: BucketNumber
    active_after: BucketNumber
    truncated_bucket: BucketNumber
    failure_reason: str | None = None


def run_rotation_once(
    conn: PgConnection,
    lock_timeout: int | str,
) -> RotationResult:
    """Clear the next bucket before publishing the next active generation."""

    timeout = _lock_timeout(lock_timeout)
    if not conn.autocommit:
        conn.rollback()
    lock_row = conn.execute(
        "SELECT pg_try_advisory_lock(%s)",
        (ROTATION_LOCK_KEY,),
    ).fetchone()
    if lock_row is None:
        raise RuntimeError("PostgreSQL did not return advisory lock state")
    acquired = bool(lock_row[0])
    if not conn.autocommit:
        conn.commit()
    if not acquired:
        generation, active = _read_state(conn)
        _finish_read(conn)
        return RotationResult(
            advanced=False,
            lock_acquired=False,
            generation_before=generation,
            generation_after=generation,
            active_before=active,
            active_after=active,
            truncated_bucket=next_bucket(generation),
            failure_reason="advisory_lock_busy",
        )

    generation = 0
    active = 0
    next_selected: BucketNumber = 0
    try:
        with conn.transaction():
            conn.execute(
                "SELECT set_config('lock_timeout', %s, true)",
                (timeout,),
            )
            generation, active = _read_state(conn)
            next_selected = next_bucket(generation)
            truncate_next_bucket(conn, next_selected)
            publish_next_active(conn)
        return RotationResult(
            advanced=True,
            lock_acquired=True,
            generation_before=generation,
            generation_after=generation + 1,
            active_before=active,
            active_after=active_bucket(generation + 1),
            truncated_bucket=next_selected,
        )
    except psycopg.DatabaseError as exc:
        if not _is_lock_timeout(exc):
            raise
        if not conn.autocommit:
            conn.rollback()
        return RotationResult(
            advanced=False,
            lock_acquired=True,
            generation_before=generation,
            generation_after=generation,
            active_before=active,
            active_after=active,
            truncated_bucket=next_selected,
            failure_reason="lock_timeout",
        )
    finally:
        _unlock(conn)


def truncate_next_bucket(conn: PgConnection, bucket: int) -> CleanupResult:
    """TRUNCATE one allowlisted bucket; caller owns the transaction."""

    selected = _validated_bucket(bucket)
    conn.execute(f"TRUNCATE TABLE {bucket_table(selected)}")
    return CleanupResult(bucket=selected, truncated=True)


def publish_next_active(conn: PgConnection) -> None:
    """Advance logged control state after the next bucket is cleared."""

    conn.execute(
        """
        UPDATE vidpg.bucket_state
        SET generation = generation + 1,
            active = (active + 1) % 3,
            switched_at = clock_timestamp()
        WHERE singleton
        """
    )


def rotation_loop(
    stop_event: Event,
    conn: PgConnection | None = None,
    *,
    interval_seconds: float = ROTATION_INTERVAL_SECONDS,
    lock_timeout: int | str = 250,
) -> None:
    """Run rotation until stopped using a supplied maintenance connection."""

    if conn is None:
        raise ValueError("rotation_loop requires a maintenance connection")
    failures = 0
    while not stop_event.is_set():
        result = run_rotation_once(conn, lock_timeout)
        if result.advanced:
            failures = 0
        else:
            failures += 1
            if failures >= ROTATION_ALERT_FAILURES:
                LOGGER.warning("PostgreSQL bucket rotation has failed repeatedly")
        stop_event.wait(interval_seconds)


def _read_state(conn: PgConnection) -> tuple[int, BucketNumber]:
    row = conn.execute(
        "SELECT generation, active FROM vidpg.bucket_state WHERE singleton"
    ).fetchone()
    if row is None:
        raise RuntimeError("vidpg.bucket_state has no singleton row")
    generation = int(row[0])
    active = int(row[1])
    expected = active_bucket(generation)
    if active != expected:
        raise RuntimeError("bucket_state active bucket disagrees with generation")
    return generation, expected


def _unlock(conn: PgConnection) -> None:
    try:
        if not conn.autocommit:
            conn.rollback()
        conn.execute(
            "SELECT pg_advisory_unlock(%s)",
            (ROTATION_LOCK_KEY,),
        )
        if not conn.autocommit:
            conn.commit()
    except psycopg.DatabaseError:
        LOGGER.exception("failed to release PostgreSQL rotation advisory lock")


def _finish_read(conn: PgConnection) -> None:
    if not conn.autocommit:
        conn.commit()


def _is_lock_timeout(exc: psycopg.DatabaseError) -> bool:
    return getattr(exc, "sqlstate", None) in {"55P03", "57014"}


def _lock_timeout(value: int | str) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        if value <= 0:
            raise ValueError("lock_timeout must be positive")
        return f"{value}ms"
    if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*(?:ms|s)", value):
        return value
    raise ValueError("lock_timeout must be positive milliseconds or a safe interval")


def _validated_bucket(bucket: int) -> BucketNumber:
    if isinstance(bucket, bool) or bucket not in (0, 1, 2):
        raise ValueError("bucket number must be one of 0, 1, or 2")
    return bucket  # type: ignore[return-value]


__all__ = [
    "ROTATION_ALERT_FAILURES",
    "ROTATION_INTERVAL_SECONDS",
    "ROTATION_LOCK_KEY",
    "CleanupResult",
    "RotationResult",
    "publish_next_active",
    "rotation_loop",
    "run_rotation_once",
    "truncate_next_bucket",
]
