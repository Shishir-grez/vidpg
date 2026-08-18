"""Fixed allowlisted bucket arithmetic for the PostgreSQL frame ring."""

from __future__ import annotations

from typing import Literal, cast

from vidpg.contracts.frame import INT64_MAX

BucketNumber = Literal[0, 1, 2]
BUCKET_COUNT = 3
BUCKET_TABLES: tuple[str, str, str] = (
    "vidpg.frame_bucket_0",
    "vidpg.frame_bucket_1",
    "vidpg.frame_bucket_2",
)


def active_bucket(generation: int) -> BucketNumber:
    """Return the bucket serving generation ``generation``."""

    return _generation_bucket(generation)


def previous_bucket(generation: int) -> BucketNumber:
    """Return the only older bucket that remains readable."""

    generation = _validate_generation(generation)
    return cast(BucketNumber, (generation + BUCKET_COUNT - 1) % BUCKET_COUNT)


def next_bucket(generation: int) -> BucketNumber:
    """Return the bucket that must be cleared before activation."""

    generation = _validate_generation(generation)
    return cast(BucketNumber, (generation + 1) % BUCKET_COUNT)


def bucket_table(number: int) -> str:
    """Return a fixed SQL table name for a validated bucket number."""

    if isinstance(number, bool) or number not in range(BUCKET_COUNT):
        raise ValueError("bucket number must be one of 0, 1, or 2")
    return BUCKET_TABLES[number]


def _generation_bucket(generation: int) -> BucketNumber:
    return cast(BucketNumber, _validate_generation(generation) % BUCKET_COUNT)


def _validate_generation(generation: int) -> int:
    if (
        not isinstance(generation, int)
        or isinstance(generation, bool)
        or not 0 <= generation <= INT64_MAX
    ):
        raise ValueError("generation must fit the non-negative signed bigint range")
    return generation


__all__ = [
    "BUCKET_COUNT",
    "BUCKET_TABLES",
    "BucketNumber",
    "active_bucket",
    "bucket_table",
    "next_bucket",
    "previous_bucket",
]
