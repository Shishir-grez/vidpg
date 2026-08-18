"""Deterministic synthetic frames and explicit corruption helpers."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from uuid import UUID, uuid5

from vidpg.contracts.frame import Codec, FrameEnvelope

_RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
_STREAM_NAMESPACE = UUID("00000000-0000-0000-0000-000000000002")
_STREAM_ID = uuid5(_STREAM_NAMESPACE, "p1-dummy-stream")
_BASE_WALL_US = 1_700_000_000_000_000


def make_frame(
    sequence: int,
    size: int = 70_000,
    codec: Codec | str = Codec.JPEG,
) -> FrameEnvelope:
    """Build a repeatable frame with exact payload size and identity."""

    if not isinstance(sequence, int) or sequence < 1:
        raise ValueError("sequence must be a positive integer")
    if not isinstance(size, int) or size < 4:
        raise ValueError("size must be an integer of at least four bytes")
    payload = _payload(sequence, size, codec)
    return FrameEnvelope(
        version=1,
        experiment_id="p1-dummy",
        run_id=_RUN_ID,
        stream_id=_STREAM_ID,
        sequence=sequence,
        captured_wall_us=_BASE_WALL_US + sequence * 100_000,
        captured_monotonic_ns=sequence * 100_000_000,
        codec=codec,
        width=640,
        height=360,
        payload_length=len(payload),
        payload_sha256=sha256(payload).digest(),
        payload=payload,
    )


def make_real_jpeg_frame(sequence: int, profile: str = "static") -> FrameEnvelope:
    """Build a deterministic marker-valid JPEG-shaped synthetic frame."""

    if profile not in {"static", "motion"}:
        raise ValueError("profile must be static or motion")
    return make_frame(sequence=sequence, size=70_000, codec=Codec.JPEG)


def corrupt_frame(frame: FrameEnvelope, mode: str) -> FrameEnvelope:
    """Return an intentionally invalid copy while preserving frame identity."""

    if mode == "payload":
        payload = bytearray(frame.payload)
        payload[0] ^= 0x01
        return replace(frame, payload=bytes(payload))
    if mode == "hash":
        digest = bytearray(frame.payload_sha256)
        digest[0] ^= 0x01
        return replace(frame, payload_sha256=bytes(digest))
    if mode == "length":
        return replace(frame, payload_length=frame.payload_length + 1)
    if mode == "truncate":
        return replace(frame, payload=frame.payload[:-1])
    raise ValueError("mode must be payload, hash, length, or truncate")


def duplicate_frame(frame: FrameEnvelope) -> FrameEnvelope:
    """Return a same-identity copy representing a duplicate delivery."""

    return replace(frame)


def change_sequence(frame: FrameEnvelope, sequence: int) -> FrameEnvelope:
    """Return a copy with a changed sequence and unchanged payload hash."""

    if not isinstance(sequence, int) or sequence < 0:
        raise ValueError("sequence must be a nonnegative integer")
    return replace(frame, sequence=sequence)


def corrupt_payload(frame: FrameEnvelope, mode: str = "payload") -> FrameEnvelope:
    """Compatibility name from the shared test-helper contract."""

    return corrupt_frame(frame, mode)


def rename_sequence(frame: FrameEnvelope, sequence: int) -> FrameEnvelope:
    """Compatibility name from the shared test-helper contract."""

    return change_sequence(frame, sequence)


def _payload(sequence: int, size: int, codec: Codec | str) -> bytes:
    try:
        selected = Codec(codec)
    except (TypeError, ValueError):
        selected = None
    body_size = size - 4 if selected is Codec.JPEG else size
    body = bytes((index * 31 + sequence * 17) % 256 for index in range(body_size))
    if selected is Codec.JPEG:
        return b"\xff\xd8" + body + b"\xff\xd9"
    return body
