"""Cheap, pre-expensive-work validation for V1 frame metadata and payloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from vidpg.contracts.frame import (
    INT64_MAX,
    INT64_MIN,
    UINT16_MAX,
    UINT32_MAX,
    UINT64_MAX,
    Codec,
    FrameMeta,
    ValidationResult,
)

if TYPE_CHECKING:
    from vidpg.queues.stream_state import StreamState


@dataclass(frozen=True, slots=True)
class ProtocolLimits:
    """V1 admission limits applied before transport or database work."""

    max_frame_bytes: int = 524_288
    min_width: int = 160
    max_width: int = 1_280
    min_height: int = 120
    max_height: int = 720
    min_sequence: int = 1
    max_sequence: int = INT64_MAX


DEFAULT_LIMITS = ProtocolLimits()
LimitsInput = ProtocolLimits | Mapping[str, int] | None


def validate_frame_meta(
    meta: FrameMeta,
    limits: LimitsInput = None,
) -> ValidationResult:
    """Validate metadata that can be represented by the V1 frame header."""

    if not isinstance(meta, FrameMeta):
        return ValidationResult.invalid("BAD_TYPE", "meta must be FrameMeta", "meta")
    selected_limits = _coerce_limits(limits)

    if not _is_int(meta.version) or meta.version != 1:
        return ValidationResult.invalid(
            "BAD_VERSION", "V1 version must be 1", "version"
        )
    if not isinstance(meta.experiment_id, str) or not meta.experiment_id.strip():
        return ValidationResult.invalid(
            "BAD_TYPE", "experiment_id must be non-empty", "experiment_id"
        )
    if not isinstance(meta.run_id, UUID) or not isinstance(meta.stream_id, UUID):
        return ValidationResult.invalid(
            "BAD_TYPE", "UUID fields must be UUIDs", "stream_id"
        )

    if not _is_int(meta.sequence):
        return ValidationResult.invalid(
            "BAD_SEQUENCE", "sequence must be an integer", "sequence"
        )
    if (
        not selected_limits.min_sequence
        <= meta.sequence
        <= selected_limits.max_sequence
    ):
        return ValidationResult.invalid(
            "BAD_SEQUENCE", "sequence is outside the V1 range", "sequence"
        )
    if meta.sequence > UINT64_MAX:
        return ValidationResult.invalid(
            "BAD_SEQUENCE", "sequence exceeds uint64", "sequence"
        )

    if (
        not _is_int(meta.captured_wall_us)
        or not INT64_MIN <= meta.captured_wall_us <= INT64_MAX
    ):
        return ValidationResult.invalid(
            "BAD_TIMESTAMP",
            "captured_wall_us must fit signed int64",
            "captured_wall_us",
        )
    if (
        not _is_int(meta.captured_monotonic_ns)
        or not 0 <= meta.captured_monotonic_ns <= UINT64_MAX
    ):
        return ValidationResult.invalid(
            "BAD_TIMESTAMP",
            "captured_monotonic_ns must fit unsigned uint64",
            "captured_monotonic_ns",
        )

    try:
        codec = Codec(meta.codec)
    except (TypeError, ValueError):
        return ValidationResult.invalid(
            "BAD_CODEC", "codec is not supported by V1", "codec"
        )
    if codec not in {Codec.JPEG, Codec.SYNTHETIC}:
        return ValidationResult.invalid(
            "BAD_CODEC", "codec is reserved or unsupported by V1", "codec"
        )

    for field, value, minimum, maximum in (
        ("width", meta.width, selected_limits.min_width, selected_limits.max_width),
        ("height", meta.height, selected_limits.min_height, selected_limits.max_height),
    ):
        if not _is_int(value) or not minimum <= value <= maximum or value > UINT16_MAX:
            return ValidationResult.invalid(
                "BAD_DIMENSION", f"{field} is outside the V1 range", field
            )

    if not _is_int(meta.payload_length) or not 1 <= meta.payload_length <= UINT32_MAX:
        return ValidationResult.invalid(
            "BAD_LENGTH", "payload_length must be a positive uint32", "payload_length"
        )
    if meta.payload_length > selected_limits.max_frame_bytes:
        return ValidationResult.invalid(
            "OVERSIZE_PAYLOAD",
            "payload exceeds the configured maximum",
            "payload_length",
        )
    return ValidationResult.valid()


def validate_payload(
    payload: bytes | bytearray | memoryview,
    limits: LimitsInput = None,
    codec: Codec | str = Codec.JPEG,
) -> ValidationResult:
    """Validate payload size and cheap codec boundaries without decoding."""

    selected_limits = _coerce_limits(limits)
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        return ValidationResult.invalid(
            "BAD_TYPE", "payload must be bytes-like", "payload"
        )
    payload_length = len(payload)
    if payload_length == 0:
        return ValidationResult.invalid(
            "BAD_LENGTH", "payload cannot be empty", "payload"
        )
    if payload_length > selected_limits.max_frame_bytes:
        return ValidationResult.invalid(
            "OVERSIZE_PAYLOAD", "payload exceeds the configured maximum", "payload"
        )

    try:
        selected_codec = Codec(codec)
    except (TypeError, ValueError):
        return ValidationResult.invalid(
            "BAD_CODEC", "codec is not supported by V1", "codec"
        )
    if selected_codec is Codec.JPEG:
        raw = bytes(payload)
        if raw[:2] != b"\xff\xd8" or raw[-2:] != b"\xff\xd9":
            return ValidationResult.invalid(
                "BAD_JPEG_MARKER",
                "JPEG payload must contain SOI and EOI markers",
                "payload",
            )
    elif selected_codec is not Codec.SYNTHETIC:
        return ValidationResult.invalid(
            "BAD_CODEC", "codec is reserved or unsupported by V1", "codec"
        )
    return ValidationResult.valid()


def validate_sequence(stream_state: StreamState, sequence: int) -> ValidationResult:
    """Check that a publisher's next sequence is strictly increasing."""

    if not _is_int(sequence):
        return ValidationResult.invalid(
            "BAD_SEQUENCE", "sequence must be an integer", "sequence"
        )
    if sequence < DEFAULT_LIMITS.min_sequence or sequence > DEFAULT_LIMITS.max_sequence:
        return ValidationResult.invalid(
            "BAD_SEQUENCE", "sequence is outside the V1 range", "sequence"
        )
    try:
        last_accepted = stream_state.last_accepted()
    except AttributeError:
        return ValidationResult.invalid(
            "BAD_TYPE", "stream_state must expose last_accepted()", "stream_state"
        )
    if sequence <= last_accepted:
        return ValidationResult.invalid(
            "BAD_SEQUENCE",
            "sequence must be greater than the last accepted sequence",
            "sequence",
        )
    return ValidationResult.valid()


def _coerce_limits(limits: LimitsInput) -> ProtocolLimits:
    if limits is None:
        return DEFAULT_LIMITS
    if isinstance(limits, ProtocolLimits):
        return limits
    return ProtocolLimits(
        max_frame_bytes=limits.get("max_frame_bytes", DEFAULT_LIMITS.max_frame_bytes),
        min_width=limits.get("min_width", DEFAULT_LIMITS.min_width),
        max_width=limits.get("max_width", DEFAULT_LIMITS.max_width),
        min_height=limits.get("min_height", DEFAULT_LIMITS.min_height),
        max_height=limits.get("max_height", DEFAULT_LIMITS.max_height),
        min_sequence=limits.get("min_sequence", DEFAULT_LIMITS.min_sequence),
        max_sequence=limits.get("max_sequence", DEFAULT_LIMITS.max_sequence),
    )


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


__all__ = [
    "DEFAULT_LIMITS",
    "ProtocolLimits",
    "validate_frame_meta",
    "validate_payload",
    "validate_sequence",
]
