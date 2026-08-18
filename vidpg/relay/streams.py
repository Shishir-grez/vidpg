"""Deterministic directional stream ownership for authenticated sessions."""

from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5

from vidpg.contracts.frame import FrameEnvelope

from .auth import Side, validate_side
from .errors import StreamOwnershipError

# The standard URL namespace is stable across processes and implementations.
STREAM_NAMESPACE = NAMESPACE_URL


def derive_upload_stream(session_id: UUID | str, side: str | Side) -> UUID:
    """Return the UUID of the stream a side is authorized to publish."""

    session = _coerce_session_id(session_id)
    selected_side = validate_side(side)
    direction = "ab" if selected_side == Side.A else "ba"
    return uuid5(STREAM_NAMESPACE, f"{session}:{direction}")


def derive_incoming_stream(session_id: UUID | str, side: str | Side) -> UUID:
    """Return the UUID delivered to a side from its peer."""

    session = _coerce_session_id(session_id)
    selected_side = validate_side(side)
    direction = "ba" if selected_side == Side.A else "ab"
    return uuid5(STREAM_NAMESPACE, f"{session}:{direction}")


def destination_side(source_side: str | Side) -> Side:
    """Return the opposite destination side for one directional upload."""

    selected_side = validate_side(source_side)
    return Side.B if selected_side == Side.A else Side.A


def assert_stream_owner(
    frame: FrameEnvelope,
    expected_stream: UUID | str,
) -> None:
    """Reject a frame before database work when its stream is not authorized."""

    if not isinstance(frame, FrameEnvelope):
        raise StreamOwnershipError()
    expected = _coerce_uuid(expected_stream, "expected_stream")
    if frame.stream_id != expected:
        raise StreamOwnershipError()


def _coerce_session_id(value: UUID | str) -> UUID:
    return _coerce_uuid(value, "session_id")


def _coerce_uuid(value: UUID | str, name: str) -> UUID:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be a UUID") from exc
    raise TypeError(f"{name} must be a UUID")


__all__ = [
    "STREAM_NAMESPACE",
    "assert_stream_owner",
    "destination_side",
    "derive_incoming_stream",
    "derive_upload_stream",
]
