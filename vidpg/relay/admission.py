"""Pre-database WebSocket frame admission for authenticated relay sessions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vidpg.contracts.frame import FrameEnvelope
from vidpg.protocol import ProtocolError, ProtocolLimits, parse_frame_message
from vidpg.protocol.validation import (
    validate_frame_meta,
    validate_payload,
    validate_sequence,
)
from vidpg.queues import OfferResult

from .auth import Side, validate_side
from .errors import StreamOwnershipError
from .streams import assert_stream_owner

if TYPE_CHECKING:
    from .sessions import RelayStreamState, Session


HEADER_SIZE = 48
MAX_WS_MESSAGE_BYTES = 524_288 + HEADER_SIZE


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    """The outcome of one frame admission attempt."""

    accepted: bool
    frame: FrameEnvelope | None = None
    stream_state: RelayStreamState | None = None
    offer: OfferResult | None = None
    reason: str | None = None

    @property
    def ok(self) -> bool:
        """Alias for callers that use validation-style result handling."""

        return self.accepted


def admit_frame(
    session: Session,
    raw_frame: bytes | bytearray | memoryview | FrameEnvelope,
    source_side: str | Side | None = None,
) -> AdmissionResult:
    """Validate ownership, protocol, size, and sequence before queueing a frame."""

    frame_result = _parse_raw_frame(raw_frame)
    if isinstance(frame_result, AdmissionResult):
        return frame_result
    frame = frame_result

    selected_state: RelayStreamState | None = None
    try:
        if source_side is not None:
            selected_side = validate_side(source_side)
            if session.clients.get(selected_side) is None:
                return reject_frame("side is not joined")
            selected_state = session.stream_for_upload(selected_side)
            assert_stream_owner(frame, selected_state.stream_id)
        else:
            matches = [
                state
                for state in session.streams.values()
                if state.stream_id == frame.stream_id
            ]
            if len(matches) != 1:
                raise StreamOwnershipError()
            selected_state = matches[0]
    except (ValueError, StreamOwnershipError) as exc:
        return reject_frame(_reason(exc))

    limits = ProtocolLimits(max_frame_bytes=session.max_frame_bytes)
    metadata_result = validate_frame_meta(frame.meta(), limits)
    if not metadata_result.ok:
        return reject_frame(metadata_result.code or "INVALID_FRAME")
    payload_result = validate_payload(frame.payload, limits, frame.codec)
    if not payload_result.ok:
        return reject_frame(payload_result.code or "INVALID_FRAME")
    sequence_result = validate_sequence(selected_state.queue_state, frame.sequence)
    if not sequence_result.ok:
        return reject_frame(sequence_result.code or "STALE_FRAME")

    selected_state.note_accepted(frame.sequence)
    offer = offer_to_input_slot(selected_state, frame)
    if not offer.accepted:
        return reject_frame(offer.reason or "STALE_FRAME")
    selected_state.input_event.set()
    return AdmissionResult(
        accepted=True,
        frame=frame,
        stream_state=selected_state,
        offer=offer,
    )


def reject_frame(reason: str) -> AdmissionResult:
    """Build a non-throwing rejection result with no database side effect."""

    return AdmissionResult(accepted=False, reason=reason)


def offer_to_input_slot(
    stream_state: RelayStreamState,
    frame: FrameEnvelope,
) -> OfferResult:
    """Offer a validated frame to its capacity-one ingress slot."""

    if stream_state.stream_id != frame.stream_id:
        return _rejected_offer("frame stream does not match stream state")
    return stream_state.input_slot().offer(frame)


def _parse_raw_frame(
    raw_frame: bytes | bytearray | memoryview | FrameEnvelope,
) -> FrameEnvelope | AdmissionResult:
    if isinstance(raw_frame, FrameEnvelope):
        return raw_frame
    if not isinstance(raw_frame, (bytes, bytearray, memoryview)):
        return reject_frame("frame must be binary")
    if len(raw_frame) > MAX_WS_MESSAGE_BYTES:
        return reject_frame("OVERSIZE_PAYLOAD")
    try:
        return parse_frame_message(raw_frame)
    except ProtocolError as exc:
        return reject_frame(exc.code)


def _rejected_offer(reason: str) -> OfferResult:
    from vidpg.queues.latest_slot import OfferOutcome

    return OfferResult(OfferOutcome.REJECTED, reason=reason)


def _reason(error: BaseException) -> str:
    if isinstance(error, StreamOwnershipError):
        return error.code
    return str(error) or "INVALID_FRAME"


__all__ = [
    "HEADER_SIZE",
    "MAX_WS_MESSAGE_BYTES",
    "AdmissionResult",
    "admit_frame",
    "offer_to_input_slot",
    "reject_frame",
]
