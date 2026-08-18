"""Capacity-one newest-frame slot with explicit ownership and metrics."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from vidpg.contracts.frame import INT64_MAX, FrameEnvelope


class OfferOutcome(StrEnum):
    """Observable result of offering a frame to a latest-value slot."""

    ACCEPTED = "accepted"
    REPLACED = "replaced"
    WAITING_WHILE_INFLIGHT = "waiting_while_inflight"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class OfferResult:
    """Result of one offer, including the frame that was discarded if any."""

    outcome: OfferOutcome
    replaced_frame: FrameEnvelope | None = None
    reason: str | None = None

    @property
    def accepted(self) -> bool:
        """Return whether the offered frame is retained by the slot."""

        return self.outcome is not OfferOutcome.REJECTED


@dataclass(frozen=True, slots=True)
class SlotStats:
    """Bounded queue state and counters suitable for stage telemetry."""

    waiting_count: int
    inflight_count: int
    replaced_total: int
    rejected_total: int = 0
    failed_total: int = 0
    waiting_bytes: int = 0
    inflight_bytes: int = 0

    @property
    def dropped_total(self) -> int:
        """Return all offers not retained as a waiting or in-flight frame."""

        return self.replaced_total + self.rejected_total


class LatestSlot:
    """Event-loop-local slot with one waiting and one in-flight frame maximum.

    The slot is intentionally not thread-safe. One asyncio task ownership chain
    must call its methods; callers must not mutate an in-flight envelope.
    """

    def __init__(self) -> None:
        self._waiting: FrameEnvelope | None = None
        self._inflight: FrameEnvelope | None = None
        self._replaced_total = 0
        self._rejected_total = 0
        self._failed_total = 0

    def offer(self, frame: FrameEnvelope) -> OfferResult:
        """Retain a newer frame, replacing only a waiting frame."""

        if not isinstance(frame, FrameEnvelope):
            self._rejected_total += 1
            return OfferResult(
                OfferOutcome.REJECTED,
                reason="frame must be a FrameEnvelope",
            )
        if (
            not isinstance(frame.sequence, int)
            or isinstance(frame.sequence, bool)
            or not 1 <= frame.sequence <= INT64_MAX
        ):
            self._rejected_total += 1
            return OfferResult(
                OfferOutcome.REJECTED,
                reason="sequence must be an integer",
            )

        newest_sequence = 0
        if self._waiting is not None:
            newest_sequence = max(newest_sequence, self._waiting.sequence)
        if self._inflight is not None:
            newest_sequence = max(newest_sequence, self._inflight.sequence)
        if frame.sequence <= newest_sequence:
            self._rejected_total += 1
            return OfferResult(
                OfferOutcome.REJECTED,
                reason="sequence is not newer than the retained frame",
            )

        replaced = self._waiting
        self._waiting = frame
        if replaced is not None:
            self._replaced_total += 1
        if self._inflight is not None:
            return OfferResult(
                OfferOutcome.WAITING_WHILE_INFLIGHT,
                replaced_frame=replaced,
            )
        if replaced is not None:
            return OfferResult(OfferOutcome.REPLACED, replaced_frame=replaced)
        return OfferResult(OfferOutcome.ACCEPTED)

    def take(self) -> FrameEnvelope | None:
        """Transfer the newest waiting frame to in-flight ownership."""

        if self._inflight is not None or self._waiting is None:
            return None
        frame = self._waiting
        self._waiting = None
        self._inflight = frame
        return frame

    def mark_inflight(self, frame: FrameEnvelope) -> None:
        """Mark a frame as in-flight, preserving single-owner semantics."""

        if not isinstance(frame, FrameEnvelope):
            raise TypeError("frame must be a FrameEnvelope")
        if self._inflight is frame:
            return
        if self._inflight is not None:
            raise ValueError("a different frame is already in flight")
        if self._waiting is frame:
            self._waiting = None
        self._inflight = frame

    def clear_inflight(self, frame: FrameEnvelope) -> None:
        """Release successful in-flight ownership without touching waiting work."""

        self._require_inflight(frame)
        self._inflight = None

    def fail_inflight(self, frame: FrameEnvelope, reason: str | None = None) -> None:
        """Release failed in-flight work and count the failure."""

        self._require_inflight(frame)
        self._inflight = None
        self._failed_total += 1

    def stats(self) -> SlotStats:
        """Return bounded occupancy and replacement/failure counters."""

        return SlotStats(
            waiting_count=int(self._waiting is not None),
            inflight_count=int(self._inflight is not None),
            replaced_total=self._replaced_total,
            rejected_total=self._rejected_total,
            failed_total=self._failed_total,
            waiting_bytes=self._waiting.payload_length
            if self._waiting is not None
            else 0,
            inflight_bytes=(
                self._inflight.payload_length if self._inflight is not None else 0
            ),
        )

    def _require_inflight(self, frame: FrameEnvelope) -> None:
        if self._inflight is None:
            raise ValueError("no frame is in flight")
        if self._inflight is not frame:
            raise ValueError("frame does not own the in-flight slot")


__all__ = ["LatestSlot", "OfferOutcome", "OfferResult", "SlotStats"]
