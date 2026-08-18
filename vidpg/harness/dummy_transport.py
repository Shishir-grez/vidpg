"""Deterministic loopback transport with explicit one-shot fault injection."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import StrEnum

from vidpg.contracts.frame import FrameEnvelope

from .frame_factory import corrupt_frame, duplicate_frame


class Fault(StrEnum):
    DUPLICATE = "duplicate"
    DROP = "drop"
    REORDER = "reorder"
    CORRUPTION = "corruption"


class TransportEmpty(LookupError):
    """Raised when no frame is available to receive."""


@dataclass(frozen=True, slots=True)
class Receipt:
    sequence: int
    accepted: bool
    queued_count: int
    fault: Fault | None = None


class DummyTransport:
    """A bounded-by-test-input in-memory transport; it never uses real sockets."""

    def __init__(self) -> None:
        self._queue: deque[FrameEnvelope] = deque()
        self._faults: deque[Fault] = deque()
        self._reorder_hold: FrameEnvelope | None = None

    def send(self, frame: FrameEnvelope) -> Receipt:
        validation = frame.validate()
        if not validation.ok:
            raise ValueError(validation.message or validation.code or "invalid frame")
        fault = self._faults.popleft() if self._faults else None
        if fault is Fault.DROP:
            return Receipt(frame.sequence, accepted=False, queued_count=0, fault=fault)
        if fault is Fault.DUPLICATE:
            self._queue.append(frame)
            self._queue.append(duplicate_frame(frame))
            return Receipt(frame.sequence, accepted=True, queued_count=2, fault=fault)
        if fault is Fault.CORRUPTION:
            self._queue.append(corrupt_frame(frame, "payload"))
            return Receipt(frame.sequence, accepted=True, queued_count=1, fault=fault)
        if fault is Fault.REORDER:
            if self._reorder_hold is None:
                self._reorder_hold = frame
                return Receipt(
                    frame.sequence, accepted=True, queued_count=0, fault=fault
                )
            self._queue.append(frame)
            self._queue.append(self._reorder_hold)
            self._reorder_hold = None
            return Receipt(frame.sequence, accepted=True, queued_count=2, fault=fault)

        if self._reorder_hold is not None:
            self._queue.append(frame)
            self._queue.append(self._reorder_hold)
            self._reorder_hold = None
            return Receipt(frame.sequence, accepted=True, queued_count=2, fault=fault)

        self._queue.append(frame)
        return Receipt(frame.sequence, accepted=True, queued_count=1)

    def receive(self) -> FrameEnvelope:
        if not self._queue and self._reorder_hold is not None:
            self._queue.append(self._reorder_hold)
            self._reorder_hold = None
        if not self._queue:
            raise TransportEmpty("dummy transport has no queued frame")
        return self._queue.popleft()

    def inject_fault(self, fault: Fault | str) -> None:
        try:
            self._faults.append(Fault(fault))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown dummy transport fault: {fault}") from exc

    @property
    def pending_count(self) -> int:
        return len(self._queue) + int(self._reorder_hold is not None)
