"""Per-direction sequence and queue state."""

from __future__ import annotations

from uuid import UUID

from vidpg.contracts.frame import INT64_MAX

from .latest_slot import LatestSlot


class StreamState:
    """Own independent ingress, egress, accepted-sequence, and publish state."""

    def __init__(self, stream_id: UUID | None = None) -> None:
        self.stream_id = stream_id
        self._input = LatestSlot()
        self._output = LatestSlot()
        self._last_accepted = 0
        self._last_published = 0

    def input_slot(self) -> LatestSlot:
        """Return the stream's ingress latest-value slot."""

        return self._input

    def output_slot(self) -> LatestSlot:
        """Return the stream's egress latest-value slot."""

        return self._output

    def last_published(self) -> int:
        """Return the greatest sequence published to the receiver."""

        return self._last_published

    def note_published(self, sequence: int) -> None:
        """Advance the published watermark strictly monotonically."""

        _require_sequence(sequence)
        if sequence <= self._last_published:
            raise ValueError("published sequence must increase")
        self._last_published = sequence

    def last_accepted(self) -> int:
        """Return the greatest sequence accepted from the publisher."""

        return self._last_accepted

    def note_accepted(self, sequence: int) -> None:
        """Advance the publisher admission watermark strictly monotonically."""

        _require_sequence(sequence)
        if sequence <= self._last_accepted:
            raise ValueError("accepted sequence must increase")
        self._last_accepted = sequence


def _require_sequence(sequence: int) -> None:
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise TypeError("sequence must be an integer")
    if not 1 <= sequence <= INT64_MAX:
        raise ValueError("sequence must fit the V1 signed sequence range")


__all__ = ["StreamState"]
