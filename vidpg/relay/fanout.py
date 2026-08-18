"""One-destination fanout and notification-driven newest-frame fetch policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from vidpg.contracts.frame import FrameEnvelope
from vidpg.db.notifications import FrameSignal
from vidpg.observability import MetricRegistry

if TYPE_CHECKING:
    from .sessions import ClientState, RelayStreamState


@dataclass(frozen=True, slots=True)
class FetchDecision:
    """A single permitted newest-row fetch for one stream."""

    should_fetch: bool
    watermark: int
    generation: int


class Fanout:
    """Maintain one subscriber per directional stream and coalesce dirty signals."""

    def __init__(self, metrics: MetricRegistry | None = None) -> None:
        self._streams: dict[UUID, RelayStreamState] = {}
        self._subscribers: dict[UUID, ClientState] = {}
        self._pending_signals: dict[UUID, int] = {}
        self.metrics = metrics
        self.published_total = 0
        self.replacement_total = 0
        self.disconnected_total = 0
        self.fetch_started_total = 0

    def register(self, stream_state: RelayStreamState) -> None:
        """Register a stream and apply any signal observed before session setup."""

        self._streams[stream_state.stream_id] = stream_state
        pending = self._pending_signals.pop(stream_state.stream_id, None)
        if pending is not None:
            self._mark_dirty(stream_state, pending)

    def publish(self, stream_id: UUID, frame: FrameEnvelope) -> None:
        """Offer one fetched frame to the current destination output slot."""

        stream_state = self._streams.get(stream_id)
        if stream_state is None or frame.stream_id != stream_id:
            return
        subscriber = self._subscribers.get(stream_id)
        if subscriber is None or subscriber.closed or subscriber.socket is None:
            self.disconnected_total += 1
            return
        result = subscriber.offer_frame(frame)
        if result.replaced_frame is not None:
            self.replacement_total += 1
            self._inc(
                "vidpg_frames_replaced_total",
                labels={"stage": "relay_output"},
            )
            self._inc("vidpg_relay_output_replaced_total")
        self.published_total += 1

    def subscribe(self, stream_id: UUID, client_state: ClientState) -> None:
        """Attach the sole destination subscriber for a directional stream."""

        stream_state = self._streams.get(stream_id)
        if stream_state is None:
            raise KeyError(f"unknown stream: {stream_id}")
        previous = self._subscribers.get(stream_id)
        if previous is not None and previous is not client_state:
            self.unsubscribe(previous)
        self._subscribers[stream_id] = client_state
        stream_state.subscriber = client_state

    def unsubscribe(self, client_state: ClientState) -> None:
        """Remove every subscription owned by a disconnected client."""

        for stream_id, subscriber in tuple(self._subscribers.items()):
            if subscriber is client_state:
                self._subscribers.pop(stream_id, None)
                stream_state = self._streams.get(stream_id)
                if stream_state is not None and stream_state.subscriber is client_state:
                    stream_state.subscriber = None
        client_state.reset_output()

    def notify(self, signal: FrameSignal) -> None:
        """Mark a stream dirty using only the greatest committed signal sequence."""

        stream_state = self._streams.get(signal.stream_id)
        if stream_state is None:
            previous = self._pending_signals.get(signal.stream_id, 0)
            if signal.sequence > previous:
                self._pending_signals[signal.stream_id] = signal.sequence
            return
        self._mark_dirty(stream_state, signal.sequence)

    def fetch_once_policy(self, stream_id: UUID) -> FetchDecision:
        """Permit at most one in-flight fetch while notifications are coalesced."""

        stream_state = self._streams.get(stream_id)
        if stream_state is None:
            return FetchDecision(False, 0, 0)
        if stream_state.fetch_in_progress:
            self._inc("vidpg_fetch_coalesced_total")
            return FetchDecision(
                False,
                stream_state.last_published_seq,
                stream_state.dirty_generation,
            )
        if stream_state.latest_signaled_seq <= stream_state.last_fetched_seq:
            stream_state.fetch_event.clear()
            return FetchDecision(
                False,
                stream_state.last_published_seq,
                stream_state.dirty_generation,
            )
        stream_state.fetch_in_progress = True
        self.fetch_started_total += 1
        self._inc("vidpg_fetch_started_total")
        return FetchDecision(
            True,
            stream_state.last_published_seq,
            stream_state.dirty_generation,
        )

    def complete_fetch(
        self,
        stream_id: UUID,
        frame: FrameEnvelope | None,
    ) -> None:
        """Publish only a newer fetched frame and release the fetch ownership."""

        stream_state = self._streams.get(stream_id)
        if stream_state is None:
            return
        if frame is not None:
            if frame.stream_id != stream_id:
                stream_state.fetch_in_progress = False
                stream_state.last_fetched_seq = max(
                    stream_state.last_fetched_seq,
                    stream_state.latest_signaled_seq,
                )
                return
            stream_state.last_fetched_seq = max(
                stream_state.last_fetched_seq,
                frame.sequence,
            )
            if frame.sequence > stream_state.last_published_seq:
                stream_state.note_published(frame.sequence)
                self._inc("vidpg_frames_fetched_total")
                self._inc("vidpg_frames_fetched_from_db_total")
                self.publish(stream_id, frame)
        else:
            # A committed notification can outlive a bucket rotation. Mark the
            # signal observed so a missing row does not create an infinite loop.
            stream_state.last_fetched_seq = max(
                stream_state.last_fetched_seq,
                stream_state.latest_signaled_seq,
            )
        stream_state.fetch_in_progress = False
        if stream_state.latest_signaled_seq > stream_state.last_fetched_seq:
            stream_state.fetch_event.set()
        else:
            stream_state.fetch_event.clear()

    def stream_state(self, stream_id: UUID) -> RelayStreamState:
        """Return registered state for worker wiring and tests."""

        return self._streams[stream_id]

    def _mark_dirty(self, stream_state: RelayStreamState, sequence: int) -> None:
        if sequence <= stream_state.latest_signaled_seq:
            return
        stream_state.latest_signaled_seq = sequence
        stream_state.dirty_generation += 1
        stream_state.fetch_event.set()

    def _inc(
        self,
        name: str,
        amount: float = 1.0,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        if self.metrics is not None:
            self.metrics.inc(name, amount, labels=labels)


__all__ = ["Fanout", "FetchDecision"]
