"""Join stage events and expose freshness/delivery anomaly detectors."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from uuid import UUID

from vidpg.contracts.events import StageEvent, StageName


@dataclass(frozen=True, slots=True)
class FrameKey:
    run_id: UUID
    experiment_id: str
    stream_id: UUID
    sequence: int


@dataclass(frozen=True, slots=True)
class FrameTrace:
    """All observed stage events for one frame identity."""

    key: FrameKey
    events: tuple[StageEvent, ...]
    delivery_history: tuple[int, ...] = ()

    @property
    def run_id(self) -> UUID:
        return self.key.run_id

    @property
    def experiment_id(self) -> str:
        return self.key.experiment_id

    @property
    def stream_id(self) -> UUID:
        return self.key.stream_id

    @property
    def sequence(self) -> int:
        return self.key.sequence


@dataclass(frozen=True, slots=True)
class SequenceGap:
    """Missing sequence values observed between two delivered values."""

    run_id: UUID
    experiment_id: str
    stream_id: UUID
    missing_sequences: tuple[int, ...]

    @property
    def missing(self) -> tuple[int, ...]:
        return self.missing_sequences

    @property
    def sequence(self) -> int:
        return self.missing_sequences[0]


def join_events(events: Iterable[StageEvent]) -> list[FrameTrace]:
    """Group validated events by the full run/experiment/stream/sequence key."""

    grouped: dict[FrameKey, list[StageEvent]] = {}
    delivery_by_stream: dict[tuple[UUID, str, UUID], list[int]] = defaultdict(list)
    for event in events:
        validation = event.validate()
        if not validation.ok:
            raise ValueError(validation.message or validation.code or "invalid event")
        key = FrameKey(
            run_id=event.run_id,
            experiment_id=event.experiment_id,
            stream_id=event.stream_id,
            sequence=event.sequence,
        )
        grouped.setdefault(key, []).append(event)
        if StageName(event.stage) is StageName.RECEIVER_ARRIVED:
            delivery_by_stream[
                (event.run_id, event.experiment_id, event.stream_id)
            ].append(event.sequence)

    traces: list[FrameTrace] = []
    for key, frame_events in grouped.items():
        stream_key = (key.run_id, key.experiment_id, key.stream_id)
        traces.append(
            FrameTrace(
                key=key,
                events=tuple(frame_events),
                delivery_history=tuple(delivery_by_stream[stream_key]),
            )
        )
    return traces


def detect_duplicate(trace: FrameTrace) -> bool:
    """Detect repeated receiver arrival for one frame identity."""

    return (
        sum(
            1
            for event in trace.events
            if StageName(event.stage) is StageName.RECEIVER_ARRIVED
        )
        > 1
    )


def detect_reorder(trace_or_traces: FrameTrace | Sequence[FrameTrace]) -> bool:
    """Detect a lower sequence delivered after a newer sequence on one stream."""

    if isinstance(trace_or_traces, FrameTrace):
        history = trace_or_traces.delivery_history
        if not history:
            history = tuple(
                event.sequence
                for event in trace_or_traces.events
                if StageName(event.stage) is StageName.RECEIVER_ARRIVED
            )
        return _has_reorder(history)

    histories: dict[tuple[UUID, str, UUID], tuple[int, ...]] = {}
    for trace in trace_or_traces:
        stream_key = (trace.run_id, trace.experiment_id, trace.stream_id)
        if stream_key in histories:
            continue
        history = trace.delivery_history or tuple(
            event.sequence
            for event in trace.events
            if StageName(event.stage) is StageName.RECEIVER_ARRIVED
        )
        histories[stream_key] = history
    return any(_has_reorder(history) for history in histories.values())


def detect_corruption(trace: FrameTrace) -> bool:
    """Detect a hash/payload validation failure recorded in the trace."""

    markers = ("hash", "corrupt", "mismatch", "payload")
    for event in trace.events:
        reason = (event.reason or "").lower()
        if any(marker in reason for marker in markers):
            return True
        if event.outcome == "failed" and StageName(event.stage) in {
            StageName.RECEIVER_ARRIVED,
            StageName.RELAY_RECEIVED,
        }:
            return True
    return False


def detect_gap(traces: Iterable[FrameTrace]) -> list[SequenceGap]:
    """Return missing sequence values independently for every stream."""

    by_stream: dict[tuple[UUID, str, UUID], set[int]] = defaultdict(set)
    for trace in traces:
        if any(
            StageName(event.stage) is StageName.RECEIVER_ARRIVED
            for event in trace.events
        ):
            by_stream[(trace.run_id, trace.experiment_id, trace.stream_id)].add(
                trace.sequence
            )

    gaps: list[SequenceGap] = []
    for (run_id, experiment_id, stream_id), sequences in by_stream.items():
        if len(sequences) < 2:
            continue
        first = min(sequences)
        last = max(sequences)
        missing = tuple(
            value for value in range(first, last + 1) if value not in sequences
        )
        if missing:
            gaps.append(
                SequenceGap(
                    run_id=run_id,
                    experiment_id=experiment_id,
                    stream_id=stream_id,
                    missing_sequences=missing,
                )
            )
    return gaps


def _has_reorder(sequences: Iterable[int]) -> bool:
    previous: int | None = None
    for sequence in sequences:
        if previous is not None and sequence < previous:
            return True
        previous = sequence
    return False
