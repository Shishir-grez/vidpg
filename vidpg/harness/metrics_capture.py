"""In-memory stage-event collector used by the synthetic harness."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from uuid import UUID

from vidpg.contracts.events import StageEvent, StageName

from .result_joiner import FrameTrace, join_events


@dataclass
class MetricsCapture:
    """Collect validated events without imposing a transport queue policy."""

    _events: list[StageEvent] = field(default_factory=list)

    def record(self, event: StageEvent) -> None:
        validation = event.validate()
        if not validation.ok:
            raise ValueError(validation.message or validation.code or "invalid event")
        self._events.append(event)

    def for_stage(self, stage: StageName) -> list[StageEvent]:
        return [
            event
            for event in self._events
            if StageName(event.stage) is StageName(stage)
        ]

    def trace_for(self, stream_id: UUID, sequence: int) -> FrameTrace:
        traces = join_events(
            event
            for event in self._events
            if event.stream_id == stream_id and event.sequence == sequence
        )
        if not traces:
            raise KeyError(f"no trace for {stream_id}/{sequence}")
        return traces[0]

    def latencies(self, start: StageName, end: StageName) -> list[int]:
        traces = join_events(self._events)
        values: list[int] = []
        for trace in traces:
            starts = [
                event
                for event in trace.events
                if StageName(event.stage) is StageName(start)
            ]
            ends = [
                event
                for event in trace.events
                if StageName(event.stage) is StageName(end)
            ]
            if not starts or not ends:
                continue
            if starts[0].host_id != ends[0].host_id:
                continue
            values.append(ends[0].monotonic_ns - starts[0].monotonic_ns)
        return values

    def reset(self) -> None:
        self._events.clear()

    @property
    def events(self) -> tuple[StageEvent, ...]:
        return tuple(self._events)

    def extend(self, events: Iterable[StageEvent]) -> None:
        for event in events:
            self.record(event)
