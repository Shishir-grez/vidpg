"""Comparable result summary built from joined stage-event traces."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from math import ceil
from typing import Any
from uuid import UUID

from vidpg.harness.result_joiner import (
    FrameTrace,
    detect_corruption,
    detect_duplicate,
    detect_gap,
    detect_reorder,
)

from .events import StageEvent, StageName
from .manifest import RunManifest


@dataclass
class ResultSummary:
    """A valid or explicitly failed summary for one manifest/run pair."""

    run_id: UUID | None = None
    experiment_id: str = ""
    status: str = "failed"
    valid: bool = False
    failure_reason: str | None = None
    offered_fps: float = 0
    accepted_fps: float = 0
    committed_fps: float = 0
    delivered_fps: float = 0
    painted_fps: float = 0
    replaced_fps: float = 0
    failed_fps: float = 0
    latency_percentiles: dict[str, dict[str, float]] = field(default_factory=dict)
    end_to_end_latency_percentiles: dict[str, float] = field(default_factory=dict)
    max_capture_age_us: float | None = None
    capture_age_slope_us_per_s: float | None = None
    max_queue_depth: int = 0
    max_queue_bytes: int = 0
    duplicate_count: int = 0
    gap_count: int = 0
    reorder_count: int = 0
    corruption_count: int = 0
    drop_reasons: dict[str, int] = field(default_factory=dict)
    pg_stats: dict[str, Any] | None = None
    experiment_metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def failed(self) -> bool:
        return not self.valid

    @classmethod
    def from_traces(
        cls,
        traces: Iterable[FrameTrace],
        manifest: RunManifest,
    ) -> ResultSummary:
        trace_list = list(traces)
        first = trace_list[0] if trace_list else None
        summary = cls(
            run_id=first.run_id if first else None,
            experiment_id=first.experiment_id if first else manifest.experiment_id,
        )

        manifest_result = manifest.validate()
        if not manifest_result.ok:
            summary.mark_failed(
                f"manifest invalid: {manifest_result.field or manifest_result.code}"
            )
            return summary
        if not trace_list:
            summary.mark_failed("no frame traces were recorded")
            return summary
        if any(
            trace.run_id != summary.run_id
            or trace.experiment_id != manifest.experiment_id
            for trace in trace_list
        ):
            summary.mark_failed("traces do not match manifest identity")
            return summary

        required = set(manifest.required_stage_names())
        missing_by_trace = {
            trace.sequence: sorted(
                stage
                for stage in required
                if not any(
                    StageName(event.stage).value == stage for event in trace.events
                )
            )
            for trace in trace_list
        }
        missing = {
            sequence: stages for sequence, stages in missing_by_trace.items() if stages
        }
        if missing:
            details = "; ".join(
                f"sequence {sequence}: {', '.join(stages)}"
                for sequence, stages in sorted(missing.items())
            )
            summary.mark_failed("missing required stages: " + details)
            return summary

        summary._populate(trace_list, manifest)
        summary.valid = True
        summary.status = "complete"
        return summary

    def mark_failed(self, reason: str) -> None:
        """Retain the run while making incompleteness impossible to rank."""

        self.valid = False
        self.status = "failed"
        self.failure_reason = reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id) if self.run_id is not None else None,
            "experiment_id": self.experiment_id,
            "status": self.status,
            "valid": self.valid,
            "failure_reason": self.failure_reason,
            "offered_fps": self.offered_fps,
            "accepted_fps": self.accepted_fps,
            "committed_fps": self.committed_fps,
            "delivered_fps": self.delivered_fps,
            "painted_fps": self.painted_fps,
            "replaced_fps": self.replaced_fps,
            "failed_fps": self.failed_fps,
            "latency_percentiles": self.latency_percentiles,
            "end_to_end_latency_percentiles": self.end_to_end_latency_percentiles,
            "max_capture_age_us": self.max_capture_age_us,
            "capture_age_slope_us_per_s": self.capture_age_slope_us_per_s,
            "max_queue_depth": self.max_queue_depth,
            "max_queue_bytes": self.max_queue_bytes,
            "duplicate_count": self.duplicate_count,
            "gap_count": self.gap_count,
            "reorder_count": self.reorder_count,
            "corruption_count": self.corruption_count,
            "drop_reasons": dict(self.drop_reasons),
            "pg_stats": self.pg_stats,
            "experiment_metrics": dict(self.experiment_metrics),
        }

    def _populate(self, traces: list[FrameTrace], manifest: RunManifest) -> None:
        seconds = manifest.measurement_seconds
        self.offered_fps = _count_stage(traces, StageName.CAPTURED) / seconds
        self.accepted_fps = (
            _count_stage(traces, StageName.NETWORK_SEND_ACCEPTED) / seconds
        )
        self.committed_fps = (
            _count_stage(traces, StageName.DB_COMMIT_CONFIRMED) / seconds
        )
        self.delivered_fps = _count_stage(traces, StageName.RECEIVER_ARRIVED) / seconds
        self.painted_fps = _count_stage(traces, StageName.PAINT_OBSERVED) / seconds
        self.replaced_fps = _count_outcome(traces, "replaced") / seconds
        self.failed_fps = _count_outcome(traces, "failed") / seconds

        self.duplicate_count = sum(detect_duplicate(trace) for trace in traces)
        self.gap_count = len(detect_gap(traces))
        self.reorder_count = int(detect_reorder(traces))
        self.corruption_count = sum(detect_corruption(trace) for trace in traces)
        self.drop_reasons = _drop_reasons(traces)
        self.max_queue_depth = max(
            (event.queue_depth or 0 for trace in traces for event in trace.events),
            default=0,
        )
        self.max_queue_bytes = max(
            (
                event.bytes
                for trace in traces
                for event in trace.events
                if event.queue_depth is not None
            ),
            default=0,
        )

        latencies: dict[str, list[float]] = {}
        end_to_end: list[float] = []
        for trace in traces:
            captured = _first_event(trace, StageName.CAPTURED)
            if captured is None:
                continue
            for event in trace.events:
                if (
                    event.host_id != captured.host_id
                    or event.monotonic_ns < captured.monotonic_ns
                ):
                    continue
                name = f"capture_to_{StageName(event.stage).value}_us"
                latencies.setdefault(name, []).append(
                    (event.monotonic_ns - captured.monotonic_ns) / 1_000
                )
            painted = _first_event(trace, StageName.PAINT_OBSERVED)
            if painted is not None and painted.host_id == captured.host_id:
                end_to_end.append(
                    (painted.monotonic_ns - captured.monotonic_ns) / 1_000
                )
        self.latency_percentiles = {
            name: _percentiles(values) for name, values in latencies.items()
        }
        self.end_to_end_latency_percentiles = _percentiles(end_to_end)


def _count_stage(traces: list[FrameTrace], stage: StageName) -> int:
    return sum(
        1
        for trace in traces
        for event in trace.events
        if StageName(event.stage) is stage
    )


def _count_outcome(traces: list[FrameTrace], outcome: str) -> int:
    return sum(
        1 for trace in traces for event in trace.events if event.outcome == outcome
    )


def _first_event(trace: FrameTrace, stage: StageName) -> StageEvent | None:
    return next(
        (event for event in trace.events if StageName(event.stage) is stage),
        None,
    )


def _drop_reasons(traces: list[FrameTrace]) -> dict[str, int]:
    reasons: dict[str, int] = {}
    for trace in traces:
        for event in trace.events:
            if event.outcome in {"replaced", "dropped", "failed"}:
                reason = event.reason or event.outcome
                reasons[reason] = reasons.get(reason, 0) + 1
    return reasons


def _percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)
    return {
        "p50": ordered[max(0, ceil(len(ordered) * 0.50) - 1)],
        "p95": ordered[max(0, ceil(len(ordered) * 0.95) - 1)],
        "p99": ordered[max(0, ceil(len(ordered) * 0.99) - 1)],
    }
