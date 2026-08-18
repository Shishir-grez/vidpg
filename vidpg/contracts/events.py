"""Shared stage-event telemetry contract."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from .frame import (
    INT64_MAX,
    INT64_MIN,
    UINT32_MAX,
    UINT64_MAX,
    ErrorCode,
    FrameEnvelope,
    ValidationResult,
)


class StageName(StrEnum):
    CAPTURED = "captured"
    ENCODE_STARTED = "encode_started"
    ENCODE_COMPLETED = "encode_completed"
    NETWORK_SEND_ACCEPTED = "network_send_accepted"
    RELAY_RECEIVED = "relay_received"
    DB_WAIT_STARTED = "db_wait_started"
    DB_COMMAND_STARTED = "db_command_started"
    DB_COMMIT_CONFIRMED = "db_commit_confirmed"
    DB_EGRESS_STARTED = "db_egress_started"
    DB_EGRESS_COMPLETED = "db_egress_completed"
    RELAY_OUTPUT_ENQUEUED = "relay_output_enqueued"
    RECEIVER_ARRIVED = "receiver_arrived"
    DECODE_COMPLETED = "decode_completed"
    PAINT_OBSERVED = "paint_observed"


class StageOutcome(StrEnum):
    ACCEPTED = "accepted"
    COMPLETED = "completed"
    REPLACED = "replaced"
    DROPPED = "dropped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class StageEvent:
    """One timestamped observation for one frame at one named stage."""

    run_id: UUID
    experiment_id: str
    host_id: str
    stream_id: UUID
    sequence: int
    stage: StageName | str
    monotonic_ns: int
    wall_us: int
    bytes: int
    outcome: StageOutcome | str
    queue_depth: int | None = None
    batch_id: int | None = None
    transaction_id: str | None = None
    lsn: str | None = None
    reason: str | None = None

    def validate(self) -> ValidationResult:
        if not isinstance(self.run_id, UUID) or not isinstance(self.stream_id, UUID):
            return ValidationResult.invalid(ErrorCode.BAD_TYPE, "IDs must be UUIDs")
        if not isinstance(self.experiment_id, str) or not self.experiment_id.strip():
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "experiment_id must be non-empty", "experiment_id"
            )
        if not isinstance(self.host_id, str) or not self.host_id.strip():
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "host_id must be non-empty", "host_id"
            )
        if not self.host_id.startswith(("browser:", "relay:")):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE,
                "host_id must use browser: or relay: prefix",
                "host_id",
            )
        if not isinstance(self.sequence, int) or isinstance(self.sequence, bool):
            return ValidationResult.invalid(
                ErrorCode.BAD_SEQUENCE, "sequence must be an integer", "sequence"
            )
        if self.sequence < 0 or self.sequence > UINT64_MAX:
            return ValidationResult.invalid(
                ErrorCode.BAD_SEQUENCE, "sequence is outside uint64", "sequence"
            )
        try:
            StageName(self.stage)
        except (TypeError, ValueError):
            return ValidationResult.invalid(
                ErrorCode.BAD_STAGE, "stage is not part of the shared contract", "stage"
            )
        if not isinstance(self.monotonic_ns, int) or isinstance(
            self.monotonic_ns, bool
        ):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "monotonic_ns must be an integer", "monotonic_ns"
            )
        if self.monotonic_ns < 0 or self.monotonic_ns > UINT64_MAX:
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "monotonic_ns is outside uint64", "monotonic_ns"
            )
        if not isinstance(self.wall_us, int) or isinstance(self.wall_us, bool):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "wall_us must be an integer", "wall_us"
            )
        if self.wall_us < INT64_MIN or self.wall_us > INT64_MAX:
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "wall_us is outside int64", "wall_us"
            )
        if not isinstance(self.bytes, int) or isinstance(self.bytes, bool):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "bytes must be an integer", "bytes"
            )
        if self.bytes < 0 or self.bytes > UINT32_MAX:
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "bytes is outside uint32", "bytes"
            )
        try:
            StageOutcome(self.outcome)
        except (TypeError, ValueError):
            return ValidationResult.invalid(
                ErrorCode.BAD_OUTCOME,
                "outcome is not part of the shared contract",
                "outcome",
            )
        if self.queue_depth is not None and (
            not isinstance(self.queue_depth, int)
            or isinstance(self.queue_depth, bool)
            or self.queue_depth < 0
            or self.queue_depth > UINT32_MAX
        ):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "queue_depth is outside uint32", "queue_depth"
            )
        if self.batch_id is not None and (
            not isinstance(self.batch_id, int)
            or isinstance(self.batch_id, bool)
            or self.batch_id < 0
            or self.batch_id > UINT64_MAX
        ):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "batch_id is outside uint64", "batch_id"
            )
        return ValidationResult.valid()

    @classmethod
    def for_frame(
        cls,
        frame: FrameEnvelope,
        stage: StageName,
    ) -> StageEvent:
        """Create a deterministic-shape synthetic relay event for a frame."""

        return cls(
            run_id=frame.run_id,
            experiment_id=frame.experiment_id,
            host_id="relay:dummy",
            stream_id=frame.stream_id,
            sequence=frame.sequence,
            stage=stage,
            monotonic_ns=time.monotonic_ns(),
            wall_us=time.time_ns() // 1_000,
            bytes=frame.payload_length,
            outcome=StageOutcome.COMPLETED,
        )
