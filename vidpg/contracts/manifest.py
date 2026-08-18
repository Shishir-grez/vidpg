"""Run provenance and experiment-comparison manifest contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, fields
from enum import Enum, StrEnum
from math import isfinite
from typing import Any

from .frame import ErrorCode, ValidationResult


class DeliverySemantics(StrEnum):
    EXACT = "exact"
    FRESHNESS = "freshness"


@dataclass(frozen=True, slots=True)
class RunManifest:
    """All provenance needed to compare one experiment run honestly."""

    source_commit: str = ""
    experiment_id: str = ""
    experiment_version: str = ""
    scenario: str = ""
    frame_size_distribution: Mapping[str, float] = field(default_factory=dict)
    target_fps: float = 0
    direction_count: int = 0
    session_count: int = 0
    delivery_semantics: DeliverySemantics | str | None = None
    hardware: str = ""
    operating_system: str = ""
    browser_version: str | None = None
    postgres_version: str | None = None
    postgres_configuration: Mapping[str, Any] = field(default_factory=dict)
    schema_storage: Mapping[str, Any] = field(default_factory=dict)
    driver_library_versions: Mapping[str, str] = field(default_factory=dict)
    network_profile: str = ""
    warmup_seconds: float = 0
    measurement_seconds: float = 0
    cooldown_seconds: float = 0
    clock_sync_method: str = ""
    clock_sync_error_us: float | None = None
    os: str | None = None
    browser_library_versions: Mapping[str, str] = field(default_factory=dict)
    postgres_config: Mapping[str, Any] | None = None
    required_stages: tuple[str, ...] = ()

    def validate(self) -> ValidationResult:
        text_fields = (
            "source_commit",
            "experiment_id",
            "experiment_version",
            "scenario",
            "hardware",
            "network_profile",
            "clock_sync_method",
        )
        for field_name in text_fields:
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                return ValidationResult.invalid(
                    ErrorCode.BAD_MANIFEST,
                    "value is required",
                    field_name,
                )

        operating_system = self.operating_system or self.os
        if not operating_system or not operating_system.strip():
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "operating system is required",
                "operating_system",
            )
        if not isinstance(self.frame_size_distribution, Mapping):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "frame_size_distribution must be a mapping",
                "frame_size_distribution",
            )
        if not self.frame_size_distribution:
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "frame_size_distribution cannot be empty",
                "frame_size_distribution",
            )
        for name, value in self.frame_size_distribution.items():
            if not isinstance(name, str) or not name.strip():
                return ValidationResult.invalid(
                    ErrorCode.BAD_MANIFEST,
                    "distribution keys must be non-empty strings",
                    "frame_size_distribution",
                )
            if not _finite_nonnegative(value):
                return ValidationResult.invalid(
                    ErrorCode.BAD_MANIFEST,
                    "distribution values must be finite and nonnegative",
                    "frame_size_distribution",
                )

        if self.delivery_semantics is None:
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "delivery_semantics must be exact or freshness",
                "delivery_semantics",
            )
        try:
            DeliverySemantics(self.delivery_semantics)
        except ValueError:
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "delivery_semantics must be exact or freshness",
                "delivery_semantics",
            )
        if not _finite_positive(self.target_fps):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST, "target_fps must be positive", "target_fps"
            )
        if not _positive_int(self.direction_count):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "direction_count must be positive",
                "direction_count",
            )
        if not _positive_int(self.session_count):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "session_count must be positive",
                "session_count",
            )

        for field_name in (
            "warmup_seconds",
            "measurement_seconds",
            "cooldown_seconds",
        ):
            if not _finite_nonnegative(getattr(self, field_name)):
                return ValidationResult.invalid(
                    ErrorCode.BAD_MANIFEST,
                    "duration must be finite and nonnegative",
                    field_name,
                )
        if self.measurement_seconds <= 0:
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "measurement_seconds must be positive",
                "measurement_seconds",
            )
        if self.clock_sync_error_us is None or not _finite_nonnegative(
            self.clock_sync_error_us
        ):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "clock_sync_error_us must be finite and nonnegative",
                "clock_sync_error_us",
            )
        if self.browser_version is None and not self.browser_library_versions:
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "browser or library version is required",
                "browser_version",
            )
        if self.postgres_config is not None and not isinstance(
            self.postgres_config, Mapping
        ):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "postgres_config must be a mapping",
                "postgres_config",
            )
        if not isinstance(self.required_stages, tuple):
            return ValidationResult.invalid(
                ErrorCode.BAD_MANIFEST,
                "required_stages must be a tuple",
                "required_stages",
            )
        return ValidationResult.valid()

    def single_variable_pair(self, other: RunManifest) -> bool:
        """Return true when exactly one top-level manifest value differs."""

        if not isinstance(other, RunManifest):
            return False
        differences = 0
        for manifest_field in fields(self):
            left = _normalise(getattr(self, manifest_field.name))
            right = _normalise(getattr(other, manifest_field.name))
            if left != right:
                differences += 1
        return differences == 1

    def required_stage_names(self) -> tuple[str, ...]:
        """Return explicit stages or the P1 synthetic/PG defaults."""

        if self.required_stages:
            return self.required_stages
        if self.postgres_version is not None:
            return (
                "captured",
                "encode_started",
                "encode_completed",
                "network_send_accepted",
                "relay_received",
                "db_wait_started",
                "db_command_started",
                "db_commit_confirmed",
                "db_egress_started",
                "db_egress_completed",
                "relay_output_enqueued",
                "receiver_arrived",
                "decode_completed",
                "paint_observed",
            )
        return ("captured", "receiver_arrived")

    def as_dict(self) -> dict[str, Any]:
        """Return the canonical schema-shaped manifest representation."""

        values = asdict(self)
        values["delivery_semantics"] = (
            DeliverySemantics(self.delivery_semantics).value
            if self.delivery_semantics is not None
            else None
        )
        values["operating_system"] = self.operating_system or self.os
        values["postgres_configuration"] = dict(
            self.postgres_config
            if self.postgres_config is not None
            else self.postgres_configuration
        )
        values.pop("os", None)
        values.pop("postgres_config", None)
        return values


def _normalise(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _normalise(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(_normalise(item) for item in value)
    return value


def _finite_nonnegative(value: object) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    return isfinite(value) and value >= 0


def _finite_positive(value: object) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    return isfinite(value) and value > 0


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0
