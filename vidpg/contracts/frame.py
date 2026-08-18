"""Shared frame identity and payload validation contract."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from hashlib import sha256
from typing import Any
from uuid import UUID

SUPPORTED_ENVELOPE_VERSION = 1
UINT8_MAX = 2**8 - 1
UINT16_MAX = 2**16 - 1
UINT32_MAX = 2**32 - 1
UINT64_MAX = 2**64 - 1
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class Codec(StrEnum):
    """Codecs recognized by the shared comparison contract."""

    JPEG = "jpeg"
    WEBP = "webp"
    SYNTHETIC = "synthetic"
    WEBRTC_NATIVE = "webrtc_native"


class ErrorCode(StrEnum):
    """Machine-readable validation outcomes shared across contract types."""

    BAD_VERSION = "BAD_VERSION"
    BAD_TYPE = "BAD_TYPE"
    BAD_CODEC = "BAD_CODEC"
    BAD_LENGTH = "BAD_LENGTH"
    BAD_HASH = "BAD_HASH"
    BAD_DIMENSION = "BAD_DIMENSION"
    BAD_SEQUENCE = "BAD_SEQUENCE"
    OVERSIZE_PAYLOAD = "OVERSIZE_PAYLOAD"
    BAD_JPEG_MARKER = "BAD_JPEG_MARKER"
    UNAUTHORIZED_STREAM = "UNAUTHORIZED_STREAM"
    BAD_STAGE = "BAD_STAGE"
    BAD_OUTCOME = "BAD_OUTCOME"
    BAD_MANIFEST = "BAD_MANIFEST"


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Non-throwing validation result with a stable machine-readable code."""

    ok: bool
    code: str | None = None
    message: str | None = None
    field: str | None = None

    def __bool__(self) -> bool:
        return self.ok

    @classmethod
    def valid(cls) -> ValidationResult:
        return cls(ok=True)

    @classmethod
    def invalid(
        cls,
        code: ErrorCode | str,
        message: str,
        field: str | None = None,
    ) -> ValidationResult:
        value = code.value if isinstance(code, ErrorCode) else code
        return cls(ok=False, code=value, message=message, field=field)


@dataclass(frozen=True, slots=True)
class FrameMeta:
    """Frame header fields without payload bytes or the payload hash."""

    version: int
    experiment_id: str
    run_id: UUID
    stream_id: UUID
    sequence: int
    captured_wall_us: int
    captured_monotonic_ns: int
    codec: Codec | str
    width: int
    height: int
    payload_length: int


@dataclass(frozen=True, slots=True)
class FrameEnvelope(FrameMeta):
    """A frame's shared identity, metadata, hash, and raw bytes."""

    payload_sha256: bytes
    payload: bytes

    def payload_hash(self) -> bytes:
        """Return the SHA-256 digest of the payload bytes."""

        return sha256(self.payload).digest()

    def is_codec(self, codec: Codec) -> bool:
        """Return whether this envelope declares the requested codec."""

        try:
            expected = Codec(codec)
        except ValueError:
            return False
        try:
            actual = Codec(self.codec)
        except ValueError:
            return False
        return actual is expected

    def validate(self) -> ValidationResult:
        """Validate identity, dimensions, declared length, and payload hash."""

        if not isinstance(self.version, int) or isinstance(self.version, bool):
            return ValidationResult.invalid(
                ErrorCode.BAD_VERSION, "version must be an integer", "version"
            )
        if self.version != SUPPORTED_ENVELOPE_VERSION:
            return ValidationResult.invalid(
                ErrorCode.BAD_VERSION,
                f"unsupported envelope version: {self.version}",
                "version",
            )

        if not isinstance(self.experiment_id, str) or not self.experiment_id.strip():
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "experiment_id must be non-empty", "experiment_id"
            )
        if not isinstance(self.run_id, UUID):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "run_id must be a UUID", "run_id"
            )
        if not isinstance(self.stream_id, UUID):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "stream_id must be a UUID", "stream_id"
            )

        sequence_result = _validate_unsigned(
            self.sequence, UINT64_MAX, ErrorCode.BAD_SEQUENCE, "sequence"
        )
        if sequence_result is not None:
            return sequence_result

        wall_result = _validate_signed(self.captured_wall_us, "captured_wall_us")
        if wall_result is not None:
            return wall_result
        monotonic_result = _validate_unsigned(
            self.captured_monotonic_ns,
            UINT64_MAX,
            ErrorCode.BAD_TYPE,
            "captured_monotonic_ns",
        )
        if monotonic_result is not None:
            return monotonic_result

        try:
            codec = Codec(self.codec)
        except (TypeError, ValueError):
            return ValidationResult.invalid(
                ErrorCode.BAD_CODEC, "codec is not supported", "codec"
            )
        if codec is Codec.WEBRTC_NATIVE:
            return ValidationResult.invalid(
                ErrorCode.BAD_CODEC,
                "webrtc_native is not valid for frame-transit experiments",
                "codec",
            )

        for field_name, value in (("width", self.width), ("height", self.height)):
            dimension_result = _validate_unsigned(
                value, UINT16_MAX, ErrorCode.BAD_DIMENSION, field_name
            )
            if dimension_result is not None:
                return dimension_result
        if codec in {Codec.JPEG, Codec.WEBP} and (self.width == 0 or self.height == 0):
            return ValidationResult.invalid(
                ErrorCode.BAD_DIMENSION,
                "real image codecs require nonzero dimensions",
                "width",
            )

        length_result = _validate_unsigned(
            self.payload_length, UINT32_MAX, ErrorCode.BAD_LENGTH, "payload_length"
        )
        if length_result is not None:
            return length_result
        if not isinstance(self.payload, (bytes, bytearray, memoryview)):
            return ValidationResult.invalid(
                ErrorCode.BAD_TYPE, "payload must contain bytes", "payload"
            )
        if self.payload_length != len(self.payload):
            return ValidationResult.invalid(
                ErrorCode.BAD_LENGTH,
                "payload_length must equal the payload byte count",
                "payload_length",
            )
        if not isinstance(self.payload_sha256, (bytes, bytearray, memoryview)):
            return ValidationResult.invalid(
                ErrorCode.BAD_HASH,
                "payload_sha256 must contain bytes",
                "payload_sha256",
            )
        if len(self.payload_sha256) != 32:
            return ValidationResult.invalid(
                ErrorCode.BAD_HASH, "payload_sha256 must be 32 bytes", "payload_sha256"
            )
        if bytes(self.payload_sha256) != self.payload_hash():
            return ValidationResult.invalid(
                ErrorCode.BAD_HASH,
                "payload_sha256 does not match payload",
                "payload_sha256",
            )
        return ValidationResult.valid()

    def meta(self) -> FrameMeta:
        """Return the envelope's metadata portion."""

        return FrameMeta(
            version=self.version,
            experiment_id=self.experiment_id,
            run_id=self.run_id,
            stream_id=self.stream_id,
            sequence=self.sequence,
            captured_wall_us=self.captured_wall_us,
            captured_monotonic_ns=self.captured_monotonic_ns,
            codec=self.codec,
            width=self.width,
            height=self.height,
            payload_length=self.payload_length,
        )

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-safe diagnostic representation without text encoding bytes."""

        values = asdict(self)
        values["run_id"] = str(self.run_id)
        values["stream_id"] = str(self.stream_id)
        try:
            values["codec"] = Codec(self.codec).value
        except (TypeError, ValueError):
            values["codec"] = self.codec
        values["payload_sha256"] = list(bytes(self.payload_sha256))
        values["payload"] = list(bytes(self.payload))
        return values


def _validate_unsigned(
    value: object,
    maximum: int,
    code: ErrorCode,
    field: str,
) -> ValidationResult | None:
    if not isinstance(value, int) or isinstance(value, bool):
        return ValidationResult.invalid(code, "must be an unsigned integer", field)
    if value < 0 or value > maximum:
        return ValidationResult.invalid(
            code, "value is outside its integer range", field
        )
    return None


def _validate_signed(value: object, field: str) -> ValidationResult | None:
    if not isinstance(value, int) or isinstance(value, bool):
        return ValidationResult.invalid(ErrorCode.BAD_TYPE, "must be an integer", field)
    if value < INT64_MIN or value > INT64_MAX:
        return ValidationResult.invalid(
            ErrorCode.BAD_TYPE, "value is outside int64", field
        )
    return None
