"""Encoding and decoding for the approved 48-byte V1 frame header."""

from __future__ import annotations

import struct
from enum import StrEnum
from uuid import UUID
from zlib import crc32

from vidpg.contracts.frame import UINT32_MAX, Codec, FrameMeta

HEADER_SIZE = 48
HEADER_FIELDS_SIZE = 44
PROTOCOL_VERSION = 0x01
MESSAGE_TYPE_VIDEO_FRAME = 0x01
CODEC_JPEG = 0x01
CODEC_SYNTHETIC = 0x7F
FLAGS_RESERVED = 0

WIRE_EXPERIMENT_ID = "v1-wire"
WIRE_RUN_ID = UUID(int=0)


class ProtocolErrorCode(StrEnum):
    """Stable reason codes for malformed V1 protocol data."""

    BAD_TYPE = "BAD_TYPE"
    BAD_VERSION = "BAD_VERSION"
    BAD_MESSAGE_TYPE = "BAD_MESSAGE_TYPE"
    BAD_CODEC = "BAD_CODEC"
    BAD_FLAGS = "BAD_FLAGS"
    BAD_SEQUENCE = "BAD_SEQUENCE"
    BAD_TIMESTAMP = "BAD_TIMESTAMP"
    BAD_DIMENSION = "BAD_DIMENSION"
    BAD_LENGTH = "BAD_LENGTH"
    OVERSIZE_PAYLOAD = "OVERSIZE_PAYLOAD"
    BAD_HEADER_CRC = "BAD_HEADER_CRC"
    TRUNCATED_HEADER = "TRUNCATED_HEADER"
    TRAILING_BYTES = "TRAILING_BYTES"
    BAD_JPEG_MARKER = "BAD_JPEG_MARKER"
    INVALID_CONTROL = "INVALID_CONTROL"


class ProtocolError(ValueError):
    """Raised when a frame or control message violates the wire contract."""

    def __init__(
        self,
        code: ProtocolErrorCode | str,
        message: str,
        *,
        field: str | None = None,
    ) -> None:
        self.code = code.value if isinstance(code, ProtocolErrorCode) else code
        self.field = field
        super().__init__(f"{self.code}: {message}")


class HeaderCodec:
    """Constants and helpers for the network-order V1 header."""

    @staticmethod
    def header_size() -> int:
        """Return the stable encoded header size."""

        return HEADER_SIZE

    @staticmethod
    def header_crc32(header_without_crc: bytes) -> int:
        """Return the unsigned CRC32 for the first 44 header bytes."""

        if len(header_without_crc) != HEADER_FIELDS_SIZE:
            raise ProtocolError(
                ProtocolErrorCode.BAD_LENGTH,
                "CRC input must contain exactly 44 bytes",
            )
        return crc32(header_without_crc) & UINT32_MAX


def encode_header(meta: FrameMeta) -> bytes:
    """Encode frame metadata into the approved 48-byte V1 header."""

    from .validation import validate_frame_meta

    validation = validate_frame_meta(meta, None)
    if not validation.ok:
        raise ProtocolError(
            validation.code or ProtocolErrorCode.BAD_TYPE,
            validation.message or "frame metadata is invalid",
            field=validation.field,
        )

    try:
        codec = _codec_to_wire(meta.codec)
    except ValueError as exc:
        raise ProtocolError(
            ProtocolErrorCode.BAD_CODEC, str(exc), field="codec"
        ) from exc

    first_44 = struct.pack(
        ">BBBB16sQqHHI",
        PROTOCOL_VERSION,
        MESSAGE_TYPE_VIDEO_FRAME,
        codec,
        FLAGS_RESERVED,
        meta.stream_id.bytes,
        meta.sequence,
        meta.captured_wall_us,
        meta.width,
        meta.height,
        meta.payload_length,
    )
    checksum = HeaderCodec.header_crc32(first_44)
    return first_44 + struct.pack(">I", checksum)


def decode_header(data: bytes) -> FrameMeta:
    """Decode and validate one complete 48-byte V1 header."""

    raw = _as_bytes(data)
    if len(raw) < HEADER_SIZE:
        raise ProtocolError(
            ProtocolErrorCode.TRUNCATED_HEADER,
            f"header requires {HEADER_SIZE} bytes, received {len(raw)}",
        )
    if len(raw) != HEADER_SIZE:
        raise ProtocolError(
            ProtocolErrorCode.BAD_LENGTH,
            f"header decoder requires exactly {HEADER_SIZE} bytes",
        )

    (
        version,
        message_type,
        codec_value,
        flags,
        stream_bytes,
        sequence,
        captured_wall_us,
        width,
        height,
        payload_length,
    ) = struct.unpack(">BBBB16sQqHHI", raw[:HEADER_FIELDS_SIZE])

    if version != PROTOCOL_VERSION:
        raise ProtocolError(
            ProtocolErrorCode.BAD_VERSION,
            f"unsupported protocol version: {version}",
            field="version",
        )
    if message_type != MESSAGE_TYPE_VIDEO_FRAME:
        raise ProtocolError(
            ProtocolErrorCode.BAD_MESSAGE_TYPE,
            f"unsupported message type: {message_type}",
            field="message_type",
        )
    if flags != FLAGS_RESERVED:
        raise ProtocolError(
            ProtocolErrorCode.BAD_FLAGS,
            "V1 flags must be zero",
            field="flags",
        )

    try:
        codec = _codec_from_wire(codec_value)
    except ValueError as exc:
        raise ProtocolError(
            ProtocolErrorCode.BAD_CODEC, str(exc), field="codec"
        ) from exc

    expected_crc = struct.unpack(">I", raw[HEADER_FIELDS_SIZE:HEADER_SIZE])[0]
    actual_crc = HeaderCodec.header_crc32(raw[:HEADER_FIELDS_SIZE])
    if expected_crc != actual_crc:
        raise ProtocolError(
            ProtocolErrorCode.BAD_HEADER_CRC,
            "header CRC32 does not match bytes 0 through 43",
            field="header_crc32",
        )

    meta = FrameMeta(
        version=version,
        experiment_id=WIRE_EXPERIMENT_ID,
        run_id=WIRE_RUN_ID,
        stream_id=UUID(bytes=stream_bytes),
        sequence=sequence,
        captured_wall_us=captured_wall_us,
        captured_monotonic_ns=0,
        codec=codec,
        width=width,
        height=height,
        payload_length=payload_length,
    )
    from .validation import validate_frame_meta

    validation = validate_frame_meta(meta, None)
    if not validation.ok:
        raise ProtocolError(
            validation.code or ProtocolErrorCode.BAD_TYPE,
            validation.message or "decoded frame metadata is invalid",
            field=validation.field,
        )
    return meta


def _codec_to_wire(codec: Codec | str) -> int:
    try:
        selected = Codec(codec)
    except (TypeError, ValueError) as exc:
        raise ValueError("codec is not supported by V1") from exc
    if selected is Codec.JPEG:
        return CODEC_JPEG
    if selected is Codec.SYNTHETIC:
        return CODEC_SYNTHETIC
    raise ValueError(f"codec is reserved or unsupported: {selected.value}")


def _codec_from_wire(value: int) -> Codec:
    if value == CODEC_JPEG:
        return Codec.JPEG
    if value == CODEC_SYNTHETIC:
        return Codec.SYNTHETIC
    raise ValueError(f"codec value is reserved or unsupported: {value}")


def _as_bytes(data: bytes | bytearray | memoryview) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise ProtocolError(
            ProtocolErrorCode.BAD_TYPE,
            "protocol data must be bytes-like",
        )
    return bytes(data)


__all__ = [
    "CODEC_JPEG",
    "CODEC_SYNTHETIC",
    "FLAGS_RESERVED",
    "HEADER_FIELDS_SIZE",
    "HEADER_SIZE",
    "MESSAGE_TYPE_VIDEO_FRAME",
    "PROTOCOL_VERSION",
    "WIRE_EXPERIMENT_ID",
    "WIRE_RUN_ID",
    "HeaderCodec",
    "ProtocolError",
    "ProtocolErrorCode",
    "decode_header",
    "encode_header",
]
