"""Frame and control message boundaries for the V1 transport."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any

from vidpg.contracts.frame import FrameEnvelope, FrameMeta

from .header import (
    HEADER_SIZE,
    WIRE_EXPERIMENT_ID,
    WIRE_RUN_ID,
    ProtocolError,
    ProtocolErrorCode,
    decode_header,
    encode_header,
)
from .validation import validate_frame_meta, validate_payload


class ControlMessage(dict[str, Any]):
    """Generic JSON control object; control shapes are defined by later stages."""

    @property
    def type(self) -> str:
        """Return the required control message type."""

        value = self.get("type")
        if not isinstance(value, str):
            raise ProtocolError(
                ProtocolErrorCode.INVALID_CONTROL,
                "control message type must be a string",
                field="type",
            )
        return value


class BlobParts:
    """Optional zero-copy-friendly representation of a header and payload."""

    __slots__ = ("header", "payload")

    def __init__(self, header: bytes, payload: bytes) -> None:
        if len(header) != HEADER_SIZE:
            raise ProtocolError(
                ProtocolErrorCode.BAD_LENGTH,
                "blob header must contain exactly 48 bytes",
            )
        self.header = header
        self.payload = payload

    def to_bytes(self) -> bytes:
        """Materialize the complete binary frame message."""

        return self.header + self.payload


def build_frame_message(
    meta: FrameMeta,
    payload: bytes | bytearray | memoryview,
) -> bytes:
    """Build one fixed-header message followed by raw payload bytes."""

    metadata_result = validate_frame_meta(meta, None)
    if not metadata_result.ok:
        raise ProtocolError(
            metadata_result.code or ProtocolErrorCode.BAD_TYPE,
            metadata_result.message or "frame metadata is invalid",
            field=metadata_result.field,
        )
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise ProtocolError(
            ProtocolErrorCode.BAD_TYPE,
            "payload must be bytes-like",
            field="payload",
        )
    raw_payload = bytes(payload)
    if meta.payload_length != len(raw_payload):
        raise ProtocolError(
            ProtocolErrorCode.BAD_LENGTH,
            "metadata payload_length does not match payload bytes",
            field="payload_length",
        )
    payload_result = validate_payload(raw_payload, None, meta.codec)
    if not payload_result.ok:
        raise ProtocolError(
            payload_result.code or ProtocolErrorCode.BAD_TYPE,
            payload_result.message or "payload is invalid",
            field=payload_result.field,
        )
    return encode_header(meta) + raw_payload


def parse_frame_message(
    data: bytes | bytearray | memoryview,
) -> FrameEnvelope:
    """Parse exactly one complete V1 frame message and compute its hash."""

    raw = _as_bytes(data)
    if len(raw) < HEADER_SIZE:
        raise ProtocolError(
            ProtocolErrorCode.TRUNCATED_HEADER,
            f"frame message requires at least {HEADER_SIZE} bytes",
        )
    meta = decode_header(raw[:HEADER_SIZE])
    expected_length = HEADER_SIZE + meta.payload_length
    if len(raw) < expected_length:
        raise ProtocolError(
            ProtocolErrorCode.BAD_LENGTH,
            "frame message ends before its declared payload length",
            field="payload_length",
        )
    if len(raw) > expected_length:
        raise ProtocolError(
            ProtocolErrorCode.TRAILING_BYTES,
            "frame message contains bytes after the declared payload",
        )

    payload = raw[HEADER_SIZE:]
    payload_result = validate_payload(payload, None, meta.codec)
    if not payload_result.ok:
        raise ProtocolError(
            payload_result.code or ProtocolErrorCode.BAD_TYPE,
            payload_result.message or "payload is invalid",
            field=payload_result.field,
        )
    return FrameEnvelope(
        version=meta.version,
        experiment_id=WIRE_EXPERIMENT_ID,
        run_id=WIRE_RUN_ID,
        stream_id=meta.stream_id,
        sequence=meta.sequence,
        captured_wall_us=meta.captured_wall_us,
        captured_monotonic_ns=0,
        codec=meta.codec,
        width=meta.width,
        height=meta.height,
        payload_length=meta.payload_length,
        payload_sha256=sha256(payload).digest(),
        payload=payload,
    )


def parse_control_message(data: bytes | str | bytearray | memoryview) -> ControlMessage:
    """Parse a UTF-8 JSON control object, never a binary frame."""

    if isinstance(data, str):
        text = data
    elif isinstance(data, (bytes, bytearray, memoryview)):
        try:
            text = bytes(data).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError(
                ProtocolErrorCode.INVALID_CONTROL,
                "control message must be UTF-8 JSON",
            ) from exc
    else:
        raise ProtocolError(
            ProtocolErrorCode.INVALID_CONTROL,
            "control message must be text or UTF-8 bytes",
        )

    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProtocolError(
            ProtocolErrorCode.INVALID_CONTROL,
            "control message is not valid JSON",
        ) from exc
    if not isinstance(value, dict):
        raise ProtocolError(
            ProtocolErrorCode.INVALID_CONTROL,
            "control message must be a JSON object",
        )
    message = ControlMessage(value)
    if not message.get("type") or not isinstance(message["type"], str):
        raise ProtocolError(
            ProtocolErrorCode.INVALID_CONTROL,
            "control message requires a non-empty string type",
            field="type",
        )
    return message


def _as_bytes(data: bytes | bytearray | memoryview) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise ProtocolError(
            ProtocolErrorCode.BAD_TYPE,
            "frame message must be bytes-like",
        )
    return bytes(data)


__all__ = [
    "BlobParts",
    "ControlMessage",
    "build_frame_message",
    "parse_control_message",
    "parse_frame_message",
]
