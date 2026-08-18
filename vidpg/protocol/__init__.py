"""V1 fixed-header frame protocol primitives."""

from .header import (
    CODEC_JPEG,
    CODEC_SYNTHETIC,
    HEADER_SIZE,
    MESSAGE_TYPE_VIDEO_FRAME,
    PROTOCOL_VERSION,
    HeaderCodec,
    ProtocolError,
    ProtocolErrorCode,
    decode_header,
    encode_header,
)
from .messages import (
    BlobParts,
    ControlMessage,
    build_frame_message,
    parse_control_message,
    parse_frame_message,
)
from .validation import (
    DEFAULT_LIMITS,
    ProtocolLimits,
    validate_frame_meta,
    validate_payload,
    validate_sequence,
)

__all__ = [
    "BlobParts",
    "CODEC_JPEG",
    "CODEC_SYNTHETIC",
    "ControlMessage",
    "DEFAULT_LIMITS",
    "HEADER_SIZE",
    "HeaderCodec",
    "MESSAGE_TYPE_VIDEO_FRAME",
    "PROTOCOL_VERSION",
    "ProtocolError",
    "ProtocolErrorCode",
    "ProtocolLimits",
    "build_frame_message",
    "decode_header",
    "encode_header",
    "parse_control_message",
    "parse_frame_message",
    "validate_frame_meta",
    "validate_payload",
    "validate_sequence",
]
