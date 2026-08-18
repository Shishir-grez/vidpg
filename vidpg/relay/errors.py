"""Stable relay errors and WebSocket close-code policy for P4."""

from __future__ import annotations

from enum import StrEnum


class RelayErrorCode(StrEnum):
    """Machine-readable relay error codes sent in control messages."""

    MALFORMED_JOIN = "MALFORMED_JOIN"
    BAD_SECRET = "BAD_SECRET"
    DUPLICATE_SIDE = "DUPLICATE_SIDE"
    SESSION_FULL = "SESSION_FULL"
    SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
    PROTOCOL_VIOLATION = "PROTOCOL_VIOLATION"
    JOIN_TIMEOUT = "JOIN_TIMEOUT"
    OUTPUT_TIMEOUT = "OUTPUT_TIMEOUT"
    CONTROL_OVERFLOW = "CONTROL_OVERFLOW"
    UNAUTHORIZED_STREAM = "UNAUTHORIZED_STREAM"
    INVALID_FRAME = "INVALID_FRAME"
    STALE_FRAME = "STALE_FRAME"


CLOSE_MALFORMED_JOIN = 4000
CLOSE_BAD_SECRET = 4001
CLOSE_DUPLICATE_SIDE = 4002
CLOSE_SESSION_FULL = 4003
CLOSE_PROTOCOL_VIOLATION = 4004
CLOSE_JOIN_TIMEOUT = 4005
CLOSE_OUTPUT_TIMEOUT = 4006
CLOSE_REPLACED = 4009


class RelayError(ValueError):
    """An expected relay failure with a stable close code."""

    def __init__(
        self,
        code: RelayErrorCode | str,
        message: str,
        close_code: int,
    ) -> None:
        self.code = code.value if isinstance(code, RelayErrorCode) else code
        self.message = message
        self.close_code = close_code
        super().__init__(f"{self.code}: {message}")


class MalformedJoinError(RelayError):
    """The URL or first join message violates the join contract."""

    def __init__(self, message: str = "Malformed join request") -> None:
        super().__init__(
            RelayErrorCode.MALFORMED_JOIN,
            message,
            CLOSE_MALFORMED_JOIN,
        )


class BadSecretError(RelayError):
    """The supplied capability secret does not authorize the session."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.BAD_SECRET,
            "Invalid session secret",
            CLOSE_BAD_SECRET,
        )


class DuplicateSideError(RelayError):
    """A side is already occupied and cannot be joined normally."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.DUPLICATE_SIDE,
            "Session side is already occupied",
            CLOSE_DUPLICATE_SIDE,
        )


class SessionFullError(RelayError):
    """The relay has reached its configured session limit."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.SESSION_FULL,
            "Session limit reached",
            CLOSE_SESSION_FULL,
        )


class SessionNotFoundError(RelayError):
    """A requested session is not present in the in-memory registry."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.SESSION_NOT_FOUND,
            "Session does not exist",
            CLOSE_BAD_SECRET,
        )


class ProtocolViolationError(RelayError):
    """A post-join WebSocket message violates the V1 protocol."""

    def __init__(self, message: str = "Protocol violation") -> None:
        super().__init__(
            RelayErrorCode.PROTOCOL_VIOLATION,
            message,
            CLOSE_PROTOCOL_VIOLATION,
        )


class JoinTimeoutError(RelayError):
    """The first join message did not arrive before the deadline."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.JOIN_TIMEOUT,
            "Join message timed out",
            CLOSE_JOIN_TIMEOUT,
        )


class OutputTimeoutError(RelayError):
    """A client socket could not accept output within the V1 deadline."""

    def __init__(self, message: str = "Client output timed out") -> None:
        super().__init__(
            RelayErrorCode.OUTPUT_TIMEOUT,
            message,
            CLOSE_OUTPUT_TIMEOUT,
        )


class ControlOverflowError(RelayError):
    """The bounded control queue cannot retain another control message."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.CONTROL_OVERFLOW,
            "Control queue is full",
            CLOSE_OUTPUT_TIMEOUT,
        )


class InvalidFrameError(RelayError):
    """A binary frame failed admission before any PostgreSQL operation."""

    def __init__(self, reason: str = "Invalid frame") -> None:
        del reason
        super().__init__(
            RelayErrorCode.INVALID_FRAME,
            "Invalid frame",
            CLOSE_PROTOCOL_VIOLATION,
        )


class StreamOwnershipError(RelayError):
    """A publisher attempted to send on a stream it does not own."""

    def __init__(self) -> None:
        super().__init__(
            RelayErrorCode.UNAUTHORIZED_STREAM,
            "Frame stream is not owned by this client",
            CLOSE_PROTOCOL_VIOLATION,
        )


__all__ = [
    "CLOSE_BAD_SECRET",
    "CLOSE_DUPLICATE_SIDE",
    "CLOSE_JOIN_TIMEOUT",
    "CLOSE_MALFORMED_JOIN",
    "CLOSE_OUTPUT_TIMEOUT",
    "CLOSE_PROTOCOL_VIOLATION",
    "CLOSE_REPLACED",
    "CLOSE_SESSION_FULL",
    "BadSecretError",
    "ControlOverflowError",
    "DuplicateSideError",
    "InvalidFrameError",
    "JoinTimeoutError",
    "MalformedJoinError",
    "OutputTimeoutError",
    "ProtocolViolationError",
    "RelayError",
    "RelayErrorCode",
    "SessionFullError",
    "SessionNotFoundError",
    "StreamOwnershipError",
]
