"""Capability-secret validation and side authorization for P4 sessions."""

from __future__ import annotations

import hmac
import re
import secrets
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from typing import TYPE_CHECKING, NewType

from .errors import RelayErrorCode

if TYPE_CHECKING:
    from .sessions import Session


SecretHash = NewType("SecretHash", str)
SECRET_HEX_LENGTH = 64
_SECRET_PATTERN = re.compile(r"[0-9a-f]{64}")


class Side(StrEnum):
    """A session side; values are deliberately limited to ``a`` and ``b``."""

    A = "a"
    B = "b"


@dataclass(frozen=True, slots=True)
class AuthResult:
    """Non-secret result of a join authorization check."""

    ok: bool
    side: Side | None = None
    reason: str | None = None

    @property
    def authorized(self) -> bool:
        """Alias used by callers that treat authorization as a capability check."""

        return self.ok


def generate_secret() -> str:
    """Generate the V1 32-byte capability token in lowercase hexadecimal."""

    return secrets.token_hex(32)


def validate_secret(secret: str) -> str:
    """Validate and return a V1 capability token without exposing its value."""

    if not isinstance(secret, str) or _SECRET_PATTERN.fullmatch(secret) is None:
        raise ValueError("secret must be 64 lowercase hexadecimal characters")
    return secret


def validate_secret_hash(secret_hash: str) -> str:
    """Validate the stored SHA-256 hexadecimal representation."""

    if (
        not isinstance(secret_hash, str)
        or _SECRET_PATTERN.fullmatch(secret_hash) is None
    ):
        raise ValueError("secret_hash must be 64 lowercase hexadecimal characters")
    return secret_hash


def hash_secret(secret: str) -> SecretHash:
    """Hash a validated random capability token with SHA-256."""

    validate_secret(secret)
    return SecretHash(sha256(secret.encode("ascii")).hexdigest())


def verify_secret(secret: str, secret_hash: str) -> bool:
    """Compare a token and stored hash without a timing-sensitive equality check."""

    try:
        validate_secret(secret)
        validate_secret_hash(secret_hash)
    except (TypeError, ValueError):
        return False
    expected = hash_secret(secret)
    return hmac.compare_digest(expected, secret_hash)


def validate_side(side: str | Side) -> Side:
    """Return a normalized side or reject any value outside the two-side contract."""

    if side == Side.A:
        return Side.A
    if side == Side.B:
        return Side.B
    raise ValueError("side must be 'a' or 'b'")


def authorize_join(
    session: Session,
    side: str | Side,
    secret: str,
) -> AuthResult:
    """Authorize a join without returning or logging the capability token."""

    try:
        selected_side = validate_side(side)
    except ValueError:
        return AuthResult(False, reason=RelayErrorCode.MALFORMED_JOIN.value)
    if not verify_secret(secret, session.secret_hash):
        return AuthResult(False, selected_side, RelayErrorCode.BAD_SECRET.value)
    return AuthResult(True, selected_side)


__all__ = [
    "AuthResult",
    "SECRET_HEX_LENGTH",
    "SecretHash",
    "Side",
    "authorize_join",
    "generate_secret",
    "hash_secret",
    "validate_secret",
    "validate_secret_hash",
    "validate_side",
    "verify_secret",
]
