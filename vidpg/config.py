"""Typed configuration loading and validation for the P0 application shell."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

MAX_FRAME_BYTES_HARD_LIMIT = 1_048_576
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_LOCAL_ORIGIN_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class SettingsError(ValueError):
    """Raised when application settings are missing or invalid."""

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        super().__init__(f"{field}: {message}")


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings required by the repository skeleton."""

    app_host: str
    app_port: int
    database_url: str
    public_origin: str
    max_sessions: int = 3
    target_fps: int = 30
    max_frame_bytes: int = 524_288
    bucket_seconds: int = 5
    log_level: str = "INFO"
    domain: str | None = None
    acme_email: str | None = None


def _value(env: Mapping[str, str], name: str) -> str | None:
    """Read the documented name, accepting the package-prefixed variant too."""

    for candidate in (name, f"VIDPG_{name}"):
        value = env.get(candidate)
        if value is not None and value.strip():
            return value.strip()
    return None


def _required_text(env: Mapping[str, str], name: str) -> str:
    value = _value(env, name)
    if value is None:
        raise SettingsError(name.lower(), "value is required")
    return value


def _integer(env: Mapping[str, str], name: str, default: int) -> int:
    value = _value(env, name)
    if value is None:
        return default
    try:
        return int(value, 10)
    except ValueError as exc:
        raise SettingsError(name.lower(), "must be an integer") from exc


def _required_integer(env: Mapping[str, str], name: str) -> int:
    value = _value(env, name)
    if value is None:
        raise SettingsError(name.lower(), "value is required")
    try:
        return int(value, 10)
    except ValueError as exc:
        raise SettingsError(name.lower(), "must be an integer") from exc


def load_settings(env: Mapping[str, str]) -> Settings:
    """Load and validate settings from an environment-like mapping."""

    settings = Settings(
        app_host=_required_text(env, "APP_HOST"),
        app_port=_required_integer(env, "APP_PORT"),
        database_url=_required_text(env, "DATABASE_URL"),
        public_origin=_required_text(env, "PUBLIC_ORIGIN"),
        max_sessions=_integer(env, "MAX_SESSIONS", 3),
        target_fps=_integer(env, "TARGET_FPS", 30),
        max_frame_bytes=_integer(env, "MAX_FRAME_BYTES", 524_288),
        bucket_seconds=_integer(env, "BUCKET_SECONDS", 5),
        log_level=(_value(env, "LOG_LEVEL") or "INFO").upper(),
        domain=_value(env, "DOMAIN"),
        acme_email=_value(env, "ACME_EMAIL"),
    )
    return validate_settings(settings)


def validate_settings(settings: Settings) -> Settings:
    """Validate settings and return the same typed value on success."""

    if not settings.app_host.strip():
        raise SettingsError("app_host", "value is required")
    if not 1 <= settings.app_port <= 65_535:
        raise SettingsError("app_port", "must be between 1 and 65535")
    if not settings.database_url:
        raise SettingsError("database_url", "value is required")
    if urlparse(settings.database_url).scheme not in {"postgres", "postgresql"}:
        raise SettingsError("database_url", "must use a PostgreSQL URL scheme")

    origin = urlparse(settings.public_origin)
    if origin.scheme not in {"http", "https"} or not origin.netloc:
        raise SettingsError("public_origin", "must be a valid HTTP(S) origin")
    if origin.scheme == "http" and origin.hostname not in _LOCAL_ORIGIN_HOSTS:
        raise SettingsError(
            "public_origin",
            "HTTP is only allowed for localhost, loopback, or ::1",
        )

    if not 1 <= settings.max_sessions <= 3:
        raise SettingsError("max_sessions", "must be between 1 and 3")
    if not 1 <= settings.target_fps <= 60:
        raise SettingsError("target_fps", "must be between 1 and 60")
    if not 0 < settings.max_frame_bytes <= MAX_FRAME_BYTES_HARD_LIMIT:
        raise SettingsError(
            "max_frame_bytes",
            f"must be between 1 and {MAX_FRAME_BYTES_HARD_LIMIT}",
        )
    if settings.bucket_seconds <= 0:
        raise SettingsError("bucket_seconds", "must be positive")
    if settings.log_level.upper() not in _LOG_LEVELS:
        raise SettingsError("log_level", "must be a standard logging level")
    if settings.domain is not None:
        if not _valid_domain(settings.domain):
            raise SettingsError("domain", "must be a hostname without a scheme or path")
    if settings.acme_email is not None:
        if "@" not in settings.acme_email or any(
            character.isspace() for character in settings.acme_email
        ):
            raise SettingsError("acme_email", "must be a valid email address")

    return settings


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    lines = path.read_text(encoding="utf-8").splitlines()
    for line_number, raw_line in enumerate(lines, 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        name, separator, value = line.partition("=")
        if not separator or not name.strip():
            raise ValueError(f"invalid environment entry on line {line_number}")
        parsed_value = value.strip()
        if len(parsed_value) >= 2 and parsed_value[0] == parsed_value[-1]:
            if parsed_value[0] in {"'", '"'}:
                parsed_value = parsed_value[1:-1]
        values[name.strip()] = parsed_value
    return values


def settings_from_file(path: Path) -> Settings:
    """Load settings from a small dotenv-compatible file."""

    return load_settings(_parse_env_file(path))


def _valid_domain(value: str) -> bool:
    """Accept DNS names and IP literals, but never a URL or Caddy path."""

    if (
        not value
        or "/" in value
        or "://" in value
        or any(character.isspace() for character in value)
    ):
        return False
    parsed = urlparse(f"//{value}")
    return parsed.hostname == value.rstrip(".").lower() and parsed.path in {"", "/"}
