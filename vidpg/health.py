"""Process and configuration health independent of PostgreSQL readiness."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from . import __version__
from .config import Settings, SettingsError, validate_settings

HealthValue = Literal["ok", "error"]


@dataclass(frozen=True, slots=True)
class HealthResponse:
    status: HealthValue
    config_ready: bool
    database_ready: bool | None
    version: str

    def as_dict(self) -> dict[str, HealthValue | bool | None | str]:
        return asdict(self)


HealthStatus = HealthResponse


def check_config_ready(settings: Settings) -> HealthStatus:
    """Return configuration readiness without contacting PostgreSQL."""

    try:
        validate_settings(settings)
    except SettingsError:
        return HealthStatus(
            status="error",
            config_ready=False,
            database_ready=None,
            version=__version__,
        )
    return HealthStatus(
        status="ok",
        config_ready=True,
        database_ready=None,
        version=__version__,
    )


def build_health_response(settings: Settings) -> HealthResponse:
    """Build the P0 health payload."""

    return check_config_ready(settings)
