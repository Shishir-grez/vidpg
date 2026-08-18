"""Dependency-free operational metrics and PostgreSQL readiness reporting."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

import psycopg
from fastapi import FastAPI
from fastapi.responses import Response

from . import __version__
from .config import Settings
from .db.schema import assert_schema_matches_contract

_METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_FORBIDDEN_LABELS = frozenset({"session", "session_id", "stream", "stream_id"})
_DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_REPLACEMENT_STAGES = ("relay_ingress", "relay_output")
_BUCKET_NAMES = ("frame_bucket_0", "frame_bucket_1", "frame_bucket_2")

_COUNTER_NAMES = (
    "vidpg_frames_received_total",
    "vidpg_frames_inserted_total",
    "vidpg_frames_fetched_total",
    "vidpg_frames_delivered_total",
    "vidpg_frames_replaced_total",
    "vidpg_frames_rejected_total",
    "vidpg_notify_received_total",
    "vidpg_capture_callbacks_total",
    "vidpg_capture_rate_gate_skips_total",
    "vidpg_encode_started_total",
    "vidpg_encode_completed_total",
    "vidpg_encode_oversize_drops_total",
    "vidpg_ws_send_calls_total",
    "vidpg_ws_buffered_drops_total",
    "vidpg_relay_frames_received_total",
    "vidpg_relay_validation_drops_total",
    "vidpg_relay_ingress_replaced_total",
    "vidpg_pg_insert_success_total",
    "vidpg_pg_insert_error_total",
    "vidpg_pg_fetch_error_total",
    "vidpg_pg_wal_records_total",
    "vidpg_pg_wal_fpi_total",
    "vidpg_pg_wal_bytes_total",
    "vidpg_pg_wal_buffers_full_total",
    "vidpg_notifications_received_total",
    "vidpg_fetch_started_total",
    "vidpg_fetch_coalesced_total",
    "vidpg_frames_fetched_from_db_total",
    "vidpg_relay_output_replaced_total",
    "vidpg_frames_rendered_total",
    "vidpg_decoder_replaced_total",
)
_GAUGE_NAMES = (
    "vidpg_active_sessions",
    "vidpg_active_websockets",
    "vidpg_bucket_size_bytes",
    "vidpg_notification_queue_fraction",
    "vidpg_listener_connected",
    "vidpg_rotation_connected",
    "vidpg_bucket_generation",
    "vidpg_active_bucket",
)
_HISTOGRAM_NAMES = ("vidpg_pg_insert_seconds", "vidpg_pg_fetch_seconds")


@dataclass(frozen=True, slots=True)
class DatabaseStatus:
    """Safe database facts exposed by readiness and status endpoints."""

    connected: bool
    schema_ready: bool
    listener_ready: bool
    rotation_ready: bool
    generation: int | None = None
    active_bucket: int | None = None
    switched_at: str | None = None
    bucket_sizes: dict[str, int] | None = None
    notification_queue_fraction: float | None = None
    wal_records: int | None = None
    wal_fpi: int | None = None
    wal_bytes: int | None = None
    wal_buffers_full: int | None = None
    error: str | None = None

    @property
    def ready(self) -> bool:
        return (
            self.connected
            and self.schema_ready
            and self.listener_ready
            and self.rotation_ready
        )

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["database_ready"] = self.connected
        result["ready"] = self.ready
        return result


@dataclass(frozen=True, slots=True)
class StatusReport:
    """Human-readable operational state without capability or frame data."""

    status: str
    version: str
    uptime_seconds: float
    config_ready: bool
    database_ready: bool
    listener_ready: bool
    schema_ready: bool
    rotation_ready: bool
    active_sessions: int
    active_websockets: int
    generation: int | None
    active_bucket: int | None
    switched_at: str | None
    bucket_sizes: dict[str, int]
    notification_queue_fraction: float | None
    error: str | None
    metrics: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetricRegistry:
    """Small in-process Prometheus registry with bounded label cardinality."""

    def __init__(self) -> None:
        self._values: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[
            tuple[str, tuple[tuple[str, str], ...]], list[float]
        ] = {}
        self._started_at = time.monotonic()
        self._initialize_defaults()

    def inc(
        self,
        name: str,
        amount: float = 1.0,
        *,
        labels: Mapping[str, object] | None = None,
    ) -> None:
        """Increment a counter without accepting unbounded identity labels."""

        key = self._key(name, labels)
        self._values[key] = self._values.get(key, 0.0) + amount

    def set(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, object] | None = None,
    ) -> None:
        """Set a gauge value."""

        self._values[self._key(name, labels)] = float(value)

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, object] | None = None,
    ) -> None:
        """Record one bounded-duration histogram observation."""

        if name not in _HISTOGRAM_NAMES:
            raise ValueError(f"unknown histogram: {name}")
        if value < 0:
            raise ValueError("histogram observations must be non-negative")
        key = self._key(name, labels)
        series = self._histograms.setdefault(key, [0.0] * (len(_DEFAULT_BUCKETS) + 2))
        for index, boundary in enumerate(_DEFAULT_BUCKETS):
            if value <= boundary:
                series[index] += 1.0
        series[-2] += 1.0
        series[-1] += value

    def snapshot(self) -> dict[str, float]:
        """Return aggregate values suitable for the status endpoint."""

        snapshot: dict[str, float] = {}
        for (name, labels), value in self._values.items():
            snapshot[_display_key(name, labels)] = value
        for (name, labels), series in self._histograms.items():
            snapshot[f"{_display_key(name, labels)}_count"] = series[-2]
            snapshot[f"{_display_key(name, labels)}_sum"] = series[-1]
        return dict(sorted(snapshot.items()))

    def render(self) -> bytes:
        """Render the registry using the Prometheus text exposition format."""

        lines: list[str] = []
        for name in _COUNTER_NAMES:
            lines.append(f"# TYPE {name} counter")
            series = sorted(
                (key, value) for key, value in self._values.items() if key[0] == name
            )
            if not series:
                lines.append(f"{name} 0")
            else:
                for (_metric_name, labels), value in series:
                    lines.append(f"{_display_key(name, labels)} {value:g}")

        for name in _GAUGE_NAMES:
            lines.append(f"# TYPE {name} gauge")
            series = sorted(
                (key, value) for key, value in self._values.items() if key[0] == name
            )
            if not series:
                lines.append(f"{name} 0")
            else:
                for (_metric_name, labels), value in series:
                    lines.append(f"{_display_key(name, labels)} {value:g}")

        for name in _HISTOGRAM_NAMES:
            lines.append(f"# TYPE {name} histogram")
            histogram_series: list[
                tuple[tuple[str, tuple[tuple[str, str], ...]], list[float]]
            ] = sorted(
                (key, value)
                for key, value in self._histograms.items()
                if key[0] == name
            )
            if not histogram_series:
                histogram_labels: tuple[tuple[str, str], ...] = ()
                values = [0.0] * (len(_DEFAULT_BUCKETS) + 2)
                histogram_series = [((name, histogram_labels), values)]
            for (_metric_name, histogram_labels), values in histogram_series:
                for index, boundary in enumerate(_DEFAULT_BUCKETS):
                    bucket_labels = dict(histogram_labels)
                    bucket_labels["le"] = f"{boundary:g}"
                    lines.append(
                        f"{_display_key(name, tuple(sorted(bucket_labels.items())))} "
                        f"{values[index]:g}"
                    )
                bucket_labels = dict(histogram_labels)
                bucket_labels["le"] = "+Inf"
                lines.append(
                    f"{_display_key(name, tuple(sorted(bucket_labels.items())))} "
                    f"{values[-2]:g}"
                )
                lines.append(
                    f"{_display_key(name, histogram_labels)}_sum {values[-1]:g}"
                )
                lines.append(
                    f"{_display_key(name, histogram_labels)}_count {values[-2]:g}"
                )

        lines.append("")
        return "\n".join(lines).encode("utf-8")

    def reset(self) -> None:
        """Reset values while keeping all required series visible."""

        self._values.clear()
        self._histograms.clear()
        self._started_at = time.monotonic()
        self._initialize_defaults()

    @property
    def uptime_seconds(self) -> float:
        return max(0.0, time.monotonic() - self._started_at)

    def _initialize_defaults(self) -> None:
        for name in _COUNTER_NAMES:
            self._values[(name, ())] = 0.0
        for stage in _REPLACEMENT_STAGES:
            self._values[("vidpg_frames_replaced_total", (("stage", stage),))] = 0.0
        for stage in _REPLACEMENT_STAGES:
            self._values[
                ("vidpg_relay_ingress_replaced_total", (("stage", stage),))
            ] = 0.0
        self._values[("vidpg_frames_rejected_total", (("reason", "unknown"),))] = 0.0
        for bucket in _BUCKET_NAMES:
            self._values[("vidpg_bucket_size_bytes", (("bucket", bucket),))] = 0.0
        for name in _GAUGE_NAMES:
            if name != "vidpg_bucket_size_bytes":
                self._values[(name, ())] = 0.0

    def _key(
        self,
        name: str,
        labels: Mapping[str, object] | None,
    ) -> tuple[str, tuple[tuple[str, str], ...]]:
        if not _METRIC_NAME.fullmatch(name):
            raise ValueError(f"invalid metric name: {name}")
        normalized: list[tuple[str, str]] = []
        for label, value in (labels or {}).items():
            if label.lower() in _FORBIDDEN_LABELS:
                raise ValueError(f"identity label is forbidden: {label}")
            normalized.append((str(label), str(value)))
        return name, tuple(sorted(normalized))


DEFAULT_METRICS = MetricRegistry()


def register_metrics(app: FastAPI) -> None:
    """Attach the process registry and `/metrics` endpoint to a FastAPI app."""

    metrics = getattr(app.state, "metrics", None)
    if not isinstance(metrics, MetricRegistry):
        metrics = DEFAULT_METRICS
        app.state.metrics = metrics
    if getattr(app.state, "metrics_route_registered", False):
        return

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        relay = getattr(app.state, "relay", None)
        if relay is not None:
            metrics.set("vidpg_active_sessions", relay.registry.session_count)
            metrics.set(
                "vidpg_active_websockets",
                sum(
                    1
                    for session in relay.registry.sessions
                    for client in session.clients.values()
                    if client.socket is not None and not client.closed
                ),
            )
        return Response(
            content=metrics.render(),
            media_type="text/plain; version=0.0.4",
        )

    app.state.metrics_route_registered = True


def build_status_report(
    registry: Any,
    db_stats: DatabaseStatus | Mapping[str, Any],
) -> StatusReport:
    """Build a safe operational report from relay/session and DB state."""

    service = registry if hasattr(registry, "registry") else None
    if isinstance(registry, MetricRegistry):
        session_registry = getattr(registry, "session_registry", None)
        metrics = registry
    else:
        session_registry = getattr(service, "registry", registry)
        candidate_metrics = getattr(service, "metrics", None) or getattr(
            session_registry, "metrics", DEFAULT_METRICS
        )
        metrics = (
            candidate_metrics
            if isinstance(candidate_metrics, MetricRegistry)
            else DEFAULT_METRICS
        )
    sessions = tuple(getattr(session_registry, "sessions", ()))
    active_websockets = sum(
        1
        for session in sessions
        for client in getattr(session, "clients", {}).values()
        if getattr(client, "socket", None) is not None
        and not getattr(client, "closed", True)
    )
    db = _coerce_database_status(db_stats)
    config_ready = True
    status = "ok" if db.ready else "degraded"
    if service is not None and getattr(service, "closed", False):
        status = "stopped"
    return StatusReport(
        status=status,
        version=__version__,
        uptime_seconds=metrics.uptime_seconds,
        config_ready=config_ready,
        database_ready=db.connected,
        listener_ready=db.listener_ready,
        schema_ready=db.schema_ready,
        rotation_ready=db.rotation_ready,
        active_sessions=len(sessions),
        active_websockets=active_websockets,
        generation=db.generation,
        active_bucket=db.active_bucket,
        switched_at=db.switched_at,
        bucket_sizes=dict(db.bucket_sizes or {}),
        notification_queue_fraction=db.notification_queue_fraction,
        error=db.error,
        metrics=metrics.snapshot(),
    )


def render_prometheus_metrics() -> bytes:
    """Render the process-wide default registry for simple integrations."""

    return DEFAULT_METRICS.render()


def probe_database(
    settings: Settings,
    *,
    listener_ready: bool = False,
    rotation_ready: bool = False,
) -> DatabaseStatus:
    """Check PostgreSQL 18, schema, bucket state, and safe size counters."""

    try:
        with psycopg.connect(
            settings.database_url,
            autocommit=True,
            connect_timeout=2,
        ) as connection:
            version = connection.execute("SHOW server_version_num").fetchone()
            if version is None or int(version[0]) < 180_000:
                raise RuntimeError("PostgreSQL 18 or newer is required")
            assert_schema_matches_contract(connection)
            state = connection.execute(
                """
                SELECT generation, active, switched_at
                FROM vidpg.bucket_state
                WHERE singleton
                """
            ).fetchone()
            if state is None:
                raise RuntimeError("bucket_state singleton is missing")
            queue = connection.execute(
                "SELECT pg_notification_queue_usage()"
            ).fetchone()
            wal = connection.execute(
                """
                SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full
                FROM pg_stat_wal
                """
            ).fetchone()
            if wal is None:
                raise RuntimeError("PostgreSQL did not return WAL statistics")
            sizes = connection.execute(
                """
                SELECT c.relname, pg_total_relation_size(c.oid)
                FROM pg_class AS c
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = 'vidpg'
                  AND c.relname IN (
                      'frame_bucket_0', 'frame_bucket_1', 'frame_bucket_2'
                  )
                ORDER BY c.relname
                """
            ).fetchall()
            bucket_sizes = {str(row[0]): int(row[1]) for row in sizes}
            return DatabaseStatus(
                connected=True,
                schema_ready=True,
                listener_ready=listener_ready,
                rotation_ready=rotation_ready,
                generation=int(state[0]),
                active_bucket=int(state[1]),
                switched_at=state[2].isoformat() if state[2] is not None else None,
                bucket_sizes=bucket_sizes,
                notification_queue_fraction=float(queue[0]) if queue else None,
                wal_records=int(wal[0]),
                wal_fpi=int(wal[1]),
                wal_bytes=int(wal[2]),
                wal_buffers_full=int(wal[3]),
            )
    except Exception as exc:
        return DatabaseStatus(
            connected=False,
            schema_ready=False,
            listener_ready=listener_ready,
            rotation_ready=rotation_ready,
            bucket_sizes={},
            error=f"{type(exc).__name__}: {str(exc)[:200]}",
        )


def _coerce_database_status(
    value: DatabaseStatus | Mapping[str, Any],
) -> DatabaseStatus:
    if isinstance(value, DatabaseStatus):
        return value
    return DatabaseStatus(
        connected=bool(value.get("connected", value.get("database_ready", False))),
        schema_ready=bool(value.get("schema_ready", False)),
        listener_ready=bool(value.get("listener_ready", False)),
        rotation_ready=bool(value.get("rotation_ready", False)),
        generation=_optional_int(value.get("generation")),
        active_bucket=_optional_int(value.get("active_bucket")),
        switched_at=_optional_str(value.get("switched_at")),
        bucket_sizes=dict(value.get("bucket_sizes", {})),
        notification_queue_fraction=_optional_float(
            value.get("notification_queue_fraction")
        ),
        error=_optional_str(value.get("error")),
    )


def _display_key(name: str, labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return name
    rendered = ",".join(f'{label}="{_escape_label(value)}"' for label, value in labels)
    return f"{name}{{{rendered}}}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("integer status field cannot be boolean")
    return int(value) if isinstance(value, (int, float, str)) else None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value) if isinstance(value, (int, float, str)) else None


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


__all__ = [
    "DEFAULT_METRICS",
    "DatabaseStatus",
    "MetricRegistry",
    "StatusReport",
    "build_status_report",
    "probe_database",
    "register_metrics",
    "render_prometheus_metrics",
]
