"""FastAPI application factory and P0 process entry point."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from . import __version__
from .config import Settings, load_settings, validate_settings
from .health import build_health_response
from .observability import (
    build_status_report,
    probe_database,
    register_metrics,
)
from .relay.websocket import RelayService, websocket_endpoint


def create_app(settings: Settings) -> FastAPI:
    """Create an app after validating configuration at startup."""

    validated_settings = validate_settings(settings)
    relay = RelayService(validated_settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        relay.ensure_started()
        try:
            yield
        finally:
            await relay.close()

    app = FastAPI(title="VidPG", version=__version__, lifespan=lifespan)
    app.state.settings = validated_settings
    app.state.relay = relay
    app.state.metrics = relay.metrics
    register_health_route(app)
    register_metrics(app)
    register_operational_routes(app)
    app.add_api_websocket_route("/ws", websocket_endpoint)
    register_static_route(app)
    return app


def register_health_route(app: FastAPI) -> None:
    """Register the configuration-only P0 health endpoint."""

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        response = build_health_response(app.state.settings)
        status_code = 200 if response.status == "ok" else 500
        return JSONResponse(status_code=status_code, content=response.as_dict())


def register_operational_routes(app: FastAPI) -> None:
    """Register database readiness and human-readable operational status."""

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        relay = app.state.relay
        database = await asyncio.to_thread(
            probe_database,
            app.state.settings,
            listener_ready=relay.listener_ready,
            rotation_ready=relay.rotation_ready,
        )
        payload = database.as_dict()
        payload.update(
            {"status": "ok" if database.ready else "error", "version": __version__}
        )
        return JSONResponse(
            status_code=200 if database.ready else 503,
            content=payload,
        )

    @app.get("/api/status")
    async def api_status() -> JSONResponse:
        relay = app.state.relay
        database = await asyncio.to_thread(
            probe_database,
            app.state.settings,
            listener_ready=relay.listener_ready,
            rotation_ready=relay.rotation_ready,
        )
        metrics = app.state.metrics
        active_sessions = relay.registry.session_count
        active_websockets = sum(
            1
            for session in relay.registry.sessions
            for client in session.clients.values()
            if client.socket is not None and not client.closed
        )
        metrics.set("vidpg_active_sessions", active_sessions)
        metrics.set("vidpg_active_websockets", active_websockets)
        metrics.set("vidpg_listener_connected", relay.listener_ready)
        metrics.set("vidpg_rotation_connected", relay.rotation_ready)
        if database.generation is not None:
            metrics.set("vidpg_bucket_generation", database.generation)
        if database.active_bucket is not None:
            metrics.set("vidpg_active_bucket", database.active_bucket)
        if database.notification_queue_fraction is not None:
            metrics.set(
                "vidpg_notification_queue_fraction",
                database.notification_queue_fraction,
            )
        if database.wal_records is not None:
            metrics.set("vidpg_pg_wal_records_total", database.wal_records)
        if database.wal_fpi is not None:
            metrics.set("vidpg_pg_wal_fpi_total", database.wal_fpi)
        if database.wal_bytes is not None:
            metrics.set("vidpg_pg_wal_bytes_total", database.wal_bytes)
        if database.wal_buffers_full is not None:
            metrics.set(
                "vidpg_pg_wal_buffers_full_total",
                database.wal_buffers_full,
            )
        for bucket, size in (database.bucket_sizes or {}).items():
            metrics.set("vidpg_bucket_size_bytes", size, labels={"bucket": bucket})
        report = build_status_report(relay, database)
        return JSONResponse(content=report.as_dict())


def register_static_route(app: FastAPI) -> None:
    """Serve the P5 browser client when the source tree or image includes it."""

    web_root = Path(__file__).resolve().parents[1] / "web"
    if web_root.is_dir():
        app.mount("/", StaticFiles(directory=web_root, html=True), name="web")


def run() -> None:
    """Load process settings and start the HTTP server."""

    settings = load_settings(os.environ)
    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()
