"""Authenticated WebSocket relay endpoint and P4 service wiring."""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import WebSocket, WebSocketDisconnect

from vidpg.config import Settings
from vidpg.db.connection import (
    PgPool,
    open_dedicated_listener,
    open_maintenance_connection,
)
from vidpg.db.notifications import (
    listen_frames,
    rescan_latest_after_listen,
)
from vidpg.db.rotation import run_rotation_once
from vidpg.observability import DEFAULT_METRICS, MetricRegistry
from vidpg.protocol import (
    ControlMessage,
    ProtocolError,
    build_frame_message,
    parse_control_message,
)

from .admission import MAX_WS_MESSAGE_BYTES, admit_frame
from .auth import Side, validate_secret, validate_side
from .errors import (
    InvalidFrameError,
    JoinTimeoutError,
    MalformedJoinError,
    OutputTimeoutError,
    ProtocolViolationError,
    RelayError,
)
from .fanout import Fanout
from .sessions import (
    WS_BUFFER_THRESHOLD_BYTES,
    ClientState,
    SessionRegistry,
    SessionView,
)
from .workers import (
    OUTPUT_WRITE_TIMEOUT_SECONDS,
    PostgresFrameFetcher,
    PostgresFrameWriter,
    run_fetch_worker,
    run_insert_worker,
    run_output_worker,
)

JOIN_TIMEOUT_SECONDS = 5.0
CONTROL_MAX_BYTES = 8_192
LISTENER_RETRY_SECONDS = 1.0
SWEEP_INTERVAL_SECONDS = 1.0
LOGGER = logging.getLogger(__name__)

RawFrame = bytes


@dataclass(frozen=True, slots=True)
class JoinMessage:
    """The only allowed first control message on a relay WebSocket."""

    token: str


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of one bounded output attempt."""

    sent: bool
    bytes_written: int
    skipped: bool = False
    reason: str | None = None


class RelayService:
    """Own the process-local registry, workers, pools, and PostgreSQL listener."""

    def __init__(
        self,
        settings: Settings,
        *,
        db_writer: Any | None = None,
        db_fetcher: Any | None = None,
        enable_listener: bool | None = None,
        metrics: MetricRegistry | None = None,
    ) -> None:
        self.settings = settings
        provided_writer = db_writer is not None
        provided_fetcher = db_fetcher is not None
        self.metrics = metrics or DEFAULT_METRICS
        self.registry = SessionRegistry(
            max_sessions=settings.max_sessions,
            target_fps=settings.target_fps,
            max_frame_bytes=settings.max_frame_bytes,
            ws_buffer_threshold_bytes=WS_BUFFER_THRESHOLD_BYTES,
        )
        self.registry.metrics = self.metrics
        self.fanout = Fanout(self.metrics)
        self._writer_pool: PgPool | None = None
        self._fetch_pool: PgPool | None = None
        if db_writer is None:
            self._writer_pool = PgPool(settings.database_url, max_size=2)
            db_writer = PostgresFrameWriter(self._writer_pool)
        if db_fetcher is None:
            self._fetch_pool = PgPool(settings.database_url, max_size=2)
            db_fetcher = PostgresFrameFetcher(self._fetch_pool)
        self.db_writer = db_writer
        self.db_fetcher = db_fetcher
        self.enable_listener = (
            not provided_writer and not provided_fetcher
            if enable_listener is None
            else enable_listener
        )
        self.enable_maintenance = (
            self.enable_listener and not provided_writer and not provided_fetcher
        )
        self._listener_task: asyncio.Task[None] | None = None
        self._maintenance_task: asyncio.Task[None] | None = None
        self._sweeper_task: asyncio.Task[None] | None = None
        self._stream_tasks: dict[UUID, tuple[asyncio.Task[None], ...]] = {}
        self._closed = False
        self.listener_connected = False
        self.last_listener_error: str | None = None
        self.rotation_connected = False
        self.last_rotation_error: str | None = None
        self.rotation_generation: int | None = None
        self.rotation_active_bucket: int | None = None

    def ensure_started(self) -> None:
        """Start background listener/sweeper tasks when called from an event loop."""

        if self._closed:
            raise RuntimeError("relay service is closed")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if self.enable_listener and self._listener_task is None:
            self._listener_task = asyncio.create_task(self._listen_loop())
        if self.enable_maintenance and self._maintenance_task is None:
            self._maintenance_task = asyncio.create_task(self._rotation_loop())
        if self._sweeper_task is None:
            self._sweeper_task = asyncio.create_task(self._sweep_loop())

    def ensure_session_workers(self, session: Any) -> None:
        """Register both directions and start one insert/fetch pair per stream."""

        self.ensure_started()
        for stream_state in session.streams.values():
            self.fanout.register(stream_state)
        if session.session_id in self._stream_tasks:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        tasks: list[asyncio.Task[None]] = []
        for stream_state in session.streams.values():
            tasks.append(
                asyncio.create_task(
                    run_insert_worker(
                        stream_state,
                        self.db_writer,
                        metrics=self.metrics,
                    )
                )
            )
            tasks.append(
                asyncio.create_task(
                    run_fetch_worker(
                        stream_state,
                        self.db_fetcher,
                        self.fanout,
                        metrics=self.metrics,
                    )
                )
            )
        self._stream_tasks[session.session_id] = tuple(tasks)

    def attach_client(self, session: Any, side: Side) -> ClientState:
        """Subscribe a connected client to its incoming directional stream."""

        client = self.registry.client_state(session.session_id, side)
        incoming = session.stream_for_incoming(side)
        self.fanout.subscribe(incoming.stream_id, client)
        return client

    async def detach_client(
        self,
        session_id: UUID,
        side: Side,
        socket: Any,
    ) -> None:
        """Remove output subscription and current socket ownership."""

        current_session = next(
            (
                candidate
                for candidate in self.registry.sessions
                if candidate.session_id == session_id
            ),
            None,
        )
        if current_session is not None:
            client = current_session.client_for(side)
            if client is not None and client.socket is socket:
                self.fanout.unsubscribe(client)
        self.registry.remove_socket(session_id, side, socket)

    async def close(self) -> None:
        """Cancel relay tasks and close all idle PostgreSQL resources."""

        if self._closed:
            return
        self._closed = True
        flat_tasks = [task for group in self._stream_tasks.values() for task in group]
        if self._listener_task is not None:
            flat_tasks.append(self._listener_task)
        if self._maintenance_task is not None:
            flat_tasks.append(self._maintenance_task)
        if self._sweeper_task is not None:
            flat_tasks.append(self._sweeper_task)
        for task in flat_tasks:
            task.cancel()
        if flat_tasks:
            await asyncio.gather(*flat_tasks, return_exceptions=True)
        self._stream_tasks.clear()
        for session in tuple(self.registry.sessions):
            self.registry.remove(session.session_id)
        if self._writer_pool is not None:
            self._writer_pool.close()
        if self._fetch_pool is not None:
            self._fetch_pool.close()
        self.listener_connected = False
        self.rotation_connected = False

    async def _listen_loop(self) -> None:
        while not self._closed:
            connection: Any | None = None
            self.listener_connected = False
            try:
                connection = await asyncio.to_thread(
                    open_dedicated_listener,
                    self.settings,
                )
                assert connection is not None
                notification_stream = await asyncio.to_thread(
                    listen_frames,
                    connection,
                )
                initial_signals = await asyncio.to_thread(
                    rescan_latest_after_listen,
                    connection,
                )
                self.listener_connected = True
                self.last_listener_error = None
                for signal in initial_signals:
                    self.fanout.notify(signal)
                while not self._closed:
                    read_notification: Any = notification_stream.read
                    signal = await asyncio.to_thread(read_notification, 1.0)
                    if signal is not None:
                        self.metrics.inc("vidpg_notify_received_total")
                        self.metrics.inc("vidpg_notifications_received_total")
                        self.fanout.notify(signal)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.listener_connected = False
                self.last_listener_error = "listener connection failed"
                LOGGER.warning("PostgreSQL listener unavailable")
                await asyncio.sleep(LISTENER_RETRY_SECONDS)
            finally:
                self.listener_connected = False
                if connection is not None:
                    try:
                        await asyncio.to_thread(connection.close)
                    except Exception:
                        pass

    async def _rotation_loop(self) -> None:
        """Keep the single maintenance connection rotating the safe bucket ring."""

        connection: Any | None = None
        try:
            while not self._closed:
                try:
                    if connection is None:
                        connection = await asyncio.to_thread(
                            open_maintenance_connection,
                            self.settings,
                        )
                        self.rotation_connected = True
                        self.last_rotation_error = None
                    assert connection is not None
                    result = await asyncio.to_thread(run_rotation_once, connection, 250)
                    self.rotation_generation = result.generation_after
                    self.rotation_active_bucket = int(result.active_after)
                    if result.failure_reason is not None:
                        self.last_rotation_error = result.failure_reason
                    await asyncio.sleep(float(self.settings.bucket_seconds))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.rotation_connected = False
                    self.last_rotation_error = "rotation connection failed"
                    LOGGER.warning("PostgreSQL bucket rotation unavailable")
                    if connection is not None:
                        try:
                            await asyncio.to_thread(connection.close)
                        except Exception:
                            pass
                        connection = None
                    await asyncio.sleep(LISTENER_RETRY_SECONDS)
        finally:
            self.rotation_connected = False
            if connection is not None:
                try:
                    await asyncio.to_thread(connection.close)
                except Exception:
                    pass

    async def _sweep_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
            expired = self.registry.prune_expired()
            for session in expired:
                await self._stop_session_workers(session.session_id)

    async def _stop_session_workers(self, session_id: UUID) -> None:
        tasks = self._stream_tasks.pop(session_id, ())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def closed(self) -> bool:
        """Return whether the relay service has been shut down."""

        return self._closed

    @property
    def listener_ready(self) -> bool:
        """Return whether the dedicated LISTEN connection is active."""

        return self.listener_connected

    @property
    def rotation_ready(self) -> bool:
        """Return whether the maintenance connection has completed a rotation."""

        return self.rotation_connected and self.rotation_generation is not None


async def websocket_endpoint(socket: WebSocket) -> None:
    """Handle one authenticated full-duplex V1 relay WebSocket."""

    await _accept(socket)
    service = _service_from_socket(socket)
    join_result: Any | None = None
    attached = False
    try:
        session_id, side = _read_route_identity(socket)
        join_message = await _read_join_with_timeout(socket)
        join_result = service.registry.join(session_id, side, join_message.token)
        service.ensure_session_workers(join_result.session)
        service.registry.attach_socket(session_id, side, socket)
        attached = True
        client_state = service.attach_client(join_result.session, side)
        await send_ready(socket, join_result.view)
    except WebSocketDisconnect:
        if attached and join_result is not None:
            await service.detach_client(join_result.session.session_id, side, socket)
        return
    except RelayError as exc:
        if attached and join_result is not None:
            await service.detach_client(join_result.session.session_id, side, socket)
        await _send_error_and_close(socket, exc)
        return
    except (ProtocolError, TypeError, ValueError) as exc:
        if attached and join_result is not None:
            await service.detach_client(join_result.session.session_id, side, socket)
        error = MalformedJoinError(str(exc) or "Malformed join request")
        await _send_error_and_close(socket, error)
        return

    output_task: asyncio.Task[None] | None = asyncio.create_task(
        run_output_worker(client_state, socket, metrics=service.metrics)
    )
    try:
        while True:
            message = await read_binary_frame(socket)
            if isinstance(message, bytes):
                if len(message) > MAX_WS_MESSAGE_BYTES:
                    raise ProtocolViolationError("WebSocket message is too large")
                admission = admit_frame(
                    join_result.session,
                    message,
                    source_side=side,
                )
                if not admission.accepted:
                    service.metrics.inc("vidpg_relay_validation_drops_total")
                    service.metrics.inc(
                        "vidpg_frames_rejected_total",
                        labels={"reason": admission.reason or "unknown"},
                    )
                    raise InvalidFrameError(admission.reason or "Invalid frame")
                service.metrics.inc("vidpg_frames_received_total")
                service.metrics.inc("vidpg_relay_frames_received_total")
                if (
                    admission.offer is not None
                    and admission.offer.replaced_frame is not None
                ):
                    service.metrics.inc(
                        "vidpg_frames_replaced_total",
                        labels={"stage": "relay_ingress"},
                    )
                    service.metrics.inc(
                        "vidpg_relay_ingress_replaced_total",
                        labels={"stage": "relay_ingress"},
                    )
                continue
            if message.type == "ping":
                try:
                    client_state.enqueue_control({"type": "pong"})
                except RelayError:
                    raise
                continue
            raise ProtocolViolationError("Unsupported control message")
    except WebSocketDisconnect:
        pass
    except RelayError as exc:
        task = output_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        output_task = None
        await _send_error_and_close(socket, exc)
    finally:
        if output_task is not None:
            output_task.cancel()
            await asyncio.gather(output_task, return_exceptions=True)
        await service.detach_client(join_result.session.session_id, side, socket)


async def read_join_message(socket: Any) -> JoinMessage:
    """Read and validate the exact first text join message."""

    message = await _receive_message(socket)
    if message.get("type") == "websocket.disconnect":
        raise WebSocketDisconnect(message.get("code", 1000))
    text = message.get("text")
    if not isinstance(text, str):
        raise MalformedJoinError("First WebSocket message must be a text join")
    if len(text.encode("utf-8")) > CONTROL_MAX_BYTES:
        raise MalformedJoinError("Join message is too large")
    control = parse_control_message(text)
    if control.get("type") != "join":
        raise MalformedJoinError("First WebSocket message must be join")
    token = control.get("token")
    if not isinstance(token, str):
        raise MalformedJoinError("Join token has an invalid format")
    try:
        validate_secret(token)
    except (TypeError, ValueError) as exc:
        raise MalformedJoinError("Join token has an invalid format") from exc
    return JoinMessage(token)


async def send_ready(socket: Any, session_view: SessionView) -> None:
    """Send the exact non-secret ready schema."""

    await _send_json(socket, session_view.as_dict())


async def read_binary_frame(socket: Any) -> RawFrame | ControlMessage:
    """Read one raw binary frame or parse one UTF-8 JSON control message."""

    message = await _receive_message(socket)
    if message.get("type") == "websocket.disconnect":
        raise WebSocketDisconnect(message.get("code", 1000))
    raw_bytes = message.get("bytes")
    if raw_bytes is not None:
        if not isinstance(raw_bytes, bytes):
            raise ProtocolViolationError("Binary WebSocket payload must be bytes")
        return raw_bytes
    text = message.get("text")
    if text is not None:
        if not isinstance(text, str) or len(text.encode("utf-8")) > CONTROL_MAX_BYTES:
            raise ProtocolViolationError("Control message is too large")
        try:
            return parse_control_message(text)
        except ProtocolError as exc:
            raise ProtocolViolationError("Control message is invalid") from exc
    raise ProtocolViolationError("WebSocket message has no payload")


async def write_binary_frame(socket: Any, frame: Any) -> WriteResult:
    """Write one encoded frame with buffer admission and a 250 ms deadline."""

    from vidpg.contracts.frame import FrameEnvelope

    if not isinstance(frame, FrameEnvelope):
        raise TypeError("frame must be a FrameEnvelope")
    buffered_amount = _buffered_amount(socket)
    threshold = _buffer_threshold(socket)
    if buffered_amount > threshold:
        return WriteResult(
            sent=False,
            bytes_written=0,
            skipped=True,
            reason="BUFFERED_AMOUNT_HIGH",
        )
    message = build_frame_message(frame.meta(), frame.payload)
    try:
        await asyncio.wait_for(
            _send_bytes(socket, message),
            timeout=OUTPUT_WRITE_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        raise OutputTimeoutError() from exc
    return WriteResult(sent=True, bytes_written=len(message))


async def _read_join_with_timeout(socket: Any) -> JoinMessage:
    try:
        return await asyncio.wait_for(
            read_join_message(socket),
            timeout=JOIN_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        raise JoinTimeoutError() from exc


async def _send_error_and_close(socket: Any, error: RelayError) -> None:
    await _send_json(
        socket,
        {
            "type": "error",
            "code": error.code,
            "message": error.message,
        },
    )
    await _close(socket, error.close_code, error.message)


async def _receive_message(socket: Any) -> dict[str, Any]:
    receive = getattr(socket, "receive", None)
    if receive is None:
        raise ProtocolViolationError("Socket does not support receive")
    result = receive()
    message = await _resolve(result)
    if not isinstance(message, dict):
        raise ProtocolViolationError("Socket receive did not return a message")
    return message


async def _accept(socket: Any) -> None:
    result = socket.accept()
    await _resolve(result)


async def _send_json(socket: Any, message: dict[str, Any]) -> None:
    send_json = getattr(socket, "send_json", None)
    if send_json is None:
        raise ProtocolViolationError("Socket does not support JSON output")
    await _resolve(send_json(message))


async def _send_bytes(socket: Any, message: bytes) -> None:
    send_bytes = getattr(socket, "send_bytes", None)
    if send_bytes is None:
        raise ProtocolViolationError("Socket does not support binary output")
    await _resolve(send_bytes(message))


async def _close(socket: Any, code: int, reason: str) -> None:
    close = getattr(socket, "close", None)
    if close is None:
        return
    try:
        result = close(code=code, reason=reason)
    except TypeError:
        result = close(code)
    await _resolve(result)


async def _resolve(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


def _service_from_socket(socket: Any) -> RelayService:
    candidates = [getattr(socket, "app", None)]
    scope = getattr(socket, "scope", None)
    if isinstance(scope, dict):
        candidates.extend((scope.get("app"), scope.get("fastapi_app")))
    for app in candidates:
        state = getattr(app, "state", None)
        service = getattr(state, "relay", None)
        if isinstance(service, RelayService):
            return service
    raise RuntimeError("FastAPI app has no RelayService")


def _read_route_identity(socket: Any) -> tuple[UUID, Side]:
    params = getattr(socket, "query_params", {})
    session_value = params.get("session")
    side_value = params.get("side")
    if not isinstance(session_value, str) or not isinstance(side_value, str):
        raise MalformedJoinError("session and side query parameters are required")
    try:
        session_id = UUID(session_value)
        side = validate_side(side_value)
    except (TypeError, ValueError) as exc:
        raise MalformedJoinError(
            "session must be a UUID and side must be a or b"
        ) from exc
    return session_id, side


def _buffered_amount(socket: Any) -> int:
    for name in ("buffered_amount", "bufferedAmount"):
        value = getattr(socket, name, 0)
        if callable(value):
            value = value()
        if isinstance(value, int) and not isinstance(value, bool):
            return max(value, 0)
    return 0


def _buffer_threshold(socket: Any) -> int:
    value = getattr(socket, "ws_buffer_threshold_bytes", 524_288)
    return value if isinstance(value, int) and value >= 0 else 524_288


__all__ = [
    "CONTROL_MAX_BYTES",
    "JOIN_TIMEOUT_SECONDS",
    "JoinMessage",
    "MAX_WS_MESSAGE_BYTES",
    "RawFrame",
    "RelayService",
    "WriteResult",
    "read_binary_frame",
    "read_join_message",
    "send_ready",
    "websocket_endpoint",
    "write_binary_frame",
]
