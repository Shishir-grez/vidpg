"""Async relay workers bridging bounded slots to the P3 PostgreSQL primitives."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from typing import Any, Protocol
from uuid import UUID

from vidpg.contracts.frame import FrameEnvelope
from vidpg.db.buckets import active_bucket
from vidpg.db.connection import PgPool
from vidpg.db.fetcher import fetch_latest_after
from vidpg.db.writer import InsertReceipt, insert_and_notify
from vidpg.observability import MetricRegistry

from .fanout import Fanout
from .sessions import ClientState, RelayStreamState

OUTPUT_WRITE_TIMEOUT_SECONDS = 0.250
FETCH_RETRY_SECONDS = 0.100


class FrameWriter(Protocol):
    """The minimal synchronous or asynchronous writer shape accepted by workers."""

    def write(self, stream_state: RelayStreamState, frame: FrameEnvelope) -> Any: ...


class FrameFetcher(Protocol):
    """The minimal synchronous or asynchronous newest-frame fetch shape."""

    def fetch(self, stream_id: UUID, watermark: int) -> Any: ...


class PostgresFrameWriter:
    """Psycopg adapter selecting the current bucket before each insert."""

    def __init__(self, pool: PgPool) -> None:
        self.pool = pool

    def write(
        self,
        stream_state: RelayStreamState,
        frame: FrameEnvelope,
    ) -> InsertReceipt:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT generation, active FROM vidpg.bucket_state WHERE singleton"
            ).fetchone()
            if row is None:
                raise RuntimeError("bucket_state has no singleton row")
            generation = int(row[0])
            bucket = active_bucket(generation)
            if int(row[1]) != bucket:
                raise RuntimeError(
                    "bucket_state active bucket disagrees with generation"
                )
            return insert_and_notify(conn, bucket, frame)


class PostgresFrameFetcher:
    """Psycopg adapter for active/previous newest-frame reads."""

    def __init__(self, pool: PgPool) -> None:
        self.pool = pool

    def fetch(self, stream_id: UUID, watermark: int) -> FrameEnvelope | None:
        with self.pool.connection() as conn:
            return fetch_latest_after(conn, stream_id, watermark)


async def run_insert_worker(
    stream_state: RelayStreamState,
    db_writer: FrameWriter | Callable[..., Any],
    *,
    metrics: MetricRegistry | None = None,
) -> None:
    """Drain one input latest slot with one database write in flight."""

    while True:
        frame = stream_state.input_slot().take()
        if frame is None:
            if stream_state.closed:
                return
            await _wait_for_event(stream_state.input_event)
            continue
        try:
            started = time.perf_counter()
            await _invoke_writer(db_writer, stream_state, frame)
        except asyncio.CancelledError:
            stream_state.input_slot().fail_inflight(frame, "worker cancelled")
            raise
        except Exception:
            if metrics is not None:
                metrics.inc("vidpg_pg_insert_error_total")
            stream_state.input_slot().fail_inflight(frame, "database write failed")
            await asyncio.sleep(FETCH_RETRY_SECONDS)
        else:
            if metrics is not None:
                metrics.inc("vidpg_frames_inserted_total")
                metrics.inc("vidpg_pg_insert_success_total")
                metrics.observe(
                    "vidpg_pg_insert_seconds",
                    max(0.0, time.perf_counter() - started),
                )
            stream_state.input_slot().clear_inflight(frame)


async def run_fetch_worker(
    stream_state: RelayStreamState,
    db_fetcher: FrameFetcher | Callable[..., Any],
    fanout: Fanout,
    *,
    metrics: MetricRegistry | None = None,
) -> None:
    """Coalesce notifications and fetch only the newest row above the watermark."""

    while True:
        if not stream_state.fetch_event.is_set():
            if stream_state.closed:
                return
            await _wait_for_event(stream_state.fetch_event)
        decision = fanout.fetch_once_policy(stream_state.stream_id)
        if not decision.should_fetch:
            if stream_state.closed:
                return
            await asyncio.sleep(0)
            continue
        try:
            started = time.perf_counter()
            frame = await _invoke_fetch(
                db_fetcher,
                stream_state.stream_id,
                decision.watermark,
            )
        except asyncio.CancelledError:
            stream_state.fetch_in_progress = False
            raise
        except Exception:
            if metrics is not None:
                metrics.inc("vidpg_pg_fetch_error_total")
            stream_state.fetch_in_progress = False
            stream_state.fetch_event.set()
            await asyncio.sleep(FETCH_RETRY_SECONDS)
        else:
            if metrics is not None:
                metrics.observe(
                    "vidpg_pg_fetch_seconds",
                    max(0.0, time.perf_counter() - started),
                )
            fanout.complete_fetch(stream_state.stream_id, frame)


async def run_output_worker(
    client_state: ClientState,
    socket: Any,
    *,
    metrics: MetricRegistry | None = None,
) -> None:
    """Own all writes for one socket and prioritize bounded control messages."""

    from .websocket import write_binary_frame

    while not client_state.closed and client_state.socket is socket:
        control = client_state.take_control()
        if control is not None:
            try:
                await asyncio.wait_for(
                    _send_json(socket, control),
                    timeout=OUTPUT_WRITE_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                client_state.timeout_total += 1
                await _close_socket(socket, 4006, "output timeout")
                return
            continue

        frame = client_state.output_slot.take()
        if frame is None:
            await _wait_for_event(client_state.output_event)
            continue
        try:
            if metrics is not None:
                metrics.inc("vidpg_ws_send_calls_total")
            result = await write_binary_frame(socket, frame)
        except asyncio.CancelledError:
            client_state.output_slot.fail_inflight(frame, "worker cancelled")
            raise
        except Exception:
            client_state.timeout_total += 1
            client_state.output_slot.fail_inflight(frame, "output failed")
            await _close_socket(socket, 4006, "output timeout")
            return
        else:
            client_state.output_slot.clear_inflight(frame)
            if result.skipped:
                client_state.skipped_total += 1
                if metrics is not None:
                    metrics.inc("vidpg_ws_buffered_drops_total")
            elif metrics is not None:
                metrics.inc("vidpg_frames_delivered_total")


async def _invoke_writer(
    writer: FrameWriter | Callable[..., Any],
    stream_state: RelayStreamState,
    frame: FrameEnvelope,
) -> Any:
    target: Any = writer.write if hasattr(writer, "write") else writer
    return await _resolve_result(_call_with_arity(target, stream_state, frame))


async def _invoke_fetch(
    fetcher: FrameFetcher | Callable[..., Any],
    stream_id: UUID,
    watermark: int,
) -> FrameEnvelope | None:
    target: Any = fetcher.fetch if hasattr(fetcher, "fetch") else fetcher
    result = await _resolve_result(_call_with_arity(target, stream_id, watermark))
    if result is not None and not isinstance(result, FrameEnvelope):
        raise TypeError("fetcher must return FrameEnvelope or None")
    return result


def _call_with_arity(target: Callable[..., Any], *args: Any) -> Any:
    """Support small test doubles that accept either a full or reduced argument list."""

    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):
        return target(*args)
    parameters = tuple(signature.parameters.values())
    positional = tuple(
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    )
    has_varargs = any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in parameters
    )
    if has_varargs or len(positional) >= len(args):
        return target(*args)
    return target(args[-1])


async def _resolve_result(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


async def _wait_for_event(event: asyncio.Event) -> None:
    if event.is_set():
        event.clear()
        return
    await event.wait()
    event.clear()


async def _send_json(socket: Any, message: dict[str, Any]) -> None:
    result = socket.send_json(message)
    if inspect.isawaitable(result):
        await result


async def _close_socket(socket: Any, code: int, reason: str) -> None:
    close = getattr(socket, "close", None)
    if close is None:
        return
    try:
        result = close(code=code, reason=reason)
    except TypeError:
        result = close(code)
    if inspect.isawaitable(result):
        await result


__all__ = [
    "FETCH_RETRY_SECONDS",
    "OUTPUT_WRITE_TIMEOUT_SECONDS",
    "PostgresFrameFetcher",
    "PostgresFrameWriter",
    "run_fetch_worker",
    "run_insert_worker",
    "run_output_worker",
]
