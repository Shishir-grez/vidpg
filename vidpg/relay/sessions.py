"""Ephemeral in-memory session state and bounded client slots for P4."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from vidpg.observability import MetricRegistry
from vidpg.queues import LatestSlot, OfferResult, StreamState

from .auth import (
    SecretHash,
    Side,
    authorize_join,
    hash_secret,
    validate_secret,
    validate_secret_hash,
    validate_side,
)
from .errors import (
    CLOSE_REPLACED,
    ControlOverflowError,
    DuplicateSideError,
    RelayError,
    SessionFullError,
    SessionNotFoundError,
)
from .streams import derive_incoming_stream, derive_upload_stream

SESSION_IDLE_EXPIRY_SECONDS = 60.0
CONTROL_QUEUE_CAPACITY = 32
TARGET_FPS = 30
MAX_FRAME_BYTES = 524_288
WS_BUFFER_THRESHOLD_BYTES = 524_288


@dataclass(slots=True)
class ClientState:
    """One socket generation and its bounded control/output state."""

    side: Side
    socket: Any | None = None
    output_slot: LatestSlot = field(default_factory=LatestSlot)
    control_queue: deque[dict[str, Any]] = field(default_factory=deque)
    output_event: asyncio.Event = field(default_factory=asyncio.Event)
    control_capacity: int = CONTROL_QUEUE_CAPACITY
    has_connected: bool = False
    closed: bool = False
    replacement_total: int = 0
    skipped_total: int = 0
    timeout_total: int = 0

    def offer_frame(self, frame: Any) -> OfferResult:
        """Offer a newest output frame without ever replacing control data."""

        result = self.output_slot.offer(frame)
        if result.accepted:
            self.output_event.set()
        if result.replaced_frame is not None:
            self.replacement_total += 1
        return result

    def enqueue_control(self, message: dict[str, Any]) -> None:
        """Queue a control message FIFO or fail the slow client."""

        if len(self.control_queue) >= self.control_capacity:
            raise ControlOverflowError()
        self.control_queue.append(dict(message))
        self.output_event.set()

    def take_control(self) -> dict[str, Any] | None:
        """Take the oldest control message, preserving control fairness."""

        if not self.control_queue:
            return None
        return self.control_queue.popleft()

    def reset_output(self) -> None:
        """Discard pending output on disconnect; stale frames are never replayed."""

        self.output_slot = LatestSlot()
        self.control_queue.clear()
        self.output_event.clear()

    def mark_disconnected(self) -> None:
        """Stop this socket generation and clear all pending destination state."""

        self.socket = None
        self.closed = True
        self.reset_output()


@dataclass(slots=True)
class RelayStreamState:
    """Per-direction state shared by admission, workers, and fanout."""

    stream_id: UUID
    queue_state: StreamState = field(init=False)
    input_event: asyncio.Event = field(init=False)
    fetch_event: asyncio.Event = field(init=False)
    latest_signaled_seq: int = 0
    dirty_generation: int = 0
    fetch_in_progress: bool = False
    last_fetched_seq: int = 0
    last_published_seq: int = 0
    subscriber: ClientState | None = None
    closed: bool = False

    def __post_init__(self) -> None:
        self.queue_state = StreamState(self.stream_id)
        self.input_event = asyncio.Event()
        self.fetch_event = asyncio.Event()

    def input_slot(self) -> LatestSlot:
        return self.queue_state.input_slot()

    def output_slot(self) -> LatestSlot:
        return self.queue_state.output_slot()

    def last_accepted(self) -> int:
        return self.queue_state.last_accepted()

    def note_accepted(self, sequence: int) -> None:
        self.queue_state.note_accepted(sequence)

    def last_published(self) -> int:
        return self.queue_state.last_published()

    def note_published(self, sequence: int) -> None:
        self.queue_state.note_published(sequence)


@dataclass(frozen=True, slots=True)
class SessionView:
    """The non-secret ready payload sent to one authenticated side."""

    side: Side
    upload_stream: UUID
    incoming_stream: UUID
    target_fps: int = TARGET_FPS
    max_frame_bytes: int = MAX_FRAME_BYTES
    ws_buffer_threshold_bytes: int = WS_BUFFER_THRESHOLD_BYTES

    def as_dict(self) -> dict[str, Any]:
        """Return the exact JSON-ready V1 ready message fields."""

        return {
            "type": "ready",
            "side": self.side,
            "upload_stream": str(self.upload_stream),
            "incoming_stream": str(self.incoming_stream),
            "target_fps": self.target_fps,
            "max_frame_bytes": self.max_frame_bytes,
            "ws_buffer_threshold_bytes": self.ws_buffer_threshold_bytes,
        }


@dataclass(frozen=True, slots=True)
class JoinResult:
    """Result of a successful join without capability material."""

    session: Session
    side: Side
    view: SessionView
    created: bool = False
    reconnect: bool = False


@dataclass(slots=True)
class Session:
    """One ephemeral room containing two independent directional streams."""

    session_id: UUID
    secret_hash: SecretHash
    created_at: float
    target_fps: int = TARGET_FPS
    max_frame_bytes: int = MAX_FRAME_BYTES
    ws_buffer_threshold_bytes: int = WS_BUFFER_THRESHOLD_BYTES
    clients: dict[Side, ClientState] = field(default_factory=dict)
    streams: dict[UUID, RelayStreamState] = field(init=False)
    last_disconnect_at: float | None = None
    removed: bool = False

    def __post_init__(self) -> None:
        upload_a = derive_upload_stream(self.session_id, Side.A)
        upload_b = derive_upload_stream(self.session_id, Side.B)
        self.streams = {
            upload_a: RelayStreamState(upload_a),
            upload_b: RelayStreamState(upload_b),
        }

    @property
    def state(self) -> str:
        """Return the documented session state-machine label."""

        connected = sum(
            1
            for client in self.clients.values()
            if (
                not client.closed
                and (client.socket is not None or not client.has_connected)
            )
        )
        if connected == 2:
            return "BOTH_CONNECTED"
        if connected == 1:
            return "ONE_CLIENT_CONNECTED"
        if len(self.clients) == 1:
            return "ONE_CLIENT_CONNECTED"
        if self.clients:
            return "EXPIRING"
        return "EMPTY_SESSION"

    def stream_for_upload(self, side: str | Side) -> RelayStreamState:
        """Return the state owned by one publisher side."""

        return self.streams[derive_upload_stream(self.session_id, side)]

    def stream_for_incoming(self, side: str | Side) -> RelayStreamState:
        """Return the state delivered to one destination side."""

        return self.streams[derive_incoming_stream(self.session_id, side)]

    def view_for(self, side: str | Side) -> SessionView:
        """Build a ready view without including the session secret."""

        selected_side = validate_side(side)
        return SessionView(
            side=selected_side,
            upload_stream=derive_upload_stream(self.session_id, selected_side),
            incoming_stream=derive_incoming_stream(self.session_id, selected_side),
            target_fps=self.target_fps,
            max_frame_bytes=self.max_frame_bytes,
            ws_buffer_threshold_bytes=self.ws_buffer_threshold_bytes,
        )

    def client_for(self, side: str | Side) -> ClientState | None:
        """Return the current client generation for a side, if joined."""

        return self.clients.get(validate_side(side))


class SessionRegistry:
    """Single-process bounded session registry; it deliberately is not persistent."""

    def __init__(
        self,
        max_count: int = 3,
        *,
        max_sessions: int | None = None,
        idle_expiry_seconds: float = SESSION_IDLE_EXPIRY_SECONDS,
        target_fps: int = TARGET_FPS,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        ws_buffer_threshold_bytes: int = WS_BUFFER_THRESHOLD_BYTES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        selected_max = max_count if max_sessions is None else max_sessions
        if not 1 <= selected_max <= 3:
            raise ValueError("max session count must be between 1 and 3")
        if idle_expiry_seconds <= 0:
            raise ValueError("idle expiry must be positive")
        self.max_count = selected_max
        self.idle_expiry_seconds = idle_expiry_seconds
        self.target_fps = target_fps
        self.max_frame_bytes = max_frame_bytes
        self.ws_buffer_threshold_bytes = ws_buffer_threshold_bytes
        self.metrics: MetricRegistry | None = None
        self._clock = clock
        self._sessions: dict[UUID, Session] = {}

    def create(self, session_id: UUID | str, secret_hash: str) -> Session:
        """Create an empty session or return an existing matching-capability room."""

        self.prune_expired()
        selected_id = _coerce_session_id(session_id)
        selected_hash = SecretHash(validate_secret_hash(secret_hash))
        existing = self._sessions.get(selected_id)
        if existing is not None:
            if existing.secret_hash == selected_hash:
                return existing
            raise RelayError("SESSION_EXISTS", "Session already exists", 4001)
        if len(self._sessions) >= self.max_count:
            raise SessionFullError()
        session = Session(
            session_id=selected_id,
            secret_hash=selected_hash,
            created_at=self._clock(),
            target_fps=self.target_fps,
            max_frame_bytes=self.max_frame_bytes,
            ws_buffer_threshold_bytes=self.ws_buffer_threshold_bytes,
        )
        self._sessions[selected_id] = session
        return session

    def join(
        self,
        session_id: UUID | str,
        side: str | Side,
        secret: str,
    ) -> JoinResult:
        """Authorize a side and add it to a room, creating the first room lazily."""

        selected_id = _coerce_session_id(session_id)
        selected_side = validate_side(side)
        validate_secret(secret)
        session = self._sessions.get(selected_id)
        created = False
        if session is None:
            session = self.create(selected_id, hash_secret(secret))
            created = True

        auth_result = authorize_join(session, selected_side, secret)
        if not auth_result.ok:
            if auth_result.reason == "BAD_SECRET":
                from .errors import BadSecretError

                raise BadSecretError()
            raise ValueError("join side is invalid")

        existing = session.clients.get(selected_side)
        reconnect = False
        if existing is not None:
            if existing.socket is None or existing.closed:
                if not existing.has_connected:
                    raise DuplicateSideError()
            else:
                raise DuplicateSideError()
        else:
            session.clients[selected_side] = ClientState(selected_side)
        session.last_disconnect_at = None
        return JoinResult(
            session=session,
            side=selected_side,
            view=session.view_for(selected_side),
            created=created,
            reconnect=reconnect,
        )

    def attach_socket(
        self,
        session_id: UUID | str,
        side: str | Side,
        socket: Any,
    ) -> None:
        """Attach one socket generation, replacing a valid old generation."""

        session = self.get(session_id)
        selected_side = validate_side(side)
        current = session.clients.get(selected_side)
        if current is None:
            raise SessionNotFoundError()
        if current.socket is not None and not current.closed:
            _schedule_socket_close(current.socket, CLOSE_REPLACED, "replaced")
            current.closed = True
            current.reset_output()
        session.clients[selected_side] = ClientState(
            selected_side,
            socket=socket,
            has_connected=True,
        )
        session.last_disconnect_at = None

    def client_state(self, session_id: UUID | str, side: str | Side) -> ClientState:
        """Return the currently attached socket state."""

        session = self.get(session_id)
        client = session.clients.get(validate_side(side))
        if client is None:
            raise SessionNotFoundError()
        return client

    def remove_socket(
        self,
        session_id: UUID | str,
        side: str | Side,
        socket: Any | None = None,
    ) -> None:
        """Clear a socket only if it is still the current generation."""

        session = self._sessions.get(_coerce_session_id(session_id))
        if session is None:
            return
        selected_side = validate_side(side)
        current = session.clients.get(selected_side)
        if current is None or (socket is not None and current.socket is not socket):
            return
        current.mark_disconnected()
        if not any(
            client.socket is not None and not client.closed
            for client in session.clients.values()
        ):
            session.last_disconnect_at = self._clock()

    def get(self, session_id: UUID | str) -> Session:
        """Return a live session or raise without revealing secret state."""

        selected_id = _coerce_session_id(session_id)
        self.prune_expired()
        session = self._sessions.get(selected_id)
        if session is None or session.removed:
            raise SessionNotFoundError()
        return session

    def remove(self, session_id: UUID | str) -> Session | None:
        """Remove a room and mark its worker state closed."""

        session = self._sessions.pop(_coerce_session_id(session_id), None)
        if session is None:
            return None
        session.removed = True
        for stream in session.streams.values():
            stream.closed = True
            stream.input_event.set()
            stream.fetch_event.set()
        for client in session.clients.values():
            client.mark_disconnected()
        return session

    def prune_expired(self, now: float | None = None) -> tuple[Session, ...]:
        """Remove rooms whose two sides have been disconnected for 60 seconds."""

        current_time = self._clock() if now is None else now
        expired: list[Session] = []
        for session_id, session in tuple(self._sessions.items()):
            if (
                session.last_disconnect_at is not None
                and current_time - session.last_disconnect_at
                >= self.idle_expiry_seconds
            ):
                self._sessions.pop(session_id, None)
                session.removed = True
                for stream in session.streams.values():
                    stream.closed = True
                    stream.input_event.set()
                    stream.fetch_event.set()
                for client in session.clients.values():
                    client.mark_disconnected()
                expired.append(session)
        return tuple(expired)

    def limit_sessions(self, max_count: int) -> None:
        """Set the bounded room count without allowing an active overflow."""

        if not 1 <= max_count <= 3:
            raise ValueError("max session count must be between 1 and 3")
        if max_count < len(self._sessions):
            raise ValueError("cannot lower limit below active session count")
        self.max_count = max_count

    @property
    def session_count(self) -> int:
        """Return the number of currently retained rooms."""

        return len(self._sessions)

    @property
    def sessions(self) -> tuple[Session, ...]:
        """Return a read-only snapshot for operational status and tests."""

        return tuple(self._sessions.values())


def _coerce_session_id(value: UUID | str) -> UUID:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError as exc:
            raise ValueError("session_id must be a UUID") from exc
    raise TypeError("session_id must be a UUID")


def _schedule_socket_close(socket: Any, code: int, reason: str) -> None:
    """Close an old Starlette or test socket without blocking registry mutation."""

    close = getattr(socket, "close", None)
    if close is None:
        return
    try:
        result = close(code=code, reason=reason)
    except TypeError:
        result = close(code)
    if inspect.isawaitable(result):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(_consume_awaitable(result))
            return
        asyncio.ensure_future(result, loop=loop)


async def _consume_awaitable(value: Any) -> None:
    await value


__all__ = [
    "CONTROL_QUEUE_CAPACITY",
    "ClientState",
    "JoinResult",
    "MAX_FRAME_BYTES",
    "RelayStreamState",
    "Session",
    "SessionRegistry",
    "SessionView",
    "TARGET_FPS",
    "WS_BUFFER_THRESHOLD_BYTES",
]
