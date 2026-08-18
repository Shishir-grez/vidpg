"""Small Psycopg 3 connection wrappers with fixed P3 pool bounds."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from queue import Empty, LifoQueue
from threading import Condition
from typing import Any

import psycopg
from psycopg.rows import tuple_row

from vidpg.config import Settings

PgConnection = psycopg.Connection[Any]  # noqa: UP040


class PgPool:
    """A bounded lazy pool for the small V1 writer/fetch workload."""

    def __init__(self, database_url: str, max_size: int = 2) -> None:
        if not database_url:
            raise ValueError("database_url must be non-empty")
        if not 1 <= max_size <= 2:
            raise ValueError("P3 pool max_size must be between 1 and 2")
        self.database_url = database_url
        self.max_size = max_size
        self._available: LifoQueue[PgConnection] = LifoQueue(maxsize=max_size)
        self._condition = Condition()
        self._created = 0
        self._closed = False

    def getconn(self) -> PgConnection:
        """Borrow one connection, opening it lazily within the pool bound."""

        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("PostgreSQL pool is closed")
                try:
                    return self._available.get_nowait()
                except Empty:
                    if self._created < self.max_size:
                        self._created += 1
                        break
                    self._condition.wait()

        try:
            connection = psycopg.connect(
                self.database_url,
                autocommit=False,
                row_factory=tuple_row,
                connect_timeout=2,
            )
            verify_session_settings(connection)
            return connection
        except BaseException:
            with self._condition:
                self._created -= 1
                self._condition.notify()
            raise

    def putconn(self, connection: PgConnection) -> None:
        """Return a connection or close it when the pool is shutting down."""

        with self._condition:
            if self._closed:
                connection.close()
            else:
                self._available.put_nowait(connection)
            self._condition.notify()

    @contextmanager
    def connection(self) -> Iterator[PgConnection]:
        """Borrow and reliably return one pooled connection."""

        connection = self.getconn()
        try:
            yield connection
        finally:
            self.putconn(connection)

    def close(self) -> None:
        """Close idle connections and reject future borrows."""

        idle: list[PgConnection] = []
        with self._condition:
            self._closed = True
            while True:
                try:
                    idle.append(self._available.get_nowait())
                except Empty:
                    break
            self._condition.notify_all()
        for connection in idle:
            connection.close()


def open_pool(settings: Settings) -> PgPool:
    """Open the bounded P3 writer/fetch pool."""

    return PgPool(settings.database_url, max_size=2)


def open_dedicated_listener(settings: Settings) -> PgConnection:
    """Open the autocommit connection dedicated to LISTEN/NOTIFY."""

    connection = psycopg.connect(
        settings.database_url,
        autocommit=True,
        row_factory=tuple_row,
        connect_timeout=2,
    )
    try:
        verify_session_settings(connection)
    except BaseException:
        connection.close()
        raise
    return connection


def open_maintenance_connection(settings: Settings) -> PgConnection:
    """Open the single transaction-capable connection used by bucket rotation."""

    connection = psycopg.connect(
        settings.database_url,
        autocommit=False,
        row_factory=tuple_row,
        connect_timeout=2,
    )
    try:
        verify_session_settings(connection)
    except BaseException:
        connection.close()
        raise
    return connection


def verify_session_settings(conn: PgConnection) -> None:
    """Require PostgreSQL 18 before using the V1 database plane."""

    row = conn.execute("SHOW server_version_num").fetchone()
    if row is None:
        raise RuntimeError("PostgreSQL did not return server_version_num")
    value = row[0]
    try:
        version_num = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("server_version_num is not numeric") from exc
    if version_num < 180000:
        raise RuntimeError("VidPG P3 requires PostgreSQL 18 or newer")
    if not conn.autocommit:
        conn.commit()


__all__ = [
    "PgConnection",
    "PgPool",
    "open_dedicated_listener",
    "open_maintenance_connection",
    "open_pool",
    "verify_session_settings",
]
