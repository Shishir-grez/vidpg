"""PostgreSQL 18 statistics snapshots and before/after deltas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from .connection import PgConnection


@dataclass(frozen=True, slots=True)
class RelationSize:
    relation: str
    heap_bytes: int
    index_bytes: int
    toast_total_bytes: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class RelationSizes:
    entries: tuple[RelationSize, ...]

    def by_relation(self) -> dict[str, RelationSize]:
        """Return a convenient relation-name lookup for delta calculation."""

        return {entry.relation: entry for entry in self.entries}


StatsRows = tuple[tuple[tuple[str, object], ...], ...]


@dataclass(frozen=True, slots=True)
class PgSnapshot:
    sampled_at: datetime
    wal_records: int
    wal_fpi: int
    wal_bytes: int
    wal_buffers_full: int
    io_rows: StatsRows
    user_table_rows: StatsRows
    all_table_rows: StatsRows
    activity_rows: StatsRows
    lock_rows: StatsRows
    notification_queue_fraction: float
    relation_sizes: RelationSizes


@dataclass(frozen=True, slots=True)
class PgDelta:
    wal_records: int
    wal_fpi: int
    wal_bytes: int
    wal_buffers_full: int
    notification_queue_fraction: float
    relation_sizes: RelationSizes


def capture_pg_stats(conn: PgConnection) -> PgSnapshot:
    """Capture the fixed PostgreSQL 18 observability views used by P3."""

    wal = _rows(
        conn,
        "SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM pg_stat_wal",
    )
    io = _rows(
        conn,
        """
        SELECT backend_type, object, context, reads, read_bytes, read_time,
               writes, write_bytes, write_time, extends, extend_bytes,
               extend_time, hits, evictions, fsyncs, fsync_time
        FROM pg_stat_io
        WHERE object IN ('relation', 'wal')
        ORDER BY object, backend_type, context
        """,
    )
    user_tables = _rows(
        conn,
        """
        SELECT relid::regclass AS relation, seq_scan, idx_scan,
               n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup,
               vacuum_count, autovacuum_count, last_vacuum, last_autovacuum
        FROM pg_stat_user_tables
        WHERE schemaname = 'vidpg'
        ORDER BY relname
        """,
    )
    all_tables = _rows(
        conn,
        """
        SELECT relid::regclass AS relation, n_live_tup, n_dead_tup,
               n_tup_ins, n_tup_upd, n_tup_del, vacuum_count,
               autovacuum_count, last_vacuum, last_autovacuum
        FROM pg_stat_all_tables
        WHERE schemaname = 'vidpg'
        ORDER BY relname
        """,
    )
    activity = _rows(
        conn,
        """
        SELECT pid, usename, application_name, state, xact_start,
               query_start, wait_event_type, wait_event
        FROM pg_stat_activity
        WHERE xact_start IS NOT NULL
        ORDER BY xact_start
        """,
    )
    locks = _rows(
        conn,
        """
        SELECT a.pid, a.state, l.mode, l.granted,
               l.relation::regclass AS relation, a.xact_start
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        JOIN pg_class c ON c.oid = l.relation
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'vidpg'
        ORDER BY l.granted, a.xact_start
        """,
    )
    queue_row = conn.execute(
        "SELECT pg_notification_queue_usage() AS queue_fraction"
    ).fetchone()
    if queue_row is None:
        raise RuntimeError("PostgreSQL did not return notification queue usage")
    sizes = relation_sizes(conn)
    if not conn.autocommit:
        conn.commit()

    if not wal:
        raise RuntimeError("PostgreSQL did not return WAL statistics")
    wal_row = wal[0]
    return PgSnapshot(
        sampled_at=datetime.now(UTC),
        wal_records=_as_int(wal_row.get("wal_records")),
        wal_fpi=_as_int(wal_row.get("wal_fpi")),
        wal_bytes=_as_int(wal_row.get("wal_bytes")),
        wal_buffers_full=_as_int(wal_row.get("wal_buffers_full")),
        io_rows=_freeze_rows(io),
        user_table_rows=_freeze_rows(user_tables),
        all_table_rows=_freeze_rows(all_tables),
        activity_rows=_freeze_rows(activity),
        lock_rows=_freeze_rows(locks),
        notification_queue_fraction=float(queue_row[0]),
        relation_sizes=sizes,
    )


def diff_pg_stats(before: PgSnapshot, after: PgSnapshot) -> PgDelta:
    """Calculate monotonic counter and relation-size deltas."""

    before_sizes = before.relation_sizes.by_relation()
    after_sizes = after.relation_sizes.by_relation()
    relations: list[RelationSize] = []
    for relation in sorted(set(before_sizes) | set(after_sizes)):
        left = before_sizes.get(relation)
        right = after_sizes.get(relation)
        relations.append(
            RelationSize(
                relation=relation,
                heap_bytes=(right.heap_bytes if right else 0)
                - (left.heap_bytes if left else 0),
                index_bytes=(right.index_bytes if right else 0)
                - (left.index_bytes if left else 0),
                toast_total_bytes=(right.toast_total_bytes if right else 0)
                - (left.toast_total_bytes if left else 0),
                total_bytes=(right.total_bytes if right else 0)
                - (left.total_bytes if left else 0),
            )
        )
    return PgDelta(
        wal_records=after.wal_records - before.wal_records,
        wal_fpi=after.wal_fpi - before.wal_fpi,
        wal_bytes=after.wal_bytes - before.wal_bytes,
        wal_buffers_full=after.wal_buffers_full - before.wal_buffers_full,
        notification_queue_fraction=(
            after.notification_queue_fraction - before.notification_queue_fraction
        ),
        relation_sizes=RelationSizes(tuple(relations)),
    )


def relation_sizes(conn: PgConnection) -> RelationSizes:
    """Return heap, index, TOAST, and total sizes for logged relations."""

    rows = _rows(
        conn,
        """
        SELECT c.oid::regclass AS relation,
               pg_relation_size(c.oid) AS heap_bytes,
               pg_indexes_size(c.oid) AS index_bytes,
               CASE WHEN c.reltoastrelid = 0 THEN 0
                    ELSE pg_total_relation_size(c.reltoastrelid) END
                    AS toast_total_bytes,
               pg_total_relation_size(c.oid) AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'vidpg' AND c.relkind = 'r'
        ORDER BY c.relname
        """,
    )
    return RelationSizes(
        tuple(
            RelationSize(
                relation=str(row["relation"]),
                heap_bytes=_as_int(row["heap_bytes"]),
                index_bytes=_as_int(row["index_bytes"]),
                toast_total_bytes=_as_int(row["toast_total_bytes"]),
                total_bytes=_as_int(row["total_bytes"]),
            )
            for row in rows
        )
    )


def _rows(conn: PgConnection, query: str) -> list[dict[str, object]]:
    cursor = conn.execute(query)
    description = cursor.description
    if description is None:
        return []
    names = [column.name for column in description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _freeze_rows(rows: list[dict[str, object]]) -> StatsRows:
    return tuple(tuple(sorted(row.items())) for row in rows)


def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise RuntimeError("PostgreSQL statistic is not numeric")
    return int(value)


__all__ = [
    "PgDelta",
    "PgSnapshot",
    "RelationSize",
    "RelationSizes",
    "capture_pg_stats",
    "diff_pg_stats",
    "relation_sizes",
]
