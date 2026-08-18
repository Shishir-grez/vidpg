"""Schema application and PostgreSQL catalog contract checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .buckets import BUCKET_TABLES
from .connection import PgConnection

SCHEMA_NAME = "vidpg"
CONTROL_TABLE = "bucket_state"
BUCKET_TABLE_NAMES = tuple(table.rsplit(".", 1)[1] for table in BUCKET_TABLES)
INDEX_NAMES = tuple(f"{name}_latest" for name in BUCKET_TABLE_NAMES)


class SchemaContractError(RuntimeError):
    """Raised when the live catalog does not match the P3 schema."""


@dataclass(frozen=True, slots=True)
class SchemaReport:
    """Validated catalog facts needed by the frame plane."""

    schema_name: str
    bucket_state_logged: bool
    bucket_tables_unlogged: tuple[str, ...]
    payload_storage_external: tuple[str, ...]
    latest_indexes: tuple[str, ...]


def apply_schema(conn: PgConnection, ddl_path: str | Path) -> None:
    """Apply one trusted migration file and commit it atomically."""

    sql = Path(ddl_path).read_text(encoding="utf-8")
    try:
        conn.execute(sql)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def assert_schema_matches_contract(conn: PgConnection) -> SchemaReport:
    """Inspect catalog persistence, storage, columns, and fixed indexes."""

    tables = _rows(
        conn,
        """
        SELECT c.relname, c.relpersistence
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname IN (%s, %s, %s, %s)
        """,
        (SCHEMA_NAME, CONTROL_TABLE, *BUCKET_TABLE_NAMES),
    )
    persistence = {str(row[0]): str(row[1]) for row in tables}

    storage_rows = _rows(
        conn,
        """
        SELECT c.relname, a.attstorage
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_attribute AS a ON a.attrelid = c.oid
        WHERE n.nspname = %s
          AND c.relname IN (%s, %s, %s)
          AND a.attname = 'frame'
          AND NOT a.attisdropped
        """,
        (SCHEMA_NAME, *BUCKET_TABLE_NAMES),
    )
    storage = {str(row[0]): str(row[1]) for row in storage_rows}

    index_rows = _rows(
        conn,
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s
          AND indexname IN (%s, %s, %s)
        """,
        (SCHEMA_NAME, *INDEX_NAMES),
    )
    indexes = {str(row[0]): str(row[1]) for row in index_rows}

    column_rows = _rows(
        conn,
        """
        SELECT table_name, column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name IN (%s, %s, %s)
        """,
        (SCHEMA_NAME, *BUCKET_TABLE_NAMES),
    )
    columns = {
        (str(row[0]), str(row[1])): (str(row[2]), str(row[3])) for row in column_rows
    }

    problems: list[str] = []
    if persistence.get(CONTROL_TABLE) != "p":
        problems.append("bucket_state is not logged")
    for table in BUCKET_TABLE_NAMES:
        if persistence.get(table) != "u":
            problems.append(f"{table} is not unlogged")
        if storage.get(table) != "e":
            problems.append(f"{table}.frame is not EXTERNAL")
        for column in (
            "stream_id",
            "seq",
            "captured_us",
            "relay_received_at",
            "inserted_at",
            "codec",
            "width",
            "height",
            "frame",
        ):
            if (table, column) not in columns:
                problems.append(f"{table}.{column} is missing")
    for index_name in INDEX_NAMES:
        definition = indexes.get(index_name, "")
        if "(stream_id, seq DESC)" not in definition:
            problems.append(f"{index_name} is not stream_id/seq DESC")

    if not persistence:
        problems.append("vidpg schema is missing")
    if problems:
        raise SchemaContractError("; ".join(problems))

    report = SchemaReport(
        schema_name=SCHEMA_NAME,
        bucket_state_logged=True,
        bucket_tables_unlogged=BUCKET_TABLE_NAMES,
        payload_storage_external=BUCKET_TABLE_NAMES,
        latest_indexes=INDEX_NAMES,
    )
    if not conn.autocommit:
        conn.commit()
    return report


def _rows(
    conn: PgConnection,
    query: str,
    params: tuple[Any, ...],
) -> list[tuple[Any, ...]]:
    cursor = conn.execute(query, params)
    return [tuple(row) for row in cursor.fetchall()]


__all__ = [
    "BUCKET_TABLE_NAMES",
    "CONTROL_TABLE",
    "INDEX_NAMES",
    "SCHEMA_NAME",
    "SchemaContractError",
    "SchemaReport",
    "apply_schema",
    "assert_schema_matches_contract",
]
